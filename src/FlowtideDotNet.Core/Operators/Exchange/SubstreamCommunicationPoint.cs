// Licensed under the Apache License, Version 2.0 (the "License")
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//  
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using FlowtideDotNet.Base;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.Operators.Exchange
{
    public struct SubstreamEventData
    {
        public int ExchangeTargetId;
        public IStreamEvent StreamEvent;
    }

    internal class SubstreamCommunicationPoint
    {
        private readonly string substreamName;
        private readonly ISubstreamCommunicationHandler _substreamCommunicationHandler;
        private ConcurrentDictionary<int, TargetInfo> _targetInfos;
        private Task? _fetchDataTask;
        private readonly object _fetchDataLock = new object();
        private readonly Dictionary<int, Func<IStreamEvent, Task>> _subscribedTargets = new Dictionary<int, Func<IStreamEvent, Task>>();
        private long _subscribeTargetsVersion = 0;
        private bool _dataHandled = false;
        private readonly object _dataHandledLock = new object();
        private List<SubstreamReadOperator> _readOperators = new List<SubstreamReadOperator>();


        private class TargetInfo
        {
            public readonly object Lock = new object();
            public SubstreamTarget Target { get; }

            public bool HasData { get; set; }

            public TargetInfo(SubstreamTarget target)
            {
                Target = target;
            }
        }

        public SubstreamCommunicationPoint(string substreamName, ISubstreamCommunicationHandler substreamCommunicationHandler)
        {
            _targetInfos = new ConcurrentDictionary<int, TargetInfo>();
            this.substreamName = substreamName;
            this._substreamCommunicationHandler = substreamCommunicationHandler;
            substreamCommunicationHandler.Initialize(GetData, DoFailAndRecover);
        }

        public void RegisterReadOperator(SubstreamReadOperator substreamReadOperator)
        {
            lock (_readOperators)
            {
                _readOperators.Add(substreamReadOperator);
            }
        }

        public void RegisterSubstreamTarget(int exchangeTargetId, SubstreamTarget target)
        {
            _targetInfos.AddOrUpdate(exchangeTargetId, new TargetInfo(target), (key, existing) => existing);
        }

        public ValueTask TargetHasData(int exchangeTargetId)
        {
            if (_targetInfos.TryGetValue(exchangeTargetId, out var targetInfo))
            {
                lock (targetInfo.Lock)
                {
                    targetInfo.HasData = true;
                }
            }
            return ValueTask.CompletedTask;
        }

        /// <summary>
        /// Fetches events from multiple exchange .
        /// The max event count is distributed as equally as possible across the different targets
        /// to fetch data from all of them if possible.
        /// </summary>
        /// <param name="exchangeTargets"></param>
        /// <param name="maxEventCount"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public async Task<IReadOnlyList<SubstreamEventData>> GetData(IReadOnlySet<int> exchangeTargets, int maxEventCount, CancellationToken cancellationToken)
        {
            List<SubstreamEventData> outputList = new List<SubstreamEventData>();

            bool hasMoreData = true;
            int maxCountPerTarget = Math.Max(1, maxEventCount / exchangeTargets.Count);
            while (!cancellationToken.IsCancellationRequested && hasMoreData && outputList.Count < maxEventCount)
            {
                hasMoreData = false;
                cancellationToken.ThrowIfCancellationRequested();

                foreach (var exchangeTarget in exchangeTargets)
                {
                    // Fetch target info
                    if (_targetInfos.TryGetValue(exchangeTarget, out var targetInfo))
                    {
                        // Fetch 10 events from a target before switching to another target to allow fairness
                        hasMoreData |= await targetInfo.Target.ReadData(outputList, maxCountPerTarget);
                    }
                }
            }

            if (outputList.Count > 0)
            {
                lock (_dataHandledLock)
                {
                    _dataHandled = true;
                }
            }

            return outputList;
        }

        private async Task DoFailAndRecover(long recoveryPoint)
        {
            var firstTarget = _targetInfos.FirstOrDefault();
            if (firstTarget.Value != null)
            {
                await firstTarget.Value.Target.FailAndRecover(recoveryPoint);
            }
            else
            {
                SubstreamReadOperator? readOperator;
                lock (_readOperators)
                {
                    readOperator = _readOperators.FirstOrDefault();
                }
                if (readOperator != null)
                {
                    await readOperator.FailAndRecover(recoveryPoint);
                }
                else
                {
                    throw new InvalidOperationException($"Failed to recover {recoveryPoint}");
                }
            }
        }

        public Task SendFailAndRecover(long recoveryPoint)
        {
            return _substreamCommunicationHandler.SendFailAndRecover(recoveryPoint);
        }

        public void Subscribe(int exchangeTarget, Func<IStreamEvent, Task> onData)
        {
            lock (_fetchDataLock)
            {
                _subscribedTargets.Add(exchangeTarget, onData);
                _subscribeTargetsVersion++;
            }
            TryStartFetchTask();
        }

        public void Unsubscribe(int exchangeTarget)
        {
            lock (_fetchDataLock)
            {
                _subscribedTargets.Remove(exchangeTarget);
                _subscribeTargetsVersion++;
            }
        }

        private void TryStartFetchTask()
        {
            lock (_fetchDataLock)
            {
                if (_fetchDataTask != null)
                {
                    return;
                }

                _fetchDataTask = Task.Factory.StartNew(async () =>
                {
                    await FetchDataLoop();
                }, TaskCreationOptions.LongRunning)
                    .Unwrap()
                    .ContinueWith((task) =>
                    {
                        if (task.IsFaulted)
                        {
                            // Handle exceptions
                            _fetchDataTask = null;
                            TryStartFetchTask();
                        }
                    }, TaskContinuationOptions.ExecuteSynchronously | TaskContinuationOptions.OnlyOnFaulted);
            }
        }

        private async Task FetchDataLoop()
        {
            long currentVersion = 0;
            HashSet<int> subscribedTargets = new HashSet<int>();
            Dictionary<int, Func<IStreamEvent, Task>> currentSubscribedTargets = new Dictionary<int, Func<IStreamEvent, Task>>();
            while (true)
            {

                lock (_fetchDataLock)
                {
                    if (_subscribeTargetsVersion > currentVersion)
                    {
                        currentVersion = _subscribeTargetsVersion;
                        subscribedTargets.Clear();
                        subscribedTargets.UnionWith(_subscribedTargets.Keys);
                        currentSubscribedTargets.Clear();
                        foreach (var kvp in _subscribedTargets)
                        {
                            currentSubscribedTargets[kvp.Key] = kvp.Value;
                        }
                    }
                }
                if (subscribedTargets.Count == 0)
                {
                    // No targets to fetch data from, wait for a while before checking again
                    await Task.Delay(100);
                    continue;
                }

                try
                {
                    // Fetch data from the substream communication handler
                    var data = await _substreamCommunicationHandler.FetchData(subscribedTargets, 100, default);
                    if (data.Count > 0)
                    {
                        lock (_dataHandledLock)
                        {
                            _dataHandled = true;
                        }
                        // Process the fetched data
                        foreach (var substreamEventData in data)
                        {
                            if (currentSubscribedTargets.TryGetValue(substreamEventData.ExchangeTargetId, out var onData))
                            {
                                await onData(substreamEventData.StreamEvent);
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    // Log the exception and continue
                }
            }
        }
    }
}
