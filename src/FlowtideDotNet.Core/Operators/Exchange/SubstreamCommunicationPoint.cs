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
using Microsoft.Extensions.Logging;
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
        private readonly ILogger _logger;
        private readonly string _selfSubstreamName;
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

        // Initialize fields
        private bool _initializedSent = false;
        private long _selfInitializeVersion = 0;
        private bool _initializeRecieved = false;
        private long _targetInitializeVersion = 0;
        private readonly object _initializeLock = new object();

        // Send checkpoint fields
        private long _lastSentCheckpointVersion;
        private readonly object _sendCheckpointLock = new object();


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

        public SubstreamCommunicationPoint(ILogger logger, string selfSubstreamName, string substreamName, ISubstreamCommunicationHandler substreamCommunicationHandler)
        {
            _targetInfos = new ConcurrentDictionary<int, TargetInfo>();
            this._logger = logger;
            this._selfSubstreamName = selfSubstreamName;
            this.substreamName = substreamName;
            this._substreamCommunicationHandler = substreamCommunicationHandler;
            substreamCommunicationHandler.Initialize(GetData, DoFailAndRecover, OnTargetSubstreamInitialize, RecieveCheckpointDone);
        }

        public void RegisterReadOperator(SubstreamReadOperator substreamReadOperator)
        {
            lock (_readOperators)
            {
                _readOperators.Add(substreamReadOperator);
            }
        }

        public Task InitializeOperator(long restorePoint)
        {
            lock (_initializeLock)
            {
                if (_initializedSent)
                {
                    return Task.CompletedTask;
                }
                if (_initializeRecieved)
                {
                    if (restorePoint != _targetInitializeVersion)
                    {
                        var minVersion = Math.Min(restorePoint, _targetInitializeVersion);
                        return DoFailAndRecover(minVersion);
                    }
                }
                _initializedSent = true;
                _selfInitializeVersion = restorePoint;
            }

            return SendInitializeRequest(restorePoint);
        }

        private async Task SendInitializeRequest(long restorePoint)
        {
            SubstreamInitializeResponse? response;

            // Retry multiple times to send the initialize request
            int tryCount = 0;
            do
            {
                _logger.LogInformation("Sending initialize request to substream {substreamName} with restore point {restorePoint}, try {tryCount}", substreamName, restorePoint, tryCount);
                response = await _substreamCommunicationHandler.SendInitializeRequest(restorePoint, default);
                tryCount++;

                if (tryCount > 10)
                {
                    throw new InvalidOperationException($"Failed to initialize substream {substreamName} after {tryCount} tries.");
                }
                if (response.NotStarted)
                {
                    _logger.LogInformation("Substream {substreamName} not started yet, retrying in {delay} ms", substreamName, Math.Min(1000 * tryCount, 10000));
                    await Task.Delay(Math.Min(1000 * tryCount, 10000));
                }
            } while (response.NotStarted);

            if (!response.Success)
            {
                await DoFailAndRecover(response.RestoreVersion);
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

        private Task<SubstreamInitializeResponse> OnTargetSubstreamInitialize(long restorePoint)
        {
            lock (_dataHandledLock)
            {
                if (_dataHandled)
                {
                    return Task.FromResult(new SubstreamInitializeResponse(false, false, restorePoint));
                }
            }
            lock (_initializeLock)
            {
                if (_initializedSent)
                {
                    if (_selfInitializeVersion != _targetInitializeVersion)
                    {
                        var minVersion = Math.Min(_selfInitializeVersion, _targetInitializeVersion);
                        return Task.FromResult(new SubstreamInitializeResponse(false, false, minVersion));
                    }
                }
                _initializeRecieved = true;
                _targetInitializeVersion = restorePoint;
            }
            return Task.FromResult(new SubstreamInitializeResponse(false, true, restorePoint));
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

        private Task RecieveCheckpointDone(long checkpointVersion)
        {
            _logger.LogDebug($"Recieved checkpoint done from substream {substreamName} to {_selfSubstreamName} with version {checkpointVersion}, notifying targets and read operators.");
            // Call all targets and read operators that the connected substream have completed the checkpoint
            return Task.Factory.StartNew(() =>
            {
                foreach (var target in _targetInfos)
                {
                    target.Value.Target.TargetSubstreamCheckpointDone(checkpointVersion);
                }
                foreach (var readOperator in _readOperators)
                {
                    readOperator.RecieveCheckpointDone(checkpointVersion);
                }
            }, default, TaskCreationOptions.None, TaskScheduler.Default);
        }

        public Task SendCheckpointDone(long checkpointVersion)
        {
            lock (_sendCheckpointLock)
            {
                if (checkpointVersion <= _lastSentCheckpointVersion)
                {
                    // Already sent this checkpoint or a later one
                    return Task.CompletedTask;
                }
                _lastSentCheckpointVersion = checkpointVersion;
            }
            _logger.LogDebug($"Sending checkpoint done to target: {substreamName} from {_selfSubstreamName}");
            return _substreamCommunicationHandler.SendCheckpointDone(checkpointVersion);
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
                            _logger.LogError(task.Exception, "Fetch data loop for substream {substreamName} terminated with an error.", substreamName);
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
                    _logger.LogError(ex, "Error fetching data from substream {substreamName}", substreamName);
                }
            }
        }
    }
}
