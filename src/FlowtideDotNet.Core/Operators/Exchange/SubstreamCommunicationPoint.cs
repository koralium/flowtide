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
using System.Collections.Concurrent;

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

        internal ILogger Logger => _logger;

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
                if (!_readOperators.Contains(substreamReadOperator))
                {
                    _readOperators.Add(substreamReadOperator);
                }
            }
        }

        public Task InitializeOperator(long restorePoint)
        {
            lock (_sendCheckpointLock)
            {
                // After a rollback the checkpoint versions start over from the restore point,
                // reset the sent tracking so checkpoint done messages for the new versions
                // are not treated as duplicates of the old ones.
                if (_lastSentCheckpointVersion > restorePoint)
                {
                    _lastSentCheckpointVersion = restorePoint;
                }
            }
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

            try
            {
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
            }
            catch
            {
                // The handshake did not complete, allow it to be retried when the stream
                // initializes again after the failure.
                lock (_initializeLock)
                {
                    _initializedSent = false;
                }
                throw;
            }

            if (!response.Success)
            {
                await DoFailAndRecover(response.RestoreVersion);
            }

            if (response.RestoreVersion != restorePoint)
            {
                _logger.LogInformation("Substream {substreamName} initialized with different restore point {targetRestoreVersion} than requested {restorePoint}, recovering.", substreamName, response.RestoreVersion, restorePoint);
                var minVersion = Math.Min(restorePoint, response.RestoreVersion);
                await DoFailAndRecover(minVersion);
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
        /// Called when the local stream fails. Resets the handshake state so the initialize
        /// handshake runs again when the stream restarts and both substreams converge on a
        /// common restore version before any events are exchanged.
        /// </summary>
        public void OnStreamFailure()
        {
            lock (_initializeLock)
            {
                _initializedSent = false;
                _initializeRecieved = false;
            }
            lock (_dataHandledLock)
            {
                _dataHandled = false;
            }
        }

        private Task<SubstreamInitializeResponse> OnTargetSubstreamInitialize(long restorePoint)
        {
            lock (_dataHandledLock)
            {
                if (_dataHandled)
                {
                    // The other substream restarted while this stream is running with events
                    // already exchanged in this epoch. This streams state can depend on events
                    // the other substream no longer knows about, so this stream rolls back to
                    // the restore point of the other substream. Not started is returned so the
                    // other substream retries the handshake after the failure here has reset
                    // the handshake state.
                    _logger.LogInformation("Substream {substreamName} initialized with restore point {restorePoint} while this stream has live exchanged data, failing over to it.", substreamName, restorePoint);
                    _ = Task.Run(() => DoFailAndRecover(restorePoint));
                    return Task.FromResult(new SubstreamInitializeResponse(true, false, restorePoint));
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
        /// Fetches events from multiple exchange targets.
        /// The max event count is distributed as equally as possible across the different targets
        /// to fetch data from all of them if possible.
        /// </summary>
        public async Task<IReadOnlyList<SubstreamEventData>> GetData(IReadOnlySet<int> targetIds, int maxEventCount, CancellationToken cancellationToken)
        {
            List<SubstreamEventData> outputList = new List<SubstreamEventData>();

            if (targetIds.Count == 0)
            {
                return outputList;
            }

            int maxCountPerTarget = Math.Max(1, maxEventCount / targetIds.Count);

            foreach (var targetId in targetIds)
            {
                if (_targetInfos.TryGetValue(targetId, out var targetInfo))
                {
                    await targetInfo.Target.ReadData(outputList, maxCountPerTarget);
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
            _logger.LogDebug("Recieved checkpoint done from substream {substreamName} to {selfSubstreamName} with version {checkpointVersion}, notifying targets and read operators.", substreamName, _selfSubstreamName, checkpointVersion);
            // Call all targets and read operators that the connected substream have completed the checkpoint
            return Task.Factory.StartNew(async () =>
            {
                try
                {
                    foreach (var target in _targetInfos)
                    {
                        await target.Value.Target.TargetSubstreamCheckpointDone(checkpointVersion);
                    }
                    List<SubstreamReadOperator> readOperators;
                    lock (_readOperators)
                    {
                        readOperators = new List<SubstreamReadOperator>(_readOperators);
                    }
                    foreach (var readOperator in readOperators)
                    {
                        readOperator.RecieveCheckpointDone(checkpointVersion);
                    }
                }
                catch (Exception ex)
                {
                    // The checkpoint done signal is advisory, an error while handling it must
                    // not fail the sending substreams checkpoint. The dependencies simply stay
                    // pending until the next signal arrives.
                    _logger.LogWarning(ex, "Error handling checkpoint done from substream {substreamName}", substreamName);
                }
            }, default, TaskCreationOptions.None, TaskScheduler.Default).Unwrap();
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
            _logger.LogDebug("Sending checkpoint done to target: {substreamName} from {selfSubstreamName}", substreamName, _selfSubstreamName);
            return _substreamCommunicationHandler.SendCheckpointDone(checkpointVersion);
        }

        /// <summary>
        /// Subscribes to events from an exchange target in the other substream.
        /// </summary>
        /// <param name="exchangeTarget">The exchange target id.</param>
        /// <param name="onData">Callback with the event.</param>
        public void Subscribe(int exchangeTarget, Func<IStreamEvent, Task> onData)
        {
            lock (_fetchDataLock)
            {
                // Use the indexer so a re-subscribe after a failure replaces the old callback
                _subscribedTargets[exchangeTarget] = onData;
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

        /// <summary>
        /// Ends the fetch loop after an error and fails the stream so it recovers to a common
        /// checkpoint with the other substream. A new fetch loop starts when a read operator
        /// subscribes again after the restore.
        /// </summary>
        private async Task FailFetchLoop(Exception exception)
        {
            lock (_fetchDataLock)
            {
                _fetchDataTask = null;
            }
            SubstreamReadOperator? readOperator;
            lock (_readOperators)
            {
                readOperator = _readOperators.FirstOrDefault();
            }
            if (readOperator != null)
            {
                await readOperator.FailAndRecoverOnFetchError(exception);
            }
        }

        internal static void DisposeEvent(IStreamEvent streamEvent)
        {
            if (streamEvent is StreamMessage<StreamEventBatch> streamMessage)
            {
                streamMessage.Data.Return();
            }
            else if (streamEvent is IRentable rentable)
            {
                rentable.Return();
            }
            else if (streamEvent is IDisposable disposable)
            {
                disposable.Dispose();
            }
        }

        private async Task FetchDataLoop()
        {
            long currentVersion = 0;
            Dictionary<int, Func<IStreamEvent, Task>> currentSubscribedTargets = new Dictionary<int, Func<IStreamEvent, Task>>();
            HashSet<int> targetIds = new HashSet<int>();
            while (true)
            {

                lock (_fetchDataLock)
                {
                    if (_subscribeTargetsVersion > currentVersion)
                    {
                        currentVersion = _subscribeTargetsVersion;
                        currentSubscribedTargets.Clear();
                        targetIds.Clear();
                        foreach (var kvp in _subscribedTargets)
                        {
                            currentSubscribedTargets[kvp.Key] = kvp.Value;
                            targetIds.Add(kvp.Key);
                        }
                    }
                    if (currentSubscribedTargets.Count == 0)
                    {
                        // No targets to fetch data from, stop the loop.
                        // A new fetch task is started when a target subscribes again.
                        _fetchDataTask = null;
                        return;
                    }
                }

                IReadOnlyList<SubstreamEventData> data;
                try
                {
                    // Fetch data from the substream communication handler
                    data = await _substreamCommunicationHandler.FetchData(targetIds, 100, default);
                }
                catch (Exception ex)
                {
                    // Fetching removes the events from the other substreams queue, a failed
                    // fetch can mean events were removed there but never arrived here. They
                    // cannot be fetched again, so the stream fails and both substreams
                    // recover to a common checkpoint where the events are regenerated.
                    _logger.LogError(ex, "Error fetching data from substream {substreamName}, failing and recovering since fetched events may have been lost.", substreamName);
                    await FailFetchLoop(ex);
                    return;
                }

                if (data.Count > 0)
                {
                    lock (_dataHandledLock)
                    {
                        _dataHandled = true;
                    }
                    // Process the fetched data
                    int processed = 0;
                    try
                    {
                        for (; processed < data.Count; processed++)
                        {
                            var substreamEventData = data[processed];
                            if (currentSubscribedTargets.TryGetValue(substreamEventData.ExchangeTargetId, out var onData))
                            {
                                await onData(substreamEventData.StreamEvent);
                            }
                            else
                            {
                                // The subscriber was removed while the fetch was in flight,
                                // this stream is recovering or stopping and does not need the
                                // event anymore.
                                DisposeEvent(substreamEventData.StreamEvent);
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        // Delivery failed, the remaining events would be dropped, dispose them
                        // and recover, the events are regenerated after the rollback.
                        _logger.LogError(ex, "Error delivering fetched events from substream {substreamName}, failing and recovering.", substreamName);
                        for (int i = processed; i < data.Count; i++)
                        {
                            DisposeEvent(data[i].StreamEvent);
                        }
                        await FailFetchLoop(ex);
                        return;
                    }
                }
                else
                {
                    // No data available, wait a short while before polling again
                    await Task.Delay(10);
                }
            }
        }
    }
}
