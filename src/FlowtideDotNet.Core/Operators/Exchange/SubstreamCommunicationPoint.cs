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
using FlowtideDotNet.Storage.Memory;
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

        // Checkpoint epoch, guarded by _initializeLock. Identifies this generation of the point:
        // the seed starts at the clock so a rebuilt point (hard restart, grain reactivation) never
        // reuses an epoch a previous generation announced, and every failure draws a fresh value.
        // The self epoch is announced through the handshake and recorded by the peer as its peer
        // epoch. Checkpoint done acks are tagged with the peer epoch, so an ack from before a
        // restart - soft or hard - carries an old epoch and is dropped by RecieveCheckpointDone.
        private static long _checkpointEpochSeed = DateTime.UtcNow.Ticks;
        private long _selfCheckpointEpoch = Interlocked.Increment(ref _checkpointEpochSeed);
        private long _peerCheckpointEpoch = 0;

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
            substreamCommunicationHandler.SetReceiveAllocatorResolver(GetReceiveAllocator);
        }

        /// <summary>
        /// Allocator that received events for the target are deserialized with (the consuming read
        /// operator's). Only fetched for subscribed targets, so that operator is already initialized.
        /// </summary>
        private IMemoryAllocator GetReceiveAllocator(int exchangeTargetId)
        {
            lock (_readOperators)
            {
                foreach (var readOperator in _readOperators)
                {
                    if (readOperator.ExchangeTargetId == exchangeTargetId)
                    {
                        return readOperator.ReceiveMemoryAllocator;
                    }
                }
            }
            throw new InvalidOperationException($"No read operator registered for exchange target {exchangeTargetId}");
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
            await SendInitializeRequest(restorePoint, allowEpochReseed: true);
        }

        private async Task SendInitializeRequest(long restorePoint, bool allowEpochReseed)
        {
            SubstreamInitializeResponse? response;
            long selfEpoch;
            lock (_initializeLock)
            {
                selfEpoch = _selfCheckpointEpoch;
            }

            try
            {
                // Retry multiple times to send the initialize request
                int tryCount = 0;
                do
                {
                    lock (_initializeLock)
                    {
                        if (_selfCheckpointEpoch != selfEpoch)
                        {
                            // The stream failed while this handshake was pending, the loop belongs
                            // to the aborted generation. It must stop announcing its stale epoch,
                            // a late announcement would regress what the restarted generation's
                            // own handshake already recorded at the peer, and every ack the peer
                            // sends afterwards would be dropped as stale.
                            return;
                        }
                    }
                    _logger.LogInformation("Sending initialize request to substream {substreamName} with restore point {restorePoint}, try {tryCount}", substreamName, restorePoint, tryCount);
                    response = await _substreamCommunicationHandler.SendInitializeRequest(restorePoint, selfEpoch, default);
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
                // initializes again after the failure. Only for the generation that started it:
                // after a failure the handshake flags belong to the restarted generation.
                lock (_initializeLock)
                {
                    if (_selfCheckpointEpoch == selfEpoch)
                    {
                        _initializedSent = false;
                    }
                }
                throw;
            }

            if (allowEpochReseed && response.RecordedCheckpointEpoch > selfEpoch)
            {
                // The peer holds a higher checkpoint epoch for this substream than this
                // generation announced, recorded from a generation that no longer exists:
                // epochs are clock-seeded per process, so a hard fail over onto a process
                // whose clock seed is behind announces lower than the dead generation did.
                // The peer's highest-wins guard keeps the dead record, so every ack it sends
                // would be tagged with it and dropped here - a permanent, silent checkpoint
                // stall. The seed is raised above the recorded epoch and the handshake re-run
                // once with a fresh draw, moving the record to this generation. Re-seeding is
                // capped at once per handshake; a lost race against an even higher claim
                // converges through the next recovery's handshake.
                lock (_initializeLock)
                {
                    if (_selfCheckpointEpoch != selfEpoch)
                    {
                        // The stream failed while the response was in flight, the restarted
                        // generation runs its own handshake.
                        return;
                    }
                    long seed;
                    do
                    {
                        seed = Interlocked.Read(ref _checkpointEpochSeed);
                    } while (seed < response.RecordedCheckpointEpoch &&
                             Interlocked.CompareExchange(ref _checkpointEpochSeed, response.RecordedCheckpointEpoch, seed) != seed);
                    _selfCheckpointEpoch = Interlocked.Increment(ref _checkpointEpochSeed);
                }
                _logger.LogWarning(
                    "The initialize handshake to substream {substreamName} announced checkpoint epoch {announced} but a higher epoch {recorded} is recorded there, re-announcing with a fresh epoch.",
                    substreamName, selfEpoch, response.RecordedCheckpointEpoch);
                await SendInitializeRequest(restorePoint, allowEpochReseed: false);
                return;
            }

            lock (_initializeLock)
            {
                if (_selfCheckpointEpoch != selfEpoch)
                {
                    // The stream failed while the response was in flight, it belongs to the
                    // aborted generation. Applying it could overwrite the peer epoch that the
                    // restarted generation's own handshake already recorded.
                    return;
                }
                // Highest wins: a response delayed across the peer's failure must not regress
                // what a newer handshake already recorded. Peer generations draw from a clock
                // seed, so the current generation's epoch is always the highest.
                _peerCheckpointEpoch = Math.Max(_peerCheckpointEpoch, response.CheckpointEpoch);
            }

            if (!response.Success)
            {
                // The peer already reconciled to response.RestoreVersion, recover to it and
                // stop. Falling through to the mismatch check below would recover a second
                // time for the same handshake (RestoreVersion is <= restorePoint on a refusal).
                await DoFailAndRecover(response.RestoreVersion);
                return;
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
                // New generation: acks tagged with the old epoch are now stale and get dropped.
                // Drawn from the shared seed so it never collides with any other generation.
                _selfCheckpointEpoch = Interlocked.Increment(ref _checkpointEpochSeed);
            }
            lock (_dataHandledLock)
            {
                _dataHandled = false;
            }
            // Lets the handler change its fetch epoch so in flight fetches from before the
            // failure are refused by the other substream instead of consuming events that the
            // restarted stream needs.
            _substreamCommunicationHandler.OnStreamFailure();
        }

        private Task<SubstreamInitializeResponse> OnTargetSubstreamInitialize(long restorePoint, long peerCheckpointEpoch)
        {
            long selfEpoch;
            long recordedPeerEpoch;
            lock (_initializeLock)
            {
                // Highest wins: a handshake from an aborted generation can land after the current
                // generation already announced (a request in flight across the peer's failure).
                // Applying it would regress the record, and every ack sent afterwards would be
                // tagged stale and dropped - stalling the peer's checkpoints while data still
                // flows, so no watchdog would fire. Peer generations draw from a clock seed, so
                // the current generation's epoch is always the highest. A peer that hard-fails
                // over onto a process whose clock is far behind legitimately announces a lower
                // epoch and is refused here; the response carries the recorded epoch so it can
                // re-seed above it and re-announce, see SendInitializeRequest.
                _peerCheckpointEpoch = Math.Max(_peerCheckpointEpoch, peerCheckpointEpoch);
                recordedPeerEpoch = _peerCheckpointEpoch;
                selfEpoch = _selfCheckpointEpoch;
            }
            lock (_dataHandledLock)
            {
                if (_dataHandled)
                {
                    // The other substream restarted mid-run after events were exchanged, so this
                    // streams state can depend on events it no longer knows about. Roll back to its
                    // restore point and return not started so it retries the handshake.
                    _logger.LogInformation("Substream {substreamName} initialized with restore point {restorePoint} while this stream has live exchanged data, failing over to it.", substreamName, restorePoint);
                    _ = Task.Run(async () =>
                    {
                        try
                        {
                            await DoFailAndRecover(restorePoint);
                        }
                        catch (Exception e)
                        {
                            // The rollback failed, the other substream retries the handshake.
                            // Must be logged, an unobserved fault here would let the
                            // substreams silently diverge.
                            _logger.LogWarning(e, "Failing over to the restarted substream {substreamName} failed, the handshake retry runs the fail over again.", substreamName);
                        }
                    });
                    return Task.FromResult(new SubstreamInitializeResponse(true, false, restorePoint, selfEpoch, recordedPeerEpoch));
                }
            }
            lock (_initializeLock)
            {
                if (_initializedSent)
                {
                    // Compare against the version in the incoming request, _targetInitializeVersion
                    // still holds the value from the previous epoch at this point which would
                    // force both substreams down to that stale version.
                    if (_selfInitializeVersion != restorePoint)
                    {
                        var minVersion = Math.Min(_selfInitializeVersion, restorePoint);
                        return Task.FromResult(new SubstreamInitializeResponse(false, false, minVersion, selfEpoch, recordedPeerEpoch));
                    }
                }
                _initializeRecieved = true;
                _targetInitializeVersion = restorePoint;
            }
            return Task.FromResult(new SubstreamInitializeResponse(false, true, restorePoint, selfEpoch, recordedPeerEpoch));
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
                    // Nothing is registered yet, the stream is still being built, so there is
                    // nothing to recover. The initialize handshake reconciles versions at start.
                    _logger.LogInformation("Received fail and recover to {recoveryPoint} before any exchange operators are registered, nothing to recover.", recoveryPoint);
                }
            }
        }

        public Task SendFailAndRecover(long recoveryPoint)
        {
            return _substreamCommunicationHandler.SendFailAndRecover(recoveryPoint);
        }

        private long _notifyFailInFlightVersion = -1;
        private readonly object _notifyFailLock = new object();

        /// <summary>
        /// Tells the other substream to fail and recover without waiting for the result, it
        /// may be unreachable and waiting out its response timeout would stall the recovery
        /// here. Concurrent notifications for the same recovery point are collapsed into one.
        /// </summary>
        public void NotifyFailAndRecover(long recoveryPoint)
        {
            lock (_notifyFailLock)
            {
                if (_notifyFailInFlightVersion == recoveryPoint)
                {
                    return;
                }
                _notifyFailInFlightVersion = recoveryPoint;
            }
            _ = Task.Run(async () =>
            {
                try
                {
                    await _substreamCommunicationHandler.SendFailAndRecover(recoveryPoint);
                }
                catch (Exception e)
                {
                    _logger.LogWarning(e, "Failed to notify substream {substreamName} about the failure, versions are reconciled at the next initialize handshake.", substreamName);
                }
                finally
                {
                    lock (_notifyFailLock)
                    {
                        if (_notifyFailInFlightVersion == recoveryPoint)
                        {
                            _notifyFailInFlightVersion = -1;
                        }
                    }
                }
            });
        }

        private Task RecieveCheckpointDone(long checkpointVersion, long checkpointEpoch)
        {
            long selfEpoch;
            lock (_initializeLock)
            {
                selfEpoch = _selfCheckpointEpoch;
            }
            if (checkpointEpoch != selfEpoch)
            {
                // Stale ack from a previous epoch, for example one in flight across a restart.
                // Crediting it would complete the current cycle without the peer acking it, so
                // it is dropped. The next handshake re-announces the current epoch.
                _logger.LogWarning("Dropping stale checkpoint done from substream {substreamName}: epoch {ackEpoch} does not match current {selfEpoch}.", substreamName, checkpointEpoch, selfEpoch);
                return Task.CompletedTask;
            }
            _logger.LogDebug("Recieved checkpoint done from substream {substreamName} to {selfSubstreamName} with version {checkpointVersion}, notifying targets and read operators.", substreamName, _selfSubstreamName, checkpointVersion);
            // Tell all targets and read operators the connected substream completed the checkpoint.
            // Task.Run to use the thread pool, this can be called from a grain turn where the grain
            // activation scheduler would otherwise be captured.
            return Task.Run(async () =>
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
            });
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
            long targetEpoch;
            lock (_initializeLock)
            {
                targetEpoch = _peerCheckpointEpoch;
            }
            _logger.LogDebug("Sending checkpoint done to target: {substreamName} from {selfSubstreamName}", substreamName, _selfSubstreamName);
            return _substreamCommunicationHandler.SendCheckpointDone(checkpointVersion, targetEpoch);
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

        // Last time the fetch loop completed an iteration, used by the stall watchdog. The
        // loop runs continuously with short delays while any subscription exists, so a long
        // gap means the loop is blocked, for example delivering an event into a pipeline that
        // deadlocked on checkpoint barrier alignment with another substream.
        private long _lastFetchLoopTick;
        private Timer? _stallWatchdog;
        // Internal so tests can shorten them, a stall test would otherwise take over a
        // minute.
        internal static TimeSpan StallLimit = TimeSpan.FromSeconds(60);
        internal static TimeSpan StallCheckInterval = TimeSpan.FromSeconds(15);

        private void TryStartFetchTask()
        {
            lock (_fetchDataLock)
            {
                if (_fetchDataTask != null)
                {
                    return;
                }

                _lastFetchLoopTick = Environment.TickCount64;
                _stallWatchdog ??= new Timer(CheckFetchLoopStall, null, StallCheckInterval, StallCheckInterval);
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
        /// Fails and recovers the stream when the fetch loop has been blocked for too long,
        /// which happens when the substreams deadlock on each others checkpoint barriers or
        /// startup acks. The recovery rolls both back to a common checkpoint.
        /// </summary>
        private void CheckFetchLoopStall(object? state)
        {
            lock (_fetchDataLock)
            {
                if (_fetchDataTask == null)
                {
                    if (_subscribedTargets.Count == 0)
                    {
                        // No fetch loop and no subscribers, the stream stopped or was
                        // disposed. The timer stops itself, a live timer would root this
                        // object graph forever, a later subscribe creates a new one.
                        _stallWatchdog?.Dispose();
                        _stallWatchdog = null;
                        return;
                    }
                    // A dead loop with live subscribers starves the stream silently, for
                    // example when the recovery started by a fetch error itself failed.
                    // The loop is restarted for the remaining subscribers.
                    _logger.LogWarning("The fetch loop for substream {substreamName} is not running while subscribers exist, restarting it.", substreamName);
                    _lastFetchLoopTick = Environment.TickCount64;
                    TryStartFetchTask();
                    return;
                }
                if (TimeSpan.FromMilliseconds(Environment.TickCount64 - _lastFetchLoopTick) < StallLimit)
                {
                    return;
                }
                // Reset so the watchdog does not fire again while the recovery runs.
                _lastFetchLoopTick = Environment.TickCount64;
            }
            _logger.LogWarning("The fetch loop for substream {substreamName} has been stalled for over {limit}, failing and recovering to break a possible deadlock between the substreams.", substreamName, StallLimit);
            _ = Task.Run(async () =>
            {
                SubstreamReadOperator? readOperator;
                lock (_readOperators)
                {
                    readOperator = _readOperators.FirstOrDefault();
                }
                if (readOperator != null)
                {
                    try
                    {
                        await readOperator.FailAndRecoverOnFetchError(new TimeoutException($"The fetch loop was stalled for over {StallLimit}."));
                    }
                    catch (Exception e)
                    {
                        _logger.LogWarning(e, "Failed to recover the stalled fetch loop for substream {substreamName}.", substreamName);
                    }
                }
            });
        }

        /// <summary>
        /// Ends the fetch loop after an error and fails the stream so it recovers to a common
        /// checkpoint with the other substream. A new fetch loop starts when a read operator
        /// subscribes again after the restore.
        /// </summary>
        private Task FailFetchLoop(Exception exception)
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
                // Fire and forget: the failure handling can wait for this fetch loops own
                // task during teardown, awaiting the recovery from inside the loop would
                // deadlock it. The loop exits right after, a new one starts when a read
                // operator subscribes again after the restore.
                _ = Task.Run(async () =>
                {
                    try
                    {
                        await readOperator.FailAndRecoverOnFetchError(exception);
                    }
                    catch (Exception e)
                    {
                        // Must be logged, an unobserved fault here would leave the stream
                        // running against a fetch loop that already ended.
                        _logger.LogWarning(e, "Failing the stream after a fetch error on substream {substreamName} failed, the stall watchdog retries the recovery.", substreamName);
                    }
                });
            }
            return Task.CompletedTask;
        }

        internal static void DisposeEvent(IStreamEvent streamEvent)
        {
            StreamEventRent.Dispose(streamEvent);
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
                    _lastFetchLoopTick = Environment.TickCount64;
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
