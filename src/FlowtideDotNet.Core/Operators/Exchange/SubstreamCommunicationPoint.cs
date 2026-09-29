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
using System.Diagnostics;

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
        // Targets paused at a barrier, see PauseFetch. Guarded by _fetchDataLock.
        private readonly HashSet<int> _pausedTargets = new HashSet<int>();
        private long _subscribeTargetsVersion = 0;
        private bool _dataHandled = false;
        private readonly object _dataHandledLock = new object();
        private List<SubstreamReadOperator> _readOperators = new List<SubstreamReadOperator>();

        // Initialize fields
        private bool _initializedSent = false;
        private long _selfInitializeVersion = 0;
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
        private CancellationTokenSource _operationAbort = new();
        private long _lastSentCheckpointVersion;
        private readonly object _sendCheckpointLock = new object();

        /// <summary>
        /// Holds the deferred checkpoint done crediting dispatch, keyed by the self substream
        /// name, so tests can interleave a failure between the arrival fence and the dispatch.
        /// </summary>
        internal static Func<string, Task>? ReceiveCheckpointDoneDispatchHookForTests;

        // Internal so tests can shorten it, nearly every distributed test start pays at
        // least one slice for the substream that loses the startup race.
        internal static int NotStartedRetrySliceMs = 500;
        // Bounds the wait for a draining peer, budget not spent.
        internal static TimeSpan PeerDrainingWaitLimit = TimeSpan.FromMinutes(5);

        // Highest checkpoint version the peer has acked (its own numbering), -1 while none.
        // A clean handoff reconnect must announce a restore point at or past it. Reset on rollback.
        private long _peerLastCommittedVersion = -1;

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

        // Announces a clean handoff resume in the next handshake. One-shot: cleared once a
        // response arrives, a later handshake (after a rollback) is no longer a clean resume.
        private bool _announceCleanHandoff;

        // The peer accepted this substreams clean handoff reconnect and kept running. Read
        // operators then start from restored state instead of waiting for its init watermarks.
        private volatile bool _cleanReconnect;

        internal bool CleanReconnect => _cleanReconnect;

        // Shared by every communication point of this substream, null outside a substream group.
        private readonly SubstreamDurabilityCoordinator? _durability;
        private readonly SubstreamRecoveryWaves _waves;

        internal SubstreamDurabilityCoordinator? Durability => _durability;

        internal SubstreamRecoveryWaves Waves => _waves;

        public SubstreamCommunicationPoint(ILogger logger, string selfSubstreamName, string substreamName, ISubstreamCommunicationHandler substreamCommunicationHandler, bool announceCleanHandoff = false, SubstreamDurabilityCoordinator? durability = null, SubstreamRecoveryWaves? waves = null)
        {
            _targetInfos = new ConcurrentDictionary<int, TargetInfo>();
            this._logger = logger;
            this._selfSubstreamName = selfSubstreamName;
            this.substreamName = substreamName;
            this._substreamCommunicationHandler = substreamCommunicationHandler;
            _announceCleanHandoff = announceCleanHandoff;
            _durability = durability;
            _waves = waves ?? new SubstreamRecoveryWaves();
            _durability?.Register(substreamName, this);
            _waves.Register(this);
            substreamCommunicationHandler.Initialize(GetData, OnPeerRecovering, OnTargetSubstreamInitialize, RecieveCheckpointDone);
            substreamCommunicationHandler.InitializeDurabilityClaims(ReceiveDurabilityClaim);
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
                _initializedSent = true;
                _selfInitializeVersion = restorePoint;
                // Each fresh handshake starts with no clean reconnect; the response re-sets it
                // only if the peer accepts one. A stale true left from an earlier migration
                // would make the read operators synthesize a second init after an ordinary
                // recovery, colliding with the peer's real init watermarks and fail-looping.
                _cleanReconnect = false;
            }

            return SendInitializeRequest(restorePoint);
        }

        private const string SupersededHandshake = "The initialize handshake belongs to a run of the stream that ended.";

        private async Task SendInitializeRequest(long restorePoint)
        {
            await SendInitializeRequest(restorePoint, allowEpochReseed: true);
        }

        private async Task SendInitializeRequest(long restorePoint, bool allowEpochReseed)
        {
            SubstreamInitializeResponse? response;
            long selfEpoch;
            CancellationToken operationAbort;
            lock (_initializeLock)
            {
                selfEpoch = _selfCheckpointEpoch;
                operationAbort = _operationAbort.Token;
            }

            try
            {
                // Retry multiple times to send the initialize request
                int tryCount = 0;
                long drainingSinceTick = -1;
                do
                {
                    lock (_initializeLock)
                    {
                        if (_selfCheckpointEpoch != selfEpoch || operationAbort.IsCancellationRequested)
                        {
                            // The stream failed while this handshake was pending, the loop belongs
                            // to the aborted generation. It must stop announcing its stale epoch,
                            // a late announcement would regress what the restarted generation's
                            // own handshake already recorded at the peer, and every ack the peer
                            // sends afterwards would be dropped as stale. The operator's initialize
                            // must not continue either, its state manager is gone.
                            throw new OperationCanceledException(SupersededHandshake);
                        }
                    }
                    var wave = _waves.Current;
                    _logger.LogInformation("Sending initialize request to substream {substreamName} with restore point {restorePoint} in wave {wave}, try {tryCount}", substreamName, restorePoint, wave, tryCount);
                    response = await _substreamCommunicationHandler.SendInitializeRequest(restorePoint, selfEpoch, _announceCleanHandoff, wave, operationAbort).WaitAsync(operationAbort);
                    if (!response.NotStarted)
                    {
                        if (_announceCleanHandoff && response.CleanReconnect)
                        {
                            // Back into a running group, its recovery is this stream's now.
                            _waves.Adopt(response.Wave);
                            _durability?.EnterWave(response.Wave);
                        }
                        else if (response.Wave > wave && response.PeerInInit && _waves.TryEnter(response.Wave))
                        {
                            // The peer starts in a recovery above this one's, this stream joins it: nothing has crossed the
                            // pair in that recovery yet, so the peer need not restart again for this stream. Entered
                            // through another point already, the handshake stands in that wave.
                            _logger.LogInformation("Substream {substreamName} starts in wave {peerWave}, this stream restarts into it.", substreamName, response.Wave);
                            await DoFailAndRecover(null);
                            return;
                        }
                        else if (response.Wave > _waves.Current || ((wave == RecoveryWave.None || _waves.RestartUnseen) && !response.PeerInInit))
                        {
                            // The peer runs in a recovery this stream never saw, or this is a fresh stream object whose earlier
                            // runs the peers cannot tell from its wave: its restart is not noticed there. A wave above the
                            // peer's makes it: announced again, the peer restarts into it. A refused clean handoff is one
                            // such restart, the announcement is not repeated.
                            // A stream object started again after a stop in the peer's wave is the same case, its peers stopped reading from it.
                            var minted = _waves.MintAbove(response.Wave);
                            _logger.LogInformation("Substream {substreamName} is in wave {peerWave}, this stream restarts the group in wave {wave}.", substreamName, response.Wave, minted);
                            _announceCleanHandoff = false;
                            if (_waves.Points.Any(p => !ReferenceEquals(p, this) && p.IsWired))
                            {
                                // Another point may have handshook in the old wave, the teardown tells every peer.
                                await DoFailAndRecover(null);
                                return;
                            }
                            _durability?.EnterWave(minted);
                            await SendInitializeRequest(restorePoint, allowEpochReseed);
                            return;
                        }
                    }
                    if (response.NotStarted && response.PeerDraining)
                    {
                        // Peer answers draining, budget not spent, wall clock bounds it.
                        if (drainingSinceTick < 0)
                        {
                            drainingSinceTick = Environment.TickCount64;
                            _logger.LogInformation("Substream {substreamName} is stopping, waiting for it to finish before initializing", substreamName);
                        }
                        else if (TimeSpan.FromMilliseconds(Environment.TickCount64 - drainingSinceTick) > PeerDrainingWaitLimit)
                        {
                            throw new InvalidOperationException($"Substream {substreamName} has been draining for over {PeerDrainingWaitLimit}, giving up on the initialize request.");
                        }
                        else
                        {
                            // Once at information, the wait can run minutes.
                            _logger.LogDebug("Substream {substreamName} is still stopping, waiting", substreamName);
                        }
                        await Task.Delay(NotStartedRetrySliceMs * 4, operationAbort);
                        continue;
                    }
                    drainingSinceTick = -1;
                    tryCount++;

                    // Bounded above a slow start budget (~60s), slices stay short.
                    if (tryCount > 32)
                    {
                        throw new InvalidOperationException($"Failed to initialize substream {substreamName} after {tryCount} tries.");
                    }
                    if (response.NotStarted)
                    {
                        // Kept short, this backoff cannot observe a stop.
                        var delay = Math.Min(NotStartedRetrySliceMs * tryCount, NotStartedRetrySliceMs * 4);
                        _logger.LogInformation("Substream {substreamName} not started yet, retrying in {delay} ms", substreamName, delay);
                        await Task.Delay(delay, operationAbort);
                    }
                } while (response.NotStarted);
                // The peer has decided on the announcement, a later generation must not repeat it.
                _announceCleanHandoff = false;
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

            if (response.CleanReconnect)
            {
                // The peer kept running and accepted the clean handoff reconnect, so no init
                // watermarks event from a peer restart will come. Read operators complete
                // their startup from restored state instead, see SubstreamReadOperator.
                // Applied before any epoch reseed re-run below: that re-run announces
                // cleanHandoff=false and returns, so it would never re-report CleanReconnect,
                // and the resumed peer never restarts - losing it here hangs startup.
                lock (_initializeLock)
                {
                    if (_selfCheckpointEpoch == selfEpoch)
                    {
                        _cleanReconnect = true;
                    }
                }
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
                    if (_selfCheckpointEpoch != selfEpoch || operationAbort.IsCancellationRequested)
                    {
                        // The stream failed while the response was in flight, the restarted
                        // generation runs its own handshake.
                        throw new OperationCanceledException(SupersededHandshake);
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
                if (_selfCheckpointEpoch != selfEpoch || operationAbort.IsCancellationRequested)
                {
                    // The stream failed while the response was in flight, it belongs to the
                    // aborted generation. Applying it could overwrite the peer epoch that the
                    // restarted generation's own handshake already recorded.
                    throw new OperationCanceledException(SupersededHandshake);
                }
                // Highest wins: a response delayed across the peer's failure must not regress
                // what a newer handshake already recorded. Peer generations draw from a clock
                // seed, so the current generation's epoch is always the highest.
                RecordPeerEpoch_NoLock(Math.Max(_peerCheckpointEpoch, response.CheckpointEpoch));
            }
            if (response.Success)
            {
                // The peer listens now, it may have missed what was claimed before.
                _durability?.ResendTo(this);
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
        /// The local stream's run ended in a stop: the next start handshakes again with a new epoch, so claims and acks of
        /// the run that ended are told apart from the next run's like after a failure.
        /// </summary>
        public void OnStreamStopped()
        {
            _waves.Stopped();
            lock (_initializeLock)
            {
                if (!_initializedSent)
                {
                    // A failure ended the run already.
                    return;
                }
                _initializedSent = false;
                _selfCheckpointEpoch = Interlocked.Increment(ref _checkpointEpochSeed);
                _durability?.Invalidate();
            }
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
                // New generation: acks tagged with the old epoch are now stale and get dropped.
                // Drawn from the shared seed so it never collides with any other generation.
                _selfCheckpointEpoch = Interlocked.Increment(ref _checkpointEpochSeed);
                // Same lock as the claim fence: nothing fenced or snapshotted before this is used after it.
                _durability?.Invalidate();
                // The peer rolls back with this stream, its committed versions restart;
                // re-baselined at the next handshake. Reset with the epoch bump so an ack
                // that passed the fence in the old epoch cannot advance it afterwards.
                Interlocked.Exchange(ref _peerLastCommittedVersion, -1);
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

        private Task<SubstreamInitializeResponse> OnTargetSubstreamInitialize(long restorePoint, long peerCheckpointEpoch, bool cleanHandoff, RecoveryWave wave)
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
                RecordPeerEpoch_NoLock(Math.Max(_peerCheckpointEpoch, peerCheckpointEpoch));
                recordedPeerEpoch = _peerCheckpointEpoch;
                selfEpoch = _selfCheckpointEpoch;
            }
            // Sent on every request, the requester re-sends its own once the response arrives.
            _durability?.ResendTo(this);
            if (cleanHandoff)
            {
                var handoffResult = TryAcceptCleanHandoff(restorePoint);
                if (handoffResult == CleanHandoffResult.Accepted)
                {
                    // The peer resumes from this version, its acked commits continue from here.
                    Interlocked.Exchange(ref _peerLastCommittedVersion, restorePoint);
                    return Task.FromResult(new SubstreamInitializeResponse(false, true, restorePoint, selfEpoch, recordedPeerEpoch, cleanReconnect: true, wave: _waves.Current, peerInInit: _waves.InInit));
                }
                if (handoffResult == CleanHandoffResult.RetryLater)
                {
                    // Transient, not started makes the peer retry, draining spares budget.
                    return Task.FromResult(new SubstreamInitializeResponse(true, false, restorePoint, selfEpoch, recordedPeerEpoch, peerDraining: true, wave: _waves.Current, peerInInit: _waves.InInit));
                }
                // Rejected: the normal handshake reconciles. A running peer that refused makes the mover restart the group.
            }
            if (_waves.TryEnter(wave))
            {
                // The peer restarts in a recovery this stream is not in: everything exchanged with it since the last
                // checkpoint is void, this stream restarts into the same wave. The peer retries once that is done.
                _logger.LogInformation("Substream {substreamName} initializes in wave {wave}, restarting into it.", substreamName, wave);
                _ = Task.Run(async () =>
                {
                    try
                    {
                        await DoFailAndRecover(null);
                    }
                    catch (Exception e)
                    {
                        _logger.LogWarning(e, "Restarting into the wave of substream {substreamName} failed, its handshake retry runs it again.", substreamName);
                    }
                });
                return Task.FromResult(new SubstreamInitializeResponse(true, false, restorePoint, selfEpoch, recordedPeerEpoch, peerDraining: true, wave: wave, peerInInit: true));
            }
            // The peer (re)starts here, its committed versions count up from this point.
            Interlocked.Exchange(ref _peerLastCommittedVersion, restorePoint);
            return Task.FromResult(new SubstreamInitializeResponse(false, true, restorePoint, selfEpoch, recordedPeerEpoch, wave: _waves.Current, peerInInit: _waves.InInit));
        }

        private enum CleanHandoffResult
        {
            Accepted,
            RetryLater,
            Rejected,
        }

        /// <summary>
        /// Decides a clean handoff reconnect. Accepted when the peer announces a restore point
        /// at or past its last acked commit and every reader has consumed its stop barrier, so
        /// no rollback is needed on either side; the readers then resubscribe on a fresh data
        /// epoch. A restore point below the last acked commit is rejected (fail over). A stop
        /// barrier not consumed here yet is transient and answers RetryLater - the consumption
        /// commit can only complete with this peer's acks, so waiting for it would deadlock.
        /// Rejected too when no data was exchanged, the normal handshake reconciles instead.
        /// </summary>
        private CleanHandoffResult TryAcceptCleanHandoff(long restorePoint)
        {
            if (_waves.InInit)
            {
                // Nothing to resume into: this stream starts itself and sends its init events, and its start waits for the
                // mover's claim, so the mover must not wait here. It starts with this stream through the normal handshake.
                return CleanHandoffResult.Rejected;
            }
            List<SubstreamReadOperator> readOperators;
            lock (_readOperators)
            {
                readOperators = new List<SubstreamReadOperator>(_readOperators);
            }

            bool dataWasHandled;
            lock (_dataHandledLock)
            {
                dataWasHandled = _dataHandled;
            }
            if (!dataWasHandled)
            {
                return CleanHandoffResult.Rejected;
            }
            long peerLastCommitted = Interlocked.Read(ref _peerLastCommittedVersion);
            if (peerLastCommitted < 0 || restorePoint < peerLastCommitted)
            {
                _logger.LogWarning(
                    "Substream {substreamName} announced a clean handoff at restore point {restorePoint}, but its last acked commit here is {peerLastCommitted}, its restored state is not the one its stop drain persisted, falling back to fail over.",
                    substreamName, restorePoint, peerLastCommitted);
                return CleanHandoffResult.Rejected;
            }
            // Stopping, answer retry, never resume into the drain.
            bool stopping = readOperators.Any(r => r.IsStopping) || _targetInfos.Values.Any(t => t.Target.StopBarrierStored);
            if (stopping)
            {
                // Debug, the peer asks again every few slices.
                _logger.LogDebug(
                    "Substream {substreamName} announced a clean handoff at restore point {restorePoint} while this stream is stopping, answering retry.",
                    substreamName, restorePoint);
                return CleanHandoffResult.RetryLater;
            }
            foreach (var readOperator in readOperators)
            {
                if (!readOperator.HasCleanPeerStop)
                {
                    _logger.LogInformation(
                        "Substream {substreamName} announced a clean handoff at restore point {restorePoint}, but its stop barrier has not been consumed here yet (target {targetId}), answering retry.",
                        substreamName, restorePoint, readOperator.ExchangeTargetId);
                    return CleanHandoffResult.RetryLater;
                }
            }

            // Readers resume all or none, a stop refuses under locks.
            if (!SubstreamReadOperator.TryResumeAllAfterPeerReconnect(readOperators))
            {
                _logger.LogInformation(
                    "Substream {substreamName} announced a clean handoff at restore point {restorePoint} but this stream began stopping, answering retry.",
                    substreamName, restorePoint);
                return CleanHandoffResult.RetryLater;
            }
            _logger.LogInformation(
                "Substream {substreamName} reconnected from a clean handoff at restore point {restorePoint}, resuming without a rollback.",
                substreamName, restorePoint);
            lock (_dataHandledLock)
            {
                // Fresh data epoch: a later real restart of the peer fails over on what flows from here.
                _dataHandled = false;
            }
            return CleanHandoffResult.Accepted;
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

        /// <summary>
        /// A peer fails in the wave. This stream restarts into it at its own last version, once per wave, the versions are
        /// compared at the gate of the start. An older wave is a message from a recovery that is over.
        /// </summary>
        private Task OnPeerRecovering(RecoveryWave wave)
        {
            if (!_waves.TryEnter(wave))
            {
                _logger.LogDebug("Substream {substreamName} recovers in wave {wave}, this stream is in {ownWave} already.", substreamName, wave, _waves.Current);
                return Task.CompletedTask;
            }
            _logger.LogInformation("Substream {substreamName} recovers in wave {wave}, restarting into it.", substreamName, wave);
            return DoFailAndRecover(null);
        }

        private async Task<bool> DoFailAndRecover(long? recoveryPoint)
        {
            if (await TryFailAndRecoverWired(recoveryPoint))
            {
                return true;
            }
            foreach (var point in _waves.Points)
            {
                // Any wired operator of the substream carries it.
                if (!ReferenceEquals(point, this) && await point.TryFailAndRecoverWired(recoveryPoint))
                {
                    return true;
                }
            }
            // Nothing wired yet: the start under way has fetched nothing, it continues in the wave. Its claims
            // and a come-down belong to that wave, like after the reset a restart would do.
            _durability?.EnterWave(_waves.ForStart());
            _logger.LogInformation("Received fail and recover to {recoveryPoint} before any exchange operator is initialized, the start continues in wave {wave}.", recoveryPoint, _waves.Current);
            return false;
        }

        /// <summary>
        /// True when a target or read operator of this point is wired; a stale wiring is dropped by the stream's generation fence.
        /// </summary>
        internal bool IsWired
        {
            get
            {
                if (_targetInfos.Values.Any(t => t.Target.CanFailAndRecover))
                {
                    return true;
                }
                lock (_readOperators)
                {
                    return _readOperators.Any(r => r.CanFailAndRecover);
                }
            }
        }

        private async Task<bool> TryFailAndRecoverWired(long? recoveryPoint)
        {
            // One rollback fails the whole stream over, any single wired operator carries
            // it. Both targets and read operators register at construction but are wired per
            // run (targets by their exchange's initialize), so unwired ones must be skipped - an
            // arbitrary pick could land on one, and for a read operator the dispatch throws
            // into a fire and forget caller and the rollback is silently lost, wedging the
            // stream against the peer.
            foreach (var targetInfo in _targetInfos.Values)
            {
                if (targetInfo.Target.CanFailAndRecover)
                {
                    await targetInfo.Target.FailAndRecover(recoveryPoint);
                    return true;
                }
            }
            SubstreamReadOperator? readOperator;
            lock (_readOperators)
            {
                readOperator = _readOperators.FirstOrDefault(r => r.CanFailAndRecover);
            }
            if (readOperator == null)
            {
                return false;
            }
            await readOperator.FailAndRecover(recoveryPoint);
            return true;
        }

        public Task SendFailAndRecover(RecoveryWave wave)
        {
            return _substreamCommunicationHandler.SendFailAndRecover(wave);
        }

        private RecoveryWave? _notifyFailInFlightWave;
        private readonly object _notifyFailLock = new object();

        /// <summary>
        /// Tells the other substream that this one fails, in the wave the failure belongs to, without waiting for the
        /// result: it may be unreachable and waiting out its response timeout would stall the recovery here. The version
        /// is not sent, the versions are compared when the group starts. Concurrent notifications of one wave are one.
        /// </summary>
        public void NotifyFailAndRecover(long recoveryPoint)
        {
            var wave = _waves.ForFailure();
            lock (_notifyFailLock)
            {
                if (_notifyFailInFlightWave == wave)
                {
                    return;
                }
                _notifyFailInFlightWave = wave;
            }
            _ = Task.Run(async () =>
            {
                try
                {
                    await _substreamCommunicationHandler.SendFailAndRecover(wave);
                }
                catch (Exception e)
                {
                    _logger.LogWarning(e, "Failed to notify substream {substreamName} about the failure, versions are reconciled at the next initialize handshake.", substreamName);
                }
                finally
                {
                    lock (_notifyFailLock)
                    {
                        if (_notifyFailInFlightWave == wave)
                        {
                            _notifyFailInFlightWave = null;
                        }
                    }
                }
            });
        }

        private Task RecieveCheckpointDone(long checkpointVersion, long checkpointEpoch, bool coversPeerStopBarrier)
        {
            lock (_initializeLock)
            {
                if (checkpointEpoch != _selfCheckpointEpoch)
                {
                    // Stale ack from a previous epoch, for example one in flight across a restart.
                    // Crediting it would complete the current cycle without the peer acking it, so
                    // it is dropped. The next handshake re-announces the current epoch.
                    _logger.LogWarning("Dropping stale checkpoint done from substream {substreamName}: epoch {ackEpoch} does not match current {selfEpoch}.", substreamName, checkpointEpoch, _selfCheckpointEpoch);
                    return Task.CompletedTask;
                }
                // Track the highest acked version. Advanced under the same lock as the fence
                // and the failure reset, a stale advance can then never overwrite the -1 a
                // failure just wrote. Still a CAS loop, the handshake re-baselines outside
                // this lock.
                long recordedCommitted;
                do
                {
                    recordedCommitted = Interlocked.Read(ref _peerLastCommittedVersion);
                } while (checkpointVersion > recordedCommitted &&
                         Interlocked.CompareExchange(ref _peerLastCommittedVersion, checkpointVersion, recordedCommitted) != recordedCommitted);
            }
            _logger.LogDebug("Recieved checkpoint done from substream {substreamName} to {selfSubstreamName} with version {checkpointVersion}, notifying targets and read operators.", substreamName, _selfSubstreamName, checkpointVersion);
            // Tell all targets and read operators the connected substream completed the checkpoint.
            // Task.Run to use the thread pool, this can be called from a grain turn where the grain
            // activation scheduler would otherwise be captured.
            return Task.Run(async () =>
            {
                var dispatchHook = ReceiveCheckpointDoneDispatchHookForTests;
                if (dispatchHook != null)
                {
                    await dispatchHook(_selfSubstreamName);
                }
                lock (_initializeLock)
                {
                    if (checkpointEpoch != _selfCheckpointEpoch)
                    {
                        // A failure landed between the arrival fence and this dispatch: the
                        // ledgers were reset for the new generation and this ack belongs to
                        // the aborted one. A credit would let a later cycle complete without
                        // a real acknowledgement.
                        _logger.LogWarning("Dropping checkpoint done from substream {substreamName}: a failure superseded epoch {ackEpoch} before the crediting ran.", substreamName, checkpointEpoch);
                        return;
                    }
                }
                try
                {
                    foreach (var target in _targetInfos)
                    {
                        await target.Value.Target.TargetSubstreamCheckpointDone(checkpointVersion, coversPeerStopBarrier);
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

        /// <summary>
        /// A peer on a new epoch restarted, what it claimed before may no longer hold.
        /// </summary>
        private void RecordPeerEpoch_NoLock(long epoch)
        {
            Debug.Assert(Monitor.IsEntered(_initializeLock));
            if (epoch != _peerCheckpointEpoch)
            {
                _peerCheckpointEpoch = epoch;
                _durability?.PeerEpochChanged(substreamName);
            }
        }

        /// <param name="generation">The coordinator generation the claim was read under.</param>
        internal Task SendDurabilityClaim(SubstreamDurabilityClaim claim, RecoveryWave wave, long generation, bool requestReply, CancellationToken cancellationToken = default)
        {
            long selfEpoch;
            long targetEpoch;
            lock (_initializeLock)
            {
                // A claim read before a failure must not leave stamped with the epochs of the run after it.
                if (_durability == null || _durability.Generation != generation)
                {
                    return Task.CompletedTask;
                }
                selfEpoch = _selfCheckpointEpoch;
                targetEpoch = _peerCheckpointEpoch;
            }
            return SendDurabilityClaimCore(claim, wave, selfEpoch, targetEpoch, requestReply, cancellationToken);
        }

        private async Task SendDurabilityClaimCore(SubstreamDurabilityClaim claim, RecoveryWave wave, long selfEpoch, long targetEpoch, bool requestReply, CancellationToken cancellationToken)
        {
            try
            {
                // Await actual transport settlement; abandoning a wait does not retire a send.
                await _substreamCommunicationHandler.SendDurabilityClaim(claim.Version, claim.Radius, claim.InitVersion, wave, selfEpoch, targetEpoch, requestReply, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                // Sent again on a timer while anything waits for the agreement.
                _logger.LogDebug(e, "Sending a durability claim to substream {substreamName} failed.", substreamName);
            }
        }

        private Task ReceiveDurabilityClaim(long version, int radius, long initVersion, RecoveryWave wave, long senderCheckpointEpoch, long targetCheckpointEpoch, bool requestReply)
        {
            if (_durability == null)
            {
                return Task.CompletedTask;
            }
            long fencedGeneration;
            lock (_initializeLock)
            {
                // Both ends: a claim from before a restart of either side must not count, the
                // version number is reused after a rollback.
                if (targetCheckpointEpoch != _selfCheckpointEpoch || senderCheckpointEpoch < _peerCheckpointEpoch)
                {
                    _logger.LogDebug("Dropping a durability claim from substream {substreamName}, its epochs {senderEpoch}/{targetEpoch} do not match {peerEpoch}/{selfEpoch}.", substreamName, senderCheckpointEpoch, targetCheckpointEpoch, _peerCheckpointEpoch, _selfCheckpointEpoch);
                    return Task.CompletedTask;
                }
                // It knows this generation, so it was sent after a handshake with it. A higher
                // sender epoch is that handshake's answer still on its way, forgets the old claims.
                RecordPeerEpoch_NoLock(senderCheckpointEpoch);
                // The write checks it again, a failure or a start after this point drops the claim.
                fencedGeneration = _durability.Generation;
            }
            _durability.PeerClaim(substreamName, new SubstreamDurabilityClaim(radius, version, initVersion), wave, fencedGeneration, requestReply);
            return Task.CompletedTask;
        }

        // Called before the engine waits for callback ownership to drain. Only local
        // waits on fenced control messages are aborted; no storage task is abandoned.
        internal void AbortPendingOperations() => Volatile.Read(ref _operationAbort).Cancel();

        // Once per start, before any handshake; never disposed, a racing abort may still hold it.
        internal void ResetPendingOperations()
        {
            var abort = Volatile.Read(ref _operationAbort);
            if (abort.IsCancellationRequested)
            {
                Interlocked.CompareExchange(ref _operationAbort, new CancellationTokenSource(), abort);
            }
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
            // Whether this committed checkpoint covers consuming the peer's stop barriers,
            // used by a stopping peer to confirm its drain. Every read operator's OnCheckpoint
            // ran before any CheckpointDone fires, so the flags are final for this version.
            // Vacuously true without read operators: the peer only checks the flag on targets,
            // and a target here is always paired with a read operator on the peer.
            bool coversPeerStopBarrier;
            lock (_readOperators)
            {
                coversPeerStopBarrier = _readOperators.All(r => r.PeerStopConsumedCommitted);
            }
            _logger.LogDebug("Sending checkpoint done to target: {substreamName} from {selfSubstreamName}", substreamName, _selfSubstreamName);
            return WaitForNotification(_substreamCommunicationHandler.SendCheckpointDone(checkpointVersion, targetEpoch, coversPeerStopBarrier), _operationAbort.Token);
        }

        private static async Task WaitForNotification(Task operation, CancellationToken cancellationToken)
        {
            try { await operation.WaitAsync(cancellationToken).ConfigureAwait(false); }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                // Observe a later transport fault without holding the stopped run open.
                _ = operation.ContinueWith(t => _ = t.Exception, CancellationToken.None,
                    TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
            }
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
                // A fresh subscription starts fetching, whatever the old one held.
                _pausedTargets.Remove(exchangeTarget);
                _subscribeTargetsVersion++;
            }
            TryStartFetchTask();
        }

        /// <summary>
        /// True until Unsubscribe, a fetch paused by PauseFetch still counts.
        /// </summary>
        public bool IsSubscribed(int exchangeTarget)
        {
            lock (_fetchDataLock)
            {
                return _subscribedTargets.ContainsKey(exchangeTarget);
            }
        }

        public void Unsubscribe(int exchangeTarget)
        {
            lock (_fetchDataLock)
            {
                _subscribedTargets.Remove(exchangeTarget);
                _pausedTargets.Remove(exchangeTarget);
                _subscribeTargetsVersion++;
            }
        }

        /// <summary>
        /// Holds a target's fetch without unsubscribing, the dequeue is destructive.
        /// </summary>
        public void PauseFetch(int exchangeTarget)
        {
            lock (_fetchDataLock)
            {
                if (_subscribedTargets.ContainsKey(exchangeTarget) && _pausedTargets.Add(exchangeTarget))
                {
                    _subscribeTargetsVersion++;
                }
            }
        }

        public void ResumeFetch(int exchangeTarget)
        {
            lock (_fetchDataLock)
            {
                if (_pausedTargets.Remove(exchangeTarget))
                {
                    _subscribeTargetsVersion++;
                }
            }
        }

        // Last time the fetch loop completed an iteration, used by the stall watchdog. The
        // loop runs continuously with short delays while any subscription exists, so a long
        // gap means the loop is blocked, for example delivering an event into a pipeline that
        // deadlocked on checkpoint barrier alignment with another substream.
        private long _lastFetchLoopTick;
        // Tick since all paused, StallLimit cannot catch a holding loop.
        private long _allPausedSince = -1;
        private Timer? _stallWatchdog;
        // Internal so tests can shorten them, a stall test would otherwise take over a
        // minute.
        internal static TimeSpan StallLimit = TimeSpan.FromSeconds(60);
        internal static TimeSpan StallCheckInterval = TimeSpan.FromSeconds(15);
        // Above StallLimit and the pairing budget, legitimate holds run long.
        internal static TimeSpan PausedStallLimit = TimeSpan.FromSeconds(180);

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
            string reason;
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
                bool loopMovedRecently = TimeSpan.FromMilliseconds(Environment.TickCount64 - _lastFetchLoopTick) < StallLimit;
                var pausedSince = Volatile.Read(ref _allPausedSince);
                bool pausedTooLong = pausedSince >= 0 && TimeSpan.FromMilliseconds(Environment.TickCount64 - pausedSince) >= PausedStallLimit;
                if (loopMovedRecently && !pausedTooLong)
                {
                    return;
                }
                // Names the limit that fired, the two hangs differ.
                reason = loopMovedRecently ? $"held at a barrier for over {PausedStallLimit}" : $"stalled for over {StallLimit}";
                // Reset so the watchdog does not fire again while the recovery runs.
                _lastFetchLoopTick = Environment.TickCount64;
                Volatile.Write(ref _allPausedSince, -1);
            }
            _logger.LogWarning("The fetch loop for substream {substreamName} has been {reason}, failing and recovering to break a possible deadlock between the substreams.", substreamName, reason);
            _ = Task.Run(async () =>
            {
                SubstreamReadOperator? readOperator;
                lock (_readOperators)
                {
                    // Only a wired operator can carry the recovery; an unwired one means the
                    // stream is restarting, which is itself the recovery the stall needs.
                    readOperator = _readOperators.FirstOrDefault(r => r.CanFailAndRecover);
                }
                if (readOperator != null)
                {
                    try
                    {
                        await readOperator.FailAndRecoverOnFetchError(new TimeoutException($"The fetch loop was {reason}."));
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
                // Only a wired operator can carry the recovery; an unwired one means the
                // stream is restarting, which is itself the recovery the failed loop needs.
                readOperator = _readOperators.FirstOrDefault(r => r.CanFailAndRecover);
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
            // Consecutive empty fetches double the poll delay up to the cap, any data resets
            // it. In Orleans every poll is a grain call, an idle stream with many substream
            // pairs would otherwise generate thousands of calls per second doing nothing. The
            // cap stays low enough that a stop drain's barriers still propagate well within
            // the drain timeout.
            const int emptyPollDelayMs = 10;
            const int emptyPollDelayCapMs = 200;
            int pollDelayMs = emptyPollDelayMs;
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
                            if (!_pausedTargets.Contains(kvp.Key))
                            {
                                targetIds.Add(kvp.Key);
                            }
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

                if (targetIds.Count == 0)
                {
                    // All targets paused, wait for a resume, watchdog bounds it.
                    if (Volatile.Read(ref _allPausedSince) < 0)
                    {
                        Volatile.Write(ref _allPausedSince, Environment.TickCount64);
                    }
                    await Task.Delay(emptyPollDelayMs);
                    continue;
                }
                // A target is fetched, the hold is over.
                Volatile.Write(ref _allPausedSince, -1);

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
                    pollDelayMs = emptyPollDelayMs;
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
                    await Task.Delay(pollDelayMs);
                    pollDelayMs = Math.Min(pollDelayMs * 2, emptyPollDelayCapMs);
                }
            }
        }
    }
}
