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
using FlowtideDotNet.Base.Vertices;
using FlowtideDotNet.Core.Utils;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Substrait.Relations;
using Microsoft.Extensions.Logging;
using System.Diagnostics;
using System.Threading.Channels;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Core.Operators.Exchange
{
    internal class SubstreamReadState
    {
        public HashSet<string>? WatermarkNames { get; set; }
    }

    /// <summary>
    /// Ingress operator that reads events from an exchange in another substream. Events are fetched
    /// through the communication point and buffered in a transient channel; after a failure both
    /// substreams roll back to a common checkpoint and the other substream replays the events.
    /// </summary>
    internal class SubstreamReadOperator : IngressVertex<StreamEventBatch>
    {
        /// <summary>
        /// Placed in the channel when this stream takes its stop checkpoint, so the fetch loop
        /// forwards the stop barrier after buffered events without waiting on the other substream.
        /// </summary>
        private sealed class LocalStopCheckpointMarker : IStreamEvent
        {
        }

        private static readonly LocalStopCheckpointMarker s_localStopCheckpointMarker = new LocalStopCheckpointMarker();

        // Internal so tests can shorten it, forcing the pairing budget to expire would
        // otherwise take minutes.
        internal static TimeSpan PairingAttemptDelay = TimeSpan.FromSeconds(5);

        // Test hook: stream, peer version, local version; versions must match.
        internal static Action<string, long, long>? PairedCheckpointHookForTests;
        // Test hook: stream, reader; fires under the reader locks.
        internal static Action<string, SubstreamReadOperator>? ResumedHookForTests;
        // Test hook: stream, peer version; simulates a drifted peer.
        internal static Func<string, long, long>? PeerBarrierVersionForTests;


        private readonly SubstreamCommunicationPoint _communicationPoint;
        private readonly SubstreamExchangeReferenceRelation _exchangeReferenceRelation;
        private readonly object _lock = new object();
        private ICheckpointEvent? _currentCheckpoint;
        private TaskCompletionSource? _waitForCheckpoint;
        private Task? _fetchTask;
        private bool _initWatermarksHandled;
        // Checkpoint done signals from the other substream that arrived before this stream
        // finished starting, replayed one per checkpoint cycle. Guarded by _lock.
        private int _pendingCheckpointDoneSignals;
        private Channel<IStreamEvent>? _channel;
        private IObjectState<SubstreamReadState>? _state;
        // Stop barrier paired with a peer barrier, peer drained.
        private volatile bool _peerStopConsumed;
        private volatile bool _peerStopConsumedCommitted;
        // Set by the first stop barrier, only restore clears it.
        private volatile bool _stopping;
        // A returning peer's restarted pipeline sends one init watermarks event that must be
        // consumed without forwarding, a second init downstream would skew barrier alignment.
        private volatile bool _swallowNextInitWatermarks;
        private bool _restoredWithWatermarkNames;
        // True after the first local checkpoint barrier since the last restore, before it an
        // unpairable barrier means the stream is still starting up, not an epoch mismatch.
        private volatile bool _localCheckpointSeen;
        // Cancels the stop deadline when the epoch ends.
        private CancellationTokenSource? _stopAlignmentCancel;
        // True while events forwarded after the last checkpoint barrier await a covering
        // cycle. Only touched from the single threaded fetch loop. The first uncovered event
        // schedules exactly one cycle; scheduling on every event would make the substreams
        // checkpoint forever, each cycle's barriers re-trigger cycles at the peers.
        private bool _uncoveredForwards;

        public SubstreamReadOperator(SubstreamCommunicationPoint communicationPoint, SubstreamExchangeReferenceRelation referenceRelation, DataflowBlockOptions options) : base(options)
        {
            this._communicationPoint = communicationPoint;
            _exchangeReferenceRelation = referenceRelation;
            _communicationPoint.RegisterReadOperator(this);
        }

        public override string DisplayName => "Substream Read";

        public int ExchangeTargetId => _exchangeReferenceRelation.ExchangeTargetId;

        /// <summary>
        /// True when this operator can carry a fail and recover into the stream. The operator
        /// registers with the communication point at construction, but the stream failure
        /// path is wired first at its initialize; dispatching before that throws into a fire
        /// and forget caller and the rollback is silently lost.
        /// </summary>
        public bool CanFailAndRecover => CanFailAndRollback;

        /// <summary>
        /// Allocator that received events are deserialized with, so fetched data is accounted on the
        /// operator that consumes it. Only valid after initialization (events are only fetched for
        /// subscribed targets).
        /// </summary>
        internal IMemoryAllocator ReceiveMemoryAllocator => MemoryAllocator;

        /// <summary>
        /// True once the stop barrier paired and that cycle committed.
        /// </summary>
        public override bool ReadyToStop => _peerStopConsumedCommitted;

        public override Task Compact()
        {
            return Task.CompletedTask;
        }

        public override Task DeleteAsync()
        {
            return Task.CompletedTask;
        }

        public override Task OnTrigger(string triggerName, object? state)
        {
            return Task.CompletedTask;
        }

        protected override Task<IReadOnlySet<string>> GetWatermarkNames()
        {
            Debug.Assert(_state?.Value != null);
            Debug.Assert(_state.Value.WatermarkNames != null);
            return Task.FromResult<IReadOnlySet<string>>(_state.Value.WatermarkNames);
        }

        protected override async Task InitializeOrRestore(long restoreTime, IStateManagerClient stateManagerClient)
        {
            await _communicationPoint.InitializeOperator(restoreTime);

            TaskCompletionSource? staleWaitForCheckpoint;
            lock (_lock)
            {
                // Clear checkpoint state from a run that was interrupted by a failure,
                // otherwise the first checkpoint after a restore could complete a stale wait.
                _currentCheckpoint = null;
                staleWaitForCheckpoint = _waitForCheckpoint;
                _waitForCheckpoint = null;
                _initWatermarksHandled = false;
                _peerStopConsumed = false;
                _peerStopConsumedCommitted = false;
                _stopping = false;
                _swallowNextInitWatermarks = false;
                _localCheckpointSeen = false;
                _uncoveredForwards = false;
                // Signals from before the restore belong to the aborted epoch, replaying
                // them would complete a new cycle too early.
                _pendingCheckpointDoneSignals = 0;
            }
            // Cancel outside the lock so a stale fetch loop that awaits it can complete and stop.
            staleWaitForCheckpoint?.TrySetCanceled();

            // A previous epochs stop deadline must not fire here.
            var staleStopAlignmentCancel = _stopAlignmentCancel;
            if (staleStopAlignmentCancel != null)
            {
                staleStopAlignmentCancel.Cancel();
                staleStopAlignmentCancel.Dispose();
            }
            _stopAlignmentCancel = new CancellationTokenSource();

            // A fresh channel on every restore, buffered events belong to the aborted epoch
            // and are regenerated by replay.
            var staleChannel = _channel;
            if (staleChannel != null)
            {
                staleChannel.Writer.TryComplete();
                while (staleChannel.Reader.TryRead(out var staleEvent))
                {
                    SubstreamCommunicationPoint.DisposeEvent(staleEvent);
                }
            }
            _channel = Channel.CreateBounded<IStreamEvent>(new BoundedChannelOptions(1024)
            {
                SingleReader = true
            });

            _state = await stateManagerClient.GetOrCreateObjectStateAsync<SubstreamReadState>("substream_read_state");
            if (_state.Value == null)
            {
                _state.Value = new SubstreamReadState();
            }
            // Captured here: the peer's init watermarks event arrives during startup and
            // populates the state before InitializationCompleted runs, only a restore has
            // the names at this point.
            _restoredWithWatermarkNames = _state.Value.WatermarkNames != null;
        }

        protected override async Task OnCheckpoint(long checkpointTime)
        {
            Debug.Assert(_state != null);
            await _state.Commit();
            // Under _lock so this read-and-set is atomic against the resets in
            // InitializeOrRestore and ResumeAfterPeerReconnect: a returning peer's reconnect
            // resets these on a grain turn while this runs on the fetch loop, and a lost
            // reset would leave a stale committed flag that stamps an outgoing ack as covering
            // a stop barrier this stream has not actually consumed.
            lock (_lock)
            {
                _peerStopConsumedCommitted = _peerStopConsumed;
            }
        }

        /// <summary>
        /// On a fresh start this operator's initial data is the other substream's initial
        /// data, which arrives asynchronously through the fetch loop; the marker is forwarded
        /// from there once the peer sends it after everything before it. Reporting done at
        /// startup would release downstream watermark alignment before the peer's data has
        /// arrived, flushing partial results. After a restore the initial data is already part
        /// of the restored downstream state, and a clean reconnect peer never resends its
        /// marker, so done is reported immediately.
        /// </summary>
        protected override bool SendInitialDataDoneAfterInitial => _restoredWithWatermarkNames;

        protected override Task SendInitial(IngressOutput<StreamEventBatch> output)
        {
            return Task.CompletedTask;
        }

        private async Task FetchData(IngressOutput<StreamEventBatch> output, object? state)
        {
            Debug.Assert(_state?.Value != null);
            Debug.Assert(_channel != null);

            var channel = _channel;

            SubscribeToPeer(channel);

            if (_communicationPoint.CleanReconnect && !_initWatermarksHandled)
            {
                if (_state.Value.WatermarkNames == null)
                {
                    // The peer accepted the reconnect and never resends its init watermarks,
                    // but the restored state holds none to resume from - startup would wait
                    // forever. Unreachable while the peer's accept fence holds (a clean
                    // reconnect needs an acked commit, every commit follows the init), so
                    // fail over loudly and let the recovery reconcile the substreams.
                    Logger.LogWarning("Substream read {name} resumed from a clean handoff without restored watermark names, failing over to reconcile the substreams.", Name);
                    DispatchFailAndRollback();
                    return;
                }
                // The peer accepted the reconnect and will not restart, so no init watermarks
                // event comes from it; complete startup from the restored watermark names.
                lock (_lock)
                {
                    _initWatermarksHandled = true;
                }
                Logger.LogInformation("Substream read {name} resumes from a clean handoff, initializing watermarks from restored state.", Name);
                await output.SendLockingEvent(new InitWatermarksEvent(_state.Value.WatermarkNames));
                SetDependenciesDone();
            }

            while (!output.CancellationToken.IsCancellationRequested)
            {
                var ev = await channel.Reader.ReadAsync(output.CancellationToken);

                if (ev is LocalStopCheckpointMarker)
                {
                    ICheckpointEvent? stopCheckpoint;
                    lock (_lock)
                    {
                        stopCheckpoint = _currentCheckpoint;
                    }
                    if (stopCheckpoint == null)
                    {
                        // An event from the other substream already paired with the stop checkpoint
                        continue;
                    }
                    // Forwarded without a peer event, a crashed peer sends none.
                    Logger.LogDebug("Substream read {name} forwards the stop checkpoint", Name);
                    await OnCheckpoint(stopCheckpoint.CheckpointTime);
                    await output.SendLockingEvent(stopCheckpoint);
                    // Everything forwarded before this barrier is covered by it.
                    _uncoveredForwards = false;
                    bool replayStopSignal = false;
                    lock (_lock)
                    {
                        _currentCheckpoint = null;
                        if (_pendingCheckpointDoneSignals > 0)
                        {
                            _pendingCheckpointDoneSignals--;
                            replayStopSignal = true;
                        }
                    }
                    if (replayStopSignal)
                    {
                        SetDependenciesDone();
                    }
                    continue;
                }

                // Guarded since this runs for every event and GetType().Name allocates.
                if (Logger.IsEnabled(LogLevel.Debug))
                {
                    Logger.SubstreamReadProcessingEvent(Name, ev.GetType().Name);
                }

                output.CancellationToken.ThrowIfCancellationRequested();

                if (ev is ICheckpointEvent checkpointEvent)
                {
                    if (ev is StopStreamCheckpoint)
                    {
                        // Peer stop consumed, every stop gate hangs off this.
                        _peerStopConsumed = true;
                        _communicationPoint.Unsubscribe(_exchangeReferenceRelation.ExchangeTargetId);
                        Logger.LogDebug("Substream read {name} consumed the other substreams stop barrier", Name);
                    }
                    ICheckpointEvent? inStreamCheckpoint = default;
                    bool scheduleCheckpoint = false;
                    lock (_lock)
                    {
                        if (_currentCheckpoint != null)
                        {
                            inStreamCheckpoint = _currentCheckpoint;
                        }
                        else if (_waitForCheckpoint == null)
                        {
                            _waitForCheckpoint = new TaskCompletionSource();
                            scheduleCheckpoint = true;
                        }
                    }
                    Logger.LogDebug("Substream read {name} recieved checkpoint event with time {time}, schedules own checkpoint: {schedule}", Name, checkpointEvent.CheckpointTime, scheduleCheckpoint);
                    if (scheduleCheckpoint)
                    {
                        // Schedule outside the lock to hinder any deadlocks with OnLockingEvent
                        // we also provide the checkpoint time to make sure that the same checkpoint from the target is scheduled twice.
                        ScheduleCheckpoint(TimeSpan.FromMilliseconds(1), checkpointEvent.CheckpointTime);
                    }
                    // A barrier that pairs with the already running local cycle schedules
                    // nothing: that is the checkpoint wave converging. Data landing after the
                    // local barrier latches one covering cycle when it is forwarded.

                    if (inStreamCheckpoint == null)
                    {
                        Debug.Assert(_waitForCheckpoint != null);
                        // Bounded so an unpairable barrier from another epoch does not park this loop
                        // forever: requested a few more times, then fail and recover so the initialize
                        // handshake reconciles. The budget is larger before the first local checkpoint,
                        // a startup barrier is resolved by the checkpoint after initial data.
                        bool checkpointArrived = false;
                        int attemptBudget = _localCheckpointSeen ? 3 : 24;
                        for (int attempt = 0; attempt < attemptBudget; attempt++)
                        {
                            // The wait observes the cancellation. A stop, delete or failure
                            // teardown waits for this task to complete, an uncancellable wait
                            // would hold the whole teardown for the remaining budget.
                            var completed = await Task.WhenAny(_waitForCheckpoint.Task, Task.Delay(PairingAttemptDelay, output.CancellationToken));
                            output.CancellationToken.ThrowIfCancellationRequested();
                            if (completed == _waitForCheckpoint.Task)
                            {
                                checkpointArrived = true;
                                break;
                            }
                            Logger.LogWarning("Substream read {name} is still waiting for a local checkpoint to pair with the other substreams barrier, requesting a new checkpoint.", Name);
                            ScheduleCheckpoint(TimeSpan.FromMilliseconds(1));
                        }
                        if (!checkpointArrived)
                        {
                            Logger.LogWarning("Substream read {name} could not pair the other substreams barrier with a local checkpoint, failing and recovering to reconcile the substreams.", Name);
                            DispatchFailAndRollback();
                            return;
                        }

                        lock (_lock)
                        {
                            _waitForCheckpoint = null; // Reset wait for checkpoint after this is completed
                            inStreamCheckpoint = _currentCheckpoint;
                        }
                    }
                    if (inStreamCheckpoint == null)
                    {
                        // The pairing wait completed but the local cycle is already gone, for
                        // example because a concurrent failure reset it. The barrier can not
                        // be consumed without a paired local checkpoint, the other substreams
                        // cycle would then never be acknowledged. Fail and recover so the
                        // initialize handshake reconciles the substreams.
                        Logger.LogWarning("Substream read {name} pairing wait completed without a local checkpoint for the other substreams barrier with time {time}, failing and recovering to reconcile the substreams.", Name, checkpointEvent.CheckpointTime);
                        DispatchFailAndRollback();
                        return;
                    }
                    var peerVersion = checkpointEvent.CheckpointVersion;
                    var driftHook = PeerBarrierVersionForTests;
                    if (driftHook != null)
                    {
                        peerVersion = driftHook(StreamName, peerVersion);
                    }
                    if (peerVersion != inStreamCheckpoint.CheckpointVersion)
                    {
                        // Drifted apart, never forward a cycle under two versions.
                        Logger.LogError("Substream read {name} paired a peer barrier with version {peerVersion} against a local checkpoint with version {localVersion}, the substreams are no longer on the same checkpoint version, failing and recovering to reconcile them.", Name, peerVersion, inStreamCheckpoint.CheckpointVersion);
                        DispatchFailAndRollback(new InvalidOperationException($"Substream read {Name} paired a peer barrier with version {peerVersion} against a local checkpoint with version {inStreamCheckpoint.CheckpointVersion}, the substreams drifted apart."));
                        return;
                    }
                    if (inStreamCheckpoint is StopStreamCheckpoint)
                    {
                        // Stop cuts at any peer barrier, later rows stay there.
                        _peerStopConsumed = true;
                        _communicationPoint.Unsubscribe(_exchangeReferenceRelation.ExchangeTargetId);
                        Logger.LogDebug("Substream read {name} drained up to the other substreams barrier with version {version} and stops fetching", Name, checkpointEvent.CheckpointVersion);
                    }
                    PairedCheckpointHookForTests?.Invoke(StreamName, checkpointEvent.CheckpointVersion, inStreamCheckpoint.CheckpointVersion);
                    await OnCheckpoint(inStreamCheckpoint.CheckpointTime);
                    // Forward this streams own checkpoint event, the other substreams
                    // event carries that streams times.
                    await output.SendLockingEvent(inStreamCheckpoint);
                    // Everything forwarded before this barrier is covered by it.
                    _uncoveredForwards = false;
                    Logger.LogDebug("Substream read {name} forwarded checkpoint with time {time} downstream", Name, inStreamCheckpoint.CheckpointTime);
                    bool replaySignal = false;
                    lock (_lock)
                    {
                        _currentCheckpoint = null;
                        if (_pendingCheckpointDoneSignals > 0)
                        {
                            _pendingCheckpointDoneSignals--;
                            replaySignal = true;
                        }
                    }
                    if (replaySignal)
                    {
                        // Replay a signal that arrived before this stream finished starting
                        SetDependenciesDone();
                    }
                    if (!_peerStopConsumed && !output.CancellationToken.IsCancellationRequested)
                    {
                        // Paired and forwarded, resume fetching behind the barrier.
                        _communicationPoint.ResumeFetch(_exchangeReferenceRelation.ExchangeTargetId);
                    }
                }
                else if (ev is InitWatermarksEvent initWatermarksEvent)
                {
                    bool swallow;
                    lock (_lock)
                    {
                        swallow = _swallowNextInitWatermarks;
                        _swallowNextInitWatermarks = false;
                    }
                    if (swallow)
                    {
                        // A returning peer's init watermarks event; this pipeline is already
                        // initialized, so consume it rather than skew the barrier alignment.
                        Logger.LogDebug("Substream read {name} consumed the returning substreams init watermarks event", Name);
                        continue;
                    }
                    bool alreadyHandled;
                    lock (_lock)
                    {
                        alreadyHandled = _initWatermarksHandled;
                        _initWatermarksHandled = true;
                    }
                    if (alreadyHandled)
                    {
                        // A second init watermarks event without this stream restarting means
                        // the other substream restarted on its own, data continuity can no
                        // longer be guaranteed.
                        await FailAndRollback();
                        return;
                    }
                    _state.Value.WatermarkNames = initWatermarksEvent.WatermarkNames.ToHashSet();
                    await output.SendLockingEvent(initWatermarksEvent);
                    SetDependenciesDone();
                }
                else if (ev is ILockingEvent lockingEvent)
                {
                    await output.SendLockingEvent(lockingEvent);
                }
                else if (ev is StreamMessage<StreamEventBatch> streamMessage)
                {
                    if (_peerStopConsumed)
                    {
                        // Past the cut, nothing commits it, lost. Never silently.
                        Logger.LogWarning("Substream read {name} received {rowCount} rows after the stop barrier, they are in no committed state on either substream.", Name, streamMessage.Data.Data.Count);
                    }
                    Logger.SubstreamReadRecievedDataBatch(Name, streamMessage.Data.Data.Count);
                    await output.SendAsync(streamMessage.Data);
                    // SendAsync rents for the pipeline, the read claim is returned after
                    streamMessage.Data.Return();
                    EnsureCoveringCheckpoint();
                }
                else if (ev is Watermark watermark)
                {
                    await output.SendWatermark(watermark);
                    EnsureCoveringCheckpoint();
                }
                else if (ev is InitialDataDoneEvent initialDataDone)
                {
                    // The other substream's initial data is complete and everything before
                    // the marker has been forwarded, downstream alignment may now release.
                    await output.SendEvent(initialDataDone);
                }
                else
                {
                    // Other event types do not flow into this stream, dispose them
                    SubstreamCommunicationPoint.DisposeEvent(ev);
                }
            }
        }

        /// <summary>
        /// Forwards the parked stop barrier.
        /// </summary>
        private void SelfForwardStopCheckpoint()
        {
            var channel = _channel;
            if (channel != null)
            {
                _ = channel.Writer.WriteAsync(s_localStopCheckpointMarker).AsTask()
                    .ContinueWith(t => Logger.LogWarning(t.Exception, "Substream read {name} could not queue the stop checkpoint marker.", Name), TaskContinuationOptions.OnlyOnFaulted);
            }
        }

        /// <summary>
        /// Bounds a parked stop; fail, never commit a one-sided cut.
        /// </summary>
        private void ScheduleStopAlignmentDeadline(ICheckpointEvent stopCheckpoint)
        {
            var cancelSource = _stopAlignmentCancel;
            if (cancelSource == null)
            {
                // Not initialized, nothing is parked.
                return;
            }
            var cancellationToken = cancelSource.Token;
            _ = Task.Run(async () =>
            {
                try
                {
                    await Task.Delay(StopDrainTimeout, cancellationToken).ConfigureAwait(false);
                    if (_peerStopConsumed)
                    {
                        return;
                    }
                    lock (_lock)
                    {
                        if (!ReferenceEquals(_currentCheckpoint, stopCheckpoint))
                        {
                            // Already paired with the peers barrier.
                            return;
                        }
                    }
                    Logger.LogWarning("Substream read {name} did not receive the other substreams barrier within the stop drain timeout, failing the stop so both substreams recover to a common checkpoint instead of committing a cut the other substream never took.", Name);
                    DispatchFailAndRollback();
                }
                catch (OperationCanceledException)
                {
                    // The epoch ended with the parked barrier.
                }
                catch (Exception e)
                {
                    Logger.LogDebug(e, "Substream read {name} stop alignment deadline ended early.", Name);
                }
            });
        }

        public override Task OnFailure(long rollbackVersion)
        {
            // This epochs stop deadline must not fire into the next.
            _stopAlignmentCancel?.Cancel();
            _communicationPoint.Unsubscribe(_exchangeReferenceRelation.ExchangeTargetId);
            _communicationPoint.OnStreamFailure();
            // Best effort, the other substream may be unreachable and waiting for its response
            // timeout would stall the recovery. The initialize handshake at restart reconciles
            // the versions when it is reachable again.
            _communicationPoint.NotifyFailAndRecover(rollbackVersion);
            return Task.CompletedTask;
        }

        public override ValueTask DisposeAsync()
        {
            // The fetch loop must not keep delivering events after the operator is disposed
            _stopAlignmentCancel?.Cancel();
            _communicationPoint.Unsubscribe(_exchangeReferenceRelation.ExchangeTargetId);
            return base.DisposeAsync();
        }

        public override Task CheckpointDone(long checkpointVersion)
        {
            // Send checkpoint done to the communication point so the other substream can set dependencies done.
            return _communicationPoint.SendCheckpointDone(checkpointVersion);
        }

        /// <summary>
        /// Consumed the peer's stop barrier, uncommitted, else the handoff deadlocks.
        /// </summary>
        internal bool HasCleanPeerStop => _peerStopConsumed;

        /// <summary>
        /// True from the first stop barrier, returning peers are refused.
        /// </summary>
        internal bool IsStopping => _stopping;

        /// <summary>
        /// True when a committed checkpoint covers the consumed stop barrier. Stamped onto
        /// outgoing checkpoint done acks so the stopping peer only confirms its drain on an
        /// ack that attests the barrier consumption, not on one that merely raced in after
        /// the barrier was fetched.
        /// </summary>
        internal bool PeerStopConsumedCommitted => _peerStopConsumedCommitted;

        /// <summary>
        /// Resumes every reader or none, a stop refuses under locks.
        /// </summary>
        internal static bool TryResumeAllAfterPeerReconnect(IReadOnlyList<SubstreamReadOperator> readOperators)
        {
            var taken = new bool[readOperators.Count];
            try
            {
                // List order, a stop holds one lock at most.
                for (int i = 0; i < readOperators.Count; i++)
                {
                    Monitor.Enter(readOperators[i]._lock, ref taken[i]);
                }
                for (int i = 0; i < readOperators.Count; i++)
                {
                    if (readOperators[i]._stopping)
                    {
                        return false;
                    }
                }
                foreach (var readOperator in readOperators)
                {
                    readOperator.ResumeLocked();
                    ResumedHookForTests?.Invoke(readOperator.StreamName, readOperator);
                }
                return true;
            }
            finally
            {
                for (int i = taken.Length - 1; i >= 0; i--)
                {
                    if (taken[i])
                    {
                        Monitor.Exit(readOperators[i]._lock);
                    }
                }
            }
        }

        private void ResumeLocked()
        {
            _peerStopConsumed = false;
            _peerStopConsumedCommitted = false;
            _swallowNextInitWatermarks = true;
            var channel = _channel;
            if (channel != null)
            {
                SubscribeToPeer(channel);
            }
        }

        /// <summary>
        /// Subscribes, holding the fetch at every barrier until paired.
        /// </summary>
        private void SubscribeToPeer(Channel<IStreamEvent> channel)
        {
            _communicationPoint.Subscribe(_exchangeReferenceRelation.ExchangeTargetId, async (ev) =>
            {
                // Pause before the write, a racing ResumeFetch would stall forever.
                if (ev is ICheckpointEvent)
                {
                    _communicationPoint.PauseFetch(_exchangeReferenceRelation.ExchangeTargetId);
                }
                await channel.Writer.WriteAsync(ev);
            });
        }

        /// <summary>
        /// Events from another substream must eventually be covered by a checkpoint in this
        /// stream, even when no local source change triggers one - uncovered, they would wait
        /// forever once the substreams paired their cycles. Latched to one covering cycle per
        /// barrier: scheduling on every event would make the substreams checkpoint forever,
        /// each cycle's barriers re-trigger cycles at the peers.
        /// </summary>
        private void EnsureCoveringCheckpoint()
        {
            if (!_uncoveredForwards)
            {
                _uncoveredForwards = true;
                ScheduleCheckpoint(TimeSpan.FromMilliseconds(100));
            }
        }

        public void RecieveCheckpointDone(long checkpointVersion)
        {
            if (!TrySetDependenciesDone())
            {
                // Not wired yet, buffer it. Uncapped, a cap hung recovery.
                lock (_lock)
                {
                    _pendingCheckpointDoneSignals++;
                }
            }
        }

        public Task FailAndRecover(long recoveryPoint)
        {
            return FailAndRollback(restoreVersion: recoveryPoint);
        }

        /// <summary>
        /// Starts a fail and recover without awaiting it. The failure teardown waits for
        /// this operators own fetch task to complete, so a rollback initiated from inside
        /// that task must never be awaited there, the await would deadlock the recovery.
        /// </summary>
        private void DispatchFailAndRollback(Exception? exception = null)
        {
            _ = Task.Run(async () =>
            {
                try
                {
                    await FailAndRollback(exception);
                }
                catch (Exception e)
                {
                    // Must be logged, an unobserved fault would leave the stream running
                    // against a barrier that was never reconciled.
                    Logger.LogWarning(e, "Substream read {name} fail and recover after an unpairable barrier failed, the fetch stall watchdog escalates if the stream does not progress.", Name);
                }
            });
        }

        /// <summary>
        /// Fails the stream after a fetch error. Fetching removes events from the other
        /// substreams queue, so a failed fetch can mean events were removed there but never
        /// arrived here. They cannot be fetched again, the stream fails and both substreams
        /// recover to a common checkpoint where the events are regenerated.
        /// </summary>
        public Task FailAndRecoverOnFetchError(Exception exception)
        {
            return FailAndRollback(exception);
        }

        public override void DoLockingEvent(ILockingEvent lockingEvent)
        {
            // At this point the operator states are stored in the checkpoint object
            // So the checkpoint object must be stored to be used inside this stream.
            // If state is handled by the state manager client instead this is not required.
            if (lockingEvent is ICheckpointEvent checkpointEvent)
            {
                TaskCompletionSource? taskSource = default;
                lock (_lock)
                {
                    _currentCheckpoint = checkpointEvent;
                    _localCheckpointSeen = true;
                    if (_waitForCheckpoint != null)
                    {
                        taskSource = _waitForCheckpoint;
                    }
                }
                Logger.LogDebug("Substream read {name} stored local checkpoint with time {time}, waiter present: {waiterPresent}", Name, checkpointEvent.CheckpointTime, taskSource != null);
                if (taskSource != null)
                {
                    // Set task completion source outside of lock to hinder any deadlocks
                    taskSource.TrySetResult();
                }
                if (checkpointEvent is StopStreamCheckpoint)
                {
                    // Flag and decision under _lock, ResumeAfterPeerReconnect must not interleave.
                    bool selfForward;
                    lock (_lock)
                    {
                        _stopping = true;
                        selfForward = _peerStopConsumed || !_communicationPoint.IsSubscribed(_exchangeReferenceRelation.ExchangeTargetId);
                    }
                    if (selfForward)
                    {
                        SelfForwardStopCheckpoint();
                    }
                    else
                    {
                        // Bounded, a silent peer must not hold the stop forever.
                        ScheduleStopAlignmentDeadline(checkpointEvent);
                    }
                }
            }
            lock (_lock)
            {
                if (_fetchTask == null)
                {
                    Task? newTask = null;
                    newTask = RunTask(FetchData)
                        .ContinueWith(t =>
                        {
                            lock (_lock)
                            {
                                // Only clear if this is still the active fetch task, so the
                                // continuation of an old task cannot clear a newly started one.
                                if (_fetchTask == newTask)
                                {
                                    _fetchTask = null;
                                }
                            }
                        });
                    _fetchTask = newTask;
                }
            }
            // Only the other substreams init watermarks event is forwarded (it always comes, the
            // substreams reinitialize together). Forwarding the local one too would emit two after a
            // restore and skew every downstream barrier alignment by one event.
        }
    }
}
