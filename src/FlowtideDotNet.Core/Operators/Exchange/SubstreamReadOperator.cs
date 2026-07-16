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

        // Handoff drain patience; raised in tests.
        internal static TimeSpan HandoffDrainTimeout = TimeSpan.FromSeconds(10);

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
        // Set when the other substreams stop barrier has been consumed, the stream may
        // first finish stopping when the consumption is part of a committed checkpoint.
        private volatile bool _peerStopConsumed;
        private volatile bool _peerStopConsumedCommitted;
        // A returning peer's restarted pipeline sends one init watermarks event that must be
        // consumed without forwarding, a second init downstream would skew barrier alignment.
        private volatile bool _swallowNextInitWatermarks;
        private bool _restoredWithWatermarkNames;
        // True after the first local checkpoint barrier since the last restore, before it an
        // unpairable barrier means the stream is still starting up, not an epoch mismatch.
        private volatile bool _localCheckpointSeen;
        // During a handoff drain no peer events arrive to pair local checkpoints with, so
        // they are self-forwarded like a stop checkpoint.
        private volatile bool _handoffDraining;
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
        /// True once this operator has consumed the other substreams stop barrier and committed a
        /// checkpoint covering it. The stopping stream runs stop cycles until then, bounded by a
        /// drain timeout in case the other substream never stops.
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
                _swallowNextInitWatermarks = false;
                _localCheckpointSeen = false;
                _handoffDraining = false;
                _uncoveredForwards = false;
                // Signals from before the restore belong to the aborted epoch, replaying
                // them would complete a new cycle too early.
                _pendingCheckpointDoneSignals = 0;
            }
            // Cancel outside the lock so a stale fetch loop that awaits it can complete and stop.
            staleWaitForCheckpoint?.TrySetCanceled();

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

            _communicationPoint.Subscribe(_exchangeReferenceRelation.ExchangeTargetId, async (ev) =>
            {
                await channel.Writer.WriteAsync(ev);
            });

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
                    // The stop checkpoint is forwarded without waiting for an event from the
                    // other substream, it may never send one when it has crashed. Later
                    // events are covered by the next stop cycle.
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
                        // The other substream is stopping and everything it sent has been
                        // received, stop fetching from it.
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

        public override Task OnFailure(long rollbackVersion)
        {
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
            _communicationPoint.Unsubscribe(_exchangeReferenceRelation.ExchangeTargetId);
            return base.DisposeAsync();
        }

        public override Task CheckpointDone(long checkpointVersion)
        {
            // Send checkpoint done to the communication point so the other substream can set dependencies done.
            return _communicationPoint.SendCheckpointDone(checkpointVersion);
        }

        /// <summary>
        /// True once this operator has consumed the peer's clean-handoff stop barrier. The
        /// consumption need not be committed - its cycle can only complete with the returning
        /// peer's acks, so requiring the commit here would deadlock the handoff; a failure
        /// before it falls back to normal recovery. Version safety is checked by the
        /// communication point against the peer's acked commit versions.
        /// </summary>
        internal bool HasCleanPeerStop => _peerStopConsumed;

        /// <summary>
        /// True when a committed checkpoint covers the consumed stop barrier. Stamped onto
        /// outgoing checkpoint done acks so the stopping peer only confirms its drain on an
        /// ack that attests the barrier consumption, not on one that merely raced in after
        /// the barrier was fetched.
        /// </summary>
        internal bool PeerStopConsumedCommitted => _peerStopConsumedCommitted;

        /// <summary>
        /// Resumes consumption from a peer that returned through a clean handoff: resets the
        /// stop tracking, re-subscribes on the same channel, and arms the swallow of the
        /// returning peer's init watermarks event.
        /// </summary>
        internal void ResumeAfterPeerReconnect()
        {
            lock (_lock)
            {
                _peerStopConsumed = false;
                _peerStopConsumedCommitted = false;
                _swallowNextInitWatermarks = true;
            }
            var channel = _channel;
            if (channel != null)
            {
                _communicationPoint.Subscribe(_exchangeReferenceRelation.ExchangeTargetId, async (ev) =>
                {
                    await channel.Writer.WriteAsync(ev);
                });
            }
        }

        /// <summary>
        /// First handoff drain phase: stop taking in new peer events. Already-fetched events
        /// stay in the channel and drain through the pipeline, and local checkpoints are
        /// self-forwarded from here since no peer event will arrive to pair them.
        /// </summary>
        public override void BeginHandoffDrain()
        {
            _handoffDraining = true;
            _communicationPoint.Unsubscribe(_exchangeReferenceRelation.ExchangeTargetId);
            // Nudge a checkpoint stored before the drain to forward behind the buffered events.
            bool pendingCheckpoint;
            lock (_lock)
            {
                pendingCheckpoint = _currentCheckpoint != null;
            }
            var channel = _channel;
            if (pendingCheckpoint && channel != null)
            {
                _ = channel.Writer.WriteAsync(s_localStopCheckpointMarker).AsTask();
            }
        }

        /// <summary>
        /// Second handoff drain phase: waits for the fetch loop to go idle and the channel to
        /// drain, then marks the peer consumption finished so the following stop completes
        /// after one committed cycle, like a consumed peer stop barrier would.
        /// </summary>
        public override async Task CompleteHandoffDrainAsync()
        {
            await _communicationPoint.WaitForFetchLoopIdleAsync(HandoffDrainTimeout);

            var channel = _channel;
            if (channel != null)
            {
                var deadline = Environment.TickCount64 + (long)HandoffDrainTimeout.TotalMilliseconds;
                while (channel.Reader.Count > 0)
                {
                    if (Environment.TickCount64 >= deadline)
                    {
                        throw new TimeoutException($"The read channel of {Name} did not drain within {HandoffDrainTimeout} during the handoff drain.");
                    }
                    await Task.Delay(10);
                }
            }

            _peerStopConsumed = true;
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
                // The dependencies done callback is not wired yet. Each checkpoint cycle
                // consumes exactly one signal, so it must be buffered instead of lost.
                // Deliberately uncapped: a stopping peer commits several versions without
                // consuming acks and each buffered ack pairs one to one with a queued barrier
                // this stream has yet to forward. The state machine stashes at most one early
                // credit per operator, so the buffer meters them out one per forwarded
                // barrier; a cap drops acks the peer never resends and starves the paired
                // cycles (a previous cap hung recovery under load). Stale acks cannot get
                // here, they are epoch fenced at the communication point, and the counter
                // resets on restore.
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
        private void DispatchFailAndRollback()
        {
            _ = Task.Run(async () =>
            {
                try
                {
                    await FailAndRollback();
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
                if (checkpointEvent is StopStreamCheckpoint || _handoffDraining)
                {
                    // The stop checkpoint must complete without a checkpoint event from the
                    // other substream, it may already have stopped and produces no more events.
                    // The marker makes the fetch loop forward the stop barrier after the events
                    // that are already buffered. If a checkpoint event from the other substream
                    // arrives before the marker it pairs with the stop checkpoint as usual.
                    // A handoff drain is the same situation: the subscription is gone.
                    var channel = _channel;
                    if (channel != null)
                    {
                        _ = channel.Writer.WriteAsync(s_localStopCheckpointMarker).AsTask();
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
