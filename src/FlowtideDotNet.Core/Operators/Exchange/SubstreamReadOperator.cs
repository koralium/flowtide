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
    /// Ingress operator that reads events from an exchange in another substream.
    ///
    /// Events are fetched through the substream communication point and buffered in a
    /// transient channel. The delivery is transient, checkpoint cycles in the other substream
    /// only complete when this stream has consumed the checkpoint barrier and sent its
    /// checkpoint done message. After a failure both substreams roll back to a common
    /// checkpoint and the other substream regenerates the events by replaying from it.
    /// </summary>
    internal class SubstreamReadOperator : IngressVertex<StreamEventBatch>
    {
        /// <summary>
        /// Marker placed in the channel when this stream takes its stop checkpoint. The fetch
        /// loop forwards the stop barrier when it reads the marker, after all already buffered
        /// events, so the stop does not depend on more events from the other substream which
        /// may already have stopped completely.
        /// </summary>
        private sealed class LocalStopCheckpointMarker : IStreamEvent
        {
        }

        private static readonly LocalStopCheckpointMarker s_localStopCheckpointMarker = new LocalStopCheckpointMarker();

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
        // True after the first local checkpoint barrier since the last restore, before it an
        // unpairable barrier means the stream is still starting up, not an epoch mismatch.
        private volatile bool _localCheckpointSeen;

        public SubstreamReadOperator(SubstreamCommunicationPoint communicationPoint, SubstreamExchangeReferenceRelation referenceRelation, DataflowBlockOptions options) : base(options)
        {
            this._communicationPoint = communicationPoint;
            _exchangeReferenceRelation = referenceRelation;
            _communicationPoint.RegisterReadOperator(this);
        }

        public override string DisplayName => "Substream Read";

        public int ExchangeTargetId => _exchangeReferenceRelation.ExchangeTargetId;

        /// <summary>
        /// Allocator that received events for this operators exchange target are deserialized
        /// with, so data fetched over the network is accounted on the operator that consumes
        /// it. Only valid after the operator has been initialized, which is guaranteed since
        /// events are only fetched for subscribed targets.
        /// </summary>
        internal IMemoryAllocator ReceiveMemoryAllocator => MemoryAllocator;

        /// <summary>
        /// The stream may first finish stopping when this operator has consumed the other
        /// substreams stop barrier and committed a checkpoint that covers it, everything the
        /// other substream sent before it is then part of this streams final state. The
        /// stopping stream runs stop checkpoint cycles until this is true, with a drain
        /// timeout that protects against another substream that never stops.
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
                _localCheckpointSeen = false;
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
        }

        protected override async Task OnCheckpoint(long checkpointTime)
        {
            Debug.Assert(_state != null);
            await _state.Commit();
            _peerStopConsumedCommitted = _peerStopConsumed;
        }

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
                    else
                    {
                        // The event pairs with a checkpoint that is already running. Data sent
                        // before the other substreams barrier can be processed after this
                        // streams barrier due to barrier alignment, a follow up cycle covers it.
                        // The request must be versionless, the version dedup would otherwise
                        // swallow it together with the sibling requests for the current cycle.
                        ScheduleCheckpoint(TimeSpan.FromMilliseconds(100));
                    }

                    if (inStreamCheckpoint == null)
                    {
                        Debug.Assert(_waitForCheckpoint != null);
                        // The wait is bounded, an unpairable barrier from another epoch would
                        // otherwise park this loop forever. The checkpoint is requested again a
                        // few times, when none arrives the stream fails and recovers so the
                        // initialize handshake can reconcile the substreams.
                        // Before the first local checkpoint the budget is much larger, a barrier
                        // arriving while this stream is still starting up is resolved by the
                        // checkpoint after initial data, failing on it would restart the slow
                        // startup in a loop.
                        bool checkpointArrived = false;
                        int attemptBudget = _localCheckpointSeen ? 3 : 24;
                        for (int attempt = 0; attempt < attemptBudget; attempt++)
                        {
                            var completed = await Task.WhenAny(_waitForCheckpoint.Task, Task.Delay(TimeSpan.FromSeconds(5)));
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
                            await FailAndRollback();
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
                        // The local checkpoint was already consumed, the unpaired barrier must
                        // not be dropped silently, a new checkpoint is requested to cover it.
                        Logger.LogWarning("Substream read {name} pairing wait completed without a local checkpoint for the other substreams barrier with time {time}, requesting a new checkpoint.", Name, checkpointEvent.CheckpointTime);
                        ScheduleCheckpoint(TimeSpan.FromMilliseconds(1));
                    }
                    if (inStreamCheckpoint != null)
                    {
                        await OnCheckpoint(inStreamCheckpoint.CheckpointTime);
                        // Forward this streams own checkpoint event, the other substreams
                        // event carries that streams times.
                        await output.SendLockingEvent(inStreamCheckpoint);
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
                    else
                    {
                        throw new InvalidOperationException("Checkpoint event not found in stream");
                    }
                }
                else if (ev is InitWatermarksEvent initWatermarksEvent)
                {
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
                    // Data from another substream must eventually be covered by a checkpoint
                    // in this stream, even when no local source change triggers one.
                    ScheduleCheckpoint(TimeSpan.FromMilliseconds(100));
                }
                else if (ev is Watermark watermark)
                {
                    await output.SendWatermark(watermark);
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

        public void RecieveCheckpointDone(long checkpointVersion)
        {
            if (!TrySetDependenciesDone())
            {
                // The dependencies done callback is not wired yet. Each checkpoint cycle
                // consumes exactly one signal, so it must be buffered instead of lost.
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
                    // The stop checkpoint must complete without a checkpoint event from the
                    // other substream, it may already have stopped and produces no more events.
                    // The marker makes the fetch loop forward the stop barrier after the events
                    // that are already buffered. If a checkpoint event from the other substream
                    // arrives before the marker it pairs with the stop checkpoint as usual.
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
            // Only the other substreams init watermarks event is forwarded, it always comes
            // since the substreams reinitialize together. Forwarding the local event as well
            // would emit two init watermark events after a restore while local paths emit one,
            // skewing every barrier alignment downstream by one event.
        }
    }
}
