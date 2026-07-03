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

        /// <summary>
        /// The last event id from the other substream that has been processed by this operator.
        /// Committed with each checkpoint, fetching resumes from the next id after a restore.
        /// </summary>
        public long LastEventId { get; set; } = -1;
    }

    /// <summary>
    /// Ingress operator that reads events from an exchange in another substream.
    ///
    /// Events are fetched by event id through the substream communication point and buffered in
    /// a transient channel. The processed position is part of this streams checkpoint state, so
    /// after a restore the operator resumes fetching from the exact event that follows its
    /// restored state. The other substream keeps events until this stream confirms them with
    /// its checkpoint done message, which makes the delivery safe against rollbacks in either
    /// substream.
    /// </summary>
    internal class SubstreamReadOperator : IngressVertex<StreamEventBatch>
    {
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
        private Channel<(long EventId, IStreamEvent Event)>? _channel;
        private IObjectState<SubstreamReadState>? _state;
        private long _lastCommittedEventId = -1;

        public SubstreamReadOperator(SubstreamCommunicationPoint communicationPoint, SubstreamExchangeReferenceRelation referenceRelation, DataflowBlockOptions options) : base(options)
        {
            this._communicationPoint = communicationPoint;
            _exchangeReferenceRelation = referenceRelation;
            _communicationPoint.RegisterReadOperator(this);
        }

        public override string DisplayName => "Substream Read";

        public int ExchangeTargetId => _exchangeReferenceRelation.ExchangeTargetId;

        /// <summary>
        /// The last event id from the other substream that is included in this streams latest
        /// completed checkpoint. Sent with checkpoint done messages so the other substream can
        /// remove confirmed events.
        /// </summary>
        public long LastCommittedEventId => Interlocked.Read(ref _lastCommittedEventId);

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
            }
            // Cancel outside the lock so a stale fetch loop that awaits it can complete and stop.
            staleWaitForCheckpoint?.TrySetCanceled();

            // A fresh channel is created on every restore, buffered events from before the
            // failure are refetched from the other substream starting at the restored position.
            _channel = Channel.CreateBounded<(long EventId, IStreamEvent Event)>(new BoundedChannelOptions(1024)
            {
                SingleReader = true
            });

            _state = await stateManagerClient.GetOrCreateObjectStateAsync<SubstreamReadState>("substream_read_state");
            if (_state.Value == null)
            {
                _state.Value = new SubstreamReadState();
            }
            Interlocked.Exchange(ref _lastCommittedEventId, _state.Value.LastEventId);
        }

        protected override async Task OnCheckpoint(long checkpointTime)
        {
            Debug.Assert(_state != null);
            await _state.Commit();
            Interlocked.Exchange(ref _lastCommittedEventId, _state.Value!.LastEventId);
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

            // Fetch data from the communication point, starting after the last processed event
            _communicationPoint.Subscribe(_exchangeReferenceRelation.ExchangeTargetId, _state.Value.LastEventId + 1, async (eventId, ev) =>
            {
                await channel.Writer.WriteAsync((eventId, ev));
            });

            while (!output.CancellationToken.IsCancellationRequested)
            {
                var (eventId, ev) = await channel.Reader.ReadAsync(output.CancellationToken);

                if (eventId <= _state.Value.LastEventId)
                {
                    // The event is already part of this streams state, can happen when events
                    // overlap after a resubscribe.
                    continue;
                }

                output.CancellationToken.ThrowIfCancellationRequested();

                if (ev is ICheckpointEvent checkpointEvent)
                {
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
                        // The event from the other substream is paired with a local checkpoint
                        // that is already running. Data the other substream sent before its
                        // barrier can be processed after this streams barrier due to barrier
                        // alignment in operators with multiple inputs, so a follow up cycle is
                        // scheduled to cover that data.
                        ScheduleCheckpoint(TimeSpan.FromMilliseconds(100), checkpointEvent.CheckpointTime);
                    }

                    if (inStreamCheckpoint == null)
                    {
                        // Wait until the checkpoint event has been collected from this stream.
                        Debug.Assert(_waitForCheckpoint != null);
                        await _waitForCheckpoint.Task;

                        lock (_lock)
                        {
                            _waitForCheckpoint = null; // Reset wait for checkpoint after this is completed
                            inStreamCheckpoint = _currentCheckpoint;
                        }
                    }
                    if (inStreamCheckpoint != null)
                    {
                        // The checkpoint event is included in the committed position
                        _state.Value.LastEventId = eventId;
                        await OnCheckpoint(inStreamCheckpoint.CheckpointTime);
                        // Forward this streams own checkpoint event, the checkpoint event from
                        // the other substream has that streams times and must not flow here.
                        await output.SendLockingEvent(inStreamCheckpoint);
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
                            // Deliver a checkpoint done signal that arrived before this stream
                            // finished starting, it completes the dependencies for this cycle.
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
                        // that the other substream failed and restarted on its own.
                        // Data continuity can no longer be guaranteed, fail and recover,
                        // the failure also propagates a fail and recover to the other substream.
                        await FailAndRollback();
                        return;
                    }
                    _state.Value.WatermarkNames = initWatermarksEvent.WatermarkNames.ToHashSet();
                    _state.Value.LastEventId = eventId;
                    await output.SendLockingEvent(initWatermarksEvent);
                    SetDependenciesDone();
                }
                else if (ev is ILockingEvent lockingEvent)
                {
                    _state.Value.LastEventId = eventId;
                    await output.SendLockingEvent(lockingEvent);
                }
                else if (ev is StreamMessage<StreamEventBatch> streamMessage)
                {
                    Logger.LogDebug("Substream read {name} recieved data batch with {count} rows", Name, streamMessage.Data.Data.Count);
                    await output.SendAsync(streamMessage.Data);
                    _state.Value.LastEventId = eventId;
                    // Data from another substream must eventually be covered by a checkpoint in
                    // this stream, even when no local source change triggers one. Without this,
                    // data that arrives after a checkpoint barrier could wait forever when both
                    // substreams paired their cycles and no new source data arrives.
                    // The scheduling is deduplicated by the stream context, so requesting it for
                    // every batch is cheap.
                    ScheduleCheckpoint(TimeSpan.FromMilliseconds(100));
                }
                else if (ev is Watermark watermark)
                {
                    _state.Value.LastEventId = eventId;
                    await output.SendWatermark(watermark);
                }
                else
                {
                    // Other event types such as locking event prepares are counted as processed
                    // but do not flow into this stream.
                    _state.Value.LastEventId = eventId;
                }
            }
        }

        public override async Task OnFailure(long rollbackVersion)
        {
            _communicationPoint.Unsubscribe(_exchangeReferenceRelation.ExchangeTargetId);
            await _communicationPoint.SendFailAndRecover(rollbackVersion);
        }

        public override ValueTask DisposeAsync()
        {
            // Stop the communication point from fetching data for this operator, the fetch
            // loop would otherwise keep delivering events after the operator is disposed.
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
                // The signal arrived before this stream finished starting, the dependencies
                // done callback is not wired yet. Each checkpoint cycle consumes exactly one
                // signal from the other substream, so the signal must not be lost, it is
                // buffered and replayed when a checkpoint runs.
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
                    if (_waitForCheckpoint != null)
                    {
                        taskSource = _waitForCheckpoint;
                    }
                }
                if (taskSource != null)
                {
                    // Set task completion source outside of lock to hinder any deadlocks
                    taskSource.TrySetResult();
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
            if (lockingEvent is InitWatermarksEvent initWatermarksEvent &&
                _state?.Value != null &&
                _state.Value.WatermarkNames != null)
            {
                // Run task to send watermark values
                base.DoLockingEvent(lockingEvent);
            }
        }
    }
}
