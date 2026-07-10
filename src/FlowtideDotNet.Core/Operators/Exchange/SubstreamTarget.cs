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
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.Utils;
using Microsoft.Extensions.Logging;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Queue;
using FlowtideDotNet.Storage.StateManager;
using System.Diagnostics;

namespace FlowtideDotNet.Core.Operators.Exchange
{
    /// <summary>
    /// Exchange target that stores events for another substream. The queue is transient (never part
    /// of a checkpoint): a checkpoint only completes once the other substream consumed the barrier
    /// and acked, so the queue is always ahead of the latest checkpoint. After a failure both roll
    /// back to a common checkpoint and this stream regenerates the queue by replaying from it.
    /// </summary>
    internal class SubstreamTarget : IExchangeTarget
    {
        private readonly int _exchangeTargetId;
        private readonly int _columnCount;
        private readonly SubstreamCommunicationPoint _substreamCommunication;
        private readonly Action _targetCallDependenciesDone;
        private IFlowtideQueue<IStreamEvent, StreamEventValueContainer>? _queue;
        private IMemoryAllocator? _memoryAllocator;
        private readonly SemaphoreSlim _lockSemaphore;
        private Func<long, Task>? _failAndRecoverFunc;

        // False between a failure and the next initialize: the queue still holds aborted-epoch events
        // until the rollback and regeneration, serving them would deliver stale events.
        private bool _ready;

        // Set once this streams stop barrier is stored (nothing is stored after it). The stream can
        // only finish stopping after the other substream has fetched it AND acknowledged a
        // checkpoint that covers consuming it, or events before it could be disposed before that
        // substream durably received them. The fetch alone is not delivery: the dequeue removes
        // the events from the queue, but the fetch response can still be lost after it (the wire
        // serializer can throw, the cross-silo call can fail in transit). The confirmation is a
        // checkpoint done acknowledgement stamped by the peer as covering its consumed stop
        // barriers, see TargetSubstreamCheckpointDone.
        private volatile bool _stopBarrierStored;
        private volatile bool _stopBarrierFetched;
        private volatile bool _stopBarrierFetchAcked;

        private PrimitiveList<int>? _weights;
        private PrimitiveList<uint>? _iterations;
        // Row indices into the current source batch, the rows this target received from the
        // partitioning. The columns are built with one bulk copy per column at batch
        // completion, copying value by value per row costs several times more.
        private PrimitiveList<int>? _offsets;
        private EventBatchWeighted? _currentBatch;
        // Reusable all-zero insert positions for the bulk copy. InsertFrom takes pre-insert
        // coordinates (item i lands at position[i] + i), so appending everything to an empty
        // column is position zero for every row.
        private int[] _zeroPositions = Array.Empty<int>();
        // Reusable scratch for composing this target's row indices through a view column's
        // offsets, bulk copies read only plain columns.
        private int[] _composedLookupScratch = Array.Empty<int>();

        public SubstreamTarget(
            int exchangeTargetId,
            int columnCount,
            SubstreamCommunicationPoint substreamCommunication,
            Action targetCallDependenciesDone)
        {
            this._exchangeTargetId = exchangeTargetId;
            _columnCount = columnCount;
            _substreamCommunication = substreamCommunication;
            this._targetCallDependenciesDone = targetCallDependenciesDone;
            _lockSemaphore = new SemaphoreSlim(1);
            _substreamCommunication.RegisterSubstreamTarget(_exchangeTargetId, this);
        }

        private void NewBuffers()
        {
            Debug.Assert(_memoryAllocator != null);
            _offsets = new PrimitiveList<int>(_memoryAllocator);
            _weights = new PrimitiveList<int>(_memoryAllocator);
            _iterations = new PrimitiveList<uint>(_memoryAllocator);
        }

        public Task AddCheckpointState(ExchangeOperatorState exchangeOperatorState)
        {
            // The queue is transient and is not saved with the checkpoint, its content is
            // regenerated by replay after a rollback.
            return Task.CompletedTask;
        }

        public void AddEvent(EventBatchWeighted weightedBatch, int index)
        {
            Debug.Assert(_offsets != null);
            Debug.Assert(_weights != null);
            Debug.Assert(_iterations != null);

            _weights.Add(weightedBatch.Weights[index]);
            _iterations.Add(weightedBatch.Iterations[index]);
            _offsets.Add(index);
        }

        public async ValueTask BatchComplete(long time)
        {
            Debug.Assert(_weights != null);
            Debug.Assert(_iterations != null);
            Debug.Assert(_offsets != null);
            Debug.Assert(_queue != null);

            var sourceBatch = _currentBatch;
            _currentBatch = null;
            if (_weights.Count > 0)
            {
                Debug.Assert(sourceBatch != null);
                await _lockSemaphore.WaitAsync();
                try
                {
                    if (_stopBarrierStored)
                    {
                        // No events are stored after the stop barrier, the other substream
                        // unsubscribes when it consumes it so the batch would never be
                        // fetched. The rows are regenerated by replay at the next start.
                        _offsets.Dispose();
                        _weights.Dispose();
                        _iterations.Dispose();
                        NewBuffers();
                        return;
                    }
                    var columns = BuildColumns(sourceBatch);
                    var msg = new StreamMessage<StreamEventBatch>(new StreamEventBatch(new EventBatchWeighted(_weights, _iterations, new EventBatchData(columns))), time);
                    _substreamCommunication.Logger.SubstreamTargetStoresBatch(_exchangeTargetId, msg.Data.Data.Weights.Count);
                    await _queue.Enqueue(msg);
                    NewBuffers();
                }
                finally
                {
                    _lockSemaphore.Release();
                }

                await _substreamCommunication.TargetHasData(_exchangeTargetId);
            }
        }

        /// <summary>
        /// Builds the queued columns with one bulk copy per column from the source batch's
        /// rows this target received. The queue outlives the source batch, so the rows are
        /// copied rather than referenced: a reference view would pin the whole source batch
        /// for as long as a slice of it sits in the queue, and the queue's spill and wire
        /// serialization only handle materialized columns.
        /// </summary>
        private IColumn[] BuildColumns(EventBatchWeighted sourceBatch)
        {
            Debug.Assert(_offsets != null);
            Debug.Assert(_memoryAllocator != null);

            int count = _offsets.Count;
            if (_zeroPositions.Length < count)
            {
                _zeroPositions = new int[Math.Max(count, _zeroPositions.Length * 2)];
            }
            ReadOnlySpan<int> lookup = _offsets.Span;
            ReadOnlySpan<int> positions = _zeroPositions.AsSpan(0, count);

            var sourceColumns = sourceBatch.EventBatchData.Columns;
            IColumn[] columns = new IColumn[_columnCount];
            for (int i = 0; i < _columnCount; i++)
            {
                var source = sourceColumns[i];
                var columnLookup = lookup;
                // Operators like joins emit view columns (offsets over an inner column); the
                // bulk copy reads only plain columns, so the row indices are composed through
                // the view's offsets instead of copying value by value. A view offset can be
                // the null marker, InsertFrom's null lookup index handles it.
                while (source is ColumnWithOffset view)
                {
                    if (_composedLookupScratch.Length < count)
                    {
                        _composedLookupScratch = new int[Math.Max(count, _composedLookupScratch.Length * 2)];
                    }
                    var viewOffsets = view.Offsets;
                    for (int j = 0; j < count; j++)
                    {
                        var offset = columnLookup[j];
                        _composedLookupScratch[j] = offset == ColumnWithOffset.NullValueIndex
                            ? ColumnWithOffset.NullValueIndex
                            : viewOffsets.Get(offset);
                    }
                    columnLookup = _composedLookupScratch.AsSpan(0, count);
                    source = view.InnerColumn;
                }
                var column = Column.Create(_memoryAllocator);
                column.InsertFrom(source, in columnLookup, in positions, ColumnWithOffset.NullValueIndex);
                columns[i] = column;
            }
            _offsets.Dispose();
            return columns;
        }

        public async Task Initialize(
            long restoreVersion,
            int targetId,
            IStateManagerClient stateManagerClient,
            ExchangeOperatorState state,
            IMemoryAllocator memoryAllocator,
            Func<long, Task> failAndRecoverFunc)
        {
            _failAndRecoverFunc = failAndRecoverFunc;
            _memoryAllocator = memoryAllocator;

            await _substreamCommunication.InitializeOperator(restoreVersion);

            _queue = await stateManagerClient.GetOrCreateQueue($"events_target_{targetId}", new FlowtideQueueOptions<IStreamEvent, StreamEventValueContainer>()
            {
                MemoryAllocator = memoryAllocator,
                ValueSerializer = new StreamEventValueSerializer(memoryAllocator)
            });

            NewBuffers();

            await _lockSemaphore.WaitAsync();
            _ready = true;
            _stopBarrierStored = false;
            _stopBarrierFetched = false;
            _stopBarrierFetchAcked = false;
            _lockSemaphore.Release();
        }

        /// <summary>
        /// True when the other substream has fetched the stop barrier and acknowledged a
        /// checkpoint after the fetch, or no stop barrier has been stored. The stream keeps
        /// running stop checkpoint cycles until then, so the events before the barrier have
        /// verifiably reached the other substream before this stream disposes its queue; a
        /// peer that never confirms is handled by the stop drain timeout.
        /// </summary>
        public bool ReadyToStop => !_stopBarrierStored || (_stopBarrierFetched && _stopBarrierFetchAcked);

        public void NewBatch(EventBatchWeighted weightedBatch)
        {
            // Alive for the whole partitioning call, the bulk copy at batch completion
            // reads the received rows from it.
            _currentBatch = weightedBatch;
        }

        public async Task OnLockingEvent(ILockingEvent lockingEvent)
        {
            Debug.Assert(_queue != null);
            // Logged before the semaphore so a barrier that reached the exchange but is
            // blocked behind an in progress read or write is distinguishable from a barrier
            // that never arrived.
            _substreamCommunication.Logger.LogDebug("Target {targetId} received locking event {eventType}", _exchangeTargetId, lockingEvent.GetType().Name);
            await _lockSemaphore.WaitAsync();
            try
            {
                if (_stopBarrierStored)
                {
                    // The stream is stopping and may run multiple stop checkpoint cycles while
                    // it drains, the other substream only needs the first stop barrier.
                    return;
                }
                _substreamCommunication.Logger.LogDebug("Target {targetId} stores locking event {eventType}", _exchangeTargetId, lockingEvent.GetType().Name);
                await _queue.Enqueue(lockingEvent);
                if (lockingEvent is StopStreamCheckpoint)
                {
                    _stopBarrierStored = true;
                }
            }
            finally
            {
                _lockSemaphore.Release();
            }
            await _substreamCommunication.TargetHasData(_exchangeTargetId);
        }

        public async Task OnLockingEventPrepare(LockingEventPrepare lockingEventPrepare)
        {
            Debug.Assert(_queue != null);
            await _lockSemaphore.WaitAsync();
            try
            {
                if (_stopBarrierStored)
                {
                    return;
                }
                await _queue.Enqueue(lockingEventPrepare);
            }
            finally
            {
                _lockSemaphore.Release();
            }
            await _substreamCommunication.TargetHasData(_exchangeTargetId);
        }

        public async Task OnWatermark(Watermark watermark)
        {
            Debug.Assert(_queue != null);
            await _lockSemaphore.WaitAsync();
            try
            {
                if (_stopBarrierStored)
                {
                    return;
                }
                await _queue.Enqueue(watermark);
            }
            finally
            {
                _lockSemaphore.Release();
            }
            await _substreamCommunication.TargetHasData(_exchangeTargetId);
        }

        /// <summary>
        /// Dequeues events for the other substream. The events are removed from the queue,
        /// they are always ahead of the latest complete checkpoint since a checkpoint only
        /// completes when the other substream has consumed its barrier, so after a failure
        /// both substreams roll back to a common checkpoint and the events are regenerated.
        /// </summary>
        public async ValueTask<bool> ReadData(List<SubstreamEventData> outputList, int maxCount)
        {
            // the target is not yet initialized
            if (_queue == null)
            {
                return false;
            }
            await _lockSemaphore.WaitAsync();
            try
            {
                if (!_ready)
                {
                    return false;
                }
                int count = 0;
                while (_queue.Count > 0 && count < maxCount)
                {
                    var val = await _queue.Dequeue();
                    // Dequeuing advances an index but the queue page still references the
                    // event and returns its rent when the page is disposed, so the event is
                    // rented again for the receiver.
                    StreamEventRent.Rent(val);
                    if (val is StopStreamCheckpoint)
                    {
                        // The stop barrier has been handed out, but the fetch response can
                        // still be lost after the dequeue; the stop may only finish once the
                        // other substream acknowledges a checkpoint after this point, see
                        // TargetSubstreamCheckpointDone.
                        _stopBarrierFetched = true;
                    }
                    outputList.Add(new SubstreamEventData()
                    {
                        ExchangeTargetId = _exchangeTargetId,
                        StreamEvent = val
                    });
                    count++;
                }
                return _queue.Count > 0;
            }
            finally
            {
                _lockSemaphore.Release();
            }
        }

        public async Task OnFailure(long recoveryPoint)
        {
            // Stop serving reads until the state has been rolled back and initialize has run,
            // waiting on the semaphore also drains a read that is currently in progress.
            await _lockSemaphore.WaitAsync();
            _ready = false;
            _lockSemaphore.Release();
            _substreamCommunication.OnStreamFailure();
            // Best effort notification that must not delay the failure handling here, see
            // NotifyFailAndRecover.
            _substreamCommunication.NotifyFailAndRecover(recoveryPoint);
        }

        /// <summary>
        /// True once initialize wired the rollback. A registered but not yet initialized
        /// target silently no-ops a rollback, routing must skip it.
        /// </summary>
        public bool CanFailAndRecover => _failAndRecoverFunc != null;

        public Task FailAndRecover(long recoveryPoint)
        {
            if (_failAndRecoverFunc == null)
            {
                return Task.CompletedTask;
            }
            return _failAndRecoverFunc(recoveryPoint);
        }

        public Task TargetSubstreamCheckpointDone(long checkpointVersion, bool coversPeerStopBarrier)
        {
            if (_stopBarrierFetched && coversPeerStopBarrier)
            {
                // The peer attests that this ack's committed cycle covers consuming the stop
                // barrier, so the drain is confirmed and the stop may finish. The fetch alone
                // is not enough (the response can be lost after the destructive dequeue), and
                // an unstamped ack proves nothing: it can be for a cycle committed before the
                // barrier was consumed, delivered after the fetch raced it.
                _stopBarrierFetchAcked = true;
            }
            _targetCallDependenciesDone();
            return Task.CompletedTask;
        }

        public Task CheckpointDone(long checkpointVersion)
        {
            return _substreamCommunication.SendCheckpointDone(checkpointVersion);
        }
    }
}
