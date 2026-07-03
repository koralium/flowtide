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
using Microsoft.Extensions.Logging;
using FlowtideDotNet.Storage.Comparers;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.Tree;
using System.Diagnostics;

namespace FlowtideDotNet.Core.Operators.Exchange
{
    /// <summary>
    /// Exchange target that stores events for another substream.
    ///
    /// Events are stored in a tree keyed on a monotonically increasing event id and are kept
    /// until the other substream has included them in a completed checkpoint, which it signals
    /// with the consumed event id on its checkpoint done message. This makes the delivery safe
    /// against rollbacks, after a recovery the other substream re-reads from the event id it
    /// had persisted and no event can be lost or applied twice.
    /// </summary>
    internal class SubstreamTarget : IExchangeTarget
    {
        private readonly DataValueContainer _dataValueContainer;
        private readonly int _exchangeTargetId;
        private readonly int _columnCount;
        private readonly SubstreamCommunicationPoint _substreamCommunication;
        private readonly Action _targetCallDependenciesDone;
        private int _targetId;
        private long _eventCounter;
        private IBPlusTree<long, IStreamEvent, FlowtideDotNet.Storage.Tree.PrimitiveListKeyContainer<long>, StreamEventValueContainer>? _events;
        private IMemoryAllocator? _memoryAllocator;
        private readonly SemaphoreSlim _lockSemaphore;
        private Func<long, Task>? _failAndRecoverFunc;

        private PrimitiveList<int>? _weights;
        private PrimitiveList<uint>? _iterations;
        private IColumn[]? _columns;

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
            _dataValueContainer = new DataValueContainer();
            _lockSemaphore = new SemaphoreSlim(1);
            _substreamCommunication.RegisterSubstreamTarget(_exchangeTargetId, this);
        }

        private void NewColumns()
        {
            Debug.Assert(_memoryAllocator != null);
            _columns = new IColumn[_columnCount];
            for (int i = 0; i < _columnCount; i++)
            {
                _columns[i] = Column.Create(_memoryAllocator);
            }

            _weights = new PrimitiveList<int>(_memoryAllocator);
            _iterations = new PrimitiveList<uint>(_memoryAllocator);
        }

        public async Task AddCheckpointState(ExchangeOperatorState exchangeOperatorState)
        {
            Debug.Assert(_events != null);
            // The semaphore serializes the commit against concurrent reads and trims from
            // the other substreams fetch requests.
            await _lockSemaphore.WaitAsync();
            try
            {
                await _events.Commit();
            }
            finally
            {
                _lockSemaphore.Release();
            }
            exchangeOperatorState.TargetsEventCounter[_targetId] = _eventCounter;
        }

        public ValueTask AddEvent(EventBatchWeighted weightedBatch, int index)
        {
            Debug.Assert(_columns != null);
            Debug.Assert(_weights != null);
            Debug.Assert(_iterations != null);

            _weights.Add(weightedBatch.Weights[index]);
            _iterations.Add(weightedBatch.Iterations[index]);

            for (int i = 0; i < _columns.Length; i++)
            {
                weightedBatch.EventBatchData.Columns[i].GetValueAt(index, _dataValueContainer, default);
                _columns[i].Add(_dataValueContainer);
            }
            return ValueTask.CompletedTask;
        }

        public async ValueTask BatchComplete(long time)
        {
            Debug.Assert(_weights != null);
            Debug.Assert(_iterations != null);
            Debug.Assert(_events != null);
            Debug.Assert(_columns != null);

            if (_weights.Count > 0)
            {
                _substreamCommunication.Logger.LogDebug("Target {targetId} waiting for lock to store batch", _exchangeTargetId);
                await _lockSemaphore.WaitAsync();
                try
                {
                    var msg = new StreamMessage<StreamEventBatch>(new StreamEventBatch(new EventBatchWeighted(_weights, _iterations, new EventBatchData(_columns))), time);
                    await _events.Upsert(_eventCounter++, msg);
                    NewColumns();
                }
                finally
                {
                    _lockSemaphore.Release();
                }
                _substreamCommunication.Logger.LogDebug("Target {targetId} stored batch", _exchangeTargetId);

                await _substreamCommunication.TargetHasData(_exchangeTargetId);
            }
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
            _targetId = targetId;
            if (state.TargetsEventCounter.TryGetValue(_targetId, out var eventCounter))
            {
                _eventCounter = eventCounter;
            }
            else
            {
                _eventCounter = 0;
            }

            await _substreamCommunication.InitializeOperator(restoreVersion);

            _events = await stateManagerClient.GetOrCreateTree($"events_target_{targetId}", new BPlusTreeOptions<long, IStreamEvent, FlowtideDotNet.Storage.Tree.PrimitiveListKeyContainer<long>, StreamEventValueContainer>()
            {
                Comparer = new PrimitiveListComparer<long>(),
                KeySerializer = new FlowtideDotNet.Storage.Serializers.PrimitiveListKeyContainerSerializer<long>(memoryAllocator),
                ValueSerializer = new StreamEventValueSerializer(memoryAllocator),
                MemoryAllocator = memoryAllocator,
                UseByteBasedPageSizes = true
            });

            NewColumns();
        }

        public void NewBatch(EventBatchWeighted weightedBatch)
        {
        }

        public async Task OnLockingEvent(ILockingEvent lockingEvent)
        {
            Debug.Assert(_events != null);
            _substreamCommunication.Logger.LogDebug("Target {targetId} waiting for lock to store locking event", _exchangeTargetId);
            await _lockSemaphore.WaitAsync();
            try
            {
                await _events.Upsert(_eventCounter++, lockingEvent);
            }
            finally
            {
                _lockSemaphore.Release();
            }
            _substreamCommunication.Logger.LogDebug("Target {targetId} stored locking event", _exchangeTargetId);
            await _substreamCommunication.TargetHasData(_exchangeTargetId);
        }

        public async Task OnLockingEventPrepare(LockingEventPrepare lockingEventPrepare)
        {
            Debug.Assert(_events != null);
            await _lockSemaphore.WaitAsync();
            try
            {
                await _events.Upsert(_eventCounter++, lockingEventPrepare);
            }
            finally
            {
                _lockSemaphore.Release();
            }
            await _substreamCommunication.TargetHasData(_exchangeTargetId);
        }

        public async Task OnWatermark(Watermark watermark)
        {
            Debug.Assert(_events != null);
            await _lockSemaphore.WaitAsync();
            try
            {
                await _events.Upsert(_eventCounter++, watermark);
            }
            finally
            {
                _lockSemaphore.Release();
            }
            await _substreamCommunication.TargetHasData(_exchangeTargetId);
        }

        /// <summary>
        /// Reads events starting at the given event id without removing them from the tree.
        /// Events are removed first when the other substream confirms them as part of a
        /// completed checkpoint through <see cref="TrimConsumedEvents"/>.
        /// </summary>
        public async ValueTask<bool> ReadData(long fromEventId, List<SubstreamEventData> outputList, int maxCount)
        {
            // the target is not yet initialized
            if (_events == null)
            {
                return false;
            }
            _substreamCommunication.Logger.LogDebug("Target {targetId} read data from {fromEventId} starting", _exchangeTargetId, fromEventId);
            await _lockSemaphore.WaitAsync();
            try
            {
                int count = 0;
                var iterator = _events.CreateIterator();
                await iterator.Seek(fromEventId);
                await foreach (var page in iterator)
                {
                    page.EnterWriteLock();
                    foreach (var kv in page)
                    {
                        if (count >= maxCount)
                        {
                            page.ExitWriteLock();
                            return true;
                        }
                        if (kv.Value is StreamMessage<StreamEventBatch> streamMessage)
                        {
                            if (streamMessage.Data is IRentable rent)
                            {
                                rent.Rent(1);
                            }
                        }
                        outputList.Add(new SubstreamEventData()
                        {
                            ExchangeTargetId = _exchangeTargetId,
                            EventId = kv.Key,
                            StreamEvent = kv.Value
                        });
                        count++;
                    }
                    page.ExitWriteLock();
                }
                return false;
            }
            finally
            {
                _lockSemaphore.Release();
                _substreamCommunication.Logger.LogDebug("Target {targetId} read data done", _exchangeTargetId);
            }
        }

        /// <summary>
        /// Removes all events up to and including the consumed event id, they are part of a
        /// completed checkpoint in the other substream and can no longer be requested.
        /// </summary>
        public async Task TrimConsumedEvents(long consumedEventId)
        {
            if (_events == null)
            {
                return;
            }
            _substreamCommunication.Logger.LogDebug("Target {targetId} trim up to {consumedEventId} starting", _exchangeTargetId, consumedEventId);
            await _lockSemaphore.WaitAsync();
            try
            {
                var keysToRemove = new List<long>();
                var iterator = _events.CreateIterator();
                await iterator.SeekFirst();
                bool done = false;
                await foreach (var page in iterator)
                {
                    page.EnterWriteLock();
                    foreach (var kv in page)
                    {
                        if (kv.Key > consumedEventId)
                        {
                            done = true;
                            break;
                        }
                        keysToRemove.Add(kv.Key);
                    }
                    page.ExitWriteLock();
                    if (done)
                    {
                        break;
                    }
                }
                foreach (var key in keysToRemove)
                {
                    // Read modify write is used so the value can be disposed under the tree lock
                    await _events.RMWNoResult(key, default, (input, current, exists) =>
                    {
                        if (exists)
                        {
                            DisposeEvent(current);
                        }
                        return (default, GenericWriteOperation.Delete);
                    });
                }
            }
            finally
            {
                _lockSemaphore.Release();
                _substreamCommunication.Logger.LogDebug("Target {targetId} trim done", _exchangeTargetId);
            }
        }

        private static void DisposeEvent(IStreamEvent? streamEvent)
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

        public Task OnFailure(long recoveryPoint)
        {
            return _substreamCommunication.SendFailAndRecover(recoveryPoint);
        }

        public Task FailAndRecover(long recoveryPoint)
        {
            if (_failAndRecoverFunc == null)
            {
                throw new InvalidOperationException("Not initialized");
            }
            return _failAndRecoverFunc(recoveryPoint);
        }

        public Task TargetSubstreamCheckpointDone(long checkpointVersion)
        {
            _targetCallDependenciesDone();
            return Task.CompletedTask;
        }

        public Task CheckpointDone(long checkpointVersion)
        {
            return _substreamCommunication.SendCheckpointDone(checkpointVersion);
        }
    }
}
