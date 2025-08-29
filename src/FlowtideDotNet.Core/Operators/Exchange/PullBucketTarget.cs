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

using Apache.Arrow.Memory;
using FlowtideDotNet.Base;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Storage.Comparers;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Serializers;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.Tree;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.Operators.Exchange
{
    internal class PullBucketTarget : IExchangeTarget
    {
        private readonly DataValueContainer _dataValueContainer;
        private int _targetId;
        private long _eventCounter;
        private IBPlusTree<long, IStreamEvent, PrimitiveListKeyContainer<long>, StreamEventValueContainer>? _events;
        private IMemoryAllocator? _memoryAllocator;
        private int _columnCount;
        private readonly SemaphoreSlim _lockSemaphore;

        private PrimitiveList<int>? _weights;
        private PrimitiveList<uint>? _iterations;
        private IColumn[]? _columns;

        public PullBucketTarget(int columnCount)
        {
            _columnCount = columnCount;
            _dataValueContainer = new DataValueContainer();
            _lockSemaphore = new SemaphoreSlim(1);
        }

        public async Task AddCheckpointState(ExchangeOperatorState exchangeOperatorState)
        {
            Debug.Assert(_events != null);
            // Commit the tree with events
            await _events.Commit();
            // Add the current event counter
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
                await _lockSemaphore.WaitAsync();
                try
                {
                    await _events.Upsert(_eventCounter++, new StreamMessage<StreamEventBatch>(new StreamEventBatch(new EventBatchWeighted(_weights, _iterations, new EventBatchData(_columns))), time));
                }
                finally
                {
                    _lockSemaphore.Release();
                }
                
                NewColumns();
            }
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

        public async Task Initialize(int targetId, IStateManagerClient stateManagerClient, ExchangeOperatorState state, IMemoryAllocator memoryAllocator)
        {
            _memoryAllocator = memoryAllocator;
            _targetId = targetId;
            if (state.TargetsEventCounter.TryGetValue(_targetId, out var eventCounter))
            {
                _eventCounter = eventCounter;
            }
            _events = await stateManagerClient.GetOrCreateTree($"events_target_{targetId}", new BPlusTreeOptions<long, IStreamEvent, PrimitiveListKeyContainer<long>, StreamEventValueContainer>()
            {
                Comparer = new PrimitiveListComparer<long>(),
                KeySerializer = new PrimitiveListKeyContainerSerializer<long>(memoryAllocator),
                ValueSerializer = new StreamEventValueSerializer(memoryAllocator),
                MemoryAllocator = memoryAllocator,
                UseByteBasedPageSizes = true
            });
            NewColumns();
        }

        public async Task OnLockingEvent(ILockingEvent lockingEvent)
        {
            Debug.Assert(_events != null);
            await _lockSemaphore.WaitAsync();
            try
            {
                await _events.Upsert(_eventCounter++, lockingEvent);
            }
            finally
            {
                _lockSemaphore.Release();
            }
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
        }

        public async Task FetchData(ExchangeFetchDataMessage message)
        {
            Debug.Assert(_events != null);
            await _lockSemaphore.WaitAsync();
            try
            {
                var iterator = _events.CreateIterator();
                await iterator.Seek(message.FromEventId);

                List<IStreamEvent> outputData = new List<IStreamEvent>();
                await foreach (var page in iterator)
                {
                    page.EnterWriteLock();
                    foreach (var kv in page)
                    {
                        message.LastEventId = kv.Key;
                        outputData.Add(kv.Value);
                    }
                    page.ExitWriteLock();
                    if (outputData.Count > 100)
                    {
                        break;
                    }
                }
                message.OutEvents = outputData;
            }
            finally
            {
                _lockSemaphore.Release();
            }
            
        }

        public void NewBatch(EventBatchWeighted weightedBatch)
        {
        }

        public Task OnFailure(long recoveryPoint)
        {
            return Task.CompletedTask;
        }
    }
}
