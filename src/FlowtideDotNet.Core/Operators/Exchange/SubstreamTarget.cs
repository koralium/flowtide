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
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Queue;
using FlowtideDotNet.Storage.StateManager;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.Tracing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.Operators.Exchange
{
    internal class SubstreamTarget : IExchangeTarget
    {
        private readonly DataValueContainer _dataValueContainer;
        private readonly int _exchangeTargetId;
        private readonly int _columnCount;
        private readonly SubstreamCommunicationPoint _substreamCommunication;
        private IFlowtideQueue<IStreamEvent, StreamEventValueContainer>? _queue;
        private IMemoryAllocator? _memoryAllocator;
        private readonly SemaphoreSlim _lockSemaphore;

        private PrimitiveList<int>? _weights;
        private PrimitiveList<uint>? _iterations;
        private IColumn[]? _columns;

        public SubstreamTarget(int exchangeTargetId, int columnCount, SubstreamCommunicationPoint substreamCommunication)
        {
            this._exchangeTargetId = exchangeTargetId;
            _columnCount = columnCount;
            _substreamCommunication = substreamCommunication;
            _dataValueContainer = new DataValueContainer();
            _lockSemaphore = new SemaphoreSlim(1);
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

        public Task AddCheckpointState(ExchangeOperatorState exchangeOperatorState)
        {
            return Task.CompletedTask;
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
            Debug.Assert(_queue != null);
            Debug.Assert(_columns != null);

            if (_weights.Count > 0)
            {
                await _lockSemaphore.WaitAsync();
                try
                {
                    await _queue.Enqueue(new StreamMessage<StreamEventBatch>(new StreamEventBatch(new EventBatchWeighted(_weights, _iterations, new EventBatchData(_columns))), time));
                }
                finally
                {
                    _lockSemaphore.Release();
                }

                NewColumns();
                await _substreamCommunication.TargetHasData(_exchangeTargetId);
            }
        }

        public async Task Initialize(int targetId, IStateManagerClient stateManagerClient, ExchangeOperatorState state, IMemoryAllocator memoryAllocator)
        {
            _memoryAllocator = memoryAllocator;
            _queue = await stateManagerClient.GetOrCreateQueue($"events_target_{targetId}", new FlowtideQueueOptions<IStreamEvent, StreamEventValueContainer>()
            {
                MemoryAllocator = memoryAllocator,
                ValueSerializer = new StreamEventValueSerializer(memoryAllocator)
            });

            NewColumns();
        }

        public void NewBatch(EventBatchWeighted weightedBatch)
        {
        }

        public async Task OnLockingEvent(ILockingEvent lockingEvent)
        {
            Debug.Assert(_queue != null);
            await _lockSemaphore.WaitAsync();
            try
            {
                await _queue.Enqueue(lockingEvent);
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
                await _queue.Enqueue(watermark);
            }
            finally
            {
                _lockSemaphore.Release();
            }
            await _substreamCommunication.TargetHasData(_exchangeTargetId);
        }

        public async ValueTask<bool> ReadData(List<IStreamEvent> outputList, int maxCount)
        {
            Debug.Assert(_queue != null);
            await _lockSemaphore.WaitAsync();
            try
            {
                while (_queue.Count > 0 && outputList.Count < maxCount)
                {
                    var val = await _queue.Dequeue();
                    outputList.Add(val);
                }
                return _queue.Count > 0;
            }
            finally
            {
                _lockSemaphore.Release();
            }

        }
    }
}
