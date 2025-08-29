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
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.StateManager;
using System.Diagnostics;

namespace FlowtideDotNet.Core.Operators.Exchange
{
    internal class StandardOutputTarget : IExchangeTarget
    {
        private readonly DataValueContainer _dataValueContainer;
        private readonly int _columnCount;
        private IMemoryAllocator? _memoryAllocator;

        private EventBatchWeighted? _currentBatch;
        private PrimitiveList<int>? _offsets;
        private PrimitiveList<int>? _weights;
        private PrimitiveList<uint>? _iterations;

        public StandardOutputTarget(int columnCount)
        {
            this._columnCount = columnCount;
            _dataValueContainer = new DataValueContainer();
        }

        public Task AddCheckpointState(ExchangeOperatorState exchangeOperatorState)
        {
            return Task.CompletedTask;
        }

        public ValueTask AddEvent(EventBatchWeighted weightedBatch, int index)
        {
            Debug.Assert(_offsets != null);
            Debug.Assert(_weights != null);
            Debug.Assert(_iterations != null);

            _weights.Add(weightedBatch.Weights[index]);
            _iterations.Add(weightedBatch.Iterations[index]);
            _offsets.Add(index);

            return ValueTask.CompletedTask;
        }

        public ValueTask BatchComplete(long time)
        {
            return ValueTask.CompletedTask;
        }

        public EventBatchWeighted? GetEvents()
        {
            Debug.Assert(_offsets != null);
            Debug.Assert(_currentBatch != null);
            Debug.Assert(_weights != null);
            Debug.Assert(_iterations != null);

            if (_weights.Count > 0)
            {
                IColumn[] outputColumns = new IColumn[_columnCount];

                for (int i = 0; i < _columnCount; i++)
                {
                    outputColumns[i] = new ColumnWithOffset(_currentBatch.EventBatchData.Columns[i], _offsets, false);
                }

                var batch = new EventBatchWeighted(_weights, _iterations, new EventBatchData(outputColumns));
                NewColumns();
                return batch;
            }
            return null;
        }

        private void NewColumns()
        {
            Debug.Assert(_memoryAllocator != null);
            _offsets = new PrimitiveList<int>(_memoryAllocator);
            _weights = new PrimitiveList<int>(_memoryAllocator);
            _iterations = new PrimitiveList<uint>(_memoryAllocator);
        }

        public Task Initialize(int targetId, IStateManagerClient stateManagerClient, ExchangeOperatorState state, IMemoryAllocator memoryAllocator)
        {
            _memoryAllocator = memoryAllocator;
            //_offsets = new PrimitiveList<int>(memoryAllocator);
            NewColumns();
            return Task.CompletedTask;
        }

        public Task OnLockingEvent(ILockingEvent lockingEvent)
        {
            // Locking events for standard output are handled automatically
            return Task.CompletedTask;
        }

        public Task OnLockingEventPrepare(LockingEventPrepare lockingEventPrepare)
        {
            // Locking event prepare for standard output are handled automatically
            return Task.CompletedTask;
        }

        public Task OnWatermark(Watermark watermark)
        {
            // Watermark for standard output are handled automatically
            return Task.CompletedTask;
        }

        public void NewBatch(EventBatchWeighted weightedBatch)
        {
            _currentBatch = weightedBatch;
        }

        public Task OnFailure(long recoveryPoint)
        {
            return Task.CompletedTask;
        }
    }
}
