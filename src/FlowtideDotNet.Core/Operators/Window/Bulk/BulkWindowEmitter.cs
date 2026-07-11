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

using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Memory;

namespace FlowtideDotNet.Core.Operators.Window.Bulk
{
    /// <summary>
    /// Collects output rows for the bulk window operator. A row consists of the input columns
    /// (copied from the stored key row) and one value per window function, mapped through the emit list.
    /// </summary>
    internal class BulkWindowEmitter
    {
        private IColumn[] _columns;
        private PrimitiveList<int> _weights;
        private PrimitiveList<uint> _iterations;
        private readonly int _inputColumnCount;
        private readonly List<int> _emitList;
        private readonly IMemoryAllocator _memoryAllocator;

        public int Count => _weights.Count;

        public BulkWindowEmitter(int inputColumnCount, List<int> emitList, IMemoryAllocator memoryAllocator)
        {
            _inputColumnCount = inputColumnCount;
            _emitList = emitList;
            _memoryAllocator = memoryAllocator;
            _columns = new IColumn[emitList.Count];
            for (int i = 0; i < emitList.Count; i++)
            {
                _columns[i] = ColumnFactory.Get(memoryAllocator);
            }
            _weights = new PrimitiveList<int>(memoryAllocator);
            _iterations = new PrimitiveList<uint>(memoryAllocator);
        }

        public EventBatchWeighted GetCurrentBatch()
        {
            var batch = new EventBatchWeighted(_weights, _iterations, new EventBatchData(_columns));

            _columns = new IColumn[_emitList.Count];
            for (int i = 0; i < _emitList.Count; i++)
            {
                _columns[i] = ColumnFactory.Get(_memoryAllocator);
            }
            _weights = new PrimitiveList<int>(_memoryAllocator);
            _iterations = new PrimitiveList<uint>(_memoryAllocator);

            return batch;
        }

        /// <summary>
        /// Adds one output row. The input column values are read from <paramref name="rowBatch"/> at
        /// <paramref name="rowIndex"/> and the function values from <paramref name="functionValues"/>.
        /// </summary>
        public void AddOutputRow(EventBatchData rowBatch, int rowIndex, IDataValue[] functionValues, int weight)
        {
            for (int i = 0; i < _emitList.Count; i++)
            {
                var emitIndex = _emitList[i];
                if (emitIndex >= _inputColumnCount)
                {
                    _columns[i].Add(functionValues[emitIndex - _inputColumnCount]);
                }
                else
                {
                    var columnValue = rowBatch.Columns[emitIndex].GetValueAt(rowIndex, default);
                    _columns[i].Add(columnValue);
                }
            }
            _weights.Add(weight);
            _iterations.Add(0);
        }
    }
}
