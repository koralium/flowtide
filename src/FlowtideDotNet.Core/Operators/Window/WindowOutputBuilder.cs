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
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Substrait.Relations;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.Operators.Window
{
    internal class WindowOutputBuilder : IWindowAddOutputRow
    {
        private IColumn[] columns;
        private PrimitiveList<int> weights;
        private PrimitiveList<uint> iterations;
        private readonly int _inputColumnCount;
        private readonly List<int> emitList;
        private readonly IMemoryAllocator memoryAllocator;

        public int Count => weights.Count;

        public WindowOutputBuilder(int inputColumnCount, List<int> emitList, IMemoryAllocator memoryAllocator)
        {
            columns = new IColumn[emitList.Count];

            for (int i = 0; i < emitList.Count; i++)
            {
                columns[i] = ColumnFactory.Get(memoryAllocator);
            }

            weights = new PrimitiveList<int>(memoryAllocator);
            iterations = new PrimitiveList<uint>(memoryAllocator);
            _inputColumnCount = inputColumnCount;
            this.emitList = emitList;
            this.memoryAllocator = memoryAllocator;
        }

        public EventBatchWeighted GetCurrentBatch()
        {
            var batch = new EventBatchWeighted(weights, iterations, new EventBatchData(columns));

            columns = new IColumn[emitList.Count];
            for (int i = 0; i < emitList.Count; i++)
            {
                columns[i] = ColumnFactory.Get(memoryAllocator);
            }
            weights = new PrimitiveList<int>(memoryAllocator);
            iterations = new PrimitiveList<uint>(memoryAllocator);

            return batch;
        }

        public void AddOutputRow<T>(ColumnRowReference columnRowReference, T value, int weight) where T : IDataValue
        {
            for(int i = 0; i < emitList.Count; i++)
            {
                var emitIndex = emitList[i];
                if (emitIndex >= _inputColumnCount)
                {
                    columns[i].Add(value);
                }
                else
                {
                    var columnValue = columnRowReference.referenceBatch.Columns[emitList[i]].GetValueAt(columnRowReference.RowIndex, default);
                    columns[i].Add(columnValue);
                }
            }
            weights.Add(weight);
            iterations.Add(0);
        }

        public void AddOutputRow(ColumnRowReference columnRowReference, IDataValue[] functionValues, int weight)
        {
            for (int i = 0; i < emitList.Count; i++)
            {
                var emitIndex = emitList[i];
                if (emitIndex >= _inputColumnCount)
                {
                    columns[i].Add(functionValues[emitIndex - _inputColumnCount]);
                }
                else
                {
                    var columnValue = columnRowReference.referenceBatch.Columns[emitList[i]].GetValueAt(columnRowReference.RowIndex, default);
                    columns[i].Add(columnValue);
                }
            }
            weights.Add(weight);
            iterations.Add(0);
        }

        internal void AddDeleteToOutput(ColumnRowReference columnRowReference, WindowValue windowValue)
        {
            if (windowValue.valueContainer._previousValueSent.Get(windowValue.index))
            {
                var stateLength = windowValue.valueContainer._functionStates[0].GetListLength(windowValue.index);

                for (int i = 0; i < emitList.Count; i++)
                {
                    var emitIndex = emitList[i];
                    if (emitIndex >= _inputColumnCount)
                    {
                        for (int w = 0; w < stateLength; w++)
                        {
                            var value = windowValue.valueContainer._functionStates[0].GetListElementValue(windowValue.index, w);
                            columns[i].Add(value);
                        }
                    }
                    else
                    {
                        var columnValue = columnRowReference.referenceBatch.Columns[emitList[i]].GetValueAt(columnRowReference.RowIndex, default);

                        for (int w = 0; w < stateLength; w++)
                        {
                            columns[i].Add(columnValue);
                        }
                    }
                }

                for (int i = 0; i < stateLength; i++)
                {
                    weights.Add(-1);
                    iterations.Add(0);
                }
            }
        }
    }
}
