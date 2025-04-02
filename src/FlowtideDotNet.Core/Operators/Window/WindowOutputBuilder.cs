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
        IColumn[] columns;
        PrimitiveList<int> weights;
        PrimitiveList<uint> iterations;
        private readonly int columnCount;
        private readonly IMemoryAllocator memoryAllocator;

        public int Count => weights.Count;

        public WindowOutputBuilder(int columnCount, IMemoryAllocator memoryAllocator)
        {
            columns = new IColumn[columnCount];

            for (int i = 0; i < columnCount; i++)
            {
                columns[i] = ColumnFactory.Get(memoryAllocator);
            }

            weights = new PrimitiveList<int>(memoryAllocator);
            iterations = new PrimitiveList<uint>(memoryAllocator);
            this.columnCount = columnCount;
            this.memoryAllocator = memoryAllocator;
        }

        public EventBatchWeighted GetCurrentBatch()
        {
            var batch = new EventBatchWeighted(weights, iterations, new EventBatchData(columns));

            columns = new IColumn[columnCount];
            for (int i = 0; i < columnCount; i++)
            {
                columns[i] = ColumnFactory.Get(memoryAllocator);
            }
            weights = new PrimitiveList<int>(memoryAllocator);
            iterations = new PrimitiveList<uint>(memoryAllocator);

            return batch;
        }

        public void AddOutputRow<T>(ColumnRowReference columnRowReference, T value, int weight) where T : IDataValue
        {
            for (int i = 0; i < columnRowReference.referenceBatch.Columns.Count; i++)
            {
                columns[i].Add(columnRowReference.referenceBatch.Columns[i].GetValueAt(columnRowReference.RowIndex, default));
            }
            columns[columns.Length - 1].Add(value);
            weights.Add(weight);
            iterations.Add(0);
        }

        internal void AddDeleteToOutput(ColumnRowReference columnRowReference, WindowValue windowValue)
        {
            if (windowValue.valueContainer._previousValueSent.Get(columnRowReference.RowIndex))
            {
                var stateLength = windowValue.valueContainer._functionStates[0].GetListLength(columnRowReference.RowIndex);
                for (int i = 0; i < columnRowReference.referenceBatch.Columns.Count; i++)
                {
                    var existingColumnValue = columnRowReference.referenceBatch.Columns[i].GetValueAt(columnRowReference.RowIndex, default);
                    for (int w = 0; w < stateLength; w++)
                    {
                        columns[i].Add(existingColumnValue);
                    }
                }

                for (int i = 0; i < stateLength; i++)
                {
                    columns[columnCount - 1].Add(windowValue.valueContainer._functionStates[0].GetListElementValue(columnRowReference.RowIndex, i));
                    weights.Add(-1);
                    iterations.Add(0);
                }
            }
        }
    }
}
