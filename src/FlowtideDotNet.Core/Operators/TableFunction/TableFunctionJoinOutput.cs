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
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.Compute.Columnar;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Memory;

namespace FlowtideDotNet.Core.Operators.TableFunction
{
    /// <summary>
    /// Accumulator the table function appends the right (generated) side of a join into.
    /// The columns and weight lists live for the whole incoming batch, so no per-row
    /// batch is allocated. The operator sets the per-input-row context
    /// (<see cref="InputIndex"/>, <see cref="InputWeight"/>, <see cref="InputIteration"/>)
    /// before invoking the function, and <see cref="CommitRow"/> folds that context in as
    /// the function produces rows.
    /// </summary>
    internal sealed class TableFunctionJoinOutput : ITableFunctionOutput
    {
        /// <summary>The generated (right side) columns, one per table function schema column.</summary>
        public IColumn[] FunctionColumns { get; }

        /// <summary>For each output row, the index of the input row it was generated from.</summary>
        public PrimitiveList<int> FoundOffsets { get; }

        public PrimitiveList<int> Weights { get; }

        public PrimitiveList<uint> Iterations { get; }

        // Per input-row context, set by the operator before each function invocation.
        public int InputIndex;
        public int InputWeight;
        public uint InputIteration;

        public TableFunctionJoinOutput(int functionOutputLength, IMemoryAllocator memoryAllocator)
        {
            FunctionColumns = new IColumn[functionOutputLength];
            for (int i = 0; i < functionOutputLength; i++)
            {
                FunctionColumns[i] = Column.Create(memoryAllocator);
            }
            FoundOffsets = new PrimitiveList<int>(memoryAllocator);
            Weights = new PrimitiveList<int>(memoryAllocator);
            Iterations = new PrimitiveList<uint>(memoryAllocator);
        }

        public IReadOnlyList<IColumn> Columns => FunctionColumns;

        public int Count => Weights.Count;

        public void CommitRow(int weight, uint iteration)
        {
            FoundOffsets.Add(InputIndex);
            Weights.Add(InputWeight * weight);
            Iterations.Add(InputIteration);
        }

        /// <summary>
        /// Emits the all-null right side used by a LEFT join when an input row produced no
        /// matching rows.
        /// </summary>
        public void AddNullRow(int inputIndex, int inputWeight)
        {
            FoundOffsets.Add(inputIndex);
            Weights.Add(inputWeight);
            Iterations.Add(0);
            for (int i = 0; i < FunctionColumns.Length; i++)
            {
                FunctionColumns[i].Add(NullValue.Instance);
            }
        }

        public void Dispose()
        {
            FoundOffsets.Dispose();
            Weights.Dispose();
            Iterations.Dispose();
            for (int i = 0; i < FunctionColumns.Length; i++)
            {
                FunctionColumns[i].Dispose();
            }
        }
    }
}
