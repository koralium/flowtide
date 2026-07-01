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
using FlowtideDotNet.Core.Compute.Columnar;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Memory;

namespace FlowtideDotNet.Core.Operators.TableFunction
{
    /// <summary>
    /// A plain reusable buffer of produced rows (columns + weights + iterations) that a
    /// table function appends into.
    /// <para>
    /// Used in two places:
    /// <list type="bullet">
    /// <item>as the scratch buffer for the join-condition path, where it is cleared per
    /// input row and its <see cref="Batch"/> is fed to the join condition before the
    /// passing rows are copied out; and</item>
    /// <item>as the output of the table function read (<c>FROM unnest(...)</c>) operator,
    /// whose buffers are handed off with <see cref="ToBatch"/>.</item>
    /// </list>
    /// </para>
    /// </summary>
    internal sealed class TableFunctionRowBuffer : ITableFunctionOutput
    {
        private readonly Column[] _columns;
        private readonly PrimitiveList<int> _weights;
        private readonly PrimitiveList<uint> _iterations;
        private readonly EventBatchData _batch;

        public TableFunctionRowBuffer(int columnCount, IMemoryAllocator memoryAllocator)
        {
            _columns = new Column[columnCount];
            for (int i = 0; i < columnCount; i++)
            {
                _columns[i] = Column.Create(memoryAllocator);
            }
            _weights = new PrimitiveList<int>(memoryAllocator);
            _iterations = new PrimitiveList<uint>(memoryAllocator);
            _batch = new EventBatchData(_columns);
        }

        public IReadOnlyList<IColumn> Columns => _columns;

        public int Count => _weights.Count;

        /// <summary>The produced rows as an <see cref="EventBatchData"/>, for use as the right side of a join condition.</summary>
        public EventBatchData Batch => _batch;

        public PrimitiveList<int> Weights => _weights;

        public PrimitiveList<uint> Iterations => _iterations;

        public void CommitRow(int weight, uint iteration)
        {
            _weights.Add(weight);
            _iterations.Add(iteration);
        }

        public void CommitRows(int count, int weight, uint iteration)
        {
            _weights.InsertStaticRange(_weights.Count, weight, count);
            _iterations.InsertStaticRange(_iterations.Count, iteration, count);
        }

        /// <summary>Clears the buffer so it can be reused for the next input row.</summary>
        public void Clear()
        {
            for (int i = 0; i < _columns.Length; i++)
            {
                _columns[i].Clear();
            }
            _weights.Clear();
            _iterations.Clear();
        }

        /// <summary>
        /// Wraps the current buffers as a batch and hands off ownership. The buffer must not
        /// be used afterwards.
        /// </summary>
        public EventBatchWeighted ToBatch()
        {
            return new EventBatchWeighted(_weights, _iterations, _batch);
        }

        public void Dispose()
        {
            for (int i = 0; i < _columns.Length; i++)
            {
                _columns[i].Dispose();
            }
            _weights.Dispose();
            _iterations.Dispose();
        }
    }
}
