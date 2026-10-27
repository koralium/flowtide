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

using FlowtideDotNet.Connector.DeltaLake.Internal.Deletes;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.Sort;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Tree;
using System.Diagnostics;

namespace FlowtideDotNet.Connector.DeltaLake.Internal
{
    // Puts input batches into the sink's temporary tree
    internal sealed class TemporaryTreeIntake
    {
        private readonly IBPlusTree<ColumnRowReference, int, ColumnKeyStorageContainer, PrimitiveListValueContainer<int>> _tree;
        private readonly IBPlusTreeBulkInserter<ColumnRowReference, int, ColumnKeyStorageContainer, PrimitiveListValueContainer<int>> _inserter;
        private readonly int _outputLength;
        private readonly bool _bulk;
        private readonly BatchSorter _sorter;
        private readonly IColumn[] _sortColumns;
        private ColumnRowReference[] _keys = Array.Empty<ColumnRowReference>();
        private int[] _values = Array.Empty<int>();
        private int[] _order = Array.Empty<int>();
        private int[] _tags = Array.Empty<int>();
        private long _bulkRows;
        private long _perRowRows;
        private long _wrapperRows;
        private long _intakeTicks;

        public TemporaryTreeIntake(IBPlusTree<ColumnRowReference, int, ColumnKeyStorageContainer, PrimitiveListValueContainer<int>> tree, int outputLength, DeleteLimits limits)
        {
            _tree = tree;
            _inserter = tree.CreateBulkInserter();
            _outputLength = outputLength;
            _bulk = limits.BulkIntake;
            // Ascending with nulls first, as the tree comparer
            _sorter = new BatchSorter(outputLength);
            _sortColumns = new IColumn[outputLength];
        }

        // Applied rows per path, tests poll them from another thread
        public long BulkRows => Interlocked.Read(ref _bulkRows);

        public long PerRowRows => Interlocked.Read(ref _perRowRows);

        // Rows of batches holding a wrapper column
        public long WrapperRows => Interlocked.Read(ref _wrapperRows);

        public long IntakeTicks => Interlocked.Read(ref _intakeTicks);

        public async Task Apply(EventBatchData batch, PrimitiveList<int> weights)
        {
            var count = weights.Count;
            if (count == 0)
            {
                return;
            }
            var start = Stopwatch.GetTimestamp();
            try
            {
                if (!_bulk || batch.Columns.Count != _outputLength)
                {
                    await PerRow(batch, weights);
                    Interlocked.Add(ref _perRowRows, count);
                }
                else
                {
                    await Bulk(batch, weights, count);
                    Interlocked.Add(ref _bulkRows, count);
                }
                if (HasWrapper(batch))
                {
                    Interlocked.Add(ref _wrapperRows, count);
                }
            }
            finally
            {
                Interlocked.Add(ref _intakeTicks, Stopwatch.GetTimestamp() - start);
            }
        }

        private async Task Bulk(EventBatchData batch, PrimitiveList<int> weights, int count)
        {
            if (_keys.Length < count)
            {
                _keys = new ColumnRowReference[count];
                _values = new int[count];
            }
            // Every key on the same batch, at its own row
            for (int i = 0; i < count; i++)
            {
                _keys[i] = new ColumnRowReference() { referenceBatch = batch, RowIndex = i };
                _values[i] = weights[i];
            }
            var bytes = batch.GetByteSize() + count * sizeof(int);
            try
            {
                Sort(batch, count);
                // The presorted call keeps our arrays as its buffers
                await _inserter.ApplyBatch(_keys, _values, count, _order, _tags, new IntakeMutator(), bytes);
            }
            finally
            {
                // Keeps no batch columns alive
                Array.Clear(_keys, 0, count);
                Array.Clear(_sortColumns);
            }
        }

        // Identity order, then the order and duplicate tags
        private void Sort(EventBatchData batch, int count)
        {
            if (_order.Length < count)
            {
                _order = new int[count];
                _tags = new int[count];
            }
            for (int i = 0; i < count; i++)
            {
                _order[i] = i;
            }
            for (int i = 0; i < _outputLength; i++)
            {
                _sortColumns[i] = batch.Columns[i];
            }
            var order = _order.AsSpan(0, count);
            var tags = _tags.AsSpan(0, count);
            _sorter.SortDataWithTags(_sortColumns, ref order, ref tags);
        }

        // Today's loop, one read-modify-write per row
        private async Task PerRow(EventBatchData batch, PrimitiveList<int> weights)
        {
            for (int i = 0; i < weights.Count; i++)
            {
                var rowRef = new ColumnRowReference() { referenceBatch = batch, RowIndex = i };
                await _tree.RMWNoResult(in rowRef, weights[i], (input, current, exists) =>
                {
                    if (exists)
                    {
                        var newWeight = current + input;
                        if (newWeight == 0)
                        {
                            return (0, GenericWriteOperation.Delete);
                        }
                        return (newWeight, GenericWriteOperation.Upsert);
                    }
                    return (input, GenericWriteOperation.Upsert);
                });
            }
        }

        private static bool HasWrapper(EventBatchData batch)
        {
            for (int i = 0; i < batch.Columns.Count; i++)
            {
                if (batch.Columns[i] is not Column)
                {
                    return true;
                }
            }
            return false;
        }

        // Sums the weights, a zero sum removes the key
        private readonly struct IntakeMutator : IRowMutator<ColumnRowReference, int>
        {
            public void GetSizePrefixSum(ColumnRowReference[] keys, ReadOnlySpan<int> indices, Span<int> sizes)
            {
                if (indices.Length == 0)
                {
                    return;
                }
                var columns = keys[indices[0]].referenceBatch.GetColumns_Unsafe();
                for (int i = 0; i < columns.Length; i++)
                {
                    columns[i].GetPrefixSumByteSizes(indices, sizes);
                }
                var valueBytes = 0;
                for (int i = 0; i < indices.Length; i++)
                {
                    valueBytes += sizeof(int);
                    sizes[i] += valueBytes;
                }
            }

            public GenericWriteOperation Process(ColumnRowReference key, bool exists, in int existingData, ref int incomingData, int sortedIndex)
            {
                if (exists)
                {
                    incomingData += existingData;
                    return incomingData == 0 ? GenericWriteOperation.Delete : GenericWriteOperation.Upsert;
                }
                return incomingData == 0 ? GenericWriteOperation.None : GenericWriteOperation.Upsert;
            }
        }
    }
}
