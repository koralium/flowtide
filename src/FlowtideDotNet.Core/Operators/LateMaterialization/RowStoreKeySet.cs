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
using FlowtideDotNet.Core.ColumnStore.Sort;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Core.Operators.Normalization;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.Tree;

namespace FlowtideDotNet.Core.Operators.LateMaterialization
{
    // A tree of keys alone, filled a batch at a time
    internal sealed class RowStoreKeySet
    {
        private readonly IBPlusTree<ColumnRowReference, ColumnRowReference, NormalizeKeyStorage, NormalizeValueStorage> _tree;
        private readonly IBPlusTreeBulkInserter<ColumnRowReference, ColumnRowReference, NormalizeKeyStorage, NormalizeValueStorage> _inserter;
        private readonly BatchSorter _sorter;
        private ColumnRowReference[] _keys = Array.Empty<ColumnRowReference>();
        private ColumnRowReference[] _values = Array.Empty<ColumnRowReference>();
        private int[] _sorted = Array.Empty<int>();
        private int[] _tags = Array.Empty<int>();
        private int[] _sizes = Array.Empty<int>();

        private RowStoreKeySet(IBPlusTree<ColumnRowReference, ColumnRowReference, NormalizeKeyStorage, NormalizeValueStorage> tree, int keyColumns)
        {
            _tree = tree;
            _inserter = tree.CreateBulkInserter();
            _sorter = new BatchSorter(keyColumns);
        }

        public static async Task<RowStoreKeySet> CreateAsync(IStateManagerClient stateManagerClient, string name, int keyColumns, IMemoryAllocator memoryAllocator)
        {
            var order = new List<int>();
            for (int i = 0; i < keyColumns; i++)
            {
                order.Add(i);
            }
            var tree = await stateManagerClient.GetOrCreateTree(name,
                new BPlusTreeOptions<ColumnRowReference, ColumnRowReference, NormalizeKeyStorage, NormalizeValueStorage>()
                {
                    Comparer = new RowStoreHostKeyComparer(order),
                    KeySerializer = new NormalizeKeyStorageSerializer(order, memoryAllocator),
                    ValueSerializer = new NormalizeValueSerializer(new List<int>(), memoryAllocator),
                    UseByteBasedPageSizes = true,
                    MemoryAllocator = memoryAllocator
                });
            return new RowStoreKeySet(tree, keyColumns);
        }

        // Adds the keys of a batch of key columns
        public ValueTask AddAsync(EventBatchData keys, int count)
        {
            if (count == 0)
            {
                return ValueTask.CompletedTask;
            }
            int distinct = Sort(keys, count);
            if (distinct == 0)
            {
                return ValueTask.CompletedTask;
            }
            return _inserter.ApplyBatch(_keys, _values, distinct, _sorted, _tags, new AddMutator(), _sizes[distinct - 1]);
        }

        // One sort, each key once, and the bytes the keys take
        private int Sort(EventBatchData keys, int count)
        {
            if (_keys.Length < count)
            {
                int size = Math.Max(count, 1024);
                _keys = new ColumnRowReference[size];
                _values = new ColumnRowReference[size];
                _sorted = new int[size];
                _tags = new int[size];
                _sizes = new int[size];
            }
            for (int i = 0; i < count; i++)
            {
                _keys[i] = new ColumnRowReference() { referenceBatch = keys, RowIndex = i };
                _values[i] = new ColumnRowReference() { referenceBatch = keys, RowIndex = i };
                _sorted[i] = i;
                _tags[i] = 0;
            }
            if (count > 1)
            {
                var sortedSpan = _sorted.AsSpan(0, count);
                var tagSpan = _tags.AsSpan(0, count);
                _sorter.SortDataWithTags((IColumn[])keys.Columns, ref sortedSpan, ref tagSpan);
            }
            int distinct = 0;
            int previous = -1;
            for (int i = 0; i < count; i++)
            {
                int row = _sorted[i];
                int tag = _tags[i];
                if (tag == previous)
                {
                    continue;
                }
                previous = tag;
                _sorted[distinct] = row;
                _tags[distinct] = distinct;
                distinct++;
            }
            if (distinct > 0)
            {
                var indices = _sorted.AsSpan(0, distinct);
                var sizes = _sizes.AsSpan(0, distinct);
                sizes.Clear();
                for (int k = 0; k < keys.Columns.Count; k++)
                {
                    keys.Columns[k].GetPrefixSumByteSizes(indices, sizes);
                }
            }
            return distinct;
        }

        // A key that is there stays as it is
        private readonly struct AddMutator : IRowMutator<ColumnRowReference, ColumnRowReference>
        {
            public void GetSizePrefixSum(ColumnRowReference[] keys, ReadOnlySpan<int> indices, Span<int> sizes)
            {
                var columns = keys[indices[0]].referenceBatch.Columns;
                for (int k = 0; k < columns.Count; k++)
                {
                    columns[k].GetPrefixSumByteSizes(indices, sizes);
                }
            }

            public GenericWriteOperation Process(ColumnRowReference key, bool exists, in ColumnRowReference existingData, ref ColumnRowReference incomingData, int sortedIndex)
            {
                return exists ? GenericWriteOperation.None : GenericWriteOperation.Upsert;
            }
        }

        public IBPlusTreeIterator<ColumnRowReference, ColumnRowReference, NormalizeKeyStorage, NormalizeValueStorage> CreateIterator()
        {
            return _tree.CreateIterator();
        }

        public ValueTask Clear()
        {
            return _tree.Clear();
        }
    }
}
