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

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Storage.Tree.Internal
{
    internal readonly struct ExternalKeyComparer<T, TKeyContainer> : IComparer<int>
        where TKeyContainer : IKeyContainer<T>
    {
        private readonly T[] _keys;
        private readonly IBplusTreeComparer<T, TKeyContainer> _comparer;

        public ExternalKeyComparer(T[] keys, IBplusTreeComparer<T, TKeyContainer> comparer)
        {
            _keys = keys;
            _comparer = comparer;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Compare(int x, int y)
        {
            return _comparer.CompareTo(_keys[x], _keys[y]);
        }
    }

    internal class BPlusTreeBulkInserter<K, V, TKeyContainer, TValueContainer> 
        : IBPlusTreeBulkInserter<K, V, TKeyContainer, TValueContainer>
        where TKeyContainer : IKeyContainer<K>
        where TValueContainer : IValueContainer<V>
    {
        private readonly BPlusTree<K, V, TKeyContainer, TValueContainer> _tree;
        private int[] _sortedIndices = Array.Empty<int>();
        private int[] _insertSortedIndices = Array.Empty<int>();
        private int[] _insertTargetPositions = Array.Empty<int>();
        private int _keyLength;
        List<BPlusTree<K, V, TKeyContainer, TValueContainer>.LeafBatchMapping> _mappings;
        private K[]? _keys;
        private V[]? _values;
        List<BPlusTree<K, V, TKeyContainer, TValueContainer>.LeafBatchMapping> _requireSplitMappings;
        List<BPlusTree<K, V, TKeyContainer, TValueContainer>.LeafBatchMapping> _requireMergeMappings;
        private int singleElementLeaf = 0;
        private int MultiElementLeaf = 0;

        public BPlusTreeBulkInserter(BPlusTree<K, V, TKeyContainer, TValueContainer> tree)
        {
            _tree = tree;
            _mappings = new List<BPlusTree<K, V, TKeyContainer, TValueContainer>.LeafBatchMapping>();
            _requireSplitMappings = new List<BPlusTree<K, V, TKeyContainer, TValueContainer>.LeafBatchMapping>();
            _requireMergeMappings = new List<BPlusTree<K, V, TKeyContainer, TValueContainer>.LeafBatchMapping>();
        }

        public async ValueTask ApplyBatch<TMutator>(K[] keys, V[] values, int keyLength, TMutator mutator)
            where TMutator : IRowMutator<K, V>
        {
            await StartBatch(keys, values, keyLength);
            await MutateBatch(mutator);
            await ApplySplitsAndMerges();
        }

        public ValueTask StartBatch(K[] keys, V[] values, int keyLength)
        {
            _keys = keys;
            _values = values;
            _mappings.Clear();
            if (keyLength > _sortedIndices.Length)
            {
                _sortedIndices = new int[keyLength];
                _insertSortedIndices = new int[keyLength];
                _insertTargetPositions = new int[keyLength];
            }
            _keyLength = keyLength;

            // Sort the keys and values by keys
            var keyComparer = _tree.m_keyComparer;

            for (int i = 0; i < keyLength; i++)
            {
                _sortedIndices[i] = i;
            }

            var indicesSpan = _sortedIndices.AsSpan().Slice(0, _keyLength);
            indicesSpan.Sort(new ExternalKeyComparer<K, TKeyContainer>(keys,keyComparer));

            return _tree.RouteBatchRootAsync(keys, keyLength, _sortedIndices, _tree.m_keyComparer, _mappings);
        }

        public async ValueTask MutateBatch<TMutator>(TMutator mutator)
            where TMutator : IRowMutator<K, V>
        {
            foreach (var map in _mappings)
            {
                var leafTask = _tree.m_stateClient.GetValue(map.LeafId);
                var leaf = (leafTask.IsCompletedSuccessfully ? leafTask.Result : await leafTask)
                            as LeafNode<K, V, TKeyContainer, TValueContainer>;

                LeafMutateFromBatch(leaf!, mutator, in map, _tree.m_keyComparer);
                leaf.Return();
            }
        }

        public async ValueTask ApplySplitsAndMerges()
        {
            // at the start we just split once, not multi splits, this can also be improved later to do X splits directly.
            for(int i = 0; i < _requireSplitMappings.Count; i++)
            {
                var map = _requireSplitMappings[i];
                var keyIndex = _sortedIndices[map.Offset];
                var firstKey = _keys[keyIndex];

                await _tree.RMWNoResult(firstKey, default, static (input, current, found) =>
                {
                    return (default, GenericWriteOperation.None);
                });
            }

            for (int i = 0; i < _requireMergeMappings.Count; i++)
            {
                var map = _requireSplitMappings[i];
                var keyIndex = _sortedIndices[map.Offset];
                var firstKey = _keys[keyIndex];

                await _tree.RMWNoResult(firstKey, default, static (input, current, found) =>
                {
                    return (default, GenericWriteOperation.None);
                });
            }

            _requireSplitMappings.Clear();
            _requireMergeMappings.Clear();
        }

        private void LeafMutateFromBatch<TMutator>(
            LeafNode<K, V, TKeyContainer, TValueContainer> leaf,
            TMutator mutator,
            in BPlusTree<K, V, TKeyContainer, TValueContainer>.LeafBatchMapping mapping,
            IBplusTreeComparer<K, TKeyContainer> searchComparer)
            where TMutator : IRowMutator<K, V>
        {
            Debug.Assert(_values != null && _keys != null);
            int previousIndex = -1;
            if (mapping.Length == 1)
            {
                singleElementLeaf++;
            }
            else
            {
                MultiElementLeaf++;
            }

            var leafCount = leaf.keys.Count;
            int insertCounter = 0;
            for (int i = 0; i < mapping.Length; i++)
            {
                var keyIndex = _sortedIndices[mapping.Offset + i];
                var key = _keys[keyIndex];

                
                // check if we are at the end of the leaf
                // // if so we can just append the remaining keys to the end of the leaf without searching
                if ((previousIndex) == leafCount)
                {
                    // This means all the remaining keys in the batch are greater than the keys in the leaf, so we can just append them to the end of the leaf without searching
                    var operation = mutator.Process(key, false, default!, ref _values[keyIndex]);
                    if (operation == GenericWriteOperation.Upsert)
                    {
                        // Insert the key and value into the leaf at the correct position
                        _insertSortedIndices[insertCounter] = keyIndex;
                        _insertTargetPositions[insertCounter] = leafCount;
                        insertCounter++;
                        //leaf.InsertAt(key, _values[keyIndex], leaf.keys.Count);
                        previousIndex = leafCount;
                    }
                    continue;
                }

                var leafIndex = searchComparer.FindIndex(key, leaf.keys);
                
                // Later on all this code can be optimized to only modify the leaf data twice, once for insert and once for delete
                // But for simplicity we will just do it in place for now, and optimize later
                if (leafIndex < 0)
                {
                    leafIndex = ~leafIndex;
                    previousIndex = leafIndex;
                    var operation = mutator.Process(key, false, default!, ref _values[keyIndex]);

                    if (operation == GenericWriteOperation.Upsert)
                    {
                        _insertSortedIndices[insertCounter] = keyIndex;
                        _insertTargetPositions[insertCounter] = leafIndex;
                        insertCounter++;
                        // Insert the key and value into the leaf at the correct position
                        //leaf.InsertAt(key, _values[keyIndex], leafIndex);
                    }
                }
                else
                {
                    previousIndex = leafIndex;
                    var existingValue = leaf.values.Get(leafIndex);
                    var operation = mutator.Process(key, true, in existingValue, ref _values[keyIndex]);

                    if (operation == GenericWriteOperation.Upsert)
                    {
                        // Update the value in the leaf
                        leaf.UpdateValueAt(leafIndex, _values[keyIndex]);
                    }
                    else if (operation == GenericWriteOperation.Delete)
                    {
                        // Remove the key and value from the leaf
                        throw new NotImplementedException();
                        //leaf.DeleteAt(leafIndex);
                    }
                }
            }

            if (insertCounter > 0)
            {
                leaf.keys.InsertFrom(_keys, _insertSortedIndices.AsSpan(0, insertCounter), _insertTargetPositions.AsSpan(0, insertCounter));
                leaf.values.InsertFrom(_values, _insertSortedIndices.AsSpan(0, insertCounter), _insertTargetPositions.AsSpan(0, insertCounter));
            }
            //if (leaf.keys is PrimitiveListKeyContainer<K> list)
            //{

            //}

            var byteSize = leaf.ByteSize;
            if (byteSize > _tree.m_stateClient.Metadata!.PageSizeBytes &&
                leaf.keys.Count > BPlusTree<K, V, TKeyContainer, TValueContainer>.minPageSizeBeforeSplit)
            {
                // Leaf is too big
                _requireSplitMappings.Add(mapping);
            }
            else if (
                leaf.Id != _tree.m_stateClient.Metadata!.Root &&
                (byteSize <= _tree.byteMinSize || leaf.keys.Count < BPlusTree<K, V, TKeyContainer, TValueContainer>.minPageSize))
            {
                _requireMergeMappings.Add(mapping);
            }
        }

    }
}
