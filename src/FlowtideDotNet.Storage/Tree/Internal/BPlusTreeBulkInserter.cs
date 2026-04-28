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

using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace FlowtideDotNet.Storage.Tree.Internal
{
    internal readonly struct ExternalKeyComparer<T, TKeyContainer, TComparer> : IComparer<int>
        where TKeyContainer : IKeyContainer<T>
        where TComparer : IBplusTreeComparer<T, TKeyContainer>
    {
        private readonly T[] _keys;
        private readonly TComparer _comparer;

        public ExternalKeyComparer(T[] keys, TComparer comparer)
        {
            _keys = keys;
            _comparer = comparer;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Compare(int x, int y)
        {
            var cmp = _comparer.CompareTo(_keys[x], _keys[y]);
            if (cmp == 0)
            {
                return x.CompareTo(y);
            }
            return cmp;
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
        private int[] _deletePositions = Array.Empty<int>();
        private int[] _lookupBuffer = Array.Empty<int>();
        List<BPlusTree<K, V, TKeyContainer, TValueContainer>.LeafBatchMapping> _mappings;
        private K[]? _keys;
        private V[]? _values;
        private readonly List<BPlusTree<K, V, TKeyContainer, TValueContainer>.LeafBatchMapping> _requireSplitMappings;
        List<BPlusTree<K, V, TKeyContainer, TValueContainer>.LeafBatchMapping> _requireMergeMappings;

        public int LeafHitCount => _mappings.Count;

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


        public async ValueTask ApplyBatch<TMutator>(K[] keys, V[] values, int keyLength, int[] sortedIndices, TMutator mutator)
            where TMutator : IRowMutator<K, V>
        {
            await StartBatch(keys, values, keyLength, sortedIndices);
            await MutateBatch(mutator);
            await ApplySplitsAndMerges();
        }

        public int[] SortAndGetIndices(K[] keys, int keyLength)
        {
            if (keyLength > _sortedIndices.Length)
            {
                _sortedIndices = new int[keyLength];
            }
            var keyComparer = _tree.m_keyComparer;
            for (int i = 0; i < keyLength; i++)
            {
                _sortedIndices[i] = i;
            }
            var indicesSpan = _sortedIndices.AsSpan().Slice(0, keyLength);
            indicesSpan.Sort(new ExternalKeyComparer<K, TKeyContainer, IBplusTreeComparer<K, TKeyContainer>>(keys, keyComparer));
            return _sortedIndices;
        }

        public ValueTask StartBatch(K[] keys, V[] values, int keyLength, int[] sortedIndices)
        {
            _keys = keys;
            _values = values;
            _sortedIndices = sortedIndices;
            _mappings.Clear();
            if (keyLength > _insertSortedIndices.Length)
            {
                _insertSortedIndices = new int[keyLength];
                _insertTargetPositions = new int[keyLength];
                _deletePositions = new int[keyLength];
                _lookupBuffer = new int[keyLength];
            }
            return _tree.RouteBatchRootAsync(keys, keyLength, sortedIndices, _tree.m_keyComparer, _mappings);
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
                _deletePositions = new int[keyLength];
                _lookupBuffer = new int[keyLength];
            }

            // Sort the keys and values by keys
            var keyComparer = _tree.m_keyComparer;

            for (int i = 0; i < keyLength; i++)
            {
                _sortedIndices[i] = i;
            }

            var indicesSpan = _sortedIndices.AsSpan().Slice(0, keyLength);
            indicesSpan.Sort(new ExternalKeyComparer<K, TKeyContainer, IBplusTreeComparer<K, TKeyContainer>>(keys,keyComparer));

            return _tree.RouteBatchRootAsync(keys, keyLength, _sortedIndices, _tree.m_keyComparer, _mappings);
        }

        public async ValueTask MutateBatch<TMutator>(TMutator mutator)
            where TMutator : IRowMutator<K, V>
        {
            for (int i = 0; i < _mappings.Count; i++)
            {
                var mapping = _mappings[i];
                var leafTask = _tree.m_stateClient.GetValue(mapping.LeafId);
                var leaf = (leafTask.IsCompletedSuccessfully ? leafTask.Result : await leafTask)
                            as LeafNode<K, V, TKeyContainer, TValueContainer>;
                Debug.Assert(leaf != null, "Expected leaf node");
                LeafMutateFromBatch(leaf, mutator, in mapping, _tree.m_keyComparer);
                leaf.Return();
            }
        }

        public async ValueTask ApplySplitsAndMerges()
        {
            Debug.Assert(_keys != null);

            // at the start we just split once, not multi splits, this can also be improved later to do X splits directly.
            for (int i = 0; i < _requireSplitMappings.Count; i++)
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
                var map = _requireMergeMappings[i];
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

            var leafCount = leaf.keys.Count;
            int insertCounter = 0;
            int deleteCounter = 0;
            int insertOffset = 0;

            // Track the previous key's state for duplicate detection.
            // Since keys are sorted, duplicates are always adjacent.
            int prevSortedKeyIndex = -1;
            bool prevWasPendingInsert = false;
            bool prevWasPendingDelete = false;
            // Whether the key physically exists in the original leaf (before
            // deferred modifications). Needed to correctly cancel pending inserts:
            // if the key was never in the leaf, canceling is a no-op on the leaf.
            bool prevKeyExistsInLeaf = false;

            for (int i = 0; i < mapping.Length; i++)
            {
                var keyIndex = _sortedIndices[mapping.Offset + i];
                var key = _keys[keyIndex];

                // Detect duplicate: current key equals previous key in sorted order.
                if (prevSortedKeyIndex >= 0
                    && searchComparer.CompareTo(key, _keys[prevSortedKeyIndex]) == 0)
                {
                    if (prevWasPendingInsert)
                    {
                        // The previous duplicate queued a pending insert.
                        // Process this as an update to that pending insert.
                        Debug.Assert(insertCounter > 0);
                        var prevInsertKeyIndex = _insertSortedIndices[insertCounter - 1];
                        var pendingValue = _values[prevInsertKeyIndex];
                        var operation = mutator.Process(key, true, in pendingValue, ref _values[keyIndex]);

                        if (operation == GenericWriteOperation.Upsert)
                        {
                            // Replace the pending insert's key index so InsertFrom uses the new value
                            _insertSortedIndices[insertCounter - 1] = keyIndex;
                        }
                        else if (operation == GenericWriteOperation.Delete)
                        {
                            // Cancel the pending insert
                            insertCounter--;
                            prevWasPendingInsert = false;

                            if (prevKeyExistsInLeaf)
                            {
                                insertOffset--;
                                prevWasPendingDelete = true;
                            }
                            else
                            {
                                // Key was never in the leaf.
                                prevWasPendingDelete = false;
                            }
                        }
                        // None: keep the previous pending insert unchanged
                    }
                    else if (prevWasPendingDelete)
                    {
                        Debug.Assert(deleteCounter > 0);
                        var operation = mutator.Process(key, false, default!, ref _values[keyIndex]);

                        if (operation == GenericWriteOperation.Upsert)
                        {
                            deleteCounter--;
                            insertOffset++;

                            leaf.UpdateValueAt(previousIndex, _values[keyIndex]);
                            prevWasPendingInsert = false;
                            prevWasPendingDelete = false;
                        }
                    }
                    else if (prevKeyExistsInLeaf)
                    {
                        // Key exists in the original leaf and was updated/no-oped.
                        // Process as exists=true with the current leaf value.
                        var existingValue = leaf.values.Get(previousIndex);
                        var operation = mutator.Process(key, true, in existingValue, ref _values[keyIndex]);

                        if (operation == GenericWriteOperation.Upsert)
                        {
                            leaf.UpdateValueAt(previousIndex, _values[keyIndex]);
                        }
                        else if (operation == GenericWriteOperation.Delete)
                        {
                            _deletePositions[deleteCounter++] = previousIndex;
                            insertOffset--;
                            prevWasPendingDelete = true;
                        }
                    }
                    else
                    {
                        var operation = mutator.Process(key, false, default!, ref _values[keyIndex]);

                        if (operation == GenericWriteOperation.Upsert)
                        {
                            _insertSortedIndices[insertCounter] = keyIndex;
                            _insertTargetPositions[insertCounter] = previousIndex + insertOffset;
                            insertCounter++;
                            prevWasPendingInsert = true;
                        }
                    }

                    prevSortedKeyIndex = keyIndex;
                    continue;
                }

                // check if we are at the end of the leaf
                // if so we can just append the remaining keys to the end of the leaf without searching
                if (previousIndex == leafCount)
                {
                    // This means all the remaining keys in the batch are greater than the keys in the leaf, so we can just append them to the end of the leaf without searching
                    var operation = mutator.Process(key, false, default!, ref _values[keyIndex]);
                    if (operation == GenericWriteOperation.Upsert)
                    {
                        // Insert the key and value into the leaf at the correct position
                        _insertSortedIndices[insertCounter] = keyIndex;
                        _insertTargetPositions[insertCounter] = leafCount + insertOffset;
                        insertCounter++;
                        previousIndex = leafCount;
                        prevWasPendingInsert = true;
                    }
                    else
                    {
                        prevWasPendingInsert = false;
                    }
                    prevWasPendingDelete = false;
                    prevKeyExistsInLeaf = false;
                    prevSortedKeyIndex = keyIndex;
                    continue;
                }

                var leafIndex = searchComparer.FindIndex(key, leaf.keys);

                if (leafIndex < 0)
                {
                    leafIndex = ~leafIndex;
                    previousIndex = leafIndex;
                    var operation = mutator.Process(key, false, default!, ref _values[keyIndex]);

                    if (operation == GenericWriteOperation.Upsert)
                    {
                        _insertSortedIndices[insertCounter] = keyIndex;
                        _insertTargetPositions[insertCounter] = leafIndex + insertOffset;
                        insertCounter++;
                        prevWasPendingInsert = true;
                    }
                    else
                    {
                        prevWasPendingInsert = false;
                    }
                    prevWasPendingDelete = false;
                    prevKeyExistsInLeaf = false;
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
                        prevWasPendingInsert = false;
                        prevWasPendingDelete = false;
                    }
                    else if (operation == GenericWriteOperation.Delete)
                    {
                        // Add the leaf index for deletion
                        _deletePositions[deleteCounter++] = leafIndex;

                        // Since we are deleting an element, the insert offset needs to be decreased by one, because the leaf will have one less element after the deletion
                        insertOffset--;
                        prevWasPendingInsert = false;
                        prevWasPendingDelete = true;
                    }
                    else
                    {
                        prevWasPendingInsert = false;
                        prevWasPendingDelete = false;
                    }
                    prevKeyExistsInLeaf = true;
                }
                prevSortedKeyIndex = keyIndex;
            }

            if (deleteCounter > 0)
            {
                var deleteSpan = _deletePositions.AsSpan(0, deleteCounter);
                leaf.keys.DeleteBatch(deleteSpan);
                leaf.values.DeleteBatch(deleteSpan);
            }

            if (insertCounter > 0)
            {
                var insertSortedIndicesSpan = _insertSortedIndices.AsSpan(0, insertCounter);
                var insertTargetPositionsSpan = _insertTargetPositions.AsSpan(0, insertCounter);
                var lookupBufferSpan = _lookupBuffer.AsSpan(0, insertCounter);
                leaf.keys.InsertFrom(_keys, insertSortedIndicesSpan, insertTargetPositionsSpan, lookupBufferSpan);
                leaf.values.InsertFrom(_values, insertSortedIndicesSpan, insertTargetPositionsSpan);
            }

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
