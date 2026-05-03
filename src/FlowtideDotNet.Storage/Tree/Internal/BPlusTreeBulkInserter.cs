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
using System.Security.Cryptography;

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
        private int[] _prefixSumBuffer = Array.Empty<int>();
        private SplitBoundary[] _splitPointsBuffer = Array.Empty<SplitBoundary>();
        private readonly List<BPlusTree<K, V, TKeyContainer, TValueContainer>.LeafBatchMapping> _mappings;
        private K[]? _keys;
        private V[]? _values;
        private readonly List<BPlusTree<K, V, TKeyContainer, TValueContainer>.LeafBatchMapping> _requireSplitMappings;
        private readonly List<BPlusTree<K, V, TKeyContainer, TValueContainer>.LeafBatchMapping> _requireMergeMappings;
        private int _totalBatchByteSize;

        public int LeafHitCount => _mappings.Count;

        public BPlusTreeBulkInserter(BPlusTree<K, V, TKeyContainer, TValueContainer> tree)
        {
            _tree = tree;
            _mappings = new List<BPlusTree<K, V, TKeyContainer, TValueContainer>.LeafBatchMapping>();
            _requireSplitMappings = new List<BPlusTree<K, V, TKeyContainer, TValueContainer>.LeafBatchMapping>();
            _requireMergeMappings = new List<BPlusTree<K, V, TKeyContainer, TValueContainer>.LeafBatchMapping>();
        }

        public async ValueTask ApplyBatch<TMutator>(K[] keys, V[] values, int keyLength, TMutator mutator, int totalBatchByteSize)
            where TMutator : IRowMutator<K, V>
        {
            _totalBatchByteSize = totalBatchByteSize;
            await StartBatch(keys, values, keyLength);
            await MutateBatch(mutator);
            await ApplySplitsAndMerges();
        }


        public async ValueTask ApplyBatch<TMutator>(K[] keys, V[] values, int keyLength, int[] sortedIndices, TMutator mutator, int totalBatchByteSize)
            where TMutator : IRowMutator<K, V>
        {
            _totalBatchByteSize = totalBatchByteSize;
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

        private ValueTask StartBatch(K[] keys, V[] values, int keyLength, int[] sortedIndices)
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

        private ValueTask StartBatch(K[] keys, V[] values, int keyLength)
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

        private async ValueTask MutateBatch<TMutator>(TMutator mutator)
            where TMutator : IRowMutator<K, V>
        {
            for (int i = 0; i < _mappings.Count; i++)
            {
                var mapping = _mappings[i];
                var leafTask = _tree.m_stateClient.GetValue(mapping.LeafId);
                var leaf = (leafTask.IsCompletedSuccessfully ? leafTask.Result : await leafTask)
                            as LeafNode<K, V, TKeyContainer, TValueContainer>;
                Debug.Assert(leaf != null, "Expected leaf node");
                var insertCount = LeafMutateFromBatch(leaf, mutator, in mapping, _tree.m_keyComparer);

                if (insertCount > 0)
                {
                    // Do a NWay split here, data has not been inserted yet
                    // But all data is in the buffers etc
                    var leafByteSize = leaf.ByteSize;
                    var incomingSize = _prefixSumBuffer[insertCount - 1];
                    await NWaySplitAndMutate(leaf, mutator, mapping, insertCount, leafByteSize, incomingSize, _tree.m_stateClient.Metadata!.PageSizeBytes);
                }

                leaf.Return();
            }
        }

        private async ValueTask ApplySplitsAndMerges()
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

        private int LeafMutateFromBatch<TMutator>(
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
            var byteSize = leaf.ByteSize;
            if (insertCounter > 0)
            {
                   
                var insertSortedIndicesSpan = _insertSortedIndices.AsSpan(0, insertCounter);

                var newCount = leaf.keys.Count + insertCounter;
                if (byteSize + _totalBatchByteSize > _tree.m_stateClient.Metadata!.PageSizeBytes &&
                    newCount > BPlusTree<K, V, TKeyContainer, TValueContainer>.minPageSizeBeforeSplit)
                {
                    CalculatePrefixSum(mutator, insertSortedIndicesSpan, insertCounter);
                    var expectedSize = byteSize + _prefixSumBuffer[insertCounter - 1];

                    // There will be a split
                    if (expectedSize > _tree.m_stateClient.Metadata!.PageSizeBytes)
                    {
                        return insertCounter;
                    }
                }
                

                var insertTargetPositionsSpan = _insertTargetPositions.AsSpan(0, insertCounter);
                var lookupBufferSpan = _lookupBuffer.AsSpan(0, insertCounter);
                leaf.keys.InsertFrom(_keys, insertSortedIndicesSpan, insertTargetPositionsSpan, lookupBufferSpan);
                leaf.values.InsertFrom(_values, insertSortedIndicesSpan, insertTargetPositionsSpan);
            }

            
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

            return 0;
        }

        private void CalculatePrefixSum<TRowMutator>(TRowMutator mutator, ReadOnlySpan<int> sortedLookup, int insertCount)
             where TRowMutator : IRowMutator<K, V>
        {
            if (_prefixSumBuffer.Length < insertCount)
            {
                _prefixSumBuffer = new int[insertCount];
            }
            var span = _prefixSumBuffer.AsSpan(0, insertCount);
            span.Clear();
            mutator.GetSizePrefixSum(_keys!, sortedLookup, span);
        }


        private void ApplyInserts(
            LeafNode<K, V, TKeyContainer, TValueContainer> leaf,
            int offset,
            int count)
        {
            Debug.Assert(_keys != null && _values != null);
            var insertSortedIndicesSpan = _insertSortedIndices.AsSpan(offset, count);
            var insertTargetPositionsSpan = _insertTargetPositions.AsSpan(offset, count);
            var lookupBufferSpan = _lookupBuffer.AsSpan(offset, count);
            leaf.keys.InsertFrom(_keys, insertSortedIndicesSpan, insertTargetPositionsSpan, lookupBufferSpan);
            leaf.values.InsertFrom(_values, insertSortedIndicesSpan, insertTargetPositionsSpan);
        }

        public readonly struct SplitBoundary
        {
            public readonly int LeafIndex;
            public readonly int BatchIndex;
            public readonly bool IsBatchMax;

            public SplitBoundary(int leafIndex, int batchIndex, bool isBatchMax)
            {
                LeafIndex = leafIndex;
                BatchIndex = batchIndex;
                IsBatchMax = isBatchMax;
            }
        }

        // --- 2. The Mass & Search Helpers ---

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private int CountInsertsBeforePosition(int position, int insertCounter)
        {
            int lo = 0, hi = insertCounter - 1;
            int result = 0;
            while (lo <= hi)
            {
                var mid = lo + (hi - lo) / 2;
                if (_insertTargetPositions[mid] < position)
                {
                    result = mid + 1;
                    lo = mid + 1;
                }
                else
                {
                    hi = mid - 1;
                }
            }
            return result;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private int GetCombinedMassBefore(
            LeafNode<K, V, TKeyContainer, TValueContainer> leaf,
            int leafIdx,
            int insertCounter)
        {
            int leafBytes = leafIdx > 0 ? leaf.GetByteSize(0, leafIdx - 1) : 0;
            int insertsBefore = CountInsertsBeforePosition(leafIdx, insertCounter);
            int batchBytes = insertsBefore > 0 ? _prefixSumBuffer[insertsBefore - 1] : 0;

            return leafBytes + batchBytes;
        }

        private SplitBoundary BinarySearchSplitBoundary(
            LeafNode<K, V, TKeyContainer, TValueContainer> leaf,
            int leafLo,
            int leafHi,
            int batchLo,
            int target,
            int insertCounter)
        {
            int leafAnchor = leafLo;

            int lo = leafLo;
            int hi = leafHi;
            while (lo <= hi)
            {
                int mid = lo + (hi - lo) / 2;
                int mass = GetCombinedMassBefore(leaf, mid, insertCounter);

                if (mass <= target)
                {
                    leafAnchor = mid;
                    lo = mid + 1;
                }
                else
                {
                    hi = mid - 1;
                }
            }

            int startBatchIdx = CountInsertsBeforePosition(leafAnchor, insertCounter);
            int endBatchIdx = CountInsertsBeforePosition(leafAnchor + 1, insertCounter) - 1;

            if (startBatchIdx < batchLo)
            {
                startBatchIdx = batchLo;
            }

            if (startBatchIdx <= endBatchIdx)
            {
                int leafBytesBeforeAnchor = leafAnchor > 0 ? leaf.GetByteSize(0, leafAnchor - 1) : 0;

                int batchSearchLo = startBatchIdx;
                int batchSearchHi = endBatchIdx;
                int bestBatchCut = startBatchIdx;

                while (batchSearchLo <= batchSearchHi)
                {
                    int batchMid = batchSearchLo + (batchSearchHi - batchSearchLo) / 2;
                    int currentMass = leafBytesBeforeAnchor + _prefixSumBuffer[batchMid];

                    if (currentMass <= target)
                    {
                        bestBatchCut = batchMid + 1;
                        batchSearchLo = batchMid + 1;
                    }
                    else
                    {
                        batchSearchHi = batchMid - 1;
                    }
                }

                bool isBatchMax = bestBatchCut > startBatchIdx;
                return new SplitBoundary(leafAnchor, bestBatchCut, isBatchMax);
            }

            return new SplitBoundary(leafAnchor, startBatchIdx, false);
        }

        private async ValueTask NWaySplitAndMutate<TMutator>(
            LeafNode<K, V, TKeyContainer, TValueContainer> leaf,
            TMutator mutator,
            BPlusTree<K, V, TKeyContainer, TValueContainer>.LeafBatchMapping mapping,
            int insertCount,
            int leafByteSize,
            int batchRangeSize,
            int maxPageSize)
            where TMutator : IRowMutator<K, V>
        {
            Debug.Assert(_keys != null && _values != null);

            var leafCount = leaf.keys.Count;
            var totalSize = leafByteSize + batchRangeSize;

            var numNodes = (totalSize + maxPageSize - 1) / maxPageSize;
            if (numNodes < 2) numNodes = 2;

            var maxPossibleNodes = (leafCount + insertCount) / BPlusTree<K, V, TKeyContainer, TValueContainer>.minPageSizeAfterSplit;
            if (maxPossibleNodes < 2)
            {
                throw new InvalidOperationException("Split called without possible split");
            }
            if (numNodes > maxPossibleNodes)
            {
                numNodes = maxPossibleNodes;
            }

            var targetPerNode = totalSize / numNodes;

            if (numNodes - 1 > _splitPointsBuffer.Length)
            {
                _splitPointsBuffer = new SplitBoundary[numNodes - 1];
            }

            int currentLeafLo = 0;
            int currentBatchLo = 0;

            for (int s = 0; s < numNodes - 1; s++)
            {
                var target = (s + 1) * targetPerNode;

                _splitPointsBuffer[s] = BinarySearchSplitBoundary(
                    leaf, currentLeafLo, leafCount, currentBatchLo, target, insertCount);

                int leafCut = _splitPointsBuffer[s].LeafIndex;
                int batchCut = _splitPointsBuffer[s].BatchIndex;
                bool isBatchMax = _splitPointsBuffer[s].IsBatchMax;

                int assignedCount = (leafCut - currentLeafLo) + (batchCut - currentBatchLo);
                int deficit = BPlusTree<K, V, TKeyContainer, TValueContainer>.minPageSizeAfterSplit - assignedCount;

                if (deficit > 0)
                {
                    for (int i = 0; i < deficit; i++)
                    {
                        bool canTakeLeaf = leafCut < leafCount;
                        bool canTakeBatch = batchCut < insertCount;

                        if (canTakeLeaf && canTakeBatch)
                        {

                            int batchTargetLeafIndex = _insertTargetPositions[batchCut];

                            if (batchTargetLeafIndex <= leafCut)
                            {
                                batchCut++;
                                isBatchMax = true;
                            }
                            else
                            {
                                leafCut++;
                                isBatchMax = false;
                            }
                        }
                        else if (canTakeLeaf)
                        {
                            leafCut++;
                            isBatchMax = false;
                        }
                        else if (canTakeBatch)
                        {
                            batchCut++;
                            isBatchMax = true;
                        }
                        else
                        {
                            break;
                        }
                    }

                    _splitPointsBuffer[s] = new SplitBoundary(leafCut, batchCut, isBatchMax);
                }

                currentLeafLo = _splitPointsBuffer[s].LeafIndex;
                currentBatchLo = _splitPointsBuffer[s].BatchIndex;
            }

            InternalNode<K, V, TKeyContainer>? parent = null;
            int indexInParent = 0;

            if (mapping.ParentId == -1)
            {
                var newRootId = _tree.m_stateClient.GetNewPageId();
                var emptyKeys = _tree.m_options.KeySerializer.CreateEmpty();
                parent = new InternalNode<K, V, TKeyContainer>(newRootId, emptyKeys, _tree.m_options.MemoryAllocator);
                parent.children.InsertAt(0, leaf.Id);
                _tree.m_stateClient.Metadata = _tree.m_stateClient.Metadata!.UpdateRootAndDepth(
                    newRootId, _tree.m_stateClient.Metadata.Depth + 1);
                indexInParent = 0;
            }
            else
            {
                var parentTask = _tree.m_stateClient.GetValue(mapping.ParentId);
                parent = (parentTask.IsCompletedSuccessfully ? parentTask.Result : await parentTask)
                         as InternalNode<K, V, TKeyContainer>;
                Debug.Assert(parent != null, "Expected internal node as parent");

                for (int c = 0; c < parent.children.Count; c++)
                {
                    if (parent.children[c] == leaf.Id)
                    {
                        indexInParent = c;
                        break;
                    }
                }
            }

            var allLeaves = new LeafNode<K, V, TKeyContainer, TValueContainer>[numNodes];
            allLeaves[0] = leaf;

            parent.EnterWriteLock();
            leaf.EnterWriteLock();

            var originalNext = leaf.next;

            for (int s = 0; s < numNodes - 1; s++)
            {
                var newNodeId = _tree.m_stateClient.GetNewPageId();
                var emptyKeys = _tree.m_options.KeySerializer.CreateEmpty();
                var emptyValues = _tree.m_options.ValueSerializer.CreateEmpty();
                var newLeaf = new LeafNode<K, V, TKeyContainer, TValueContainer>(newNodeId, emptyKeys, emptyValues);
                newLeaf.EnterWriteLock();

                // Slice the old leaf using the calculated boundaries
                var rangeStart = _splitPointsBuffer[s].LeafIndex;
                var rangeEnd = (s < numNodes - 2) ? _splitPointsBuffer[s + 1].LeafIndex : leafCount;
                var rangeLen = rangeEnd - rangeStart;

                newLeaf.keys.AddRangeFrom(leaf.keys, rangeStart, rangeLen);
                newLeaf.values.AddRangeFrom(leaf.values, rangeStart, rangeLen);

                newLeaf.next = (s < numNodes - 2) ? 0 : originalNext;

                if (_tree.m_usePreviousPointer)
                {
                    if (s == 0) newLeaf.previous = leaf.Id;
                    else newLeaf.previous = allLeaves[s].Id;
                }

                newLeaf.ExitWriteLock();
                allLeaves[s + 1] = newLeaf;

                int leafCut = _splitPointsBuffer[s].LeafIndex;
                int batchCut = _splitPointsBuffer[s].BatchIndex;
                K splitKey;

                if (leafCut == 0)
                {
                    int keyIndex = _insertSortedIndices[batchCut - 1];
                    splitKey = _keys[keyIndex];
                }
                else if (batchCut == 0)
                {
                    splitKey = leaf.keys.Get(leafCut - 1);
                }
                else if (_splitPointsBuffer[s].IsBatchMax)
                {
                    int keyIndex = _insertSortedIndices[batchCut - 1];
                    splitKey = _keys[keyIndex];
                }
                else
                {
                    splitKey = leaf.keys.Get(leafCut - 1);
                }

                parent.keys.Insert_Internal(indexInParent + s, splitKey);
                parent.children.InsertAt(indexInParent + s + 1, newNodeId);
            }

            for (int s = 0; s < numNodes - 1; s++)
            {
                allLeaves[s].next = allLeaves[s + 1].Id;
            }

            var keepCount = _splitPointsBuffer[0].LeafIndex;
            leaf.keys.RemoveRange(keepCount, leafCount - keepCount);
            leaf.values.RemoveRange(keepCount, leafCount - keepCount);

            
            parent.ExitWriteLock();

            if (_tree.m_usePreviousPointer && originalNext != 0)
            {
                var nextNodeTask = _tree.m_stateClient.GetValue(originalNext);
                var nextNodeObj = nextNodeTask.IsCompletedSuccessfully ? nextNodeTask.Result : await nextNodeTask;
                if (nextNodeObj is LeafNode<K, V, TKeyContainer, TValueContainer> nextNode)
                {
                    nextNode.previous = allLeaves[numNodes - 1].Id;
                    _tree.m_stateClient.AddOrUpdate(nextNode.Id, nextNode);
                    nextNode.Return();
                }
            }

            _tree.m_stateClient.AddOrUpdate(parent.Id, parent);

            var parentByteSize = parent.GetByteSize();
            if (parentByteSize > maxPageSize &&
                parent.keys.Count > BPlusTree<K, V, TKeyContainer, TValueContainer>.minPageSizeBeforeSplit)
            {
                _requireSplitMappings.Add(mapping);
            }

            if (mapping.ParentId != -1)
            {
                parent.Return();
            }

            var batchStart = 0;

            currentLeafLo = 0;

            for (int s = 0; s < numNodes; s++)
            {
                int batchEnd = (s < numNodes - 1) ? _splitPointsBuffer[s].BatchIndex : insertCount; // mapping.Length here is insertCounter
                var subLength = batchEnd - batchStart;

                if (subLength > 0)
                {
                    int offset = batchStart;

                    if (currentLeafLo > 0 || batchStart > 0)
                    {
                        for (int i = 0; i < subLength; i++)
                        {
                            _insertTargetPositions[offset + i] -= (currentLeafLo);
                        }
                    }

                    ApplyInserts(allLeaves[s], offset, subLength);
                }

                _tree.m_stateClient.AddOrUpdate(allLeaves[s].Id, allLeaves[s]);

                batchStart = batchEnd;
                if (s < numNodes - 1)
                {
                    currentLeafLo = _splitPointsBuffer[s].LeafIndex;
                }
            }

            leaf.ExitWriteLock();
        }
    }
}
