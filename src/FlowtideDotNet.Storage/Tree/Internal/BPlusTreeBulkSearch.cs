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
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Storage.Tree.Internal
{

    internal class BPlusTreeBulkSearch<K, V, TKeyContainer, TValueContainer, TComparer> : IBplusTreeBulkSearch<K, V, TKeyContainer, TValueContainer, TComparer>
        where TKeyContainer : IKeyContainer<K>
        where TValueContainer : IValueContainer<V>
        where TComparer : IBplusTreeComparer<K ,TKeyContainer>
    {
        private int[] _sortedIndices = Array.Empty<int>();
        private readonly BPlusTree<K, V, TKeyContainer, TValueContainer> _tree;
        private readonly TComparer _comparer;
        private readonly List<BPlusTree<K, V, TKeyContainer, TValueContainer>.LeafBatchMapping> _mappings;

        private K[] _keys = Array.Empty<K>();
        private int _mappingIndex;
        private LeafNode<K, V, TKeyContainer, TValueContainer>? _currentLeaf;

        // Keys that carry over from the previous leaf.
        private List<int> _carryOverRead = new List<int>(64);
        private List<int> _carryOverWrite = new List<int>(64);

        // Results for the current leaf.
        private readonly List<BulkSearchKeyResult> _currentResults = new List<BulkSearchKeyResult>();

        public BPlusTreeBulkSearch(BPlusTree<K, V, TKeyContainer, TValueContainer> tree, TComparer comparer)
        {
            _tree = tree;
            this._comparer = comparer;
            _mappings = new List<BPlusTree<K, V, TKeyContainer, TValueContainer>.LeafBatchMapping>();
        }

        /// <summary>
        /// The current leaf node. Only valid after <see cref="MoveNextLeaf"/> returns true.
        /// </summary>
        public LeafNode<K, V, TKeyContainer, TValueContainer> CurrentLeaf
        {
            get
            {
                Debug.Assert(_currentLeaf != null);
                return _currentLeaf;
            }
        }

        /// <summary>
        /// The search results for the current leaf. Each entry describes one search key
        /// and its found boundaries within the leaf.
        /// </summary>
        public IReadOnlyList<BulkSearchKeyResult> CurrentResults => _currentResults;

        public ValueTask Start(K[] keys, int keyLength)
        {
            if (keyLength > _sortedIndices.Length)
            {
                _sortedIndices = new int[keyLength];
            }

            for (int i = 0; i < keyLength; i++)
            {
                _sortedIndices[i] = i;
            }

            var indicesSpan = _sortedIndices.AsSpan().Slice(0, keyLength);
            indicesSpan.Sort(new ExternalKeyComparer<K, TKeyContainer, TComparer>(keys, _comparer));

            return Start(keys, keyLength, _sortedIndices);
        }

        public ValueTask Start(K[] keys, int keyLength, int[] sortedIndices)
        {
            _keys = keys;
            _sortedIndices = sortedIndices;
            _mappingIndex = 0;
            _carryOverRead.Clear();
            _carryOverWrite.Clear();
            _currentResults.Clear();
            if (_currentLeaf != null)
            {
                _currentLeaf.Return();
                _currentLeaf = null;
            }
            _mappings.Clear();
            return _tree.RouteBatchRootAsync(keys, keyLength, sortedIndices, _comparer, _mappings);
        }

        /// <summary>
        /// Advances to the next leaf that has search keys mapped to it.
        /// Returns false when all leaves have been visited.
        /// Await this only when moving between leaves; iteration within a leaf is synchronous via <see cref="CurrentResults"/>.
        /// </summary>
        public ValueTask<bool> MoveNextLeaf()
        {
            // If there are no more mappings and no carry-over keys, we are done.
            if (_mappingIndex >= _mappings.Count && _carryOverRead.Count == 0)
            {
                if (_currentLeaf != null)
                {
                    _currentLeaf.Return();
                    _currentLeaf = null;
                }
                return ValueTask.FromResult(false);
            }

            if (_mappingIndex >= _mappings.Count)
            {
                Debug.Assert(_currentLeaf != null);
                if (_currentLeaf!.next == 0)
                {
                    // No next leaf, carry-over keys have no more matches.
                    _currentLeaf.Return();
                    _currentLeaf = null;
                    _carryOverRead.Clear();
                    return ValueTask.FromResult(false);
                }

                var getNextTask = _tree.m_stateClient.GetValue(_currentLeaf.next);
                if (!getNextTask.IsCompletedSuccessfully)
                {
                    return MoveNextLeaf_CarryOverSlow(getNextTask);
                }

                _currentLeaf.Return();
                _currentLeaf = (getNextTask.Result as LeafNode<K, V, TKeyContainer, TValueContainer>)!;
                ProcessCarryOverOnly();
                return ValueTask.FromResult(true);
            }

            var mapping = _mappings[_mappingIndex];
            _mappingIndex++;

            var getLeafTask = _tree.m_stateClient.GetValue(mapping.LeafId);
            if (!getLeafTask.IsCompletedSuccessfully)
            {
                return MoveNextLeaf_Slow(getLeafTask, mapping);
            }

            if (_currentLeaf != null)
            {
                _currentLeaf.Return();
            }
            _currentLeaf = (getLeafTask.Result as LeafNode<K, V, TKeyContainer, TValueContainer>)!;
            ProcessLeaf(mapping);
            return ValueTask.FromResult(true);
        }

        private async ValueTask<bool> MoveNextLeaf_Slow(
            ValueTask<IBPlusTreeNode?> getLeafTask,
            BPlusTree<K, V, TKeyContainer, TValueContainer>.LeafBatchMapping mapping)
        {
            var node = await getLeafTask;
            if (_currentLeaf != null)
            {
                _currentLeaf.Return();
            }
            _currentLeaf = (node as LeafNode<K, V, TKeyContainer, TValueContainer>)!;
            ProcessLeaf(mapping);
            return true;
        }

        private async ValueTask<bool> MoveNextLeaf_CarryOverSlow(ValueTask<IBPlusTreeNode?> getNextTask)
        {
            var node = await getNextTask;
            _currentLeaf!.Return();
            _currentLeaf = (node as LeafNode<K, V, TKeyContainer, TValueContainer>)!;
            ProcessCarryOverOnly();
            return true;
        }

        private void ProcessLeaf(BPlusTree<K, V, TKeyContainer, TValueContainer>.LeafBatchMapping mapping)
        {
            _currentResults.Clear();
            var leaf = _currentLeaf!;
            var leafKeyCount = leaf.keys.Count;
            var lastIndex = leafKeyCount - 1;
            var comparer = _comparer;

            _carryOverWrite.Clear();
            for (int c = 0; c < _carryOverRead.Count; c++)
            {
                var keyIndex = _carryOverRead[c];
                var boundries = comparer.FindBoundries(_keys[keyIndex], leaf.keys, 0, lastIndex);

                var result = new BulkSearchKeyResult
                {
                    KeyIndex = keyIndex,
                    LowerBound = boundries.lowerBounds,
                    UpperBound = boundries.upperBounds,
                    ContinuesToNextLeaf = false
                };

                if (boundries.lowerBounds >= 0 && boundries.upperBounds == lastIndex && leaf.next != 0)
                {
                    result.ContinuesToNextLeaf = true;
                    _carryOverWrite.Add(keyIndex);
                }

                _currentResults.Add(result);
            }

            var end = mapping.Offset + mapping.Length;
            int lowerBound = 0;
            for (int i = mapping.Offset; i < end; i++)
            {
                var keyIndex = _sortedIndices[i];
                var boundries = comparer.FindBoundries(_keys[keyIndex], leaf.keys, lowerBound, lastIndex);

                if (boundries.lowerBounds < 0)
                {
                    lowerBound = ~boundries.lowerBounds;
                }
                else
                {
                    lowerBound = boundries.lowerBounds;
                }

                var result = new BulkSearchKeyResult
                {
                    KeyIndex = keyIndex,
                    LowerBound = boundries.lowerBounds,
                    UpperBound = boundries.upperBounds,
                    ContinuesToNextLeaf = false
                };

                if (boundries.lowerBounds >= 0 && boundries.upperBounds == lastIndex && leaf.next != 0)
                {
                    result.ContinuesToNextLeaf = true;
                    _carryOverWrite.Add(keyIndex);
                }

                _currentResults.Add(result);
            }

            var temp = _carryOverRead;
            _carryOverRead = _carryOverWrite;
            _carryOverWrite = temp;
        }

        private void ProcessCarryOverOnly()
        {
            _currentResults.Clear();
            var leaf = _currentLeaf!;
            var leafKeyCount = leaf.keys.Count;
            var lastIndex = leafKeyCount - 1;
            var comparer = _comparer;

            _carryOverWrite.Clear();
            for (int c = 0; c < _carryOverRead.Count; c++)
            {
                var keyIndex = _carryOverRead[c];
                var boundries = comparer.FindBoundries(_keys[keyIndex], leaf.keys, 0, lastIndex);

                var result = new BulkSearchKeyResult
                {
                    KeyIndex = keyIndex,
                    LowerBound = boundries.lowerBounds,
                    UpperBound = boundries.upperBounds,
                    ContinuesToNextLeaf = false
                };

                if (boundries.lowerBounds >= 0 && boundries.upperBounds == lastIndex && leaf.next != 0)
                {
                    result.ContinuesToNextLeaf = true;
                    _carryOverWrite.Add(keyIndex);
                }

                _currentResults.Add(result);
            }

            var temp = _carryOverRead;
            _carryOverRead = _carryOverWrite;
            _carryOverWrite = temp;
        }
    }
}
