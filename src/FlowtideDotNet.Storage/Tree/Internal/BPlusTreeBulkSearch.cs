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
        private int[] _lowerBounds = Array.Empty<int>();
        private int[] _upperBounds = Array.Empty<int>();
        private int[] _lookupBuffer = Array.Empty<int>();
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

        /// <summary>
        /// Mapped leaves that were already cached when the search started, held so eviction
        /// cannot drop one before the traversal reaches it. Holding takes no read, a leaf that
        /// is not cached has nothing to protect and is read at its turn.
        /// </summary>
        private LeafNode<K, V, TKeyContainer, TValueContainer>?[] _heldLeaves = Array.Empty<LeafNode<K, V, TKeyContainer, TValueContainer>?>();
        private int _heldLeafCount;

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
                _lowerBounds = new int[keyLength];
                _upperBounds = new int[keyLength];
                _lookupBuffer = new int[keyLength];
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
            // A pass the consumer abandoned part way still holds leaves, let them go.
            ReleaseHeldLeaves();
            _mappings.Clear();
            if (keyLength > _lowerBounds.Length)
            {
                _lowerBounds = new int[keyLength];
                _upperBounds = new int[keyLength];
                _lookupBuffer = new int[keyLength];
            }
            if (_comparer is IRouteToLeftmost routeToLeftmost && routeToLeftmost.RouteToLeftmost)
            {
                // One mapping, the traversal follows next pointers from the leftmost leaf, so
                // there is nothing mapped ahead to hold.
                _mappings.Add(new BPlusTree<K, V, TKeyContainer, TValueContainer>.LeafBatchMapping(_tree.m_stateClient.Metadata!.Left, 0, keyLength, -1));
                return ValueTask.CompletedTask;
            }

            var task = _tree.RouteBatchRootAsync(keys, keyLength, sortedIndices, _comparer, _mappings, _lowerBounds, _upperBounds, _lookupBuffer);
            if (task.IsCompletedSuccessfully)
            {
                if (_comparer.SeekPreviousPageForValue)
                {
                    _mappings.Reverse();
                }
                HoldMappedLeaves();
                return ValueTask.CompletedTask;
            }
            return StartSlow(task);
        }

        private async ValueTask StartSlow(ValueTask task)
        {
            await task;
            if (_comparer.SeekPreviousPageForValue)
            {
                _mappings.Reverse();
            }
            HoldMappedLeaves();
        }

        /// <summary>
        /// Every mapped leaf will be read before the search ends, so the ones already cached are
        /// held for its duration. The traversal still fetches each leaf at its turn, which now
        /// finds it cached, the extra rent is only what keeps it there.
        /// </summary>
        private void HoldMappedLeaves()
        {
            var holdCount = Math.Min(_mappings.Count, _tree.m_stateClient.MaxHeldPages);
            if (_heldLeaves.Length < holdCount)
            {
                _heldLeaves = new LeafNode<K, V, TKeyContainer, TValueContainer>?[holdCount];
            }
            for (int i = 0; i < holdCount; i++)
            {
                _heldLeaves[i] = _tree.m_stateClient.TryGetCachedValue(_mappings[i].LeafId, out var cached)
                            ? cached as LeafNode<K, V, TKeyContainer, TValueContainer>
                            : null;
            }
            _heldLeafCount = holdCount;
        }

        /// <summary>
        /// Hands over the leaf held for a mapping, leaving the slot empty so the rent is not
        /// released twice. Null when it was not cached at the start, or when the mapping no
        /// longer points at it.
        /// </summary>
        private LeafNode<K, V, TKeyContainer, TValueContainer>? TakeHeldLeaf(int mappingIndex, long leafId)
        {
            if (mappingIndex >= _heldLeafCount)
            {
                return null;
            }
            var held = _heldLeaves[mappingIndex];
            if (held == null)
            {
                return null;
            }
            _heldLeaves[mappingIndex] = null;
            if (held.Id != leafId)
            {
                held.Return();
                return null;
            }
            return held;
        }

        private void ReleaseHeldLeaves()
        {
            for (int i = 0; i < _heldLeafCount; i++)
            {
                _heldLeaves[i]?.Return();
                _heldLeaves[i] = null;
            }
            _heldLeafCount = 0;
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
                return MoveNextLeaf_FollowLinkedList();
            }

            var mapping = _mappings[_mappingIndex];

            // Carry-over keys exist: check if we need to visit intermediate leaves first.
            if (_carryOverRead.Count > 0 && _currentLeaf != null)
            {
                var nextOrPrev = _comparer.SeekPreviousPageForValue ? _currentLeaf.previous : _currentLeaf.next;
                if (nextOrPrev == 0)
                {
                    // No next/prev leaf - drop carry-over, proceed to mapping below.
                    _carryOverRead.Clear();
                }
                else if (nextOrPrev != mapping.LeafId)
                {
                    // Intermediate leaf exists between current position and the mapping's leaf.
                    // Follow the linked list without consuming the mapping.
                    return MoveNextLeaf_FollowLinkedList();
                }
                // else: next/prev leaf IS the mapping's leaf - fall through to process both.
            }

            var mappingIndex = _mappingIndex;
            _mappingIndex++;

            // Held since the search started, so it is already the current page for this mapping.
            // Taking it hands the rent over to _currentLeaf, fetching again would only repeat the
            // lookup and count the same read twice.
            var held = TakeHeldLeaf(mappingIndex, mapping.LeafId);
            if (held != null)
            {
                if (_currentLeaf != null)
                {
                    _currentLeaf.Return();
                }
                _currentLeaf = held;
                ProcessLeaf(mapping);
                return ValueTask.FromResult(true);
            }

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

        /// <summary>
        /// Follows the leaf linked list to process carry-over keys in the adjacent leaf.
        /// </summary>
        private ValueTask<bool> MoveNextLeaf_FollowLinkedList()
        {
            Debug.Assert(_currentLeaf != null);
            Debug.Assert(_carryOverRead.Count > 0);

            var nextOrPrev = _comparer.SeekPreviousPageForValue ? _currentLeaf.previous : _currentLeaf.next;
            if (nextOrPrev == 0)
            {
                // No next/prev leaf, carry-over keys have no more matches.
                _currentLeaf.Return();
                _currentLeaf = null;
                _carryOverRead.Clear();
                return ValueTask.FromResult(false);
            }

            var getNextTask = _tree.m_stateClient.GetValue(nextOrPrev);
            if (!getNextTask.IsCompletedSuccessfully)
            {
                return MoveNextLeaf_CarryOverSlow(getNextTask);
            }

            _currentLeaf.Return();
            _currentLeaf = (getNextTask.Result as LeafNode<K, V, TKeyContainer, TValueContainer>)!;
            ProcessCarryOverOnly();
            return ValueTask.FromResult(true);
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
                    ContinuesToNextLeaf = false,
                    ContinuesToPreviousLeaf = false
                };

                if (comparer.SeekNextPageForValue && leaf.next != 0 &&
                    ((boundries.lowerBounds >= 0 && boundries.upperBounds == lastIndex) ||
                     (boundries.lowerBounds < 0 && (~boundries.lowerBounds) > lastIndex)))
                {
                    result.ContinuesToNextLeaf = true;
                    _carryOverWrite.Add(keyIndex);
                }
                else if (comparer.SeekPreviousPageForValue && leaf.previous != 0 &&
                         ((boundries.lowerBounds == 0 && boundries.upperBounds >= 0) ||
                          (boundries.lowerBounds < 0 && (~boundries.lowerBounds) == 0)))
                {
                    result.ContinuesToPreviousLeaf = true;
                    _carryOverWrite.Add(keyIndex);
                }

                if (!(boundries.lowerBounds == -1 && boundries.upperBounds == -1))
                {
                    _currentResults.Add(result);
                }
            }

            var sortedIndicesSpan = _sortedIndices.AsSpan(mapping.Offset, mapping.Length);
            var lowerBoundsSpan = _lowerBounds.AsSpan(0, mapping.Length);
            var upperBoundsSpan = _upperBounds.AsSpan(0, mapping.Length);
            var lookupBufferSpan = _lookupBuffer.AsSpan(0, mapping.Length);
            _comparer.FindBoundriesBulk(_keys, sortedIndicesSpan, leaf.keys, lowerBoundsSpan, upperBoundsSpan, lookupBufferSpan);

            for (int i = 0; i < mapping.Length; i++)
            {
                var keyIndex = sortedIndicesSpan[i];
                var foundLowerBound = lowerBoundsSpan[i];
                var foundUpperBound = upperBoundsSpan[i];

                var result = new BulkSearchKeyResult
                {
                    KeyIndex = keyIndex,
                    LowerBound = foundLowerBound,
                    UpperBound = foundUpperBound,
                    ContinuesToNextLeaf = false,
                    ContinuesToPreviousLeaf = false
                };
                
                if (comparer.SeekNextPageForValue && leaf.next != 0 &&
                    ((foundLowerBound >= 0 && foundUpperBound == lastIndex) ||
                     (foundLowerBound < 0 && (~foundLowerBound) > lastIndex)))
                {
                    result.ContinuesToNextLeaf = true;
                    _carryOverWrite.Add(keyIndex);
                }
                else if (comparer.SeekPreviousPageForValue && leaf.previous != 0 &&
                         ((foundLowerBound == 0 && foundUpperBound >= 0) ||
                          (foundLowerBound < 0 && (~foundLowerBound) == 0)))
                {
                    result.ContinuesToPreviousLeaf = true;
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
                    ContinuesToNextLeaf = false,
                    ContinuesToPreviousLeaf = false
                };

                if (comparer.SeekNextPageForValue && leaf.next != 0 &&
                    ((boundries.lowerBounds >= 0 && boundries.upperBounds == lastIndex) ||
                     (boundries.lowerBounds < 0 && (~boundries.lowerBounds) > lastIndex)))
                {
                    result.ContinuesToNextLeaf = true;
                    _carryOverWrite.Add(keyIndex);
                }
                else if (comparer.SeekPreviousPageForValue && leaf.previous != 0 &&
                         ((boundries.lowerBounds == 0 && boundries.upperBounds >= 0) ||
                          (boundries.lowerBounds < 0 && (~boundries.lowerBounds) == 0)))
                {
                    result.ContinuesToPreviousLeaf = true;
                    _carryOverWrite.Add(keyIndex);
                }

                if (!(boundries.lowerBounds == -1 && boundries.upperBounds == -1))
                {
                    _currentResults.Add(result);
                }
            }

            var temp = _carryOverRead;
            _carryOverRead = _carryOverWrite;
            _carryOverWrite = temp;
        }

        public void Dispose()
        {
            if (_currentLeaf != null)
            {
                _currentLeaf.Return();
                _currentLeaf = null;
            }
            ReleaseHeldLeaves();
            _carryOverRead.Clear();
            _carryOverWrite.Clear();
            _currentResults.Clear();
        }
    }
}
