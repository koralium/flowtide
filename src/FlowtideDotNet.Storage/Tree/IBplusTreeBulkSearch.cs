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

using FlowtideDotNet.Storage.Tree.Internal;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Storage.Tree
{
    /// <summary>
    /// The result of searching one key within a leaf during a bulk search: which key it was and the index range it
    /// matched in that leaf.
    /// </summary>
    public struct BulkSearchKeyResult
    {
        /// <summary>
        /// The original index of the search key in the input array.
        /// </summary>
        public int KeyIndex;

        /// <summary>
        /// The lower bound index in the leaf. Negative (bitwise complement) if the key was not found.
        /// </summary>
        public int LowerBound;

        /// <summary>
        /// The upper bound index in the leaf. Negative (bitwise complement) if the key was not found.
        /// </summary>
        public int UpperBound;

        /// <summary>
        /// True if this key may continue into the next leaf (upper bound is the last index of the leaf).
        /// </summary>
        public bool ContinuesToNextLeaf;

        /// <summary>
        /// True if this key may continue into the previous leaf (lower bound is the first index of the leaf).
        /// </summary>
        public bool ContinuesToPreviousLeaf;

        /// <summary>
        /// True when the key was found in the leaf, that is when <see cref="LowerBound"/> is non-negative.
        /// </summary>
        public bool Found => LowerBound >= 0;
    }

    /// <summary>
    /// Searches many keys against a B+ tree in a single pass. The keys are sorted and grouped by the leaf they fall in,
    /// so each leaf is loaded once and the results are produced one leaf at a time. After <see cref="Start(K[], int)"/>,
    /// call <see cref="MoveNextLeaf"/> in a loop and read <see cref="CurrentLeaf"/> and <see cref="CurrentResults"/> for
    /// each leaf until it returns false. A key whose match runs to the end of a leaf is carried over into the next leaf.
    /// Dispose the searcher to release the leaf page it holds.
    /// </summary>
    /// <typeparam name="K">The key type.</typeparam>
    /// <typeparam name="V">The value type.</typeparam>
    /// <typeparam name="TKeyContainer">The container used to store keys within a node.</typeparam>
    /// <typeparam name="TValueContainer">The container used to store values within a node.</typeparam>
    /// <typeparam name="TComparer">The comparer used to locate the keys.</typeparam>
    public interface IBplusTreeBulkSearch<K, V, TKeyContainer, TValueContainer, TComparer> : IDisposable
        where TKeyContainer : IKeyContainer<K>
        where TValueContainer : IValueContainer<V>
        where TComparer : IBplusTreeComparer<K, TKeyContainer>
    {
        /// <summary>
        /// Begins a search for a batch of keys, sorting them internally before routing them to the leaves.
        /// </summary>
        /// <param name="keys">The keys to search for.</param>
        /// <param name="keyLength">The number of entries in <paramref name="keys"/> to use.</param>
        ValueTask Start(K[] keys, int keyLength);

        /// <summary>
        /// Begins a search for a batch of keys using precomputed sorted indices, avoiding an internal sort.
        /// </summary>
        /// <param name="keys">The keys to search for.</param>
        /// <param name="keyLength">The number of entries in <paramref name="keys"/> to use.</param>
        /// <param name="sortedIndices">The indices of the keys in ascending key order.</param>
        ValueTask Start(K[] keys, int keyLength, int[] sortedIndices);

        /// <summary>
        /// Advances to the next leaf that has search keys mapped to it, exposing its matches through
        /// <see cref="CurrentLeaf"/> and <see cref="CurrentResults"/>. Returns false when all leaves have been visited.
        /// </summary>
        /// <returns>True if a leaf with results is now current, false when the search is complete.</returns>
        ValueTask<bool> MoveNextLeaf();

        /// <summary>
        /// The current leaf node. Only valid after <see cref="MoveNextLeaf"/> returns true.
        /// </summary>
        LeafNode<K, V, TKeyContainer, TValueContainer> CurrentLeaf { get; }

        /// <summary>
        /// The search results for the current leaf. Each entry describes one search key and the index range it matched in the leaf.
        /// </summary>
        IReadOnlyList<BulkSearchKeyResult> CurrentResults { get; }
    }
}
