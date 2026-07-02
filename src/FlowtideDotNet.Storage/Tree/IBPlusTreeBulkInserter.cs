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
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Storage.Tree
{
    /// <summary>
    /// Applies a whole batch of rows to a B+ tree in a single pass. The batch is sorted by key and grouped by the leaf
    /// each row lands in, so each affected leaf is loaded once and any required node splits and merges are handled
    /// together. This is more efficient than upserting rows one at a time when writing a large batch.
    /// Each row's effect (insert, update, delete or no change) is decided by an <see cref="IRowMutator{K, V}"/>.
    /// </summary>
    /// <typeparam name="K">The key type.</typeparam>
    /// <typeparam name="V">The value type.</typeparam>
    /// <typeparam name="TKeyContainer">The container used to store keys within a node.</typeparam>
    /// <typeparam name="TValueContainer">The container used to store values within a node.</typeparam>
    public interface IBPlusTreeBulkInserter<K, V, TKeyContainer, TValueContainer>
        where TKeyContainer : IKeyContainer<K>
        where TValueContainer : IValueContainer<V>
    {
        /// <summary>
        /// Sorts the first <paramref name="keyLength"/> keys and returns their indices in sorted order, without
        /// reordering the input array. The result can be passed to the overload of <see cref="ApplyBatch"/> that takes
        /// precomputed indices to avoid sorting again. The returned array is an internal buffer that is reused between calls.
        /// </summary>
        /// <param name="keys">The keys to sort.</param>
        /// <param name="keyLength">The number of entries in <paramref name="keys"/> to use.</param>
        /// <returns>The indices of the keys in ascending key order.</returns>
        int[] SortAndGetIndices(K[] keys, int keyLength);

        /// <summary>
        /// Applies a batch of rows to the tree, sorting the keys internally.
        /// </summary>
        /// <typeparam name="TMutator">The mutator type that decides each row's operation.</typeparam>
        /// <param name="keys">The keys of the batch.</param>
        /// <param name="values">The values of the batch, aligned with <paramref name="keys"/>.</param>
        /// <param name="keyLength">The number of rows in the batch to use.</param>
        /// <param name="mutator">The mutator that decides the operation for each row.</param>
        /// <param name="totalBatchByteSize">The total serialized byte size of the batch, used to decide page splitting.</param>
        ValueTask ApplyBatch<TMutator>(K[] keys, V[] values, int keyLength, TMutator mutator, int totalBatchByteSize)
            where TMutator : IRowMutator<K, V>;

        /// <summary>
        /// Applies a batch of rows to the tree using precomputed sorted indices and duplicate tags, avoiding an internal sort.
        /// </summary>
        /// <typeparam name="TMutator">The mutator type that decides each row's operation.</typeparam>
        /// <param name="keys">The keys of the batch.</param>
        /// <param name="values">The values of the batch, aligned with <paramref name="keys"/>.</param>
        /// <param name="keyLength">The number of rows in the batch to use.</param>
        /// <param name="sortedIndices">The indices of the rows in ascending key order, as produced by <see cref="SortAndGetIndices"/>.</param>
        /// <param name="duplicateTags">Per sorted row, a group id that is equal for rows with the same key.</param>
        /// <param name="mutator">The mutator that decides the operation for each row.</param>
        /// <param name="totalBatchByteSize">The total serialized byte size of the batch, used to decide page splitting.</param>
        ValueTask ApplyBatch<TMutator>(K[] keys, V[] values, int keyLength, int[] sortedIndices, int[] duplicateTags, TMutator mutator, int totalBatchByteSize)
            where TMutator : IRowMutator<K, V>;

        /// <summary>
        /// The number of distinct leaves the most recent batch touched.
        /// </summary>
        int LeafHitCount { get; }
    }
}
