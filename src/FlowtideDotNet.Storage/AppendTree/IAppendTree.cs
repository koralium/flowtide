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

using FlowtideDotNet.Storage.AppendTree;
using FlowtideDotNet.Storage.Tree;

namespace FlowtideDotNet.Storage
{
    /// <summary>
    /// A B+ tree that only supports appending entries in strictly increasing key order, which keeps all writes in the
    /// rightmost leaf and avoids the node splitting a general purpose tree needs. It is suited to time ordered data such
    /// as a time series, where new entries always have a larger key than everything already stored.
    /// Data is paged through a state client so the tree can grow beyond memory and be persisted across checkpoints.
    /// </summary>
    /// <typeparam name="K">The key type, appended in increasing order.</typeparam>
    /// <typeparam name="V">The value type stored against each key.</typeparam>
    /// <typeparam name="TKeyContainer">The container used to store keys within a node.</typeparam>
    /// <typeparam name="TValueContainer">The container used to store values within a node.</typeparam>
    public interface IAppendTree<K, V, TKeyContainer, TValueContainer>
        where TKeyContainer : IKeyContainer<K>
    {
        /// <summary>
        /// Appends an entry to the end of the tree. The key must be greater than the last appended key,
        /// otherwise an <see cref="InvalidOperationException"/> is thrown.
        /// </summary>
        /// <param name="key">The key to append, which must be larger than every key already in the tree.</param>
        /// <param name="value">The value to store against <paramref name="key"/>.</param>
        ValueTask Append(in K key, in V value);

        /// <summary>
        /// Persists pending changes through the underlying state client.
        /// </summary>
        ValueTask Commit();

        /// <summary>
        /// Removes data from the start of the tree up to <paramref name="key"/>. Whole leaf nodes are dropped without
        /// being loaded into memory; data within a partially covered leaf is trimmed in place.
        /// </summary>
        /// <param name="key">Entries with a key less than or equal to this are pruned.</param>
        ValueTask Prune(K key);

        /// <summary>
        /// For debugging purposes only. Returns a Graphviz dot representation of the tree.
        /// </summary>
        internal Task<string> Print();

        /// <summary>
        /// Creates an iterator that can seek to a key and enumerate entries in ascending key order.
        /// </summary>
        IAppendTreeIterator<K, V, TKeyContainer> CreateIterator();

        /// <summary>
        /// Clears all data and resets the tree to an empty state. Useful for temporary storage that should be
        /// discarded on demand.
        /// </summary>
        ValueTask Clear();
    }
}
