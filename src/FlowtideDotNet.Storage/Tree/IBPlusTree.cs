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

namespace FlowtideDotNet.Storage.Tree
{
    /// <summary>
    /// The change a read-modify-write applied to a key.
    /// </summary>
    public enum GenericWriteOperation
    {
        /// <summary>
        /// Nothing was changed.
        /// </summary>
        None = 0,
        /// <summary>
        /// Value is updated
        /// </summary>
        Upsert = 1,
        /// <summary>
        /// Value is deleted
        /// </summary>
        Delete = 2
    }

    /// <summary>
    /// A read-modify-write callback invoked for a key during a write. It receives the value supplied to the write call,
    /// the current stored value (or default when absent), and whether a value currently exists, and returns the new
    /// value together with the <see cref="GenericWriteOperation"/> to apply.
    /// </summary>
    /// <typeparam name="V">The value type.</typeparam>
    /// <param name="input">The value supplied to the write call.</param>
    /// <param name="current">The current stored value, or default when the key is not present.</param>
    /// <param name="exists">True when a value already exists for the key.</param>
    /// <returns>The new value and the operation to perform.</returns>
    public delegate (V? result, GenericWriteOperation operation) GenericWriteFunction<V>(V? input, V? current, bool exists);

    /// <summary>
    /// A persistent B+ tree mapping keys to values. Keys and values are held in containers and the tree is paged through
    /// a state client, so it can grow beyond memory, offload to storage and be persisted across checkpoints.
    /// Reads and writes are asynchronous because a lookup may need to load pages from storage.
    /// </summary>
    /// <typeparam name="K">The key type.</typeparam>
    /// <typeparam name="V">The value type.</typeparam>
    /// <typeparam name="TKeyContainer">The container used to store keys within a node.</typeparam>
    /// <typeparam name="TValueContainer">The container used to store values within a node.</typeparam>
    public interface IBPlusTree<K, V, TKeyContainer, TValueContainer>
        where TKeyContainer : IKeyContainer<K>
        where TValueContainer : IValueContainer<V>
    {
        /// <summary>
        /// Inserts the key with the given value, or replaces the existing value if the key is already present.
        /// </summary>
        /// <param name="key">The key to insert or update.</param>
        /// <param name="value">The value to store.</param>
        ValueTask Upsert(in K key, in V value);

        /// <summary>
        /// Removes the key from the tree if it is present.
        /// </summary>
        /// <param name="key">The key to remove.</param>
        ValueTask Delete(in K key);

        /// <summary>
        /// Performs a read-modify-write on the key by running <paramref name="function"/> and applying the operation it
        /// returns, without returning the resulting value. Cheaper than <see cref="RMW"/> when the result is not needed.
        /// </summary>
        /// <param name="key">The key to write.</param>
        /// <param name="value">The value passed to <paramref name="function"/> as its input.</param>
        /// <param name="function">The callback that decides the new value and the operation to apply.</param>
        /// <returns>The operation that was applied.</returns>
        ValueTask<GenericWriteOperation> RMWNoResult(in K key, in V? value, in GenericWriteFunction<V> function);

        /// <summary>
        /// Performs a read-modify-write on the key by running <paramref name="function"/> and applying the operation it
        /// returns, also returning the resulting value.
        /// </summary>
        /// <param name="key">The key to write.</param>
        /// <param name="value">The value passed to <paramref name="function"/> as its input.</param>
        /// <param name="function">The callback that decides the new value and the operation to apply.</param>
        /// <returns>The operation that was applied and the resulting value.</returns>
        ValueTask<(GenericWriteOperation operation, V? result)> RMW(in K key, in V? value, in GenericWriteFunction<V> function);

        /// <summary>
        /// Looks up the value stored for a key.
        /// </summary>
        /// <param name="key">The key to look up.</param>
        /// <returns>Whether the key was found and, if so, its value.</returns>
        ValueTask<(bool found, V? value)> GetValue(in K key);

        /// <summary>
        /// Looks up the stored key that matches the given key. Useful when the stored key carries more data than the
        /// part used for lookup.
        /// </summary>
        /// <param name="key">The key to look up.</param>
        /// <returns>Whether a matching key was found and, if so, the stored key.</returns>
        ValueTask<(bool found, K? key)> GetKey(in K key);

        /// <summary>
        /// Creates an iterator that walks the tree in ascending key order.
        /// </summary>
        IBPlusTreeIterator<K, V, TKeyContainer, TValueContainer> CreateIterator();

        /// <summary>
        /// Creates an iterator that walks the tree in descending key order.
        /// </summary>
        IBPlusTreeIterator<K, V, TKeyContainer, TValueContainer> CreateBackwardIterator();

        /// <summary>
        /// For debugging purposes only
        /// </summary>
        /// <returns></returns>
        internal Task<string> Print();

        /// <summary>
        /// Persists pending changes through the underlying state client.
        /// </summary>
        ValueTask Commit();

        /// <summary>
        /// Clears the tree and returns a blank new tree.
        ///
        /// This is useful for temporary data storage which will offload to disk when the size grows, but can be deleted on demand.
        /// </summary>
        /// <returns></returns>
        ValueTask Clear();

        /// <summary>
        /// The number of reads that missed the in-memory cache and had to load a page from storage.
        /// </summary>
        long CacheMisses { get; }

        /// <summary>
        /// Creates a searcher that looks up many keys in one pass across the leaves, using the given comparer.
        /// More efficient than calling <see cref="GetValue"/> per key for a large, sorted batch.
        /// </summary>
        /// <typeparam name="TComparer">The comparer type used to locate the keys.</typeparam>
        /// <param name="comparer">The comparer used to locate the keys.</param>
        IBplusTreeBulkSearch<K, V, TKeyContainer, TValueContainer, TComparer> CreateBulkSearcher<TComparer>(TComparer comparer)
            where TComparer : IBplusTreeComparer<K, TKeyContainer>;

        /// <summary>
        /// Creates an inserter that applies a batch of rows in one pass, more efficiently than inserting each row on its own.
        /// </summary>
        IBPlusTreeBulkInserter<K, V, TKeyContainer, TValueContainer> CreateBulkInserter();
    }
}
