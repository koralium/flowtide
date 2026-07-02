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
    /// Stores the keys of a single B+ tree node. Implementations choose how keys are laid out, for example a managed list
    /// or a contiguous unmanaged buffer, and the tree uses this abstraction to search, insert, update and remove keys
    /// within a node. Keys within a container are kept in sorted order.
    /// </summary>
    /// <typeparam name="K">The key type.</typeparam>
    public interface IKeyContainer<K> : IDisposable
    {
        /// <summary>
        /// Binary searches the container for the key.
        /// </summary>
        /// <param name="key">The key to locate.</param>
        /// <param name="comparer">The comparer that defines the key ordering.</param>
        /// <returns>The index of the key, or the bitwise complement of the index where it would be inserted when not found.</returns>
        int BinarySearch(K key, IComparer<K> comparer);

        /// <summary>
        /// Appends a key to the end of the container.
        /// </summary>
        /// <param name="key">The key to append.</param>
        void Add(K key);

        /// <summary>
        /// Inserts a key at the given index, shifting later keys up. Used for keys stored in a leaf node.
        /// </summary>
        /// <param name="index">The index to insert at.</param>
        /// <param name="key">The key to insert.</param>
        void Insert(int index, K key);

        /// <summary>
        /// Inserts a separator key at the given index for use in an internal node, shifting later keys up.
        /// Containers that store leaf and internal keys differently can handle this case separately.
        /// </summary>
        /// <param name="index">The index to insert at.</param>
        /// <param name="key">The key to insert.</param>
        void Insert_Internal(int index, K key);

        /// <summary>
        /// Replaces the key at the given index.
        /// </summary>
        /// <param name="index">The index to overwrite.</param>
        /// <param name="key">The new key.</param>
        void Update(int index, K key);

        /// <summary>
        /// Removes the key at the given index, shifting later keys down.
        /// </summary>
        /// <param name="index">The index of the key to remove.</param>
        void RemoveAt(int index);

        /// <summary>
        /// The number of keys in the container.
        /// </summary>
        int Count { get; }

        /// <summary>
        /// Gets the key at the given index.
        /// </summary>
        /// <param name="index">The index of the key.</param>
        K Get(in int index);

        /// <summary>
        /// Appends a range of keys copied from another container of the same kind.
        /// </summary>
        /// <param name="container">The container to copy keys from.</param>
        /// <param name="start">The index in <paramref name="container"/> to start copying from.</param>
        /// <param name="count">The number of keys to copy.</param>
        void AddRangeFrom(IKeyContainer<K> container, int start, int count);

        /// <summary>
        /// Removes a run of keys, shifting later keys down.
        /// </summary>
        /// <param name="start">The index of the first key to remove.</param>
        /// <param name="count">The number of keys to remove.</param>
        void RemoveRange(int start, int count);

        /// <summary>
        /// Returns the serialized byte size of all keys, or -1 for containers that do not track byte sizes.
        /// </summary>
        int GetByteSize();

        /// <summary>
        /// Returns the serialized byte size of the keys between <paramref name="start"/> (inclusive) and
        /// <paramref name="end"/> (exclusive), or -1 for containers that do not track byte sizes.
        /// </summary>
        /// <param name="start">The index of the first key in the range.</param>
        /// <param name="end">The exclusive index of the end of the range.</param>
        int GetByteSize(int start, int end);

        /// <summary>
        /// Inserts selected keys from an array at scattered positions in a single pass, used when merging a batch into the node.
        /// </summary>
        /// <param name="keys">The array to insert keys from.</param>
        /// <param name="sortedLookup">The indices in <paramref name="keys"/> of the keys to insert.</param>
        /// <param name="targetPositions">The positions to insert at, in pre-insert coordinate space, in ascending order.</param>
        /// <param name="lookupBuffer">A scratch buffer the implementation may use during the insert.</param>
        void InsertFrom(
            K[] keys,
            ReadOnlySpan<int> sortedLookup,
            ReadOnlySpan<int> targetPositions,
            Span<int> lookupBuffer);

        /// <summary>
        /// Removes the keys at the given sorted positions in a single pass.
        /// </summary>
        /// <param name="positions">The ascending indices of the keys to delete.</param>
        void DeleteBatch(ReadOnlySpan<int> positions);
    }
}
