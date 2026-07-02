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
    /// Stores the values of a single B+ tree node, in the same order as the keys in the node's key container.
    /// Implementations choose how values are laid out, for example a managed list or a contiguous unmanaged buffer, and
    /// the tree uses this abstraction to read, insert, update and remove values within a node.
    /// </summary>
    /// <typeparam name="V">The value type.</typeparam>
    public interface IValueContainer<V> : IDisposable
    {
        /// <summary>
        /// Inserts a value at the given index, shifting later values up.
        /// </summary>
        /// <param name="index">The index to insert at.</param>
        /// <param name="value">The value to insert.</param>
        void Insert(int index, V value);

        /// <summary>
        /// Replaces the value at the given index.
        /// </summary>
        /// <param name="index">The index to overwrite.</param>
        /// <param name="value">The new value.</param>
        void Update(int index, V value);

        /// <summary>
        /// Removes the value at the given index, shifting later values down.
        /// </summary>
        /// <param name="index">The index of the value to remove.</param>
        void RemoveAt(int index);

        /// <summary>
        /// The number of values in the container.
        /// </summary>
        int Count { get; }

        /// <summary>
        /// Gets the value at the given index.
        /// </summary>
        /// <param name="index">The index of the value.</param>
        V Get(int index);

        /// <summary>
        /// Gets a reference to the value at the given index, allowing it to be read or modified in place.
        /// </summary>
        /// <param name="index">The index of the value.</param>
        ref V GetRef(int index);

        /// <summary>
        /// Appends a range of values copied from another container of the same kind.
        /// </summary>
        /// <param name="container">The container to copy values from.</param>
        /// <param name="start">The index in <paramref name="container"/> to start copying from.</param>
        /// <param name="count">The number of values to copy.</param>
        void AddRangeFrom(IValueContainer<V> container, int start, int count);

        /// <summary>
        /// Removes a run of values, shifting later values down.
        /// </summary>
        /// <param name="start">The index of the first value to remove.</param>
        /// <param name="count">The number of values to remove.</param>
        void RemoveRange(int start, int count);

        /// <summary>
        /// Returns the serialized byte size of all values, or -1 for containers that do not track byte sizes.
        /// </summary>
        int GetByteSize();

        /// <summary>
        /// Returns the serialized byte size of the values between <paramref name="start"/> (inclusive) and
        /// <paramref name="end"/> (exclusive), or -1 for containers that do not track byte sizes.
        /// </summary>
        /// <param name="start">The index of the first value in the range.</param>
        /// <param name="end">The exclusive index of the end of the range.</param>
        int GetByteSize(int start, int end);

        /// <summary>
        /// Inserts selected values from an array at scattered positions in a single pass, used when merging a batch into the node.
        /// </summary>
        /// <param name="values">The array to insert values from.</param>
        /// <param name="sortedLookup">The indices in <paramref name="values"/> of the values to insert.</param>
        /// <param name="targetPositions">The positions to insert at, in pre-insert coordinate space, in ascending order.</param>
        void InsertFrom(V[] values, ReadOnlySpan<int> sortedLookup, ReadOnlySpan<int> targetPositions);

        /// <summary>
        /// Removes the values at the given sorted positions in a single pass.
        /// </summary>
        /// <param name="positions">The ascending indices of the values to delete.</param>
        void DeleteBatch(ReadOnlySpan<int> positions);
    }
}
