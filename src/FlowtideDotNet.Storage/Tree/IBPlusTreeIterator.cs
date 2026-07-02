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
    /// A cursor over a B+ tree that enumerates the tree one leaf page at a time. Position it first with
    /// <see cref="SeekFirst"/> or <see cref="Seek"/>, then enumerate it to walk the pages from that position onward;
    /// each yielded <see cref="IBPlusTreePageIterator{K, V, TKeyContainer, TValueContainer}"/> exposes the entries of a leaf.
    /// </summary>
    /// <typeparam name="K">The key type.</typeparam>
    /// <typeparam name="V">The value type.</typeparam>
    /// <typeparam name="TKeyContainer">The container used to store keys within a node.</typeparam>
    /// <typeparam name="TValueContainer">The container used to store values within a node.</typeparam>
    public interface IBPlusTreeIterator<K, V, TKeyContainer, TValueContainer> : IAsyncEnumerable<IBPlusTreePageIterator<K, V, TKeyContainer, TValueContainer>>, IDisposable
        where TKeyContainer : IKeyContainer<K>
        where TValueContainer : IValueContainer<V>
    {
        /// <summary>
        /// Positions the cursor at the first entry of the tree.
        /// </summary>
        ValueTask SeekFirst();

        /// <summary>
        /// Positions the cursor at the first entry whose key matches the given key.
        /// </summary>
        /// <param name="key">The key to seek to.</param>
        /// <param name="searchComparer">An optional comparer used to locate the key, separate from the tree's key comparer.</param>
        ValueTask Seek(in K key, in IBplusTreeComparer<K, TKeyContainer>? searchComparer = null);

        /// <summary>
        /// Resets the iterator and frees any rented pages, so the same iterator instance can be reused over time.
        /// </summary>
        void Reset();

        /// <summary>
        /// Copies the current seek position into another iterator, letting it continue from the same place.
        /// </summary>
        /// <param name="other">The iterator to copy the current position into.</param>
        void CloneSeekResultTo(IBPlusTreeIterator<K, V, TKeyContainer, TValueContainer> other);
    }
}
