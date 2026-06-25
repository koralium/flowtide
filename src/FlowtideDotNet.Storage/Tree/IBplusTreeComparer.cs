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
    /// The inclusive index range of the entries in a key container that match a searched key.
    /// When the key is not present both bounds are the bitwise complement of the index where it would be inserted.
    /// </summary>
    public struct FindBoundriesResult
    {
        /// <summary>
        /// The index of the first matching entry, or the bitwise complement of the insertion index when not found.
        /// </summary>
        public readonly int lowerBounds;

        /// <summary>
        /// The index of the last matching entry, or the bitwise complement of the insertion index when not found.
        /// </summary>
        public readonly int upperBounds;

        /// <summary>
        /// Creates a result with the given inclusive bounds.
        /// </summary>
        /// <param name="lowerBounds">The index of the first matching entry.</param>
        /// <param name="upperBounds">The index of the last matching entry.</param>
        public FindBoundriesResult(int lowerBounds, int upperBounds)
        {
            this.lowerBounds = lowerBounds;
            this.upperBounds = upperBounds;
        }
    }

    /// <summary>
    /// Defines how keys are compared and located within a key container for a B+ tree. Implementations can provide a
    /// search strategy tailored to how the keys are stored, for example a binary search over a sorted list.
    /// </summary>
    /// <typeparam name="K">The key type.</typeparam>
    /// <typeparam name="TKeyContainer">The container that stores the keys of a node.</typeparam>
    public interface IBplusTreeComparer<K, TKeyContainer>
        where TKeyContainer : IKeyContainer<K>
    {
        /// <summary>
        /// Finds the index of the key in the container.
        /// This allows custom search algorithms that suit the storage solution.
        /// </summary>
        /// <param name="key">The key to locate.</param>
        /// <param name="keyContainer">The container to search.</param>
        /// <returns>
        /// The index of the key, or the bitwise complement of the index where it would be inserted when not found.
        /// </returns>
        int FindIndex(in K key, in TKeyContainer keyContainer);

        /// <summary>
        /// Finds the matching index range for a batch of keys in one pass. The keys are visited in the order given by
        /// <paramref name="sortedLookup"/>, which lets the search advance through the container instead of restarting for each key.
        /// </summary>
        /// <param name="keys">The keys to look up.</param>
        /// <param name="sortedLookup">Indices into <paramref name="keys"/> in ascending key order, defining the visit order.</param>
        /// <param name="keyContainer">The container to search.</param>
        /// <param name="lowerBounds">Receives, per lookup, the lower bound from <see cref="FindBoundries"/>.</param>
        /// <param name="upperBounds">Receives, per lookup, the upper bound from <see cref="FindBoundries"/>.</param>
        /// <param name="lookupBuffer">A scratch buffer the implementation may use during the search.</param>
        void FindBoundriesBulk(ReadOnlySpan<K> keys, ReadOnlySpan<int> sortedLookup, in TKeyContainer keyContainer, Span<int> lowerBounds, Span<int> upperBounds, Span<int> lookupBuffer)
        {
            int currentStart = 0;
            int maxEnd = keyContainer.Count - 1;

            for (int i = 0; i < sortedLookup.Length; i++)
            {
                var keyIndex = sortedLookup[i];
                var bounds = FindBoundries(keys[keyIndex], in keyContainer, currentStart, maxEnd);

                lowerBounds[i] = bounds.lowerBounds;
                upperBounds[i] = bounds.upperBounds;

                currentStart = bounds.lowerBounds < 0 ? ~bounds.lowerBounds : bounds.lowerBounds;
            }
        }

        /// <summary>
        /// Finds the inclusive index range of the entries that match the key, searching within the given bounds.
        /// Handles duplicate keys by returning the first and last matching index.
        /// </summary>
        /// <param name="key">The key to locate.</param>
        /// <param name="keyContainer">The container to search.</param>
        /// <param name="startIndex">The first index to search from, inclusive.</param>
        /// <param name="endIndex">The last index to search to, inclusive.</param>
        /// <returns>
        /// The bounds of the matching range, or a range whose bounds are the bitwise complement of the insertion index when not found.
        /// </returns>
        FindBoundriesResult FindBoundries(in K key, in TKeyContainer keyContainer, int startIndex, int endIndex);

        /// <summary>
        /// Compares two keys with each other.
        /// </summary>
        /// <param name="x">The first key.</param>
        /// <param name="y">The second key.</param>
        /// <returns>
        /// A negative number when <paramref name="x"/> is less than <paramref name="y"/>, zero when they are equal,
        /// and a positive number when <paramref name="x"/> is greater.
        /// </returns>
        int CompareTo(in K x, in K y);

        /// <summary>
        /// Compares the key against the key stored at the given index in the container.
        /// </summary>
        /// <param name="key">The key to compare.</param>
        /// <param name="keyContainer">The container holding the key to compare against.</param>
        /// <param name="index">The index of the key in <paramref name="keyContainer"/>.</param>
        /// <returns>
        /// A negative number when <paramref name="key"/> is less than the stored key, zero when they are equal,
        /// and a positive number when <paramref name="key"/> is greater.
        /// </returns>
        int CompareTo(in K key, in TKeyContainer keyContainer, in int index);

        /// <summary>
        /// Set to true if seek in iterator should check the next page for the value if it is not found in the current page
        /// and the key to search was the largest in the page.
        /// </summary>
        bool SeekNextPageForValue { get; }
    }
}
