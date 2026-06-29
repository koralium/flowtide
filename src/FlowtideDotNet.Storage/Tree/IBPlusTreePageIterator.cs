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

using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.Tree.Internal;

namespace FlowtideDotNet.Storage.Tree
{
    /// <summary>
    /// A view over a single leaf page yielded while iterating a B+ tree. It enumerates the page's key/value entries,
    /// exposes the page's <see cref="Keys"/> and <see cref="Values"/> containers, supports write locking, and can persist
    /// in-place edits through <see cref="SavePage"/>.
    /// </summary>
    /// <typeparam name="K">The key type.</typeparam>
    /// <typeparam name="V">The value type.</typeparam>
    /// <typeparam name="TKeyContainer">The container used to store the keys of the page.</typeparam>
    /// <typeparam name="TValueContainer">The container used to store the values of the page.</typeparam>
    public interface IBPlusTreePageIterator<K, V, TKeyContainer, TValueContainer> : IEnumerable<KeyValuePair<K, V>>, ILockableObject
        where TKeyContainer : IKeyContainer<K>
        where TValueContainer : IValueContainer<V>
    {
        /// <summary>
        /// Saves the current page, allows the user to modify values on the page and then trigger a save.
        /// </summary>
        /// <param name="checkForResize">
        /// When true, checks whether the page has grown past the page size and, if so, triggers a tree traversal so the page can be split.
        /// </param>
        ValueTask SavePage(bool checkForResize);

        /// <summary>
        /// Get the current leaf node, should only be used for very specific scenarios.
        /// </summary>
        LeafNode<K, V, TKeyContainer, TValueContainer> CurrentPage { get; }

        /// <summary>
        /// The key container of the current page.
        /// </summary>
        TKeyContainer Keys { get; }

        /// <summary>
        /// The value container of the current page.
        /// </summary>
        TValueContainer Values { get; }
    }
}
