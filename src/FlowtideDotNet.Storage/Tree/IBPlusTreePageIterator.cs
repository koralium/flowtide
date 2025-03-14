﻿// Licensed under the Apache License, Version 2.0 (the "License")
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
    public interface IBPlusTreePageIterator<K, V, TKeyContainer, TValueContainer> : IEnumerable<KeyValuePair<K, V>>, ILockableObject
        where TKeyContainer : IKeyContainer<K>
        where TValueContainer : IValueContainer<V>
    {
        /// <summary>
        /// Saves the current page, allows the user to modify values on the page and then trigger a save.
        /// </summary>
        /// <returns></returns>
        ValueTask SavePage(bool checkForResize);

        /// <summary>
        /// Get the current leaf node, should only be used for very specific scenarios.
        /// </summary>
        LeafNode<K, V, TKeyContainer, TValueContainer> CurrentPage { get; }

        TKeyContainer Keys { get; }

        TValueContainer Values { get; }
    }
}
