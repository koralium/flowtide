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
    public interface IBPlusTreeIterator<K, V, TKeyContainer, TValueContainer> : IAsyncEnumerable<IBPlusTreePageIterator<K, V>>
        where TKeyContainer: IKeyContainer<K>
        where TValueContainer: IValueContainer<V>
    {
        /// <summary>
        /// Goes to the beginning of the values
        /// </summary>
        /// <returns></returns>
        ValueTask SeekFirst();

        /// <summary>
        /// Seek position of a specific value
        /// </summary>
        /// <param name="key"></param>
        /// <param name="searchComparer">A custom comparer seperate from the key comparer.</param>
        /// <returns></returns>
        ValueTask Seek(in K key, in IBplusTreeComparer<K, TKeyContainer>? searchComparer = null);
    }
}
