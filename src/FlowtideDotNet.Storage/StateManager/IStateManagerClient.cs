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

using FlowtideDotNet.Storage.Tree;

namespace FlowtideDotNet.Storage.StateManager
{
    public interface IStateManagerClient
    {
        /// <summary>
        /// Gets or creates a BPlusTree with the specified name.
        /// </summary>
        /// <param name="name"></param>
        /// <returns></returns>
        ValueTask<IBPlusTree<K, V>> GetOrCreateTree<K, V, TKeyContainer, TValueContainer>(string name, BPlusTreeOptions<K, V, TKeyContainer, TValueContainer> options)
            where TKeyContainer: IKeyContainer<K>
            where TValueContainer: IValueContainer<V>;

        //ValueTask<IAppendTree<K,V>> GetOrCreateAppendTree<K, V>(string name, BPlusTreeOptions<K, V> options);

        IStateManagerClient GetChildManager(string name);
    }
}
