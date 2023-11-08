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
using FlowtideDotNet.Storage.Tree.Internal;

namespace FlowtideDotNet.Storage.StateManager.Internal.Sync
{
    internal class StateManagerSyncClient : IStateManagerClient
    {
        private readonly string m_name;
        private readonly StateManagerSync stateManager;

        internal StateManagerSyncClient(string name, StateManagerSync stateManager)
        {
            this.m_name = name;
            this.stateManager = stateManager;
        }

        public IStateManagerClient GetChildManager(string name)
        {
            return new StateManagerSyncClient($"{m_name}_{name}", stateManager);
        }

        public async ValueTask<IBPlusTree<K, V>> GetOrCreateTree<K, V>(string name, BPlusTreeOptions<K, V> options)
        {
            var stateClient = await CreateStateClient<IBPlusTreeNode, BPlusTreeMetadata>(name, new BPlusTreeSerializer<K, V>(options.KeySerializer, options.ValueSerializer));
            var tree = new BPlusTree<K, V>(stateClient, options);
            await tree.InitializeAsync();
            return tree;
        }

        private ValueTask<IStateClient<V, TMetadata>> CreateStateClient<V, TMetadata>(string name, IStateSerializer<V> serializer)
            where V : ICacheObject
        {
            var combinedName = $"{m_name}_{name}";
            return stateManager.CreateClientAsync<V, TMetadata>(combinedName, new StateClientOptions<V>()
            {
                ValueSerializer = serializer
            });
        }
    }
}
