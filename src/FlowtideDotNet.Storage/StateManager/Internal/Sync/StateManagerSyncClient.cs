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

using FlowtideDotNet.Storage.AppendTree.Internal;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Queue;
using FlowtideDotNet.Storage.Queue.Internal;
using FlowtideDotNet.Storage.Tree;
using FlowtideDotNet.Storage.Tree.Internal;
using System.Diagnostics;

namespace FlowtideDotNet.Storage.StateManager.Internal.Sync
{
    internal class StateManagerSyncClient : IStateManagerClient
    {
        private readonly string m_name;
        private readonly StateManagerSync stateManager;
        private readonly TagList tagList;

        internal StateManagerSyncClient(string name, StateManagerSync stateManager, TagList tagList)
        {
            this.m_name = name;
            this.stateManager = stateManager;
            this.tagList = tagList;
        }

        public IStateManagerClient GetChildManager(string name)
        {
            return new StateManagerSyncClient($"{m_name}_{name}", stateManager, tagList);
        }

        public async ValueTask<IBPlusTree<K, V, TKeyContainer, TValueContainer>> GetOrCreateTree<K, V, TKeyContainer, TValueContainer>(string name, BPlusTreeOptions<K, V, TKeyContainer, TValueContainer> options)
            where TKeyContainer : IKeyContainer<K>
            where TValueContainer : IValueContainer<V>
        {
            var serializer = new BPlusTreeSerializer<K, V, TKeyContainer, TValueContainer>(options.KeySerializer, options.ValueSerializer, options.MemoryAllocator);
            var stateClient = await CreateStateClient<IBPlusTreeNode, BPlusTreeMetadata>(name, serializer, options.MemoryAllocator);

            if (options.BucketSize == null)
            {
                options.BucketSize = stateClient.BPlusTreePageSize;
            }
            if (options.PageSizeBytes == null)
            {
                options.PageSizeBytes = stateClient.BPlusTreePageSizeBytes;
            }

            var tree = new BPlusTree<K, V, TKeyContainer, TValueContainer>(stateClient, options);
            await tree.InitializeAsync();
            return tree;
        }

        public async ValueTask<IAppendTree<K, V, TKeyContainer, TValueContainer>> GetOrCreateAppendTree<K, V, TKeyContainer, TValueContainer>(string name, BPlusTreeOptions<K, V, TKeyContainer, TValueContainer> options)
            where TKeyContainer : IKeyContainer<K>
            where TValueContainer : IValueContainer<V>
        {
            var stateClient = await CreateStateClient<IBPlusTreeNode, AppendTreeMetadata>(name, new BPlusTreeSerializer<K, V, TKeyContainer, TValueContainer>(options.KeySerializer, options.ValueSerializer, options.MemoryAllocator), options.MemoryAllocator);

            if (options.BucketSize == null)
            {
                options.BucketSize = stateClient.BPlusTreePageSize;
            }

            var tree = new AppendTree<K, V, TKeyContainer, TValueContainer>(stateClient, options);
            await tree.InitializeAsync();
            return tree;
        }


        private async ValueTask<IStateClient<V, TMetadata>> CreateStateClient<V, TMetadata>(string name, IStateSerializer<V> serializer, IMemoryAllocator memoryAllocator)
            where V : ICacheObject
            where TMetadata : class, IStorageMetadata
        {
            var combinedName = $"{m_name}_{name}";

            if (stateManager.SerializeOptions.CompressionType == CompressionType.Zstd && stateManager.SerializeOptions.CompressionMethod == CompressionMethod.Page)
            {
                serializer = new CompressedStateSerializer<V>(serializer, stateManager.SerializeOptions.CompressionLevel.HasValue ? stateManager.SerializeOptions.CompressionLevel.Value : 3, memoryAllocator);
            }

            var stateClient = await stateManager.CreateClientAsync<V, TMetadata>(combinedName, new StateClientOptions<V>()
            {
                ValueSerializer = serializer,
                TagList = tagList
            }, memoryAllocator);
            await stateClient.InitializeSerializerAsync();
            return stateClient;
        }

        public ValueTask<IObjectState<T>> GetOrCreateObjectStateAsync<T>(string name)
        {
            var combinedName = $"{m_name}_{name}";

            return stateManager.CreateObjectStateAsync<T>(combinedName);
        }

        public async ValueTask<IFlowtideQueue<V, TValueContainer>> GetOrCreateQueue<V, TValueContainer>(string name, FlowtideQueueOptions<V, TValueContainer> options) where TValueContainer : IValueContainer<V>
        {
            var stateClient = await CreateStateClient<IBPlusTreeNode, FlowtideQueueMetadata>(name, new FlowtideQueueSerializer<V, TValueContainer>(options.ValueSerializer), options.MemoryAllocator);
            if (options.PageSizeBytes == null)
            {
                options.PageSizeBytes = stateClient.BPlusTreePageSize;
            }
            var queue = new FlowtideQueue<V, TValueContainer>(stateClient, options);
            await queue.InitializeAsync();
            return queue;
        }
    }
}
