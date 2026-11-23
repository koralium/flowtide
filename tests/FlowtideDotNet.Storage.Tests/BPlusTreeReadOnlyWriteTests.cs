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

using FlowtideDotNet.Storage.Comparers;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Persistence.CacheStorage;
using FlowtideDotNet.Storage.Serializers;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.StateManager.Internal;
using FlowtideDotNet.Storage.StateManager.Internal.Sync;
using FlowtideDotNet.Storage.Tests.BPlusTreeByteBased;
using FlowtideDotNet.Storage.Tree;
using FlowtideDotNet.Storage.Tree.Internal;
using Microsoft.Extensions.Logging.Abstractions;
using System.Diagnostics.Metrics;

namespace FlowtideDotNet.Storage.Tests
{
    public class BPlusTreeReadOnlyWriteTests
    {
        private static async Task<StateManagerSync<object>> CreateStateManager(string name, FileCachePersistentStorage storage)
        {
            var stateManager = new StateManagerSync<object>(new StateManagerOptions()
            {
                CachePageCount = 1000000,
                PersistentStorage = storage
            }, NullLoggerFactory.Instance, new Meter(name), name, GlobalMemoryManager.Instance);
            await stateManager.InitializeAsync();
            return stateManager;
        }

        private static int Modified(IStateClient<IBPlusTreeNode, BPlusTreeMetadata> client)
        {
            return ((SyncStateClient<IBPlusTreeNode, BPlusTreeMetadata>)client).ModifiedCountForTests;
        }

        [Theory]
        [InlineData(4)]
        [InlineData(400)]
        public async Task AWriteThatChangesNothingMarksNoPageModified(int rows)
        {
            var name = "read_only_write_" + rows;
            using var storage = new FileCachePersistentStorage(new FileCacheOptions() { DirectoryPath = $"./data/temp/{name}" });
            using var stateManager = await CreateStateManager(name, storage);
            var tree = (BPlusTree<long, string, ListKeyContainer<long>, ListValueContainer<string>>)await stateManager.GetOrCreateClient("node1")
                .GetOrCreateTree<long, string, ListKeyContainer<long>, ListValueContainer<string>>("tree",
                new BPlusTreeOptions<long, string, ListKeyContainer<long>, ListValueContainer<string>>()
                {
                    BucketSize = 8,
                    Comparer = new BPlusTreeListComparer<long>(new LongComparer()),
                    KeySerializer = new KeyListSerializer<long>(new LongSerializer()),
                    ValueSerializer = new ValueListSerializer<string>(new StringSerializer()),
                    MemoryAllocator = GlobalMemoryManager.Instance
                });
            for (long i = 0; i < rows; i++)
            {
                await tree.Upsert(i * 2, $"{i}");
            }
            await tree.Commit();
            Assert.Equal(0, Modified(tree.m_stateClient));

            for (long i = 0; i < rows * 2; i++)
            {
                var operation = await tree.RMWNoResult(i, default, (input, current, exists) => (current, GenericWriteOperation.None));
                Assert.Equal(GenericWriteOperation.None, operation);
            }

            Assert.Equal(0, Modified(tree.m_stateClient));
            for (long i = 0; i < rows; i++)
            {
                Assert.Equal((true, $"{i}"), await tree.GetValue(i * 2));
            }
        }

        [Theory]
        [InlineData(4)]
        [InlineData(400)]
        public async Task AWriteThatChangesNothingMarksNoPageModifiedWithPagesSizedInBytes(int rows)
        {
            var name = "read_only_write_bytes_" + rows;
            using var storage = new FileCachePersistentStorage(new FileCacheOptions() { DirectoryPath = $"./data/temp/{name}" });
            using var stateManager = await CreateStateManager(name, storage);
            var tree = (BPlusTree<KeyValuePair<long, long>, string, ListKeyContainerWithSize, ListValueContainer<string>>)await stateManager.GetOrCreateClient("node1")
                .GetOrCreateTree<KeyValuePair<long, long>, string, ListKeyContainerWithSize, ListValueContainer<string>>("tree",
                new BPlusTreeOptions<KeyValuePair<long, long>, string, ListKeyContainerWithSize, ListValueContainer<string>>()
                {
                    BucketSize = 8,
                    Comparer = new ListWithSizeComparer(new LongComparer()),
                    KeySerializer = new ListKeyWithSizeSerializer(2000),
                    ValueSerializer = new ValueListSerializer<string>(new StringSerializer()),
                    UseByteBasedPageSizes = true,
                    MemoryAllocator = GlobalMemoryManager.Instance
                });
            for (long i = 0; i < rows; i++)
            {
                await tree.Upsert(new KeyValuePair<long, long>(i * 2, 2000), $"{i}");
            }
            await tree.Commit();
            Assert.Equal(0, Modified(tree.m_stateClient));
            Assert.Equal(rows > 4, tree.m_stateClient.Metadata!.Depth > 1);

            for (long i = 0; i < rows * 2; i++)
            {
                var operation = await tree.RMWNoResult(new KeyValuePair<long, long>(i, 2000), default, (input, current, exists) => (current, GenericWriteOperation.None));
                Assert.Equal(GenericWriteOperation.None, operation);
            }

            Assert.Equal(0, Modified(tree.m_stateClient));
            for (long i = 0; i < rows; i++)
            {
                Assert.Equal((true, $"{i}"), await tree.GetValue(new KeyValuePair<long, long>(i * 2, 2000)));
            }
        }
    }
}
