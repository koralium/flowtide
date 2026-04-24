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
using FlowtideDotNet.Storage.Tree;
using Microsoft.Extensions.Logging.Abstractions;
using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Storage.Tests
{
    public class BPlusTreeBackwardTests
    {
        private IBPlusTree<long, string, ListKeyContainer<long>, ListValueContainer<string>> _tree;
        StateManager.StateManagerSync? stateManager;
        public BPlusTreeBackwardTests()
        {
            _tree = Init().GetAwaiter().GetResult();
        }

        private async Task<IBPlusTree<long, string, ListKeyContainer<long>, ListValueContainer<string>>> Init()
        {
            stateManager = new StateManager.StateManagerSync<object>(new StateManagerOptions()
            {
                CachePageCount = 1000000,
                PersistentStorage = new FileCachePersistentStorage(new FileCacheOptions())
            }, NullLoggerFactory.Instance, new Meter($"storage"), "storage");
            await stateManager.InitializeAsync();

            var nodeClient = stateManager.GetOrCreateClient("node1");
            var tree = await nodeClient.GetOrCreateTree<long, string, ListKeyContainer<long>, ListValueContainer<string>>("tree",
                new Tree.BPlusTreeOptions<long, string, ListKeyContainer<long>, ListValueContainer<string>>()
                {
                    BucketSize = 8,
                    Comparer = new BPlusTreeListComparer<long>(new LongComparer()),
                    KeySerializer = new KeyListSerializer<long>(new LongSerializer()),
                    ValueSerializer = new ValueListSerializer<string>(new StringSerializer()),
                    MemoryAllocator = GlobalMemoryManager.Instance,
                    UsePreviousPointers = true
                });
            return tree;
        }

        [Fact]
        public async Task TestInsert()
        {
            for (int i = 0; i < 10; i++)
            {
                await _tree.Upsert(i, $"{i}");
            }
            var it = _tree.CreateBackwardIterator();
            await it.Seek(9);

            int count = 9;
            await foreach (var page in it)
            {
                foreach (var kv in page)
                {
                    Assert.Equal(count, kv.Key);
                    count--;
                }
            }
            Assert.Equal(-1, count);

            // Seek a value in the top that does not exist to make sure it can still iterate backwards
            await it.Seek(10);

            count = 9;
            await foreach (var page in it)
            {
                foreach (var kv in page)
                {
                    Assert.Equal(count, kv.Key);
                    count--;
                }
            }
            Assert.Equal(-1, count);

            // Seek most left value
            await it.Seek(-1);

            count = 0;
            await foreach (var page in it)
            {
                foreach (var kv in page)
                {
                    count++;
                }
            }
            Assert.Equal(0, count);
        }
    }
}
