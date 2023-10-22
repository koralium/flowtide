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
using FlowtideDotNet.Storage.Serializers;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.Tree;
using FASTER.core;

namespace FlowtideDotNet.Storage.Tests
{
    public class BPlusTreeTests
    {
        private IBPlusTree<long, string> _tree;
        public BPlusTreeTests()
        {
            _tree = Init().GetAwaiter().GetResult();
        }

        private async Task<IBPlusTree<long, string>> Init()
        {
            var localStorage = new LocalStorageNamedDeviceFactory(deleteOnClose: true);
            localStorage.Initialize("./data/temp");
            StateManager.StateManagerAdvanced stateManager = new StateManager.StateManagerAdvanced<object>(new StateManagerOptions()
            {
                CachePageCount = 10,
                LogDevice = Devices.CreateLogDevice("./data/persistent2.log"),
                CheckpointDir = "./data",
                TemporaryStorageFactory = localStorage
            });
            await stateManager.InitializeAsync();

            var nodeClient = stateManager.GetOrCreateClient("node1");
            var tree = await nodeClient.GetOrCreateTree<long, string>("tree", new Tree.BPlusTreeOptions<long, string>()
            {
                BucketSize = 8,
                Comparer = new LongComparer(),
                KeySerializer = new LongSerializer(),
                ValueSerializer = new StringSerializer()
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
            var it = _tree.CreateIterator();
            await it.SeekFirst();

            int count = 0;
            await foreach(var page in it)
            {
                foreach(var kv in page)
                {
                    Assert.Equal(count, kv.Key);
                    count++;
                }
            }
            Assert.Equal(10, count);
        }

        [Fact]
        public async Task TestDelete()
        {
            for (int i = 0; i < 10; i++)
            {
                await _tree.Upsert(i, $"{i}");
            }

            await _tree.Delete(3);
            await _tree.Delete(9);

            var graph = KrokiUrlBuilder.ToKrokiUrl(await _tree.Print());
            var it = _tree.CreateIterator();
            await it.SeekFirst();

            List<long> expected = new List<long>();
            expected.Add(0);
            expected.Add(1);
            expected.Add(2);
            expected.Add(4);
            expected.Add(5);
            expected.Add(6);
            expected.Add(7);
            expected.Add(8);

            int count = 0;
            await foreach (var page in it)
            {
                foreach (var kv in page)
                {
                    Assert.Equal(expected[count], kv.Key);
                    count++;
                }
            }
            Assert.Equal(8, count);
        }

        [Fact]
        public async Task TestSplitInternalNode()
        {
            for (int i = 0; i < 1000; i++)
            {
                await _tree.Upsert(i, $"{i}");
            }

            var it = _tree.CreateIterator();
            await it.SeekFirst();

            int count = 0;
            await foreach (var page in it)
            {
                foreach (var kv in page)
                {
                    Assert.Equal(count, kv.Key);
                    count++;
                }
            }
            Assert.Equal(1000, count);
        }

        [Fact]
        public async Task TestMergeInternalNodeWithRight()
        {
            for (int i = 0; i < 1000; i++)
            {
                await _tree.Upsert(i, $"{i}");
            }

            for (int i = 0; i < 800; i++)
            {
                await _tree.Delete(i);
            }

            var it = _tree.CreateIterator();
            await it.SeekFirst();

            int count = 800;
            await foreach (var page in it)
            {
                foreach (var kv in page)
                {
                    Assert.Equal(count, kv.Key);
                    count++;
                }
            }
            Assert.Equal(1000, count);
        }

        [Fact]
        public async Task TestMergeInternalNodeWithLeft()
        {
            for (int i = 0; i < 1000; i++)
            {
                await _tree.Upsert(i, $"{i}");
            }

            for (int i = 1000; i >= 200; i--)
            {
                await _tree.Delete(i);
            }

            var it = _tree.CreateIterator();
            await it.SeekFirst();

            int count = 0;
            await foreach (var page in it)
            {
                foreach (var kv in page)
                {
                    Assert.Equal(count, kv.Key);
                    count++;
                }
            }
            Assert.Equal(200, count);
        }
    }
}
