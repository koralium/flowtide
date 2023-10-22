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
using FASTER.core;

namespace FlowtideDotNet.Storage.Tests
{
    public class UnitTest1
    {
        [Fact]
        public async Task Test1()
        {
            StateManager.StateManager stateManager = new StateManager.StateManager<object>(new StateManagerOptions()
            {
                LogDevice = Devices.CreateLogDevice("./data/persistent")
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


            for (int i = 0; i < 10; i++)
            {
                await tree.Upsert(i, $"hello{i}");
            }

            var print = await tree.Print();
            var visualUrl = KrokiUrlBuilder.ToKrokiUrl(print);
        }

        [Fact]
        public async Task TestCache()
        {
            var localStorage = new LocalStorageNamedDeviceFactory(deleteOnClose: true);
            localStorage.Initialize("./data/temp");
            StateManager.StateManager stateManager = new StateManager.StateManager<object>(new StateManagerOptions()
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


            for (int i = 0; i < 100; i++)
            {
                await tree.Upsert(i, $"hello{i}");
            }

            var iterator = tree.CreateIterator();
            await iterator.Seek(101);

            await foreach(var page in iterator)
            {
                foreach(var kv in page)
                {

                }
            }

            var (found, val) = await tree.GetValue(28);

            await tree.Commit();
            await stateManager.CheckpointAsync();
            var print = await tree.Print();
            var visualUrl = KrokiUrlBuilder.ToKrokiUrl(print);
        }

        [Fact]
        public async Task TestInsertDelete()
        {
            var localStorage = new LocalStorageNamedDeviceFactory(deleteOnClose: true);
            localStorage.Initialize("./data/temp");
            StateManager.StateManager stateManager = new StateManager.StateManager<object>(new StateManagerOptions()
            {
                CachePageCount = 10,
                LogDevice = Devices.CreateLogDevice("./data/persistent.log"),
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


            for (int i = 0; i < 100; i++)
            {
                await tree.Upsert(i, $"hello{i}");
            }

            for (int i = 0; i < 100; i++)
            {
                await tree.Delete(i);
            }

            await tree.Commit();
            await stateManager.CheckpointAsync();
            var print = await tree.Print();
            var visualUrl = KrokiUrlBuilder.ToKrokiUrl(print);
        }

        [Fact]
        public async Task TestOneMillion()
        {
            var localStorage = new LocalStorageNamedDeviceFactory(deleteOnClose: true);
            localStorage.Initialize("./data/temp");
            StateManager.StateManager stateManager = new StateManager.StateManager<object>(new StateManagerOptions()
            {
                CachePageCount = 1000,
                LogDevice = localStorage.Get(new FileDescriptor("persistent", "perstitent.log")),
                CheckpointDir = "./data",
                TemporaryStorageFactory = localStorage
            });

            await stateManager.InitializeAsync();

            var nodeClient = stateManager.GetOrCreateClient("node1");
            var tree = await nodeClient.GetOrCreateTree<long, string>("tree", new Tree.BPlusTreeOptions<long, string>()
            {
                BucketSize = 512,
                Comparer = new LongComparer(),
                KeySerializer = new LongSerializer(),
                ValueSerializer = new StringSerializer()
            });


            for (int i = 0; i < 1_000_000; i++)
            {
                await tree.Upsert(i, $"hello{i}");
            }

            await stateManager.CheckpointAsync();
            //var iterator = tree.CreateIterator();
            //await iterator.Seek(101);

            //await foreach (var page in iterator)
            //{
            //    foreach (var kv in page)
            //    {

            //    }
            //}

            //var (found, val) = await tree.GetValue(28);

            //await tree.Commit();
            //await stateManager.CheckpointAsync();
            //var print = await tree.Print();
            //var visualUrl = KrokiUrlBuilder.ToKrokiUrl(print);
        }
    }
}