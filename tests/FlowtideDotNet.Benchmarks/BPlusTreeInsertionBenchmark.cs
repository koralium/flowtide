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

using BenchmarkDotNet.Attributes;
using FlowtideDotNet.Storage.Comparers;
using FlowtideDotNet.Storage.Serializers;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.Tree;
using FASTER.core;
using Microsoft.Extensions.Logging.Abstractions;

namespace DifferntialCompute.Benchmarks
{
    public class BPlusTreeInsertionBenchmark
    {
        private IStateManagerClient nodeClient;
        private IBPlusTree<long, string, ListKeyContainer<long>, ListValueContainer<string>> tree;

        //[Params(1000, 5000, 10000)]
        //public int CachePageCount;

        [GlobalSetup]
        public void GlobalSetup()
        {
            var localStorage = new LocalStorageNamedDeviceFactory(deleteOnClose: true);
            localStorage.Initialize("./data/temp");
            StateManagerSync stateManager = new StateManagerSync<object>(new StateManagerOptions()
            {
                CachePageCount = 1_000_000,
                LogDevice = localStorage.Get(new FileDescriptor("persistent", "perstitent.log")),
                CheckpointDir = "./data",
                TemporaryStorageFactory = localStorage
            }, NullLogger.Instance, new System.Diagnostics.Metrics.Meter("storage"), "storage");
            
            stateManager.InitializeAsync().GetAwaiter().GetResult();

            nodeClient = stateManager.GetOrCreateClient("node1");
        }

        [IterationSetup]
        public void IterationSetup()
        {
            tree = nodeClient.GetOrCreateTree("tree", new BPlusTreeOptions<long, string, ListKeyContainer<long>, ListValueContainer<string>>()
            {
                BucketSize = 1024,
                Comparer = new BPlusTreeListComparer<long>(new LongComparer()),
                KeySerializer = new KeyListSerializer<long>(new LongSerializer()),
                ValueSerializer = new ValueListSerializer<string>(new StringSerializer())
            }).GetAwaiter().GetResult();
            tree.Clear();
        }

        [Benchmark]
        public async Task InsertInOrder()
        {
            for (int i = 0; i < 1_000_000; i++)
            {
                await tree.Upsert(i, $"hello{i}");
            }
        }
    }
}
