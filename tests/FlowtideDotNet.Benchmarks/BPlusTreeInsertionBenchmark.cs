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
using FASTER.core;
using FlowtideDotNet.Storage.Comparers;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Serializers;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.Tree;
using Microsoft.Extensions.Logging.Abstractions;
using System.Diagnostics;

namespace DifferntialCompute.Benchmarks
{
    public class BPlusTreeInsertionBenchmark
    {
        private IStateManagerClient? nodeClient;
        private IBPlusTree<long, string, ListKeyContainer<long>, ListValueContainer<string>>? tree;

        [GlobalSetup]
        public void GlobalSetup()
        {
            var localStorage = new LocalStorageNamedDeviceFactory(deleteOnClose: true);
            localStorage.Initialize("./data/temp");
            StateManagerSync stateManager = new StateManagerSync<object>(new StateManagerOptions()
            {
                CachePageCount = 1_000_000
            }, NullLogger.Instance, new System.Diagnostics.Metrics.Meter("storage"), "storage");

            stateManager.InitializeAsync().GetAwaiter().GetResult();

            nodeClient = stateManager.GetOrCreateClient("node1");
        }

        [IterationSetup]
        public void IterationSetup()
        {
            Debug.Assert(nodeClient != null);
            tree = nodeClient.GetOrCreateTree("tree", new BPlusTreeOptions<long, string, ListKeyContainer<long>, ListValueContainer<string>>()
            {
                BucketSize = 1024,
                Comparer = new BPlusTreeListComparer<long>(new LongComparer()),
                KeySerializer = new KeyListSerializer<long>(new LongSerializer()),
                ValueSerializer = new ValueListSerializer<string>(new StringSerializer()),
                MemoryAllocator = GlobalMemoryManager.Instance
            }).GetAwaiter().GetResult();
            tree.Clear();
        }

        [Benchmark]
        public async Task InsertInOrder()
        {
            Debug.Assert(tree != null);
            for (int i = 0; i < 1_000_000; i++)
            {
                await tree.Upsert(i, $"hello{i}");
            }
        }
    }
}
