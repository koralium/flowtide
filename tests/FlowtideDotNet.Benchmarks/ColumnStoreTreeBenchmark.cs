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
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Storage.Comparers;
using FlowtideDotNet.Storage.Serializers;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.Tree;
using Microsoft.Extensions.Logging.Abstractions;
using Substrait.Protobuf;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Benchmarks
{
    public class ColumnStoreTreeBenchmark
    {
        private IStateManagerClient nodeClient;
        private IBPlusTree<ColumnRowReference, string, ColumnKeyStorageContainer, ListValueContainer<string>> tree;

        //[Params(1000, 5000, 10000)]
        //public int CachePageCount;
        private EventBatchData data = new EventBatchData(new List<Column>()
        {
            new Column()
        });

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

            for (int i = 0; i < 1_000_000; i++)
            {
                data.Columns[0].Add(new Int64Value(i));
            }
        }

        [IterationSetup]
        public void IterationSetup()
        {
            tree = nodeClient.GetOrCreateTree("tree", new BPlusTreeOptions<ColumnRowReference, string, ColumnKeyStorageContainer, ListValueContainer<string>>()
            {
                BucketSize = 1024,
                Comparer = new ColumnComparer(1),
                KeySerializer = new ColumnStoreSerializer(),
                ValueSerializer = new ValueListSerializer<string>(new StringSerializer())
            }).GetAwaiter().GetResult();
            tree.Clear();
        }

        [Benchmark]
        public async Task InsertInOrder()
        {
            for (int i = 0; i < 1_000_000; i++)
            {
                await tree.Upsert(new ColumnRowReference()
                {
                    referenceBatch = data,
                    RowIndex = i
                }, $"hello{i}");
            }
        }
    }
}
