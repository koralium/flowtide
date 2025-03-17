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

//using BenchmarkDotNet.Attributes;
//using FASTER.core;
//using FlowtideDotNet.Storage;
//using FlowtideDotNet.Storage.Comparers;
//using FlowtideDotNet.Storage.Serializers;
//using FlowtideDotNet.Storage.StateManager;
//using FlowtideDotNet.Storage.Tree;
//using Microsoft.Extensions.Logging.Abstractions;
//using System;
//using System.Collections.Generic;
//using System.Linq;
//using System.Text;
//using System.Threading.Tasks;

//namespace FlowtideDotNet.Benchmarks
//{
//    public class AppendTreeBenchmark
//    {
//        private IStateManagerClient? nodeClient;
//        private IAppendTree<long, string>? tree;

//        [Params(1000, 5000, 10000)]
//        public int CachePageCount;

//        [GlobalSetup]
//        public void GlobalSetup()
//        {
//            var localStorage = new LocalStorageNamedDeviceFactory(deleteOnClose: true);
//            localStorage.Initialize("./data/temp");
//            StateManagerSync stateManager = new StateManagerSync<object>(new StateManagerOptions()
//            {
//                CachePageCount = CachePageCount
//            }, NullLogger.Instance, new System.Diagnostics.Metrics.Meter("storage"), "storage");

//            stateManager.InitializeAsync().GetAwaiter().GetResult();

//            nodeClient = stateManager.GetOrCreateClient("node1");
//        }

//        [IterationSetup]
//        public void IterationSetup()
//        {
//            tree = nodeClient!.GetOrCreateAppendTree<long, string>("tree", new BPlusTreeOptions<long, string>()
//            {
//                BucketSize = 1024,
//                Comparer = new LongComparer(),
//                KeySerializer = new LongSerializer(),
//                ValueSerializer = new StringSerializer()
//            }).GetAwaiter().GetResult();
//            tree.Clear().GetAwaiter().GetResult();
//        }

//        [Benchmark]
//        public async Task Append()
//        {
//            for (int i = 0; i < 1_000_000; i++)
//            {
//                await tree!.Append(i, $"hello{i}");
//            }
//        }
//    }
//}
