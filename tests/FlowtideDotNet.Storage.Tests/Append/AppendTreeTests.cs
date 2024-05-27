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

using FASTER.core;
using FlowtideDotNet.Storage.AppendTree.Internal;
using FlowtideDotNet.Storage.Comparers;
using FlowtideDotNet.Storage.Persistence.CacheStorage;
using FlowtideDotNet.Storage.Serializers;
using FlowtideDotNet.Storage.StateManager;
using Microsoft.Extensions.Logging.Abstractions;
using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Storage.Tests.Append
{
    public class AppendTreeTests
    {
        [Fact]
        public async Task TestInsertion()
        {
            var localStorage = new LocalStorageNamedDeviceFactory(deleteOnClose: true);
            localStorage.Initialize("./data/temp");
            var stateManager = new StateManager.StateManagerSync<object>(new StateManagerOptions()
            {
                CachePageCount = 1000000,
                PersistentStorage = new FileCachePersistentStorage(new FileCacheOptions())
            }, new NullLogger<StateManagerSync>(), new Meter($"storage"), "storage");
            await stateManager.InitializeAsync();
            var nodeClient = stateManager.GetOrCreateClient("node1");
            var appendTree = await nodeClient.GetOrCreateAppendTree("test", new Tree.BPlusTreeOptions<long, long>()
            {
                BucketSize = 32,
                Comparer = new LongComparer(),
                KeySerializer = new LongSerializer(),
                ValueSerializer = new LongSerializer(),
            });

            for (int i = 0; i < 1000_000; i++)
            {
                await appendTree.Append(i, i);
            }
            //await appendTree.Append(6, 6);
            //await appendTree.Append(7, 7);
            //await appendTree.Append(8, 8);

            await appendTree.Prune(999_950);

            var graph = await appendTree.Print();
        }
    }
}
