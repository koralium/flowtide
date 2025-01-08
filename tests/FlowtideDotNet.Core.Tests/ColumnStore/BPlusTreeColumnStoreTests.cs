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
using FlowtideDotNet.Storage.Comparers;
using FlowtideDotNet.Storage.Persistence.CacheStorage;
using FlowtideDotNet.Storage.Serializers;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.Tree;
using FlowtideDotNet.Storage;
using Microsoft.EntityFrameworkCore.ChangeTracking.Internal;
using Microsoft.Extensions.Logging.Abstractions;
using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Storage.Memory;

namespace FlowtideDotNet.Core.Tests.ColumnStore
{
    public class BPlusTreeColumnStoreTests
    {
        [Fact]
        public async Task TestSplit()
        {
            var stateManager = new StateManagerSync<object>(new StateManagerOptions()
            {
                CachePageCount = 1000000,
                PersistentStorage = new FileCachePersistentStorage(new FileCacheOptions())
            }, new NullLogger<StateManagerSync>(), new Meter($"storage"), "storage");
            await stateManager.InitializeAsync();

            var nodeClient = stateManager.GetOrCreateClient("node1");
            var tree = await nodeClient.GetOrCreateTree<ColumnRowReference, string, ColumnKeyStorageContainer, ListValueContainer<string>>("tree",
                new BPlusTreeOptions<ColumnRowReference, string, ColumnKeyStorageContainer, ListValueContainer<string>>()
                {
                    BucketSize = 4,
                    Comparer = new ColumnComparer(1),
                    KeySerializer = new ColumnStoreSerializer(1, GlobalMemoryManager.Instance),
                    ValueSerializer = new ValueListSerializer<string>(new StringSerializer()),
                    MemoryAllocator = GlobalMemoryManager.Instance
                });

            var valueColumn = new Column(GlobalMemoryManager.Instance);
            for (int i = 0; i < 10; i++)
            {
                valueColumn.Add(new Int64Value(i));
            }
            var insertBatch = new EventBatchData([ valueColumn ]);

            for (int i = 0; i < 4; i++)
            {

                await tree.Upsert(new ColumnRowReference()
                {
                    referenceBatch = insertBatch,
                    RowIndex = i
                }, i.ToString());
            }

            var printedTree = await tree.Print();
        }
    }
}
