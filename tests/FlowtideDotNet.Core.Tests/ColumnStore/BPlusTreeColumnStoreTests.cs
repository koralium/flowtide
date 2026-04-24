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

using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Storage;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Persistence.CacheStorage;
using FlowtideDotNet.Storage.Serializers;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.Tree;
using Microsoft.Extensions.Logging.Abstractions;
using System.Diagnostics.Metrics;

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
            }, NullLoggerFactory.Instance, new Meter($"storage"), "storage");
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
            var insertBatch = new EventBatchData([valueColumn]);

            for (int i = 0; i < 4; i++)
            {

                await tree.Upsert(new ColumnRowReference()
                {
                    referenceBatch = insertBatch,
                    RowIndex = i
                }, i.ToString());
            }

            var printedTree = await tree.Print();
            var expected = @"digraph g {
splines=line
node [shape = none,height=.1];
node4[label = <<table border=""0"" cellborder=""1"" cellspacing=""0""><tr><td port=""f0""></td><td>{1}</td><td port=""f1""></td></tr></table>>];
node3[label = <<table border=""0"" cellborder=""1"" cellspacing=""0""><tr><td>{0}</td><td>{1}</td><td port=""f0"" rowspan=""2""></td></tr><tr><td>0</td><td>1</td></tr></table>>];
""node4"":f0 -> ""node3""
node5[label = <<table border=""0"" cellborder=""1"" cellspacing=""0""><tr><td>{2}</td><td>{3}</td><td port=""f0"" rowspan=""2""></td></tr><tr><td>2</td><td>3</td></tr></table>>];
""node4"":f1 -> ""node5""
""node3"":f1 -> ""node5"" [constraint=false];
}
";
            Assert.Equal(expected, printedTree);
        }
    }
}
