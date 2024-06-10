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
using FlowtideDotNet.Storage.Persistence.CacheStorage;
using FlowtideDotNet.Storage.Serializers;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.StateManager.Internal;
using FlowtideDotNet.Storage.StateManager.Internal.Sync;
using FlowtideDotNet.Storage.Tree;
using FlowtideDotNet.Storage.Tree.Internal;
using Microsoft.Extensions.Logging.Abstractions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Storage.Tests
{
    public class SyncStateClientTests
    {
        /// <summary>
        /// Tests an edge case where a version 1 is evicted and then a two writes are done which becomes version 1 after a commit.
        /// If this is evicted it should overwrite the previous evicted version 1.
        /// </summary>
        /// <returns></returns>
        [Fact]
        public async Task TestEvictVersionZeroAndWriteEvict()
        {
            var persist = new FileCachePersistentStorage(new FileCacheOptions()
            {
                DirectoryPath = "./testEvictVersionZeroAndWriteEvict"
            });
            var a = new StateManagerOptions()
            {
                PersistentStorage = persist,
                CachePageCount = 0,
                UseReadCache = true
            };
            var manager = new StateManagerSync<StateManagerMetadata>(a, NullLogger.Instance, new System.Diagnostics.Metrics.Meter("tmp"), "test");
            await manager.InitializeAsync();
            await manager.LruTable.StopCleanupTask();

            var client = manager.GetOrCreateClient("client");
            var tree = await client.GetOrCreateTree("tree", 
                new BPlusTreeOptions<long, int, ListKeyContainer<long>, ListValueContainer<int>>()
            {
                Comparer = new BPlusTreeListComparer<long>(new LongComparer()),
                KeySerializer = new KeyListSerializer<long>(new LongSerializer()),
                ValueSerializer = new ValueListSerializer<int>(new IntSerializer())
            });
            //Version 0
            await tree.Upsert(1, 1);
            
            await manager.LruTable.ForceCleanup();

            //Version 1
            await tree.Upsert(2, 2);
            await tree.Commit();
            //Version 0
            await tree.Upsert(3, 3);
            //Version 1
            await tree.Upsert(4, 4);
            await manager.LruTable.ForceCleanup();
            var val = await tree.GetValue(2);
            Assert.True(val.found);
            Assert.Equal(2, val.value);
        }
    }
}
