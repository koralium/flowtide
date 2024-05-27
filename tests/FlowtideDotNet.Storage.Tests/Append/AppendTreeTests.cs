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
using FlowtideDotNet.Storage.Persistence.FasterStorage;
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
    public class AppendTreeTests : IDisposable
    {
        StateManager.StateManagerSync? stateManager;

        public void Dispose()
        {
            if (stateManager != null)
            {
                stateManager.Dispose();
            }
        }

        private async Task<IAppendTree<long, long>> CreateTree(int bucketSize = 8, string path = "./data/temp", bool deleteOnClose = true)
        {
            stateManager = new StateManager.StateManagerSync<object>(new StateManagerOptions()
            {
                CachePageCount = 1000000,
                PersistentStorage = new FasterKvPersistentStorage(new FasterKVSettings<long, SpanByte>(path, deleteOnClose))
            }, new NullLogger<StateManagerSync>(), new Meter($"storage"), "storage");
            await stateManager.InitializeAsync();

            var nodeClient = stateManager.GetOrCreateClient("node1");
            var tree = await nodeClient.GetOrCreateAppendTree<long, long>("tree", new Tree.BPlusTreeOptions<long, long>()
            {
                BucketSize = bucketSize,
                Comparer = new LongComparer(),
                KeySerializer = new LongSerializer(),
                ValueSerializer = new LongSerializer()
            });
            return tree;
        }

        [Fact]
        public async Task TestNewRootFromLeaf()
        {
            var tree = await CreateTree(1);

            await tree.Append(1, 1);
            await tree.Append(2, 2);

            var graph = KrokiUrlBuilder.ToKrokiUrl(await tree.Print());

            // Check the graph
            Assert.Equal(
                "https://kroki.io/graphviz/svg/eJyskM2KwyAUhfeBvMPF9bSp_dl0oi8SujDxJhFERYVZlL57NakDocximFko3nM8ng-lmrxwM0xwr6vgtDIYWN7ryliJ0IVZOAQGxhr8mFFNc2R7evtc_UunRY86-W0bRa8ReuslekYOBAbUuox0HYMTgzJTtnkbfVoSnPWRkTErTZRZ4rScikmL2eRQs1RxXihOf6d4r0xRb7_SVcPIcVO_TbwRkeVjyHU8wI7DMp3ICnr-B9Djr0FfiZ9B6TfoOYG-iLcydIM1IXqhTGSj0AHzC4-6egIAAP__AwD4E7Oj",
                graph);
        }

        [Fact]
        public async Task PruneFrom2LevelTo1Level()
        {
            var tree = await CreateTree(1);

            await tree.Append(1, 1);
            await tree.Append(2, 2);

            await tree.Prune(2);

            var graph = KrokiUrlBuilder.ToKrokiUrl(await tree.Print());

            // Check the graph
            Assert.Equal(
                "https://kroki.io/graphviz/svg/eJxUjsEKwyAMhu-FvkPwPNa17Db1RcoOtmYqiIoKPYy9-6Jlhx0S8uf7fxLtTFbJgoH3OJTkXcAiWh-HEDXCWqxKCAJCDHix6Iyt4jo_Hye_r15t6IlzXtXmEbaYNWbBbgx29P4n51OWpHYXTMOS10yl5cIn6jRBirkK9qJojgdZg2AL-Tqeuvsv0VdTPytl--gzDl8AAAD__wMA3OtCLQ==",
                graph);
        }

        [Fact]
        public async Task TestNewRootFromInternalNode()
        {
            var tree = await CreateTree(1);

            await tree.Append(1, 1);
            await tree.Append(2, 2);
            await tree.Append(3, 3);
            await tree.Append(4, 4);

            var graph = KrokiUrlBuilder.ToKrokiUrl(await tree.Print());

            Assert.Equal(
                "https://kroki.io/graphviz/svg/eJy0ks1ugzAMx-9IvIOV8zbKRyntCC-CdgiQAlKUoCTSDtPefeEjrbJqrdDoIVHsvx3_ZLvpW0mGDlr48j01sJ5Thcfb97hoKJSqIwMFDFxw-tLRvu00fgs_3mf9UDJSUWb0PNekYhQqIRsqMdohqClj1gxnUw2k7nk7ykWupTkNDEJqjM6jJ9DN6Coi-7JiaMVgTAqmUkVhKfbPoQjXUcT_p7gtaVKl-DShHKPIKe9m3BChqTHodN7BawGTFaMZNNkANFoNGj0CDS-giQFdiF03lLXgSkvSc43PhCl6-SFxQtN7oQenK_ulK9lzlihet0TpBrOJV88mvjebzGlXurTruAFoshp0yfgb9LoCR7tE6S_3g824hmbmh2_f-wEAAP__AwDXnZbc",
                graph);
        }

        [Fact]
        public async Task PruneFrom3LevelTo2Level()
        {
            var tree = await CreateTree(1);

            await tree.Append(1, 1);
            await tree.Append(2, 2);
            await tree.Append(3, 3);
            await tree.Append(4, 4);

            await tree.Prune(3);

            var graph = KrokiUrlBuilder.ToKrokiUrl(await tree.Print());

            Assert.Equal(
                "https://kroki.io/graphviz/svg/eJyskMGKwyAURfeB_MPD9bRpZkppO9EfCbMw8SURREWFWZT--2hSB0LporQLxXev13tQyNFxO8EIl7LwVkmNnqa9LLQRCK2fuEWgoI3GjwnlOAW6rX--F__YKt6hin7TBN4phM44gY6SHYEelcpjvYze8l7qMdmsCS4uAda4QMmQlCqIJLGvfMpmnc0qhaq5irFMcXid4r4yRp35jVc1JZ-r-nXijojMH0POww42DObpQBbQ0xtA90-D3hKPQet_0FMEvRGvZWh7o31wXOpAB648pheuZfEHAAD__wMAGG2zzg==",
                graph);
        }

        [Fact]
        public async Task TestCommit()
        {
            if (Directory.Exists("./data/temp/testcommit"))
            {
                Directory.Delete("./data/temp/testcommit", true);
            }
            
            var tree = await CreateTree(1, "./data/temp/testcommit", false);

            await tree.Append(1, 1);
            await tree.Append(2, 2);

            await tree.Commit();
            await stateManager!.CheckpointAsync();
            stateManager!.Dispose();
            tree = await CreateTree(1, "./data/temp/testcommit", false);

            var graph = KrokiUrlBuilder.ToKrokiUrl(await tree.Print());

            // Check the graph
            Assert.Equal(
                "https://kroki.io/graphviz/svg/eJyskM2KwyAUhfeBvMPF9bSp_dl0oi8SujDxJhFERYVZlL57NakDocximFko3nM8ng-lmrxwM0xwr6vgtDIYWN7ryliJ0IVZOAQGxhr8mFFNc2R7evtc_UunRY86-W0bRa8ReuslekYOBAbUuox0HYMTgzJTtnkbfVoSnPWRkTErTZRZ4rScikmL2eRQs1RxXihOf6d4r0xRb7_SVcPIcVO_TbwRkeVjyHU8wI7DMp3ICnr-B9Djr0FfiZ9B6TfoOYG-iLcydIM1IXqhTGSj0AHzC4-6egIAAP__AwD4E7Oj",
                graph);
        }

        [Fact]
        public async Task InsertOutOfOrder()
        {
            var tree = await CreateTree(1);

            await tree.Append(2, 2);

            var ex = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            {
                await tree.Append(1, 1);
            });
            Assert.Equal("Key must be greater than the last key in the right node.", ex.Message);
        }

    }
}
