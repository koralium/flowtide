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
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Persistence.Reservoir.Internal;
using FlowtideDotNet.Storage.Persistence.Reservoir.MemoryDisk;
using FlowtideDotNet.Storage.Serializers;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.Tree;
using FlowtideDotNet.Storage.AppendTree.Internal;
using Microsoft.Extensions.Logging.Abstractions;
using System.Diagnostics.Metrics;

namespace FlowtideDotNet.Storage.Tests.Append
{
    public class AppendTreeTests : IDisposable
    {
        private MemoryFileProvider _provider = new MemoryFileProvider();
        StateManager.StateManagerSync? stateManager;

        public void Dispose()
        {
            if (stateManager != null)
            {
                stateManager.Dispose();
            }
        }

        private async Task<IAppendTree<long, long, ListKeyContainer<long>, ListValueContainer<long>>> CreateTree(int bucketSize = 8, int cachePageCount = 1000000)
        {
            stateManager = new StateManager.StateManagerSync<object>(new StateManagerOptions()
            {
                CachePageCount = cachePageCount,
                PersistentStorage = new ReservoirPersistentStorage(new Persistence.Reservoir.ReservoirStorageOptions()
                {
                    FileProvider = _provider,
                })
            }, NullLoggerFactory.Instance, new Meter($"storage"), "storage", GlobalMemoryManager.Instance);
            await stateManager.InitializeAsync();

            var nodeClient = stateManager.GetOrCreateClient("node1");
            var tree = await nodeClient.GetOrCreateAppendTree("tree", new Tree.BPlusTreeOptions<long, long, ListKeyContainer<long>, ListValueContainer<long>>()
            {
                BucketSize = bucketSize,
                Comparer = new BPlusTreeListComparer<long>(new LongComparer()),
                KeySerializer = new KeyListSerializer<long>(new LongSerializer()),
                ValueSerializer = new ValueListSerializer<long>(new LongSerializer()),
                MemoryAllocator = GlobalMemoryManager.Instance
            });
            return tree;
        }

        [Fact]
        public async Task DisposingAnAppendTreeEnumeratorTwicePreservesTheTreeAndCacheRents()
        {
            var (manager, storage) = await BackgroundCommitTests.CreateManager("repeated_enumerator_disposal");
            using var storageLifetime = storage;
            using var managerLifetime = manager;
            var tree = await BackgroundCommitTests.CreateAppendTree(manager);
            await tree.Append(1, 42);
            var concrete = (AppendTree<long, long, ListKeyContainer<long>, ListValueContainer<long>>)tree;
            Assert.True(manager.TryPeekCacheEntry(concrete.m_stateClient.Metadata!.Root, out var entry));
            var originalRents = entry.Value.RentCount;
            using var iterator = tree.CreateIterator();
            await iterator.Seek(1);
            await using (var enumerator = iterator.GetAsyncEnumerator())
            {
                Assert.True(await enumerator.MoveNextAsync());
                Assert.Equal(new KeyValuePair<long, long>(1, 42), enumerator.Current);
                await enumerator.DisposeAsync();
            }

            // Repeated disposal must preserve tree and cache ownership.
            Assert.Equal(originalRents, entry.Value.RentCount);
            await tree.Append(2, 43);
            await iterator.Seek(1);
            await using var remaining = iterator.GetAsyncEnumerator();
            Assert.True(await remaining.MoveNextAsync());
            Assert.Equal(new KeyValuePair<long, long>(1, 42), remaining.Current);
            Assert.True(await remaining.MoveNextAsync());
            Assert.Equal(new KeyValuePair<long, long>(2, 43), remaining.Current);
            Assert.False(await remaining.MoveNextAsync());
        }

        [Fact]
        public async Task FailedAppendTreeSeekDoesNotReturnThePreviousLeafTwice()
        {
            var timeout = TimeSpan.FromSeconds(30);
            var (manager, storage) = await BackgroundCommitTests.CreateManager("review_failed_seek");
            using var storageLifetime = storage;
            using var managerLifetime = manager;
            var tree = await BackgroundCommitTests.CreateAppendTree(manager);
            for (long i = 0; i < 40; i++) await tree.Append(i, i);
            var concrete = (AppendTree<long, long, ListKeyContainer<long>, ListValueContainer<long>>)tree;
            var leafId = concrete.m_stateClient.Metadata!.Left;
            Assert.True(manager.TryPeekCacheEntry(leafId, out var entry));
            var cacheRent = entry.Value.RentCount;
            var iterator = tree.CreateIterator();
            await iterator.Seek(0);
            var session = storage.Sessions.Single();
            session.FaultingKeys[concrete.m_stateClient.Metadata.Root] = 1;
            using var gate = new BackgroundCommitTests.WalkGate(manager);
            try
            {
                await tree.Commit().AsTask().WaitAsync(timeout);
                await gate.Blocked.WaitAsync(timeout);
                // Failed seeks must return each leaf only once.
                await Assert.ThrowsAsync<IOException>(() => iterator.Seek(0).AsTask());
                iterator.Dispose();
                Assert.Equal(cacheRent, entry.Value.RentCount);
            }
            finally
            {
                gate.Release();
            }
        }

        [Fact]
        public async Task FailedAppendTreePageAdvanceDoesNotReturnThePreviousLeafTwice()
        {
            var timeout = TimeSpan.FromSeconds(30);
            var (manager, storage) = await BackgroundCommitTests.CreateManager("review_failed_advance");
            using var storageLifetime = storage;
            using var managerLifetime = manager;
            var tree = await BackgroundCommitTests.CreateAppendTree(manager);
            for (long i = 0; i < 40; i++) await tree.Append(i, i);
            var concrete = (AppendTree<long, long, ListKeyContainer<long>, ListValueContainer<long>>)tree;
            Assert.True(manager.TryPeekCacheEntry(concrete.m_stateClient.Metadata!.Left, out var entry));
            var leaf = (Tree.Internal.LeafNode<long, long, ListKeyContainer<long>, ListValueContainer<long>>)entry.Value;
            var cacheRent = leaf.RentCount;
            using var iterator = tree.CreateIterator();
            await iterator.Seek(0);
            var enumerator = iterator.GetAsyncEnumerator();
            var enumeratorDisposed = false;
            using var gate = new BackgroundCommitTests.WalkGate(manager);
            try
            {
                for (var i = 0; i < leaf.keys.Count; i++) Assert.True(await enumerator.MoveNextAsync());
                var session = storage.Sessions.Single();
                session.FaultingKeys[leaf.next] = 1;
                await tree.Commit().AsTask().WaitAsync(timeout);
                await gate.Blocked.WaitAsync(timeout);
                // Failed advances preserve the cache's existing leaf rent.
                await Assert.ThrowsAsync<IOException>(() => enumerator.MoveNextAsync().AsTask());
                await enumerator.DisposeAsync();
                enumeratorDisposed = true;
                Assert.Equal(cacheRent, leaf.RentCount);
            }
            finally
            {
                if (!enumeratorDisposed) await enumerator.DisposeAsync();
                gate.Release();
            }
        }

        [Fact]
        public async Task PruningToTheRightmostLeafReturnsTheFetchedLeafRent()
        {
            var tree = await CreateTree(bucketSize: 16);
            for (long i = 0; i < 40; i++) await tree.Append(i, i);
            var concrete = (AppendTree<long, long, ListKeyContainer<long>, ListValueContainer<long>>)tree;
            var rightId = concrete.m_stateClient.Metadata!.Right;
            Assert.NotEqual(rightId, concrete.m_stateClient.Metadata.Root);
            Assert.True(stateManager!.TryPeekCacheEntry(rightId, out var entry));
            var rentsBeforePrune = entry.Value.RentCount;

            await tree.Prune(40);

            Assert.Equal(rightId, concrete.m_stateClient.Metadata.Root);
            // Root collapse must return its fetched leaf rent.
            Assert.Equal(rentsBeforePrune, entry.Value.RentCount);
        }

        [Fact]
        public async Task PrintingTheTreeReturnsTheFetchedRootRent()
        {
            var tree = await CreateTree();
            await tree.Append(1, 1);
            var concrete = (AppendTree<long, long, ListKeyContainer<long>, ListValueContainer<long>>)tree;
            Assert.True(stateManager!.TryPeekCacheEntry(concrete.m_stateClient.Metadata!.Root, out var entry));
            var rentsBeforePrinting = entry.Value.RentCount;

            await tree.Print();

            // Printing must return the fetched root rent.
            Assert.Equal(rentsBeforePrinting, entry.Value.RentCount);
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
        public async Task PruneFrom3LevelTo1Level()
        {
            var tree = await CreateTree(1);

            await tree.Append(1, 1);
            await tree.Append(2, 2);
            await tree.Append(3, 3);
            await tree.Append(4, 4);

            var beforePruneGraph = KrokiUrlBuilder.ToKrokiUrl(await tree.Print());

            await tree.Prune(3);

            var graph = KrokiUrlBuilder.ToKrokiUrl(await tree.Print());

            Assert.Equal(
                "https://kroki.io/graphviz/svg/eJxUjsEKwyAMhu-FvkPwPNZ17DKmvkjZwdZMBVFRoYexd1-07LBDQv58_0-inckqWTDwHoeSvAtYROvjEKJGWIpVCUFAiAFPFp2xVZzn5-Pg98WrFT1xzqtaPcIas8Ys2IXBht7_5HzIktTmgmlY8pqptLzxiTpNkGKugr0omuNO1iDYlXwdT939l-irqZ-Vsn30GYcvAAAA__8DAOCQQjY=",
                graph);
        }

        [Fact]
        public async Task PruneFrom4LevelTo2Level()
        {
            var tree = await CreateTree(1);

            await tree.Append(1, 1);
            await tree.Append(2, 2);
            await tree.Append(3, 3);
            await tree.Append(4, 4);
            await tree.Append(5, 5);
            await tree.Append(6, 6);
            await tree.Append(7, 7);
            await tree.Append(8, 8);

            var beforePruneGraph = KrokiUrlBuilder.ToKrokiUrl(await tree.Print());

            Assert.Equal(
                "https://kroki.io/graphviz/svg/eJy0ltGOqyAQhu-b9B2I17unjija3eqLNHtBK1UTgkZJ9mKz777QSittgiFHLzTCDM6Xf_4AZVP1tKtRhX62m6HjjWBDrt_bjWhLho5DTTuGciRawd5q1lS1zP_B1-ctDtGR0xPjKuFwkPTEGTq1fcn6PAgDdGacmyHchkNHz42odLg4yF49JeraXubBRc_sZKmnith8mSCY4E4v2l1LFYXBSNehiPwoknUowI8C_z_Fa0m1tG-_VarIg8gqb694IQquwgQflxC9F-g6wsENNF4ANPIGjeZA4Q4aK9CR2J5Gx3MrBtnTRsj8QvnA7n-IrVTiSk0tVZJRlWwdE2E_E5EFeoO9e4Ndvcksucgo134B0NgbNJ4DfVhgb0xEnqYdzthbqRDOueiRm5lqEFlypaNcgNexF_GzF8A6GIknRriAexJv9yQu9wBYjVPNH1mX2C6JNyuZZZ0Y9b5hKuqngMPBKmolJ85kbMsDRh6yjqNST0ctcAd4rTnbpdTZJWJLZk4aWODWVGTerOMKB-vEC3rfMtRPgTmTTJL1YfHYFScBfRX53W7-AAAA__8DAHNTYsU=",
                beforePruneGraph);

            await tree.Prune(1);

            var prune1Graph = KrokiUrlBuilder.ToKrokiUrl(await tree.Print());

            Assert.Equal(
                "https://kroki.io/graphviz/svg/eJy0Vl1vgyAUfW_S_0B43qb4gXar_pFmD7RSNSFokGQPy_77oJVW2gVDpg9-cM-Fezz3BKzaWpC-ATX43m6GnrWcDoW-bze8qyg4DA3pKSgA7zh9aWhbN7J4Q58fVxxFB0aOlKmE_V6SI6Pg2ImKigKGEJwoY2aIrsOhJ6eW1xou91KoqwJ9J2QBzzoSyEqHysS8GRAZMNCTgkupsjQ0snVYRH4s0hVY_Fko-X-h529TU0X3pVJ5ASOrvD3jiRG8fDt8P4fgtQSXUQLHcKLC6BbGEBxOHR-kIC2XxZmwgd5WyKwVUrWCfubrdDb26yxeQPDYW_DYJXhuyYVHuXYLEE28iSZzRO8W2Bln4Iewwxk7KxWFcy665-amGoosubJRLhSvYy_sZy-E1qGRetIIF3BP6u2e1OUehKzGqeaPXJfYA7E3VzzLdWLU2y6oWD8ADgcr1EpOncmxLQ8y8uB1HJV5OmqBI_G55myXMmeXsC2ZOWnQAj8RZe7NdZzh4Drxgt63DOsHYM4kk2R9WNx3xQkQK-Bnu_kFAAD__wMAXSsYgA==",
                prune1Graph);

            await tree.Prune(2);

            var prune2Graph = KrokiUrlBuilder.ToKrokiUrl(await tree.Print());

            Assert.Equal(
                "https://kroki.io/graphviz/svg/eJy0ldFugyAUhu-b9B0I19vqqYptV3yRZhe0UjUhaJBkF8vefdBKJ-1iQ6YXGuE_eD7O-QNFXSrWVqhEX8tF14pa8o7a93Ihm4KjQ1exliOKZCP5S8XrstL0DT7erzqsD4IduTAB-71mR8HRsVEFVxRHGJ24EG4I12HXslMtSyvne63MU6C2UZris51Z6cJO5Yn7ciI4cWUXrS6p8txhZDNQ_JloM89247Dtkv9TPKY0S1XzaUIlxWsvvb_igQhfCoN35wi95ugyIvgKup0ANAkGTZ6Bwg10a0B7Yn8aHU6N7LRitdT0zETHb3_YeqEQjcVmXlk2LhusvfmsLxfE89iLhNkLYB6MNBAjmsA9abB70jH3AHiNM83vWZMJWEkwK3nKOjBqcnNfdC-MONioXnA6Ghz75QFXHjKPo7JAR6UTdCkL7lI22iXilyx1JZvgTss3waz9ihHWgRfsueWo74RnJhkE28vi91QcCLERvpeLHwAAAP__AwD5Mqbw",
                prune2Graph);

            await tree.Prune(3);

            var prune3Graph = KrokiUrlBuilder.ToKrokiUrl(await tree.Print());

            Assert.Equal(
                "https://kroki.io/graphviz/svg/eJy8VMtugzAQvCPxD5bPbcEkPNLG_pGoBwccQLIMsi31UPXfa0gc4aQisgo5gPDMrnfYHW3V1pL2DajBdxionreCKTy8w0B0FQMH1dCeAQxEJ9hLw9q60fgNfX6ceZQcOD0ybgL2e02PnIFjJysmMYwhKBnn9ojOR9XTshX1QJO9luapQN9JjeFpQCJdDRDZ2i9LIktGQ1I0liLEyshXUPFnoeJZhXb_L3TfRJMquy8TKjBMnPJuxp0iOP47fD_F4JWA8bSDF3hnYHSFkalxKDuhtKSt0PhEuWLXK3LnisJegRIHzw0-wpt1zJX5mQuhdWSknjLiBSyRelsinbMEQs7gzPAvWrcLaM28tWYPtU6Mur26L74lZhxsWCc4nQ3euO1Btj3ZOo7KPR2VLjCl3HtK-eyUMrdlqW3ZAiueFN5aLxkzWideGPaWVX1DPDLJJDhztuKE2BjiJwx-AQAA__8DADcHXJQ=",
                prune3Graph);

            await tree.Prune(4);

            var graph = KrokiUrlBuilder.ToKrokiUrl(await tree.Print());

            Assert.Equal(
                "https://kroki.io/graphviz/svg/eJy0ks9qhDAQxu-C7zDk3FbT1Vha44tID1HjHwiJJIEeSt-9sW52111wkboHg5NvJvPjm2mGTrOxhw6-w8CMYpDc0OkMA6kaDqXp2ciBglSSP_V86HpLX_Dnx6zjQylYxYVLyHPLKsGhUrrhmqIYQc2F8CGeQzOyepDdJBe51e5rYFTaUtRON5FtpquC-D8vYi9GU1H016ooThj4MRjpRoz4_xi3PV2pVl8uVVL0uui_rLhBQrM16L2N4bmAOYzRkTXZgZVsZiV3WfGZNXGsnvpKgLJW0ljNBmlpy4Th51eSZXK6mnxY2oO9PeQxG5Vt3Kh0hyllm6eUrU6JLC1LvWXZDqxvm1mPFSusF7uQnTYqvRbuLclFMnGv_ITBLwAAAP__AwAjSpua",
                graph);
        }

        [Fact]
        public async Task TestCommit()
        {
            var tree = await CreateTree(1);

            await tree.Append(1, 1);
            await tree.Append(2, 2);

            await tree.Commit();
            await stateManager!.CheckpointAsync();
            stateManager!.Dispose();
            tree = await CreateTree(1);

            var printed = await tree.Print();
            var graph = KrokiUrlBuilder.ToKrokiUrl(printed);

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

        [Fact]
        public async Task TestIterator()
        {
            var tree = await CreateTree(2);

            for (int i = 0; i < 10; i++)
            {
                await tree.Append(i, i);
            }

            var iterator = tree.CreateIterator();
            await iterator.Seek(0);

            int counter = 0;
            await foreach (var kv in iterator)
            {
                Assert.Equal(counter, kv.Key);
                Assert.Equal(counter, kv.Value);
                counter++;
            }
        }

        [Fact]
        public async Task TestIteratorEmptyTree()
        {
            var tree = await CreateTree(2);

            var iterator = tree.CreateIterator();
            await iterator.Seek(0);

            int counter = 0;
            await foreach (var kv in iterator)
            {
                counter++;
            }

            Assert.Equal(0, counter);
        }

        /// <summary>
        /// Iterator dispose must return rented leaf node reference.
        /// </summary>
        [Fact]
        public async Task IteratorDisposeReleasesRentedNodeWhenNotEnumerated()
        {
            var tree = await CreateTree(bucketSize: 2);
            for (int i = 0; i < 10; i++)
            {
                await tree.Append(i, i);
            }
            var iterator = tree.CreateIterator();
            await iterator.Seek(0);
            iterator.Dispose();

            // Iterator dispose must return rented leaf node reference.
            var appendTree = (AppendTree<long, long, ListKeyContainer<long>, ListValueContainer<long>>)tree;
            var node = await appendTree.FindLeafNode(0, new BPlusTreeListComparer<long>(new LongComparer()));
            try
            {
                Assert.Equal(2, node.RentCount);
            }
            finally
            {
                node.Return();
            }
        }

        /// <summary>
        /// Multiple enumerations must not dispose active leaf node.
        /// </summary>
        [Fact]
        public async Task IteratorMultipleEnumerationDoesNotDisposeCachedNode()
        {
            var tree = await CreateTree(bucketSize: 2);
            for (int i = 0; i < 10; i++)
            {
                await tree.Append(i, i);
            }
            var iterator = tree.CreateIterator();
            await iterator.Seek(0);

            var e1 = iterator.GetAsyncEnumerator();
            var e2 = iterator.GetAsyncEnumerator();
            await e1.DisposeAsync();
            await e2.DisposeAsync();

            // Multiple enumerations must not dispose active leaf node.
            var it2 = tree.CreateIterator();
            await it2.Seek(0);
            var count = 0;
            await foreach (var _ in it2)
            {
                count++;
            }
            Assert.Equal(10, count);
        }

        [Fact]
        public async Task IteratorRetainsTheSplitLeafUntilItsEnumeratorIsDisposed()
        {
            var timeout = TimeSpan.FromSeconds(30);
            var (manager, storage) = await BackgroundCommitTests.CreateManager("iterator_split_ownership", cachePageCount: 0);
            using var storageLifetime = storage;
            using var managerLifetime = manager;
            var tree = await BackgroundCommitTests.CreateAppendTree(manager);
            for (long i = 0; i < 16; i++)
            {
                await tree.Append(i, i);
            }

            var concrete = (AppendTree<long, long, ListKeyContainer<long>, ListValueContainer<long>>)tree;
            var leafId = concrete.m_stateClient.Metadata!.Right;
            Assert.True(manager.TryPeekCacheEntry(leafId, out var entry));
            var heldNode = entry.Value;
            Assert.Equal(2, heldNode.RentCount);
            using var iterator = tree.CreateIterator();
            await iterator.Seek(0);
            Assert.Equal(3, heldNode.RentCount);

            await using (var enumerator = iterator.GetAsyncEnumerator())
            {
                Assert.Equal(3, heldNode.RentCount);
                for (long i = 16; i < 48; i++)
                {
                    await tree.Append(i, i);
                }

                Assert.NotEqual(leafId, concrete.m_stateClient.Metadata.Right);
                await manager.CacheTable.ForceCleanup().WaitAsync(timeout);
                Assert.True(manager.TryPeekCacheEntry(leafId, out var retainedEntry));
                Assert.Same(heldNode, retainedEntry.Value);
                Assert.Equal(2, heldNode.RentCount);
                for (long i = 0; i < 16; i++)
                {
                    Assert.True(await enumerator.MoveNextAsync());
                    Assert.Equal(new KeyValuePair<long, long>(i, i), enumerator.Current);
                }
            }

            Assert.Equal(1, heldNode.RentCount);
            await manager.CacheTable.ForceCleanup().WaitAsync(timeout);
            Assert.False(manager.TryPeekCacheEntry(leafId, out _));
            Assert.Equal(0, heldNode.RentCount);
        }

        /// <summary>
        /// Root split updates metadata across multiple checkpoints.
        /// </summary>
        [Fact]
        public async Task AppendTreeRootSplitPersistsUpdatedMetadataOnSecondCommit()
        {
            var tree = await CreateTree(bucketSize: 2);
            await tree.Append(1, 1);

            // First commit persists initial single-leaf tree metadata.
            await tree.Commit();
            await stateManager!.CheckpointAsync();

            // Insert enough elements to split leaf and create new root.
            await tree.Append(2, 2);
            await tree.Append(3, 3);
            await tree.Append(4, 4);

            // Second commit must persist updated root metadata.
            await tree.Commit();
            await stateManager.CheckpointAsync();

            stateManager.Dispose();
            stateManager = null;

            // Reopen tree from storage to verify recovered root.
            var recoveredTree = await CreateTree(bucketSize: 2);
            var iterator = recoveredTree.CreateIterator();
            await iterator.Seek(4);
            var enumerator = iterator.GetAsyncEnumerator();
            var hasValue = await enumerator.MoveNextAsync();
            Assert.True(hasValue);
            Assert.Equal(4, enumerator.Current.Key);
        }
    }
}


