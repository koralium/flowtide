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
using FlowtideDotNet.Storage.Queue;
using FlowtideDotNet.Storage.Serializers;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.StateManager.Internal;
using FlowtideDotNet.Storage.StateManager.Internal.Sync;
using FlowtideDotNet.Storage.Tree;
using FlowtideDotNet.Storage.Tree.Internal;
using System.Buffers;

namespace FlowtideDotNet.Storage.Tests
{
    public class EphemeralStateClientTests
    {
        private static readonly TimeSpan Timeout = TimeSpan.FromMinutes(1);

        // Counts serializer lifecycle calls around the shared test serializer
        private sealed class CountingSerializer : IStateSerializer<TestPage>
        {
            private readonly TestPageSerializer _inner = new TestPageSerializer();
            public int Initializations;
            public int Checkpoints;

            public Task CheckpointAsync<TMetadata>(IStateSerializerCheckpointWriter checkpointWriter, StateClientMetadata<TMetadata> metadata)
                where TMetadata : IStorageMetadata
            {
                Interlocked.Increment(ref Checkpoints);
                return _inner.CheckpointAsync(checkpointWriter, metadata);
            }

            public Task InitializeAsync<TMetadata>(IStateSerializerInitializeReader reader, StateClientMetadata<TMetadata> metadata)
                where TMetadata : IStorageMetadata
            {
                Interlocked.Increment(ref Initializations);
                return _inner.InitializeAsync(reader, metadata);
            }

            public void Serialize(in IBufferWriter<byte> bufferWriter, in TestPage value) => _inner.Serialize(bufferWriter, value);

            public TestPage Deserialize(ReadOnlySequence<byte> bytes, int length) => _inner.Deserialize(bytes, length);

            public void Serialize(in IBufferWriter<byte> bufferWriter, in ICacheObject value) => _inner.Serialize(bufferWriter, value);

            public ICacheObject DeserializeCacheObject(ReadOnlySequence<byte> bytes, int length) => _inner.DeserializeCacheObject(bytes, length);

            public void ClearTemporaryAllocations() => _inner.ClearTemporaryAllocations();

            public void Dispose() => _inner.Dispose();
        }

        internal static async Task<IStateClient<TestPage, TestMetadata>> CreateClient(EphemeralHarness harness, string name, bool ephemeral, IStateSerializer<TestPage>? serializer = null)
        {
            var client = await harness.Manager.CreateClientAsync<TestPage, TestMetadata>(name, new StateClientOptions<TestPage>() { ValueSerializer = serializer ?? new TestPageSerializer() }, GlobalMemoryManager.Instance, ephemeral);
            await client.InitializeSerializerAsync();
            return client;
        }

        internal static SyncStateClient<TestPage, TestMetadata> Sync(IStateClient<TestPage, TestMetadata> client) => (SyncStateClient<TestPage, TestMetadata>)client;

        internal static SpyFileCache Spy<V, TMetadata>(IStateClient<V, TMetadata> client)
            where V : ICacheObject
            where TMetadata : class, IStorageMetadata => (SpyFileCache)((SyncStateClient<V, TMetadata>)client).FileCacheForTests;

        internal static List<long> AddPages(IStateClient<TestPage, TestMetadata> client, int count, int firstValue = 0)
        {
            var keys = new List<long>();
            for (int i = 0; i < count; i++)
            {
                var key = client.GetNewPageId();
                client.AddOrUpdate(key, new TestPage(firstValue + i));
                keys.Add(key);
            }
            return keys;
        }

        private static async Task AssertValue(IStateClient<TestPage, TestMetadata> client, long key, int expected)
        {
            var page = await client.GetValue(key);
            Assert.NotNull(page);
            try
            {
                Assert.Equal(expected, page!.Value);
            }
            finally
            {
                page!.Return();
            }
        }

        private static BPlusTreeOptions<long, int, ListKeyContainer<long>, ListValueContainer<int>> TreeOptions() => new BPlusTreeOptions<long, int, ListKeyContainer<long>, ListValueContainer<int>>()
        {
            BucketSize = 8,
            Comparer = new BPlusTreeListComparer<long>(new LongComparer()),
            KeySerializer = new KeyListSerializer<long>(new LongSerializer()),
            ValueSerializer = new ValueListSerializer<int>(new IntSerializer()),
            MemoryAllocator = GlobalMemoryManager.Instance
        };

        [Fact]
        public async Task DeleteForgetsAPageThatWasNeverPersisted()
        {
            using var harness = await EphemeralHarness.Create("c1");
            var client = await CreateClient(harness, "client", ephemeral: true);
            var keys = AddPages(client, 100);
            for (int i = 0; i < 60; i++)
            {
                client.Delete(keys[i]);
            }

            Assert.Equal(40, Sync(client).ModifiedCountForTests);
            Assert.Equal(keys.Skip(60).OrderBy(k => k), Sync(client).ModifiedKeysForTests.OrderBy(k => k));
            for (int i = 0; i < 60; i++)
            {
                Assert.False(harness.Manager.TryPeekCacheEntry(keys[i], out _), $"page {keys[i]} is still cached");
            }
        }

        [Fact]
        public async Task DefaultClientKeepsTheDeleteForItsCommit()
        {
            using var harness = await EphemeralHarness.Create("c2");
            var client = await CreateClient(harness, "client", ephemeral: false);
            var keys = AddPages(client, 2);
            await client.Commit();
            await harness.Manager.CheckpointAsync();
            Assert.True(harness.Storage.TryGetValue(keys[0], out _));

            client.Delete(keys[0]);
            // The tombstone stays until the commit writes the delete.
            Assert.Equal(1, Sync(client).ModifiedCountForTests);
            Assert.Contains(keys[0], Sync(client).ModifiedKeysForTests);

            await client.Commit();
            await harness.Manager.CheckpointAsync();
            await harness.Restart();

            Assert.False(harness.Storage.TryGetValue(keys[0], out _));
            Assert.True(harness.Storage.TryGetValue(keys[1], out _));
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task CommitOfAnEphemeralClientWritesNothing(bool backgroundCommit)
        {
            using var harness = await EphemeralHarness.Create($"c3_{backgroundCommit}", backgroundCommit: backgroundCommit);
            var sessionsBefore = harness.Storage.Sessions.Count;
            var client = await CreateClient(harness, "client", ephemeral: true);
            Assert.Equal(sessionsBefore + 1, harness.Storage.Sessions.Count);
            var session = harness.Storage.Sessions[sessionsBefore];
            client.Metadata = new TestMetadata() { Updated = true };
            var keys = AddPages(client, 10);
            client.Delete(keys[0]);

            await client.Commit();
            await harness.Manager.CheckpointAsync();

            Assert.Empty(session.TotalWrites);
            Assert.False(harness.Storage.TryGetValue(Sync(client).MetadataId, out _), "the client metadata page was written");
            foreach (var key in keys)
            {
                Assert.False(harness.Storage.TryGetValue(key, out _), $"page {key} was written");
            }
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task EvictionSpillsEveryLiveEphemeralPage(bool useReadCache)
        {
            using var harness = await EphemeralHarness.Create($"c5_{useReadCache}", cachePageCount: 0, useReadCache: useReadCache);
            var client = await CreateClient(harness, "client", ephemeral: true);
            var sync = Sync(client);
            var keys = AddPages(client, 200);
            await harness.Manager.CacheTable.ForceCleanup();
            for (int i = 0; i < keys.Count; i++)
            {
                await AssertValue(client, keys[i], i);
            }
            var live = new List<long>();
            for (int i = 0; i < keys.Count; i++)
            {
                if (i % 2 == 0)
                {
                    client.Delete(keys[i]);
                }
                else
                {
                    live.Add(keys[i]);
                }
            }
            await harness.Manager.CacheTable.ForceCleanup();

            Assert.Equal(live.Count, sync.ModifiedCountForTests);
            Assert.Equal(live.OrderBy(k => k), sync.SpillVersionKeysForTests.OrderBy(k => k));
            Assert.Equal(live.OrderBy(k => k), Spy(client).Inner.GetLayoutForTests().Allocations.Keys.OrderBy(k => k));
            for (int i = 1; i < keys.Count; i += 2)
            {
                await AssertValue(client, keys[i], i);
            }
        }

        [Fact]
        public async Task ResetEmptiesAnEphemeralClient()
        {
            using var harness = await EphemeralHarness.Create("c6", cachePageCount: 0);
            var client = await CreateClient(harness, "client", ephemeral: true);
            var sync = Sync(client);
            var keys = AddPages(client, 50);
            await harness.Manager.CacheTable.ForceCleanup();
            Assert.NotEmpty(sync.SpillVersionKeysForTests);

            await client.Reset(true);

            Assert.Equal(0, sync.ModifiedCountForTests);
            Assert.Empty(sync.SpillVersionKeysForTests);
            Assert.Empty(Spy(client).Inner.GetLayoutForTests().Allocations);
        }

        public enum StateKind
        {
            Tree,
            AppendTree,
            Queue,
            ObjectState
        }

        private static async Task OpenOrdinary(IStateManagerClient client, StateKind kind, string name)
        {
            switch (kind)
            {
                case StateKind.Tree:
                    await client.GetOrCreateTree(name, TreeOptions());
                    break;
                case StateKind.AppendTree:
                    await client.GetOrCreateAppendTree(name, TreeOptions());
                    break;
                case StateKind.Queue:
                    await client.GetOrCreateQueue(name, new FlowtideQueueOptions<long, PrimitiveListValueContainer<long>>()
                    {
                        MemoryAllocator = GlobalMemoryManager.Instance,
                        ValueSerializer = new PrimitiveListValueContainerSerializer<long>(GlobalMemoryManager.Instance)
                    });
                    break;
                case StateKind.ObjectState:
                    await client.GetOrCreateObjectStateAsync<int>(name);
                    break;
            }
        }

        // StateExists takes the manager lock, a dedicated thread keeps a held lock from pinning the pool
        private static async Task AssertManagerLockIsFree(EphemeralHarness harness)
        {
            var probe = Task.Factory.StartNew(() => harness.Manager.StateExists("lock_probe"), CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default);
            var completed = await Task.WhenAny(probe, Task.Delay(TimeSpan.FromSeconds(30)));
            Assert.True(completed == probe, "the manager lock is still held after the rejected open");
        }

        [Theory]
        [InlineData(StateKind.Tree, true)]
        [InlineData(StateKind.AppendTree, true)]
        [InlineData(StateKind.Queue, true)]
        [InlineData(StateKind.ObjectState, true)]
        [InlineData(StateKind.Tree, false)]
        [InlineData(StateKind.AppendTree, false)]
        [InlineData(StateKind.Queue, false)]
        [InlineData(StateKind.ObjectState, false)]
        public async Task LiveCollisionsInvolvingAnEphemeralClientThrow(StateKind kind, bool ephemeralFirst)
        {
            // Not disposed on failure, a held manager lock would hang the dispose.
            var harness = await EphemeralHarness.Create($"c7_{kind}_{ephemeralFirst}");
            var client = harness.Manager.GetOrCreateClient("node");
            if (ephemeralFirst)
            {
                var first = (BPlusTree<long, int, ListKeyContainer<long>, ListValueContainer<int>>)await client.GetOrCreateEphemeralTree("state", TreeOptions());
                var registered = harness.Manager.RegisteredClientCountForTests;

                await Assert.ThrowsAsync<InvalidOperationException>(async () => await OpenOrdinary(client, kind, "state"));
                Assert.Equal(registered, harness.Manager.RegisteredClientCountForTests);
                await AssertManagerLockIsFree(harness);

                var reopened = (BPlusTree<long, int, ListKeyContainer<long>, ListValueContainer<int>>)await client.GetOrCreateEphemeralTree("state", TreeOptions());
                Assert.Same(first.m_stateClient, reopened.m_stateClient);
            }
            else
            {
                await OpenOrdinary(client, kind, "state");
                var registered = harness.Manager.RegisteredClientCountForTests;

                await Assert.ThrowsAsync<InvalidOperationException>(async () => await client.GetOrCreateEphemeralTree("state", TreeOptions()));
                Assert.Equal(registered, harness.Manager.RegisteredClientCountForTests);
                await AssertManagerLockIsFree(harness);
            }
            harness.Dispose();
        }

        [Fact]
        public async Task SerializerIsInitializedButNeverCheckpointed()
        {
            using var harness = await EphemeralHarness.Create("c8");
            var serializer = new CountingSerializer();
            var client = await CreateClient(harness, "client", ephemeral: true, serializer);
            AddPages(client, 5);

            await client.Commit();
            await harness.Manager.CheckpointAsync();

            Assert.True(serializer.Initializations >= 1);
            Assert.Equal(0, serializer.Checkpoints);
        }

        [Fact]
        public async Task LookupSlotCollisionKeepsTheOtherPage()
        {
            using var harness = await EphemeralHarness.Create("c9");
            var client = await CreateClient(harness, "client", ephemeral: true);
            var sync = Sync(client);
            var keys = AddPages(client, 1010);
            var first = keys[0];
            var other = keys.Single(k => k == first + 1009);
            // Reads the other page into the shared slot.
            await AssertValue(client, other, keys.IndexOf(other));
            var hits = sync.LookupTableHitsForTests;

            client.Delete(first);
            await AssertValue(client, other, keys.IndexOf(other));

            Assert.Equal(hits + 1, sync.LookupTableHitsForTests);
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task VictimCapturedBeforeAnEphemeralDeleteWritesNothing(bool useReadCache)
        {
            using var harness = await EphemeralHarness.Create($"c10_{useReadCache}", useReadCache: useReadCache);
            var client = await CreateClient(harness, "client", ephemeral: true);
            var sync = Sync(client);
            var key = AddPages(client, 1)[0];
            Assert.True(harness.Manager.TryPeekCacheEntry(key, out var entry));

            client.Delete(key);
            await sync.Evict(new List<(S3FifoCacheEntry, long)>() { (entry, entry.Version) }, false);

            Assert.False(Spy(client).Inner.Exists(key));
            Assert.False(sync.HasSpillVersionForTests(key));
        }

        [Fact]
        public async Task DispatchReachesTheEphemeralImplementations()
        {
            var fileProvider = new FlowtideDotNet.Storage.Persistence.Reservoir.MemoryDisk.MemoryFileProvider();
            using (var harness = await EphemeralHarness.Create("c11", fileProvider: fileProvider))
            {
                var node = harness.Manager.GetOrCreateClient("node");
                var sessionsBefore = harness.Storage.Sessions.Count;
                var tree = (BPlusTree<long, int, ListKeyContainer<long>, ListValueContainer<int>>)await node.GetOrCreateEphemeralTree("tree", TreeOptions());
                Assert.Equal(sessionsBefore + 1, harness.Storage.Sessions.Count);
                var session = harness.Storage.Sessions[sessionsBefore];
                Assert.IsType<EphemeralSyncStateClient<IBPlusTreeNode, BPlusTreeMetadata>>(tree.m_stateClient);
                // A never committed ordinary tree, reopened after the restart below.
                var ordinary = (BPlusTree<long, int, ListKeyContainer<long>, ListValueContainer<int>>)await node.GetOrCreateTree("ordinary", TreeOptions());
                Assert.IsType<SyncStateClient<IBPlusTreeNode, BPlusTreeMetadata>>(ordinary.m_stateClient);

                await ChurnAndAssertForgotten(tree);

                await tree.Commit();
                await harness.Manager.CheckpointAsync();
                Assert.Empty(session.TotalWrites);

                var reopened = (BPlusTree<long, int, ListKeyContainer<long>, ListValueContainer<int>>)await node.GetOrCreateEphemeralTree("tree", TreeOptions());
                Assert.Same(tree.m_stateClient, reopened.m_stateClient);
                await ChurnAndAssertForgotten(reopened);

                // The restart reopens both through the binding that has no page.
                await harness.Restart();
                var restartedNode = harness.Manager.GetOrCreateClient("node");
                var restarted = (BPlusTree<long, int, ListKeyContainer<long>, ListValueContainer<int>>)await restartedNode.GetOrCreateEphemeralTree("tree", TreeOptions());
                Assert.IsType<EphemeralSyncStateClient<IBPlusTreeNode, BPlusTreeMetadata>>(restarted.m_stateClient);
                await ChurnAndAssertForgotten(restarted);
                var restartedOrdinary = (BPlusTree<long, int, ListKeyContainer<long>, ListValueContainer<int>>)await restartedNode.GetOrCreateTree("ordinary", TreeOptions());
                Assert.IsType<SyncStateClient<IBPlusTreeNode, BPlusTreeMetadata>>(restartedOrdinary.m_stateClient);
            }
        }

        private static async Task ChurnAndAssertForgotten(BPlusTree<long, int, ListKeyContainer<long>, ListValueContainer<int>> tree)
        {
            for (int i = 0; i < 400; i++)
            {
                await tree.Upsert(i, i);
            }
            for (int i = 0; i < 400; i++)
            {
                await tree.Delete(i);
            }
            var reachable = await TreeWalk.ReachablePages(tree);
            var client = (SyncStateClient<IBPlusTreeNode, BPlusTreeMetadata>)tree.m_stateClient;
            Assert.Equal(reachable.Pages.OrderBy(k => k), client.ModifiedKeysForTests.OrderBy(k => k));
        }

        [Fact]
        public async Task BuiltInFileCacheReadsCompleteSynchronously()
        {
            using var fileCache = new FlowtideDotNet.Storage.FileCache.FileCache(new FileCacheOptions() { DirectoryPath = "./data/ephemeral_c12" }, "c12", GlobalMemoryManager.Instance);
            var serializer = new TestPageSerializer();
            fileCache.Write(1, new SerializableObject(new TestPage(7), serializer));

            var raw = fileCache.Read(1);
            var typed = fileCache.Read(1, serializer);

            Assert.True(raw.IsCompletedSuccessfully);
            Assert.True(typed.IsCompletedSuccessfully);
            var page = await typed;
            Assert.Equal(7, page.Value);
        }
    }

    [Collection("EphemeralSerial")]
    public class EphemeralStateClientRaceTests
    {
        private static readonly TimeSpan Timeout = TimeSpan.FromMinutes(1);

        [Fact]
        public async Task EphemeralDeleteFreesASpillWrittenDuringTheDelete()
        {
            using var harness = await EphemeralHarness.Create("c4");
            for (int rep = 0; rep < 50; rep++)
            {
                var client = await EphemeralStateClientTests.CreateClient(harness, $"client{rep}", ephemeral: true);
                var sync = EphemeralStateClientTests.Sync(client);
                var spy = EphemeralStateClientTests.Spy(client);
                var key = EphemeralStateClientTests.AddPages(client, 1)[0];
                Assert.True(harness.Manager.TryPeekCacheEntry(key, out var entry));

                using var gate = new ManualResetEventSlim(false);
                var hookReached = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                ((EphemeralSyncStateClient<TestPage, TestMetadata>)sync).BeforeCacheDeleteHookForTests = _ => hookReached.TrySetResult();
                var writeEntered = spy.ArmWriteGate(key, gate);
                Task? evict = null;
                Task? delete = null;
                try
                {
                    // Parks inside the spill write, holding the entry lock with the version published.
                    evict = Task.Run(() => sync.Evict(new List<(S3FifoCacheEntry, long)>() { (entry, entry.Version) }, false));
                    await writeEntered.WaitAsync(Timeout);

                    delete = Task.Run(() => client.Delete(key));
                    await hookReached.Task.WaitAsync(Timeout);
                    // The delete waits for the entry lock the spill holds.
                    Assert.False(delete.IsCompleted);
                }
                finally
                {
                    gate.Set();
                    if (evict != null)
                    {
                        await evict.WaitAsync(Timeout);
                    }
                    if (delete != null)
                    {
                        await delete.WaitAsync(Timeout);
                    }
                }

                Assert.False(spy.Inner.Exists(key), $"rep {rep}: the spill written during the delete was orphaned");
                Assert.False(sync.HasSpillVersionForTests(key), $"rep {rep}: a spill version remains");
                Assert.DoesNotContain(key, sync.ModifiedKeysForTests);
            }
        }
    }
}
