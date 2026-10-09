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

using FlowtideDotNet.Storage.AppendTree;
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
using System.Runtime.CompilerServices;

namespace FlowtideDotNet.Storage.Tests
{
    public class EphemeralTreeTests
    {
        private sealed class ClientWithoutEphemeralTrees : IStateManagerClient
        {
            public ValueTask<IBPlusTree<K, V, TKeyContainer, TValueContainer>> GetOrCreateTree<K, V, TKeyContainer, TValueContainer>(string name, BPlusTreeOptions<K, V, TKeyContainer, TValueContainer> options)
                where TKeyContainer : IKeyContainer<K>
                where TValueContainer : IValueContainer<V> => throw new InvalidOperationException("not called");

            public ValueTask<IAppendTree<K, V, TKeyContainer, TValueContainer>> GetOrCreateAppendTree<K, V, TKeyContainer, TValueContainer>(string name, BPlusTreeOptions<K, V, TKeyContainer, TValueContainer> options)
                where TKeyContainer : IKeyContainer<K>
                where TValueContainer : IValueContainer<V> => throw new InvalidOperationException("not called");

            public ValueTask<IFlowtideQueue<V, TValueContainer>> GetOrCreateQueue<V, TValueContainer>(string name, FlowtideQueueOptions<V, TValueContainer> options)
                where TValueContainer : IValueContainer<V> => throw new InvalidOperationException("not called");

            public ValueTask<IObjectState<T>> GetOrCreateObjectStateAsync<T>(string name) => throw new InvalidOperationException("not called");

            public bool StateExists(string name) => throw new InvalidOperationException("not called");

            public IStateManagerClient GetChildManager(string name) => throw new InvalidOperationException("not called");
        }

        [Fact]
        public async Task TheInterfaceDefaultThrows()
        {
            IStateManagerClient client = new ClientWithoutEphemeralTrees();
            await Assert.ThrowsAsync<NotSupportedException>(async () => await client.GetOrCreateEphemeralTree("tree", new BPlusTreeOptions<long, int, ListKeyContainer<long>, ListValueContainer<int>>()
            {
                Comparer = new BPlusTreeListComparer<long>(new LongComparer()),
                KeySerializer = new KeyListSerializer<long>(new LongSerializer()),
                ValueSerializer = new ValueListSerializer<int>(new IntSerializer()),
                MemoryAllocator = GlobalMemoryManager.Instance
            }));
        }

        [Theory]
        [InlineData(TreeShape.CountBased, false, false)]
        [InlineData(TreeShape.CountBased, true, false)]
        [InlineData(TreeShape.ByteBased, false, false)]
        [InlineData(TreeShape.ByteBased, true, false)]
        [InlineData(TreeShape.ByteBased, false, true)]
        public async Task BookkeepingFollowsLivePagesUnderChurn(TreeShape shape, bool useReadCache, bool zstd)
        {
            var result = await EphemeralChurn.RunT1(shape, useReadCache, zstd, ephemeral: true, $"t1_{shape}_{useReadCache}_{zstd}");

            Assert.True(result.Report.Failed.Count == 0, result.Report + " | " + result.Describe());
            Assert.True(result.Rounds >= 50, result.Describe());
            Assert.True(result.NonVacuous, result.Describe());
        }

        // The production default writer on Linux
        [LinuxOnlyTheory]
        [InlineData(TreeShape.CountBased)]
        [InlineData(TreeShape.ByteBased)]
        public async Task BookkeepingFollowsLivePagesUnderChurnWithDirectIO(TreeShape shape)
        {
            var result = await EphemeralChurn.RunT1(shape, useReadCache: false, zstd: false, ephemeral: true, $"t1_{shape}_direct", directIO: true);

            Assert.True(result.Report.Failed.Count == 0, result.Report + " | " + result.Describe());
            Assert.True(result.Rounds >= 50, result.Describe());
            Assert.True(result.NonVacuous, result.Describe());
        }

        [Fact]
        public async Task AnOrdinaryTreeFailsTheChurnBound()
        {
            var result = await EphemeralChurn.RunT1(TreeShape.CountBased, useReadCache: false, zstd: false, ephemeral: false, "t12", maxRounds: 50);

            // Tombstones break the set equality and the capacity ceiling, ordinary deletes still free their spill at once.
            Assert.True(result.Report.Failed.Contains(1), result.Report.ToString());
            Assert.True(result.Report.Failed.Contains(3), result.Report.ToString());
            Assert.False(result.Report.Failed.Contains(2), result.Report.ToString());
            Assert.False(result.Report.Failed.Contains(6), result.Report.ToString());
            // Every page is still there, #1 fails on surplus tombstones.
            Assert.False(result.Report.Failed.Contains(9), result.Report.ToString());
        }

        private static readonly TimeSpan Timeout = TimeSpan.FromMinutes(1);

        private static async Task<BPlusTree<long, int, ListKeyContainer<long>, ListValueContainer<int>>> OpenCount(IStateManagerClient node, string name, bool ephemeral)
        {
            return (BPlusTree<long, int, ListKeyContainer<long>, ListValueContainer<int>>)(ephemeral
                ? await node.GetOrCreateEphemeralTree(name, EphemeralChurn.CountOptions())
                : await node.GetOrCreateTree(name, EphemeralChurn.CountOptions()));
        }

        private static async Task Churn<K, V, TKeyContainer, TValueContainer>(EphemeralChurn.ChurnTree<K, V, TKeyContainer, TValueContainer> churn, long firstKey, int inserts, int round)
            where TKeyContainer : IKeyContainer<K>
            where TValueContainer : IValueContainer<V>
        {
            for (long key = firstKey; key < firstKey + inserts; key++)
            {
                await churn.Upsert(key, round);
            }
            // Every other key, so leaves merge across the whole range.
            for (long key = firstKey; key < firstKey + inserts; key += 2)
            {
                await churn.Delete(key);
            }
        }

        // The counts follow the live pages exactly: assertions 1 and 2 of plan section 8.2
        private static async Task AssertExactBookkeeping<K, V, TKeyContainer, TValueContainer>(EphemeralChurn.ChurnTree<K, V, TKeyContainer, TValueContainer> churn)
            where TKeyContainer : IKeyContainer<K>
            where TValueContainer : IValueContainer<V>
        {
            var reachable = await TreeWalk.ReachablePages(churn.Tree);
            Assert.Equal(reachable.Pages.OrderBy(k => k), churn.Client.ModifiedKeysForTests.OrderBy(k => k));
            var allocations = churn.Spy.Inner.GetLayoutForTests().Allocations.Keys.OrderBy(k => k).ToList();
            Assert.Equal(allocations, churn.Client.SpillVersionKeysForTests.OrderBy(k => k));
            Assert.All(allocations, key => Assert.Contains(key, reachable.Pages));
            Assert.Null(await churn.CompareWithModel());
        }

        [Fact]
        public async Task AnEphemeralTreeStartsEmptyAfterARestart()
        {
            using var harness = await EphemeralHarness.Create("t3", cachePageCount: EphemeralChurn.CachePageCount);
            var node = harness.Manager.GetOrCreateClient("node");
            var sessionsBefore = harness.Storage.Sessions.Count;
            var ephemeral = EphemeralChurn.CountTree(harness, await OpenCount(node, "ephemeral", ephemeral: true));
            Assert.Equal(sessionsBefore + 1, harness.Storage.Sessions.Count);
            var session = harness.Storage.Sessions[sessionsBefore];
            var persisted = EphemeralChurn.CountTree(harness, await OpenCount(node, "persisted", ephemeral: false));
            await Churn(ephemeral, 0, 3000, 1);
            await Churn(persisted, 0, 3000, 1);

            await ephemeral.Tree.Commit();
            await persisted.Tree.Commit();
            await harness.Manager.CheckpointAsync();
            Assert.Empty(session.TotalWrites);
            await harness.Restart();

            node = harness.Manager.GetOrCreateClient("node");
            var restartedPersisted = EphemeralChurn.CountTree(harness, await OpenCount(node, "persisted", ephemeral: false));
            foreach (var (key, value) in persisted.Model)
            {
                restartedPersisted.Model[key] = value;
            }
            Assert.Null(await restartedPersisted.CompareWithModel());
            var restartedEphemeral = EphemeralChurn.CountTree(harness, await OpenCount(node, "ephemeral", ephemeral: true));
            Assert.Null(await restartedEphemeral.CompareWithModel());
            Assert.Empty(session.TotalWrites);
        }

        [Fact]
        public async Task AnEphemeralOpenOfAPersistedNameThrows()
        {
            using var harness = await EphemeralHarness.Create("t4");
            var node = harness.Manager.GetOrCreateClient("node");
            var tree = await OpenCount(node, "x", ephemeral: false);
            await tree.Upsert(1, 1);
            await tree.Commit();
            await harness.Manager.CheckpointAsync();
            await harness.Restart();

            node = harness.Manager.GetOrCreateClient("node");
            var registered = harness.Manager.RegisteredClientCountForTests;
            await Assert.ThrowsAsync<InvalidOperationException>(async () => await OpenCount(node, "x", ephemeral: true));
            Assert.Null(harness.Manager.RegisteredClientForTests("node_x"));
            Assert.Equal(registered, harness.Manager.RegisteredClientCountForTests);
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task RecoveryEmptiesAnEphemeralTree(bool afterAFailedCommit)
        {
            using var harness = await EphemeralHarness.Create($"t5_{afterAFailedCommit}", cachePageCount: EphemeralChurn.CachePageCount);
            var node = harness.Manager.GetOrCreateClient("node");
            var sessionsBefore = harness.Storage.Sessions.Count;
            var persisted = await OpenCount(node, "persisted", ephemeral: false);
            Assert.Equal(sessionsBefore + 1, harness.Storage.Sessions.Count);
            var persistedSession = harness.Storage.Sessions[sessionsBefore];
            await persisted.Upsert(1, 1);
            await persisted.Commit();
            await harness.Manager.CheckpointAsync();

            var ephemeral = EphemeralChurn.CountTree(harness, await OpenCount(node, "ephemeral", ephemeral: true));
            await Churn(ephemeral, 0, 3000, 1);
            await harness.Manager.CacheTable.ForceCleanup();

            if (afterAFailedCommit)
            {
                await persisted.Upsert(2, 2);
                persistedSession.FaultingKeys[persisted.m_stateClient.Metadata!.Root] = 1;
                await persisted.Commit();
                // The checkpoint fails as it does today.
                await Assert.ThrowsAsync<IOException>(() => harness.Manager.CheckpointAsync().AsTask().WaitAsync(Timeout));
                persistedSession.FaultingKeys.Clear();
            }
            await harness.Manager.InitializeAsync();

            var reopened = await OpenCount(node, "ephemeral", ephemeral: true);
            Assert.Same(ephemeral.Tree.m_stateClient, reopened.m_stateClient);
            var recovered = EphemeralChurn.CountTree(harness, reopened);
            Assert.Null(await recovered.CompareWithModel());
            Assert.Equal(1, recovered.Client.ModifiedCountForTests);
            Assert.Empty(recovered.Spy.Inner.GetLayoutForTests().Allocations);

            await Churn(recovered, 10_000, 3000, 2);
            await harness.Manager.CacheTable.ForceCleanup();
            await AssertExactBookkeeping(recovered);
        }

        [Fact]
        public async Task ClearDropsEphemeralBookkeepingAtOnce()
        {
            using var harness = await EphemeralHarness.Create("t6", cachePageCount: EphemeralChurn.CachePageCount);
            var churn = EphemeralChurn.CountTree(harness, await OpenCount(harness.Manager.GetOrCreateClient("node"), "tree", ephemeral: true));
            await Churn(churn, 0, 5000, 1);
            await harness.Manager.CacheTable.ForceCleanup();

            await churn.Clear();

            Assert.Equal(1, churn.Client.ModifiedCountForTests);
            Assert.Empty(churn.Client.SpillVersionKeysForTests);
        }

        [Fact]
        public async Task EphemeralRootCollapseReturnsBothRents()
        {
            using var harness = await EphemeralHarness.Create("t8");
            var tree = (BPlusTree<long, string, ListKeyContainer<long>, ListValueContainer<string>>)await harness.Manager.GetOrCreateClient("node")
                .GetOrCreateEphemeralTree("tree", new BPlusTreeOptions<long, string, ListKeyContainer<long>, ListValueContainer<string>>()
                {
                    BucketSize = 8,
                    Comparer = new BPlusTreeListComparer<long>(new LongComparer()),
                    KeySerializer = new KeyListSerializer<long>(new LongSerializer()),
                    ValueSerializer = new ValueListSerializer<string>(new StringSerializer()),
                    MemoryAllocator = GlobalMemoryManager.Instance
                });

            // Enough for one split, so the root is an internal node over two leaves.
            for (var i = 0; i < 10; i++)
            {
                await tree.Upsert(i, $"{i}");
            }
            var rootId = tree.m_stateClient.Metadata!.Root;
            var root = Assert.IsType<InternalNode<long, string, ListKeyContainer<long>>>(await tree.m_stateClient.GetValue(rootId));
            root.Return();
            Assert.Equal(1, root.RentCount);

            for (var i = 0; i < 10 && tree.m_stateClient.Metadata.Root == rootId; i++)
            {
                await tree.Delete(i);
            }
            Assert.NotEqual(rootId, tree.m_stateClient.Metadata.Root);

            Assert.Equal(0, root.RentCount);
        }

        private sealed class RetainedQuantity
        {
            private readonly List<long> _cycleMax = new List<long>();
            public required string Name { get; init; }
            public required bool IsCapacity { get; init; }

            public void StartCycle() => _cycleMax.Add(0);

            public void Observe(long value) => _cycleMax[^1] = Math.Max(_cycleMax[^1], value);

            // From cycle 3 on, cycles that pass the earlier maximum by more than one tolerance step
            public List<string> Ratchets(long byteStep)
            {
                var ratchets = new List<string>();
                for (int cycle = 2; cycle < _cycleMax.Count; cycle++)
                {
                    var earlier = _cycleMax.Take(cycle).Max();
                    // One step is byteStep for bytes, one growth for a capacity, a dictionary grows to the next prime above double.
                    var allowed = IsCapacity ? 2 * earlier + earlier / 5 + 8 : earlier + byteStep;
                    if (_cycleMax[cycle] > allowed)
                    {
                        ratchets.Add($"{Name} cycle {cycle + 1}: {_cycleMax[cycle]} over {allowed}");
                    }
                }
                return ratchets;
            }
        }

        [Theory]
        [InlineData(TreeShape.CountBased)]
        [InlineData(TreeShape.ByteBased)]
        public Task GrowShrinkFollowsTheLivePages(TreeShape shape) => GrowShrink(shape, directIO: false);

        [LinuxOnlyTheory]
        [InlineData(TreeShape.CountBased)]
        [InlineData(TreeShape.ByteBased)]
        public Task GrowShrinkFollowsTheLivePagesWithDirectIO(TreeShape shape) => GrowShrink(shape, directIO: true);

        private static async Task GrowShrink(TreeShape shape, bool directIO)
        {
            using var harness = await EphemeralHarness.Create($"t9_{shape}_{directIO}", cachePageCount: EphemeralChurn.CachePageCount, directIO: directIO);
            var node = harness.Manager.GetOrCreateClient("node");
            if (shape == TreeShape.CountBased)
            {
                await GrowShrink(EphemeralChurn.CountTree(harness, await OpenCount(node, "tree", ephemeral: true)), shape, directIO);
            }
            else
            {
                var tree = (BPlusTree<int, string, PrimitiveListKeyContainer<int>, SizedStringValueContainer>)await node.GetOrCreateEphemeralTree("tree", EphemeralChurn.ByteOptions());
                await GrowShrink(EphemeralChurn.ByteTree(harness, tree), shape, directIO);
            }
        }

        private static async Task GrowShrink<K, V, TKeyContainer, TValueContainer>(EphemeralChurn.ChurnTree<K, V, TKeyContainer, TValueContainer> churn, TreeShape shape, bool directIO)
            where TKeyContainer : IKeyContainer<K>
            where TValueContainer : IValueContainer<V>
        {
            const int L = 500;
            // Cycles run until the non-vacuity clauses hold.
            const int MinCycles = 5;
            const int MaxCycles = 40;
            var table = churn.Harness.Manager.CacheTable;
            var limits = ChurnLimits.Derive(shape, zstd: false, churn.Spy.Inner.GetLayoutForTests().Alignment);
            var result = new ChurnResult() { Report = new BoundReport(), Limits = limits };
            var state = new EphemeralChurn.SampleState();
            var random = new Random(9);
            var quantities = new Dictionary<string, RetainedQuantity>();
            RetainedQuantity Quantity(string name, bool isCapacity) => quantities.TryGetValue(name, out var q) ? q : quantities[name] = new RetainedQuantity() { Name = name, IsCapacity = isCapacity };
            // Pages created or deleted between passes: one op splits or merges at most one page per level.
            const int PagesPerPassWindow = 4 * EphemeralChurn.OpsPerPass;
            var ghostCapacity = table.GhostCapacityForTests;

            async Task SampleRetained()
            {
                await EphemeralChurn.Sample(churn, result, state);
                var pPeak = result.PPeak;
                var layout = churn.Spy.Inner.GetLayoutForTests();
                var shapeNow = table.GetQueueShapeForTests();
                var values = new (string Name, bool IsCapacity, long Value, long Ceiling)[]
                {
                    ("modified capacity", true, churn.Client.ModifiedCapacityForTests, 4L * pPeak),
                    ("allocation index capacity", true, layout.AllocationIndexCapacity, 4L * pPeak),
                    // Slots are live entries plus stale ones, a queue doubles past its slots.
                    ("small queue capacity", true, shapeNow.SmallCapacity, 2L * (2L * (pPeak + PagesPerPassWindow) + 1024 + PagesPerPassWindow)),
                    ("main queue capacity", true, shapeNow.MainCapacity, 2L * (2L * (pPeak + PagesPerPassWindow) + 1024 + PagesPerPassWindow)),
                    ("ghost queue capacity", true, shapeNow.GhostCapacity, 2L * (2L * ghostCapacity + 1)),
                    ("buffer writer", true, layout.BufferWriterCapacity, limits.BufferWriterCeiling),
                    ("direct read buffer", true, layout.DirectReadBufferSize, limits.DirectReadBufferCeiling),
                    ("highest end", false, layout.HighestAllocatedEnd, limits.ByteCeiling(pPeak)),
                    ("file length", false, layout.SegmentLengths.Values.DefaultIfEmpty(0).Max(), limits.ByteCeiling(pPeak)),
                };
                foreach (var (name, isCapacity, value, ceiling) in values)
                {
                    Quantity(name, isCapacity).Observe(value);
                    result.Report.Check(20, value <= ceiling, () => $"{name} {value} over its ceiling {ceiling}");
                }
            }

            long nextKey = 0;
            var cycle = 0;
            for (; cycle < MaxCycles && (cycle < MinCycles || !result.NonVacuous); cycle++)
            {
                foreach (var quantity in quantities.Values)
                {
                    quantity.StartCycle();
                }
                if (cycle == 0)
                {
                    foreach (var name in new[] { "modified capacity", "allocation index capacity", "small queue capacity", "main queue capacity", "ghost queue capacity", "buffer writer", "direct read buffer" })
                    {
                        Quantity(name, true).StartCycle();
                    }
                    Quantity("highest end", false).StartCycle();
                    Quantity("file length", false).StartCycle();
                }

                // Fresh keys, so fresh page ids, every cycle.
                while (churn.Model.Count < 20 * L)
                {
                    await churn.Upsert(nextKey++, cycle);
                }
                await SampleRetained();

                var keys = churn.Model.Keys.ToList();
                for (int i = keys.Count - 1; i > 0; i--)
                {
                    var j = random.Next(i + 1);
                    (keys[i], keys[j]) = (keys[j], keys[i]);
                }
                foreach (var key in keys.Take(keys.Count - L))
                {
                    await churn.Delete(key);
                }
                await SampleRetained();

                for (int round = 0; round < 3; round++)
                {
                    var live = churn.Model.Keys.ToList();
                    foreach (var key in live.Where((_, i) => i % 2 == round % 2))
                    {
                        await churn.Delete(key);
                    }
                    while (churn.Model.Count < L)
                    {
                        await churn.Upsert(nextKey++, cycle);
                    }
                    await SampleRetained();
                }
                result.Rounds = cycle + 1;
            }

            Assert.True(result.Report.Failed.Count == 0, result.Report + " | " + result.Describe());
            // Spilled pages, and so the bytes, vary by the pages a pass leaves resident without a spill.
            var byteStep = (long)limits.MaxAlloc * (table.GetQueueShapeForTests().CleanupStart + 1);
            var ratchets = quantities.Values.SelectMany(q => q.Ratchets(byteStep)).ToList();
            Assert.True(ratchets.Count <= 1, string.Join("; ", ratchets));
            Assert.True(result.NonVacuous, result.Describe());

            var segment = churn.Spy.Inner.SegmentFileNameForTests(0);
            await churn.Clear();
            if (directIO)
            {
                // The direct writer leaves its file, the next writer with the name deletes it.
                Assert.True(File.Exists(segment));
                for (long key = 0; key < 2000; key++)
                {
                    await churn.Upsert(nextKey++, cycle);
                }
                await table.ForceCleanup();
                var layout = churn.Spy.Inner.GetLayoutForTests();
                Assert.True(new FileInfo(segment).Length <= layout.HighestAllocatedEnd, $"the old file survived, {new FileInfo(segment).Length} bytes for a highest end of {layout.HighestAllocatedEnd}");
            }
            else
            {
                // Freeing the last allocation closes the DeleteOnClose segment.
                Assert.False(File.Exists(segment), "the spill file is still there after Clear freed every allocation");
            }
        }

        [Fact]
        public async Task PageIdsReissuedAfterRecoveryServeNoStalePage()
        {
            using var harness = await EphemeralHarness.Create("t10", cachePageCount: EphemeralChurn.CachePageCount);
            var node = harness.Manager.GetOrCreateClient("node");
            var churn = EphemeralChurn.CountTree(harness, await OpenCount(node, "tree", ephemeral: true));
            await harness.Manager.CheckpointAsync();

            await Churn(churn, 0, 4000, 1);
            await harness.Manager.CacheTable.ForceCleanup();
            var before = (await TreeWalk.ReachablePages(churn.Tree)).Pages;
            await harness.Manager.InitializeAsync();

            var recovered = EphemeralChurn.CountTree(harness, await OpenCount(node, "tree", ephemeral: true));
            await Churn(recovered, 0, 4000, 2);
            await harness.Manager.CacheTable.ForceCleanup();

            var after = (await TreeWalk.ReachablePages(recovered.Tree)).Pages;
            Assert.True(after.Overlaps(before), "no page id was reissued after the recovery");
            await AssertExactBookkeeping(recovered);
        }

        [Fact]
        public Task RepeatedClearAndRecoveryDoNotAccumulate() => ClearAndRecoveryCycles(directIO: false);

        [LinuxOnlyFact]
        public Task RepeatedClearAndRecoveryDoNotAccumulateWithDirectIO() => ClearAndRecoveryCycles(directIO: true);

        private static async Task ClearAndRecoveryCycles(bool directIO)
        {
            using var harness = await EphemeralHarness.Create($"t11_{directIO}", cachePageCount: EphemeralChurn.CachePageCount, directIO: directIO);
            var node = harness.Manager.GetOrCreateClient("node");
            var churn = EphemeralChurn.CountTree(harness, await OpenCount(node, "tree", ephemeral: true));
            var registered = harness.Manager.RegisteredClientCountForTests;
            var hitCounters = harness.Manager.CacheTable.ExternalHitCounterCountForTests;
            int SpillFiles() => Directory.Exists(harness.TempDirectory) ? Directory.GetFiles(harness.TempDirectory).Length : 0;
            var files = SpillFiles();

            void AssertBaseline(string after)
            {
                Assert.True(registered == harness.Manager.RegisteredClientCountForTests, $"registrations after {after}");
                Assert.True(hitCounters == harness.Manager.CacheTable.ExternalHitCounterCountForTests, $"hit counters after {after}");
                // The direct writer leaves its one file until the next writer with the name replaces it.
                Assert.True(directIO ? SpillFiles() <= files + 1 : files == SpillFiles(), $"{SpillFiles()} spill files after {after}");
                Assert.True(1 == churn.Client.ModifiedCountForTests, $"modified count after {after}");
                Assert.True(churn.Client.SpillVersionKeysForTests.Count == 0, $"spill versions after {after}");
                Assert.True(churn.Spy.Inner.GetLayoutForTests().Allocations.Count == 0, $"allocations after {after}");
            }

            for (int cycle = 0; cycle < 4; cycle++)
            {
                await Churn(churn, cycle * 10_000, 3000, cycle);
                await harness.Manager.CacheTable.ForceCleanup();
                Assert.NotEmpty(churn.Client.SpillVersionKeysForTests);
                await churn.Clear();
                AssertBaseline($"clear {cycle}");

                await Churn(churn, cycle * 10_000 + 5000, 3000, cycle);
                await harness.Manager.CacheTable.ForceCleanup();
                await harness.Manager.InitializeAsync();
                churn = EphemeralChurn.CountTree(harness, await OpenCount(node, "tree", ephemeral: true));
                AssertBaseline($"recovery {cycle}");
            }
        }

        [Fact]
        public async Task APageUnderALiveEphemeralIdStaysMasked()
        {
            var fileProvider = new FlowtideDotNet.Storage.Persistence.Reservoir.MemoryDisk.MemoryFileProvider();
            using var harness = await EphemeralHarness.Create("t13", cachePageCount: EphemeralChurn.CachePageCount, fileProvider: fileProvider);
            var node = harness.Manager.GetOrCreateClient("node");
            var churn = EphemeralChurn.CountTree(harness, await OpenCount(node, "tree", ephemeral: true));
            var metadataId = ((StateClient)churn.Tree.m_stateClient).MetadataId;

            // A valid client metadata page under the live ephemeral id, written through another session.
            var injected = new StateClientMetadata<BPlusTreeMetadata>()
            {
                Metadata = BPlusTreeMetadata.Create(EphemeralChurn.BucketSize, 999_999, 999_999, 32 * 1024, new List<long>(), new List<long>(), 0),
                CommitedOnce = true
            };
            var session = harness.Storage.CreateSession();
            await session.Write(metadataId, new SerializableObject(StateClientMetadataSerializer.Serialize(injected)));
            await session.Commit();
            await harness.Manager.CheckpointAsync();
            Assert.True(harness.Storage.TryGetValue(metadataId, out _));

            await Churn(churn, 0, 3000, 1);
            await harness.Manager.InitializeAsync();
            var recovered = EphemeralChurn.CountTree(harness, await OpenCount(node, "tree", ephemeral: true));
            Assert.Null(await recovered.CompareWithModel());
            Assert.Equal(1, recovered.Client.ModifiedCountForTests);

            await harness.Restart();
            node = harness.Manager.GetOrCreateClient("node");
            await Assert.ThrowsAsync<InvalidOperationException>(async () => await OpenCount(node, "tree", ephemeral: true));
        }

        // Stays reachable for as long as a compressor built for one open is rooted
        private sealed class ProbeAllocator : IMemoryAllocator
        {
            public IMemoryOwner<byte> Allocate(int size, int alignment) => GlobalMemoryManager.Instance.Allocate(size, alignment);

            public IMemoryOwner<byte> Realloc(IMemoryOwner<byte> memory, int size, int alignment) => GlobalMemoryManager.Instance.Realloc(memory, size, alignment);

            public void RegisterAllocationToMetrics(int size) => GlobalMemoryManager.Instance.RegisterAllocationToMetrics(size);

            public void RegisterFreeToMetrics(int size) => GlobalMemoryManager.Instance.RegisterFreeToMetrics(size);
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static async Task<WeakReference> OpenPersistedNameAsEphemeral(IStateManagerClient node)
        {
            var allocator = new ProbeAllocator();
            var options = EphemeralChurn.CountOptions();
            options.MemoryAllocator = allocator;
            await Assert.ThrowsAsync<InvalidOperationException>(async () => await node.GetOrCreateEphemeralTree("x", options));
            return new WeakReference(allocator);
        }

        [Fact]
        public async Task ARejectedCompressedOpenReleasesItsCompressor()
        {
            using var harness = await EphemeralHarness.Create("f10", zstd: true);
            var node = harness.Manager.GetOrCreateClient("node");
            var tree = await OpenCount(node, "x", ephemeral: false);
            await tree.Upsert(1, 1);
            await tree.Commit();
            await harness.Manager.CheckpointAsync();
            await harness.Restart();
            node = harness.Manager.GetOrCreateClient("node");

            var allocator = await OpenPersistedNameAsEphemeral(node);
            for (int i = 0; i < 3 && allocator.IsAlive; i++)
            {
                GC.Collect();
                GC.WaitForPendingFinalizers();
            }

            // The rejected open's compressor pins itself, and through it the allocator, until disposed.
            Assert.False(allocator.IsAlive, "the compressor built for the rejected open is still rooted");
        }

        // Fails initialization and root creation a set number of times, then behaves
        private sealed class FlakyKeySerializer : IBPlusTreeKeySerializer<long, ListKeyContainer<long>>
        {
            private readonly KeyListSerializer<long> _inner = new KeyListSerializer<long>(new LongSerializer());
            public int InitializeFailures;
            public int CreateEmptyFailures;

            public Task CheckpointAsync(IBPlusTreeSerializerCheckpointContext context) => _inner.CheckpointAsync(context);

            public ListKeyContainer<long> CreateEmpty()
            {
                if (CreateEmptyFailures > 0)
                {
                    CreateEmptyFailures--;
                    throw new InvalidOperationException("Injected root creation failure");
                }
                return _inner.CreateEmpty();
            }

            public ListKeyContainer<long> Deserialize(ref SequenceReader<byte> reader) => _inner.Deserialize(ref reader);

            public Task InitializeAsync(IBPlusTreeSerializerInitializeContext context)
            {
                if (InitializeFailures > 0)
                {
                    InitializeFailures--;
                    throw new InvalidOperationException("Injected serializer initialization failure");
                }
                return _inner.InitializeAsync(context);
            }

            public void Serialize(in IBufferWriter<byte> writer, in ListKeyContainer<long> values) => _inner.Serialize(writer, values);
        }

        private static BPlusTreeOptions<long, int, ListKeyContainer<long>, ListValueContainer<int>> FlakyOptions(FlakyKeySerializer keySerializer) => new BPlusTreeOptions<long, int, ListKeyContainer<long>, ListValueContainer<int>>()
        {
            BucketSize = 8,
            Comparer = new BPlusTreeListComparer<long>(new LongComparer()),
            KeySerializer = keySerializer,
            ValueSerializer = new ValueListSerializer<int>(new IntSerializer()),
            MemoryAllocator = GlobalMemoryManager.Instance
        };

        [Fact]
        public async Task AFailedCreationStaysRegisteredAndIsReused()
        {
            using var harness = await EphemeralHarness.Create("t14");
            var node = harness.Manager.GetOrCreateClient("node");
            var registered = harness.Manager.RegisteredClientCountForTests;
            var hitCounters = harness.Manager.CacheTable.ExternalHitCounterCountForTests;

            // The first serializer stays with the client, so its initialization fails once and then succeeds.
            var first = new FlakyKeySerializer() { InitializeFailures = 1 };
            await Assert.ThrowsAsync<InvalidOperationException>(async () => await node.GetOrCreateEphemeralTree("tree", FlakyOptions(first)));
            Assert.Equal(0, first.InitializeFailures);
            var client = harness.Manager.RegisteredClientForTests("node_tree");
            Assert.IsType<EphemeralSyncStateClient<IBPlusTreeNode, BPlusTreeMetadata>>(client);
            Assert.Equal(registered + 1, harness.Manager.RegisteredClientCountForTests);
            Assert.Equal(hitCounters + 1, harness.Manager.CacheTable.ExternalHitCounterCountForTests);
            await harness.Manager.InitializeAsync();

            // Root creation uses the new call's options.
            var second = new FlakyKeySerializer() { CreateEmptyFailures = 1 };
            await Assert.ThrowsAsync<InvalidOperationException>(async () => await node.GetOrCreateEphemeralTree("tree", FlakyOptions(second)));
            Assert.Equal(0, second.CreateEmptyFailures);
            Assert.Same(client, harness.Manager.RegisteredClientForTests("node_tree"));
            Assert.Equal(registered + 1, harness.Manager.RegisteredClientCountForTests);
            Assert.Equal(hitCounters + 1, harness.Manager.CacheTable.ExternalHitCounterCountForTests);
            await harness.Manager.InitializeAsync();

            var tree = (BPlusTree<long, int, ListKeyContainer<long>, ListValueContainer<int>>)await node.GetOrCreateEphemeralTree("tree", FlakyOptions(new FlakyKeySerializer()));
            Assert.Same(client, tree.m_stateClient);
            Assert.Equal(registered + 1, harness.Manager.RegisteredClientCountForTests);
            Assert.Equal(hitCounters + 1, harness.Manager.CacheTable.ExternalHitCounterCountForTests);
            var reachable = await TreeWalk.ReachablePages(tree);
            Assert.Equal(new[] { tree.m_stateClient.Metadata!.Root }, reachable.Pages);
            Assert.Equal(new[] { tree.m_stateClient.Metadata!.Root }, ((SyncStateClient<IBPlusTreeNode, BPlusTreeMetadata>)tree.m_stateClient).ModifiedKeysForTests);
        }
    }

    [CollectionDefinition("EphemeralSerial", DisableParallelization = true)]
    public class EphemeralSerialCollection
    {
    }

    [Collection("EphemeralSerial")]
    public class EphemeralTreeConcurrencyTests
    {
        [Fact]
        public async Task ChurnUnderContinuousEvictionKeepsEveryValue()
        {
            using var harness = await EphemeralHarness.Create("t2", cachePageCount: EphemeralChurn.CachePageCount);
            var tree = (BPlusTree<long, int, ListKeyContainer<long>, ListValueContainer<int>>)await harness.Manager.GetOrCreateClient("node").GetOrCreateEphemeralTree("tree", EphemeralChurn.CountOptions());
            var churn = EphemeralChurn.CountTree(harness, tree);
            churn.DrivePasses = false;
            var random = new Random(2);
            using var stop = new CancellationTokenSource();
            var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            long passes = 0;
            // One evictor, as in production.
            var evictor = Task.Run(async () =>
            {
                started.TrySetResult();
                while (!stop.IsCancellationRequested)
                {
                    await harness.Manager.CacheTable.ForceCleanup();
                    Interlocked.Increment(ref passes);
                }
            });
            // Waits for a pass that starts after this call, so it sees the tree as it is now
            async Task AwaitBackgroundPass(string where)
            {
                var target = Interlocked.Read(ref passes) + 2;
                var deadline = DateTime.UtcNow + TimeSpan.FromMinutes(1);
                while (Interlocked.Read(ref passes) < target)
                {
                    Assert.True(DateTime.UtcNow < deadline, $"no background pass completed {where}");
                    await Task.Delay(1);
                }
            }
            long nextKey = 0;
            try
            {
                await started.Task.WaitAsync(TimeSpan.FromMinutes(1));
                for (int round = 1; round <= 40; round++)
                {
                    if (round % 10 == 0)
                    {
                        await churn.Clear();
                        await AwaitBackgroundPass($"after the clear in round {round}");
                    }
                    while (churn.Model.Count < 4000)
                    {
                        await churn.Upsert(nextKey++, round);
                    }
                    var live = churn.Model.Keys.ToList();
                    var start = random.Next(0, live.Count - 800);
                    for (int i = start; i < start + 800; i++)
                    {
                        await churn.Delete(live[i]);
                    }
                    await AwaitBackgroundPass($"mid round {round}");
                    for (int i = 0; i < 200; i++)
                    {
                        var remaining = churn.Model.Keys.ElementAt(random.Next(churn.Model.Count));
                        await churn.Delete(remaining);
                    }
                    var mismatch = await churn.CompareWithModel();
                    Assert.True(mismatch == null, $"round {round}: {mismatch}");
                }
            }
            finally
            {
                stop.Cancel();
                await evictor.WaitAsync(TimeSpan.FromMinutes(1));
            }

            // Exact once the evictor has stopped.
            await harness.Manager.CacheTable.ForceCleanup();
            var reachable = await TreeWalk.ReachablePages(tree);
            Assert.Equal(reachable.Pages.OrderBy(k => k), churn.Client.ModifiedKeysForTests.OrderBy(k => k));
            var allocations = churn.Spy.Inner.GetLayoutForTests().Allocations.Keys.OrderBy(k => k).ToList();
            Assert.Equal(allocations, churn.Client.SpillVersionKeysForTests.OrderBy(k => k));
            Assert.All(allocations, key => Assert.Contains(key, reachable.Pages));
            Assert.Null(await churn.CompareWithModel());
        }
    }
}
