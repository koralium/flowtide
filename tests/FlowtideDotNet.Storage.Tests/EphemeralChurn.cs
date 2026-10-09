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
using FlowtideDotNet.Storage.Serializers;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.StateManager.Internal;
using FlowtideDotNet.Storage.StateManager.Internal.Sync;
using FlowtideDotNet.Storage.Tree;
using FlowtideDotNet.Storage.Tree.Internal;
using System.Numerics;
using ZstdSharp;

namespace FlowtideDotNet.Storage.Tests
{
    // Inside the namespace, the FlowtideDotNet.MiMalloc namespace otherwise shadows the type
    using MiMalloc = FlowtideDotNet.MiMalloc.MiMalloc;

    public enum TreeShape
    {
        CountBased,
        ByteBased
    }

    /// <summary>
    /// Records the first violation of each numbered bound, so a negative control can name which ones fail.
    /// </summary>
    internal sealed class BoundReport
    {
        private readonly SortedDictionary<int, string> _first = new SortedDictionary<int, string>();

        public IReadOnlyCollection<int> Failed => _first.Keys;

        public void Check(int assertion, bool holds, Func<string> describe)
        {
            if (!holds && !_first.ContainsKey(assertion))
            {
                _first[assertion] = describe();
            }
        }

        public override string ToString() => string.Join("; ", _first.Select(kv => $"#{kv.Key}: {kv.Value}"));
    }

    /// <summary>
    /// Ceilings declared before a churn run, each derived from the code and never from observations.
    /// </summary>
    internal sealed record ChurnLimits(int Alignment, int MaxSerializedPageBytes, int MaxAlloc, long BufferWriterCeiling, long CompressorCeiling, long DirectReadBufferCeiling, bool PagesBelowAlignmentFloor)
    {
        // Declared fragmentation allowance, not a proof of best fit's worst case
        public const int F = 2;

        public long ByteCeiling(int pPeak) => (long)F * pPeak * MaxAlloc + 1024 * 1024;

        public static int AlignUp(long size, int alignment) => (int)((size + alignment - 1) / alignment * alignment);

        // The allocator hands out mimalloc size classes when mimalloc is in use
        public static long AllocatorSize(long size) => FlowtideMemoryAllocation.CanQueryAllocationSize ? (long)MiMalloc.mi_good_size((nuint)size) : size;

        public static ChurnLimits Derive(TreeShape shape, bool zstd, int alignment)
        {
            int maxSerialized;
            if (shape == TreeShape.CountBased)
            {
                // A leaf splits when it reaches 16 keys: 25 header + key list 8 + 8n + value list 8 + 4n.
                var leaf = 25 + (8 + 8 * EphemeralChurn.BucketSize) + (8 + 4 * EphemeralChurn.BucketSize);
                // 9 header + key list 8 + 8n + children length 4 + 8 per child.
                var internalNode = 9 + (8 + 8 * EphemeralChurn.BucketSize) + 4 + 8 * (EphemeralChurn.BucketSize + 1);
                maxSerialized = Math.Max(leaf, internalNode);
            }
            else
            {
                // An entry is a 4 byte key plus a 4 byte length and at most MaxValueLength ASCII bytes.
                var maxEntry = 4 + 4 + EphemeralChurn.MaxValueLength;
                // Sixteen entries fit the page, past sixteen keys a leaf splits once it is over the page by one entry.
                var leafBytes = Math.Max(16 * maxEntry, EphemeralChurn.PageSizeBytes + maxEntry);
                // 25 header + keys 8 + 4n + values 4 + the entry bytes, so 37 over GetByteSize.
                var leaf = 37 + leafBytes;
                // Internal GetByteSize is 4n + 8(n + 1), one key and child over the page at most, serialized 21 over it.
                var internalNode = 21 + Math.Max(16 * 12 + 8, EphemeralChurn.PageSizeBytes + 12);
                maxSerialized = Math.Max(leaf, internalNode);
            }
            var uncompressed = maxSerialized;
            long maxSizeHint = maxSerialized;
            long compressorCeiling = 0;
            if (zstd)
            {
                // An 8 byte header plus the zstd bound of the uncompressed page.
                maxSerialized = 8 + Compressor.GetCompressBound(uncompressed);
                // The wrapper asks the file cache for the bound of the page plus its header.
                maxSizeHint = Compressor.GetCompressBound(uncompressed + 8);
                // ArrayBufferWriter doubles, staying below twice the page plus the inner serializer's largest hint.
                compressorCeiling = Math.Max(256, 2L * (uncompressed + uncompressed));
            }
            var maxAlloc = (int)BitOperations.RoundUpToPowerOf2((uint)AlignUp(maxSerialized, alignment));
            // GetMemory grows to position + max(hint, 4096), aligned.
            var bufferWriterCeiling = AllocatorSize(AlignUp(maxSerialized + Math.Max(maxSizeHint, 4096), alignment));
            var directReadBufferCeiling = AlignUp(maxSerialized, alignment);
            return new ChurnLimits(alignment, maxSerialized, maxAlloc, bufferWriterCeiling, compressorCeiling, directReadBufferCeiling, shape == TreeShape.CountBased);
        }
    }

    internal sealed class ChurnResult
    {
        public required BoundReport Report { get; init; }
        public required ChurnLimits Limits { get; init; }
        public int PPeak { get; set; }
        public long DeletedPages { get; set; }
        public long FreedAllocations { get; set; }
        public bool PagesEntered { get; set; }
        public bool PagesLeft { get; set; }
        public int MaxInternalNodes { get; set; }
        public int Rounds { get; set; }

        public bool PageClauseHolds => DeletedPages >= 2L * 4 * PPeak;

        public bool ByteClauseHolds => FreedAllocations * Limits.Alignment >= 2 * Limits.ByteCeiling(PPeak);

        public bool NonVacuous => PageClauseHolds && ByteClauseHolds && PagesEntered && PagesLeft && MaxInternalNodes > 1;

        public string Describe() => $"rounds {Rounds}, P_peak {PPeak}, deleted pages {DeletedPages}, freed allocations {FreedAllocations}, entered {PagesEntered}, left {PagesLeft}, internal nodes {MaxInternalNodes}, alignment {Limits.Alignment}, maxAlloc {Limits.MaxAlloc}";
    }

    internal static class EphemeralChurn
    {
        public const int BucketSize = 16;
        public const int PageSizeBytes = 512;
        public const int MinValueLength = 8;
        public const int MaxValueLength = 24;
        public const int CachePageCount = 32;
        public const int OpsPerPass = 1000;

        public static BPlusTreeOptions<long, int, ListKeyContainer<long>, ListValueContainer<int>> CountOptions() => new BPlusTreeOptions<long, int, ListKeyContainer<long>, ListValueContainer<int>>()
        {
            BucketSize = BucketSize,
            Comparer = new BPlusTreeListComparer<long>(new LongComparer()),
            KeySerializer = new KeyListSerializer<long>(new LongSerializer()),
            ValueSerializer = new ValueListSerializer<int>(new IntSerializer()),
            MemoryAllocator = GlobalMemoryManager.Instance
        };

        public static BPlusTreeOptions<int, string, PrimitiveListKeyContainer<int>, SizedStringValueContainer> ByteOptions() => new BPlusTreeOptions<int, string, PrimitiveListKeyContainer<int>, SizedStringValueContainer>()
        {
            PageSizeBytes = PageSizeBytes,
            UseByteBasedPageSizes = true,
            Comparer = new PrimitiveListComparer<int>(),
            KeySerializer = new PrimitiveListKeyContainerSerializer<int>(GlobalMemoryManager.Instance),
            ValueSerializer = new SizedStringValueSerializer(),
            MemoryAllocator = GlobalMemoryManager.Instance
        };

        public static int CountValue(long key, int round) => (int)(key * 31 + round);

        // ASCII digits of a bounded length, varied by key
        public static string ByteValue(long key, int round)
        {
            var length = MinValueLength + (int)((key * 2654435761L + round) % (MaxValueLength - MinValueLength + 1));
            return (key * 7919 + round).ToString("D24").Substring(24 - length);
        }

        /// <summary>
        /// A tree over one shape with a reference model of its contents.
        /// </summary>
        internal sealed class ChurnTree<K, V, TKeyContainer, TValueContainer>
            where TKeyContainer : IKeyContainer<K>
            where TValueContainer : IValueContainer<V>
        {
            private readonly Func<long, K> _toKey;
            private readonly Func<K, long> _fromKey;
            private readonly Func<long, int, V> _value;

            public ChurnTree(EphemeralHarness harness, BPlusTree<K, V, TKeyContainer, TValueContainer> tree, Func<long, K> toKey, Func<K, long> fromKey, Func<long, int, V> value)
            {
                Harness = harness;
                Tree = tree;
                _toKey = toKey;
                _fromKey = fromKey;
                _value = value;
            }

            public EphemeralHarness Harness { get; }

            public BPlusTree<K, V, TKeyContainer, TValueContainer> Tree { get; private set; }

            public SyncStateClient<IBPlusTreeNode, BPlusTreeMetadata> Client => (SyncStateClient<IBPlusTreeNode, BPlusTreeMetadata>)Tree.m_stateClient;

            public SpyFileCache Spy => (SpyFileCache)Client.FileCacheForTests;

            public SortedDictionary<long, V> Model { get; } = new SortedDictionary<long, V>();

            public long Operations { get; private set; }

            // Off when another task drives the passes, production runs one evictor
            public bool DrivePasses { get; set; } = true;

            public void Replace(BPlusTree<K, V, TKeyContainer, TValueContainer> tree) => Tree = tree;

            public async Task Upsert(long key, int round)
            {
                var value = _value(key, round);
                await Tree.Upsert(_toKey(key), value);
                Model[key] = value;
                await AfterOperation();
            }

            public async Task Delete(long key)
            {
                await Tree.Delete(_toKey(key));
                Model.Remove(key);
                await AfterOperation();
            }

            private async Task AfterOperation()
            {
                Operations++;
                if (DrivePasses && Operations % OpsPerPass == 0)
                {
                    await Harness.Manager.CacheTable.ForceCleanup();
                }
            }

            public async Task Clear()
            {
                await Tree.Clear();
                Model.Clear();
            }

            // Iterates the whole tree in order against the model, disposing the iterator before the next mutation
            public async Task<string?> CompareWithModel()
            {
                using var iterator = Tree.CreateIterator();
                await iterator.SeekFirst();
                using var expected = Model.GetEnumerator();
                await foreach (var page in iterator)
                {
                    foreach (var kv in page)
                    {
                        if (!expected.MoveNext())
                        {
                            return $"tree has extra key {_fromKey(kv.Key)}";
                        }
                        var key = _fromKey(kv.Key);
                        if (key != expected.Current.Key || !EqualityComparer<V>.Default.Equals(kv.Value, expected.Current.Value))
                        {
                            return $"tree has {key}={kv.Value}, model has {expected.Current.Key}={expected.Current.Value}";
                        }
                    }
                }
                return expected.MoveNext() ? $"tree is missing key {expected.Current.Key}" : null;
            }

            public int RentCount(long pageId)
            {
                if (!Harness.Manager.TryPeekCacheEntry(pageId, out var entry))
                {
                    return -1;
                }
                return ((BaseNode<K, TKeyContainer>)entry.Value).RentCount;
            }
        }

        public static ChurnTree<long, int, ListKeyContainer<long>, ListValueContainer<int>> CountTree(EphemeralHarness harness, BPlusTree<long, int, ListKeyContainer<long>, ListValueContainer<int>> tree)
            => new ChurnTree<long, int, ListKeyContainer<long>, ListValueContainer<int>>(harness, tree, k => k, k => k, CountValue);

        public static ChurnTree<int, string, PrimitiveListKeyContainer<int>, SizedStringValueContainer> ByteTree(EphemeralHarness harness, BPlusTree<int, string, PrimitiveListKeyContainer<int>, SizedStringValueContainer> tree)
            => new ChurnTree<int, string, PrimitiveListKeyContainer<int>, SizedStringValueContainer>(harness, tree, k => checked((int)k), k => k, ByteValue);

        /// <summary>
        /// State carried between samples of one run.
        /// </summary>
        internal sealed class SampleState
        {
            public HashSet<long>? Previous;
        }

        /// <summary>
        /// Checks the bounds of plan section 8.2 at a quiescent point after an evicting pass.
        /// </summary>
        public static async Task Sample<K, V, TKeyContainer, TValueContainer>(ChurnTree<K, V, TKeyContainer, TValueContainer> churn, ChurnResult result, SampleState state, bool checkScratch = true)
            where TKeyContainer : IKeyContainer<K>
            where TValueContainer : IValueContainer<V>
        {
            var report = result.Report;
            var limits = result.Limits;
            // Table quantities are read before the walk, which loads every page back into the cache.
            var table = churn.Harness.Manager.CacheTable;
            var client = churn.Client;

            // At or below cleanupStart the last driven pass was the evicting one and nothing ran since.
            if (table.Count > table.GetQueueShapeForTests().CleanupStart)
            {
                await table.ForceCleanup();
            }
            var ghost = table.GetQueueShapeForTests();
            var ghostRecords = table.GetQueueCountsForTests().GhostCount;
            var capacity = table.GhostCapacityForTests;
            var layout = churn.Spy.Inner.GetLayoutForTests();
            var modifiedKeys = client.ModifiedKeysForTests;
            var modifiedCapacity = client.ModifiedCapacityForTests;
            var spillKeys = client.SpillVersionKeysForTests;
            var compressorCapacity = client.ValueSerializerForTests is CompressedStateSerializer<IBPlusTreeNode> compressed ? compressed.BufferCapacityForTests : 0;

            // A following idle pass runs the compaction check.
            for (int i = 0; i < 5 && table.Count > table.GetQueueShapeForTests().CleanupStart; i++)
            {
                await table.ForceCleanup();
            }
            await table.ForceCleanup();
            var queues = table.GetQueueCountsForTests();
            var stale = queues.SmallStale + queues.MainStale;
            var live = queues.SmallCount + queues.MainCount - stale;

            var reachable = await TreeWalk.ReachablePages(churn.Tree);
            var pages = reachable.Pages;
            result.PPeak = Math.Max(result.PPeak, pages.Count);
            result.MaxInternalNodes = Math.Max(result.MaxInternalNodes, reachable.InternalNodes);
            var pPeak = result.PPeak;

            // 7 premise: once the walk returned its rents, the cache holds the only rent.
            foreach (var page in pages)
            {
                var rents = churn.RentCount(page);
                report.Check(7, rents == 1, () => $"page {page} has rent count {rents}");
            }

            report.Check(1, modifiedKeys.Count == pages.Count && modifiedKeys.All(pages.Contains), () => $"{modifiedKeys.Count} modified keys against {pages.Count} reachable pages");

            var allocationKeys = layout.Allocations.Keys.ToHashSet();
            report.Check(2, spillKeys.Count == allocationKeys.Count && spillKeys.All(allocationKeys.Contains), () => $"{spillKeys.Count} spill versions against {allocationKeys.Count} allocations");
            report.Check(2, allocationKeys.All(pages.Contains), () => $"{allocationKeys.Count(k => !pages.Contains(k))} allocations outside the reachable pages");

            report.Check(3, modifiedCapacity <= 4L * pPeak, () => $"modified capacity {modifiedCapacity} over 4 x {pPeak}");

            long activeBytes = 0;
            foreach (var (key, allocation) in layout.Allocations)
            {
                activeBytes += allocation.AllocatedSize;
                report.Check(4, allocation.Size <= limits.MaxSerializedPageBytes, () => $"page {key} serialized to {allocation.Size} bytes over {limits.MaxSerializedPageBytes}");
                report.Check(4, allocation.AllocatedSize <= limits.MaxAlloc, () => $"page {key} allocated {allocation.AllocatedSize} over {limits.MaxAlloc}");
                if (limits.PagesBelowAlignmentFloor)
                {
                    report.Check(4, allocation.Size < 512 && allocation.AllocatedSize == limits.Alignment, () => $"page {key} of {allocation.Size} bytes allocated {allocation.AllocatedSize}, not the alignment {limits.Alignment}");
                }
            }
            report.Check(4, activeBytes <= (long)pPeak * limits.MaxAlloc, () => $"{activeBytes} active bytes over {pPeak} x {limits.MaxAlloc}");

            var n = layout.Allocations.Count;
            report.Check(5, layout.MemoryNodeCount <= 2 * n + 1, () => $"{layout.MemoryNodeCount} memory nodes for {n} allocations");
            report.Check(5, layout.FreeNodeCount <= n + 1, () => $"{layout.FreeNodeCount} free nodes for {n} allocations");
            report.Check(5, layout.AllocationIndexCount == layout.AllocatedNodeCount, () => $"allocation index {layout.AllocationIndexCount} against {layout.AllocatedNodeCount} allocated nodes");
            report.Check(5, layout.FreeIndexCount == layout.FreeNodeCount, () => $"free index {layout.FreeIndexCount} against {layout.FreeNodeCount} free nodes");

            var byteCeiling = limits.ByteCeiling(pPeak);
            report.Check(6, layout.HighestAllocatedEnd <= byteCeiling, () => $"highest end {layout.HighestAllocatedEnd} over {byteCeiling}");
            foreach (var (file, length) in layout.SegmentLengths)
            {
                report.Check(6, length <= byteCeiling, () => $"segment {file} is {length} bytes, over {byteCeiling}");
            }

            report.Check(7, ghost.GhostKeys <= capacity, () => $"{ghost.GhostKeys} ghost keys over {capacity}");
            report.Check(7, ghostRecords <= 2 * capacity, () => $"{ghostRecords} ghost records over 2 x {capacity}");

            report.Check(8, stale <= Math.Max(1024, live), () => $"{stale} stale slots after an idle pass with {live} live");

            var mismatch = await churn.CompareWithModel();
            report.Check(9, mismatch == null, () => mismatch!);

            if (checkScratch)
            {
                report.Check(11, layout.BufferWriterCapacity <= limits.BufferWriterCeiling, () => $"buffer writer capacity {layout.BufferWriterCapacity} over {limits.BufferWriterCeiling}");
                report.Check(12, compressorCapacity <= limits.CompressorCeiling, () => $"compressor buffer {compressorCapacity} over {limits.CompressorCeiling}");
                report.Check(13, layout.DirectReadBufferSize <= limits.DirectReadBufferCeiling, () => $"direct read buffer {layout.DirectReadBufferSize} over {limits.DirectReadBufferCeiling}");
            }

            if (state.Previous != null)
            {
                var left = state.Previous.Count(p => !pages.Contains(p));
                result.DeletedPages += left;
                result.PagesLeft |= left > 0;
                result.PagesEntered |= pages.Any(p => !state.Previous.Contains(p));
            }
            state.Previous = pages;
            result.FreedAllocations = churn.Spy.FreedAllocations;
        }

        /// <summary>
        /// Runs constant-live churn: each round deletes a window and random keys, then inserts as many fresh keys.
        /// </summary>
        public static async Task<ChurnResult> RunConstantLive<K, V, TKeyContainer, TValueContainer>(ChurnTree<K, V, TKeyContainer, TValueContainer> churn, ChurnLimits limits, int liveKeys, int window, int randomDeletes, int minRounds, int maxRounds, int seed)
            where TKeyContainer : IKeyContainer<K>
            where TValueContainer : IValueContainer<V>
        {
            var random = new Random(seed);
            var result = new ChurnResult() { Report = new BoundReport(), Limits = limits };
            var state = new SampleState();
            long nextKey = 0;
            for (; nextKey < liveKeys; nextKey++)
            {
                await churn.Upsert(nextKey, 0);
            }
            await Sample(churn, result, state);

            for (int round = 1; round <= maxRounds; round++)
            {
                var live = churn.Model.Keys.ToList();
                var start = random.Next(0, live.Count - window);
                for (int i = start; i < start + window; i++)
                {
                    await churn.Delete(live[i]);
                }
                var remaining = churn.Model.Keys.ToList();
                for (int i = 0; i < randomDeletes; i++)
                {
                    var index = random.Next(0, remaining.Count);
                    await churn.Delete(remaining[index]);
                    remaining[index] = remaining[^1];
                    remaining.RemoveAt(remaining.Count - 1);
                }
                for (int i = 0; i < window + randomDeletes; i++)
                {
                    await churn.Upsert(nextKey++, round);
                }
                await Sample(churn, result, state);
                result.Rounds = round;
                if (round >= minRounds && result.NonVacuous)
                {
                    break;
                }
            }
            return result;
        }

        public static async Task<ChurnResult> RunT1(TreeShape shape, bool useReadCache, bool zstd, bool ephemeral, string name, int liveKeys = 20_000, int window = 4_000, int randomDeletes = 1_000, int minRounds = 50, int maxRounds = 400, int seed = 1, bool directIO = false)
        {
            using var harness = await EphemeralHarness.Create(name, cachePageCount: CachePageCount, useReadCache: useReadCache, zstd: zstd, directIO: directIO);
            var node = harness.Manager.GetOrCreateClient("node");
            if (shape == TreeShape.CountBased)
            {
                var tree = (BPlusTree<long, int, ListKeyContainer<long>, ListValueContainer<int>>)(ephemeral
                    ? await node.GetOrCreateEphemeralTree("tree", CountOptions())
                    : await node.GetOrCreateTree("tree", CountOptions()));
                var churn = CountTree(harness, tree);
                var limits = ChurnLimits.Derive(shape, zstd, churn.Spy.Inner.GetLayoutForTests().Alignment);
                return await RunConstantLive(churn, limits, liveKeys, window, randomDeletes, minRounds, maxRounds, seed);
            }
            else
            {
                var tree = (BPlusTree<int, string, PrimitiveListKeyContainer<int>, SizedStringValueContainer>)(ephemeral
                    ? await node.GetOrCreateEphemeralTree("tree", ByteOptions())
                    : await node.GetOrCreateTree("tree", ByteOptions()));
                var churn = ByteTree(harness, tree);
                var limits = ChurnLimits.Derive(shape, zstd, churn.Spy.Inner.GetLayoutForTests().Alignment);
                return await RunConstantLive(churn, limits, liveKeys, window, randomDeletes, minRounds, maxRounds, seed);
            }
        }
    }
}
