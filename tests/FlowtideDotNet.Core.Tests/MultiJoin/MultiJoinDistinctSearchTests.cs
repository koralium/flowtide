using FlowtideDotNet.Base.Engine;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Core.Operators.Join.MultiJoin;
using FlowtideDotNet.Storage;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Persistence.CacheStorage;
using FlowtideDotNet.Storage.StateManager;
using Microsoft.Extensions.Logging.Abstractions;
using System.Diagnostics.Metrics;

namespace FlowtideDotNet.Core.Tests.MultiJoin
{
    [Collection(nameof(MultiJoinAllocationCollection))]
    public class MultiJoinDistinctSearchTests
    {
        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task DistinctSearchDoesNotAddBoxingPerNonScalarKey(bool nested)
        {
            string folder = $"./data/multiJoinDistinctSearch/{Guid.NewGuid():N}";
            bool priorDistinct = MultiJoinTreeLookup.DistinctKeySearch;
            try
            {
                using var storage = new FileCachePersistentStorage(new FileCacheOptions { DirectoryPath = folder + "/persistent" });
                using var meter = new Meter("distinct-allocation-review");
                using var manager = new StateManagerSync<StreamState>(new StateManagerOptions
                {
                    PersistentStorage = storage,
                    TemporaryStorageOptions = new FileCacheOptions { DirectoryPath = folder + "/temporary" }
                }, NullLoggerFactory.Instance, meter, "stream", GlobalMemoryManager.Instance);
                await manager.InitializeAsync();
                var writer = await MultiJoinInputState.CreateWriter(manager.GetOrCreateClient("lookup"), "rows", new List<int> { 0 }, 1, GlobalMemoryManager.Instance);
                var lookup = new MultiJoinTreeLookup(writer.Tree, new List<int> { 0 });
                using var keys = Column.Create(GlobalMemoryManager.Instance);
                for (int i = 0; i < 512; i++)
                {
                    if (nested) keys.Add(new ListValue(new Int64Value(i)));
                    else if (i % 2 == 0) keys.Add(new Int64Value(i));
                    else keys.Add(new DoubleValue(i));
                }
                long Measure(int count, bool distinct)
                {
                    MultiJoinTreeLookup.DistinctKeySearch = distinct;
                    lookup.KeyColumns[0] = keys;
                    for (int i = 0; i < 4; i++)
                    {
                        var warm = lookup.Start(count);
                        Assert.True(warm.IsCompletedSuccessfully);
                        warm.GetAwaiter().GetResult();
                    }
                    long before = GC.GetAllocatedBytesForCurrentThread();
                    var work = lookup.Start(count);
                    long allocated = GC.GetAllocatedBytesForCurrentThread() - before;
                    Assert.True(work.IsCompletedSuccessfully);
                    work.GetAwaiter().GetResult();
                    return allocated;
                }
                try
                {
                    long smallWithout = Measure(64, false);
                    long smallWith = Measure(64, true);
                    long largeWithout = Measure(512, false);
                    long largeWith = Measure(512, true);
                    long extraGrowth = (largeWith - largeWithout) - (smallWith - smallWithout);
                    Assert.True(extraGrowth <= 4096,
                        $"Distinct-key tagging adds per-row managed allocation: 64 rows off/on={smallWithout}/{smallWith}, 512 rows off/on={largeWithout}/{largeWith}, extra growth={extraGrowth}.");
                }
                finally { lookup.Abandon(); }
            }
            finally
            {
                MultiJoinTreeLookup.DistinctKeySearch = priorDistinct;
                if (Directory.Exists(folder)) Directory.Delete(folder, true);
            }
        }

        [Theory]
        [InlineData(1, false)]
        [InlineData(17, false)]
        [InlineData(1024, false)]
        [InlineData(1, true)]
        [InlineData(17, true)]
        [InlineData(1024, true)]
        public async Task DuplicateCompositeKeysRetainParentsAcrossLeavesAndRestarts(int budget, bool matchNulls)
        {
            string folder = $"./data/multiJoinDistinctSearch/{Guid.NewGuid():N}";
            bool priorDistinct = MultiJoinTreeLookup.DistinctKeySearch;
            MultiJoinTreeLookup.DistinctKeySearch = true;
            try
            {
                using var storage = new FileCachePersistentStorage(new FileCacheOptions { DirectoryPath = folder + "/persistent" });
                using var meter = new Meter("distinct-search-review");
                using var manager = new StateManagerSync<StreamState>(new StateManagerOptions
                {
                    PersistentStorage = storage,
                    TemporaryStorageOptions = new FileCacheOptions { DirectoryPath = folder + "/temporary" },
                    DefaultBPlusTreePageSizeBytes = 512
                }, NullLoggerFactory.Instance, meter, "stream", GlobalMemoryManager.Instance);
                await manager.InitializeAsync();
                var writer = await MultiJoinInputState.CreateWriter(manager.GetOrCreateClient("lookup"), "rows", new List<int> { 0, 1, 2 }, 3, GlobalMemoryManager.Instance);
                using var key = Column.Create(GlobalMemoryManager.Instance);
                using var decimalKey = Column.Create(GlobalMemoryManager.Instance);
                using var payload = Column.Create(GlobalMemoryManager.Instance);
                const int fanout = 129;
                for (int group = 0; group < 3; group++)
                {
                    for (int row = 0; row < fanout; row++)
                    {
                        if (group == 0) key.Add(NullValue.Instance);
                        else key.Add(new Int64Value(group));
                        decimalKey.Add(new DecimalValue(1.0m));
                        payload.Add(new Int64Value(row));
                    }
                }
                var batch = new EventBatchData(new IColumn[] { key, decimalKey, payload });
                for (int row = 0; row < fanout * 3; row++)
                {
                    await writer.Tree.Upsert(new ColumnRowReference { referenceBatch = batch, RowIndex = row }, new MultiJoinWeight(row % fanout + 1));
                }
                int pagesForKeyOne = 0;
                using (var iterator = writer.Tree.CreateIterator())
                {
                    await iterator.SeekFirst();
                    await foreach (var page in iterator)
                    {
                        if (page.Any(pair => pair.Key.referenceBatch.Columns[0].GetValueAt(pair.Key.RowIndex, default).Type == ArrowTypeId.Int64 &&
                                             pair.Key.referenceBatch.Columns[0].GetValueAt(pair.Key.RowIndex, default).AsLong == 1))
                        {
                            pagesForKeyOne++;
                        }
                    }
                }
                Assert.True(pagesForKeyOne > 1, "The duplicated key must span multiple leaves.");
                var lookup = new MultiJoinTreeLookup(writer.Tree, new List<int> { 0, 1 }, matchNulls);
                using var probes = Column.Create(GlobalMemoryManager.Instance);
                using var decimals = Column.Create(GlobalMemoryManager.Instance);
                int[] groups = { 2, 1, 0, 2, 1, 2, 9, 0, 1 };
                for (int i = 0; i < groups.Length; i++)
                {
                    if (groups[i] == 0) probes.Add(NullValue.Instance);
                    else probes.Add(new Int64Value(groups[i]));
                    decimals.Add(new DecimalValue(i == groups.Length - 1 ? 2.0m : i % 2 == 0 ? 1.0m : 1.00m));
                }
                var frame = new MultiJoinFrame();
                var copies = new[] { 2 };
                void ReleasePart()
                {
                    foreach (var column in frame.Columns) column?.Dispose();
                    Array.Clear(frame.Columns);
                }
                async Task Start(int count)
                {
                    lookup.KeyColumns[0] = probes;
                    lookup.KeyColumns[1] = decimals;
                    await lookup.Start(count);
                }
                async Task Verify(int count)
                {
                    await Start(count);
                    var found = new HashSet<(int parent, long payload)>();
                    while (await lookup.NextPart(frame, copies, budget, GlobalMemoryManager.Instance))
                    {
                        Assert.InRange(frame.Count, 1, budget);
                        try
                        {
                            for (int row = 0; row < frame.Count; row++)
                            {
                                int parent = frame.Parent[row];
                                long value = frame.Columns[0].GetValueAt(row, default).AsLong;
                                Assert.InRange(parent, 0, count - 1);
                                Assert.True(groups[parent] is 1 or 2 || (matchNulls && groups[parent] == 0));
                                Assert.NotEqual(groups.Length - 1, parent);
                                Assert.Equal(value + 1, frame.Weight[row]);
                                Assert.True(found.Add((parent, value)), "A source parent received a stored row twice.");
                            }
                        }
                        finally { ReleasePart(); }
                    }
                    for (int parent = 0; parent < count; parent++)
                    {
                        bool expected = parent != groups.Length - 1 && (groups[parent] is 1 or 2 || (matchNulls && groups[parent] == 0));
                        Assert.Equal(expected ? fanout : 0, found.Count(x => x.parent == parent));
                    }
                }
                try
                {
                    await Verify(groups.Length);
                    await Verify(1);
                    await Verify(groups.Length);
                    await Start(groups.Length);
                    Assert.True(await lookup.NextPart(frame, copies, budget, GlobalMemoryManager.Instance));
                    ReleasePart();
                    lookup.Abandon();
                    await Verify(groups.Length);
                }
                finally { lookup.Abandon(); }
            }
            finally
            {
                MultiJoinTreeLookup.DistinctKeySearch = priorDistinct;
                if (Directory.Exists(folder)) Directory.Delete(folder, true);
            }
        }
    }
}
