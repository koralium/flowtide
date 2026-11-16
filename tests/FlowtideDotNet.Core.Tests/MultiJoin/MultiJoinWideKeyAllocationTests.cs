using FlowtideDotNet.Base.Engine;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.DataValues;
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
    public class MultiJoinWideKeyAllocationTests
    {
        [Fact]
        public async Task DistinctSearchDoesNotAddBoxingPerEightColumnKey()
        {
            string folder = $"./data/multiJoinWideKey/{Guid.NewGuid():N}";
            bool priorDistinct = MultiJoinTreeLookup.DistinctKeySearch;
            var keys = new Column[8];
            try
            {
                using var storage = new FileCachePersistentStorage(new FileCacheOptions { DirectoryPath = folder + "/persistent" });
                using var meter = new Meter("wide-key-allocation");
                using var manager = new StateManagerSync<StreamState>(new StateManagerOptions
                {
                    PersistentStorage = storage,
                    TemporaryStorageOptions = new FileCacheOptions { DirectoryPath = folder + "/temporary" }
                }, NullLoggerFactory.Instance, meter, "stream", GlobalMemoryManager.Instance);
                await manager.InitializeAsync();
                var keyOrder = Enumerable.Range(0, keys.Length).ToList();
                var writer = await MultiJoinInputState.CreateWriter(manager.GetOrCreateClient("lookup"), "rows", keyOrder, keys.Length, GlobalMemoryManager.Instance);
                var lookup = new MultiJoinTreeLookup(writer.Tree, keyOrder);
                for (int column = 0; column < keys.Length; column++)
                {
                    keys[column] = Column.Create(GlobalMemoryManager.Instance);
                    for (int row = 0; row < 512; row++)
                    {
                        keys[column].Add(new Int64Value(column == 7 ? row % 8 : 1));
                    }
                    lookup.KeyColumns[column] = keys[column];
                }

                long Measure(int count, bool distinct)
                {
                    MultiJoinTreeLookup.DistinctKeySearch = distinct;
                    for (int iteration = 0; iteration < 4; iteration++)
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
                        $"Distinct tagging adds boxing for wide keys: 64 rows off/on={smallWithout}/{smallWith}, 512 rows off/on={largeWithout}/{largeWith}, extra growth={extraGrowth}.");
                }
                finally
                {
                    lookup.Abandon();
                }
            }
            finally
            {
                MultiJoinTreeLookup.DistinctKeySearch = priorDistinct;
                foreach (var column in keys) column?.Dispose();
                if (Directory.Exists(folder)) Directory.Delete(folder, true);
            }
        }
    }
}
