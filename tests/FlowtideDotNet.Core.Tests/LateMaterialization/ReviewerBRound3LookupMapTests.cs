using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Core.Operators.LateMaterialization;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Memory;

namespace FlowtideDotNet.Core.Tests.LateMaterialization;

public class ReviewerBRound3LookupMapTests
{
    [Fact]
    public async Task SingleLookupDoesNotOverwriteTheLastBulkLookupRowMap()
    {
        var allocator = GlobalMemoryManager.Instance;
        using var stream = await RowHeapTests.Open();
        using var host = await RowStoreHost.CreateAsync(stream.Client, "keys", "rows",
            new RowStoreHostLayout(2, [0], [], [1]), new RowStoreOptions { Mode = RowStoreMode.Keyed }, allocator);
        using var input = new EventBatchData([Column.Create(allocator), Column.Create(allocator)]);
        input.Columns[0].Add(new Int64Value(1));
        input.Columns[1].Add(new StringValue("first"));
        using var weights = new PrimitiveList<int>(allocator) { 1 };
        using (await host.ApplyAsync(input, weights, null))
        {
        }
        using var keys = new EventBatchData([Column.Create(allocator)]);
        keys.Columns[0].Add(new Int64Value(999));
        keys.Columns[0].Add(new Int64Value(1));
        var (bulkRows, found) = await host.LookupManyAsync(keys, 2);
        try
        {
            Assert.Equal(1, found);
            Assert.Equal(1, host.LookupRows(found)[0]);
            var single = await host.LookupAsync(new ColumnRowReference { referenceBatch = input, RowIndex = 0 });
            try
            {
                Assert.NotNull(single);
                Assert.Equal(1, host.LookupRows(found)[0]);
            }
            finally
            {
                if (single != null)
                {
                    foreach (var column in single)
                    {
                        column.Dispose();
                    }
                }
            }
        }
        finally
        {
            if (bulkRows != null)
            {
                foreach (var column in bulkRows)
                {
                    column.Dispose();
                }
            }
        }
    }
}
