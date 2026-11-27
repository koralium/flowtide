using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Core.Operators.LateMaterialization;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Memory;

namespace FlowtideDotNet.Core.Tests.LateMaterialization;

public class RowStoreHostZeroWeightTests
{
    private static readonly IMemoryAllocator Allocator = GlobalMemoryManager.Instance;

    private static Task<RowStoreHost> Host(RowHeapTests.Stream stream) => RowStoreHost.CreateAsync(
        stream.Client, "keys", "rows", new RowStoreHostLayout(2, [0], [], [1]),
        new RowStoreOptions { Mode = RowStoreMode.Keyed }, Allocator);

    private static async Task<(int Inserts, int Deletes)> Apply(RowStoreHost host, params (long Key, string Payload, int Weight)[] rows)
    {
        using var batch = new EventBatchData(new IColumn[] { Column.Create(Allocator), Column.Create(Allocator) });
        using var weights = new PrimitiveList<int>(Allocator);
        foreach (var row in rows)
        {
            batch.Columns[0].Add(new Int64Value(row.Key));
            batch.Columns[1].Add(new StringValue(row.Payload));
            weights.Add(row.Weight);
        }
        using var result = await host.ApplyAsync(batch, weights, null);
        return (result.InsertRows.Count, result.DeleteRows.Count);
    }

    private static async Task<string?> Lookup(RowStoreHost host, long key)
    {
        using var batch = new EventBatchData(new IColumn[] { Column.Create(Allocator) });
        batch.Columns[0].Add(new Int64Value(key));
        var result = await host.LookupAsync(new ColumnRowReference { referenceBatch = batch, RowIndex = 0 });
        if (result == null)
        {
            return null;
        }
        try
        {
            return result[1].GetValueAt(0, default).AsString.ToString();
        }
        finally
        {
            foreach (var column in result)
            {
                column.Dispose();
            }
        }
    }

    [Fact]
    public async Task TrailingZeroWeightDoesNotSuppressEarlierInsertForSameKey()
    {
        using var stream = await RowHeapTests.Open();
        using var host = await Host(stream);
        var sent = await Apply(host, (1, "new", 1), (1, "ignored", 0));
        Assert.Equal((1, 0), sent);
        Assert.Equal("new", await Lookup(host, 1));
    }

    [Fact]
    public async Task TrailingZeroWeightDoesNotSuppressEarlierDeleteForSameKey()
    {
        using var stream = await RowHeapTests.Open();
        using var host = await Host(stream);
        await Apply(host, (1, "old", 1));
        var sent = await Apply(host, (1, "old", -1), (1, "ignored", 0));
        Assert.Equal((0, 1), sent);
        Assert.Null(await Lookup(host, 1));
    }

    [Fact]
    public async Task EmptyBulkLookupReturnsNoRowsAndLeavesHostUsable()
    {
        using var stream = await RowHeapTests.Open();
        using var host = await Host(stream);
        using var keys = new EventBatchData(new IColumn[] { Column.Create(Allocator) });
        var (rows, found) = await host.LookupManyAsync(keys, 0);
        Assert.Null(rows);
        Assert.Equal(0, found);
        Assert.Equal((1, 0), await Apply(host, (1, "new", 1)));
    }

}
