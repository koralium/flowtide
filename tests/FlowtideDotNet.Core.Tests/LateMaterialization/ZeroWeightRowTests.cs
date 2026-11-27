using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Core.Operators.LateMaterialization;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Memory;

namespace FlowtideDotNet.Core.Tests.LateMaterialization;

public class ZeroWeightRowTests
{
    private static readonly IMemoryAllocator Allocator = GlobalMemoryManager.Instance;

    private static Task<RowStoreHost> Host(RowHeapTests.Stream stream, RowStoreMode mode) => RowStoreHost.CreateAsync(
        stream.Client, "keys", "rows", new RowStoreHostLayout(2, [0], [], [1]),
        new RowStoreOptions { Mode = mode }, Allocator);

    private static async Task<(int Inserts, int Deletes)> Apply(RowStoreHost host, bool zeroWeightIsRow, params (long Key, string Payload, int Weight)[] rows)
    {
        using var batch = new EventBatchData(new IColumn[] { Column.Create(Allocator), Column.Create(Allocator) });
        using var weights = new PrimitiveList<int>(Allocator);
        foreach (var row in rows)
        {
            batch.Columns[0].Add(new Int64Value(row.Key));
            batch.Columns[1].Add(new StringValue(row.Payload));
            weights.Add(row.Weight);
        }
        using var result = await host.ApplyAsync(batch, weights, null, zeroWeightIsRow);
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
    public async Task ABatchOfOnlyZeroWeightRowsDoesNothingOnAManyLeafTree()
    {
        using var stream = await RowHeapTests.Open();
        using var host = await Host(stream, RowStoreMode.Keyed);
        var load = Enumerable.Range(0, 5000).Select(i => ((long)i, "payload" + i + new string('x', 40), 1)).ToArray();
        Assert.Equal((5000, 0), await Apply(host, false, load));

        Assert.Equal((0, 0), await Apply(host, false, (10, "changed", 0), (4000, "changed", 0), (99999, "new", 0)));
        Assert.Equal((0, 0), await Apply(host, false, (4000, "changed", 0)));
        Assert.Equal((0, 1), await Apply(host, false, (10, "", -1), (10, "ignored", 0)));
        await host.BeginCheckpointAsync();
        await host.CheckpointAsync();

        Assert.Null(await Lookup(host, 10));
        Assert.Null(await Lookup(host, 99999));
        Assert.Equal("payload4000" + new string('x', 40), await Lookup(host, 4000));
    }

    [Fact]
    public async Task AnAppendOnlyFullLoadKeepsARowOfWeightZeroAsTheKeyedLoadDoes()
    {
        using var stream = await RowHeapTests.Open();
        using var keyed = await Host(stream, RowStoreMode.Keyed);
        Assert.Equal((1, 0), await Apply(keyed, true, (1, "zero", 0)));

        using var other = await RowHeapTests.Open();
        using var appendOnly = await Host(other, RowStoreMode.Append);
        Assert.Equal((1, 0), await Apply(appendOnly, true, (1, "zero", 0)));
    }

    [Fact]
    public async Task ALoadedRowOfWeightZeroThatFailsTheFilterRemovesItsKey()
    {
        using var stream = await RowHeapTests.Open();
        using var host = await Host(stream, RowStoreMode.Keyed);
        Assert.Equal((1, 0), await Apply(host, true, (1, "old", 1)));

        using var batch = new EventBatchData(new IColumn[] { Column.Create(Allocator), Column.Create(Allocator) });
        using var weights = new PrimitiveList<int>(Allocator);
        batch.Columns[0].Add(new Int64Value(1));
        batch.Columns[1].Add(new StringValue("filtered"));
        weights.Add(0);
        using (var result = await host.ApplyAsync(batch, weights, (b, row) => false, zeroWeightIsRow: true))
        {
            Assert.Equal((0, 1), (result.InsertRows.Count, result.DeleteRows.Count));
        }
        Assert.Null(await Lookup(host, 1));
    }
}
