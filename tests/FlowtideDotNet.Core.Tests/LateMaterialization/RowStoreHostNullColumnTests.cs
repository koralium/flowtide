using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.Operators.LateMaterialization;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Memory;

namespace FlowtideDotNet.Core.Tests.LateMaterialization;

public class RowStoreHostNullColumnTests
{
    [Fact]
    public async Task AppendOnlyFullLoadTreatsZeroWeightAsARow()
    {
        using var stream = await RowHeapTests.Open();
        using var host = await RowStoreHost.CreateAsync(stream.Client, "keys", "rows",
            new RowStoreHostLayout(2, [], [0], [1]),
            new RowStoreOptions { Mode = RowStoreMode.Append }, GlobalMemoryManager.Instance);
        using var input = new EventBatchData([Column.Create(GlobalMemoryManager.Instance), Column.Create(GlobalMemoryManager.Instance)]);
        input.Columns[0].Add(new Int64Value(1));
        input.Columns[1].Add(new StringValue("payload"));
        using var weights = new PrimitiveList<int>(GlobalMemoryManager.Instance) { 0 };

        using var result = await host.ApplyAsync(input, weights, null, zeroWeightIsRow: true);

        Assert.Single(result.InsertRows);
    }

    [Fact]
    public async Task AppendOnlyHostAcceptsAlwaysNullDeferredColumn()
    {
        using var stream = await RowHeapTests.Open();
        using var host = await RowStoreHost.CreateAsync(stream.Client, "keys", "rows",
            new RowStoreHostLayout(2, [], [0], [1]),
            new RowStoreOptions { Mode = RowStoreMode.Append }, GlobalMemoryManager.Instance);
        using var kept = Column.Create(GlobalMemoryManager.Instance);
        kept.Add(new Int64Value(1));
        kept.Add(new Int64Value(2));
        var input = new EventBatchData([kept, AlwaysNullColumn.Instance]);
        using var weights = new PrimitiveList<int>(GlobalMemoryManager.Instance) { 1, 1 };

        using var result = await host.ApplyAsync(input, weights, null);

        Assert.Equal(2, result.InsertRows.Count);
        Assert.Equal(2, host.Store.RowCount);
    }

    [Fact]
    public async Task KeyedHostAcceptsOffsetViewOfAlwaysNullKeptColumn()
    {
        using var stream = await RowHeapTests.Open();
        using var host = await RowStoreHost.CreateAsync(stream.Client, "keys", "rows",
            new RowStoreHostLayout(3, [0], [1], [2]),
            new RowStoreOptions { Mode = RowStoreMode.Keyed }, GlobalMemoryManager.Instance);
        using var keys = Column.Create(GlobalMemoryManager.Instance);
        using var payload = Column.Create(GlobalMemoryManager.Instance);
        keys.Add(new Int64Value(1));
        keys.Add(new Int64Value(2));
        payload.Add(new StringValue("first"));
        payload.Add(new StringValue("second"));
        using var offsets = new PrimitiveList<int>(GlobalMemoryManager.Instance) { 0, 1 };
        var input = new EventBatchData([keys, new ColumnWithOffset(AlwaysNullColumn.Instance, offsets), payload]);
        using var weights = new PrimitiveList<int>(GlobalMemoryManager.Instance) { 1, 1 };

        using var result = await host.ApplyAsync(input, weights, null);

        Assert.Equal(2, result.InsertRows.Count);
    }
}
