using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Core.Operators.LateMaterialization;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Memory;

namespace FlowtideDotNet.Core.Tests.LateMaterialization;

public class AlwaysNullStoredColumnTests
{
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task AnOffsetViewOfAnAlwaysNullStoredColumnIsStoredAsNulls(bool keyed)
    {
        using var stream = await RowHeapTests.Open();
        var layout = keyed ? new RowStoreHostLayout(3, [0], [1], [2]) : new RowStoreHostLayout(3, [], [0, 1], [2]);
        using var host = await RowStoreHost.CreateAsync(stream.Client, "keys", "rows", layout,
            new RowStoreOptions { Mode = keyed ? RowStoreMode.Keyed : RowStoreMode.Append }, GlobalMemoryManager.Instance);
        using var keys = Column.Create(GlobalMemoryManager.Instance);
        using var kept = Column.Create(GlobalMemoryManager.Instance);
        keys.Add(new Int64Value(1));
        keys.Add(new Int64Value(2));
        kept.Add(new Int64Value(10));
        kept.Add(new Int64Value(20));
        using var offsets = new PrimitiveList<int>(GlobalMemoryManager.Instance) { 0, 1 };
        var input = new EventBatchData([keys, kept, new ColumnWithOffset(AlwaysNullColumn.Instance, offsets)]);
        using var weights = new PrimitiveList<int>(GlobalMemoryManager.Instance) { 1, 1 };

        using var result = await host.ApplyAsync(input, weights, null);

        Assert.Equal(2, result.InsertRows.Count);
        Assert.Equal(2, host.Store.RowCount);
        if (keyed)
        {
            var row = await host.LookupAsync(new ColumnRowReference { referenceBatch = input, RowIndex = 1 });
            Assert.NotNull(row);
            try
            {
                Assert.Equal(20, row[1].GetValueAt(0, default).AsLong);
                Assert.Equal(ArrowTypeId.Null, row[2].GetValueAt(0, default).Type);
            }
            finally
            {
                foreach (var column in row)
                {
                    column.Dispose();
                }
            }
        }
    }
}
