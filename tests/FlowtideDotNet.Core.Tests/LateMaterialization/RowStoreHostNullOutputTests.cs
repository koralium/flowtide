using System.Reflection;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.ColumnStore.Sort;
using FlowtideDotNet.Core.Operators.LateMaterialization;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Memory;

namespace FlowtideDotNet.Core.Tests.LateMaterialization;

public class RowStoreHostNullOutputTests
{
    private static bool Disposed(PrimitiveList<int> rows) => (bool)typeof(PrimitiveList<int>)
        .GetField("_disposedValue", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(rows)!;

    private static IColumn Nulls(bool view, int count)
    {
        if (!view)
        {
            return AlwaysNullColumn.Instance;
        }
        var offsets = new PrimitiveList<int>(GlobalMemoryManager.Instance);
        for (int i = 0; i < count; i++)
        {
            offsets.Add(i);
        }
        return new ColumnWithOffset(AlwaysNullColumn.Instance, offsets);
    }

    private static EventBatchWeighted Input(IColumn[] columns, params int[] weights)
    {
        var allocator = GlobalMemoryManager.Instance;
        var batch = new EventBatchWeighted(new PrimitiveList<int>(allocator), new PrimitiveList<uint>(allocator), new EventBatchData(columns));
        foreach (var weight in weights)
        {
            batch.Weights.Add(weight);
            batch.Iterations.Add(0);
        }
        batch.Rent(1);
        return batch;
    }

    private static void Sort(IColumn[] columns)
    {
        int[] sorted = [0, 1];
        int[] tags = [0, 0];
        var sortedSpan = sorted.AsSpan();
        var tagSpan = tags.AsSpan();
        new BatchSorter(columns.Length).SortDataWithTags(columns, ref sortedSpan, ref tagSpan);
        Assert.Equal(new[] { 1, 0 }, sorted);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task NullOutputColumnsRemainSortableWithoutReleasingSharedRowIndices(bool view)
    {
        using var stream = await RowHeapTests.Open();
        var allocator = GlobalMemoryManager.Instance;
        using var host = await RowStoreHost.CreateAsync(stream.Client, "keys", "rows",
            new RowStoreHostLayout(3, [0], [1], [2]), new RowStoreOptions { Mode = RowStoreMode.Keyed }, allocator);
        var keys = Column.Create(allocator);
        keys.Add(new Int64Value(2));
        keys.Add(new Int64Value(1));
        var payload = Column.Create(allocator);
        payload.Add(new StringValue("two"));
        payload.Add(new StringValue("one"));
        var input = Input([keys, Nulls(view, 2), payload], 1, 1);
        EventBatchWeighted? output = null;
        try
        {
            var result = await host.ApplyAsync(input.EventBatchData, input.Weights, null);
            var batches = host.CreateBatches(input.EventBatchData, result, [0, 1, 2]);
            output = Assert.IsType<EventBatchWeighted>(batches.inserts);
            Assert.Null(batches.deletes);
            output.Rent(1);

            Assert.IsType<Column>(output.EventBatchData.Columns[1]);
            Assert.Equal(2, output.EventBatchData.Columns[1].Count);
            Assert.Equal(ArrowTypeId.Null, output.EventBatchData.Columns[1].GetValueAt(1, default).Type);
            Assert.False(Disposed(result.InsertRows));
            Sort([output.EventBatchData.Columns[0], output.EventBatchData.Columns[1]]);

            output.Return();
            output = null;
            Assert.True(Disposed(result.InsertRows));
            Assert.Equal(2, keys.GetValueAt(0, default).AsLong);
            if (view)
            {
                Assert.False(Disposed(((ColumnWithOffset)input.EventBatchData.Columns[1]).Offsets));
            }
        }
        finally
        {
            output?.Return();
            input.Return();
        }
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task NullOnlyOutputDisposesUnusedRowIndices(bool deletion, bool view)
    {
        using var stream = await RowHeapTests.Open();
        var allocator = GlobalMemoryManager.Instance;
        var layout = deletion ? new RowStoreHostLayout(2, [0], [], [1]) : new RowStoreHostLayout(2, [], [0], [1]);
        using var host = await RowStoreHost.CreateAsync(stream.Client, "keys", "rows", layout,
            new RowStoreOptions { Mode = deletion ? RowStoreMode.Keyed : RowStoreMode.Append }, allocator);
        var payload = Column.Create(allocator);
        payload.Add(new StringValue("payload"));
        var input = Input([Nulls(view, 1), payload], 1);
        EventBatchWeighted? output = null;
        try
        {
            if (deletion)
            {
                using var initial = await host.ApplyAsync(input.EventBatchData, input.Weights, null);
                input.Weights[0] = -1;
            }
            var result = await host.ApplyAsync(input.EventBatchData, input.Weights, null);
            var rows = deletion ? result.DeleteRows : result.InsertRows;
            var batches = host.CreateBatches(input.EventBatchData, result, [0, 1]);
            output = Assert.IsType<EventBatchWeighted>(deletion ? batches.deletes : batches.inserts);
            Assert.Null(deletion ? batches.inserts : batches.deletes);
            output.Rent(1);

            Assert.True(Disposed(rows));
            var nulls = Assert.IsType<Column>(output.EventBatchData.Columns[0]);
            Assert.Equal(1, nulls.Count);
            Assert.Equal(ArrowTypeId.Null, nulls.GetValueAt(0, default).Type);
            nulls.GetColumnState();
            Assert.Equal(deletion ? -1 : 1, output.Weights[0]);
            if (view)
            {
                Assert.False(Disposed(((ColumnWithOffset)input.EventBatchData.Columns[0]).Offsets));
            }
        }
        finally
        {
            output?.Return();
            input.Return();
        }
    }
}
