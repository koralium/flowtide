using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Core.Operators.LateMaterialization;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Substrait.Expressions;

namespace FlowtideDotNet.Core.Tests.LateMaterialization;

public class ParallelLookupScratchTests
{
    private static readonly IMemoryAllocator Allocator = GlobalMemoryManager.Instance;

    private sealed class GatedKeyColumn : Column, IColumn
    {
        private readonly TaskCompletionSource _copied;
        private readonly ManualResetEventSlim _release;

        public GatedKeyColumn(TaskCompletionSource copied, ManualResetEventSlim release) : base(Allocator)
        {
            _copied = copied;
            _release = release;
        }

        void IColumn.GetValueAt(in int index, in DataValueContainer value, in ReferenceSegment? child)
        {
            base.GetValueAt(index, value, child);
            _copied.TrySetResult();
            if (!_release.Wait(TimeSpan.FromSeconds(30)))
            {
                throw new TimeoutException("Lookup copy was not released.");
            }
        }
    }

    [Fact]
    public async Task ParallelLookupsHaveIndependentKeyCopyScratch()
    {
        using var stream = await RowHeapTests.Open();
        using var host = await RowStoreHost.CreateAsync(stream.Client, "keys", "rows",
            new RowStoreHostLayout(2, [0], [], [1]),
            new RowStoreOptions { Mode = RowStoreMode.Keyed }, Allocator);
        using var input = new EventBatchData([Column.Create(Allocator), Column.Create(Allocator)]);
        using var weights = new PrimitiveList<int>(Allocator);
        foreach (long key in new long[] { 1, 2 })
        {
            input.Columns[0].Add(new Int64Value(key));
            input.Columns[1].Add(new StringValue($"payload-{key}"));
            weights.Add(1);
        }
        using var applied = await host.ApplyAsync(input, weights, null);
        using var release = new ManualResetEventSlim();
        var copied = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var firstKey = new EventBatchData([new GatedKeyColumn(copied, release)]);
        using var secondKey = new EventBatchData([Column.Create(Allocator)]);
        firstKey.Columns[0].Add(new Int64Value(1));
        secondKey.Columns[0].Add(new Int64Value(2));
        Column[]? firstRows = null;
        Column[]? secondRows = null;
        var first = Task.Run(async () => await host.LookupAsync(new ColumnRowReference { referenceBatch = firstKey, RowIndex = 0 }));
        try
        {
            await copied.Task.WaitAsync(TimeSpan.FromSeconds(30));
            secondRows = await host.LookupAsync(new ColumnRowReference { referenceBatch = secondKey, RowIndex = 0 });
        }
        finally
        {
            release.Set();
            firstRows = await first.WaitAsync(TimeSpan.FromSeconds(30));
        }
        try
        {
            Assert.NotNull(firstRows);
            Assert.NotNull(secondRows);
            Assert.Equal(1, firstRows[0].GetValueAt(0, default).AsLong);
            Assert.Equal("payload-1", firstRows[1].GetValueAt(0, default).AsString.ToString());
            Assert.Equal(2, secondRows[0].GetValueAt(0, default).AsLong);
        }
        finally
        {
            if (firstRows != null) foreach (var column in firstRows) column.Dispose();
            if (secondRows != null) foreach (var column in secondRows) column.Dispose();
        }
    }
}
