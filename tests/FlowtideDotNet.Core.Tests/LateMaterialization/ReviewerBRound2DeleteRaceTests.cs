using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Core.Operators.LateMaterialization;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Memory;
using System.Buffers;

namespace FlowtideDotNet.Core.Tests.LateMaterialization;

[CollectionDefinition(nameof(ReviewerBRound2DeleteRaceCollection), DisableParallelization = true)]
public class ReviewerBRound2DeleteRaceCollection { }

[Collection(nameof(ReviewerBRound2DeleteRaceCollection))]
public class ReviewerBRound2DeleteRaceTests
{
    private sealed class GatedAllocator : IMemoryAllocator, IDisposable
    {
        private readonly IMemoryAllocator _inner = GlobalMemoryManager.Instance;
        private readonly ManualResetEventSlim _release = new(false);
        private int _armed;

        public TaskCompletionSource Entered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public void Arm() => Interlocked.Exchange(ref _armed, 1);

        public void Release() => _release.Set();

        public IMemoryOwner<byte> Allocate(int size, int alignment) => _inner.Allocate(size, alignment);

        public IMemoryOwner<byte> Realloc(IMemoryOwner<byte> memory, int size, int alignment) => _inner.Realloc(memory, size, alignment);

        public void RegisterAllocationToMetrics(int size)
        {
            _inner.RegisterAllocationToMetrics(size);
            if (Interlocked.Exchange(ref _armed, 0) != 0)
            {
                Entered.SetResult();
                _release.Wait();
            }
        }

        public void RegisterFreeToMetrics(int size) => _inner.RegisterFreeToMetrics(size);

        public void Dispose() => _release.Dispose();
    }

    [Fact]
    public async Task ConcurrentLookupCannotChangeFullLoadDeleteKey()
    {
        var allocator = GlobalMemoryManager.Instance;
        using var gate = new GatedAllocator();
        using var stream = await RowHeapTests.Open();
        using var host = await RowStoreHost.CreateAsync(stream.Client, "keys", "rows",
            new RowStoreHostLayout(2, [0], [], [1]), new RowStoreOptions { Mode = RowStoreMode.Keyed }, gate);
        using var initial = new EventBatchData(new IColumn[] { Column.Create(allocator), Column.Create(allocator) });
        initial.Columns[0].Add(new StringValue("a"));
        initial.Columns[0].Add(new StringValue("b"));
        initial.Columns[1].Add(new StringValue("first"));
        initial.Columns[1].Add(new StringValue("second"));
        using var weights = new PrimitiveList<int>(allocator);
        weights.Add(1);
        weights.Add(1);
        using (var applied = await host.ApplyAsync(initial, weights, null)) { }
        var key = Column.Create(allocator);
        using var deleteKeys = new EventBatchData(new IColumn[] { key });
        key.Add(new StringValue("a"));
        var result = host.CreateResult();
        await host.RemoveAsync(deleteKeys, 1, result);
        using var lookupKey = new EventBatchData(new IColumn[] { Column.Create(allocator) });
        lookupKey.Columns[0].Add(new StringValue("b"));
        gate.Arm();
        var deleting = Task.Run(() => host.CreateDeleteBatch([key], result, [0, 1]));
        EventBatchWeighted? deleted = null;
        try
        {
            await gate.Entered.Task.WaitAsync(TimeSpan.FromSeconds(30));
            var (found, foundCount) = await host.LookupManyAsync(lookupKey, 1);
            Assert.Equal(1, foundCount);
            Assert.NotNull(found);
            foreach (var column in found)
            {
                column.Dispose();
            }
        }
        finally
        {
            gate.Release();
            deleted = await deleting;
        }
        Assert.NotNull(deleted);
        try
        {
            Assert.Equal("a", deleted.EventBatchData.Columns[0].GetValueAt(0, default).AsString.ToString());
        }
        finally
        {
            deleted.EventBatchData.Dispose();
            deleted.Weights.Dispose();
            deleted.Iterations.Dispose();
        }
    }
}
