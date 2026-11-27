using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.Operators.LateMaterialization;
using FlowtideDotNet.Core.Operators.Normalization;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Persistence.Reservoir;
using FlowtideDotNet.Storage.Persistence.Reservoir.Internal;
using FlowtideDotNet.Storage.Persistence.Reservoir.MemoryDisk;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Type;
using System.Buffers;
using System.Diagnostics;
using System.Reflection;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Core.Tests.LateMaterialization;

public class HostedNormalizationFailureTests : OperatorTestBase
{
    protected override StateManagerOptions CreateStateManagerOptions() => new()
    {
        PersistentStorage = new ReservoirPersistentStorage(new ReservoirStorageOptions { FileProvider = new MemoryFileProvider() })
    };

    private static StreamEventBatch Batch(string payload)
    {
        var allocator = GlobalMemoryManager.Instance;
        var columns = new IColumn[] { Column.Create(allocator), Column.Create(allocator) };
        columns[0].Add(new Int64Value(1));
        columns[1].Add(new StringValue(payload));
        var batch = new StreamEventBatch(new EventBatchWeighted(
            new PrimitiveList<int>(allocator) { 1 }, new PrimitiveList<uint>(allocator) { 0 }, new EventBatchData(columns)));
        batch.Rent(1);
        return batch;
    }

    private static void Release(StreamEventBatch batch)
    {
        batch.Rent(1);
        batch.Return();
    }

    private ColumnNormalizationOperator CreateOperator()
    {
        var relation = new NormalizationRelation
        {
            KeyIndex = [0],
            Input = new ReadRelation
            {
                NamedTable = new NamedTable { Names = ["t"] },
                BaseSchema = new NamedStruct
                {
                    Names = ["key", "payload"],
                    Struct = new Struct { Types = [new AnyType(), new AnyType()] }
                }
            }
        };
        relation.RowStore = new RowStoreDefinition() { Name = "rows", StoredColumns = [1] };
        return new ColumnNormalizationOperator(relation, FunctionsRegister, new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = 1 });
    }

    private sealed class OutputFailingAllocator : IMemoryAllocator
    {
        private readonly IMemoryAllocator _inner = GlobalMemoryManager.Instance;

        private static void Check()
        {
            if (new StackTrace().GetFrames().Any(frame => frame.GetMethod()?.Name == nameof(RowStoreHost.CreateBatches)))
            {
                throw new InvalidOperationException("injected normalization-output allocation failure");
            }
        }

        public FlowtideMemory AllocateMemory(int size)
        {
            Check();
            return _inner.AllocateMemory(size);
        }

        public void Realloc(ref FlowtideMemory memory, int size)
        {
            Check();
            _inner.Realloc(ref memory, size);
        }

        public IMemoryOwner<byte> Allocate(int size, int alignment)
        {
            Check();
            return _inner.Allocate(size, alignment);
        }

        public IMemoryOwner<byte> Realloc(IMemoryOwner<byte> memory, int size, int alignment)
        {
            Check();
            return _inner.Realloc(memory, size, alignment);
        }

        public void Free(ref FlowtideMemory memory) => _inner.Free(ref memory);
        public void RegisterAllocationToMetrics(int size) => _inner.RegisterAllocationToMetrics(size);
        public void RegisterFreeToMetrics(int size) => _inner.RegisterFreeToMetrics(size);
    }

    [Fact]
    public async Task FailedNormalizationOutputConstructionClosesTheHost()
    {
        var op = CreateOperator();
        await InitializeOperator(op);
        var input = Batch("stored and not sent");
        try
        {
            var flags = BindingFlags.Instance | BindingFlags.NonPublic;
            var host = (RowStoreHost)typeof(ColumnNormalizationOperator).GetField("_host", flags)!.GetValue(op)!;
            typeof(RowStoreHost).GetField("_memoryAllocator", flags)!.SetValue(host, new OutputFailingAllocator());
            await using var iterator = op.OnRecieve(input, 1).GetAsyncEnumerator();

            var error = await Assert.ThrowsAsync<InvalidOperationException>(() => iterator.MoveNextAsync().AsTask());

            Assert.Equal("injected normalization-output allocation failure", error.Message);
            Assert.Equal(1, host.Store.RowCount);
            var cut = await Assert.ThrowsAsync<InvalidOperationException>(() => host.BeginCheckpointAsync());
            Assert.Contains("failed in an earlier call", cut.Message);
        }
        finally
        {
            input.Return();
            await op.DisposeAsync();
        }
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task AbandoningNormalizationBeforeItsDeleteClosesOnlyTheAbandonedHost(bool abandon)
    {
        var op = CreateOperator();
        await InitializeOperator(op);
        var initial = Batch("old");
        var changed = Batch("new");
        try
        {
            await foreach (var output in op.OnRecieve(initial, 1))
            {
                Release(output);
            }
            await using (var iterator = op.OnRecieve(changed, 2).GetAsyncEnumerator())
            {
                Assert.True(await iterator.MoveNextAsync());
                Assert.Equal(1, iterator.Current.Data.Weights[0]);
                Release(iterator.Current);
                if (!abandon)
                {
                    Assert.True(await iterator.MoveNextAsync());
                    Assert.Equal(-1, iterator.Current.Data.Weights[0]);
                    Release(iterator.Current);
                    Assert.False(await iterator.MoveNextAsync());
                }
            }
            var host = (RowStoreHost)typeof(ColumnNormalizationOperator).GetField("_host", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(op)!;
            if (abandon)
            {
                var error = await Assert.ThrowsAsync<InvalidOperationException>(() => host.BeginCheckpointAsync());
                Assert.Contains("failed in an earlier call", error.Message);
            }
            else
            {
                await host.BeginCheckpointAsync();
                await host.CheckpointAsync();
            }
        }
        finally
        {
            initial.Return();
            changed.Return();
            await op.DisposeAsync();
        }
    }
}
