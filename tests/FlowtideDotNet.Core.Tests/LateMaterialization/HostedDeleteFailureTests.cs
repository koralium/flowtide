using FlowtideDotNet.Base.Vertices;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Core.Operators.LateMaterialization;
using FlowtideDotNet.Core.Operators.Read;
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

public class HostedDeleteFailureTests : OperatorTestBase
{
    protected override StateManagerOptions CreateStateManagerOptions() => new()
    {
        PersistentStorage = new ReservoirPersistentStorage(new ReservoirStorageOptions { FileProvider = new MemoryFileProvider() })
    };

    private sealed class DeleteOutputFailingAllocator : IMemoryAllocator
    {
        private readonly IMemoryAllocator _inner = GlobalMemoryManager.Instance;

        private static void Check()
        {
            if (new StackTrace().GetFrames().Any(frame => frame.GetMethod()?.Name == nameof(RowStoreHost.CreateDeleteBatch)))
            {
                throw new InvalidOperationException("injected delete-output allocation failure");
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

    private sealed class Source : ColumnBatchReadBaseOperator
    {
        public Source() : base(new ReadRelation
        {
            NamedTable = new NamedTable { Names = ["t"] },
            BaseSchema = new NamedStruct
            {
                Names = ["key", "payload"],
                Struct = new Struct { Types = [new AnyType(), new AnyType()] }
            },
            RowStore = new RowStoreDefinition() { Name = "rows", StoredColumns = [1] }
        }, new FunctionsRegister(), new DataflowBlockOptions()) { }

        public override string DisplayName => "delete output failure source";
        public override Task Compact() => Task.CompletedTask;
        protected override Task Checkpoint(long checkpointTime) => Task.CompletedTask;
        protected override Task<IReadOnlySet<string>> GetWatermarkNames() => Task.FromResult<IReadOnlySet<string>>(new HashSet<string> { "t" });
        protected override ValueTask<List<int>> GetPrimaryKeyColumns() => ValueTask.FromResult(new List<int> { 0 });
        protected override IAsyncEnumerable<ColumnReadEvent> FullLoad(CancellationToken cancellationToken, CancellationToken enumeratorCancellationToken = default) => throw new NotSupportedException();
        protected override IAsyncEnumerable<DeltaReadEvent> DeltaLoad(Func<Task> enter, Action exit, CancellationToken cancellationToken, CancellationToken enumeratorCancellationToken = default) => throw new NotSupportedException();
    }

    [Fact]
    public async Task FailedDeleteBatchConstructionPoisonsTheHostBeforeCheckpoint()
    {
        var source = new Source();
        await InitializeOperator(source);
        try
        {
            var flags = BindingFlags.Instance | BindingFlags.NonPublic;
            var host = (RowStoreHost)typeof(ColumnBatchReadBaseOperator).GetField("_host", flags)!.GetValue(source)!;
            var deleted = (RowStoreKeySet)typeof(ColumnBatchReadBaseOperator).GetField("_hostedDeleteKeys", flags)!.GetValue(source)!;
            using var input = new EventBatchData([Column.Create(GlobalMemoryManager.Instance), Column.Create(GlobalMemoryManager.Instance)]);
            input.Columns[0].Add(new StringValue("a"));
            input.Columns[1].Add(new StringValue("payload"));
            using var weights = new PrimitiveList<int>(GlobalMemoryManager.Instance) { 1 };
            using (await host.ApplyAsync(input, weights, null)) { }
            await deleted.AddAsync(new EventBatchData([input.Columns[0]]), 1);
            typeof(RowStoreHost).GetField("_memoryAllocator", flags)!.SetValue(host, new DeleteOutputFailingAllocator());
            var remove = typeof(ColumnBatchReadBaseOperator).GetMethod("OutputDeletedRowsFromFullLoadHosted", flags)!;

            var error = await Assert.ThrowsAsync<InvalidOperationException>(() => (Task<bool>)remove.Invoke(source, [null])!);
            Assert.Equal("injected delete-output allocation failure", error.Message);
            await Assert.ThrowsAsync<InvalidOperationException>(() => host.BeginCheckpointAsync());
        }
        finally
        {
            await source.DisposeAsync();
        }
    }
}
