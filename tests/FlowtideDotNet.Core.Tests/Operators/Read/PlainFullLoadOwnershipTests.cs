using FlowtideDotNet.Base;
using FlowtideDotNet.Base.Vertices;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Core.Operators.Read;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Persistence.Reservoir;
using FlowtideDotNet.Storage.Persistence.Reservoir.Internal;
using FlowtideDotNet.Storage.Persistence.Reservoir.MemoryDisk;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Type;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Core.Tests.Operators.Read;

public class PlainFullLoadOwnershipTests : OperatorTestBase
{
    protected override StateManagerOptions CreateStateManagerOptions() => new()
    {
        PersistentStorage = new ReservoirPersistentStorage(new ReservoirStorageOptions { FileProvider = new MemoryFileProvider() })
    };

    private sealed class Source : ColumnBatchReadBaseOperator
    {
        public EventBatchWeighted? Batch { get; set; }

        public Source() : base(new ReadRelation
        {
            NamedTable = new NamedTable { Names = ["t"] },
            BaseSchema = new NamedStruct
            {
                Names = ["key"],
                Struct = new Struct { Types = [new AnyType()] }
            }
        }, new FunctionsRegister(), new DataflowBlockOptions()) { }

        public override string DisplayName => "plain ownership test";
        public override Task Compact() => Task.CompletedTask;
        protected override Task Checkpoint(long checkpointTime) => Task.CompletedTask;
        protected override Task<IReadOnlySet<string>> GetWatermarkNames() => Task.FromResult<IReadOnlySet<string>>(new HashSet<string> { "t" });
        protected override ValueTask<List<int>> GetPrimaryKeyColumns() => ValueTask.FromResult(new List<int> { 0 });
        public Task RunFull(IngressOutput<StreamEventBatch> output) => DoFullLoad(output);

        protected override async IAsyncEnumerable<ColumnReadEvent> FullLoad(CancellationToken cancellationToken, [EnumeratorCancellation] CancellationToken enumeratorCancellationToken = default)
        {
            await Task.CompletedTask;
            if (Batch != null)
            {
                var batch = Batch;
                Batch = null;
                yield return new ColumnReadEvent(batch, 1);
            }
        }

        protected override async IAsyncEnumerable<DeltaReadEvent> DeltaLoad(Func<Task> enter, Action exit, CancellationToken cancellationToken, [EnumeratorCancellation] CancellationToken enumeratorCancellationToken = default)
        {
            await Task.CompletedTask;
            yield break;
        }
    }

    private sealed class ThrowingTarget : ITargetBlock<IStreamEvent>
    {
        public Task Completion => Task.CompletedTask;
        public void Complete() { }
        public void Fault(Exception exception) { }

        public DataflowMessageStatus OfferMessage(DataflowMessageHeader header, IStreamEvent value, ISourceBlock<IStreamEvent>? source, bool consumeToAccept)
            => throw new InvalidOperationException("the send failed");
    }

    [Fact]
    public async Task AFailedPlainFullLoadSendReleasesItsInputColumns()
    {
        var source = new Source();
        await InitializeOperator(source);
        var initialized = (IngressState<StreamEventBatch>)typeof(IngressVertex<StreamEventBatch>)
            .GetField("_ingressState", BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Instance)!.GetValue(source)!;
        using var output = new IngressOutput<StreamEventBatch>(new IngressState<StreamEventBatch>
        {
            _linkCount = 1,
            _tokenSource = new CancellationTokenSource(),
            _checkpointLock = new SemaphoreSlim(1, 1),
            _vertexHandler = initialized._vertexHandler
        }, new ThrowingTarget());
        var key = Column.Create(GlobalMemoryManager.Instance);
        key.Add(new Int64Value(1));
        var weights = new PrimitiveList<int>(GlobalMemoryManager.Instance);
        weights.Add(1);
        var iterations = new PrimitiveList<uint>(GlobalMemoryManager.Instance);
        iterations.Add(0);
        source.Batch = new EventBatchWeighted(weights, iterations, new EventBatchData([key]));
        try
        {
            var error = await Assert.ThrowsAsync<InvalidOperationException>(() => source.RunFull(output));
            Assert.Equal("the send failed", error.Message);
            await source.DisposeAsync();
            Assert.True((bool)typeof(Column).GetField("disposedValue", BindingFlags.NonPublic | BindingFlags.Instance)!.GetValue(key)!,
                "The failed send and source disposal must release the full-load input column.");
        }
        finally
        {
            key.Dispose();
            weights.Dispose();
            iterations.Dispose();
            await source.DisposeAsync();
        }
    }
}
