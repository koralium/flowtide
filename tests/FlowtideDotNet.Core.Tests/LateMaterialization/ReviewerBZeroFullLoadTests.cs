using FlowtideDotNet.Base;
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
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Core.Tests.LateMaterialization;

public class ReviewerBZeroFullLoadTests : OperatorTestBase
{
    protected override StateManagerOptions CreateStateManagerOptions() => new()
    {
        PersistentStorage = new ReservoirPersistentStorage(new ReservoirStorageOptions { FileProvider = new MemoryFileProvider() })
    };

    private sealed class Source : ColumnBatchReadBaseOperator
    {
        public Source(bool hosted) : base(Relation(hosted), new FunctionsRegister(), new DataflowBlockOptions()) { }
        public int Weight { get; set; }
        public override string DisplayName => "zero-weight source";
        public override Task Compact() => Task.CompletedTask;
        protected override Task Checkpoint(long checkpointTime) => Task.CompletedTask;
        protected override Task<IReadOnlySet<string>> GetWatermarkNames() => Task.FromResult<IReadOnlySet<string>>(new HashSet<string> { "t" });
        protected override ValueTask<List<int>> GetPrimaryKeyColumns() => ValueTask.FromResult(new List<int> { 0 });
        public Task RunFull(IngressOutput<StreamEventBatch> output) => DoFullLoad(output);

        public void MarkLoaded()
        {
            var initial = (IObjectState<bool>)typeof(ColumnBatchReadBaseOperator).GetField("_initialSent", BindingFlags.NonPublic | BindingFlags.Instance)!.GetValue(this)!;
            initial.Value = true;
        }

        protected override async IAsyncEnumerable<ColumnReadEvent> FullLoad(CancellationToken cancellationToken, [EnumeratorCancellation] CancellationToken enumeratorCancellationToken = default)
        {
            await Task.CompletedTask;
            var columns = new IColumn[] { Column.Create(GlobalMemoryManager.Instance), Column.Create(GlobalMemoryManager.Instance) };
            columns[0].Add(new Int64Value(1));
            columns[1].Add(new StringValue("same"));
            var weights = new PrimitiveList<int>(GlobalMemoryManager.Instance) { Weight };
            var iterations = new PrimitiveList<uint>(GlobalMemoryManager.Instance) { 0 };
            yield return new ColumnReadEvent(new EventBatchWeighted(weights, iterations, new EventBatchData(columns)), 1);
        }

        protected override async IAsyncEnumerable<DeltaReadEvent> DeltaLoad(Func<Task> enter, Action exit, CancellationToken cancellationToken, [EnumeratorCancellation] CancellationToken enumeratorCancellationToken = default)
        {
            await Task.CompletedTask;
            yield break;
        }
    }

    private static ReadRelation Relation(bool hosted)
    {
        var relation = new ReadRelation
        {
            NamedTable = new NamedTable { Names = ["t"] },
            BaseSchema = new NamedStruct { Names = ["key", "payload"], Struct = new Struct { Types = [new AnyType(), new AnyType()] } }
        };
        if (hosted) relation.RowStore = new RowStoreDefinition() { Name = "rows", StoredColumns = [1] };
        return relation;
    }

    private sealed class Target : ITargetBlock<IStreamEvent>
    {
        public List<int> Weights { get; } = new();
        public Task Completion => Task.CompletedTask;
        public void Complete() { }
        public void Fault(Exception exception) { }
        public DataflowMessageStatus OfferMessage(DataflowMessageHeader header, IStreamEvent value, ISourceBlock<IStreamEvent>? source, bool consumeToAccept)
        {
            if (consumeToAccept)
            {
                value = source!.ConsumeMessage(header, this, out bool consumed)!;
                if (!consumed) return DataflowMessageStatus.NotAvailable;
            }
            if (value is StreamMessage<StreamEventBatch> message)
            {
                for (int i = 0; i < message.Data.Data.Count; i++) Weights.Add(message.Data.Data.Weights[i]);
                message.Data.Return();
            }
            return DataflowMessageStatus.Accepted;
        }
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(true, false)]
    [InlineData(false, true)]
    [InlineData(true, true)]
    public async Task FullLoadZeroWeightMatchesPlainSnapshotSemantics(bool hosted, bool reload)
    {
        var source = new Source(hosted);
        await InitializeOperator(source);
        var initialized = (IngressState<StreamEventBatch>)typeof(IngressVertex<StreamEventBatch>).GetField("_ingressState", BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Instance)!.GetValue(source)!;
        var target = new Target();
        using var output = new IngressOutput<StreamEventBatch>(new IngressState<StreamEventBatch>
        {
            _linkCount = 1,
            _tokenSource = new CancellationTokenSource(),
            _checkpointLock = new SemaphoreSlim(1, 1),
            _vertexHandler = initialized._vertexHandler
        }, target);
        try
        {
            if (reload)
            {
                source.Weight = 1;
                await source.RunFull(output);
                source.MarkLoaded();
                target.Weights.Clear();
            }
            source.Weight = 0;
            await source.RunFull(output);
            Assert.Equal(reload ? Array.Empty<int>() : new[] { 1 }, target.Weights);
        }
        finally
        {
            await source.DisposeAsync();
        }
    }
}
