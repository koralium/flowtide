using FlowtideDotNet.Base;
using FlowtideDotNet.Base.Metrics;
using FlowtideDotNet.Base.Vertices;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.ColumnStore.Sort;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
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
using Microsoft.Extensions.Logging;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Core.Tests.LateMaterialization;

public class ReviewerCRound5Tests : OperatorTestBase
{
    protected override StateManagerOptions CreateStateManagerOptions() => new()
    {
        PersistentStorage = new ReservoirPersistentStorage(new ReservoirStorageOptions { FileProvider = new MemoryFileProvider() })
    };

    private sealed class EmptySource : ColumnBatchReadBaseOperator
    {
        public EmptySource() : base(new ReadRelation
        {
            NamedTable = new NamedTable { Names = ["t"] },
            BaseSchema = new NamedStruct
            {
                Names = ["key", "kept", "payload"],
                Struct = new Struct { Types = [new AnyType(), new AnyType(), new AnyType()] }
            },
            Hint = new FlowtideDotNet.Substrait.Hints.Hint
            {
                Optimizations = new FlowtideDotNet.Substrait.Hints.HintOptimizations
                {
                    Properties = new Dictionary<string, string> { [RowStoreHost.DeferredColumnsHint] = "2" }
                }
            }
        }, new FunctionsRegister(), new DataflowBlockOptions())
        {
        }

        public bool Loaded => ((IObjectState<bool>)typeof(ColumnBatchReadBaseOperator)
            .GetField("_initialSent", BindingFlags.NonPublic | BindingFlags.Instance)!.GetValue(this)!).Value;

        public override string DisplayName => "empty hosted source";
        public override Task Compact() => Task.CompletedTask;
        protected override Task Checkpoint(long checkpointTime) => Task.CompletedTask;
        protected override Task<IReadOnlySet<string>> GetWatermarkNames() => Task.FromResult<IReadOnlySet<string>>(new HashSet<string> { "t" });
        protected override ValueTask<List<int>> GetPrimaryKeyColumns() => ValueTask.FromResult(new List<int> { 0 });
        public Task RunFull(IngressOutput<StreamEventBatch> output) => DoFullLoad(output);

        protected override async IAsyncEnumerable<ColumnReadEvent> FullLoad(CancellationToken cancellationToken, [EnumeratorCancellation] CancellationToken enumeratorCancellationToken = default)
        {
            await Task.CompletedTask;
            yield break;
        }

        protected override async IAsyncEnumerable<DeltaReadEvent> DeltaLoad(Func<Task> enter, Action exit, CancellationToken cancellationToken, [EnumeratorCancellation] CancellationToken enumeratorCancellationToken = default)
        {
            await Task.CompletedTask;
            yield break;
        }
    }

    private sealed class CountingHandler : IVertexHandler
    {
        private readonly IVertexHandler _inner;

        public CountingHandler(IVertexHandler inner)
        {
            _inner = inner;
        }

        public int Scheduled { get; private set; }
        public string OperatorId => _inner.OperatorId;
        public string StreamName => _inner.StreamName;
        public IMeter Metrics => _inner.Metrics;
        public IStateManagerClient StateClient => _inner.StateClient;
        public ILoggerFactory LoggerFactory => _inner.LoggerFactory;
        public IOperatorMemoryManager MemoryManager => _inner.MemoryManager;
        public void ScheduleCheckpoint(TimeSpan time, long? providedCheckpointToken) => Scheduled++;
        public Task RegisterTrigger(string name, TimeSpan? scheduledInterval = null) => _inner.RegisterTrigger(name, scheduledInterval);
        public Task FailAndRollback(Exception? exception, long? restoreVersion = default) => _inner.FailAndRollback(exception, restoreVersion);
    }

    private sealed class DiscardingTarget : ITargetBlock<IStreamEvent>
    {
        public Task Completion => Task.CompletedTask;
        public void Complete() { }
        public void Fault(Exception exception) { }

        public DataflowMessageStatus OfferMessage(DataflowMessageHeader header, IStreamEvent value, ISourceBlock<IStreamEvent>? source, bool consumeToAccept)
        {
            if (value is StreamMessage<StreamEventBatch> message)
            {
                message.Data.Return();
            }
            return DataflowMessageStatus.Accepted;
        }
    }

    [Fact]
    public async Task AnEmptyInitialHostedLoadSchedulesACheckpointAndAnEmptyReloadDoesNot()
    {
        var source = new EmptySource();
        await InitializeOperator(source);
        var state = (IngressState<StreamEventBatch>)typeof(IngressVertex<StreamEventBatch>)
            .GetField("_ingressState", BindingFlags.NonPublic | BindingFlags.Instance)!.GetValue(source)!;
        var handler = new CountingHandler(state._vertexHandler!);
        state._vertexHandler = handler;
        using var output = new IngressOutput<StreamEventBatch>(new IngressState<StreamEventBatch>
        {
            _linkCount = 1,
            _tokenSource = new CancellationTokenSource(),
            _checkpointLock = new SemaphoreSlim(1, 1),
            _vertexHandler = handler
        }, new DiscardingTarget());
        try
        {
            await source.RunFull(output);
            Assert.True(source.Loaded);
            Assert.Equal(1, handler.Scheduled);

            await source.RunFull(output);
            Assert.Equal(1, handler.Scheduled);
        }
        finally
        {
            await source.DisposeAsync();
        }
    }

    [Fact]
    public async Task ABareAlwaysNullKeptColumnIsSentAsASortableColumnOfNulls()
    {
        using var stream = await RowHeapTests.Open();
        var layout = new RowStoreHostLayout(3, [0], [1], [2]);
        using var host = await RowStoreHost.CreateAsync(stream.Client, "keys", "rows", layout, new RowStoreOptions { Mode = RowStoreMode.Keyed }, GlobalMemoryManager.Instance);
        using var keys = Column.Create(GlobalMemoryManager.Instance);
        using var payload = Column.Create(GlobalMemoryManager.Instance);
        keys.Add(new Int64Value(2));
        keys.Add(new Int64Value(1));
        payload.Add(new StringValue("a"));
        payload.Add(new StringValue("b"));
        var batch = new EventBatchData([keys, AlwaysNullColumn.Instance, payload]);
        using var weights = new PrimitiveList<int>(GlobalMemoryManager.Instance) { 1, 1 };

        var result = await host.ApplyAsync(batch, weights, null);
        var (inserts, deletes) = host.CreateBatches(batch, result, [0, 1, 2]);
        Assert.NotNull(inserts);
        try
        {
            Assert.Null(deletes);
            var nulls = inserts.EventBatchData.Columns[1];
            Assert.IsType<Column>(nulls);
            Assert.Equal(2, nulls.Count);
            Assert.Equal(ArrowTypeId.Null, nulls.GetValueAt(0, default).Type);
            Assert.Equal(ArrowTypeId.Null, nulls.GetValueAt(1, default).Type);
            Assert.Equal(new int[] { 1, 0 }, Sort(inserts.EventBatchData.Columns[0], nulls));
        }
        finally
        {
            inserts.Rent(1);
            inserts.Return();
        }
    }

    private static int[] Sort(IColumn keys, IColumn nulls)
    {
        var sorter = new BatchSorter(2);
        int[] sorted = [0, 1];
        int[] tags = [0, 0];
        var sortedSpan = sorted.AsSpan();
        var tagSpan = tags.AsSpan();
        sorter.SortDataWithTags([keys, nulls], ref sortedSpan, ref tagSpan);
        return sorted;
    }
}
