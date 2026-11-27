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

public class RowStoreHostRegressionTests : OperatorTestBase
{
    private static readonly IMemoryAllocator Allocator = GlobalMemoryManager.Instance;

    protected override StateManagerOptions CreateStateManagerOptions() => new()
    {
        PersistentStorage = new ReservoirPersistentStorage(new ReservoirStorageOptions { FileProvider = new MemoryFileProvider() })
    };

    private static Task<RowStoreHost> Host(RowHeapTests.Stream stream) => RowStoreHost.CreateAsync(
        stream.Client, "keys", "rows", new RowStoreHostLayout(3, [0], [1], [2]),
        new RowStoreOptions { Mode = RowStoreMode.Keyed }, Allocator);

    private static EventBatchWeighted Batch(long key, string payload, bool keyOnly = false, int weight = 1)
    {
        var columns = new IColumn[] { Column.Create(Allocator), Column.Create(Allocator), Column.Create(Allocator) };
        columns[0].Add(new Int64Value(key));
        if (!keyOnly)
        {
            columns[1].Add(new Int64Value(10));
            columns[2].Add(new StringValue(payload));
        }
        var weights = new PrimitiveList<int>(Allocator);
        weights.Add(weight);
        var iterations = new PrimitiveList<uint>(Allocator);
        iterations.Add(0);
        return new EventBatchWeighted(weights, iterations, new EventBatchData(columns));
    }

    private static void DisposeBatch(EventBatchWeighted batch)
    {
        batch.EventBatchData.Dispose();
        batch.Weights.Dispose();
        batch.Iterations.Dispose();
    }

    [Fact]
    public async Task KeyOnlyDeleteUsesTheStoredReference()
    {
        using var stream = await RowHeapTests.Open();
        using var host = await Host(stream);
        var initial = Batch(1, "old");
        var deletion = Batch(1, "", keyOnly: true, weight: -1);
        try
        {
            using var first = await host.ApplyAsync(initial.EventBatchData, initial.Weights, null);
            var reference = first.InsertReferences.GetValueAt(0, default).AsLong;
            using var removed = await host.ApplyAsync(deletion.EventBatchData, deletion.Weights, null);
            Assert.Equal(1, removed.DeleteRows.Count);
            Assert.Equal(reference, removed.DeleteReferences.GetValueAt(0, default).AsLong);
            Assert.Equal(0, host.VerifiedRows);
        }
        finally
        {
            DisposeBatch(initial);
            DisposeBatch(deletion);
        }
    }

    [Fact]
    public async Task FailedHostCreationReleasesItsHeapWindow()
    {
        using var stream = await RowHeapTests.Open(cachePages: 340);
        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => Host(stream));
        Assert.Contains("reader", error.Message);
        Assert.Equal(16, stream.Manager.CacheTable.ReservedHeldPages);
    }

    private sealed class ThrowingKeptColumn : Column, IColumn
    {
        public ThrowingKeptColumn() : base(Allocator) { }

        IDataValue IColumn.GetValueAt(in int index, in FlowtideDotNet.Substrait.Expressions.ReferenceSegment? child)
            => throw new InvalidOperationException("injected kept-column read failure");

        void IColumn.GetValueAt(in int index, in DataValueContainer dataValueContainer, in FlowtideDotNet.Substrait.Expressions.ReferenceSegment? child)
            => throw new InvalidOperationException("injected kept-column read failure");
    }

    [Fact]
    public async Task FailedSearchReleasesItsLeafWhenTheHostIsDisposed()
    {
        using var stream = await RowHeapTests.Open();
        using var host = await Host(stream);
        var initial = Batch(1, "old");
        var changed = Batch(1, "old");
        changed.EventBatchData.Columns[1].Dispose();
        var throwing = new ThrowingKeptColumn();
        throwing.Add(new Int64Value(10));
        changed.EventBatchData.GetColumns_Unsafe()[1] = throwing;
        try
        {
            using var first = await host.ApplyAsync(initial.EventBatchData, initial.Weights, null);
            var error = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
                await host.ApplyAsync(changed.EventBatchData, changed.Weights, null));
            Assert.Contains("injected", error.Message);
            var searcher = typeof(RowStoreHost).GetField("_searcher", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(host)!;
            host.Dispose();
            var currentLeaf = searcher.GetType().GetField("_currentLeaf", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(searcher);
            Assert.Null(currentLeaf);
        }
        finally
        {
            DisposeBatch(initial);
            DisposeBatch(changed);
        }
    }

    private sealed class Source : ColumnBatchReadBaseOperator
    {
        public Queue<EventBatchWeighted> Batches { get; } = new();

        public Source(bool hosted = true) : base(new ReadRelation
        {
            NamedTable = new NamedTable { Names = ["t"] },
            BaseSchema = new NamedStruct
            {
                Names = ["key", "kept", "payload"],
                Struct = new Struct { Types = [new AnyType(), new AnyType(), new AnyType()] }
            },
            RowStore = new RowStoreDefinition() { Name = "rows", StoredColumns = [2] }
        }, new FunctionsRegister(), new DataflowBlockOptions()) { SetHosted(hosted); }

        public void SetHosted(bool hosted)
        {
            var relation = (ReadRelation)typeof(ColumnBatchReadBaseOperator)
                .GetField("_readRelation", BindingFlags.NonPublic | BindingFlags.Instance)!.GetValue(this)!;
            relation.RowStore = hosted ? new RowStoreDefinition() { Name = "rows", StoredColumns = [2] } : null;
        }

        public Task Cut() => OnCheckpoint(1);

        public bool Append { get; set; }

        protected override bool AppendOnly => Append;

        public bool Loaded => ((FlowtideDotNet.Storage.StateManager.IObjectState<bool>)typeof(ColumnBatchReadBaseOperator)
            .GetField("_initialSent", BindingFlags.NonPublic | BindingFlags.Instance)!.GetValue(this)!).Value;

        public void MarkLoaded()
        {
            var sent = (FlowtideDotNet.Storage.StateManager.IObjectState<bool>)typeof(ColumnBatchReadBaseOperator)
                .GetField("_initialSent", BindingFlags.NonPublic | BindingFlags.Instance)!.GetValue(this)!;
            sent.Value = true;
        }

        public override string DisplayName => "ownership regression source";
        public override Task Compact() => Task.CompletedTask;
        protected override Task Checkpoint(long checkpointTime) => Task.CompletedTask;
        protected override Task<IReadOnlySet<string>> GetWatermarkNames() => Task.FromResult<IReadOnlySet<string>>(new HashSet<string> { "t" });
        protected override ValueTask<List<int>> GetPrimaryKeyColumns() => ValueTask.FromResult(new List<int> { 0 });
        public Task RunFull(IngressOutput<StreamEventBatch> output) => DoFullLoad(output);

        protected override async IAsyncEnumerable<ColumnReadEvent> FullLoad(CancellationToken cancellationToken, [EnumeratorCancellation] CancellationToken enumeratorCancellationToken = default)
        {
            await Task.CompletedTask;
            while (Batches.TryDequeue(out var batch))
            {
                yield return new ColumnReadEvent(batch, 1);
            }
            if (FailAfterBatches)
            {
                throw new InvalidOperationException("the source failed");
            }
        }

        protected override async IAsyncEnumerable<DeltaReadEvent> DeltaLoad(Func<Task> enter, Action exit, CancellationToken cancellationToken, [EnumeratorCancellation] CancellationToken enumeratorCancellationToken = default)
        {
            await Task.CompletedTask;
            while (Deltas.TryDequeue(out var batch))
            {
                yield return new DeltaReadEvent(batch, null);
            }
        }

        public Queue<EventBatchWeighted> Deltas { get; } = new();

        public bool FailAfterBatches { get; set; }

        public Task RunDelta(IngressOutput<StreamEventBatch> output) => DoDeltaLoad(output);
    }

    private sealed class ImmediateTarget : ITargetBlock<IStreamEvent>
    {
        public Action<StreamEventBatch>? BeforeReturn { get; set; }
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
                BeforeReturn?.Invoke(message.Data);
                message.Data.Return();
            }
            return DataflowMessageStatus.Accepted;
        }
    }

    [Fact]
    public async Task AFailedSendClosesTheHostSoNoCheckpointKeepsUnsentRows()
    {
        var source = new Source();
        await InitializeOperator(source);
        var target = new ImmediateTarget();
        using var output = Output(source, target);
        target.BeforeReturn = _ => throw new InvalidOperationException("the send failed");
        source.Batches.Enqueue(Batch(1, "stored and not sent"));
        try
        {
            var send = await Assert.ThrowsAsync<InvalidOperationException>(() => source.RunFull(output));
            Assert.Equal("the send failed", send.Message);
            var cut = await Assert.ThrowsAsync<InvalidOperationException>(() => source.Cut());
            Assert.Equal("Row store 'rows' failed in an earlier call, it is opened anew from the last checkpoint.", cut.Message);
        }
        finally
        {
            await source.DisposeAsync();
        }
    }

    [Fact]
    public async Task AFailedSendOfADeltaClosesTheHost()
    {
        var source = new Source();
        await InitializeOperator(source);
        var target = new ImmediateTarget();
        using var output = Output(source, target);
        target.BeforeReturn = _ => throw new InvalidOperationException("the send failed");
        source.Deltas.Enqueue(Batch(1, "stored and not sent"));
        try
        {
            var send = await Assert.ThrowsAsync<InvalidOperationException>(() => source.RunDelta(output));
            Assert.Equal("the send failed", send.Message);
            var cut = await Assert.ThrowsAsync<InvalidOperationException>(() => source.Cut());
            Assert.Equal("Row store 'rows' failed in an earlier call, it is opened anew from the last checkpoint.", cut.Message);
        }
        finally
        {
            await source.DisposeAsync();
        }
    }

    [Fact]
    public async Task ASourceThatFailsInALoadClosesTheHost()
    {
        var source = new Source() { FailAfterBatches = true };
        await InitializeOperator(source);
        var target = new ImmediateTarget();
        using var output = Output(source, target);
        source.Batches.Enqueue(Batch(1, "half a load"));
        try
        {
            var load = await Assert.ThrowsAsync<InvalidOperationException>(() => source.RunFull(output));
            Assert.Equal("the source failed", load.Message);
            var cut = await Assert.ThrowsAsync<InvalidOperationException>(() => source.Cut());
            Assert.Equal("Row store 'rows' failed in an earlier call, it is opened anew from the last checkpoint.", cut.Message);
        }
        finally
        {
            await source.DisposeAsync();
        }
    }

    [Fact]
    public async Task AHostedLoadIsMarkedLoadedBeforeItsLockIsReleased()
    {
        var source = new Source();
        await InitializeOperator(source);
        var target = new ImmediateTarget();
        using var output = Output(source, target);
        source.Batches.Enqueue(Batch(1, "loaded"));
        try
        {
            Assert.False(source.Loaded);
            await source.RunFull(output);
            Assert.True(source.Loaded);
        }
        finally
        {
            await source.DisposeAsync();
        }
    }

    [Fact]
    public async Task APlainReloadKeepsItsKeyColumnUntilTheDeletesAreSent()
    {
        var source = new Source(hosted: false);
        await InitializeOperator(source);
        var target = new ImmediateTarget();
        using var output = Output(source, target);
        var sent = new List<string>();
        target.BeforeReturn = message =>
        {
            var columns = message.Data.EventBatchData.Columns;
            sent.Add($"{message.Data.Weights[0]}: {columns[0].GetValueAt(0, default).AsLong}|{columns[2].GetValueAt(0, default).AsString}");
        };
        source.Batches.Enqueue(Batch(1, "old"));
        try
        {
            await source.RunFull(output);
            source.MarkLoaded();
            source.Batches.Enqueue(Batch(1, "new"));
            await source.RunFull(output);
            Assert.Equal(["1: 1|old", "1: 1|new", "-1: 1|old"], sent);
        }
        finally
        {
            await source.DisposeAsync();
        }
    }

    [Fact]
    public async Task APlainLoadWhoseSendFailsFreesItsInputColumns()
    {
        var source = new Source(hosted: false);
        await InitializeOperator(source);
        var target = new ImmediateTarget();
        using var output = Output(source, target);
        target.BeforeReturn = _ => throw new InvalidOperationException("the send failed");
        var batch = Batch(1, "not sent");
        source.Batches.Enqueue(batch);
        try
        {
            var send = await Assert.ThrowsAsync<InvalidOperationException>(() => source.RunFull(output));
            Assert.Equal("the send failed", send.Message);
            Assert.All(batch.EventBatchData.Columns, column => Assert.True(IsDisposed(column)));
        }
        finally
        {
            await source.DisposeAsync();
            DisposeBatch(batch);
        }
    }

    [Fact]
    public async Task AFailedDeleteSendAfterAReloadClosesTheHost()
    {
        var source = new Source();
        await InitializeOperator(source);
        var target = new ImmediateTarget();
        using var output = Output(source, target);
        source.Batches.Enqueue(Batch(1, "removed by the reload"));
        await source.RunFull(output);
        source.MarkLoaded();
        target.BeforeReturn = message =>
        {
            if (message.Data.Weights[0] < 0) throw new InvalidOperationException("the delete send failed");
        };
        source.Batches.Enqueue(Batch(2, "new"));
        try
        {
            var send = await Assert.ThrowsAsync<InvalidOperationException>(() => source.RunFull(output));
            Assert.Equal("the delete send failed", send.Message);
            var cut = await Assert.ThrowsAsync<InvalidOperationException>(() => source.Cut());
            Assert.Equal("Row store 'rows' failed in an earlier call, it is opened anew from the last checkpoint.", cut.Message);
        }
        finally
        {
            await source.DisposeAsync();
        }
    }

    [Fact]
    public async Task AnAppendOnlySourceRefusesASecondFullLoad()
    {
        var source = new Source() { Append = true };
        await InitializeOperator(source);
        using var output = Output(source, new ImmediateTarget());
        source.Batches.Enqueue(Batch(1, "once"));
        await source.RunFull(output);
        source.MarkLoaded();
        var again = Batch(1, "once");
        source.Batches.Enqueue(again);
        try
        {
            var error = await Assert.ThrowsAsync<InvalidOperationException>(() => source.RunFull(output));
            Assert.Equal("Source 't' is append-only and stores its rows behind references, it is loaded once.", error.Message);
        }
        finally
        {
            DisposeBatch(again);
            await source.DisposeAsync();
        }
    }

    private static bool IsDisposed(IColumn column) => (bool)typeof(Column)
        .GetField("disposedValue", BindingFlags.NonPublic | BindingFlags.Instance)!.GetValue(column)!;

    private static IngressOutput<StreamEventBatch> Output(Source source, ImmediateTarget target)
    {
        var initialized = (IngressState<StreamEventBatch>)typeof(IngressVertex<StreamEventBatch>)
            .GetField("_ingressState", BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Instance)!.GetValue(source)!;
        return new(new IngressState<StreamEventBatch>
        {
            _linkCount = 1,
            _tokenSource = new CancellationTokenSource(),
            _checkpointLock = new SemaphoreSlim(1, 1),
            _vertexHandler = initialized._vertexHandler
        }, target);
    }

    [Fact]
    public async Task FullLoadKeepsTheKeyAliveUntilBothOutputsAreSent()
    {
        var source = new Source();
        await InitializeOperator(source);
        var target = new ImmediateTarget();
        using var output = Output(source, target);
        source.Batches.Enqueue(Batch(1, "old"));
        await source.RunFull(output);
        var replacement = Batch(1, "new");
        var key = replacement.EventBatchData.Columns[0];
        bool? disposedAtDelete = null;
        target.BeforeReturn = message =>
        {
            if (message.Data.Weights[0] < 0) disposedAtDelete = IsDisposed(key);
        };
        source.Batches.Enqueue(replacement);
        await source.RunFull(output);
        await source.DisposeAsync();
        Assert.NotNull(disposedAtDelete);
        Assert.False(disposedAtDelete);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task UnchangedFullLoadDisposesEveryInputColumn(bool hosted)
    {
        var source = new Source(hosted);
        await InitializeOperator(source);
        using var output = Output(source, new ImmediateTarget());
        source.Batches.Enqueue(Batch(1, "same"));
        await source.RunFull(output);
        var repeated = Batch(1, "same");
        source.Batches.Enqueue(repeated);
        await source.RunFull(output);
        await source.DisposeAsync();
        try
        {
            Assert.All(repeated.EventBatchData.Columns, column => Assert.True(IsDisposed(column)));
        }
        finally
        {
            DisposeBatch(repeated);
        }
    }
}
