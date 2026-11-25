// Licensed under the Apache License, Version 2.0 (the "License")
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using FlowtideDotNet.Base;
using FlowtideDotNet.Base.Engine;
using FlowtideDotNet.Base.Engine.Internal;
using FlowtideDotNet.Base.Metrics;
using FlowtideDotNet.Base.Vertices;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Core.Connectors;
using FlowtideDotNet.Core.Engine;
using FlowtideDotNet.Core.Lineage;
using FlowtideDotNet.Core.Operators.LateMaterialization;
using FlowtideDotNet.Core.Operators.Read;
using FlowtideDotNet.Core.Operators.Write;
using FlowtideDotNet.Core.Operators.Write.Column;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Persistence.Reservoir;
using FlowtideDotNet.Storage.Persistence.Reservoir.Internal;
using FlowtideDotNet.Storage.Persistence.Reservoir.MemoryDisk;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Substrait;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Type;
using Microsoft.Extensions.Logging.Abstractions;
using System.Buffers;
using System.Collections.Immutable;
using System.Runtime.CompilerServices;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Core.Tests.LateMaterialization;

public class HostedSinkGuardTests
{
    private sealed class PeakMemoryManager : IOperatorMemoryManager
    {
        private readonly RowHeapTests.CountingAllocator _inner = new();
        public long OutstandingBytes => _inner.OutstandingBytes;
        public long Peak { get; private set; }
        public void ResetPeak() => Peak = _inner.OutstandingBytes;
        public IMemoryOwner<byte> Allocate(int size, int alignment)
        {
            var memory = _inner.Allocate(size, alignment);
            Peak = Math.Max(Peak, _inner.OutstandingBytes);
            return memory;
        }
        public IMemoryOwner<byte> Realloc(IMemoryOwner<byte> memory, int size, int alignment)
        {
            var result = _inner.Realloc(memory, size, alignment);
            Peak = Math.Max(Peak, _inner.OutstandingBytes);
            return result;
        }
        public void RegisterAllocationToMetrics(int size)
        {
            _inner.RegisterAllocationToMetrics(size);
            Peak = Math.Max(Peak, _inner.OutstandingBytes);
        }
        public void RegisterFreeToMetrics(int size) => _inner.RegisterFreeToMetrics(size);
    }

    private sealed class MeteredGroupedSink : ColumnGroupedWriteOperator
    {
        private readonly PeakMemoryManager _memory;
        private readonly IReadOnlyList<int> _primaryKeys;

        public MeteredGroupedSink(WriteRelation writeRelation, PeakMemoryManager memory, IReadOnlyList<int> primaryKeys)
            : base(ExecutionMode.OnCheckpoint, writeRelation, new ExecutionDataflowBlockOptions { BoundedCapacity = 100, MaxDegreeOfParallelism = 1 })
        {
            _memory = memory;
            _primaryKeys = primaryKeys;
        }

        public long MaxLiveBytesDuringUpload { get; set; }
        public List<string> Operations { get; } = new();

        public override string DisplayName => "metered grouped sink";

        protected override ValueTask<IReadOnlyList<int>> GetPrimaryKeyColumns() => ValueTask.FromResult(_primaryKeys);

        protected override void Checkpoint(long checkpointTime)
        {
        }

        protected override async Task UploadChanges(IAsyncEnumerable<ColumnWriteOperation> rows, Watermark watermark, bool isInitialData, CancellationToken cancellationToken)
        {
            await foreach (var row in rows.WithCancellation(cancellationToken))
            {
                long current = _memory.OutstandingBytes;
                if (current > MaxLiveBytesDuringUpload)
                {
                    MaxLiveBytesDuringUpload = current;
                }
                if (row.IsDeleted)
                {
                    var keyText = _primaryKeys.Count == 0
                        ? "empty"
                        : row.EventBatchData.Columns[_primaryKeys[0]].GetValueAt(row.Index, default).AsLong.ToString();
                    Operations.Add("delete:" + keyText);
                }
                else
                {
                    var cols = row.EventBatchData.Columns;
                    var lastValue = cols[cols.Count - 1].GetValueAt(row.Index, default);
                    var summary = lastValue.Type == ArrowTypeId.String ? lastValue.AsString.ToString() : lastValue.Type.ToString();
                    Operations.Add("upsert:" + summary);
                }
            }
        }
    }

    private sealed class SinkHarness : IAsyncDisposable
    {
        private readonly StreamMetrics _metrics = new("hosted-sink-audit");
        private readonly RowHeapTests.Stream _stream;
        private TaskCompletionSource? _checkpointDone;
        private long _checkpointId;

        private SinkHarness(RowHeapTests.Stream stream)
        {
            _stream = stream;
        }

        public required MeteredGroupedSink Sink { get; init; }
        public required PeakMemoryManager Memory { get; init; }

        public static async Task<SinkHarness> CreateAsync(
            RowHeapTests.Stream stream,
            WriteRelation writeRelation,
            FetchRequiredColumnsRelation? fetch,
            IReadOnlyList<RowStoreHandle>? handles,
            IReadOnlyList<int> primaryKeys)
        {
            var memory = new PeakMemoryManager();
            var sink = new MeteredGroupedSink(writeRelation, memory, primaryKeys);
            if (fetch != null && handles != null)
            {
                Assert.True(((IRowStoreFetchHostVertex)sink).TryAttachFetch(fetch, handles));
            }
            var harness = new SinkHarness(stream) { Sink = sink, Memory = memory };
            sink.Setup("hosted-sink-audit", "sink");
            sink.CreateBlock();
            sink.Link();
            ((IStreamEgressVertex)sink).SetCheckpointDoneFunction((_, _) => harness._checkpointDone?.TrySetResult(), (_, _) => { });
            var handler = new VertexHandler(
                "hosted-sink-audit",
                "sink",
                (_, _) => { },
                (_, _, _) => Task.CompletedTask,
                harness._metrics.GetOrCreateVertexMeter("sink", () => "sink"),
                stream.Manager.GetOrCreateClient("sink"),
                NullLoggerFactory.Instance,
                memory,
                (exception, _) =>
                {
                    if (exception != null)
                    {
                        harness._checkpointDone?.TrySetException(exception);
                    }
                    return Task.CompletedTask;
                });
            await sink.Initialize("sink", 0, 0, handler, null);
            return harness;
        }

        public async Task SendBatchAsync(IColumn[] columns, IReadOnlyList<int> rowWeights)
        {
            var allocator = GlobalMemoryManager.Instance;
            var weights = new PrimitiveList<int>(allocator);
            var iterations = new PrimitiveList<uint>(allocator);
            for (int i = 0; i < rowWeights.Count; i++)
            {
                weights.Add(rowWeights[i]);
                iterations.Add(0);
            }
            var batch = new StreamEventBatch(new EventBatchWeighted(weights, iterations, new EventBatchData(columns)));
            batch.Data.Rent(1);
            await ((ITargetBlock<IStreamEvent>)Sink).SendAsync(new StreamMessage<StreamEventBatch>(batch, 1));
        }

        public async Task CheckpointAsync()
        {
            _checkpointId++;
            var done = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            _checkpointDone = done;
            await ((ITargetBlock<IStreamEvent>)Sink).SendAsync(new Checkpoint(_checkpointId, _checkpointId + 1, 1));
            var completed = await Task.WhenAny(done.Task, Sink.Completion);
            if (completed == Sink.Completion)
            {
                await Sink.Completion;
            }
            await done.Task;
            await _stream.Manager.CheckpointAsync();
        }

        public async ValueTask DisposeAsync()
        {
            await Sink.DisposeAsync();
            _metrics.Dispose();
        }
    }

    private sealed class SingleBatchSource : ColumnBatchReadBaseOperator
    {
        public SingleBatchSource(ReadRelation readRelation, IFunctionsRegister functionsRegister, DataflowBlockOptions options)
            : base(readRelation, functionsRegister, options)
        {
        }

        public override string DisplayName => "single batch source";
        public override Task Compact() => Task.CompletedTask;
        protected override Task Checkpoint(long checkpointTime) => Task.CompletedTask;
        protected override Task<IReadOnlySet<string>> GetWatermarkNames() => Task.FromResult<IReadOnlySet<string>>(new HashSet<string> { "t" });
        protected override ValueTask<List<int>> GetPrimaryKeyColumns() => ValueTask.FromResult(new List<int> { 0 });

        protected override async IAsyncEnumerable<ColumnReadEvent> FullLoad(CancellationToken cancellationToken, [EnumeratorCancellation] CancellationToken enumeratorCancellationToken = default)
        {
            await Task.CompletedTask;
            var allocator = GlobalMemoryManager.Instance;
            var cols = new IColumn[]
            {
                Column.Create(allocator),
                Column.Create(allocator),
                Column.Create(allocator),
                Column.Create(allocator)
            };
            cols[0].Add(new Int64Value(1));
            cols[1].Add(new Int64Value(10));
            cols[2].Add(new StringValue("hello"));
            cols[3].Add(new StringValue("world"));
            yield return new ColumnReadEvent(
                new EventBatchWeighted(
                    new PrimitiveList<int>(allocator) { 1 },
                    new PrimitiveList<uint>(allocator) { 0 },
                    new EventBatchData(cols)),
                1);
        }

        protected override async IAsyncEnumerable<DeltaReadEvent> DeltaLoad(Func<Task> EnterCheckpointLock, Action ExitCheckpointLock, CancellationToken cancellationToken, [EnumeratorCancellation] CancellationToken enumeratorCancellationToken = default)
        {
            await Task.CompletedTask;
            await EnterCheckpointLock();
            try
            {
                var empty = new IColumn[]
                {
                    Column.Create(GlobalMemoryManager.Instance),
                    Column.Create(GlobalMemoryManager.Instance),
                    Column.Create(GlobalMemoryManager.Instance),
                    Column.Create(GlobalMemoryManager.Instance)
                };
                var weights = new PrimitiveList<int>(GlobalMemoryManager.Instance);
                var iterations = new PrimitiveList<uint>(GlobalMemoryManager.Instance);
                yield return new DeltaReadEvent(
                    new EventBatchWeighted(weights, iterations, new EventBatchData(empty)),
                    new Watermark("t", LongWatermarkValue.Create(1)));
            }
            finally
            {
                ExitCheckpointLock();
            }
        }
    }

    private sealed class SingleBatchSourceFactory : RegexConnectorSourceFactory
    {
        public SingleBatchSourceFactory() : base("*")
        {
        }

        public override IStreamIngressVertex CreateSource(ReadRelation readRelation, IFunctionsRegister functionsRegister, DataflowBlockOptions dataflowBlockOptions)
            => new SingleBatchSource(readRelation, functionsRegister, dataflowBlockOptions);

        public override TableLineageMetadata GetLineageMetadata(ReadRelation readRelation, bool includeSchema)
            => new("test", readRelation.NamedTable.DotSeperated, default);
    }

    private sealed class InspectingGroupedSink : ColumnGroupedWriteOperator
    {
        private readonly int _expectedWidth;
        private readonly TaskCompletionSource<string> _uploaded;

        public InspectingGroupedSink(WriteRelation writeRelation, TaskCompletionSource<string> uploaded, ExecutionDataflowBlockOptions options)
            : base(ExecutionMode.OnWatermark, writeRelation, options)
        {
            _expectedWidth = writeRelation.OutputLength;
            _uploaded = uploaded;
        }

        public override string DisplayName => "inspecting grouped sink";

        protected override ValueTask<IReadOnlyList<int>> GetPrimaryKeyColumns() => ValueTask.FromResult<IReadOnlyList<int>>(new List<int> { 0 });

        protected override void Checkpoint(long checkpointTime)
        {
        }

        protected override async Task OnRecieve(StreamEventBatch msg, long time)
        {
            try
            {
                var columns = msg.Data.EventBatchData.Columns;
                if (columns.Count != _expectedWidth)
                {
                    throw new InvalidOperationException($"Expected {_expectedWidth} columns matching WriteRelation, got {columns.Count}.");
                }
                for (int r = 0; r < msg.Data.Weights.Count; r++)
                {
                    for (int c = 0; c < _expectedWidth; c++)
                    {
                        if (columns[c].GetTypeAt(r, default) == ArrowTypeId.RowRef)
                        {
                            throw new InvalidOperationException($"Unexpected RowRef in column {c} of OnRecieve.");
                        }
                    }
                }
                await base.OnRecieve(msg, time);
            }
            catch (Exception ex)
            {
                _uploaded.TrySetException(ex);
                throw;
            }
        }

        protected override async Task UploadChanges(IAsyncEnumerable<ColumnWriteOperation> rows, Watermark watermark, bool isInitialData, CancellationToken cancellationToken)
        {
            await foreach (var row in rows.WithCancellation(cancellationToken))
            {
                if (!row.IsDeleted)
                {
                    var cols = row.EventBatchData.Columns;
                    var formatted = $"{cols[0].GetValueAt(row.Index, default).AsLong}|{cols[1].GetValueAt(row.Index, default).AsLong}|{cols[2].GetValueAt(row.Index, default).AsString}|{cols[3].GetValueAt(row.Index, default).AsString}";
                    _uploaded.TrySetResult(formatted);
                }
            }
        }
    }

    private sealed class InspectingSinkFactory : RegexConnectorSinkFactory
    {
        private readonly TaskCompletionSource<string> _uploaded;

        public InspectingSinkFactory(TaskCompletionSource<string> uploaded) : base("*")
        {
            _uploaded = uploaded;
        }

        public override IStreamEgressVertex CreateSink(WriteRelation writeRelation, IFunctionsRegister functionsRegister, ExecutionDataflowBlockOptions dataflowBlockOptions)
            => new InspectingGroupedSink(writeRelation, _uploaded, dataflowBlockOptions);

        public override TableLineageMetadata GetLineageMetadata(WriteRelation writeRelation, bool includeSchema)
            => new("test", writeRelation.NamedObject.DotSeperated, default);
    }

    private static (ReadRelation read, FetchRequiredColumnsRelation fetch, WriteRelation write) BuildThreeColumnFetchPlan()
    {
        var read = new ReadRelation
        {
            NamedTable = new NamedTable { Names = ["t"] },
            BaseSchema = new NamedStruct
            {
                Names = ["key", "number", "payload"],
                Struct = new Struct { Types = [new AnyType(), new AnyType(), new AnyType()] }
            },
            RowStore = new RowStoreDefinition { Name = "rows", StoredColumns = [2] }
        };
        var fetch = new FetchRequiredColumnsRelation
        {
            Input = read,
            Fetches = [new RowStoreFetch { StoreName = "rows", ReferenceColumn = 2, Columns = [0] }],
            Emit = [0, 1, 3]
        };
        var write = new WriteRelation
        {
            Input = fetch,
            NamedObject = new NamedTable { Names = ["out"] },
            TableSchema = new NamedStruct { Names = ["key", "number", "payload"] }
        };
        return (read, fetch, write);
    }

    [Fact]
    public async Task SingleKeyExceedingChunkBoundsStreamsInBoundedChunks()
    {
        using var stream = await RowHeapTests.Open();
        var allocator = GlobalMemoryManager.Instance;
        var (read, fetch, write) = BuildThreeColumnFetchPlan();
        var layout = RowStoreHost.ReadLayout(read.RowStore!, 3, [0, 1], [0, 1, 2])!;
        using var host = await RowStoreHost.CreateAsync(stream.Client, "host-keys", "rows", layout, new RowStoreOptions { Mode = RowStoreMode.Keyed }, allocator);
        const int rowCount = 2500;
        var payload = new string('x', 8192);
        using var source = new EventBatchData(new IColumn[] { Column.Create(allocator), Column.Create(allocator), Column.Create(allocator) });
        using var sourceWeights = new PrimitiveList<int>(allocator);
        for (int i = 0; i < rowCount; i++)
        {
            source.Columns[0].Add(new Int64Value(7));
            source.Columns[1].Add(new Int64Value(i));
            source.Columns[2].Add(new StringValue(payload));
            sourceWeights.Add(1);
        }
        using var applied = await host.ApplyAsync(source, sourceWeights, null);
        var handle = new RowStoreHandle("rows") { HasHost = true };
        handle.Open(host);

        await using var harness = await SinkHarness.CreateAsync(stream, write, fetch, [handle], [0]);
        var columns = new IColumn[] { Column.Create(allocator), Column.Create(allocator), Column.Create(allocator) };
        var weights = new int[rowCount];
        for (int i = 0; i < rowCount; i++)
        {
            columns[0].Add(new Int64Value(7));
            columns[1].Add(new Int64Value(i));
            columns[2].Add(new RowRefValue((ulong)applied.InsertReferences.GetValueAt(i, default).AsLong));
            weights[i] = 1;
        }
        await harness.SendBatchAsync(columns, weights);
        harness.Sink.MaxLiveBytesDuringUpload = 0;
        await harness.CheckpointAsync();

        Assert.Single(harness.Sink.Operations);
        Assert.True(
            harness.Sink.MaxLiveBytesDuringUpload <= 12L * 1024 * 1024,
            $"Single-key upload materialized {harness.Sink.MaxLiveBytesDuringUpload} live bytes, exceeding the 8 MiB chunk bound.");
    }

    [Fact]
    public async Task SingleKeyExceedingChunkBoundsPeaksWithinTheBoundWhileItIsRead()
    {
        using var stream = await RowHeapTests.Open();
        var allocator = GlobalMemoryManager.Instance;
        var (read, fetch, write) = BuildThreeColumnFetchPlan();
        var layout = RowStoreHost.ReadLayout(read.RowStore!, 3, [0, 1], [0, 1, 2])!;
        using var host = await RowStoreHost.CreateAsync(stream.Client, "host-keys-peak", "rows", layout, new RowStoreOptions { Mode = RowStoreMode.Keyed }, allocator);
        const int rowCount = 5000;
        var payload = new string('x', 8192);
        using var source = new EventBatchData(new IColumn[] { Column.Create(allocator), Column.Create(allocator), Column.Create(allocator) });
        using var sourceWeights = new PrimitiveList<int>(allocator);
        for (int i = 0; i < rowCount; i++)
        {
            source.Columns[0].Add(new Int64Value(7));
            source.Columns[1].Add(new Int64Value(i));
            source.Columns[2].Add(new StringValue(payload));
            sourceWeights.Add(1);
        }
        using var applied = await host.ApplyAsync(source, sourceWeights, null);
        var handle = new RowStoreHandle("rows") { HasHost = true };
        handle.Open(host);

        await using var harness = await SinkHarness.CreateAsync(stream, write, fetch, [handle], [0]);
        var columns = new IColumn[] { Column.Create(allocator), Column.Create(allocator), Column.Create(allocator) };
        var weights = new int[rowCount];
        for (int i = 0; i < rowCount; i++)
        {
            columns[0].Add(new Int64Value(7));
            columns[1].Add(new Int64Value(i));
            columns[2].Add(new RowRefValue((ulong)applied.InsertReferences.GetValueAt(i, default).AsLong));
            weights[i] = 1;
        }
        await harness.SendBatchAsync(columns, weights);
        harness.Memory.ResetPeak();
        long before = harness.Memory.Peak;
        await harness.CheckpointAsync();

        Assert.Single(harness.Sink.Operations);
        Assert.InRange(harness.Memory.Peak - before, 0, 24L * 1024 * 1024);
    }

    [Fact]
    public async Task DeleteOnlyChunksDoNotGrowFetchChunkRowsBackToMax()
    {
        using var stream = await RowHeapTests.Open();
        var allocator = GlobalMemoryManager.Instance;
        var (read, fetch, write) = BuildThreeColumnFetchPlan();
        var layout = RowStoreHost.ReadLayout(read.RowStore!, 3, [0], [0, 1, 2])!;
        using var host = await RowStoreHost.CreateAsync(stream.Client, "host-keys-del", "rows", layout, new RowStoreOptions { Mode = RowStoreMode.Keyed }, allocator);
        const int initialRows = 3100;
        const int secondWaveRows = 1024;
        var wide = new string('w', 36_000);
        using var source = new EventBatchData(new IColumn[] { Column.Create(allocator), Column.Create(allocator), Column.Create(allocator) });
        using var sourceWeights = new PrimitiveList<int>(allocator);
        for (int i = 0; i < initialRows + secondWaveRows; i++)
        {
            source.Columns[0].Add(new Int64Value(i));
            source.Columns[1].Add(new Int64Value(i));
            source.Columns[2].Add(new StringValue(wide));
            sourceWeights.Add(1);
        }
        using var applied = await host.ApplyAsync(source, sourceWeights, null);
        var handle = new RowStoreHandle("rows") { HasHost = true };
        handle.Open(host);

        await using var harness = await SinkHarness.CreateAsync(stream, write, fetch, [handle], [0]);
        var insertCols = new IColumn[] { Column.Create(allocator), Column.Create(allocator), Column.Create(allocator) };
        var insertWeights = new int[initialRows];
        for (int i = 0; i < initialRows; i++)
        {
            insertCols[0].Add(new Int64Value(i));
            insertCols[1].Add(new Int64Value(i));
            insertCols[2].Add(new RowRefValue((ulong)applied.InsertReferences.GetValueAt(i, default).AsLong));
            insertWeights[i] = 1;
        }
        await harness.SendBatchAsync(insertCols, insertWeights);
        await harness.CheckpointAsync();

        var deleteCols = new IColumn[] { Column.Create(allocator), Column.Create(allocator), Column.Create(allocator) };
        var deleteWeights = new int[initialRows];
        for (int i = 0; i < initialRows; i++)
        {
            deleteCols[0].Add(new Int64Value(i));
            deleteCols[1].Add(new Int64Value(i));
            deleteCols[2].Add(new RowRefValue((ulong)applied.InsertReferences.GetValueAt(i, default).AsLong));
            deleteWeights[i] = -1;
        }
        await harness.SendBatchAsync(deleteCols, deleteWeights);
        await harness.CheckpointAsync();

        var nextCols = new IColumn[] { Column.Create(allocator), Column.Create(allocator), Column.Create(allocator) };
        var nextWeights = new int[secondWaveRows];
        for (int i = 0; i < secondWaveRows; i++)
        {
            int id = initialRows + i;
            nextCols[0].Add(new Int64Value(id));
            nextCols[1].Add(new Int64Value(id));
            nextCols[2].Add(new RowRefValue((ulong)applied.InsertReferences.GetValueAt(id, default).AsLong));
            nextWeights[i] = 1;
        }
        await harness.SendBatchAsync(nextCols, nextWeights);
        harness.Sink.MaxLiveBytesDuringUpload = 0;
        await harness.CheckpointAsync();

        Assert.True(
            harness.Sink.MaxLiveBytesDuringUpload <= 12L * 1024 * 1024,
            $"Delete-only chunks reset adaptive chunk size, causing {harness.Sink.MaxLiveBytesDuringUpload} live bytes on the next upsert wave.");
    }

    [Fact]
    public async Task SubclassOverridingOnRecieveIsNotSilentlyHandedRowReferences()
    {
        var uploaded = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        var read = new ReadRelation
        {
            NamedTable = new NamedTable { Names = ["t"] },
            BaseSchema = new NamedStruct
            {
                Names = ["k", "n", "text", "note"],
                Struct = new Struct { Types = [new AnyType(), new AnyType(), new AnyType(), new AnyType()] }
            },
            RowStore = new RowStoreDefinition { Name = "t", StoredColumns = [2, 3] }
        };
        var fetch = new FetchRequiredColumnsRelation
        {
            Input = read,
            Fetches = [new RowStoreFetch { ReferenceColumn = 2, StoreName = "t", Columns = [1, 0] }],
            Emit = [0, 1, 3, 4]
        };
        var plan = new Plan
        {
            Relations =
            [
                new WriteRelation
                {
                    Input = fetch,
                    NamedObject = new NamedTable { Names = ["output"] },
                    TableSchema = new NamedStruct { Names = ["c0", "c1", "c2", "c3"] }
                }
            ]
        };
        var connectors = new ConnectorManager();
        connectors.AddSource(new SingleBatchSourceFactory());
        connectors.AddSink(new InspectingSinkFactory(uploaded));
        await using var dataflow = new FlowtideBuilder("subclass-onrecieve-audit")
            .AddPlan(plan, false)
            .AddConnectorManager(connectors)
            .WithStateOptions(new StateManagerOptions
            {
                PersistentStorage = new ReservoirPersistentStorage(new ReservoirStorageOptions { FileProvider = new MemoryFileProvider() })
            })
            .Build();

        await dataflow.StartAsync();
        var result = await uploaded.Task.WaitAsync(TimeSpan.FromSeconds(15));
        Assert.Equal("1|10|world|hello", result);
    }

    [Fact]
    public async Task HostedSinkRefusesRowReferencesInADeletedKey()
    {
        using var stream = await RowHeapTests.Open();
        var allocator = GlobalMemoryManager.Instance;
        var (read, fetch, write) = BuildThreeColumnFetchPlan();
        var layout = RowStoreHost.ReadLayout(read.RowStore!, 3, [0], [0, 1, 2])!;
        using var host = await RowStoreHost.CreateAsync(stream.Client, "host-deleted-key-guard", "rows", layout, new RowStoreOptions { Mode = RowStoreMode.Keyed }, allocator);
        using var source = new EventBatchData(new IColumn[] { Column.Create(allocator), Column.Create(allocator), Column.Create(allocator) });
        source.Columns[0].Add(new Int64Value(1));
        source.Columns[1].Add(new Int64Value(10));
        source.Columns[2].Add(new StringValue("stored"));
        using var sourceWeights = new PrimitiveList<int>(allocator) { 1 };
        using var applied = await host.ApplyAsync(source, sourceWeights, null);
        var handle = new RowStoreHandle("rows") { HasHost = true };
        handle.Open(host);

        await using var harness = await SinkHarness.CreateAsync(stream, write, fetch, [handle], [0]);
        var columns = new IColumn[] { Column.Create(allocator), Column.Create(allocator), Column.Create(allocator) };
        for (int i = 0; i < 2; i++)
        {
            columns[0].Add(new RowRefValue(999));
            columns[1].Add(new Int64Value(10));
            columns[2].Add(new RowRefValue((ulong)applied.InsertReferences.GetValueAt(0, default).AsLong));
        }

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await harness.SendBatchAsync(columns, [1, -1]);
            await harness.CheckpointAsync();
        });
        Assert.Contains("received row references", exception.Message);
    }

    [Fact]
    public async Task HostedSinkRefusesRowReferencesInKeptColumns()
    {
        using var stream = await RowHeapTests.Open();
        var allocator = GlobalMemoryManager.Instance;
        var (read, fetch, write) = BuildThreeColumnFetchPlan();
        var layout = RowStoreHost.ReadLayout(read.RowStore!, 3, [0], [0, 1, 2])!;
        using var host = await RowStoreHost.CreateAsync(stream.Client, "host-keys-guard", "rows", layout, new RowStoreOptions { Mode = RowStoreMode.Keyed }, allocator);
        using var source = new EventBatchData(new IColumn[] { Column.Create(allocator), Column.Create(allocator), Column.Create(allocator) });
        source.Columns[0].Add(new Int64Value(1));
        source.Columns[1].Add(new Int64Value(10));
        source.Columns[2].Add(new StringValue("stored"));
        using var sourceWeights = new PrimitiveList<int>(allocator) { 1 };
        using var applied = await host.ApplyAsync(source, sourceWeights, null);
        var handle = new RowStoreHandle("rows") { HasHost = true };
        handle.Open(host);

        await using var harness = await SinkHarness.CreateAsync(stream, write, fetch, [handle], [0]);
        var columns = new IColumn[] { Column.Create(allocator), Column.Create(allocator), Column.Create(allocator) };
        columns[0].Add(new Int64Value(1));
        columns[1].Add(new RowRefValue(999));
        columns[2].Add(new RowRefValue((ulong)applied.InsertReferences.GetValueAt(0, default).AsLong));

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await harness.SendBatchAsync(columns, [1]);
            await harness.CheckpointAsync();
        });
        Assert.Contains("received row references", exception.Message);
    }

    [Fact]
    public async Task OutOfRangeEmitIsRefusedInsteadOfSilentlySubstitutingColumnZero()
    {
        using var stream = await RowHeapTests.Open();
        var allocator = GlobalMemoryManager.Instance;
        var (read, _, _) = BuildThreeColumnFetchPlan();
        var layout = RowStoreHost.ReadLayout(read.RowStore!, 3, [0], [0, 1, 2])!;
        using var host = await RowStoreHost.CreateAsync(stream.Client, "host-keys-emit", "rows", layout, new RowStoreOptions { Mode = RowStoreMode.Keyed }, allocator);
        using var source = new EventBatchData(new IColumn[] { Column.Create(allocator), Column.Create(allocator), Column.Create(allocator) });
        source.Columns[0].Add(new Int64Value(1));
        source.Columns[1].Add(new Int64Value(10));
        source.Columns[2].Add(new StringValue("unintended-column-zero"));
        using var sourceWeights = new PrimitiveList<int>(allocator) { 1 };
        using var applied = await host.ApplyAsync(source, sourceWeights, null);
        var reference = (ulong)applied.InsertReferences.GetValueAt(0, default).AsLong;
        var handle = new RowStoreHandle("rows") { HasHost = true };
        handle.Open(host);

        var badFetch = new FetchRequiredColumnsRelation
        {
            Input = read,
            Fetches = [new RowStoreFetch { StoreName = "rows", ReferenceColumn = 2, Columns = [0] }],
            Emit = [0, 99]
        };
        var write = new WriteRelation
        {
            Input = badFetch,
            NamedObject = new NamedTable { Names = ["out"] },
            TableSchema = new NamedStruct { Names = ["key", "payload"] }
        };

        await Assert.ThrowsAnyAsync<Exception>(async () =>
        {
            await using var harness = await SinkHarness.CreateAsync(stream, write, badFetch, [handle], [0]);
            var insertCols = new IColumn[] { Column.Create(allocator), Column.Create(allocator), Column.Create(allocator) };
            insertCols[0].Add(new Int64Value(1));
            insertCols[1].Add(new Int64Value(10));
            insertCols[2].Add(new RowRefValue(reference));
            await harness.SendBatchAsync(insertCols, [1]);
            await harness.CheckpointAsync();
        });
    }
}
