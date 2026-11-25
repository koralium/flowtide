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
using FlowtideDotNet.Base.Engine.Internal;
using FlowtideDotNet.Base.Metrics;
using FlowtideDotNet.Base.Vertices;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.Operators.LateMaterialization;
using FlowtideDotNet.Core.Operators.Write;
using FlowtideDotNet.Core.Operators.Write.Column;
using FlowtideDotNet.Core.Tests.LateMaterialization.Differential;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Type;
using Microsoft.Extensions.Logging.Abstractions;
using System.Buffers;
using System.Reflection;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Core.Tests.LateMaterialization;

public class HostedSinkBoundTests
{
    private static Task Invoke(HostedSinkOperatorTests fixture, string method, params object[] arguments)
    {
        return (Task)typeof(HostedSinkOperatorTests).GetMethod(method, BindingFlags.Instance | BindingFlags.NonPublic)!.Invoke(fixture, arguments)!;
    }

    private static DifferentialDestination Destination(HostedSinkOperatorTests fixture)
    {
        return (DifferentialDestination)typeof(HostedSinkOperatorTests).GetField("_hostedDestination", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(fixture)!;
    }

    [Fact]
    public async Task MultiRowKeyAfterPriorKeysCannotPushChunkPastRowLimit()
    {
        using var fixture = new HostedSinkOperatorTests();
        await Invoke(fixture, "Start");

        var warmup = Enumerable.Range(0, 64).Select(x => ((long)x, (long)x, (long)x, "warm " + x)).ToArray();
        await Invoke(fixture, "Store", warmup);
        await Invoke(fixture, "Send", warmup.Select(x => (1, x.Item2, x.Item3, x.Item4)).ToArray());

        int maximumRows = 0;
        Destination(fixture).ObserveBatch = batch =>
        {
            if (batch.Columns[0] is not AlwaysNullColumn)
            {
                maximumRows = Math.Max(maximumRows, batch.Count);
            }
        };

        var keyA = Enumerable.Range(0, 1023).Select(x => (1000L + x, 500L, (long)x, "a " + x)).ToArray();
        var keyB = Enumerable.Range(0, 1024).Select(x => (3000L + x, 501L, (long)x, "b " + x)).ToArray();
        var all = keyA.Concat(keyB).ToArray();
        await Invoke(fixture, "Store", all);
        await Invoke(fixture, "Send", all.Select(x => (1, x.Item2, x.Item3, x.Item4)).ToArray());

        Assert.InRange(maximumRows, 1, 1024);
    }

    [Fact]
    public async Task MultiRowKeyAfterPriorKeysCannotPushChunkPastByteLimit()
    {
        using var fixture = new HostedSinkOperatorTests();
        await Invoke(fixture, "Start");

        string wide = new string('w', 65536);
        var warmup = Enumerable.Range(0, 64).Select(x => ((long)x, (long)x, (long)x, wide)).ToArray();
        await Invoke(fixture, "Store", warmup);
        await Invoke(fixture, "Send", warmup.Select(x => (1, x.Item2, x.Item3, x.Item4)).ToArray());

        long maximumBytes = 0;
        Destination(fixture).ObserveBatch = batch => maximumBytes = Math.Max(maximumBytes, batch.GetByteSize());

        var keyA = Enumerable.Range(0, 125).Select(x => (1000L + x, 500L, (long)x, wide)).ToArray();
        var keyB = Enumerable.Range(0, 126).Select(x => (3000L + x, 501L, (long)x, wide)).ToArray();
        var all = keyA.Concat(keyB).ToArray();
        await Invoke(fixture, "Store", all);
        await Invoke(fixture, "Send", all.Select(x => (1, x.Item2, x.Item3, x.Item4)).ToArray());

        Assert.InRange(maximumBytes, 1, 8 * 1024 * 1024 + 256 * 1024);
    }

    private sealed class CountingMemoryManager : IOperatorMemoryManager
    {
        private readonly RowHeapTests.CountingAllocator _inner = new();
        public long OutstandingBytes => _inner.OutstandingBytes;
        public long BytesFreedByFinalizers => _inner.BytesFreedByFinalizers;
        public IMemoryOwner<byte> Allocate(int size, int alignment) => _inner.Allocate(size, alignment);
        public IMemoryOwner<byte> Realloc(IMemoryOwner<byte> memory, int size, int alignment) => _inner.Realloc(memory, size, alignment);
        public void RegisterAllocationToMetrics(int size) => _inner.RegisterAllocationToMetrics(size);
        public void RegisterFreeToMetrics(int size) => _inner.RegisterFreeToMetrics(size);
    }

    private sealed class InspectDeleteSink : ColumnGroupedWriteOperator
    {
        private readonly bool _throwOnFirstRow;
        private readonly bool _expectView;

        public InspectDeleteSink(WriteRelation writeRelation, bool throwOnFirstRow = false, bool expectView = false)
            : base(ExecutionMode.OnCheckpoint, writeRelation, new ExecutionDataflowBlockOptions { BoundedCapacity = 100, MaxDegreeOfParallelism = 1 })
        {
            _throwOnFirstRow = throwOnFirstRow;
            _expectView = expectView;
        }

        public override string DisplayName => "inspect delete sink";

        protected override ValueTask<IReadOnlyList<int>> GetPrimaryKeyColumns() => ValueTask.FromResult<IReadOnlyList<int>>([0]);

        protected override void Checkpoint(long checkpointTime)
        {
        }

        protected override async Task UploadChanges(IAsyncEnumerable<ColumnWriteOperation> rows, Watermark watermark, bool isInitialData, CancellationToken cancellationToken)
        {
            await foreach (var row in rows.WithCancellation(cancellationToken))
            {
                _ = row.EventBatchData.Columns[0].GetTypeAt(row.Index, default);
                if (_throwOnFirstRow)
                {
                    if (_expectView)
                    {
                        Assert.IsType<ColumnWithOffset>(row.EventBatchData.Columns[2]);
                    }
                    throw new InvalidOperationException("consumer failed");
                }
            }
        }
    }

    [Fact]
    public async Task HostedSinkRefusesRowReferencesInKeptKeyOnDelete()
    {
        using var stream = await RowHeapTests.Open();
        var allocator = GlobalMemoryManager.Instance;
        var read = new ReadRelation
        {
            NamedTable = new NamedTable { Names = ["t"] },
            BaseSchema = new NamedStruct
            {
                Names = ["key", "payload"],
                Struct = new Struct { Types = [new AnyType(), new AnyType()] }
            },
            RowStore = new RowStoreDefinition { Name = "rows", StoredColumns = [1] }
        };
        var fetch = new FetchRequiredColumnsRelation
        {
            Input = read,
            Fetches = [new RowStoreFetch { StoreName = "rows", ReferenceColumn = 1, Columns = [0] }],
            Emit = [0, 2]
        };
        var write = new WriteRelation
        {
            Input = fetch,
            NamedObject = new NamedTable { Names = ["out"] },
            TableSchema = new NamedStruct { Names = ["key", "payload"] }
        };
        var layout = RowStoreHost.ReadLayout(read.RowStore!, 2, [0], [0, 1])!;
        using var host = await RowStoreHost.CreateAsync(stream.Client, "host-keys-del-ref", "rows", layout, new RowStoreOptions { Mode = RowStoreMode.Keyed }, allocator);
        using var source = new EventBatchData([Column.Create(allocator), Column.Create(allocator)]);
        source.Columns[0].Add(new Int64Value(1));
        source.Columns[1].Add(new StringValue("stored"));
        using var sourceWeights = new PrimitiveList<int>(allocator) { 1 };
        using var applied = await host.ApplyAsync(source, sourceWeights, null);
        ulong validRef = (ulong)applied.InsertReferences.GetValueAt(0, default).AsLong;
        var handle = new RowStoreHandle("rows") { HasHost = true };
        handle.Open(host);

        var sink = new InspectDeleteSink(write);
        Assert.True(((IRowStoreFetchHostVertex)sink).TryAttachFetch(fetch, [handle]));
        using var metrics = new StreamMetrics("del-ref-audit");
        TaskCompletionSource? checkpointDone = null;
        sink.Setup("del-ref-audit", "sink");
        sink.CreateBlock();
        sink.Link();
        ((IStreamEgressVertex)sink).SetCheckpointDoneFunction((_, _) => checkpointDone?.TrySetResult(), (_, _) => { });
        var handler = new VertexHandler(
            "del-ref-audit",
            "sink",
            (_, _) => { },
            (_, _, _) => Task.CompletedTask,
            metrics.GetOrCreateVertexMeter("sink", () => "sink"),
            stream.Manager.GetOrCreateClient("sink"),
            NullLoggerFactory.Instance,
            GlobalMemoryManager.Instance,
            (ex, _) =>
            {
                if (ex != null)
                {
                    checkpointDone?.TrySetException(ex);
                }
                return Task.CompletedTask;
            });
        await sink.Initialize("sink", 0, 0, handler, null);

        var cols = new IColumn[] { Column.Create(allocator), Column.Create(allocator) };
        cols[0].Add(new RowRefValue(999));
        cols[0].Add(new RowRefValue(999));
        cols[1].Add(new RowRefValue(validRef));
        cols[1].Add(new RowRefValue(validRef));
        var batch = new StreamEventBatch(new EventBatchWeighted(
            new PrimitiveList<int>(allocator) { 1, -1 },
            new PrimitiveList<uint>(allocator) { 0, 0 },
            new EventBatchData(cols)));
        batch.Data.Rent(1);

        var error = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await ((ITargetBlock<IStreamEvent>)sink).SendAsync(new StreamMessage<StreamEventBatch>(batch, 1));
            checkpointDone = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            await ((ITargetBlock<IStreamEvent>)sink).SendAsync(new Checkpoint(1, 2, 1));
            var finished = await Task.WhenAny(checkpointDone.Task, sink.Completion);
            if (finished == sink.Completion)
            {
                await sink.Completion;
            }
            await checkpointDone.Task;
        });
        Assert.Contains("received row references", error.Message);
        await sink.DisposeAsync();
    }

    [Theory]
    [InlineData(30)]
    [InlineData(90)]
    public async Task ThrowingUploadConsumerFreesAllChunkAndViewMemoryWithoutFinalizers(int firstKeyRows)
    {
        using var stream = await RowHeapTests.Open();
        var allocator = GlobalMemoryManager.Instance;
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
        var layout = RowStoreHost.ReadLayout(read.RowStore!, 3, [0, 1], [0, 1, 2])!;
        using var host = await RowStoreHost.CreateAsync(stream.Client, "host-throw", "rows", layout, new RowStoreOptions { Mode = RowStoreMode.Keyed }, allocator);
        using var source = new EventBatchData([Column.Create(allocator), Column.Create(allocator), Column.Create(allocator)]);
        using var sourceWeights = new PrimitiveList<int>(allocator);
        for (int i = 0; i < 120; i++)
        {
            source.Columns[0].Add(new Int64Value(i < firstKeyRows ? 7 : i));
            source.Columns[1].Add(new Int64Value(120 - i));
            source.Columns[2].Add(new StringValue("val-" + i));
            sourceWeights.Add(1);
        }
        using var applied = await host.ApplyAsync(source, sourceWeights, null);
        var handle = new RowStoreHandle("rows") { HasHost = true };
        handle.Open(host);

        var memory = new CountingMemoryManager();
        var sink = new InspectDeleteSink(write, throwOnFirstRow: true, expectView: firstKeyRows == 30);
        Assert.True(((IRowStoreFetchHostVertex)sink).TryAttachFetch(fetch, [handle]));
        using var metrics = new StreamMetrics("throw-audit");
        TaskCompletionSource? checkpointDone = null;
        sink.Setup("throw-audit", "sink");
        sink.CreateBlock();
        sink.Link();
        ((IStreamEgressVertex)sink).SetCheckpointDoneFunction((_, _) => checkpointDone?.TrySetResult(), (_, _) => { });
        var handler = new VertexHandler(
            "throw-audit",
            "sink",
            (_, _) => { },
            (_, _, _) => Task.CompletedTask,
            metrics.GetOrCreateVertexMeter("sink", () => "sink"),
            stream.Manager.GetOrCreateClient("sink"),
            NullLoggerFactory.Instance,
            memory,
            (ex, _) =>
            {
                if (ex != null)
                {
                    checkpointDone?.TrySetException(ex);
                }
                return Task.CompletedTask;
            });
        await sink.Initialize("sink", 0, 0, handler, null);

        var cols = new IColumn[] { Column.Create(allocator), Column.Create(allocator), Column.Create(allocator) };
        var weights = new PrimitiveList<int>(allocator);
        var iterations = new PrimitiveList<uint>(allocator);
        for (int i = 0; i < 120; i++)
        {
            cols[0].Add(new Int64Value(i < firstKeyRows ? 7 : i));
            cols[1].Add(new Int64Value(120 - i));
            cols[2].Add(new RowRefValue((ulong)applied.InsertReferences.GetValueAt(i, default).AsLong));
            weights.Add(1);
            iterations.Add(0);
        }
        var batch = new StreamEventBatch(new EventBatchWeighted(weights, iterations, new EventBatchData(cols)));
        batch.Data.Rent(1);

        await ((ITargetBlock<IStreamEvent>)sink).SendAsync(new StreamMessage<StreamEventBatch>(batch, 1));
        checkpointDone = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await ((ITargetBlock<IStreamEvent>)sink).SendAsync(new Checkpoint(1, 2, 1));
        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            var finished = await Task.WhenAny(checkpointDone.Task, sink.Completion);
            if (finished == sink.Completion)
            {
                await sink.Completion;
            }
            await checkpointDone.Task;
        });

        await sink.DisposeAsync();
        await stream.Manager.CheckpointAsync();
        stream.Dispose();

        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();

        Assert.Equal(0, memory.BytesFreedByFinalizers);
        Assert.Equal(0, memory.OutstandingBytes);
    }

    [Fact]
    public async Task StructAndLargeStructDuplicateKeysMatchPlainSink()
    {
        var headerA = StructHeader.Create("a", "b");
        var headerB = StructHeader.Create("x");
        var values = new List<IDataValue>
        {
            NullValue.Instance,
            new StructValue(headerA, new Int64Value(1), new StringValue("z")),
            new StructValue(headerA, new Int64Value(1), new StringValue("a")),
            new StructValue(headerA, NullValue.Instance, new StringValue("z")),
            new StructValue(headerB, new Int64Value(0)),
            new DecimalValue(1.50m),
            new DecimalValue(1.5m),
            new DoubleValue(-0.0),
            new DoubleValue(0.0)
        };
        for (int i = 0; i < 80; i++)
        {
            values.Add(new StructValue(headerA, new Int64Value(100 + i), new StringValue("s" + i)));
        }

        var test = new HostedSinkTypeTests();
        var start = typeof(HostedSinkTypeTests).GetMethod("Start", BindingFlags.Instance | BindingFlags.NonPublic)!;
        var store = typeof(HostedSinkTypeTests).GetMethod("Store", BindingFlags.Instance | BindingFlags.NonPublic)!;
        var send = typeof(HostedSinkTypeTests).GetMethod("Send", BindingFlags.Instance | BindingFlags.NonPublic)!;
        var hostedField = typeof(HostedSinkTypeTests).GetField("_hosted", BindingFlags.Instance | BindingFlags.NonPublic)!;
        var plainField = typeof(HostedSinkTypeTests).GetField("_plain", BindingFlags.Instance | BindingFlags.NonPublic)!;

        await (Task)start.Invoke(test, null)!;
        var rows = values.Select((v, i) => ((long)i, 7L, (long)(values.Count - i), v)).ToList();
        await (Task)store.Invoke(test, [rows])!;
        await (Task)send.Invoke(test, [1, rows])!;

        var hosted = (TypedGroupedSink)hostedField.GetValue(test)!;
        var plain = (TypedGroupedSink)plainField.GetValue(test)!;
        Assert.Equal(plain.Operations, hosted.Operations);
    }
}
