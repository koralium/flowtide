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
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Type;
using Microsoft.Extensions.Logging.Abstractions;
using System.Buffers;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Core.Tests.LateMaterialization;

public class HostedSinkChunkCleanupTests
{
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

    private sealed class CountingSink : ColumnGroupedWriteOperator
    {
        private readonly int _throwAt;

        public CountingSink(WriteRelation writeRelation, int throwAt)
            : base(ExecutionMode.OnCheckpoint, writeRelation, new ExecutionDataflowBlockOptions { BoundedCapacity = 100, MaxDegreeOfParallelism = 1 })
        {
            _throwAt = throwAt;
        }

        public int Seen { get; private set; }

        public override string DisplayName => "counting sink";

        protected override ValueTask<IReadOnlyList<int>> GetPrimaryKeyColumns() => ValueTask.FromResult<IReadOnlyList<int>>([0]);

        protected override void Checkpoint(long checkpointTime)
        {
        }

        protected override async Task UploadChanges(IAsyncEnumerable<ColumnWriteOperation> rows, Watermark watermark, bool isInitialData, CancellationToken cancellationToken)
        {
            await foreach (var row in rows.WithCancellation(cancellationToken))
            {
                _ = row.EventBatchData.Columns[2].GetValueAt(row.Index, default);
                if (Seen++ == _throwAt)
                {
                    throw new InvalidOperationException("consumer failed");
                }
            }
        }
    }

    [Theory]
    [InlineData(-1)]
    [InlineData(10)]
    public async Task ChunksOfSmallKeysAreFreedWithoutFinalizers(int throwAt)
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
        using var host = await RowStoreHost.CreateAsync(stream.Client, "host-small-keys", "rows", layout, new RowStoreOptions { Mode = RowStoreMode.Keyed }, allocator);
        using var source = new EventBatchData([Column.Create(allocator), Column.Create(allocator), Column.Create(allocator)]);
        using var sourceWeights = new PrimitiveList<int>(allocator);
        for (int i = 0; i < 120; i++)
        {
            source.Columns[0].Add(new Int64Value(i));
            source.Columns[1].Add(new Int64Value(120 - i));
            source.Columns[2].Add(new StringValue("val-" + i));
            sourceWeights.Add(1);
        }
        using var applied = await host.ApplyAsync(source, sourceWeights, null);
        var handle = new RowStoreHandle("rows") { HasHost = true };
        handle.Open(host);

        var memory = new CountingMemoryManager();
        var sink = new CountingSink(write, throwAt);
        Assert.True(((IRowStoreFetchHostVertex)sink).TryAttachFetch(fetch, [handle]));
        using var metrics = new StreamMetrics("small-keys");
        TaskCompletionSource? checkpointDone = null;
        sink.Setup("small-keys", "sink");
        sink.CreateBlock();
        sink.Link();
        ((IStreamEgressVertex)sink).SetCheckpointDoneFunction((_, _) => checkpointDone?.TrySetResult(), (_, _) => { });
        var handler = new VertexHandler(
            "small-keys",
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
            cols[0].Add(new Int64Value(i));
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
        var run = async () =>
        {
            var finished = await Task.WhenAny(checkpointDone.Task, sink.Completion).WaitAsync(TimeSpan.FromSeconds(60));
            if (finished == sink.Completion)
            {
                await sink.Completion;
            }
            await checkpointDone.Task;
        };
        if (throwAt < 0)
        {
            await run();
            Assert.Equal(120, sink.Seen);
        }
        else
        {
            await Assert.ThrowsAsync<InvalidOperationException>(run);
            Assert.Equal(throwAt + 1, sink.Seen);
        }

        await sink.DisposeAsync();
        await stream.Manager.CheckpointAsync();
        stream.Dispose();

        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();

        Assert.Equal(0, memory.BytesFreedByFinalizers);
        Assert.Equal(0, memory.OutstandingBytes);
    }
}
