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

using FlowtideDotNet.Base.Engine.Internal;
using FlowtideDotNet.Base.Metrics;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.Operators.LateMaterialization;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Type;
using Microsoft.Extensions.Logging.Abstractions;
using System.Buffers;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Core.Tests.LateMaterialization;

public class FetchOffsetsOwnershipTests
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

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ReorderedFetchReleasesItsOffsetsEvenWhenEmitDropsAllFetchedColumns(bool dropFetchedColumn)
    {
        using var stream = await RowHeapTests.Open();
        var allocator = GlobalMemoryManager.Instance;
        var definition = new RowStoreDefinition { Name = "rows", StoredColumns = [1] };
        var relation = new ReadRelation
        {
            NamedTable = new NamedTable { Names = ["t"] },
            BaseSchema = new NamedStruct { Names = ["key", "payload"] },
            RowStore = definition
        };
        var layout = RowStoreHost.ReadLayout(definition, 2, [0], [0, 1])!;
        using var host = await RowStoreHost.CreateAsync(stream.Client, "keys", "rows", layout, new RowStoreOptions { Mode = RowStoreMode.Keyed }, allocator);
        using var source = new EventBatchData(new IColumn[] { Column.Create(allocator), Column.Create(allocator) });
        source.Columns[0].Add(new Int64Value(1));
        source.Columns[0].Add(new Int64Value(2));
        source.Columns[1].Add(new StringValue("first"));
        source.Columns[1].Add(new StringValue("second"));
        using var sourceWeights = new PrimitiveList<int>(allocator) { 1, 1 };
        using var applied = await host.ApplyAsync(source, sourceWeights, null);
        var first = (ulong)applied.InsertReferences.GetValueAt(0, default).AsLong;
        var second = (ulong)applied.InsertReferences.GetValueAt(1, default).AsLong;
        Assert.True(first < second);
        var handle = new RowStoreHandle("rows") { HasHost = true };
        handle.Open(host);
        var fetch = new FetchRequiredColumnsOperator(new FetchRequiredColumnsRelation
        {
            Input = relation,
            Fetches = [new RowStoreFetch { StoreName = "rows", ReferenceColumn = 1, Columns = [0] }],
            Emit = dropFetchedColumn ? [0] : [0, 2]
        }, [handle], new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = 1 });
        var memory = new CountingMemoryManager();
        using var metrics = new StreamMetrics("ownership-review");
        fetch.Setup("ownership-review", "fetch");
        fetch.CreateBlock();
        fetch.LinkTo(new BufferBlock<FlowtideDotNet.Base.IStreamEvent>());
        fetch.Link();
        var handler = new VertexHandler("ownership-review", "fetch", (_, _) => { }, (_, _, _) => Task.CompletedTask,
            metrics.GetOrCreateVertexMeter("fetch", () => "fetch"), stream.Client, NullLoggerFactory.Instance, memory, (_, _) => Task.CompletedTask);
        await fetch.Initialize("fetch", 0, 0, handler, null);
        var columns = new IColumn[] { Column.Create(allocator), Column.Create(allocator) };
        columns[0].Add(new Int64Value(2));
        columns[0].Add(new Int64Value(1));
        columns[1].Add(new RowRefValue(second));
        columns[1].Add(new RowRefValue(first));
        var input = new StreamEventBatch(new EventBatchWeighted(new PrimitiveList<int>(allocator) { 1, 1 },
            new PrimitiveList<uint>(allocator) { 0, 0 }, new EventBatchData(columns)));
        input.Rent(1);
        try
        {
            await foreach (var output in fetch.OnRecieve(input, 0))
            {
                output.Rent(1);
                Assert.Equal(2, output.Data.Count);
                if (!dropFetchedColumn)
                {
                    Assert.Equal("second", output.Data.EventBatchData.Columns[1].GetValueAt(0, default).AsString.ToString());
                    Assert.Equal("first", output.Data.EventBatchData.Columns[1].GetValueAt(1, default).AsString.ToString());
                }
                output.Return();
            }
        }
        finally
        {
            input.Return();
            await fetch.DisposeAsync();
        }
        var finalizerBytes = memory.BytesFreedByFinalizers;
        Assert.Equal(0, memory.OutstandingBytes + finalizerBytes);
    }
    [Fact]
    public async Task TwoStoredPositionsOfOneColumnAreFetchedFromItsOneHeapColumn()
    {
        using var stream = await RowHeapTests.Open();
        var allocator = GlobalMemoryManager.Instance;
        var definition = new RowStoreDefinition { Name = "rows", StoredColumns = [1, 2] };
        var relation = new ReadRelation
        {
            NamedTable = new NamedTable { Names = ["t"] },
            BaseSchema = new NamedStruct { Names = ["key", "number", "text"] },
            Emit = [0, 2, 2],
            RowStore = definition
        };
        var layout = RowStoreHost.ReadLayout(definition, 3, [0], [0, 2, 2])!;
        Assert.Equal([0, 0], layout.StoredColumnOfPosition);
        using var host = await RowStoreHost.CreateAsync(stream.Client, "keys", "rows", layout, new RowStoreOptions { Mode = RowStoreMode.Keyed }, allocator);
        using var source = new EventBatchData(new IColumn[] { Column.Create(allocator), Column.Create(allocator), Column.Create(allocator) });
        source.Columns[0].Add(new Int64Value(1));
        source.Columns[1].Add(new Int64Value(10));
        source.Columns[2].Add(new StringValue("first"));
        using var sourceWeights = new PrimitiveList<int>(allocator) { 1 };
        using var applied = await host.ApplyAsync(source, sourceWeights, null);
        var reference = (ulong)applied.InsertReferences.GetValueAt(0, default).AsLong;
        var handle = new RowStoreHandle("rows") { HasHost = true };
        handle.Open(host);
        var fetch = new FetchRequiredColumnsOperator(new FetchRequiredColumnsRelation
        {
            Input = relation,
            Fetches = [new RowStoreFetch { StoreName = "rows", ReferenceColumn = 1, Columns = [1, 0] }]
        }, [handle], new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = 1 });
        using var metrics = new StreamMetrics("stored-positions");
        fetch.Setup("stored-positions", "fetch");
        fetch.CreateBlock();
        fetch.LinkTo(new BufferBlock<FlowtideDotNet.Base.IStreamEvent>());
        fetch.Link();
        var handler = new VertexHandler("stored-positions", "fetch", (_, _) => { }, (_, _, _) => Task.CompletedTask,
            metrics.GetOrCreateVertexMeter("fetch", () => "fetch"), stream.Client, NullLoggerFactory.Instance, new CountingMemoryManager(), (_, _) => Task.CompletedTask);
        await fetch.Initialize("fetch", 0, 0, handler, null);
        var columns = new IColumn[] { Column.Create(allocator), Column.Create(allocator) };
        columns[0].Add(new Int64Value(1));
        columns[1].Add(new RowRefValue(reference));
        var input = new StreamEventBatch(new EventBatchWeighted(new PrimitiveList<int>(allocator) { 1 },
            new PrimitiveList<uint>(allocator) { 0 }, new EventBatchData(columns)));
        input.Rent(1);
        var texts = new List<string>();
        try
        {
            await foreach (var output in fetch.OnRecieve(input, 0))
            {
                output.Rent(1);
                texts.Add(output.Data.EventBatchData.Columns[2].GetValueAt(0, default).AsString.ToString());
                texts.Add(output.Data.EventBatchData.Columns[3].GetValueAt(0, default).AsString.ToString());
                output.Return();
            }
        }
        finally
        {
            input.Return();
            await fetch.DisposeAsync();
        }
        Assert.Equal(["first", "first"], texts);
    }
}

