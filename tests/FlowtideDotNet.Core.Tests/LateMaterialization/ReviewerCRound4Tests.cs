using FlowtideDotNet.Base;
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
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Core.Tests.LateMaterialization;

public class ReviewerCRound4Tests : OperatorTestBase
{
    protected override StateManagerOptions CreateStateManagerOptions() => new()
    {
        PersistentStorage = new ReservoirPersistentStorage(new ReservoirStorageOptions { FileProvider = new MemoryFileProvider() })
    };

    private ColumnNormalizationOperator Operator()
    {
        var relation = new NormalizationRelation
        {
            KeyIndex = [0],
            Input = new ReadRelation
            {
                NamedTable = new NamedTable { Names = ["t"] },
                BaseSchema = new NamedStruct
                {
                    Names = ["k", "kept", "text"],
                    Struct = new Struct { Types = [new AnyType(), new AnyType(), new AnyType()] }
                }
            }
        };
        relation.RowStore = new RowStoreDefinition() { Name = "rows", StoredColumns = [2] };
        return new ColumnNormalizationOperator(relation, FunctionsRegister, new ExecutionDataflowBlockOptions { BoundedCapacity = 100, MaxDegreeOfParallelism = 1 });
    }

    private static StreamEventBatch Batch(long key, long kept, string text)
    {
        var allocator = GlobalMemoryManager.Instance;
        var columns = new IColumn[] { Column.Create(allocator), Column.Create(allocator), Column.Create(allocator) };
        columns[0].Add(new Int64Value(key));
        columns[1].Add(new Int64Value(kept));
        columns[2].Add(new StringValue(text));
        var weights = new PrimitiveList<int>(allocator) { 1 };
        var iterations = new PrimitiveList<uint>(allocator) { 0 };
        var batch = new StreamEventBatch(new EventBatchWeighted(weights, iterations, new EventBatchData(columns)));
        batch.Data.Rent(1);
        return batch;
    }

    private static void Free(StreamEventBatch batch)
    {
        batch.Data.Rent(1);
        batch.Data.Return();
    }

    [Fact]
    public async Task AnAbandonedReceiveWithADeleteLeftUnsentClosesTheHost()
    {
        var op = Operator();
        await InitializeOperator(op);
        try
        {
            var first = Batch(1, 10, "a");
            await foreach (var sent in op.OnRecieve(first, 1))
            {
                Free(sent);
            }
            first.Data.Return();

            var update = Batch(1, 10, "b");
            var enumerator = op.OnRecieve(update, 1).GetAsyncEnumerator();
            Assert.True(await enumerator.MoveNextAsync());
            Free(enumerator.Current);
            await enumerator.DisposeAsync();
            update.Data.Return();

            var error = await Assert.ThrowsAsync<InvalidOperationException>(() => op.OnCheckpoint());
            Assert.Equal("Row store 'rows' failed in an earlier call, it is opened anew from the last checkpoint.", error.Message);
        }
        finally
        {
            await op.DisposeAsync();
        }
    }
}
