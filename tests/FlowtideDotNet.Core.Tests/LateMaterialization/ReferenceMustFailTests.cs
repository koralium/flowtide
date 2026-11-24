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
using System.Reflection;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Core.Tests.LateMaterialization
{
    // The arms of the plan that must fail
    public class ReferenceMustFailTests : OperatorTestBase
    {
        protected override StateManagerOptions CreateStateManagerOptions()
        {
            return new StateManagerOptions()
            {
                PersistentStorage = new ReservoirPersistentStorage(new ReservoirStorageOptions() { FileProvider = new MemoryFileProvider() })
            };
        }

        private static ExecutionDataflowBlockOptions Options()
        {
            return new ExecutionDataflowBlockOptions() { BoundedCapacity = 100, MaxDegreeOfParallelism = 1 };
        }

        private static NormalizationRelation Relation()
        {
            return new NormalizationRelation()
            {
                KeyIndex = new List<int>() { 0 },
                RowStore = new RowStoreDefinition() { Name = "rows", StoredColumns = new List<int>() { 1, 2 } },
                Input = new ReadRelation()
                {
                    NamedTable = new NamedTable() { Names = new List<string>() { "t" } },
                    BaseSchema = new NamedStruct()
                    {
                        Names = new List<string>() { "k", "number", "text" },
                        Struct = new Struct() { Types = new List<SubstraitBaseType>() { new AnyType(), new AnyType(), new AnyType() } }
                    }
                }
            };
        }

        private static StreamMessage<StreamEventBatch> Rows(int weight, params (long key, long number, string text)[] rows)
        {
            var allocator = GlobalMemoryManager.Instance;
            var columns = new IColumn[] { Column.Create(allocator), Column.Create(allocator), Column.Create(allocator) };
            var weights = new PrimitiveList<int>(allocator);
            var iterations = new PrimitiveList<uint>(allocator);
            foreach (var row in rows)
            {
                columns[0].Add(new Int64Value(row.key));
                columns[1].Add(new Int64Value(row.number));
                columns[2].Add(new StringValue(row.text));
                weights.Add(weight);
                iterations.Add(0);
            }
            var batch = new StreamEventBatch(new EventBatchWeighted(weights, iterations, new EventBatchData(columns)));
            batch.Data.Rent(1);
            return new StreamMessage<StreamEventBatch>(batch, 1);
        }

        // What a carrier would hand the fetch: keys, references and weights
        private sealed class Held
        {
            public List<(long key, ulong reference, int weight)> Rows { get; } = new List<(long key, ulong reference, int weight)>();

            public StreamMessage<StreamEventBatch> Message()
            {
                var allocator = GlobalMemoryManager.Instance;
                var columns = new IColumn[] { Column.Create(allocator), Column.Create(allocator) };
                var weights = new PrimitiveList<int>(allocator);
                var iterations = new PrimitiveList<uint>(allocator);
                foreach (var row in Rows)
                {
                    columns[0].Add(new Int64Value(row.key));
                    columns[1].Add(new RowRefValue(row.reference));
                    weights.Add(row.weight);
                    iterations.Add(0);
                }
                var batch = new StreamEventBatch(new EventBatchWeighted(weights, iterations, new EventBatchData(columns)));
                batch.Data.Rent(1);
                return new StreamMessage<StreamEventBatch>(batch, 1);
            }
        }

        // Rows the host sends until its checkpoint comes out
        private static async Task<Held> UntilCheckpoint(BufferBlock<IStreamEvent> hostOutput)
        {
            var held = new Held();
            while (true)
            {
                var received = await hostOutput.ReceiveAsync(TimeSpan.FromSeconds(30));
                if (received is ICheckpointEvent)
                {
                    return held;
                }
                if (received is StreamMessage<StreamEventBatch> message)
                {
                    var data = message.Data.Data;
                    for (int i = 0; i < data.Count; i++)
                    {
                        held.Rows.Add((data.EventBatchData.Columns[0].GetValueAt(i, default).AsLong, (ulong)data.EventBatchData.Columns[1].GetValueAt(i, default).AsLong, data.Weights.Get(i)));
                    }
                    data.Return();
                }
            }
        }

        private static async Task<List<string>> Fetched(BufferBlock<IStreamEvent> output)
        {
            var message = (StreamMessage<StreamEventBatch>)await output.ReceiveAsync(TimeSpan.FromSeconds(30));
            var data = message.Data.Data;
            var rows = new List<string>();
            for (int i = 0; i < data.Count; i++)
            {
                var columns = data.EventBatchData.Columns;
                rows.Add($"{data.Weights.Get(i)}: {columns[0].GetValueAt(i, default).AsLong}|{columns[1].GetValueAt(i, default).AsLong}|{columns[2].GetValueAt(i, default).AsString}");
            }
            data.Return();
            rows.Sort(StringComparer.Ordinal);
            return rows;
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task ARetractionHeldPastItsCheckpointFaultsTheFetch(bool heldPastTheCheckpoint)
        {
            var relation = Relation();
            var handle = new RowStoreHandle("rows") { HasHost = true };
            var fetch = new FetchRequiredColumnsOperator(new FetchRequiredColumnsRelation()
            {
                Input = relation,
                Fetches = new List<RowStoreFetch>() { new RowStoreFetch() { ReferenceColumn = 1, StoreName = "rows", Columns = new List<int>() { 0, 1 } } },
                Emit = [0, 2, 3]
            }, [handle], Options());
            var output = new BufferBlock<IStreamEvent>();
            fetch.LinkTo(output);
            await InitializeOperator(fetch);
            var host = new ColumnNormalizationOperator(relation, FunctionsRegister, Options());
            host.AttachRowStoreHandle(handle);
            var hostOutput = new BufferBlock<IStreamEvent>();
            host.LinkTo(hostOutput);
            await InitializeOperator(host);

            await host.SendAsync(Rows(1, (1, 10, "a"), (2, 20, "b")));
            await host.SendAsync(new Checkpoint(1, 2, 1));
            var inserted = await UntilCheckpoint(hostOutput);
            await fetch.SendAsync(inserted.Message());
            Assert.Equal(["1: 1|10|a", "1: 2|20|b"], await Fetched(output));
            await host.BeforeSaveCheckpoint();
            await SaveCheckpoint();

            await host.SendAsync(Rows(-1, (1, 10, "a"), (2, 20, "b")));
            await host.SendAsync(new Checkpoint(2, 3, 1));
            var retracted = await UntilCheckpoint(hostOutput);
            Assert.Equal(inserted.Rows.Select(x => (x.key, x.reference, -1)).Order(), retracted.Rows.Order());

            if (!heldPastTheCheckpoint)
            {
                await fetch.SendAsync(retracted.Message());
                Assert.Equal(["-1: 1|10|a", "-1: 2|20|b"], await Fetched(output));
                await host.BeforeSaveCheckpoint();
                await SaveCheckpoint();
                return;
            }
            await host.BeforeSaveCheckpoint();
            await SaveCheckpoint();
            await fetch.SendAsync(retracted.Message());

            var error = await Assert.ThrowsAsync<InvalidOperationException>(async () => await fetch.Completion.WaitAsync(TimeSpan.FromSeconds(30)));
            Assert.Contains("points at no page", error.Message);
            Assert.Equal(0, output.Count);
        }

        [Fact]
        public async Task WithOneRowAPageALateReferenceFaultsWhileAnotherRowLives()
        {
            var relation = Relation();
            var handle = new RowStoreHandle("rows") { HasHost = true };
            var fetch = new FetchRequiredColumnsOperator(new FetchRequiredColumnsRelation()
            {
                Input = relation,
                Fetches = new List<RowStoreFetch>() { new RowStoreFetch() { ReferenceColumn = 1, StoreName = "rows", Columns = new List<int>() { 0, 1 } } },
                Emit = [0, 2, 3]
            }, [handle], Options());
            var output = new BufferBlock<IStreamEvent>();
            fetch.LinkTo(output);
            await InitializeOperator(fetch);
            var host = new ColumnNormalizationOperator(relation, FunctionsRegister, Options());
            host.RowHeapOptions = new RowHeapOptions() { PageTargetBytes = 1 };
            host.AttachRowStoreHandle(handle);
            var hostOutput = new BufferBlock<IStreamEvent>();
            host.LinkTo(hostOutput);
            await InitializeOperator(host);
            await host.SendAsync(Rows(1, (1, 10, "a"), (2, 20, "b")));
            await host.SendAsync(new Checkpoint(1, 2, 1));
            await UntilCheckpoint(hostOutput);
            await host.BeforeSaveCheckpoint();
            await SaveCheckpoint();
            await host.SendAsync(Rows(-1, (1, 10, "a")));
            await host.SendAsync(new Checkpoint(2, 3, 1));
            var retracted = await UntilCheckpoint(hostOutput);
            Assert.Single(retracted.Rows);
            await host.BeforeSaveCheckpoint();
            await SaveCheckpoint();

            await fetch.SendAsync(retracted.Message());

            var error = await Assert.ThrowsAsync<InvalidOperationException>(async () => await fetch.Completion.WaitAsync(TimeSpan.FromSeconds(30)));
            Assert.Contains("points at no page", error.Message);
            Assert.Equal(0, output.Count);
        }

        [Fact]
        public async Task ARowFunctionOverABatchWithReferencesFaultsTheFilter()
        {
            FunctionsRegister.RegisterScalarFunctionWithExpression("extension:test:rows", "is_odd", x => FlexBuffers.FlxValue.FromBytes(FlexBuffers.FlexBuffer.SingleValue(x.AsLong % 2 == 1)));
            var filter = new Core.Operators.Filter.ColumnFilterOperator(new FilterRelation()
            {
                Input = Relation(),
                Condition = new Substrait.Expressions.ScalarFunction()
                {
                    ExtensionUrn = "extension:test:rows",
                    ExtensionName = "is_odd",
                    Arguments = [new Substrait.Expressions.DirectFieldReference() { ReferenceSegment = new Substrait.Expressions.StructReferenceSegment() { Field = 0 } }]
                }
            }, FunctionsRegister, Options());
            var output = new BufferBlock<IStreamEvent>();
            filter.LinkTo(output);
            await InitializeOperator(filter);
            var held = new Held();
            held.Rows.Add((1, 1UL << 20, 1));

            await filter.SendAsync(held.Message());

            var error = await Assert.ThrowsAsync<NotSupportedException>(async () => await filter.Completion.WaitAsync(TimeSpan.FromSeconds(30)));
            Assert.Contains("converted to a row event", error.Message);
            Assert.Equal(0, output.Count);
        }
    }
}
