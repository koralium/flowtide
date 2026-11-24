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
    public class FetchAllocationTests : OperatorTestBase
    {
        private sealed class Probe : FetchRequiredColumnsOperator
        {
            public Probe(FetchRequiredColumnsRelation relation, IReadOnlyList<RowStoreHandle> handles, ExecutionDataflowBlockOptions options)
                : base(relation, handles, options)
            {
            }

            public long Outstanding
            {
                get
                {
                    var manager = MemoryAllocator;
                    var type = manager.GetType();
                    long allocated = (long)type.GetField("_allocatedMemory", BindingFlags.NonPublic | BindingFlags.Instance)!.GetValue(manager)!;
                    long freed = (long)type.GetField("_freedMemory", BindingFlags.NonPublic | BindingFlags.Instance)!.GetValue(manager)!;
                    return allocated - freed;
                }
            }
        }

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

        private static StreamMessage<StreamEventBatch> Rows(params (long key, long number, string text)[] rows)
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
                weights.Add(1);
                iterations.Add(0);
            }
            var batch = new StreamEventBatch(new EventBatchWeighted(weights, iterations, new EventBatchData(columns)));
            batch.Data.Rent(1);
            return new StreamMessage<StreamEventBatch>(batch, 1);
        }

        private static StreamMessage<StreamEventBatch> References(params (long key, ulong reference)[] rows)
        {
            var allocator = GlobalMemoryManager.Instance;
            var columns = new IColumn[] { Column.Create(allocator), Column.Create(allocator) };
            var weights = new PrimitiveList<int>(allocator);
            var iterations = new PrimitiveList<uint>(allocator);
            foreach (var row in rows)
            {
                columns[0].Add(new Int64Value(row.key));
                columns[1].Add(new RowRefValue(row.reference));
                weights.Add(1);
                iterations.Add(0);
            }
            var batch = new StreamEventBatch(new EventBatchWeighted(weights, iterations, new EventBatchData(columns)));
            batch.Data.Rent(1);
            return new StreamMessage<StreamEventBatch>(batch, 1);
        }

        private async Task<(Probe fetch, BufferBlock<IStreamEvent> output, Dictionary<long, ulong> references)> Start(List<int> emit)
        {
            var relation = Relation();
            var handle = new RowStoreHandle("rows") { HasHost = true };
            var fetch = new Probe(new FetchRequiredColumnsRelation()
            {
                Input = relation,
                Fetches = new List<RowStoreFetch>() { new RowStoreFetch() { ReferenceColumn = 1, StoreName = "rows", Columns = new List<int>() { 0, 1 } } },
                Emit = emit
            }, [handle], Options());
            var output = new BufferBlock<IStreamEvent>();
            fetch.LinkTo(output);
            await InitializeOperator(fetch);

            var host = new ColumnNormalizationOperator(relation, FunctionsRegister, Options());
            host.AttachRowStoreHandle(handle);
            var hostOutput = new BufferBlock<IStreamEvent>();
            host.LinkTo(hostOutput);
            await InitializeOperator(host);

            await host.SendAsync(Rows((1, 10, "a"), (2, 20, "b")));
            await host.SendAsync(new Checkpoint(1, 2, 1));
            var references = new Dictionary<long, ulong>();
            while (true)
            {
                var received = await hostOutput.ReceiveAsync(TimeSpan.FromSeconds(30));
                if (received is ICheckpointEvent)
                {
                    await host.BeforeSaveCheckpoint();
                    await SaveCheckpoint();
                    break;
                }
                if (received is StreamMessage<StreamEventBatch> message)
                {
                    var data = message.Data.Data;
                    for (int i = 0; i < data.Count; i++)
                    {
                        references[data.EventBatchData.Columns[0].GetValueAt(i, default).AsLong] = (ulong)data.EventBatchData.Columns[1].GetValueAt(i, default).AsLong;
                    }
                    data.Return();
                }
            }
            return (fetch, output, references);
        }

        private static async Task ReceiveAndReturn(BufferBlock<IStreamEvent> output)
        {
            var message = (StreamMessage<StreamEventBatch>)await output.ReceiveAsync(TimeSpan.FromSeconds(30));
            message.Data.Data.Return();
        }

        [Fact]
        public async Task ANeverStoredReferenceFaultsTheFetchWithItsOwnError()
        {
            var (fetch, output, references) = await Start([0, 2, 3]);

            await fetch.SendAsync(References((1, references[1] + (1UL << 40))));

            var error = await Assert.ThrowsAsync<InvalidOperationException>(async () => await fetch.Completion.WaitAsync(TimeSpan.FromSeconds(30)));
            Assert.Contains("points at no page", error.Message);
            Assert.Equal(0, output.Count);
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task AFetchFreesWhatItAllocatedWhenEmitKeepsOnlyInputColumns(bool inOrder)
        {
            var (fetch, output, references) = await Start([0, 1]);
            long before = fetch.Outstanding;

            if (inOrder)
            {
                await fetch.SendAsync(References((1, references[1]), (2, references[2])));
            }
            else
            {
                await fetch.SendAsync(References((2, references[2]), (1, references[1])));
            }
            await ReceiveAndReturn(output);

            Assert.Equal(before, fetch.Outstanding);
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task AFetchFreesWhatItAllocatedWhenItsColumnsAreSent(bool inOrder)
        {
            var (fetch, output, references) = await Start([0, 2, 3]);
            long before = fetch.Outstanding;

            if (inOrder)
            {
                await fetch.SendAsync(References((1, references[1]), (2, references[2])));
            }
            else
            {
                await fetch.SendAsync(References((2, references[2]), (1, references[1])));
            }
            await ReceiveAndReturn(output);

            Assert.Equal(before, fetch.Outstanding);
        }
    }
}
