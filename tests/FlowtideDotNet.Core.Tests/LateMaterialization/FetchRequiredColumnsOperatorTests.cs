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
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Core.Tests.LateMaterialization
{
    public class FetchRequiredColumnsOperatorTests : OperatorTestBase
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

        // Key, number and text, the last two stored
        private static NormalizationRelation Relation(string store)
        {
            return new NormalizationRelation()
            {
                KeyIndex = new List<int>() { 0 },
                RowStore = new RowStoreDefinition() { Name = store, StoredColumns = new List<int>() { 1, 2 } },
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

        // Stores the rows in a hosted normalization, gives each key's reference
        private async Task<Dictionary<long, ulong>> Store(ColumnNormalizationOperator host, BufferBlock<IStreamEvent> hostOutput, long checkpoint, params (long key, long number, string text)[] rows)
        {
            await host.SendAsync(Rows(rows));
            await host.SendAsync(new Checkpoint(checkpoint, checkpoint + 1, 1));
            var references = new Dictionary<long, ulong>();
            while (true)
            {
                var received = await hostOutput.ReceiveAsync(TimeSpan.FromSeconds(30));
                if (received is ICheckpointEvent)
                {
                    await host.BeforeSaveCheckpoint();
                    await SaveCheckpoint();
                    return references;
                }
                if (received is StreamMessage<StreamEventBatch> message)
                {
                    var data = message.Data.Data;
                    for (int i = 0; i < data.Count; i++)
                    {
                        if (data.Weights.Get(i) > 0)
                        {
                            references[data.EventBatchData.Columns[0].GetValueAt(i, default).AsLong] = (ulong)data.EventBatchData.Columns[1].GetValueAt(i, default).AsLong;
                        }
                    }
                    data.Return();
                }
            }
        }

        // Keys and references in order, null allowed
        private static StreamMessage<StreamEventBatch> References(params (long key, ulong? reference)[] rows)
        {
            var allocator = GlobalMemoryManager.Instance;
            var columns = new IColumn[] { Column.Create(allocator), Column.Create(allocator) };
            var weights = new PrimitiveList<int>(allocator);
            var iterations = new PrimitiveList<uint>(allocator);
            foreach (var row in rows)
            {
                columns[0].Add(new Int64Value(row.key));
                if (row.reference == null)
                {
                    columns[1].Add(NullValue.Instance);
                }
                else
                {
                    columns[1].Add(new RowRefValue(row.reference.Value));
                }
                weights.Add(1);
                iterations.Add(0);
            }
            var batch = new StreamEventBatch(new EventBatchWeighted(weights, iterations, new EventBatchData(columns)));
            batch.Data.Rent(1);
            return new StreamMessage<StreamEventBatch>(batch, 1);
        }

        private static async Task<List<string>> Receive(BufferBlock<IStreamEvent> output)
        {
            var message = (StreamMessage<StreamEventBatch>)await output.ReceiveAsync(TimeSpan.FromSeconds(30));
            var data = message.Data.Data;
            var rows = new List<string>();
            for (int i = 0; i < data.Count; i++)
            {
                var parts = new List<string>();
                for (int c = 0; c < data.EventBatchData.Columns.Count; c++)
                {
                    var value = data.EventBatchData.Columns[c].GetValueAt(i, default);
                    parts.Add(value.Type == ArrowTypeId.Null ? "null" : value.Type == ArrowTypeId.Int64 ? value.AsLong.ToString() : value.Type == ArrowTypeId.RowRef ? "ref" : value.AsString.ToString());
                }
                rows.Add(string.Join("|", parts));
            }
            data.Return();
            return rows;
        }

        private async Task<(ColumnNormalizationOperator host, BufferBlock<IStreamEvent> hostOutput, FetchRequiredColumnsOperator fetch, BufferBlock<IStreamEvent> output)> Start(List<int> columns, List<int>? emit)
        {
            var relation = Relation("rows");
            var handle = new RowStoreHandle("rows") { HasHost = true };
            var fetch = new FetchRequiredColumnsOperator(new FetchRequiredColumnsRelation()
            {
                Input = relation,
                Fetches = new List<RowStoreFetch>() { new RowStoreFetch() { ReferenceColumn = 1, StoreName = "rows", Columns = columns } },
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
            return (host, hostOutput, fetch, output);
        }

        [Fact]
        public async Task FetchedColumnsFollowTheInputRowsWhateverTheirOrder()
        {
            var (host, hostOutput, fetch, output) = await Start([0, 1], [0, 2, 3]);
            var references = await Store(host, hostOutput, 1, (1, 10, "a"), (2, 20, "b"), (3, 30, "c"), (4, 40, "d"));

            await fetch.SendAsync(References((3, references[3]), (1, references[1]), (4, references[4]), (2, references[2])));

            Assert.Equal(["3|30|c", "1|10|a", "4|40|d", "2|20|b"], await Receive(output));
        }

        [Fact]
        public async Task ReferencesInStoredOrderNeedNoReordering()
        {
            var (host, hostOutput, fetch, output) = await Start([1], null);
            var references = await Store(host, hostOutput, 1, (1, 10, "a"), (2, 20, "b"));

            await fetch.SendAsync(References((1, references[1]), (2, references[2])));

            Assert.Equal(["1|ref|a", "2|ref|b"], await Receive(output));
        }

        [Fact]
        public async Task ManyReferencesOutOfOrderAreFetchedInInputOrder()
        {
            var (host, hostOutput, fetch, output) = await Start([1, 0], [0, 2, 3]);
            var rows = Enumerable.Range(1, 600).Select(i => ((long)i, (long)i * 10, "text" + i)).ToArray();
            var references = await Store(host, hostOutput, 1, rows);
            var random = new Random(123);
            var order = rows.Select(x => x.Item1).OrderBy(x => random.Next()).ToArray();

            await fetch.SendAsync(References(order.Select(key => (key, (ulong?)references[key])).ToArray()));

            Assert.Equal(order.Select(key => $"{key}|text{key}|{key * 10}"), await Receive(output));
        }

        [Fact]
        public async Task AFetchThatGetsRowsBeforeItsStoreIsOpenWaitsForIt()
        {
            var (host, hostOutput, fetch, output) = await Start([0, 1], [0, 2, 3]);
            var references = await Store(host, hostOutput, 1, (1, 10, "a"));
            var storeHost = (RowStoreHost)typeof(ColumnNormalizationOperator).GetField("_host", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)!.GetValue(host)!;
            var handle = (RowStoreHandle)typeof(ColumnNormalizationOperator).GetField("_rowStoreHandle", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)!.GetValue(host)!;

            await handle.CloseAsync();
            await fetch.SendAsync(References((1, references[1])));
            await Assert.ThrowsAsync<TimeoutException>(() => output.ReceiveAsync(TimeSpan.FromMilliseconds(300)));

            handle.Open(storeHost);
            Assert.Equal(["1|10|a"], await Receive(output));
        }

        [Fact]
        public async Task AHostDoesNotCloseItsStoreUnderAFetchThatIsReading()
        {
            var (host, hostOutput, fetch, output) = await Start([0, 1], [0, 2, 3]);
            var references = await Store(host, hostOutput, 1, (1, 10, "a"), (2, 20, "b"));
            var flags = System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic;
            var storeHost = (RowStoreHost)typeof(ColumnNormalizationOperator).GetField("_host", flags)!.GetValue(host)!;
            var heap = (RowHeap)typeof(RowStore).GetField("_heap", flags)!.GetValue(storeHost.Store)!;
            var reading = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            using var release = new ManualResetEventSlim();
            heap.Options.PagesResolvedHookForTests = () =>
            {
                reading.TrySetResult();
                release.Wait(TimeSpan.FromSeconds(30));
            };

            await fetch.SendAsync(References((2, references[2]), (1, references[1])));
            await reading.Task.WaitAsync(TimeSpan.FromSeconds(30));
            var closing = host.DisposeAsync().AsTask();
            await Task.Delay(300);
            bool closedUnderTheRead = closing.IsCompleted;
            heap.Options.PagesResolvedHookForTests = null;
            release.Set();

            Assert.Equal(["2|20|b", "1|10|a"], await Receive(output));
            await closing.WaitAsync(TimeSpan.FromSeconds(30));
            Assert.False(closedUnderTheRead);
        }

        [Fact]
        public async Task ANullReferenceGivesNulls()
        {
            var (host, hostOutput, fetch, output) = await Start([0, 1], [0, 2, 3]);
            var references = await Store(host, hostOutput, 1, (1, 10, "a"), (2, 20, "b"));

            await fetch.SendAsync(References((2, references[2]), (7, null), (1, references[1]), (8, null)));

            Assert.Equal(["2|20|b", "7|null|null", "1|10|a", "8|null|null"], await Receive(output));
        }

        [Fact]
        public async Task AFetchedColumnCanBeSentTwice()
        {
            var (host, hostOutput, fetch, output) = await Start([1], [2, 0, 2]);
            var references = await Store(host, hostOutput, 1, (1, 10, "a"), (2, 20, "b"));

            await fetch.SendAsync(References((2, references[2]), (1, references[1])));

            Assert.Equal(["b|2|b", "a|1|a"], await Receive(output));
        }

        [Fact]
        public async Task AnEmptyBatchPassesThrough()
        {
            var (host, hostOutput, fetch, output) = await Start([0, 1], [0, 2, 3]);
            await Store(host, hostOutput, 1, (1, 10, "a"));

            await fetch.SendAsync(References());

            Assert.Empty(await Receive(output));
        }

        [Fact]
        public async Task AFetchReadsTheStoreAgainAfterItsHostWasOpenedAnew()
        {
            var (host, hostOutput, fetch, output) = await Start([0, 1], [0, 2, 3]);
            var references = await Store(host, hostOutput, 1, (1, 10, "a"), (2, 20, "b"));
            await fetch.SendAsync(References((2, references[2])));
            Assert.Equal(["2|20|b"], await Receive(output));

            await ReinitializeOperator(host);

            await fetch.SendAsync(References((1, references[1]), (2, references[2])));
            Assert.Equal(["1|10|a", "2|20|b"], await Receive(output));
        }

        [Fact]
        public async Task TwoStoresAreFetchedInOnePass()
        {
            var first = Relation("first");
            var firstHandle = new RowStoreHandle("first") { HasHost = true };
            var secondHandle = new RowStoreHandle("second") { HasHost = true };
            var fetch = new FetchRequiredColumnsOperator(new FetchRequiredColumnsRelation()
            {
                Input = new ReadRelation()
                {
                    NamedTable = new NamedTable() { Names = new List<string>() { "joined" } },
                    BaseSchema = new NamedStruct() { Names = new List<string>() { "k", "firstReference", "secondReference" } }
                },
                Fetches = new List<RowStoreFetch>()
                {
                    new RowStoreFetch() { ReferenceColumn = 1, StoreName = "first", Columns = new List<int>() { 1 } },
                    new RowStoreFetch() { ReferenceColumn = 2, StoreName = "second", Columns = new List<int>() { 0, 1 } }
                },
                Emit = new List<int>() { 0, 3, 4, 5 }
            }, [firstHandle, secondHandle], Options());
            var output = new BufferBlock<IStreamEvent>();
            fetch.LinkTo(output);
            await InitializeOperator(fetch);

            // A second host needs its own state name
            var firstHost = new ColumnNormalizationOperator(first, FunctionsRegister, Options());
            firstHost.AttachRowStoreHandle(firstHandle);
            var firstOutput = new BufferBlock<IStreamEvent>();
            firstHost.LinkTo(firstOutput);
            await InitializeOperator(firstHost);
            var firstReferences = await Store(firstHost, firstOutput, 1, (1, 10, "a"), (2, 20, "b"));

            var secondHost = new ColumnNormalizationOperator(Relation("second"), FunctionsRegister, Options());
            secondHost.AttachRowStoreHandle(secondHandle);
            var secondOutput = new BufferBlock<IStreamEvent>();
            secondHost.LinkTo(secondOutput);
            await InitializeOperator(secondHost);
            var secondReferences = await Store(secondHost, secondOutput, 1, (1, 100, "x"), (2, 200, "y"));

            var allocator = GlobalMemoryManager.Instance;
            var columns = new IColumn[] { Column.Create(allocator), Column.Create(allocator), Column.Create(allocator) };
            var weights = new PrimitiveList<int>(allocator);
            var iterations = new PrimitiveList<uint>(allocator);
            foreach (var (key, firstKey, secondKey) in new (long, long, long?)[] { (1, 2, 1), (2, 1, 2), (3, 1, null) })
            {
                columns[0].Add(new Int64Value(key));
                columns[1].Add(new RowRefValue(firstReferences[firstKey]));
                if (secondKey == null)
                {
                    columns[2].Add(NullValue.Instance);
                }
                else
                {
                    columns[2].Add(new RowRefValue(secondReferences[secondKey.Value]));
                }
                weights.Add(1);
                iterations.Add(0);
            }
            var batch = new StreamEventBatch(new EventBatchWeighted(weights, iterations, new EventBatchData(columns)));
            batch.Data.Rent(1);
            await fetch.SendAsync(new StreamMessage<StreamEventBatch>(batch, 1));

            Assert.Equal(["1|b|100|x", "2|a|200|y", "3|a|null|null"], await Receive(output));
        }

        [Fact]
        public async Task AFetchWaitsForItsStoreToOpen()
        {
            var handle = new RowStoreHandle("rows");
            using var cancel = new CancellationTokenSource();
            var waiting = handle.GetHostAsync(cancel.Token);
            Assert.False(waiting.IsCompleted);
            Assert.Equal(0, handle.Generation);

            var relation = Relation("rows");
            var host = new ColumnNormalizationOperator(relation, FunctionsRegister, Options());
            host.AttachRowStoreHandle(handle);
            host.LinkTo(new BufferBlock<IStreamEvent>());
            await InitializeOperator(host);

            Assert.NotNull(await waiting.WaitAsync(TimeSpan.FromSeconds(30)));
            Assert.Equal(1, handle.Generation);

            await handle.CloseAsync();
            Assert.Equal(2, handle.Generation);
            var closed = handle.GetHostAsync(cancel.Token);
            Assert.False(closed.IsCompleted);
            cancel.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => closed);
        }

        [Fact]
        public void TwoFetchesThroughOneReferenceColumnAreRefusedWhenTheOperatorIsBuilt()
        {
            var relation = new FetchRequiredColumnsRelation()
            {
                Input = Relation("rows"),
                Fetches = new List<RowStoreFetch>()
                {
                    new RowStoreFetch() { ReferenceColumn = 1, StoreName = "rows", Columns = new List<int>() { 0 } },
                    new RowStoreFetch() { ReferenceColumn = 1, StoreName = "rows", Columns = new List<int>() { 1 } }
                }
            };
            var handle = new RowStoreHandle("rows");
            var exception = Assert.Throws<InvalidOperationException>(() => new FetchRequiredColumnsOperator(relation, [handle, handle], Options()));
            Assert.Equal("Two fetches read through reference column 1, name every column of a store in one fetch.", exception.Message);
        }

        [Theory]
        [InlineData(5, 0, "A fetch from row store 'rows' reads its references from column 5, the input has 2.")]
        [InlineData(1, -1, null)]
        public void AWrongFetchIsRefusedWhenTheOperatorIsBuilt(int reference, int column, string? message)
        {
            var relation = new FetchRequiredColumnsRelation()
            {
                Input = Relation("rows"),
                Fetches = new List<RowStoreFetch>() { new RowStoreFetch() { ReferenceColumn = reference, StoreName = "rows", Columns = column < 0 ? new List<int>() : new List<int>() { column } } }
            };
            var exception = Assert.Throws<InvalidOperationException>(() => new FetchRequiredColumnsOperator(relation, [new RowStoreHandle("rows")], Options()));
            Assert.Equal(message ?? "A fetch from row store 'rows' reads no column.", exception.Message);
        }
    }
}
