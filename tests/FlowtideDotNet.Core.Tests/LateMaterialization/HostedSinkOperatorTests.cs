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
using FlowtideDotNet.Core.Tests.LateMaterialization.Differential;
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
    // A hosting sink next to a plain one
    public class HostedSinkOperatorTests : OperatorTestBase
    {
        private long _checkpoint;
        private ColumnNormalizationOperator? _host;
        private BufferBlock<IStreamEvent>? _hostOutput;
        private DifferentialGroupedSink? _hosted;
        private DifferentialGroupedSink? _plain;
        private readonly DifferentialDestination _hostedDestination = new DifferentialDestination();
        private readonly DifferentialDestination _plainDestination = new DifferentialDestination();
        // The reference the host gave each row it was sent
        private readonly Dictionary<(long key, long number, string text), ulong> _references = new Dictionary<(long key, long number, string text), ulong>();

        protected override StateManagerOptions CreateStateManagerOptions()
        {
            return new StateManagerOptions()
            {
                PersistentStorage = new ReservoirPersistentStorage(new ReservoirStorageOptions() { FileProvider = new MemoryFileProvider() })
            };
        }

        // Each operator gets a state manager of its own
        private async Task<Func<Task>> Initialize(Base.Vertices.IStreamVertex vertex)
        {
            await InitializeOperator(vertex);
            var manager = typeof(OperatorTestBase).GetField("_stateManager", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!.GetValue(this)!;
            var save = manager.GetType().GetMethod("CheckpointAsync", Type.EmptyTypes) ?? manager.GetType().GetMethods().First(x => x.Name == "CheckpointAsync");
            return async () =>
            {
                var parameters = save.GetParameters().Select(x => x.HasDefaultValue ? x.DefaultValue : null).ToArray();
                await (dynamic)save.Invoke(manager, parameters)!;
            };
        }

        private Func<Task>? _saveHost;
        private Func<Task>? _saveHosted;
        private Func<Task>? _savePlain;

        private static ExecutionDataflowBlockOptions Options()
        {
            return new ExecutionDataflowBlockOptions() { BoundedCapacity = 100, MaxDegreeOfParallelism = 1 };
        }

        // id, key, number, text; text is stored
        private static NormalizationRelation Relation(bool hosted)
        {
            return new NormalizationRelation()
            {
                KeyIndex = new List<int>() { 0 },
                RowStore = hosted ? new RowStoreDefinition() { Name = "rows", StoredColumns = new List<int>() { 3 } } : null,
                Input = new ReadRelation()
                {
                    NamedTable = new NamedTable() { Names = new List<string>() { "t" } },
                    BaseSchema = new NamedStruct()
                    {
                        Names = new List<string>() { "id", "key", "number", "text" },
                        Struct = new Struct() { Types = new List<SubstraitBaseType>() { new AnyType(), new AnyType(), new AnyType(), new AnyType() } }
                    }
                }
            };
        }

        private static WriteRelation Write(Relation input)
        {
            return new WriteRelation()
            {
                Input = input,
                NamedObject = new NamedTable() { Names = new List<string>() { "out" } },
                TableSchema = new NamedStruct() { Names = new List<string>() { "key", "text", "number" } }
            };
        }

        // Written as key, text, number: text decides
        private async Task Start()
        {
            var relation = Relation(true);
            var handle = new RowStoreHandle("rows") { HasHost = true };
            _host = new ColumnNormalizationOperator(relation, FunctionsRegister, Options());
            _host.AttachRowStoreHandle(handle);
            _hostOutput = new BufferBlock<IStreamEvent>();
            _host.LinkTo(_hostOutput);
            _saveHost = await Initialize(_host);

            var fetch = new FetchRequiredColumnsRelation()
            {
                Input = relation,
                Fetches = new List<RowStoreFetch>() { new RowStoreFetch() { ReferenceColumn = 3, StoreName = "rows", Columns = new List<int>() { 0 } } },
                Emit = [1, 4, 2]
            };
            _hosted = new DifferentialGroupedSink(Write(fetch), _hostedDestination);
            Assert.True(((IRowStoreFetchHostVertex)_hosted).TryAttachFetch(fetch, [handle]));
            _saveHosted = await Initialize(_hosted);

            var plain = Relation(false);
            plain.Emit = [1, 3, 2];
            _plain = new DifferentialGroupedSink(Write(plain), _plainDestination);
            _savePlain = await Initialize(_plain);
        }

        private static StreamMessage<StreamEventBatch> Message(IColumn[] columns, IReadOnlyList<int> rowWeights)
        {
            var weights = new PrimitiveList<int>(GlobalMemoryManager.Instance);
            var iterations = new PrimitiveList<uint>(GlobalMemoryManager.Instance);
            for (int i = 0; i < rowWeights.Count; i++)
            {
                weights.Add(rowWeights[i]);
                iterations.Add(0);
            }
            var batch = new StreamEventBatch(new EventBatchWeighted(weights, iterations, new EventBatchData(columns)));
            batch.Data.Rent(1);
            return new StreamMessage<StreamEventBatch>(batch, 1);
        }

        private static IColumn[] Columns(int count)
        {
            var columns = new IColumn[count];
            for (int i = 0; i < count; i++)
            {
                columns[i] = Column.Create(GlobalMemoryManager.Instance);
            }
            return columns;
        }

        // Gives the rows to the host and notes their references
        private async Task Store(params (long id, long key, long number, string text)[] rows)
        {
            var columns = Columns(4);
            foreach (var row in rows)
            {
                columns[0].Add(new Int64Value(row.id));
                columns[1].Add(new Int64Value(row.key));
                columns[2].Add(new Int64Value(row.number));
                columns[3].Add(new StringValue(row.text));
            }
            await _host!.SendAsync(Message(columns, rows.Select(x => 1).ToList()));
            _checkpoint++;
            await _host.SendAsync(new Checkpoint(_checkpoint, _checkpoint + 1, 1));
            while (true)
            {
                var received = await _hostOutput!.ReceiveAsync(TimeSpan.FromSeconds(30));
                if (received is ICheckpointEvent)
                {
                    break;
                }
                if (received is StreamMessage<StreamEventBatch> message)
                {
                    var data = message.Data.Data;
                    var batch = data.EventBatchData.Columns;
                    for (int i = 0; i < data.Count; i++)
                    {
                        if (data.Weights.Get(i) > 0)
                        {
                            long id = batch[0].GetValueAt(i, default).AsLong;
                            var row = rows.First(x => x.id == id);
                            _references[(row.key, row.number, row.text)] = (ulong)batch[3].GetValueAt(i, default).AsLong;
                        }
                    }
                    data.Return();
                }
            }
            await _host.BeforeSaveCheckpoint();
            await _saveHost!();
        }

        private async Task Checkpoint(DifferentialGroupedSink sink)
        {
            _checkpoint++;
            await SendCheckpoint(sink, _checkpoint);
            var done = EgressWaitForCheckpointDone();
            if (await Task.WhenAny(done, sink.Completion) != done)
            {
                await sink.Completion;
                throw new InvalidOperationException("The sink completed before its checkpoint was done.");
            }
        }

        // The same weighted rows to both sinks, then a checkpoint each
        private async Task Send(params (int weight, long key, long number, string text)[] rows)
        {
            var hosted = Columns(4);
            var plain = Columns(3);
            foreach (var row in rows)
            {
                hosted[0].Add(new Int64Value(0));
                hosted[1].Add(new Int64Value(row.key));
                hosted[2].Add(new Int64Value(row.number));
                hosted[3].Add(new RowRefValue(_references[(row.key, row.number, row.text)]));
                plain[0].Add(new Int64Value(row.key));
                plain[1].Add(new StringValue(row.text));
                plain[2].Add(new Int64Value(row.number));
            }
            var weights = rows.Select(x => x.weight).ToList();
            await _hosted!.SendAsync(Message(hosted, weights));
            await _plain!.SendAsync(Message(plain, weights));
            await Checkpoint(_hosted);
            await _saveHosted!();
            await Checkpoint(_plain);
            await _savePlain!();
        }

        private void AssertSameOperations(params string[] last)
        {
            Assert.Equal(_plainDestination.Operations, _hostedDestination.Operations);
            Assert.Equal(last, _plainDestination.Operations.TakeLast(last.Length));
        }

        [Fact]
        public async Task OfSeveralRowsForAKeyTheSinkWritesTheOneThePlainSinkWrites()
        {
            await Start();
            await Store((1, 7, 1, "m"), (2, 7, 2, "a"), (3, 7, 3, "z"), (4, 8, 1, "only"));

            await Send((1, 7, 1, "m"), (1, 7, 2, "a"), (1, 7, 3, "z"), (1, 8, 1, "only"));
            AssertSameOperations("upsert 7|a|2", "upsert 8|only|1");

            await Send((-1, 7, 2, "a"));
            AssertSameOperations("upsert 7|m|1");

            await Send((-1, 7, 1, "m"), (-1, 7, 3, "z"));
            AssertSameOperations("delete 7");
        }

        [Fact]
        public async Task ARowThatArrivesBelowZeroIsWrittenAsThePlainSinkWritesIt()
        {
            await Start();
            await Store((1, 7, 1, "first"), (2, 7, 2, "second"));

            await Send((-1, 7, 1, "first"));
            AssertSameOperations("upsert 7|first|1");

            await Send((1, 7, 1, "first"), (1, 7, 2, "second"));
            AssertSameOperations("upsert 7|second|2");

            await Send((-1, 7, 2, "second"));
            AssertSameOperations("delete 7");
        }

        [Fact]
        public async Task EqualWrittenRowsWithDifferentReferencesCancelTheirWeights()
        {
            await Start();
            await Store((1, 7, 1, "same"));
            ulong firstReference = _references[(7, 1, "same")];
            await Store((2, 7, 1, "same"));
            ulong secondReference = _references[(7, 1, "same")];
            Assert.NotEqual(firstReference, secondReference);

            var hosted = Columns(4);
            var plain = Columns(3);
            foreach (ulong reference in new[] { firstReference, secondReference })
            {
                hosted[0].Add(new Int64Value(0));
                hosted[1].Add(new Int64Value(7));
                hosted[2].Add(new Int64Value(1));
                hosted[3].Add(new RowRefValue(reference));
                plain[0].Add(new Int64Value(7));
                plain[1].Add(new StringValue("same"));
                plain[2].Add(new Int64Value(1));
            }

            await _hosted!.SendAsync(Message(hosted, [1, -1]));
            await _plain!.SendAsync(Message(plain, [1, -1]));
            await Checkpoint(_hosted);
            await _saveHosted!();
            await Checkpoint(_plain);
            await _savePlain!();

            Assert.Equal(["delete 7"], _plainDestination.Operations);
            Assert.Equal(_plainDestination.Operations, _hostedDestination.Operations);
        }

        // Weighted rows with the reference each one carries
        private async Task SendWithReferences(params (int weight, long key, long number, string text, ulong reference)[] rows)
        {
            var hosted = Columns(4);
            var plain = Columns(3);
            foreach (var row in rows)
            {
                hosted[0].Add(new Int64Value(0));
                hosted[1].Add(new Int64Value(row.key));
                hosted[2].Add(new Int64Value(row.number));
                hosted[3].Add(new RowRefValue(row.reference));
                plain[0].Add(new Int64Value(row.key));
                plain[1].Add(new StringValue(row.text));
                plain[2].Add(new Int64Value(row.number));
            }
            var weights = rows.Select(x => x.weight).ToList();
            await _hosted!.SendAsync(Message(hosted, weights));
            await _plain!.SendAsync(Message(plain, weights));
            await Checkpoint(_hosted);
            await _saveHosted!();
            await Checkpoint(_plain);
            await _savePlain!();
        }

        [Theory]
        [InlineData(3)]
        [InlineData(3000)]
        public async Task WhenTheLowestRowsOfAKeyCancelTheNextRowIsWritten(int others)
        {
            await Start();
            await Store((1, 7, 1, "a lowest"));
            ulong first = _references[(7, 1, "a lowest")];
            await Store((2, 7, 1, "a lowest"));
            ulong second = _references[(7, 1, "a lowest")];
            var rows = Enumerable.Range(0, others).Select(x => ((long)(10 + x), 7L, (long)(10 + x), "b " + x.ToString("D5"))).ToArray();
            await Store(rows);

            var sent = rows.Select(x => (1, x.Item2, x.Item3, x.Item4, _references[(x.Item2, x.Item3, x.Item4)])).ToList();
            sent.Add((1, 7, 1, "a lowest", first));
            sent.Add((-1, 7, 1, "a lowest", second));
            await SendWithReferences(sent.ToArray());

            AssertSameOperations("upsert 7|b 00000|10");
        }

        [Fact]
        public async Task ALargeKeyUsesThePlainSinksWeightArithmetic()
        {
            await Start();
            var references = new List<ulong>();
            for (int i = 0; i < 4; i++)
            {
                await Store((i, 7, 1, "a lowest"));
                references.Add(_references[(7, 1, "a lowest")]);
            }
            var rows = Enumerable.Range(10, 70).Select(x => ((long)x, 7L, (long)x, "b " + x.ToString("D5"))).ToArray();
            await Store(rows);
            var sent = rows.Select(x => (1, x.Item2, x.Item3, x.Item4, _references[(x.Item2, x.Item3, x.Item4)])).ToList();
            int[] weights = [int.MaxValue, int.MaxValue, 1, 1];
            for (int i = 0; i < references.Count; i++)
            {
                sent.Add((weights[i], 7, 1, "a lowest", references[i]));
            }
            await SendWithReferences(sent.ToArray());

            AssertSameOperations("upsert 7|b 00010|10");
        }

        [Fact]
        public async Task WhenEveryRowOfALargeKeyCancelsTheKeyIsDeleted()
        {
            await Start();
            var rows = Enumerable.Range(0, 1500).Select(x => ((long)x, 7L, (long)x, "text " + x)).ToArray();
            await Store(rows);
            var first = rows.ToDictionary(x => x.Item1, x => _references[(x.Item2, x.Item3, x.Item4)]);
            await Store(rows.Select(x => (x.Item1 + 5000, x.Item2, x.Item3, x.Item4)).ToArray());

            var sent = rows.Select(x => (1, x.Item2, x.Item3, x.Item4, first[x.Item1])).ToList();
            sent.AddRange(rows.Select(x => (-1, x.Item2, x.Item3, x.Item4, _references[(x.Item2, x.Item3, x.Item4)])));
            await SendWithReferences(sent.ToArray());

            AssertSameOperations("delete 7");
        }

        [Fact]
        public async Task AKeyWhoseRowsFillSeveralTreePagesIsReadToItsEnd()
        {
            await Start();
            var rows = Enumerable.Range(0, 6000).Select(x => ((long)x, 7L, (long)x, new string((char)('b' + x % 20), 40) + x)).ToArray();
            await Store(rows.Append((6000L, 7L, 6000L, "a lowest")).ToArray());

            await Send(rows.Select(x => (1, x.Item2, x.Item3, x.Item4)).Append((1, 7L, 6000L, "a lowest")).ToArray());
            AssertSameOperations("upsert 7|a lowest|6000");

            await Send((-1, 7, 6000, "a lowest"));
            Assert.Equal(_plainDestination.Operations, _hostedDestination.Operations);
        }

        [Fact]
        public async Task ManyChangedKeysAreFetchedInChunks()
        {
            await Start();
            var rows = Enumerable.Range(0, 2600).Select(x => ((long)x, (long)x, (long)(x % 5), "text " + x)).ToArray();
            await Store(rows);

            await Send(rows.Select(x => (1, x.Item2, x.Item3, x.Item4)).ToArray());
            Assert.Equal(_plainDestination.Operations, _hostedDestination.Operations);
            Assert.Equal(2600, _hostedDestination.Rows().Count);

            await Send(rows.Where(x => x.Item1 % 2 == 0).Select(x => (-1, x.Item2, x.Item3, x.Item4)).ToArray());
            Assert.Equal(_plainDestination.Operations, _hostedDestination.Operations);
            Assert.Equal(1300, _hostedDestination.Rows().Count);
        }
    }
}
