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
using FlowtideDotNet.Base.Vertices;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Core.Compute.Internal;
using FlowtideDotNet.Core.Operators.LateMaterialization;
using FlowtideDotNet.Core.Operators.Read;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Persistence.Reservoir;
using FlowtideDotNet.Storage.Persistence.Reservoir.Internal;
using FlowtideDotNet.Storage.Persistence.Reservoir.MemoryDisk;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Substrait.Expressions.Literals;
using FlowtideDotNet.Substrait.FunctionExtensions;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Type;
using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Core.Tests.Operators.Read
{
    public class HostedReadTests
    {
        private sealed record Row(long Key, long Kept, string Text, int Weight = 1);

        private sealed class Source : ColumnBatchReadBaseOperator
        {
            private long _watermark;

            private readonly bool _appendOnly;

            public Source(ReadRelation relation, List<Row> table, bool appendOnly = false, FunctionsRegister? functions = null)
                : base(relation, functions ?? Functions(), new DataflowBlockOptions())
            {
                Table = table;
                _appendOnly = appendOnly;
            }

            private static FunctionsRegister Functions()
            {
                var register = new FunctionsRegister();
                BuiltinFunctions.RegisterFunctions(register);
                return register;
            }

            protected override bool AppendOnly => _appendOnly;

            public List<Row> Table { get; }

            public ConcurrentQueue<Row[]> Deltas { get; } = new ConcurrentQueue<Row[]>();

            public override string DisplayName => "hosted source";

            public override Task Compact() => Task.CompletedTask;

            protected override Task Checkpoint(long checkpointTime) => Task.CompletedTask;

            protected override Task<IReadOnlySet<string>> GetWatermarkNames() => Task.FromResult<IReadOnlySet<string>>(new HashSet<string>() { "t" });

            protected override ValueTask<List<int>> GetPrimaryKeyColumns() => ValueTask.FromResult(new List<int>() { 0 });

            private static EventBatchWeighted Batch(IReadOnlyList<Row> rows)
            {
                var allocator = GlobalMemoryManager.Instance;
                var columns = new IColumn[] { Column.Create(allocator), Column.Create(allocator), Column.Create(allocator) };
                var weights = new PrimitiveList<int>(allocator);
                var iterations = new PrimitiveList<uint>(allocator);
                foreach (var row in rows)
                {
                    columns[0].Add(new Int64Value(row.Key));
                    columns[1].Add(new Int64Value(row.Kept));
                    columns[2].Add(new StringValue(row.Text));
                    weights.Add(row.Weight);
                    iterations.Add(0);
                }
                return new EventBatchWeighted(weights, iterations, new EventBatchData(columns));
            }

            protected override async IAsyncEnumerable<ColumnReadEvent> FullLoad(CancellationToken cancellationToken, [EnumeratorCancellation] CancellationToken enumeratorCancellationToken = default)
            {
                await Task.Yield();
                Row[] rows;
                lock (Table)
                {
                    rows = Table.ToArray();
                }
                if (rows.Length > 0)
                {
                    yield return new ColumnReadEvent(Batch(rows), ++_watermark);
                }
            }

            protected override async IAsyncEnumerable<DeltaReadEvent> DeltaLoad(Func<Task> EnterCheckpointLock, Action ExitCheckpointLock, CancellationToken cancellationToken, [EnumeratorCancellation] CancellationToken enumeratorCancellationToken = default)
            {
                while (Deltas.TryDequeue(out var rows))
                {
                    await EnterCheckpointLock();
                    try
                    {
                        yield return new DeltaReadEvent(Batch(rows), new Watermark("t", LongWatermarkValue.Create(++_watermark)));
                    }
                    finally
                    {
                        ExitCheckpointLock();
                    }
                }
            }

            public async Task<string?> Lookup(long key)
            {
                var allocator = GlobalMemoryManager.Instance;
                var columns = new IColumn[] { Column.Create(allocator), Column.Create(allocator), Column.Create(allocator) };
                using var batch = new EventBatchData(columns);
                columns[0].Add(new Int64Value(key));
                columns[1].Add(NullValue.Instance);
                columns[2].Add(NullValue.Instance);
                var (found, value) = await LookupRowValue(new ColumnRowReference() { referenceBatch = batch, RowIndex = 0 });
                if (!found)
                {
                    return null;
                }
                var text = $"{value.referenceBatch.Columns[0].GetValueAt(value.RowIndex, default).AsLong}|{value.referenceBatch.Columns[1].GetValueAt(value.RowIndex, default).AsString}";
                if (LookupRowIsOwned)
                {
                    value.referenceBatch.Dispose();
                }
                return text;
            }
        }

        private sealed class Sink() : EgressVertex<StreamEventBatch>(new ExecutionDataflowBlockOptions())
        {
            private int _checkpoints;

            public ConcurrentQueue<(string row, ulong reference)> Rows { get; } = new ConcurrentQueue<(string row, ulong reference)>();

            public int Checkpoints => Volatile.Read(ref _checkpoints);

            public override string DisplayName => "capturing sink";

            protected override Task OnRecieve(StreamEventBatch msg, long time)
            {
                var data = msg.Data;
                var columns = data.EventBatchData.Columns;
                for (int i = 0; i < data.Count; i++)
                {
                    var parts = new List<string>();
                    ulong reference = 0;
                    for (int c = 0; c < columns.Count; c++)
                    {
                        var value = columns[c].GetValueAt(i, default);
                        if (value.Type == ArrowTypeId.RowRef)
                        {
                            reference = (ulong)value.AsLong;
                            parts.Add("ref");
                        }
                        else
                        {
                            parts.Add(value.Type == ArrowTypeId.Int64 ? value.AsLong.ToString() : value.AsString.ToString());
                        }
                    }
                    Rows.Enqueue(($"{data.Weights.Get(i)}: {string.Join("|", parts)}", reference));
                }
                return Task.CompletedTask;
            }

            protected override Task OnCheckpoint(long checkpointTime)
            {
                Interlocked.Increment(ref _checkpoints);
                return Task.CompletedTask;
            }

            protected override Task InitializeOrRestore(long restoreTime, IStateManagerClient stateManagerClient) => Task.CompletedTask;

            public override Task Compact() => Task.CompletedTask;

            public override Task DeleteAsync() => Task.CompletedTask;
        }

        private static ReadRelation Relation(bool hosted)
        {
            var relation = new ReadRelation()
            {
                NamedTable = new NamedTable() { Names = new List<string>() { "t" } },
                BaseSchema = new NamedStruct()
                {
                    Names = new List<string>() { "k", "kept", "text" },
                    Struct = new Struct() { Types = new List<SubstraitBaseType>() { new AnyType(), new AnyType(), new AnyType() } }
                }
            };
            if (hosted)
            {
                relation.RowStore = new RowStoreDefinition() { Name = "rows", StoredColumns = [2] };
            }
            return relation;
        }

        // Sends k and text, keeps rows whose kept is 10
        private static ReadRelation FilteredRelation(bool hosted)
        {
            var relation = Relation(false);
            relation.Emit = new List<int>() { 0, 2 };
            relation.Filter = new ScalarFunction()
            {
                ExtensionUrn = FunctionsComparison.Urn,
                ExtensionName = FunctionsComparison.Equal,
                Arguments = new List<Expression>()
                {
                    new DirectFieldReference() { ReferenceSegment = new StructReferenceSegment() { Field = 1 } },
                    new NumericLiteral() { Value = 10 }
                }
            };
            if (hosted)
            {
                relation.RowStore = new RowStoreDefinition() { Name = "rows", StoredColumns = [1] };
            }
            return relation;
        }

        private static Base.Engine.DataflowStream Build(string name, ReservoirPersistentStorage storage, Source source, Sink sink)
        {
            source.LinkTo(sink, new DataflowLinkOptions() { PropagateCompletion = true });
            return new DataflowStreamBuilder(name)
                .AddIngressBlock("source", source)
                .AddEgressBlock("sink", sink)
                .WithStateOptions(new StateManagerOptions() { PersistentStorage = storage })
                .Build();
        }

        // Source stores its text, the fetch reads it
        private static Base.Engine.DataflowStream BuildWithFetch(string name, ReservoirPersistentStorage storage, Source source, Sink sink, ReadRelation relation)
        {
            var handle = new RowStoreHandle(relation.RowStore!.Name) { HasHost = true };
            ((IRowStoreHostVertex)source).AttachRowStoreHandle(handle);
            var fetch = new FetchRequiredColumnsOperator(new FetchRequiredColumnsRelation()
            {
                Input = relation,
                Fetches = new List<RowStoreFetch>()
                {
                    new RowStoreFetch() { ReferenceColumn = 2, StoreName = relation.RowStore.Name, Columns = new List<int>() { 0 } }
                },
                Emit = new List<int>() { 0, 1, 3 }
            }, [handle], new ExecutionDataflowBlockOptions() { BoundedCapacity = 100, MaxDegreeOfParallelism = 1 });
            source.LinkTo(fetch, new DataflowLinkOptions() { PropagateCompletion = true });
            fetch.LinkTo(sink, new DataflowLinkOptions() { PropagateCompletion = true });
            return new DataflowStreamBuilder(name)
                .AddIngressBlock("source", source)
                .AddPropagatorBlock("fetch", fetch)
                .AddEgressBlock("sink", sink)
                .WithStateOptions(new StateManagerOptions() { PersistentStorage = storage })
                .Build();
        }

        private static async Task WaitUntil(Func<bool> condition)
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(20));
            while (!condition())
            {
                await Task.Delay(10, timeout.Token);
            }
        }

        // A restarted source registers its triggers after it runs
        private static async Task<List<(string row, ulong reference)>> TriggerAndTake(Base.Engine.DataflowStream stream, string trigger, Sink sink, int rows)
        {
            int checkpoints = sink.Checkpoints;
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(20));
            while (sink.Rows.Count < rows)
            {
                await stream.CallTrigger(trigger, null);
                await Task.Delay(50, timeout.Token);
            }
            return await Take(sink, rows, checkpoints);
        }

        // The rows of a load, after its checkpoint
        private static async Task<List<(string row, ulong reference)>> Take(Sink sink, int rows, int checkpoints)
        {
            await WaitUntil(() => sink.Rows.Count >= rows && sink.Checkpoints > checkpoints);
            var taken = new List<(string row, ulong reference)>();
            while (sink.Rows.TryDequeue(out var row))
            {
                taken.Add(row);
            }
            taken.Sort((x, y) => string.CompareOrdinal(x.row, y.row));
            return taken;
        }

        [Fact]
        public async Task AHostedSourceSendsReferencesThroughLoadsDeltasAndARestart()
        {
            using var storage = new ReservoirPersistentStorage(new ReservoirStorageOptions() { FileProvider = new MemoryFileProvider() });
            var name = "hosted-read-" + Guid.NewGuid().ToString("N");
            int checkpoints;
            var table = new List<Row>() { new Row(1, 10, "a"), new Row(2, 20, "b"), new Row(3, 30, "c") };
            ulong second;
            ulong third;

            var source = new Source(Relation(true), table);
            var sink = new Sink();
            await using (var stream = Build(name, storage, source, sink))
            {
                await stream.StartAsync();
                var loaded = await Take(sink, 3, 0);
                Assert.Equal(["1: 1|10|ref", "1: 2|20|ref", "1: 3|30|ref"], loaded.Select(x => x.row));
                Assert.Equal(3, loaded.Select(x => x.reference).Distinct().Count());
                Assert.Equal("10|a", await source.Lookup(1));
                Assert.Null(await source.Lookup(9));

                source.Deltas.Enqueue([new Row(2, 20, "changed"), new Row(3, 0, "", -1)]);
                checkpoints = sink.Checkpoints;
                await stream.CallTrigger("delta_load", null);
                var delta = await Take(sink, 3, checkpoints);
                Assert.Equal(["-1: 2|20|ref", "-1: 3|30|ref", "1: 2|20|ref"], delta.Select(x => x.row));
                Assert.Equal(loaded[1].reference, delta[0].reference);
                Assert.Equal(loaded[2].reference, delta[1].reference);
                Assert.NotEqual(loaded[1].reference, delta[2].reference);
                second = delta[2].reference;
                Assert.Equal("20|changed", await source.Lookup(2));

                lock (table)
                {
                    table.Clear();
                    table.Add(new Row(2, 21, "changed"));
                    table.Add(new Row(4, 40, "d"));
                }
                checkpoints = sink.Checkpoints;
                await stream.CallTrigger("full_load", null);
                var reloaded = await Take(sink, 4, checkpoints);
                Assert.Equal(["-1: 1|10|ref", "-1: 2|20|ref", "1: 2|21|ref", "1: 4|40|ref"], reloaded.Select(x => x.row));
                Assert.Equal(loaded[0].reference, reloaded[0].reference);
                Assert.Equal(second, reloaded[1].reference);
                Assert.Equal(second, reloaded[2].reference);
                third = reloaded[3].reference;

                await stream.StopAsync().WaitAsync(TimeSpan.FromSeconds(20));
            }

            var restartedSource = new Source(Relation(true), table);
            var restartedSink = new Sink();
            await using var restarted = Build(name, storage, restartedSource, restartedSink);
            await restarted.StartAsync();
            await WaitUntil(() => restarted.State == StreamStateValue.Running);
            Assert.Equal("21|changed", await restartedSource.Lookup(2));
            Assert.Null(await restartedSource.Lookup(1));

            lock (table)
            {
                table.RemoveAll(x => x.Key == 4);
            }
            var afterRestart = await TriggerAndTake(restarted, "full_load", restartedSink, 1);
            Assert.Equal(["-1: 4|40|ref"], afterRestart.Select(x => x.row));
            Assert.Equal(third, afterRestart[0].reference);
        }

        [Fact]
        public async Task AFetchGivesBackTheRowsAPlainSourceSends()
        {
            using var storage = new ReservoirPersistentStorage(new ReservoirStorageOptions() { FileProvider = new MemoryFileProvider() });
            var name = "fetched-read-" + Guid.NewGuid().ToString("N");
            int checkpoints;
            var table = new List<Row>() { new Row(1, 10, "a"), new Row(2, 20, "b"), new Row(3, 30, "c") };

            var relation = Relation(true);
            var source = new Source(relation, table);
            var sink = new Sink();
            await using (var stream = BuildWithFetch(name, storage, source, sink, relation))
            {
                await stream.StartAsync();
                var loaded = await Take(sink, 3, 0);
                Assert.Equal(["1: 1|10|a", "1: 2|20|b", "1: 3|30|c"], loaded.Select(x => x.row));

                source.Deltas.Enqueue([new Row(2, 20, "changed"), new Row(3, 0, "", -1)]);
                checkpoints = sink.Checkpoints;
                await stream.CallTrigger("delta_load", null);
                var delta = await Take(sink, 3, checkpoints);
                Assert.Equal(["-1: 2|20|b", "-1: 3|30|c", "1: 2|20|changed"], delta.Select(x => x.row));

                lock (table)
                {
                    table.Clear();
                    table.Add(new Row(2, 21, "changed"));
                    table.Add(new Row(4, 40, "d"));
                }
                checkpoints = sink.Checkpoints;
                await stream.CallTrigger("full_load", null);
                var reloaded = await Take(sink, 4, checkpoints);
                Assert.Equal(["-1: 1|10|a", "-1: 2|20|changed", "1: 2|21|changed", "1: 4|40|d"], reloaded.Select(x => x.row));

                await stream.StopAsync().WaitAsync(TimeSpan.FromSeconds(20));
            }

            var restartedRelation = Relation(true);
            var restartedSource = new Source(restartedRelation, table);
            var restartedSink = new Sink();
            await using var restarted = BuildWithFetch(name, storage, restartedSource, restartedSink, restartedRelation);
            await restarted.StartAsync();
            await WaitUntil(() => restarted.State == StreamStateValue.Running);

            lock (table)
            {
                table.RemoveAll(x => x.Key == 4);
            }
            var afterRestart = await TriggerAndTake(restarted, "full_load", restartedSink, 1);
            Assert.Equal(["-1: 4|40|d"], afterRestart.Select(x => x.row));
        }

        private sealed class TableSourceFactory(List<Row> table) : Core.Connectors.RegexConnectorSourceFactory("*")
        {
            public override IStreamIngressVertex CreateSource(ReadRelation readRelation, IFunctionsRegister functionsRegister, DataflowBlockOptions dataflowBlockOptions)
            {
                return new Source(readRelation, table);
            }

            public override Core.Lineage.TableLineageMetadata GetLineageMetadata(ReadRelation readRelation, bool includeSchema)
            {
                return new Core.Lineage.TableLineageMetadata("test", readRelation.NamedTable.DotSeperated, default);
            }
        }

        private sealed class CapturingSinkFactory(Sink sink) : Core.Connectors.RegexConnectorSinkFactory("*")
        {
            public override IStreamEgressVertex CreateSink(WriteRelation writeRelation, IFunctionsRegister functionsRegister, ExecutionDataflowBlockOptions dataflowBlockOptions)
            {
                return sink;
            }

            public override Core.Lineage.TableLineageMetadata GetLineageMetadata(WriteRelation writeRelation, bool includeSchema)
            {
                return new Core.Lineage.TableLineageMetadata("test", writeRelation.NamedObject.DotSeperated, default);
            }
        }

        [Fact]
        public async Task APlanBuiltByTheEngineFetchesThroughTheStoreItsSourceHosts()
        {
            using var storage = new ReservoirPersistentStorage(new ReservoirStorageOptions() { FileProvider = new MemoryFileProvider() });
            var table = new List<Row>() { new Row(1, 10, "a"), new Row(2, 20, "b") };
            var sink = new Sink();
            var connectorManager = new Core.ConnectorManager();
            connectorManager.AddSource(new TableSourceFactory(table));
            connectorManager.AddSink(new CapturingSinkFactory(sink));
            var plan = new Substrait.Plan()
            {
                Relations =
                [
                    new WriteRelation()
                    {
                        NamedObject = new NamedTable() { Names = ["output"] },
                        TableSchema = new NamedStruct() { Names = ["k", "kept", "text"] },
                        Input = new FetchRequiredColumnsRelation()
                        {
                            Input = Relation(true),
                            Fetches = [new RowStoreFetch() { ReferenceColumn = 2, StoreName = "rows", Columns = [0] }],
                            Emit = [0, 1, 3]
                        }
                    }
                ]
            };
            await using var stream = new Core.Engine.FlowtideBuilder("built-fetch-" + Guid.NewGuid().ToString("N"))
                .AddPlan(plan, false)
                .AddConnectorManager(connectorManager)
                .WithStateOptions(new StateManagerOptions() { PersistentStorage = storage })
                .Build();
            await stream.StartAsync();

            var loaded = await Take(sink, 2, 0);
            Assert.Equal(["1: 1|10|a", "1: 2|20|b"], loaded.Select(x => x.row));

            await stream.StopAsync().WaitAsync(TimeSpan.FromSeconds(20));
        }

        [Fact]
        public async Task AnAppendOnlySourceStoresRowsWithoutKnowingThemByKey()
        {
            using var storage = new ReservoirPersistentStorage(new ReservoirStorageOptions() { FileProvider = new MemoryFileProvider() });
            var name = "append-read-" + Guid.NewGuid().ToString("N");
            int checkpoints;
            var table = new List<Row>() { new Row(1, 10, "a"), new Row(2, 20, "b") };
            List<(string row, ulong reference)> loaded;

            var source = new Source(Relation(true), table, appendOnly: true);
            var sink = new Sink();
            await using (var stream = Build(name, storage, source, sink))
            {
                await stream.StartAsync();
                loaded = await Take(sink, 2, 0);
                Assert.Equal(["1: 1|10|ref", "1: 2|20|ref"], loaded.Select(x => x.row));

                source.Deltas.Enqueue([new Row(3, 30, "c"), new Row(1, 10, "a")]);
                checkpoints = sink.Checkpoints;
                await stream.CallTrigger("delta_load", null);
                var delta = await Take(sink, 2, checkpoints);
                Assert.Equal(["1: 1|10|ref", "1: 3|30|ref"], delta.Select(x => x.row));
                Assert.Equal(4, loaded.Concat(delta).Select(x => x.reference).Distinct().Count());

                await stream.StopAsync().WaitAsync(TimeSpan.FromSeconds(20));
            }

            var restartedSource = new Source(Relation(true), table, appendOnly: true);
            var restartedSink = new Sink();
            await using var restarted = Build(name, storage, restartedSource, restartedSink);
            await restarted.StartAsync();
            await WaitUntil(() => restarted.State == StreamStateValue.Running);
            restartedSource.Deltas.Enqueue([new Row(4, 40, "d")]);
            var afterRestart = await TriggerAndTake(restarted, "delta_load", restartedSink, 1);
            Assert.Equal(["1: 4|40|ref"], afterRestart.Select(x => x.row));
            Assert.DoesNotContain(afterRestart[0].reference, loaded.Select(x => x.reference));
        }

        [Fact]
        public async Task WithoutTheHintASourceSendsWholeRowsAndFindsDeletedKeys()
        {
            using var storage = new ReservoirPersistentStorage(new ReservoirStorageOptions() { FileProvider = new MemoryFileProvider() });
            var table = new List<Row>() { new Row(1, 10, "a"), new Row(2, 20, "b") };
            var source = new Source(Relation(false), table);
            var sink = new Sink();
            await using var stream = Build("plain-read-" + Guid.NewGuid().ToString("N"), storage, source, sink);
            await stream.StartAsync();
            var loaded = await Take(sink, 2, 0);
            Assert.Equal(["1: 1|10|a", "1: 2|20|b"], loaded.Select(x => x.row));
            int checkpoints;

            lock (table)
            {
                table.RemoveAll(x => x.Key == 1);
                table.Add(new Row(3, 30, "c"));
            }
            checkpoints = sink.Checkpoints;
            await stream.CallTrigger("full_load", null);
            var reloaded = await Take(sink, 2, checkpoints);
            Assert.Equal(["-1: 1|10|a", "1: 3|30|c"], reloaded.Select(x => x.row));
            Assert.Equal("20|b", await source.Lookup(2));
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task AReloadedRowThatNowFailsAFilterOnAnUnsentColumnIsTakenBack(bool hosted)
        {
            using var storage = new ReservoirPersistentStorage(new ReservoirStorageOptions() { FileProvider = new MemoryFileProvider() });
            var table = new List<Row>() { new Row(1, 10, "a"), new Row(2, 10, "b") };
            var source = new Source(FilteredRelation(hosted), table);
            var sink = new Sink();
            await using var stream = Build("filter-reload-" + Guid.NewGuid().ToString("N"), storage, source, sink);
            await stream.StartAsync();
            var loaded = await Take(sink, 2, 0);
            Assert.Equal(2, loaded.Count);

            lock (table)
            {
                table[0] = new Row(1, 11, "a");
                table.Add(new Row(3, 10, "c"));
            }
            int checkpoints = sink.Checkpoints;
            await stream.CallTrigger("full_load", null);
            var reloaded = await Take(sink, 1, checkpoints);
            Assert.Equal(hosted ? ["-1: 1|ref", "1: 3|ref"] : ["-1: 1|a", "1: 3|c"], reloaded.Select(x => x.row));
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task AChangedRowThatNowFailsAFilterOnAnUnsentColumnIsTakenBack(bool hosted)
        {
            using var storage = new ReservoirPersistentStorage(new ReservoirStorageOptions() { FileProvider = new MemoryFileProvider() });
            var table = new List<Row>() { new Row(1, 10, "a"), new Row(2, 10, "b") };
            var source = new Source(FilteredRelation(hosted), table);
            var sink = new Sink();
            await using var stream = Build("filter-delta-" + Guid.NewGuid().ToString("N"), storage, source, sink);
            await stream.StartAsync();
            var loaded = await Take(sink, 2, 0);
            Assert.Equal(2, loaded.Count);

            source.Deltas.Enqueue([new Row(2, 11, "b"), new Row(3, 10, "c")]);
            int checkpoints = sink.Checkpoints;
            await stream.CallTrigger("delta_load", null);
            var changed = await Take(sink, 1, checkpoints);
            Assert.Equal(hosted ? ["-1: 2|ref", "1: 3|ref"] : ["-1: 2|b", "1: 3|c"], changed.Select(x => x.row));
        }

        [Theory]
        [InlineData(false, false)]
        [InlineData(false, true)]
        [InlineData(true, false)]
        [InlineData(true, true)]
        public async Task AnEqualRowThatFailsItsFilterEvaluatesItOnce(bool hosted, bool fullReload)
        {
            int calls = 0;
            var functions = new FunctionsRegister();
            BuiltinFunctions.RegisterFunctions(functions);
            functions.RegisterColumnScalarFunction("extension:test:counted", "counted",
                (function, parameters, visitor, services) => System.Linq.Expressions.Expression.Block(
                    System.Linq.Expressions.Expression.Invoke(
                        System.Linq.Expressions.Expression.Constant((Action)(() => Interlocked.Increment(ref calls)))),
                    visitor.Visit(function.Arguments[0], parameters)!));
            var relation = FilteredRelation(hosted);
            relation.Filter = new ScalarFunction()
            {
                ExtensionUrn = "extension:test:counted",
                ExtensionName = "counted",
                Arguments = new List<Expression>() { relation.Filter! }
            };
            using var storage = new ReservoirPersistentStorage(new ReservoirStorageOptions() { FileProvider = new MemoryFileProvider() });
            var table = new List<Row>() { new Row(1, 10, "a") };
            var source = new Source(relation, table, functions: functions);
            var sink = new Sink();
            await using var stream = Build("counted-filter-" + Guid.NewGuid().ToString("N"), storage, source, sink);
            await stream.StartAsync();
            Assert.Single(await Take(sink, 1, 0));
            Assert.Equal(1, Volatile.Read(ref calls));
            Interlocked.Exchange(ref calls, 0);

            if (fullReload)
            {
                lock (table)
                {
                    table[0] = new Row(1, 11, "a");
                }
            }
            else
            {
                source.Deltas.Enqueue([new Row(1, 11, "a")]);
            }

            int checkpoints = sink.Checkpoints;
            await stream.CallTrigger(fullReload ? "full_load" : "delta_load", null);
            var retracted = await Take(sink, 1, checkpoints);
            Assert.Equal([hosted ? "-1: 1|ref" : "-1: 1|a"], retracted.Select(x => x.row));
            Assert.Equal(1, Volatile.Read(ref calls));
        }

        // Sums weights per row and reference, drops zeros
        private static List<string> Net(IEnumerable<(string row, ulong reference)> rows)
        {
            var sums = new Dictionary<(string, ulong), int>();
            foreach (var (row, reference) in rows)
            {
                var split = row.IndexOf(": ");
                var key = (row.Substring(split + 2), reference);
                sums[key] = sums.GetValueOrDefault(key) + int.Parse(row.Substring(0, split));
            }
            return sums.Where(x => x.Value != 0).Select(x => $"{x.Value}: {x.Key.Item1}").OrderBy(x => x, StringComparer.Ordinal).ToList();
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task AKeyTwiceInOneDeltaThatEndsFailingTheFilterIsTakenBack(bool hosted)
        {
            using var storage = new ReservoirPersistentStorage(new ReservoirStorageOptions() { FileProvider = new MemoryFileProvider() });
            var table = new List<Row>() { new Row(1, 10, "a"), new Row(2, 10, "b") };
            var source = new Source(FilteredRelation(hosted), table);
            var sink = new Sink();
            await using var stream = Build("filter-twice-" + Guid.NewGuid().ToString("N"), storage, source, sink);
            await stream.StartAsync();
            var loaded = await Take(sink, 2, 0);
            Assert.Equal(2, loaded.Count);

            source.Deltas.Enqueue([new Row(2, 10, "c"), new Row(2, 11, "c")]);
            var changed = await TriggerAndTake(stream, "delta_load", sink, 1);
            Assert.Equal([hosted ? "-1: 2|ref" : "-1: 2|b"], Net(changed));
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task ARowTakenBackByTheFilterIsSentAgainWhenItPassesAfterARestart(bool hosted)
        {
            using var storage = new ReservoirPersistentStorage(new ReservoirStorageOptions() { FileProvider = new MemoryFileProvider() });
            var name = "filter-restart-" + Guid.NewGuid().ToString("N");
            var table = new List<Row>() { new Row(1, 10, "a"), new Row(2, 10, "b") };
            var source = new Source(FilteredRelation(hosted), table);
            var sink = new Sink();
            await using (var stream = Build(name, storage, source, sink))
            {
                await stream.StartAsync();
                var loaded = await Take(sink, 2, 0);
                Assert.Equal(2, loaded.Count);

                source.Deltas.Enqueue([new Row(2, 11, "b")]);
                var changed = await TriggerAndTake(stream, "delta_load", sink, 1);
                Assert.Equal([hosted ? "-1: 2|ref" : "-1: 2|b"], changed.Select(x => x.row));
                await stream.StopAsync().WaitAsync(TimeSpan.FromSeconds(20));
            }

            var restartedSource = new Source(FilteredRelation(hosted), table);
            var restartedSink = new Sink();
            await using var restarted = Build(name, storage, restartedSource, restartedSink);
            await restarted.StartAsync();
            await WaitUntil(() => restarted.State == StreamStateValue.Running);
            restartedSource.Deltas.Enqueue([new Row(2, 10, "b")]);
            var back = await TriggerAndTake(restarted, "delta_load", restartedSink, 1);
            Assert.Equal([hosted ? "1: 2|ref" : "1: 2|b"], back.Select(x => x.row));
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task AReloadedRowThatFailsThenPassesTheFilterIsSentAgain(bool hosted)
        {
            using var storage = new ReservoirPersistentStorage(new ReservoirStorageOptions() { FileProvider = new MemoryFileProvider() });
            var table = new List<Row>() { new Row(1, 10, "a"), new Row(2, 10, "b") };
            var source = new Source(FilteredRelation(hosted), table);
            var sink = new Sink();
            await using var stream = Build("filter-reload-back-" + Guid.NewGuid().ToString("N"), storage, source, sink);
            await stream.StartAsync();
            var loaded = await Take(sink, 2, 0);
            Assert.Equal(2, loaded.Count);

            lock (table)
            {
                table[0] = new Row(1, 11, "a");
            }
            int checkpoints = sink.Checkpoints;
            await stream.CallTrigger("full_load", null);
            var reloaded = await Take(sink, 1, checkpoints);
            Assert.Equal([hosted ? "-1: 1|ref" : "-1: 1|a"], reloaded.Select(x => x.row));

            lock (table)
            {
                table[0] = new Row(1, 10, "a");
            }
            checkpoints = sink.Checkpoints;
            await stream.CallTrigger("full_load", null);
            var back = await Take(sink, 1, checkpoints);
            Assert.Equal([hosted ? "1: 1|ref" : "1: 1|a"], back.Select(x => x.row));
        }
    }
}
