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
using FlowtideDotNet.Core.Operators.Write;
using FlowtideDotNet.Core.Operators.Write.Column;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Persistence.Reservoir;
using FlowtideDotNet.Storage.Persistence.Reservoir.Internal;
using FlowtideDotNet.Storage.Persistence.Reservoir.MemoryDisk;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Type;
using System.Collections.Concurrent;
using System.Globalization;
using System.Text;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Core.Tests.LateMaterialization
{
    internal sealed class TypedGroupedSink : ColumnGroupedWriteOperator
    {
        private readonly List<int> _keys;

        public TypedGroupedSink(WriteRelation relation, int key = 0)
            : base(ExecutionMode.OnCheckpoint, relation, new ExecutionDataflowBlockOptions() { BoundedCapacity = 100, MaxDegreeOfParallelism = 1 })
        {
            _keys = new List<int>() { key };
        }

        public ConcurrentQueue<string> Operations { get; } = new ConcurrentQueue<string>();

        public override string DisplayName => "typed grouped sink";

        protected override ValueTask<IReadOnlyList<int>> GetPrimaryKeyColumns() => ValueTask.FromResult<IReadOnlyList<int>>(_keys);

        protected override void Checkpoint(long checkpointTime)
        {
        }

        public static string Render(IDataValue value)
        {
            switch (value.Type)
            {
                case ArrowTypeId.Null:
                    return "null";
                case ArrowTypeId.Int64:
                    return "i" + value.AsLong;
                case ArrowTypeId.Double:
                    return "d" + value.AsDouble.ToString("R", CultureInfo.InvariantCulture);
                case ArrowTypeId.Decimal128:
                    return "m" + value.AsDecimal.ToString(CultureInfo.InvariantCulture);
                case ArrowTypeId.String:
                    return "s" + value.AsString.ToString();
                case ArrowTypeId.Boolean:
                    return value.AsBool ? "true" : "false";
                case ArrowTypeId.Binary:
                    return "b" + Convert.ToHexString(value.AsBinary);
                case ArrowTypeId.Timestamp:
                    return "t" + value.AsTimestamp.ticks;
                case ArrowTypeId.List:
                    {
                        var list = value.AsList;
                        var builder = new StringBuilder("[");
                        for (int i = 0; i < list.Count; i++)
                        {
                            builder.Append(i == 0 ? "" : ",").Append(Render(list.GetAt(i)));
                        }
                        return builder.Append(']').ToString();
                    }
                case ArrowTypeId.Map:
                    {
                        var map = value.AsMap;
                        var builder = new StringBuilder("{");
                        for (int i = 0; i < map.GetLength(); i++)
                        {
                            builder.Append(i == 0 ? "" : ",").Append(Render(map.GetKeyAt(i))).Append(':').Append(Render(map.GetValueAt(i)));
                        }
                        return builder.Append('}').ToString();
                    }
                default:
                    return value.Type.ToString();
            }
        }

        protected override async Task UploadChanges(IAsyncEnumerable<ColumnWriteOperation> rows, Watermark watermark, bool isInitialData, CancellationToken cancellationToken)
        {
            await foreach (var row in rows)
            {
                var columns = row.EventBatchData.Columns;
                if (row.IsDeleted)
                {
                    Operations.Enqueue("delete " + string.Join("|", columns.Select(x => Render(x.GetValueAt(row.Index, default)))));
                    continue;
                }
                var parts = new string[columns.Count];
                for (int c = 0; c < parts.Length; c++)
                {
                    parts[c] = Render(columns[c].GetValueAt(row.Index, default));
                }
                Operations.Enqueue("upsert " + string.Join("|", parts));
            }
        }
    }

    // Rows of one key, one per type
    public class HostedSinkTypeTests : OperatorTestBase
    {
        private long _checkpoint;
        private ColumnNormalizationOperator? _host;
        private BufferBlock<IStreamEvent>? _hostOutput;
        private TypedGroupedSink? _hosted;
        private TypedGroupedSink? _plain;
        private readonly Dictionary<long, ulong> _references = new Dictionary<long, ulong>();
        private Func<Task>? _saveHost;
        private Func<Task>? _saveHosted;
        private Func<Task>? _savePlain;

        protected override StateManagerOptions CreateStateManagerOptions()
        {
            return new StateManagerOptions()
            {
                PersistentStorage = new ReservoirPersistentStorage(new ReservoirStorageOptions() { FileProvider = new MemoryFileProvider() })
            };
        }

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

        private static ExecutionDataflowBlockOptions Options()
        {
            return new ExecutionDataflowBlockOptions() { BoundedCapacity = 100, MaxDegreeOfParallelism = 1 };
        }

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
                        Names = new List<string>() { "id", "key", "number", "value" },
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
                TableSchema = new NamedStruct() { Names = new List<string>() { "a", "b", "c" } }
            };
        }

        // Write layout key, value, number unless the key moves
        private int _plainKey;
        private List<int> _fetchEmit = [1, 4, 2];
        private List<int> _plainEmit = [1, 3, 2];

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
                Emit = _fetchEmit
            };
            _hosted = new TypedGroupedSink(Write(fetch), _plainKey);
            Assert.True(((IRowStoreFetchHostVertex)_hosted).TryAttachFetch(fetch, [handle]));
            _saveHosted = await Initialize(_hosted);

            var plain = Relation(false);
            plain.Emit = _plainEmit;
            _plain = new TypedGroupedSink(Write(plain), _plainKey);
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

        private async Task Store(IReadOnlyList<(long id, long key, long number, IDataValue value)> rows)
        {
            var columns = Columns(4);
            foreach (var row in rows)
            {
                columns[0].Add(new Int64Value(row.id));
                columns[1].Add(new Int64Value(row.key));
                columns[2].Add(new Int64Value(row.number));
                columns[3].Add(row.value);
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
                            _references[batch[0].GetValueAt(i, default).AsLong] = (ulong)batch[3].GetValueAt(i, default).AsLong;
                        }
                    }
                    data.Return();
                }
            }
            await _host.BeforeSaveCheckpoint();
            await _saveHost!();
        }

        private async Task Checkpoint(TypedGroupedSink sink)
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

        private async Task Send(int weight, IReadOnlyList<(long id, long key, long number, IDataValue value)> rows)
        {
            var hosted = Columns(4);
            var plain = Columns(3);
            foreach (var row in rows)
            {
                hosted[0].Add(new Int64Value(0));
                hosted[1].Add(new Int64Value(row.key));
                hosted[2].Add(new Int64Value(row.number));
                hosted[3].Add(new RowRefValue(_references[row.id]));
                IDataValue[] source = [new Int64Value(row.id), new Int64Value(row.key), new Int64Value(row.number), row.value];
                for (int c = 0; c < plain.Length; c++)
                {
                    plain[c].Add(source[_plainEmit[c]]);
                }
            }
            var weights = rows.Select(x => weight).ToList();
            await _hosted!.SendAsync(Message(hosted, weights));
            await _plain!.SendAsync(Message(plain, weights));
            await Checkpoint(_hosted);
            await _saveHosted!();
            await Checkpoint(_plain);
            await _savePlain!();
        }

        private static IDataValue[] Values()
        {
            return
            [
                NullValue.Instance,
                new Int64Value(5),
                new Int64Value(-3),
                new DoubleValue(2.5),
                new DoubleValue(double.NaN),
                new DoubleValue(double.NegativeInfinity),
                new DecimalValue(1.5m),
                new DecimalValue(-7.25m),
                new StringValue("b"),
                new StringValue("a"),
                new StringValue(""),
                new BoolValue(true),
                new BoolValue(false),
                new BinaryValue(new byte[] { 1, 2 }),
                new BinaryValue(new byte[] { 1 }),
                new TimestampTzValue(1000, 0),
                new TimestampTzValue(-5, 0),
                new ListValue(new Int64Value(1), new Int64Value(2)),
                new ListValue(new Int64Value(1)),
                new ListValue(new StringValue("x"), NullValue.Instance),
                new MapValue(new KeyValuePair<IDataValue, IDataValue>(new StringValue("a"), new Int64Value(1))),
                new MapValue(new KeyValuePair<IDataValue, IDataValue>(new StringValue("a"), new Int64Value(0)))
            ];
        }

        [Fact]
        public async Task OfAKeysRowsOfEveryTypeTheHostedSinkWritesWhatThePlainSinkWrites()
        {
            await Start();
            var values = Values();
            var rows = values.Select((value, i) => ((long)i, 7L, (long)(values.Length - i), value)).ToList();
            await Store(rows);

            await Send(1, rows);
            Assert.Equal(_plain!.Operations, _hosted!.Operations);

            var left = rows.ToList();
            while (left.Count > 0)
            {
                var written = _plain.Operations.Last();
                var number = long.Parse(written.Substring(written.LastIndexOf("|i", StringComparison.Ordinal) + 2), CultureInfo.InvariantCulture);
                var row = left.Single(x => x.Item3 == number);
                left.Remove(row);
                await Send(-1, [row]);
                Assert.Equal(_plain.Operations, _hosted.Operations);
            }
            Assert.Equal("delete i7|null|null", _plain.Operations.Last());
        }

        [Theory]
        [InlineData(3)]
        [InlineData(80)]
        public async Task StructOnlyDuplicateKeysSelectThePlainSinksLowestRow(int count)
        {
            await Start();
            var header = StructHeader.Create("number", "text");
            var rows = Enumerable.Range(0, count)
                .Select(i => ((long)i, 7L, (long)(count - i),
                    (IDataValue)new StructValue(header, new Int64Value(i / 2), new StringValue(i % 2 == 0 ? "a" : "z"))))
                .ToList();
            await Store(rows);

            await Send(1, rows);

            Assert.Equal($"upsert i7|Struct|i{count}", _plain!.Operations.Last());
            Assert.Equal(_plain.Operations, _hosted!.Operations);

            await Send(-1, [rows[0]]);

            Assert.Equal($"upsert i7|Struct|i{count - 1}", _plain.Operations.Last());
            Assert.Equal(_plain.Operations, _hosted.Operations);
        }

        private async Task SendWeighted(IReadOnlyList<(int weight, (long id, long key, long number, IDataValue value) row)> rows)
        {
            var hosted = Columns(4);
            var plain = Columns(3);
            foreach (var (_, row) in rows)
            {
                hosted[0].Add(new Int64Value(0));
                hosted[1].Add(new Int64Value(row.key));
                hosted[2].Add(new Int64Value(row.number));
                hosted[3].Add(new RowRefValue(_references[row.id]));
                IDataValue[] source = [new Int64Value(row.id), new Int64Value(row.key), new Int64Value(row.number), row.value];
                for (int c = 0; c < plain.Length; c++)
                {
                    plain[c].Add(source[_plainEmit[c]]);
                }
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
        [InlineData(-2)]
        [InlineData(-1)]
        [InlineData(0)]
        [InlineData(1)]
        [InlineData(4)]
        [InlineData(8)]
        [InlineData(11)]
        [InlineData(13)]
        [InlineData(15)]
        [InlineData(17)]
        [InlineData(19)]
        [InlineData(21)]
        public async Task OfALargeKeysRowsOfEveryTypeTheHostedSinkWritesWhatThePlainSinkWrites(int survivor)
        {
            await Start();
            var values = Values();
            var originals = new List<(long id, long key, long number, IDataValue value)>();
            var copies = new List<(long id, long key, long number, IDataValue value)>();
            long id = 0;
            foreach (var key in new long[] { 5, 6 })
            {
                for (int v = 0; v < 3; v++)
                {
                    originals.Add((id, key, 100 - id, values[(v * 7 + (int)key) % values.Length]));
                    id++;
                }
            }
            for (int v = 0; v < values.Length; v++)
            {
                for (int c = 0; c < 4; c++)
                {
                    long number = 1000 - id;
                    originals.Add((id, 7, number, values[v]));
                    if (survivor != -2 && v != survivor)
                    {
                        copies.Add((id + 5000, 7, number, values[v]));
                    }
                    id++;
                }
            }
            for (int v = 0; v < 3; v++)
            {
                originals.Add((id, 8, 100 - v, values[(v * 5) % values.Length]));
                id++;
            }
            await Store(originals.Concat(copies).ToList());

            await SendWeighted(originals.Select(x => (1, x)).Concat(copies.Select(x => (-1, x))).ToList());

            Assert.Equal(_plain!.Operations, _hosted!.Operations);
            if (survivor == -1)
            {
                Assert.Contains("delete i7|null|null", _plain.Operations);
            }
        }

        [Fact]
        public async Task ADeleteCarriesItsKeyWhereTheWriteHasIt()
        {
            _fetchEmit = [4, 2, 1];
            _plainEmit = [3, 2, 1];
            _plainKey = 2;
            await Start();
            var rows = new List<(long, long, long, IDataValue)>()
            {
                (0, 7, 1, new StringValue("x")),
                (1, 7, 2, new StringValue("w")),
                (2, 8, 3, new StringValue("y"))
            };
            await Store(rows);

            await Send(1, rows);
            Assert.Equal(_plain!.Operations, _hosted!.Operations);

            await Send(-1, rows.Where(x => x.Item2 == 7).ToList());
            Assert.Equal("delete null|null|i7", _plain.Operations.Last());
            Assert.Equal(_plain.Operations, _hosted.Operations);
        }
    }
}
