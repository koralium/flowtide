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
using FlowtideDotNet.Base.Vertices;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Core.Operators.Join.MergeJoin;
using FlowtideDotNet.Core.Operators.Set;
using FlowtideDotNet.Core.Operators.Set.Structs;
using FlowtideDotNet.Core.Operators.Write;
using FlowtideDotNet.Core.Operators.Write.Column;
using FlowtideDotNet.Storage;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Persistence.CacheStorage;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.Tree;
using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Type;
using System.Reflection;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Core.Tests.Operators
{
    public class ZeroWeightTests : OperatorTestBase, IDisposable
    {
        private readonly BufferBlock<IStreamEvent> _outputBlock = new BufferBlock<IStreamEvent>();
        private readonly string _directory = $"./data/zeroWeightTests/{Guid.NewGuid():N}";
        private FileCachePersistentStorage? _storage;
        private long _checkpoint;

        protected override StateManagerOptions CreateStateManagerOptions()
        {
            _storage = new FileCachePersistentStorage(new FileCacheOptions() { DirectoryPath = $"{_directory}/persistent" });
            return new StateManagerOptions()
            {
                PersistentStorage = _storage,
                TemporaryStorageOptions = new FileCacheOptions() { DirectoryPath = $"{_directory}/tmp" }
            };
        }

        void IDisposable.Dispose()
        {
            Dispose();
            _storage?.Dispose();
            try
            {
                Directory.Delete(_directory, true);
            }
            catch (IOException)
            {
                // A file may still be closing
            }
        }

        private sealed class TestJoin : ColumnBulkMergeJoin
        {
            public TestJoin(MergeJoinRelation relation, FlowtideDotNet.Core.Compute.FunctionsRegister register)
                : base(relation, register, new ExecutionDataflowBlockOptions() { BoundedCapacity = 100, MaxDegreeOfParallelism = 1 })
            {
            }

            public Task<int> LeftRows => Count(_leftTree!);

            public Task<int> RightRows => Count(_rightTree!);
        }

        private sealed class TestSink : ColumnGroupedWriteOperator
        {
            private readonly EventBatchData? _existing;

            public TestSink(WriteRelation relation, EventBatchData? existing)
                : base(ExecutionMode.OnCheckpoint, relation, new ExecutionDataflowBlockOptions() { BoundedCapacity = 100, MaxDegreeOfParallelism = 1 })
            {
                _existing = existing;
            }

            protected override bool FetchExistingData => _existing != null;

            protected override async IAsyncEnumerable<EventBatchData> GetExistingData()
            {
                await Task.CompletedTask;
                yield return _existing!;
            }

            public List<string> Uploads { get; } = new List<string>();

            public override string DisplayName => "test sink";

            protected override void Checkpoint(long checkpointTime)
            {
            }

            protected override ValueTask<IReadOnlyList<int>> GetPrimaryKeyColumns()
            {
                return ValueTask.FromResult<IReadOnlyList<int>>(new List<int>() { 0 });
            }

            protected override async Task UploadChanges(IAsyncEnumerable<ColumnWriteOperation> rows, Watermark watermark, bool isInitialData, CancellationToken cancellationToken)
            {
                await foreach (var row in rows)
                {
                    var key = Text(row.EventBatchData.Columns[0], row.Index);
                    Uploads.Add(row.IsDeleted ? $"delete {key}" : $"upsert {key}|{Text(row.EventBatchData.Columns[1], row.Index)}");
                }
            }
        }

        private static async Task<int> Count<V, TValues>(IBPlusTree<ColumnRowReference, V, ColumnKeyStorageContainer, TValues> tree)
            where TValues : IValueContainer<V>
        {
            using var iterator = tree.CreateIterator();
            await iterator.SeekFirst();
            int count = 0;
            await foreach (var page in iterator)
            {
                foreach (var row in page)
                {
                    count++;
                }
            }
            return count;
        }

        private static T Field<T>(object target, Type type, string name)
        {
            return (T)type.GetField(name, BindingFlags.NonPublic | BindingFlags.Instance)!.GetValue(target)!;
        }

        private static Task<int> SetRows(MultipleInputVertex<StreamEventBatch> op)
        {
            return Count(Field<IBPlusTree<ColumnRowReference, InputWeights2, ColumnKeyStorageContainer, PrimitiveListValueContainer<InputWeights2>>>(op, op.GetType(), "_tree"));
        }

        private static Task<int> SinkRows(TestSink sink)
        {
            return Count(Field<IBPlusTree<ColumnRowReference, int, ColumnKeyStorageContainer, ListValueContainer<int>>>(sink, typeof(ColumnGroupedWriteOperator), "m_tree"));
        }

        private static ReadRelation Read(string name)
        {
            return new ReadRelation()
            {
                NamedTable = new NamedTable() { Names = new List<string>() { name } },
                BaseSchema = new NamedStruct()
                {
                    Names = new List<string>() { "k", "v" },
                    Struct = new Struct() { Types = new List<SubstraitBaseType>() { new AnyType(), new AnyType() } }
                }
            };
        }

        private static FieldReference Column(int field)
        {
            return new DirectFieldReference() { ReferenceSegment = new StructReferenceSegment() { Field = field } };
        }

        private static (int weight, long key, string value) Row(int weight, long key, string value = "v")
        {
            return (weight, key, value);
        }

        private static EventBatchData Batch(params (int weight, long key, string value)[] rows)
        {
            var allocator = GlobalMemoryManager.Instance;
            var keys = FlowtideDotNet.Core.ColumnStore.Column.Create(allocator);
            var values = FlowtideDotNet.Core.ColumnStore.Column.Create(allocator);
            foreach (var row in rows)
            {
                keys.Add(new Int64Value(row.key));
                values.Add(new StringValue(row.value));
            }
            return new EventBatchData(new IColumn[] { keys, values });
        }

        private static StreamMessage<StreamEventBatch> Message(params (int weight, long key, string value)[] rows)
        {
            var allocator = GlobalMemoryManager.Instance;
            var weights = new PrimitiveList<int>(allocator);
            var iterations = new PrimitiveList<uint>(allocator);
            foreach (var row in rows)
            {
                weights.Add(row.weight);
                iterations.Add(0);
            }
            var batch = new StreamEventBatch(new EventBatchWeighted(weights, iterations, Batch(rows)));
            // The operator returns the sender's rent
            batch.Data.Rent(1);
            return new StreamMessage<StreamEventBatch>(batch, 1);
        }

        private static string Text(IColumn column, int index)
        {
            var value = column.GetValueAt(index, default);
            if (value.IsNull)
            {
                return "NULL";
            }
            return value.Type == ArrowTypeId.Int64 ? value.AsLong.ToString() : value.AsString.ToString();
        }

        // Every row that came out, with its weight
        private async Task<List<string>> Drain(MultipleInputVertex<StreamEventBatch> op)
        {
            _checkpoint++;
            foreach (var target in op.Targets)
            {
                await target.SendAsync(new Checkpoint(_checkpoint, _checkpoint + 1, 1));
            }
            var rows = new List<string>();
            while (true)
            {
                var receive = _outputBlock.ReceiveAsync(TimeSpan.FromSeconds(30));
                if (await Task.WhenAny(receive, op.Completion) != receive)
                {
                    await op.Completion;
                    throw new InvalidOperationException("The operator completed before its checkpoint came out");
                }
                var received = await receive;
                if (received is ICheckpointEvent)
                {
                    rows.Sort(StringComparer.Ordinal);
                    return rows;
                }
                if (received is StreamMessage<StreamEventBatch> message)
                {
                    var data = message.Data.Data;
                    for (int i = 0; i < data.Count; i++)
                    {
                        var columns = data.EventBatchData.Columns;
                        rows.Add($"{data.Weights.Get(i)}: {string.Join("|", Enumerable.Range(0, columns.Count).Select(c => Text(columns[c], i)))}");
                    }
                    data.Return();
                }
            }
        }

        private async Task<TestJoin> StartJoin(JoinType type = JoinType.Inner)
        {
            var op = new TestJoin(new MergeJoinRelation()
            {
                Type = type,
                Left = Read("a"),
                Right = Read("b"),
                LeftKeys = new List<FieldReference>() { Column(0) },
                RightKeys = new List<FieldReference>() { Column(2) }
            }, FunctionsRegister);
            op.LinkTo(_outputBlock);
            await InitializeOperator(op);
            return op;
        }

        private async Task<MultipleInputVertex<StreamEventBatch>> StartSet(SetOperation operation)
        {
            var op = ColumnSetOperatorFactory.CreateColumnSetOperator(new SetRelation()
            {
                Inputs = new List<Relation>() { Read("a"), Read("b") },
                Operation = operation
            }, new ExecutionDataflowBlockOptions() { BoundedCapacity = 100, MaxDegreeOfParallelism = 1 });
            op.LinkTo(_outputBlock);
            await InitializeOperator(op);
            return op;
        }

        private async Task<TestSink> StartSink(EventBatchData? existing = null)
        {
            var sink = new TestSink(new WriteRelation()
            {
                Input = Read("a"),
                NamedObject = new NamedTable() { Names = new List<string>() { "out" } },
                TableSchema = new NamedStruct() { Names = new List<string>() { "k", "v" } }
            }, existing);
            await InitializeOperator(sink);
            return sink;
        }

        private async Task<List<string>> Send(TestSink sink, params (int weight, long key, string value)[] rows)
        {
            sink.Uploads.Clear();
            await sink.SendAsync(Message(rows));
            _checkpoint++;
            await SendCheckpoint(sink, _checkpoint);
            var done = EgressWaitForCheckpointDone();
            if (await Task.WhenAny(done, sink.Completion) != done)
            {
                await sink.Completion;
                throw new InvalidOperationException("The sink completed before its checkpoint was done");
            }
            sink.Uploads.Sort(StringComparer.Ordinal);
            return sink.Uploads.ToList();
        }

        [Theory]
        [InlineData(JoinType.Inner)]
        [InlineData(JoinType.Left)]
        [InlineData(JoinType.Outer)]
        public async Task JoinDoesNotStoreARowWithoutWeight(JoinType type)
        {
            var op = await StartJoin(type);

            await op.Targets[0].SendAsync(Message(Row(0, 1), Row(0, 2)));
            await op.Targets[1].SendAsync(Message(Row(0, 2), Row(0, 3)));
            await Drain(op);

            Assert.Equal(0, await op.LeftRows);
            Assert.Equal(0, await op.RightRows);

            await op.Targets[1].SendAsync(Message(Row(1, 1, "r")));
            var output = await Drain(op);

            Assert.Equal(type == JoinType.Outer ? ["1: NULL|NULL|1|r"] : [], output);
            Assert.Equal(1, await op.RightRows);
        }

        [Fact]
        public async Task JoinKeepsSignedRowsAndForgetsCancelledOnes()
        {
            var op = await StartJoin();

            await op.Targets[0].SendAsync(Message(Row(1, 1), Row(-1, 1), Row(-1, 2)));
            await Drain(op);
            Assert.Equal(1, await op.LeftRows);

            await op.Targets[1].SendAsync(Message(Row(1, 1, "r"), Row(1, 2, "r")));
            Assert.Equal(["-1: 2|v|2|r"], await Drain(op));

            await op.Targets[0].SendAsync(Message(Row(1, 2)));
            Assert.Equal(["1: 2|v|2|r"], await Drain(op));
            Assert.Equal(0, await op.LeftRows);
        }

        [Fact]
        public async Task JoinRowComesBackAfterItWasRemoved()
        {
            var op = await StartJoin();
            await op.Targets[1].SendAsync(Message(Row(1, 1, "r")));
            await Drain(op);

            await op.Targets[0].SendAsync(Message(Row(1, 1)));
            Assert.Equal(["1: 1|v|1|r"], await Drain(op));
            await op.Targets[0].SendAsync(Message(Row(-1, 1)));
            Assert.Equal(["-1: 1|v|1|r"], await Drain(op));
            await op.Targets[0].SendAsync(Message(Row(0, 1)));
            await Drain(op);
            Assert.Equal(0, await op.LeftRows);

            await op.Targets[0].SendAsync(Message(Row(1, 1)));
            Assert.Equal(["1: 1|v|1|r"], await Drain(op));
            Assert.Equal(1, await op.LeftRows);
        }

        // Equal rows keep their order in a small batch
        [Fact]
        public async Task JoinDoesNotBringBackARowRemovedInTheSameBatch()
        {
            var op = await StartJoin();
            await op.Targets[0].SendAsync(Message(Row(1, 1)));
            await Drain(op);

            await op.Targets[0].SendAsync(Message(Row(-1, 1), Row(0, 1), Row(1, 2), Row(-1, 2), Row(0, 2)));
            await Drain(op);
            Assert.Equal(0, await op.LeftRows);

            await op.Targets[1].SendAsync(Message(Row(1, 1, "r"), Row(1, 2, "r")));
            Assert.Equal([], await Drain(op));
        }

        [Fact]
        public async Task JoinStateDoesNotGrowFromRowsWithoutWeight()
        {
            var op = await StartJoin();
            await op.Targets[0].SendAsync(Message(Row(1, -1)));
            await op.Targets[1].SendAsync(Message(Row(1, -1, "r")));
            await Drain(op);

            for (int batch = 0; batch < 10; batch++)
            {
                var rows = Enumerable.Range(batch * 200, 200).Select(x => Row(0, x)).ToArray();
                await op.Targets[batch % 2].SendAsync(Message(rows));
            }
            await op.Targets[0].SendAsync(Message(Row(0, -1)));
            await Drain(op);

            Assert.Equal(1, await op.LeftRows);
            Assert.Equal(1, await op.RightRows);
        }

        [Theory]
        [InlineData(SetOperation.UnionDistinct)]
        [InlineData(SetOperation.MinusPrimary)]
        [InlineData(SetOperation.IntersectionMultiset)]
        public async Task SetDoesNotStoreARowWithoutWeight(SetOperation operation)
        {
            var op = await StartSet(operation);

            await op.Targets[0].SendAsync(Message(Enumerable.Range(0, 500).Select(x => Row(0, x)).ToArray()));
            await op.Targets[1].SendAsync(Message(Enumerable.Range(250, 500).Select(x => Row(0, x)).ToArray()));

            Assert.Equal([], await Drain(op));
            Assert.Equal(0, await SetRows(op));
        }

        [Fact]
        public async Task SetKeepsARowThatOnlyTheOtherInputHas()
        {
            var op = await StartSet(SetOperation.MinusPrimary);

            await op.Targets[1].SendAsync(Message(Row(1, 1)));
            Assert.Equal([], await Drain(op));
            Assert.Equal(1, await SetRows(op));

            await op.Targets[0].SendAsync(Message(Row(1, 1), Row(0, 2)));
            Assert.Equal([], await Drain(op));
            await op.Targets[1].SendAsync(Message(Row(-1, 1)));
            Assert.Equal(["1: 1|v"], await Drain(op));
            Assert.Equal(1, await SetRows(op));

            await op.Targets[0].SendAsync(Message(Row(-1, 1)));
            Assert.Equal(["-1: 1|v"], await Drain(op));
            Assert.Equal(0, await SetRows(op));
        }

        // Equal rows keep their order in a small batch
        [Fact]
        public async Task SetDoesNotBringBackARowRemovedInTheSameBatch()
        {
            var op = await StartSet(SetOperation.UnionDistinct);
            await op.Targets[0].SendAsync(Message(Row(1, 1)));
            Assert.Equal(["1: 1|v"], await Drain(op));

            await op.Targets[0].SendAsync(Message(Row(-1, 1), Row(0, 1), Row(1, 2), Row(-1, 2), Row(0, 2)));
            Assert.Equal(["-1: 1|v", "-1: 2|v", "1: 2|v"], await Drain(op));
            Assert.Equal(0, await SetRows(op));
        }

        [Fact]
        public async Task SetKeepsARowThatStartsWithARetraction()
        {
            var op = await StartSet(SetOperation.MinusPrimary);

            await op.Targets[0].SendAsync(Message(Row(-1, 1)));
            Assert.Equal(["-1: 1|v"], await Drain(op));
            Assert.Equal(1, await SetRows(op));

            await op.Targets[0].SendAsync(Message(Row(0, 1)));
            Assert.Equal([], await Drain(op));
            Assert.Equal(1, await SetRows(op));

            await op.Targets[0].SendAsync(Message(Row(1, 1)));
            Assert.Equal(["1: 1|v"], await Drain(op));
            Assert.Equal(0, await SetRows(op));

            await op.Targets[0].SendAsync(Message(Row(0, 1)));
            Assert.Equal([], await Drain(op));
            Assert.Equal(0, await SetRows(op));

            await op.Targets[0].SendAsync(Message(Row(1, 1)));
            Assert.Equal(["1: 1|v"], await Drain(op));
            Assert.Equal(1, await SetRows(op));

            await op.Targets[0].SendAsync(Message(Row(1, 2), Row(-1, 2), Row(0, 2)));
            Assert.Equal(["-1: 2|v", "1: 2|v"], await Drain(op));
            Assert.Equal(1, await SetRows(op));
        }

        [Fact]
        public async Task SinkDoesNotUploadARowWithoutWeight()
        {
            var sink = await StartSink();

            Assert.Equal([], await Send(sink, Row(0, 1), Row(0, 2)));
            Assert.Equal(0, await SinkRows(sink));

            Assert.Equal(["upsert 1|a"], await Send(sink, Row(1, 1, "a"), Row(0, 1, "b")));
            Assert.Equal(1, await SinkRows(sink));
        }

        [Fact]
        public async Task SinkSendsNothingForARowWithoutWeightOnAStoredKey()
        {
            var sink = await StartSink();
            Assert.Equal(["upsert 1|a"], await Send(sink, Row(1, 1, "a")));

            Assert.Equal([], await Send(sink, Row(0, 1, "a"), Row(0, 1, "b"), Row(0, 2, "c")));
            Assert.Equal(1, await SinkRows(sink));
        }

        [Fact]
        public async Task SinkRowComesBackAfterItWasRemoved()
        {
            var sink = await StartSink();
            Assert.Equal(["upsert 1|a"], await Send(sink, Row(1, 1, "a")));
            Assert.Equal(["delete 1"], await Send(sink, Row(-1, 1, "a")));
            Assert.Equal([], await Send(sink, Row(0, 1, "a")));
            Assert.Equal(0, await SinkRows(sink));

            await Send(sink, Row(1, 2, "b"), Row(-1, 2, "b"));
            Assert.Equal(["upsert 1|a"], await Send(sink, Row(1, 1, "a")));
            Assert.Equal(1, await SinkRows(sink));
        }

        [Fact]
        public async Task SinkDeletesAnExistingKeyThatOnlyCameWithoutWeight()
        {
            var sink = await StartSink(Batch(Row(1, 1, "old"), Row(1, 2, "keep")));

            Assert.Equal(["delete 1"], await Send(sink, Row(0, 1, "x"), Row(1, 2, "keep")));
            Assert.Equal(1, await SinkRows(sink));
        }
    }
}
