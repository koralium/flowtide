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
using FlowtideDotNet.Core.Operators.Join.MergeJoin;
using FlowtideDotNet.Core.Operators.Write;
using FlowtideDotNet.Core.Operators.Write.Column;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Type;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Core.Tests.Operators
{
    public class RowRefOperatorTests : OperatorTestBase
    {
        private sealed class TestSink : ColumnGroupedWriteOperator
        {
            public TestSink(WriteRelation relation)
                : base(ExecutionMode.OnCheckpoint, relation, new ExecutionDataflowBlockOptions() { BoundedCapacity = 100, MaxDegreeOfParallelism = 1 })
            {
            }

            public int NameReads;

            public override string DisplayName
            {
                get
                {
                    NameReads++;
                    return "test sink";
                }
            }

            protected override void Checkpoint(long checkpointTime)
            {
            }

            protected override ValueTask<IReadOnlyList<int>> GetPrimaryKeyColumns()
            {
                return ValueTask.FromResult<IReadOnlyList<int>>(new List<int>() { 0 });
            }

            protected override Task UploadChanges(IAsyncEnumerable<ColumnWriteOperation> rows, Watermark watermark, bool isInitialData, CancellationToken cancellationToken)
            {
                return Task.CompletedTask;
            }
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

        private static StreamMessage<StreamEventBatch> Message(params (int weight, long key, IDataValue value)[] rows)
        {
            var allocator = GlobalMemoryManager.Instance;
            var keys = Column.Create(allocator);
            var values = Column.Create(allocator);
            var weights = new PrimitiveList<int>(allocator);
            var iterations = new PrimitiveList<uint>(allocator);
            foreach (var row in rows)
            {
                keys.Add(new Int64Value(row.key));
                values.Add(row.value);
                weights.Add(row.weight);
                iterations.Add(0);
            }
            var batch = new StreamEventBatch(new EventBatchWeighted(weights, iterations, new EventBatchData(new IColumn[] { keys, values })));
            // The operator returns the sender's rent
            batch.Data.Rent(1);
            return new StreamMessage<StreamEventBatch>(batch, 1);
        }

        private static string Text(IColumn column, int index)
        {
            var value = column.GetValueAt(index, default);
            switch (value.Type)
            {
                case ArrowTypeId.Null:
                    return "NULL";
                case ArrowTypeId.RowRef:
                    return "ref " + (ulong)value.AsLong;
                case ArrowTypeId.Int64:
                    return value.AsLong.ToString();
                default:
                    return value.AsString.ToString();
            }
        }

        private async Task<List<string>> Drain(ColumnBulkMergeJoin op, BufferBlock<IStreamEvent> output, long checkpoint)
        {
            foreach (var target in op.Targets)
            {
                await target.SendAsync(new Checkpoint(checkpoint, checkpoint + 1, 1));
            }
            var rows = new List<string>();
            while (true)
            {
                var receive = output.ReceiveAsync(TimeSpan.FromSeconds(30));
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

        [Theory]
        [InlineData(JoinType.Inner)]
        [InlineData(JoinType.Left)]
        public async Task AMergeJoinCarriesRowReferences(JoinType type)
        {
            var output = new BufferBlock<IStreamEvent>();
            var op = new ColumnBulkMergeJoin(new MergeJoinRelation()
            {
                Type = type,
                Left = Read("a"),
                Right = Read("b"),
                LeftKeys = new List<FieldReference>() { new DirectFieldReference() { ReferenceSegment = new StructReferenceSegment() { Field = 0 } } },
                RightKeys = new List<FieldReference>() { new DirectFieldReference() { ReferenceSegment = new StructReferenceSegment() { Field = 2 } } }
            }, FunctionsRegister, new ExecutionDataflowBlockOptions() { BoundedCapacity = 100, MaxDegreeOfParallelism = 1 });
            op.LinkTo(output);
            await InitializeOperator(op);
            ulong high = 1UL << 63;

            await op.Targets[0].SendAsync(Message((1, 1, new RowRefValue(high)), (1, 1, new RowRefValue(5)), (1, 2, new RowRefValue(7)), (1, 3, NullValue.Instance)));
            var padded = await Drain(op, output, 1);
            await op.Targets[1].SendAsync(Message((1, 1, new StringValue("r")), (1, 3, new RowRefValue(9))));
            var joined = await Drain(op, output, 2);
            await op.Targets[0].SendAsync(Message((-1, 1, new RowRefValue(high)), (-1, 2, new RowRefValue(7))));
            var removed = await Drain(op, output, 3);

            if (type == JoinType.Left)
            {
                Assert.Equal([$"1: 1|ref 5|NULL|NULL", $"1: 1|ref {high}|NULL|NULL", "1: 2|ref 7|NULL|NULL", "1: 3|NULL|NULL|NULL"], padded);
                Assert.Equal([$"-1: 1|ref 5|NULL|NULL", $"-1: 1|ref {high}|NULL|NULL", "-1: 3|NULL|NULL|NULL", "1: 1|ref 5|1|r", $"1: 1|ref {high}|1|r", "1: 3|NULL|3|ref 9"], joined);
                Assert.Equal([$"-1: 1|ref {high}|1|r", "-1: 2|ref 7|NULL|NULL"], removed);
            }
            else
            {
                Assert.Equal([], padded);
                Assert.Equal(["1: 1|ref 5|1|r", $"1: 1|ref {high}|1|r", "1: 3|NULL|3|ref 9"], joined);
                Assert.Equal([$"-1: 1|ref {high}|1|r"], removed);
            }
        }

        private static TestSink Sink()
        {
            return new TestSink(new WriteRelation()
            {
                Input = new ReadRelation()
                {
                    NamedTable = new NamedTable() { Names = new List<string>() { "a" } },
                    BaseSchema = new NamedStruct() { Names = new List<string>() { "k", "v" } }
                },
                NamedObject = new NamedTable() { Names = new List<string>() { "out" } },
                TableSchema = new NamedStruct() { Names = new List<string>() { "k", "v" } }
            });
        }

        [Fact]
        public async Task ASinkRefusesRowReferences()
        {
            var sink = Sink();
            await InitializeOperator(sink);

            await sink.SendAsync(Message((1, 1, new RowRefValue(7))));

            var error = await Assert.ThrowsAsync<InvalidOperationException>(() => sink.Completion.WaitAsync(TimeSpan.FromSeconds(30)));
            Assert.Contains("test sink received row references in column 1", error.Message);
        }

        [Fact]
        public void AReferenceInsideAnotherColumnIsRefused()
        {
            var sink = Sink();
            var holders = new IDataValue[]
            {
                new StructValue(StructHeader.Create("r"), new RowRefValue(7)),
                new ListValue(new RowRefValue(7)),
                new MapValue(new KeyValuePair<IDataValue, IDataValue>(new RowRefValue(7), new Int64Value(1))),
                new MapValue(new KeyValuePair<IDataValue, IDataValue>(new StringValue("k"), new ListValue(new RowRefValue(7))))
            };
            var columns = new List<IColumn>();
            foreach (var holder in holders)
            {
                var column = Column.Create(GlobalMemoryManager.Instance);
                column.Add(holder);
                columns.Add(column);
            }
            var union = Column.Create(GlobalMemoryManager.Instance);
            union.Add(new Int64Value(1));
            union.Add(new RowRefValue(7));
            columns.Add(union);
            using var offsets = new PrimitiveList<int>(GlobalMemoryManager.Instance);
            offsets.Add(0);
            var viewed = Column.Create(GlobalMemoryManager.Instance);
            viewed.Add(new StringValue("a"));
            viewed.Add(new RowRefValue(7));
            columns.Add(new ColumnWithOffset(viewed, offsets));
            var plain = Column.Create(GlobalMemoryManager.Instance);
            plain.Add(new StructValue(StructHeader.Create("a"), new ListValue(new Int64Value(1))));
            plain.Add(new StringValue("a"));
            using var clean = new EventBatchData([plain]);

            RowRefGuard.ThrowIfPresent(clean, sink);
            foreach (var column in columns)
            {
                using var batch = new EventBatchData([column]);
                var error = Assert.Throws<InvalidOperationException>(() => RowRefGuard.ThrowIfPresent(batch, sink));
                Assert.Contains("test sink received row references in column 0", error.Message);
            }
        }

        [Fact]
        public async Task ACleanBatchDoesNotReadTheSinkName()
        {
            var sink = Sink();
            await InitializeOperator(sink);
            var before = sink.NameReads;

            for (int i = 0; i < 5; i++)
            {
                await sink.SendAsync(Message((1, i, new Int64Value(i))));
            }
            await sink.SendAsync(Message((1, 9, new RowRefValue(7))));
            await Assert.ThrowsAsync<InvalidOperationException>(() => sink.Completion.WaitAsync(TimeSpan.FromSeconds(30)));

            Assert.Equal(1, sink.NameReads - before);
        }
    }
}
