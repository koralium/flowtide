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

namespace FlowtideDotNet.Core.Tests.Operators.Normalization
{
    public class HostedNormalizationTests : OperatorTestBase
    {
        protected override StateManagerOptions CreateStateManagerOptions()
        {
            return new StateManagerOptions()
            {
                PersistentStorage = new ReservoirPersistentStorage(new ReservoirStorageOptions() { FileProvider = new MemoryFileProvider() })
            };
        }

        private ColumnNormalizationOperator Operator(string? deferred)
        {
            var relation = new NormalizationRelation()
            {
                KeyIndex = new List<int>() { 0 },
                Input = new ReadRelation()
                {
                    NamedTable = new NamedTable() { Names = new List<string>() { "t" } },
                    BaseSchema = new NamedStruct()
                    {
                        Names = new List<string>() { "k", "kept", "text" },
                        Struct = new Struct() { Types = new List<SubstraitBaseType>() { new AnyType(), new AnyType(), new AnyType() } }
                    }
                }
            };
            if (deferred != null)
            {
                relation.Hint.Optimizations.Properties[RowStoreHost.DeferredColumnsHint] = deferred;
            }
            return new ColumnNormalizationOperator(relation, FunctionsRegister, new ExecutionDataflowBlockOptions() { BoundedCapacity = 100, MaxDegreeOfParallelism = 1 });
        }

        private static StreamMessage<StreamEventBatch> Message(params (int weight, long key, long kept, string text)[] rows)
        {
            var allocator = GlobalMemoryManager.Instance;
            var columns = new IColumn[] { Column.Create(allocator), Column.Create(allocator), Column.Create(allocator) };
            var weights = new PrimitiveList<int>(allocator);
            var iterations = new PrimitiveList<uint>(allocator);
            foreach (var row in rows)
            {
                columns[0].Add(new Int64Value(row.key));
                columns[1].Add(new Int64Value(row.kept));
                columns[2].Add(new StringValue(row.text));
                weights.Add(row.weight);
                iterations.Add(0);
            }
            var batch = new StreamEventBatch(new EventBatchWeighted(weights, iterations, new EventBatchData(columns)));
            batch.Data.Rent(1);
            return new StreamMessage<StreamEventBatch>(batch, 1);
        }

        private static string Text(IColumn column, int index)
        {
            var value = column.GetValueAt(index, default);
            switch (value.Type)
            {
                case ArrowTypeId.RowRef:
                    return "ref";
                case ArrowTypeId.Int64:
                    return value.AsLong.ToString();
                default:
                    return value.AsString.ToString();
            }
        }

        // Rows until the checkpoint comes out, with their references
        private async Task<(List<string> rows, Dictionary<string, ulong> references)> Drain(ColumnNormalizationOperator op, BufferBlock<IStreamEvent> output, long checkpoint)
        {
            await op.SendAsync(new Checkpoint(checkpoint, checkpoint + 1, 1));
            var rows = new List<string>();
            var references = new Dictionary<string, ulong>();
            while (true)
            {
                var received = await output.ReceiveAsync(TimeSpan.FromSeconds(30));
                if (received is ICheckpointEvent)
                {
                    await op.BeforeSaveCheckpoint();
                    await SaveCheckpoint();
                    rows.Sort(StringComparer.Ordinal);
                    return (rows, references);
                }
                if (received is StreamMessage<StreamEventBatch> message)
                {
                    var data = message.Data.Data;
                    var columns = data.EventBatchData.Columns;
                    for (int i = 0; i < data.Count; i++)
                    {
                        var row = $"{data.Weights.Get(i)}: {string.Join("|", Enumerable.Range(0, columns.Count).Select(c => Text(columns[c], i)))}";
                        rows.Add(row);
                        var last = columns[columns.Count - 1].GetValueAt(i, default);
                        if (last.Type == ArrowTypeId.RowRef)
                        {
                            references[row] = (ulong)last.AsLong;
                        }
                    }
                    data.Return();
                }
            }
        }

        [Fact]
        public async Task AHostedNormalizationSendsKeptColumnsAndAReference()
        {
            var op = Operator("2");
            var output = new BufferBlock<IStreamEvent>();
            op.LinkTo(output);
            await InitializeOperator(op);

            await op.SendAsync(Message((1, 1, 10, "a"), (1, 2, 20, "b")));
            var (first, firstReferences) = await Drain(op, output, 1);

            Assert.Equal(["1: 1|10|ref", "1: 2|20|ref"], first);
            Assert.NotEqual(firstReferences["1: 1|10|ref"], firstReferences["1: 2|20|ref"]);

            await op.SendAsync(Message((1, 1, 10, "a"), (1, 2, 20, "changed")));
            var (second, secondReferences) = await Drain(op, output, 2);

            Assert.Equal(["-1: 2|20|ref", "1: 2|20|ref"], second);
            Assert.Equal(firstReferences["1: 2|20|ref"], secondReferences["-1: 2|20|ref"]);
            Assert.NotEqual(firstReferences["1: 2|20|ref"], secondReferences["1: 2|20|ref"]);

            await op.SendAsync(Message((1, 1, 11, "a"), (-1, 2, 0, "")));
            var (third, thirdReferences) = await Drain(op, output, 3);

            Assert.Equal(["-1: 1|10|ref", "-1: 2|20|ref", "1: 1|11|ref"], third);
            Assert.Equal(firstReferences["1: 1|10|ref"], thirdReferences["-1: 1|10|ref"]);
            Assert.Equal(firstReferences["1: 1|10|ref"], thirdReferences["1: 1|11|ref"]);
            Assert.Equal(secondReferences["1: 2|20|ref"], thirdReferences["-1: 2|20|ref"]);
        }

        [Theory]
        [InlineData("2", "-1: 1|10|ref")]
        [InlineData(null, "-1: 1|10|a")]
        public async Task ARowOfWeightZeroDeletesWithAndWithoutTheHint(string? deferred, string deleted)
        {
            var op = Operator(deferred);
            var output = new BufferBlock<IStreamEvent>();
            op.LinkTo(output);
            await InitializeOperator(op);

            await op.SendAsync(Message((1, 1, 10, "a"), (1, 2, 20, "b")));
            await Drain(op, output, 1);
            await op.SendAsync(Message((0, 1, 10, "a")));
            var (rows, _) = await Drain(op, output, 2);

            Assert.Equal([deleted], rows);
        }

        [Fact]
        public async Task WithoutTheHintTheOperatorSendsWholeRows()
        {
            var op = Operator(null);
            var output = new BufferBlock<IStreamEvent>();
            op.LinkTo(output);
            await InitializeOperator(op);

            await op.SendAsync(Message((1, 1, 10, "a")));
            var (rows, references) = await Drain(op, output, 1);

            Assert.Equal(["1: 1|10|a"], rows);
            Assert.Empty(references);
        }

        [Theory]
        [InlineData("0", "is a key column")]
        [InlineData("3", "is not an output column")]
        [InlineData("2,2", "is named twice")]
        public void AWrongHintIsRefused(string deferred, string message)
        {
            var exception = Assert.Throws<InvalidOperationException>(() => Operator(deferred));
            Assert.Contains(message, exception.Message);
        }
    }
}
