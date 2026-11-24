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

using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Core.Operators.Normalization;
using FlowtideDotNet.Core.Operators.Read;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Substrait;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Type;
using System.Reflection;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Core.Tests.LateMaterialization.Differential
{
    public class DifferentialOracleTests
    {
        private static ReadRelation Read(bool hosted)
        {
            return new ReadRelation
            {
                NamedTable = new NamedTable { Names = ["t"] },
                BaseSchema = new NamedStruct
                {
                    Names = ["k", "n", "text", "note"],
                    Struct = new Struct { Types = [new AnyType(), new AnyType(), new AnyType(), new AnyType()] }
                },
                RowStore = hosted ? new RowStoreDefinition { Name = "t", StoredColumns = [2, 3] } : null
            };
        }

        private static Plan Plan(bool hosted)
        {
            Relation input = Read(hosted);
            if (hosted)
            {
                input = new FetchRequiredColumnsRelation
                {
                    Input = input,
                    Fetches = [new RowStoreFetch { StoreName = "t", ReferenceColumn = 2, Columns = [0, 1] }],
                    Emit = [0, 1, 3, 4]
                };
            }
            return new Plan
            {
                Relations = [new WriteRelation
                {
                    Input = input,
                    NamedObject = new NamedTable { Names = ["output"] },
                    TableSchema = new NamedStruct { Names = ["k", "n", "text", "note"] }
                }]
            };
        }

        private static DifferentialTable[] Tables()
        {
            var table = new DifferentialTable("t", 4);
            table.Load([1L, 2L, "a", "b"]);
            return [table];
        }

        private static async Task SendNegativeRow(DifferentialSink sink)
        {
            var allocator = GlobalMemoryManager.Instance;
            var columns = Enumerable.Range(0, 4).Select(_ => (IColumn)Column.Create(allocator)).ToArray();
            columns[0].Add(new Int64Value(99));
            columns[1].Add(new Int64Value(2));
            columns[2].Add(new StringValue("a"));
            columns[3].Add(new StringValue("ghost"));
            var weights = new PrimitiveList<int>(allocator);
            weights.Add(-1);
            var iterations = new PrimitiveList<uint>(allocator);
            iterations.Add(0);
            var batch = new EventBatchWeighted(weights, iterations, new EventBatchData(columns));
            batch.Rent(1);
            try
            {
                var receive = typeof(DifferentialSink).GetMethod("OnRecieve", BindingFlags.NonPublic | BindingFlags.Instance)!;
                await (Task)receive.Invoke(sink, [new StreamEventBatch(batch), 0L])!;
            }
            finally
            {
                batch.Return();
            }
        }

        [Fact]
        public async Task ExpectRejectsAnExtraNegativeRowWhenThePositiveRowsMatch()
        {
            await using var pair = new DifferentialPair(Plan, Tables);
            await pair.StartAsync();
            await pair.Expect("1|2|a|b");
            await SendNegativeRow(pair.Hosted.Sink);
            Assert.Equal(["1|2|a|b"], pair.Hosted.Result());
            pair.SettleTimeout = TimeSpan.FromMilliseconds(100);

            var error = await Assert.ThrowsAnyAsync<Exception>(() => pair.Expect("1|2|a|b"));

            Assert.Contains("below zero [99|2|a|ghost (weight -1)]", error.Message);
        }

        [Fact]
        public async Task DisposePreservesTheOriginalFailureAndCompleteInnerMessages()
        {
            var detail = new string('x', 200) + " points at no page";
            async Task Fail()
            {
                await using var pair = new DifferentialPair(Plan, Tables);
                pair.Hosted.Failures.Enqueue(new AggregateException(
                    new AggregateException(new InvalidOperationException(detail)),
                    new ArgumentException("second failure")));
                await pair.Expect("1|2|a|b");
            }

            var error = await Assert.ThrowsAnyAsync<Exception>(Fail);

            Assert.StartsWith("The hosted stream failed:", error.Message);
            Assert.Contains("InvalidOperationException: " + detail, error.Message);
            Assert.Contains("ArgumentException: second failure", error.Message);
        }

        private sealed class OrdinarySource() : ColumnBatchReadBaseOperator(Read(false), new FunctionsRegister(), new DataflowBlockOptions())
        {
            public override string DisplayName => "ordinary source";
            public override Task Compact() => Task.CompletedTask;
            protected override Task Checkpoint(long checkpointTime) => Task.CompletedTask;
            protected override Task<IReadOnlySet<string>> GetWatermarkNames() => Task.FromResult<IReadOnlySet<string>>(new HashSet<string> { "t" });
            protected override ValueTask<List<int>> GetPrimaryKeyColumns() => ValueTask.FromResult(new List<int> { 0 });
            protected override IAsyncEnumerable<ColumnReadEvent> FullLoad(CancellationToken cancellationToken, CancellationToken enumeratorCancellationToken = default) => throw new NotSupportedException();
            protected override IAsyncEnumerable<DeltaReadEvent> DeltaLoad(Func<Task> enter, Action exit, CancellationToken cancellationToken, CancellationToken enumeratorCancellationToken = default) => throw new NotSupportedException();
        }

        [Fact]
        public async Task OrdinaryReadDoesNotAllocateHeapOptions()
        {
            await using var source = new OrdinarySource();

            Assert.Null(source.RowHeapOptions);
        }

        [Fact]
        public async Task OrdinaryNormalizationDoesNotAllocateHeapOptions()
        {
            await using var normalization = new ColumnNormalizationOperator(
                new NormalizationRelation { Input = Read(false), KeyIndex = [0] },
                new FunctionsRegister(), new ExecutionDataflowBlockOptions());

            Assert.Null(normalization.RowHeapOptions);
        }
    }
}
