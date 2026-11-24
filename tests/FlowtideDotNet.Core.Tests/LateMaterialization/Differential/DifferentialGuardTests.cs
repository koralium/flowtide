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

using FlowtideDotNet.Base.Engine.Internal.StateMachine;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Substrait;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Type;
using System.Reflection;

namespace FlowtideDotNet.Core.Tests.LateMaterialization.Differential
{
    [CollectionDefinition(nameof(DifferentialGuardTests), DisableParallelization = true)]
    public class DifferentialGuardCollection;

    [Collection(nameof(DifferentialGuardTests))]
    public class DifferentialGuardTests
    {
        private static Plan Plan(bool hosted)
        {
            Relation input = new ReadRelation
            {
                NamedTable = new NamedTable { Names = ["t"] },
                BaseSchema = new NamedStruct
                {
                    Names = ["k", "n", "text", "note"],
                    Struct = new Struct { Types = [new AnyType(), new AnyType(), new AnyType(), new AnyType()] }
                },
                RowStore = hosted ? new RowStoreDefinition { Name = "t", StoredColumns = [2, 3] } : null
            };
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

        private static async Task SendToSink(DifferentialSink sink, int weight, string note)
        {
            var allocator = GlobalMemoryManager.Instance;
            var columns = Enumerable.Range(0, 4).Select(_ => (IColumn)Column.Create(allocator)).ToArray();
            columns[0].Add(new Int64Value(9));
            columns[1].Add(new Int64Value(2));
            columns[2].Add(new StringValue("a"));
            columns[3].Add(new StringValue(note));
            var weights = new PrimitiveList<int>(allocator);
            weights.Add(weight);
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
        public async Task ExpectMustRejectARowTheHostedSinkHoldsBelowZero()
        {
            await using var pair = new DifferentialPair(Plan, Tables);
            await pair.StartAsync();
            await pair.Expect("1|2|a|b");
            await SendToSink(pair.Hosted.Sink, -1, "ghost");
            pair.SettleTimeout = TimeSpan.FromSeconds(3);

            var error = await Assert.ThrowsAnyAsync<Exception>(() => pair.Expect("1|2|a|b"));

            Assert.Contains("9|2|a|ghost (weight -1)", error.Message);
        }
    
        [Fact]
        public async Task ExpectAfterARestartWaitsForACheckpointOfTheRestartedRun()
        {
            await using var pair = new DifferentialPair(Plan, Tables);
            await pair.StartAsync();
            await pair.Expect("1|2|a|b");
            pair.Change(tables => tables["t"].Upsert([1L, 2L, "a1", "b"]));
            await pair.Expect("1|2|a1|b");
            pair.Change(tables => tables["t"].Upsert([1L, 2L, "a2", "b"]));
            await pair.Expect("1|2|a2|b");
            await pair.RestartAsync();

            var hosted = pair.Hosted;
            var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            StreamContext.CheckpointCommitHookForTests = async (name, version) =>
            {
                if (name == hosted.Name)
                {
                    await release.Task;
                }
            };
            Task expect;
            try
            {
                pair.Change(tables => tables["t"].Upsert([1L, 2L, "a3", "b"]));
                expect = pair.Expect("1|2|a3|b");
                var first = await Task.WhenAny(expect, Task.Delay(TimeSpan.FromSeconds(3)));

                Assert.False(first == expect, "Expect returned before the restarted run saved a checkpoint");
            }
            finally
            {
                release.TrySetResult();
                StreamContext.CheckpointCommitHookForTests = null;
            }
            await expect;
        }
    }
}
