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

using FlowtideDotNet.Base.Engine;
using FlowtideDotNet.Base.Engine.Internal.StateMachine;
using System.Collections.Concurrent;
using System.Reflection;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Substrait;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Type;

namespace FlowtideDotNet.Core.Tests.LateMaterialization.Differential
{
    [CollectionDefinition("Differential harness checkpoint gates", DisableParallelization = true)]
    public class DifferentialHarnessCheckpointCollection;

    [Collection("Differential harness checkpoint gates")]
    public class DifferentialHarnessTests
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

        [Fact]
        public async Task ResultsMustDistinguishNegativeWeightsFromStringPayloads()
        {
            await using var negative = new DifferentialStream(() => Plan(false), null, Tables());
            await using var positive = new DifferentialStream(() => Plan(false), null, Tables());
            await SendToSink(negative.Sink, -1, "b");
            await SendToSink(positive.Sink, 1, "b has weight -1");

            Assert.False(negative.Result().SequenceEqual(positive.Result()), "A negative row aliases a different positive row in the differential oracle.");
        }

        private static async Task SendToSink(DifferentialSink sink, int weight, string note)
        {
            var allocator = GlobalMemoryManager.Instance;
            var columns = Enumerable.Range(0, 4).Select(_ => (IColumn)Column.Create(allocator)).ToArray();
            columns[0].Add(new Int64Value(1));
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
        public async Task ExpectMustWaitForTheCheckpointCoveringAlreadyDeliveredChanges()
        {
            await using var pair = new DifferentialPair(Plan, Tables);
            await pair.StartAsync();
            await pair.Expect("1|2|a|b");
            var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var names = new ConcurrentDictionary<string, byte>();
            int hostedCompleted = pair.Hosted.Completed;
            int plainCompleted = pair.Plain.Completed;
            Task? expectation = null;
            StreamContext.CheckpointCommitScheduledHookForTests = name =>
            {
                names.TryAdd(name, 0);
                if (names.Count == 2) entered.TrySetResult();
                return release.Task;
            };
            try
            {
                pair.Change(tables => tables["t"].Upsert([1L, 2L, "changed", "b"]));
                await pair.Hosted.TriggerAsync();
                await pair.Plain.TriggerAsync();
                await entered.Task.WaitAsync(TimeSpan.FromSeconds(30));
                Assert.Empty(pair.Hosted.Tables["t"].Deltas);
                Assert.Empty(pair.Plain.Tables["t"].Deltas);
                Assert.Equal(["1|2|changed|b"], pair.Hosted.Result());
                Assert.Equal(["1|2|changed|b"], pair.Plain.Result());
                Assert.Equal(hostedCompleted, pair.Hosted.Completed);
                Assert.Equal(plainCompleted, pair.Plain.Completed);
                expectation = pair.Expect("1|2|changed|b");
                Assert.False(expectation.IsCompleted, "Expect returned before either covering checkpoint could collect or save.");
            }
            finally
            {
                StreamContext.CheckpointCommitScheduledHookForTests = null;
                release.TrySetResult();
                if (expectation != null) await expectation.WaitAsync(TimeSpan.FromSeconds(30));
            }
        }

        [Fact]
        public async Task ExpectMustDistinguishNullFromTheStringNull()
        {
            await using var pair = new DifferentialPair(Plan, Tables);
            pair.Hosted.Tables["t"].Rows[0][2] = "null";
            pair.Plain.Tables["t"].Rows[0][2] = null;
            pair.SettleTimeout = TimeSpan.FromSeconds(3);
            await pair.StartAsync();

            var error = await Assert.ThrowsAnyAsync<Exception>(() => pair.Expect("1|2|null|b"));

            Assert.Contains("The hosted stream did not settle", error.Message);
            Assert.Contains("rows [1|2|\"null\"|b]", error.Message);
        }

        [Fact]
        public async Task ExpectMustRejectFailedStreamsWhoseLastRowsMatch()
        {
            await using var pair = new DifferentialPair(Plan, Tables);
            await pair.StartAsync();
            await pair.Expect("1|2|a|b");
            pair.Hosted.Stream.Pause();
            pair.Plain.Stream.Pause();
            try
            {
                await pair.Hosted.Stream.InjectFailureForTests(new InvalidOperationException("Checkpoint collection failed."));
                await pair.Plain.Stream.InjectFailureForTests(new InvalidOperationException("Checkpoint collection failed."));
                Assert.Equal(StreamStateValue.Failure, pair.Hosted.Stream.State);
                Assert.Equal(StreamStateValue.Failure, pair.Plain.Stream.State);
                await Assert.ThrowsAnyAsync<Exception>(() => pair.Expect("1|2|a|b"));
            }
            finally
            {
                pair.Hosted.Stream.Resume();
                pair.Plain.Stream.Resume();
                pair.Hosted.Failures.Clear();
                pair.Plain.Failures.Clear();
            }
        }
    }
}
