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

using FlowtideDotNet.Substrait;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Type;

namespace FlowtideDotNet.Core.Tests.LateMaterialization.Differential
{
    public class DifferentialRestartTests
    {
        private static Plan Plan(bool hosted)
        {
            Relation input = new ReadRelation
            {
                NamedTable = new NamedTable { Names = ["t"] },
                BaseSchema = new NamedStruct
                {
                    Names = ["k", "text"],
                    Struct = new Struct { Types = [new AnyType(), new AnyType()] }
                },
                RowStore = hosted ? new RowStoreDefinition { Name = "t", StoredColumns = [1] } : null
            };
            if (hosted)
            {
                input = new FetchRequiredColumnsRelation
                {
                    Input = input,
                    Fetches = [new RowStoreFetch { StoreName = "t", ReferenceColumn = 1, Columns = [0] }],
                    Emit = [0, 2]
                };
            }
            return new Plan
            {
                Relations = [new WriteRelation
                {
                    Input = input,
                    NamedObject = new NamedTable { Names = ["output"] },
                    TableSchema = new NamedStruct { Names = ["k", "text"] }
                }]
            };
        }

        private static DifferentialTable[] Tables()
        {
            var table = new DifferentialTable("t", 2);
            table.Load([1L, "a"]);
            return [table];
        }

        [Fact]
        public async Task ExpectAfterRestartWithoutChangesMustSaveACheckpoint()
        {
            await using var pair = new DifferentialPair(Plan, Tables);
            await pair.StartAsync();
            await pair.Expect("1|a");
            await pair.RestartAsync();
            pair.SettleTimeout = TimeSpan.FromSeconds(3);

            await pair.Expect("1|a");

            Assert.True(pair.Hosted.Covered);
            Assert.True(pair.Plain.Covered);
        }

        [Fact]
        public async Task ExpectWaitsWhileAnOutputNeutralChangeIsDequeued()
        {
            await using var pair = new DifferentialPair(Plan, Tables);
            await pair.StartAsync();
            await pair.Expect("1|a");
            var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var hostedEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var plainEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            pair.Hosted.Tables["t"].AfterDequeue = () =>
            {
                hostedEntered.TrySetResult();
                return release.Task;
            };
            pair.Plain.Tables["t"].AfterDequeue = () =>
            {
                plainEntered.TrySetResult();
                return release.Task;
            };
            Task? triggers = null;
            Task? expectation = null;
            try
            {
                pair.Change(tables => tables["t"].Upsert([1L, "a"]));
                triggers = Task.WhenAll(pair.Hosted.TriggerAsync(), pair.Plain.TriggerAsync());
                await Task.WhenAll(hostedEntered.Task, plainEntered.Task).WaitAsync(TimeSpan.FromSeconds(30));
                Assert.Empty(pair.Hosted.Tables["t"].Deltas);
                Assert.Empty(pair.Plain.Tables["t"].Deltas);

                expectation = pair.Expect("1|a");

                Assert.False(expectation.IsCompleted, "Expect accepted a dequeued change before its coverage marker was published.");
            }
            finally
            {
                pair.Hosted.Tables["t"].AfterDequeue = null;
                pair.Plain.Tables["t"].AfterDequeue = null;
                release.TrySetResult();
                if (triggers != null) await triggers.WaitAsync(TimeSpan.FromSeconds(30));
                if (expectation != null) await expectation.WaitAsync(TimeSpan.FromSeconds(30));
            }
        }
    }
}
