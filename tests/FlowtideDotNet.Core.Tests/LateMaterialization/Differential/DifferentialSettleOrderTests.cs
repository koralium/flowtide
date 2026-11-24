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
using FlowtideDotNet.Substrait;
using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Type;

namespace FlowtideDotNet.Core.Tests.LateMaterialization.Differential
{
    [Collection(nameof(DifferentialSettleOrderTests))]
    [CollectionDefinition(nameof(DifferentialSettleOrderTests), DisableParallelization = true)]
    public class DifferentialSettleOrderTests
    {
        private static ReadRelation Read(string table, bool hosted)
        {
            return new ReadRelation()
            {
                NamedTable = new NamedTable() { Names = [table] },
                BaseSchema = new NamedStruct()
                {
                    Names = ["k", "n", "text", "note"],
                    Struct = new Struct() { Types = [new AnyType(), new AnyType(), new AnyType(), new AnyType()] }
                },
                RowStore = hosted ? new RowStoreDefinition() { Name = table, StoredColumns = [2, 3] } : null
            };
        }

        private static DirectFieldReference Field(int field)
        {
            return new DirectFieldReference() { ReferenceSegment = new StructReferenceSegment() { Field = field } };
        }

        // l joined with r on l.n = r.k
        private static Plan Join(bool hosted)
        {
            var left = Read("l", hosted);
            var join = new MergeJoinRelation()
            {
                Left = left,
                Right = Read("r", hosted),
                Type = JoinType.Inner,
                LeftKeys = [Field(1)],
                RightKeys = [Field(left.OutputLength)]
            };
            Relation input = join;
            if (hosted)
            {
                input = new FetchRequiredColumnsRelation()
                {
                    Input = join,
                    Fetches =
                    [
                        new RowStoreFetch() { ReferenceColumn = 2, StoreName = "l", Columns = [0, 1] },
                        new RowStoreFetch() { ReferenceColumn = 5, StoreName = "r", Columns = [0, 1] }
                    ],
                    Emit = [0, 1, 6, 7, 3, 4, 8, 9]
                };
            }
            return new Plan()
            {
                Relations =
                [
                    new WriteRelation()
                    {
                        Input = input,
                        NamedObject = new NamedTable() { Names = ["output"] },
                        TableSchema = new NamedStruct() { Names = Enumerable.Range(0, 8).Select(x => "c" + x).ToList() }
                    }
                ]
            };
        }

        private static DifferentialTable[] Tables()
        {
            var left = new DifferentialTable("l", 4);
            left.Load([1L, 10L, "l1", "a"]);
            var right = new DifferentialTable("r", 4);
            right.Load([10L, 1L, "r10", "x"]);
            return [left, right];
        }

        [Fact]
        public async Task ExpectWaitsForACheckpointSavedAfterEverySourceTookItsChange()
        {
            await using var pair = new DifferentialPair(Join, Tables);
            await pair.StartAsync();
            await pair.Expect("1|10|l1|a|10|1|r10|x");

            var hosted = pair.Hosted;
            int before = hosted.Completed;
            int taken = 0;
            var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            hosted.Tables["r"].BeforeTake = async () =>
            {
                hosted.Tables["r"].BeforeTake = null;
                while (hosted.Completed <= before)
                {
                    await Task.Delay(5);
                }
                Volatile.Write(ref taken, 1);
            };
            StreamContext.CheckpointCommitHookForTests = async (name, version) =>
            {
                if (name == hosted.Name && Volatile.Read(ref taken) == 1)
                {
                    await release.Task;
                }
            };
            Task expect;
            try
            {
                pair.Change(tables =>
                {
                    tables["l"].Upsert([2L, 10L, "l2", "b"]);
                    tables["r"].Upsert([10L, 1L, "r10b", "y"]);
                });
                expect = pair.Expect("1|10|l1|a|10|1|r10b|y", "2|10|l2|b|10|1|r10b|y");
                var first = await Task.WhenAny(expect, Task.Delay(TimeSpan.FromSeconds(3)));

                Assert.True(Volatile.Read(ref taken) == 1, "r took its change after the first checkpoint");
                Assert.False(first == expect, "Expect returned while the checkpoint after r's change was not saved");
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
