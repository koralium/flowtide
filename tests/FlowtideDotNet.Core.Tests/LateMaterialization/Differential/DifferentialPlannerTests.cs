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
using FlowtideDotNet.Core.Operators.Write;
using FlowtideDotNet.Substrait.Relations;
using static FlowtideDotNet.Core.Tests.LateMaterialization.Differential.DifferentialCarrierTests;

namespace FlowtideDotNet.Core.Tests.LateMaterialization.Differential
{
    public class DifferentialPlannerTests
    {
        [Fact]
        public async Task AFilterAndAProjectGetAStoreAndAFetch()
        {
            await using var pair = DifferentialPair.Planned(() => FilterAndProject(false), () => [Table("t", Row(1, 5, "a", "x"), Row(2, 20, "b", null), Row(3, 30, null, "z"))]);
            await pair.StartAsync();
            await pair.Expect("2|20|b|null|false", "3|30|null|z|true");
            Assert.Equal(2, Count(pair.Hosted, "Fetch required columns"));
            Assert.Equal(0, Count(pair.Plain, "Fetch required columns"));

            pair.Change(tables => tables["t"].Upsert(Row(1, 40, "a2", "x"), Row(2, 20, "b2", "y")));
            await pair.Expect("1|40|a2|x|true", "2|20|b2|y|false", "3|30|null|z|true");

            pair.Change(tables => tables["t"].Upsert(Row(3, 7, "gone", "z")));
            pair.Change(tables => tables["t"].Delete(2));
            await pair.Expect("1|40|a2|x|true");

            await pair.RestartAsync();
            pair.Change(tables => tables["t"].Upsert(Row(4, 11, "d", "w"), Row(1, 41, "a3", "x")));
            await pair.Expect("1|41|a3|x|true", "4|11|d|w|false");
        }

        [Theory]
        [InlineData(JoinType.Inner)]
        [InlineData(JoinType.Left)]
        [InlineData(JoinType.Right)]
        [InlineData(JoinType.Outer)]
        public async Task AMergeJoinGetsItsKeysFetchedBelowAndTheRestAbove(JoinType type)
        {
            await using var pair = DifferentialPair.Planned(
                () => Join(false, type),
                () =>
                [
                    Table("l", Row(1, 10, "l1", "a"), Row(2, 20, "l2", null), Row(3, 30, "l3", "c")),
                    Table("r", Row(10, 1, "r10", "x"), Row(20, 2, "r20", "y"), Row(40, 4, "r40", null))
                ]);
            await pair.StartAsync();
            await pair.SettleBoth();
            Assert.Equal(pair.Plain.Result(), pair.Hosted.Result());
            Assert.Equal(2, Count(pair.Hosted, "Fetch required columns"));

            pair.Change(tables => tables["l"].Upsert(Row(2, 40, "l2b", "b"), Row(4, 10, "l4", null)));
            pair.Change(tables => tables["r"].Delete(10));
            await pair.SettleBoth();
            Assert.Equal(pair.Plain.Result(), pair.Hosted.Result());

            await pair.RestartAsync();
            pair.Change(tables => tables["r"].Upsert(Row(10, 9, "r10b", "z"), Row(30, 3, "r30", null)));
            pair.Change(tables => tables["l"].Delete(1));
            await pair.SettleBoth();
            Assert.Equal(pair.Plain.Result(), pair.Hosted.Result());
            Assert.NotEmpty(pair.Hosted.Result());
        }

        [Theory]
        [InlineData(JoinType.Inner)]
        [InlineData(JoinType.Left)]
        public async Task AMultiJoinStaysOneOperatorWithStoresUnderIt(JoinType second)
        {
            await using var pair = DifferentialPair.Planned(
                () => ThreeTables(false, second),
                () =>
                [
                    Table("a", Row(1, 10, "a1", "x"), Row(2, 20, "a2", null), Row(3, 10, null, "z")),
                    Table("b", Row(10, 100, "b10", "p"), Row(20, 200, "b20", "q")),
                    Table("c", Row(100, 0, "c100", null))
                ]);
            await pair.StartAsync();
            var rows = new List<string>() { "1|10|a1|x|10|100|b10|p|100|0|c100|null", "3|10|null|z|10|100|b10|p|100|0|c100|null" };
            if (second == JoinType.Left)
            {
                rows.Add("2|20|a2|null|20|200|b20|q|null|null|null|null");
            }
            await pair.Expect(rows.ToArray());
            Assert.Equal(1, Count(pair.Hosted, "Multi Join"));
            Assert.Equal(1, Count(pair.Plain, "Multi Join"));
            Assert.Equal(3, Count(pair.Hosted, "Fetch required columns"));

            pair.Change(tables => tables["c"].Upsert(Row(200, 1, "c200", "w"), Row(100, 0, "c100b", null)));
            pair.Change(tables => tables["a"].Delete(3));
            pair.Change(tables => tables["b"].Upsert(Row(10, 100, "b10b", null)));
            await pair.Expect("1|10|a1|x|10|100|b10b|null|100|0|c100b|null", "2|20|a2|null|20|200|b20|q|200|1|c200|w");

            await pair.RestartAsync();
            pair.Change(tables => tables["b"].Delete(20));
            pair.Change(tables => tables["a"].Upsert(Row(4, 10, "a4", "v")));
            await pair.Expect("1|10|a1|x|10|100|b10b|null|100|0|c100b|null", "4|10|a4|v|10|100|b10b|null|100|0|c100b|null");
        }

        [Fact]
        public async Task TwoReadersOfOneRelationShareItsStore()
        {
            await using var pair = DifferentialPair.Planned(() => UnionOfTwoFilters(false), () => [Table("t", Row(1, 5, "a", "x"), Row(2, 20, "b", null), Row(3, 30, null, "z"))]);
            await pair.StartAsync();
            await pair.Expect("1|5|a|x", "2|20|b|null", "2|20|b|null", "3|30|null|z");

            pair.Change(tables => tables["t"].Upsert(Row(1, 15, "a2", "x"), Row(3, 31, "c", null)));
            pair.Change(tables => tables["t"].Delete(2));
            await pair.Expect("1|15|a2|x", "1|15|a2|x", "3|31|c|null");

            await pair.RestartAsync();
            pair.Change(tables => tables["t"].Upsert(Row(1, 40, "a3", null)));
            await pair.Expect("1|40|a3|null", "3|31|c|null");
        }

        [Fact]
        public async Task ARowFunctionGetsEveryStoredColumnFetchedBeforeIt()
        {
            await using var pair = DifferentialPair.Planned(() => RowFunctionFilter(false, true), () => [Table("t", Row(1, 5, "a", "x"), Row(2, 20, "b", null), Row(3, 30, null, "z"))], RegisterRowsOnly);
            await pair.StartAsync();
            await pair.Expect("1|5|a|x", "3|30|null|z");

            pair.Change(tables => tables["t"].Upsert(Row(5, 50, "e", "v"), Row(1, 6, "a2", "x")));
            pair.Change(tables => tables["t"].Delete(3));
            await pair.Expect("1|6|a2|x", "5|50|e|v");
        }

        [Theory]
        [InlineData(ExecutionMode.OnCheckpoint)]
        [InlineData(ExecutionMode.OnWatermark)]
        public async Task AKeyedSinkFetchesWhatThePlannerLeftStored(ExecutionMode mode)
        {
            await using var pair = DifferentialPair.Planned(
                () => Join(false, JoinType.Inner),
                () =>
                [
                    Table("l", Row(1, 10, "l1", "a"), Row(2, 20, "l2", null), Row(3, 30, "l3", "c")),
                    Table("r", Row(10, 1, "r10", "x"), Row(20, 2, "r20", "y"), Row(30, 4, "r30", null))
                ],
                null,
                () => new DifferentialDestination() { Mode = mode });
            await pair.StartAsync();
            await pair.Expect("1|10|l1|a|10|1|r10|x", "2|20|l2|null|20|2|r20|y", "3|30|l3|c|30|4|r30|null");
            Assert.Equal(1, Count(pair.Hosted, "Fetch required columns"));

            pair.Change(tables => tables["l"].Upsert(Row(2, 20, "l2b", "b")));
            pair.Change(tables => tables["r"].Delete(10));
            await pair.Expect("2|20|l2b|b|20|2|r20|y", "3|30|l3|c|30|4|r30|null");

            await pair.RestartAsync();
            pair.Change(tables => tables["r"].Upsert(Row(10, 9, "r10b", "z")));
            await pair.Expect("1|10|l1|a|10|9|r10b|z", "2|20|l2b|b|20|2|r20|y", "3|30|l3|c|30|4|r30|null");
        }

        [Theory]
        [InlineData(1, 1)]
        [InlineData(2, 1)]
        [InlineData(3, 3)]
        [InlineData(4, 3)]
        public async Task RandomChangesGiveTheSameRowsWithThePlannerOnAndOff(int seed, int partitions)
        {
            var random = new Random(seed);
            object?[] Random(long key)
            {
                return Row(key, random.Next(4) == 0 ? null : random.Next(6), random.Next(5) == 0 ? null : "t" + random.Next(1000), random.Next(3) == 0 ? null : new string('n', random.Next(1, 40)));
            }
            var left = Enumerable.Range(0, 30).Select(x => Random(x)).ToArray();
            var right = Enumerable.Range(0, 4).Select(x => Random(x)).ToArray();
            await using var pair = DifferentialPair.Planned(
                () => partitions == 1 ? Join(false, JoinType.Left) : Core.Optimizer.MergeJoinParallelize.ParallelizeOptimizer.Optimize(Join(false, JoinType.Left), partitions),
                () => [Table("l", left), Table("r", right)]);
            await pair.StartAsync();
            await pair.Expect(LeftJoined(pair.Plain.Tables));
            Assert.NotEqual(0, Count(pair.Hosted, "Fetch required columns"));

            for (int round = 0; round < 12; round++)
            {
                var upserts = Enumerable.Range(0, random.Next(1, 6)).Select(x => Random(random.Next(45))).DistinctBy(x => x[0]).ToArray();
                var deletes = Enumerable.Range(0, random.Next(0, 4)).Select(x => (long)random.Next(45)).Distinct().ToArray();
                var rightUpserts = Enumerable.Range(0, random.Next(0, 3)).Select(x => Random(random.Next(6))).DistinctBy(x => x[0]).ToArray();
                var rightDeletes = Enumerable.Range(0, random.Next(0, 2)).Select(x => (long)random.Next(6)).ToArray();
                pair.Change(tables =>
                {
                    tables["l"].Upsert(upserts);
                    tables["l"].Delete(deletes);
                    tables["r"].Upsert(rightUpserts);
                    tables["r"].Delete(rightDeletes);
                });
                await pair.Expect(LeftJoined(pair.Plain.Tables));
                if (round == 5)
                {
                    await pair.RestartAsync();
                }
            }
        }
    }
}
