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

using FlowtideDotNet.Core.Operators.Join.MultiJoin;
using FlowtideDotNet.Core.Optimizer;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Sql;

namespace FlowtideDotNet.Core.Tests.MultiJoin
{
    public class MultiJoinOuterThenInnerTests
    {
        private static int Groups(string sql)
        {
            var builder = new SqlPlanBuilder();
            builder.Sql(MultiJoinPlanBuilderTests.Tables + sql);
            var plan = PlanOptimizer.Optimize(builder.GetPlan(), new PlanOptimizerSettings() { MultiJoin = true });
            return MultiJoinMarker.Count(plan);
        }

        [Fact]
        public void AnInnerJoinKeyedOnThePreservedSideOfALeftJoinIsGrouped()
        {
            Assert.Equal(1, Groups(@"
                INSERT INTO output
                SELECT o.orderkey, u.name, p.name
                FROM orders o
                LEFT JOIN users u ON o.userkey = u.userkey
                INNER JOIN products p ON o.productkey = p.productkey;"));
        }

        [Theory]
        // Keyed on the padded side of a left join below
        [InlineData("INSERT INTO output SELECT o.orderkey, u.name, c.name FROM orders o LEFT JOIN users u ON o.userkey = u.userkey INNER JOIN companies c ON u.companyid = c.companyid;")]
        // Keyed on either side of a full outer join below
        [InlineData("INSERT INTO output SELECT o.orderkey, u.name, c.name FROM orders o FULL OUTER JOIN users u ON o.userkey = u.userkey INNER JOIN companies c ON u.companyid = c.companyid;")]
        [InlineData("INSERT INTO output SELECT o.orderkey, u.name, p.name FROM orders o FULL OUTER JOIN users u ON o.userkey = u.userkey INNER JOIN products p ON o.productkey = p.productkey;")]
        public void AnInnerJoinKeyedOnAPaddedSideIsGrouped(string sql)
        {
            // The key's not-null filter the optimizer leaves above the outer join must not end the group
            Assert.Equal(1, Groups(sql));
        }

        [Fact]
        public void AFilterWithAnotherConditionAboveALeftJoinEndsTheGroup()
        {
            Assert.Equal(0, Groups(@"
                INSERT INTO output
                SELECT o.orderkey, u.name, c.name
                FROM orders o
                LEFT JOIN users u ON o.userkey = u.userkey
                INNER JOIN companies c ON u.companyid = c.companyid AND u.active = 1;"));
        }

        private const string FourTables = @"
            INSERT INTO output
            SELECT o.orderkey, u.name, c.name, p.name
            FROM orders o
            LEFT JOIN users u ON o.userkey = u.userkey
            INNER JOIN companies c ON u.companyid = c.companyid
            INNER JOIN products p ON o.productkey = p.productkey;";

        [Fact]
        public void TheJoinBelowTheFilterIsPartOfTheGroup()
        {
            var plan = MultiJoinPlanBuilderTests.Optimized(FourTables);
            var group = MultiJoinPlanBuilder.Discover(MultiJoinPlanBuilderTests.RootJoin(plan), 5)!;
            Assert.Equal(4, group.Inputs.Count);
            Assert.DoesNotContain(group.Inputs, x => x.Relation is FilterRelation filter && filter.Input is MergeJoinRelation);
        }

        [Fact]
        public void AJoinOverTheCapKeepsItsFilter()
        {
            var plan = MultiJoinPlanBuilderTests.Optimized(FourTables);
            var group = MultiJoinPlanBuilder.Discover(MultiJoinPlanBuilderTests.RootJoin(plan), 3)!;
            Assert.Equal(3, group.Inputs.Count);
            Assert.Single(group.Inputs, x => x.Relation is FilterRelation filter && filter.Input is MergeJoinRelation);
        }
    }
}
