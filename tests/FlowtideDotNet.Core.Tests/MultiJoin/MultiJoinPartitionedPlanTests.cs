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
using FlowtideDotNet.Substrait;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Sql;

namespace FlowtideDotNet.Core.Tests.MultiJoin
{
    public class MultiJoinPartitionedPlanTests
    {
        private sealed class ShapeVisitor : OptimizerBaseVisitor
        {
            public List<MultiJoinPlan> Groups { get; } = new();
            public int OrdinaryJoins { get; private set; }
            public int Exchanges { get; private set; }

            public override Relation VisitMergeJoinRelation(MergeJoinRelation relation, object state)
            {
                if (relation.MultiJoinGroup != null)
                {
                    var group = MultiJoinPlanBuilder.Build(relation);
                    Groups.Add(group);
                    foreach (var input in group.Inputs)
                    {
                        Visit(input.Relation, state);
                    }
                    return relation;
                }
                OrdinaryJoins++;
                return base.VisitMergeJoinRelation(relation, state);
            }

            public override Relation VisitExchangeRelation(ExchangeRelation relation, object state)
            {
                Exchanges++;
                return base.VisitExchangeRelation(relation, state);
            }
        }

        private static ShapeVisitor Optimize(string sql, int maxInputs)
        {
            var builder = new SqlPlanBuilder();
            builder.Sql(@"
                CREATE TABLE a (k any, v any);
                CREATE TABLE b (k any, v any);
                CREATE TABLE c (k any, v any);
                CREATE TABLE d (k any, v any);
                CREATE TABLE e (k any, v any);
                CREATE TABLE f (k any, v any);" + sql);
            Plan plan = PlanOptimizer.Optimize(builder.GetPlan(), new PlanOptimizerSettings()
            {
                MultiJoin = true,
                MultiJoinMaxInputs = maxInputs,
                Parallelization = 4
            });
            var shape = new ShapeVisitor();
            shape.VisitPlan(plan);
            return shape;
        }

        [Fact]
        public void CappedGroupsKeepTheirBoundariesAboveAPartitionedJoin()
        {
            var shape = Optimize(@"
                INSERT INTO output
                SELECT ta.v, tb.v, tc.v, td.v, te.v, tf.v
                FROM a ta
                INNER JOIN b tb ON ta.k = tb.k
                INNER JOIN c tc ON tb.k = tc.k
                INNER JOIN d td ON tc.k = td.k
                INNER JOIN e te ON td.k = te.k
                INNER JOIN f tf ON te.k = tf.k;", 3);

            Assert.Equal(2, shape.Groups.Count);
            Assert.All(shape.Groups, group => Assert.Equal(3, group.Inputs.Count));
            Assert.All(shape.Groups, group => Assert.Equal(2, group.Joins.Count));
            Assert.Equal(2, shape.Groups.Select(group => group.Joins[0].MultiJoinGroup).Distinct().Count());
            Assert.Equal(4, shape.OrdinaryJoins);
            Assert.Equal(2, shape.Exchanges);
            Assert.Contains(shape.Groups, group => group.Inputs.Any(input => input.Relation is SetRelation));
        }

        [Fact]
        public void AGroupBelowAPartitionedJoinIsBuiltFromItsUnchangedMarks()
        {
            var shape = Optimize(@"
                INSERT INTO output
                SELECT ta.v, tb.v, tc.v, td.v
                FROM a ta
                INNER JOIN b tb ON ta.k = tb.k
                INNER JOIN c tc ON tb.k = tc.k
                INNER JOIN d td ON ta.k = td.k AND tb.v = td.v;", 3);

            var group = Assert.Single(shape.Groups);
            Assert.Equal(3, group.Inputs.Count);
            Assert.Equal(2, group.Joins.Count);
            Assert.Equal(4, shape.OrdinaryJoins);
            Assert.Equal(2, shape.Exchanges);
        }
    }
}
