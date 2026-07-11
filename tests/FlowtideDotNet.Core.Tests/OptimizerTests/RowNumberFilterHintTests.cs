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

using FlowtideDotNet.Core.Optimizer;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Sql;
using FlowtideDotNet.Substrait.Type;

namespace FlowtideDotNet.Core.Tests.OptimizerTests
{
    public class RowNumberFilterHintTests
    {
        private static Substrait.Plan BuildOptimizedPlan(string sql)
        {
            var builder = new SqlPlanBuilder();
            builder.AddTableDefinition("users", new NamedStruct()
            {
                Names = new List<string>() { "userkey", "companyid", "value" },
                Struct = new Struct()
                {
                    Types = new List<SubstraitBaseType>() { new AnyType(), new AnyType(), new AnyType() }
                }
            });
            builder.Sql(sql);
            var plan = builder.GetPlan();
            return PlanOptimizer.Optimize(plan);
        }

        private static ConsistentPartitionWindowRelation FindWindowRelation(Relation relation)
        {
            if (relation is ConsistentPartitionWindowRelation windowRelation)
            {
                return windowRelation;
            }
            if (relation is FilterRelation filterRelation)
            {
                return FindWindowRelation(filterRelation.Input);
            }
            if (relation is ProjectRelation projectRelation)
            {
                return FindWindowRelation(projectRelation.Input);
            }
            if (relation is WriteRelation writeRelation)
            {
                return FindWindowRelation(writeRelation.Input);
            }
            if (relation is RootRelation rootRelation)
            {
                return FindWindowRelation(rootRelation.Input);
            }
            throw new InvalidOperationException($"No window relation found under {relation.GetType().Name}");
        }

        [Fact]
        public void EqualsOneSetsHint()
        {
            var plan = BuildOptimizedPlan(@"
            INSERT INTO output
            SELECT userkey, companyid
            FROM users
            WHERE ROW_NUMBER() OVER (PARTITION BY companyid ORDER BY userkey) = 1");

            var windowRelation = FindWindowRelation(plan.Relations[plan.Relations.Count - 1]);
            var rowNumberFunction = Assert.Single(windowRelation.WindowFunctions);
            Assert.NotNull(rowNumberFunction.Options);
            Assert.Equal("1", rowNumberFunction.Options["max_row_number"]);
        }

        [Fact]
        public void LessThanOrEqualSetsHint()
        {
            var plan = BuildOptimizedPlan(@"
            INSERT INTO output
            SELECT userkey, companyid
            FROM users
            WHERE ROW_NUMBER() OVER (PARTITION BY companyid ORDER BY userkey) <= 3");

            var windowRelation = FindWindowRelation(plan.Relations[plan.Relations.Count - 1]);
            var rowNumberFunction = Assert.Single(windowRelation.WindowFunctions);
            Assert.NotNull(rowNumberFunction.Options);
            Assert.Equal("3", rowNumberFunction.Options["max_row_number"]);
        }

        [Fact]
        public void LessThanSetsHintMinusOne()
        {
            var plan = BuildOptimizedPlan(@"
            INSERT INTO output
            SELECT userkey, companyid
            FROM users
            WHERE ROW_NUMBER() OVER (PARTITION BY companyid ORDER BY userkey) < 3");

            var windowRelation = FindWindowRelation(plan.Relations[plan.Relations.Count - 1]);
            var rowNumberFunction = Assert.Single(windowRelation.WindowFunctions);
            Assert.NotNull(rowNumberFunction.Options);
            Assert.Equal("2", rowNumberFunction.Options["max_row_number"]);
        }

        [Fact]
        public void ModuloConditionDoesNotSetHint()
        {
            var plan = BuildOptimizedPlan(@"
            INSERT INTO output
            SELECT userkey, companyid
            FROM users
            WHERE ROW_NUMBER() OVER (PARTITION BY companyid ORDER BY userkey) % 2 = 0");

            var windowRelation = FindWindowRelation(plan.Relations[plan.Relations.Count - 1]);
            var rowNumberFunction = Assert.Single(windowRelation.WindowFunctions);
            Assert.True(rowNumberFunction.Options == null || !rowNumberFunction.Options.ContainsKey("max_row_number"));
        }

        [Fact]
        public void LowerBoundConditionDoesNotSetHint()
        {
            var plan = BuildOptimizedPlan(@"
            INSERT INTO output
            SELECT userkey, companyid
            FROM users
            WHERE ROW_NUMBER() OVER (PARTITION BY companyid ORDER BY userkey) >= 2");

            var windowRelation = FindWindowRelation(plan.Relations[plan.Relations.Count - 1]);
            var rowNumberFunction = Assert.Single(windowRelation.WindowFunctions);
            Assert.True(rowNumberFunction.Options == null || !rowNumberFunction.Options.ContainsKey("max_row_number"));
        }
    }
}
