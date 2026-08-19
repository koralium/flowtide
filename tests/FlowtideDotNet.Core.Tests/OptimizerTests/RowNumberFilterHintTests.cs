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
            Assert.True(rowNumberFunction.Options.ContainsKey("emit_only_within_max_row_number"));
        }

        [Fact]
        public void FilterIsKeptAboveTheWindowRelation()
        {
            // The filter carries the query's semantics through re-optimization; emission suppression
            // means it only sees rows within the bound, but it must stay in the plan.
            var plan = BuildOptimizedPlan(@"
            INSERT INTO output
            SELECT userkey, companyid
            FROM users
            WHERE ROW_NUMBER() OVER (PARTITION BY companyid ORDER BY userkey) = 1");

            bool ContainsFilterAboveWindow(Relation relation)
            {
                if (relation is FilterRelation filterRelation)
                {
                    return filterRelation.Input is ConsistentPartitionWindowRelation || ContainsFilterAboveWindow(filterRelation.Input);
                }
                if (relation is ProjectRelation projectRelation)
                {
                    return ContainsFilterAboveWindow(projectRelation.Input);
                }
                if (relation is WriteRelation writeRelation)
                {
                    return ContainsFilterAboveWindow(writeRelation.Input);
                }
                if (relation is RootRelation rootRelation)
                {
                    return ContainsFilterAboveWindow(rootRelation.Input);
                }
                return false;
            }

            Assert.True(ContainsFilterAboveWindow(plan.Relations[plan.Relations.Count - 1]));
        }

        [Fact]
        public void StaleEmitOptionIsRemovedWhenNoFilterExists()
        {
            var plan = BuildOptimizedPlan(@"
            INSERT INTO output
            SELECT userkey, companyid, ROW_NUMBER() OVER (PARTITION BY companyid ORDER BY userkey)
            FROM users");

            var windowRelation = FindWindowRelation(plan.Relations[plan.Relations.Count - 1]);
            var rowNumberFunction = Assert.Single(windowRelation.WindowFunctions);
            rowNumberFunction.Options = new SortedList<string, string>
            {
                { "max_row_number", "1" },
                { "emit_only_within_max_row_number", "true" }
            };

            plan = PlanOptimizer.Optimize(plan);

            windowRelation = FindWindowRelation(plan.Relations[plan.Relations.Count - 1]);
            rowNumberFunction = Assert.Single(windowRelation.WindowFunctions);
            Assert.True(rowNumberFunction.Options == null || !rowNumberFunction.Options.ContainsKey("max_row_number"));
            Assert.True(rowNumberFunction.Options == null || !rowNumberFunction.Options.ContainsKey("emit_only_within_max_row_number"));
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

        [Fact]
        public void TwoUpperBoundConditionsKeepTheSmallest()
        {
            var plan = BuildOptimizedPlan(@"
            INSERT INTO output
            SELECT userkey, companyid
            FROM users
            WHERE ROW_NUMBER() OVER (PARTITION BY companyid ORDER BY userkey) <= 5
              AND ROW_NUMBER() OVER (PARTITION BY companyid ORDER BY userkey) <= 3");

            var windowRelation = FindWindowRelation(plan.Relations[plan.Relations.Count - 1]);
            foreach (var windowFunction in windowRelation.WindowFunctions)
            {
                Assert.NotNull(windowFunction.Options);
                Assert.Equal("3", windowFunction.Options["max_row_number"]);
            }
        }

        [Fact]
        public void StaleSmallerHintIsReplacedOnReoptimize()
        {
            // A stored plan can carry a hint from an earlier optimization. When the filter has been
            // relaxed the smaller stale hint must not survive, it would silently drop rows.
            var plan = BuildOptimizedPlan(@"
            INSERT INTO output
            SELECT userkey, companyid
            FROM users
            WHERE ROW_NUMBER() OVER (PARTITION BY companyid ORDER BY userkey) <= 5");

            var windowRelation = FindWindowRelation(plan.Relations[plan.Relations.Count - 1]);
            var rowNumberFunction = Assert.Single(windowRelation.WindowFunctions);
            rowNumberFunction.Options!["max_row_number"] = "1";

            plan = PlanOptimizer.Optimize(plan);

            windowRelation = FindWindowRelation(plan.Relations[plan.Relations.Count - 1]);
            rowNumberFunction = Assert.Single(windowRelation.WindowFunctions);
            Assert.NotNull(rowNumberFunction.Options);
            Assert.Equal("5", rowNumberFunction.Options["max_row_number"]);
        }

        [Fact]
        public void StaleHintIsRemovedWhenNoFilterExists()
        {
            // When the filter is gone the hint must go too, row numbers past the bound become null.
            var plan = BuildOptimizedPlan(@"
            INSERT INTO output
            SELECT userkey, companyid, ROW_NUMBER() OVER (PARTITION BY companyid ORDER BY userkey)
            FROM users");

            var windowRelation = FindWindowRelation(plan.Relations[plan.Relations.Count - 1]);
            var rowNumberFunction = Assert.Single(windowRelation.WindowFunctions);
            rowNumberFunction.Options = new SortedList<string, string>
            {
                { "max_row_number", "1" }
            };

            plan = PlanOptimizer.Optimize(plan);

            windowRelation = FindWindowRelation(plan.Relations[plan.Relations.Count - 1]);
            rowNumberFunction = Assert.Single(windowRelation.WindowFunctions);
            Assert.True(rowNumberFunction.Options == null || !rowNumberFunction.Options.ContainsKey("max_row_number"));
        }

        [Fact]
        public void ReoptimizeKeepsTheSameHint()
        {
            var plan = BuildOptimizedPlan(@"
            INSERT INTO output
            SELECT userkey, companyid
            FROM users
            WHERE ROW_NUMBER() OVER (PARTITION BY companyid ORDER BY userkey) <= 3");

            plan = PlanOptimizer.Optimize(plan);

            var windowRelation = FindWindowRelation(plan.Relations[plan.Relations.Count - 1]);
            var rowNumberFunction = Assert.Single(windowRelation.WindowFunctions);
            Assert.NotNull(rowNumberFunction.Options);
            Assert.Equal("3", rowNumberFunction.Options["max_row_number"]);
        }
    }
}
