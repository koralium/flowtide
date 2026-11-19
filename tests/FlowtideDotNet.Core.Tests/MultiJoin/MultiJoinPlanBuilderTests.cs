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
using FlowtideDotNet.Core.Optimizer.EmitPushdown;
using FlowtideDotNet.Substrait;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Sql;

namespace FlowtideDotNet.Core.Tests.MultiJoin
{
    public class MultiJoinPlanBuilderTests
    {
        internal const string Tables = @"
            CREATE TABLE orders (orderkey any, userkey any, productkey any, amount any);
            CREATE TABLE users (userkey any, companyid any, name any, active any);
            CREATE TABLE companies (companyid any, name any);
            CREATE TABLE products (productkey any, name any);
            CREATE TABLE extra (a any, b any, v any);
        ";

        internal static Plan Optimized(string sql, string tables = Tables)
        {
            var builder = new SqlPlanBuilder();
            builder.Sql(tables + sql);
            // Without marks, the tests set the ones they need
            return PlanOptimizer.Optimize(builder.GetPlan(), new PlanOptimizerSettings() { MultiJoin = false });
        }

        private sealed class RootJoinFinder : OptimizerBaseVisitor
        {
            public MergeJoinRelation? Root;

            public override Relation VisitMergeJoinRelation(MergeJoinRelation mergeJoinRelation, object state)
            {
                Root ??= mergeJoinRelation;
                return mergeJoinRelation;
            }
        }

        internal static MergeJoinRelation RootJoin(Plan plan)
        {
            var finder = new RootJoinFinder();
            foreach (var relation in plan.Relations)
            {
                finder.Visit(relation, null!);
            }
            Assert.NotNull(finder.Root);
            return finder.Root!;
        }

        // The table at the bottom of an input, through filters and projections
        private static string Table(Relation relation)
        {
            return relation switch
            {
                ReadRelation read => read.NamedTable.DotSeperated,
                FilterRelation filter => Table(filter.Input),
                ProjectRelation project => Table(project.Input),
                _ => relation.GetType().Name
            };
        }

        // The inputs a condition reads, through the columns its field indexes address
        private static List<string> TablesRead(MultiJoinPlan plan, MultiJoinCheck check)
        {
            var usage = new ExpressionFieldUsageVisitor(int.MaxValue);
            usage.Visit(check.Condition, null);
            return usage.UsedFieldsLeft.Select(x => Table(plan.Inputs[check.Columns[x].Input].Relation)).Distinct().OrderBy(x => x).ToList();
        }

        private static MultiJoinInput Input(MultiJoinPlan plan, string table)
        {
            return plan.Inputs.Single(x => Table(x.Relation) == table);
        }

        private static (MultiJoinPlan? plan, List<MultiJoinLeftOut> leftOut) Discover(string sql, int maxInputs = 5)
        {
            var leftOut = new List<MultiJoinLeftOut>();
            var plan = MultiJoinPlanBuilder.Discover(RootJoin(Optimized(sql)), maxInputs, leftOut);
            return (plan, leftOut);
        }

        [Fact]
        public void InnerChainWithKeysOnDifferentTables()
        {
            var (plan, leftOut) = Discover(@"
                INSERT INTO output
                SELECT o.orderkey, u.name, c.name
                FROM orders o
                INNER JOIN users u ON o.userkey = u.userkey
                INNER JOIN companies c ON u.companyid = c.companyid;");

            Assert.NotNull(plan);
            Assert.Empty(leftOut);
            Assert.Equal(new[] { "orders", "users", "companies" }, plan.Inputs.Select(x => Table(x.Relation)));
            Assert.Equal(2, plan.InnerJoinCount);
            Assert.Equal(0, plan.LeftJoinCount);
            Assert.Equal(2, plan.Joins.Count);
            Assert.Equal(3, plan.Output.Count);

            // The second join is keyed from users, not from orders
            var users = Input(plan, "users");
            Assert.Same(users, plan.Inputs[plan.Root.LeftKey!.Input]);
            Assert.Same(Input(plan, "companies"), plan.Inputs[plan.Root.RightKey!.Input]);

            // Users is looked up by two different keys, the second one through a secondary index
            Assert.Equal(2, users.KeyLists.Count);
            Assert.Equal(0, users.FindKeyList(plan.Root.Left!.RightKey!.Columns));
            Assert.Equal(1, users.FindKeyList(plan.Root.LeftKey.Columns));
            var secondary = Assert.Single(users.SecondaryLayouts);
            Assert.Equal(new[] { users.KeyLists[1][0], users.KeyLists[0][0] }, secondary);
            Assert.Equal(new[] { users.KeyLists[0][0], users.KeyLists[1][0] }, users.PrimaryOrder);
            Assert.Equal(new[] { 1, 2 }, users.PrefixLengths);

            Assert.Single(Input(plan, "orders").KeyLists);
            Assert.Empty(Input(plan, "orders").SecondaryLayouts);
        }

        [Fact]
        public void StarKeepsOneKeyListPerDistinctKey()
        {
            var (plan, _) = Discover(@"
                INSERT INTO output
                SELECT o.orderkey, u.name, p.name, c.name
                FROM orders o
                INNER JOIN users u ON o.userkey = u.userkey
                INNER JOIN products p ON o.productkey = p.productkey
                INNER JOIN companies c ON u.companyid = c.companyid;");

            Assert.NotNull(plan);
            Assert.Equal(4, plan.Inputs.Count);
            Assert.Equal(2, Input(plan, "orders").KeyLists.Count);
            Assert.Equal(2, Input(plan, "users").KeyLists.Count);
            Assert.Single(Input(plan, "products").KeyLists);
            Assert.Equal(4, plan.Root.Leaves.Count);
        }

        [Fact]
        public void NeededColumnsAreEmittedAndKeyColumns()
        {
            var (plan, _) = Discover(@"
                INSERT INTO output
                SELECT o.orderkey, c.name
                FROM orders o
                INNER JOIN users u ON o.userkey = u.userkey
                INNER JOIN companies c ON u.companyid = c.companyid;");

            Assert.NotNull(plan);
            var users = Input(plan, "users");
            // Nothing of users is emitted, only its two keys are needed
            Assert.Equal(users.PrimaryOrder.OrderBy(x => x), users.NeededColumns.OrderBy(x => x));
            foreach (var column in plan.Output)
            {
                Assert.Contains(column.Column, plan.Inputs[column.Input].NeededColumns);
            }
        }

        [Fact]
        public void MixedInnerAndLeftJoins()
        {
            var (plan, _) = Discover(@"
                INSERT INTO output
                SELECT o.orderkey, u.name, c.name, p.name
                FROM orders o
                INNER JOIN users u ON o.userkey = u.userkey
                LEFT JOIN companies c ON u.companyid = c.companyid
                LEFT JOIN products p ON o.productkey = p.productkey;");

            Assert.NotNull(plan);
            Assert.Equal(1, plan.InnerJoinCount);
            Assert.Equal(2, plan.LeftJoinCount);
            Assert.True(plan.Root.PadRight);
            Assert.True(plan.Root.Left!.PadRight);
            Assert.False(plan.Root.Left.Left!.PadRight);
        }

        [Fact]
        public void RightAndFullOuterJoinsKeepTheirSides()
        {
            var (plan, leftOut) = Discover(@"
                INSERT INTO output
                SELECT o.orderkey, u.name, c.name, p.name
                FROM orders o
                INNER JOIN users u ON o.userkey = u.userkey
                RIGHT JOIN companies c ON u.companyid = c.companyid
                FULL OUTER JOIN products p ON o.productkey = p.productkey;");

            Assert.NotNull(plan);
            Assert.Empty(leftOut);
            Assert.Equal(4, plan.Inputs.Count);
            Assert.True(plan.Root.PadLeft && plan.Root.PadRight);
            Assert.True(plan.Root.Left!.PadLeft);
            Assert.False(plan.Root.Left.PadRight);
        }

        [Fact]
        public void MarkJoinGivesItsLeftSideAndAMark()
        {
            var (plan, leftOut) = Discover(@"
                INSERT INTO output
                SELECT o.orderkey, u.name
                FROM orders o
                INNER JOIN users u ON o.userkey = u.userkey
                WHERE EXISTS (SELECT 1 FROM companies c WHERE c.companyid = u.companyid);");

            Assert.NotNull(plan);
            Assert.Empty(leftOut);
            Assert.True(plan.Root.IsMark);
            Assert.Same(plan.Root, Assert.Single(plan.Marks));
            Assert.Equal("companies", Table(plan.Inputs[plan.Root.RightKey!.Input].Relation));
            // Nothing of the right side comes out, the mark is its only column
            Assert.DoesNotContain(plan.Output, x => !x.IsMark && x.Input == plan.Root.RightKey.Input);
            Assert.Contains(plan.Output, x => x.IsMark && x.Column == plan.Root.MarkId);
        }

        [Fact]
        public void RightSideConditionOfLeftJoinBecomesAnInputFilter()
        {
            var optimized = Optimized(@"
                INSERT INTO output
                SELECT o.orderkey, u.name, c.name
                FROM orders o
                LEFT JOIN users u ON o.userkey = u.userkey AND u.active = true
                LEFT JOIN companies c ON u.companyid = c.companyid;");
            var root = RootJoin(optimized);
            var lowerJoin = Assert.IsType<MergeJoinRelation>(root.Left);
            Assert.NotNull(lowerJoin.PostJoinFilter);
            var filterBefore = lowerJoin.PostJoinFilter.Clone();

            var plan = MultiJoinPlanBuilder.Discover(root, 5);

            Assert.NotNull(plan);
            Assert.Equal(2, plan.LeftJoinCount);
            var users = Input(plan, "users");
            Assert.IsType<FilterRelation>(users.Relation);
            Assert.Null(plan.Root.Left!.KeptOnlyCheck);
            Assert.Empty(plan.Root.Left.Checks);
            // The plan itself is untouched
            Assert.Equal(filterBefore, lowerJoin.PostJoinFilter);
            var filter = (FilterRelation)users.Relation;
            Assert.Same(lowerJoin.Right, filter.Input);

            // The original condition addresses the joined row, the copy addresses the right input alone
            var original = new ExpressionFieldUsageVisitor(lowerJoin.Left.OutputLength);
            original.Visit(lowerJoin.PostJoinFilter, null);
            var copied = new ExpressionFieldUsageVisitor(int.MaxValue);
            copied.Visit(filter.Condition, null);
            Assert.Empty(original.UsedFieldsLeft);
            Assert.Equal(
                original.UsedFieldsRight.Select(x => x - lowerJoin.Left.OutputLength),
                copied.UsedFieldsLeft);
            Assert.All(copied.UsedFieldsLeft, x => Assert.InRange(x, 0, lowerJoin.Right.OutputLength - 1));
        }

        [Fact]
        public void LeftSideConditionOfLeftJoinBecomesAKeptOnlyCheck()
        {
            var (plan, _) = Discover(@"
                INSERT INTO output
                SELECT o.orderkey, u.name, c.name
                FROM orders o
                LEFT JOIN users u ON o.userkey = u.userkey AND o.amount = 5
                LEFT JOIN companies c ON u.companyid = c.companyid;");

            Assert.NotNull(plan);
            var lower = plan.Root.Left!;
            Assert.NotNull(lower.KeptOnlyCheck);
            Assert.Empty(lower.Checks);
            // The column the condition reads is needed although it is not emitted
            var orders = Input(plan, "orders");
            Assert.Equal(2, orders.NeededColumns.Count);
        }

        [Fact]
        public void ConditionOnBothSidesOfALeftJoinLeavesTheJoinOut()
        {
            var (plan, leftOut) = Discover(@"
                INSERT INTO output
                SELECT o.orderkey, u.name, c.name
                FROM orders o
                INNER JOIN users u ON o.userkey = u.userkey
                LEFT JOIN companies c ON u.companyid = c.companyid AND o.amount <> c.name;");

            Assert.Null(plan);
            Assert.Contains(leftOut, x => x.Reason == "condition on both sides of a left join");
        }

        [Fact]
        public void ConditionOnBothSidesOfAnInnerJoinBecomesACheck()
        {
            var (plan, _) = Discover(@"
                INSERT INTO output
                SELECT o.orderkey, u.name, c.name
                FROM orders o
                INNER JOIN users u ON o.userkey = u.userkey
                INNER JOIN companies c ON u.companyid = c.companyid AND o.amount <> c.name;");

            Assert.NotNull(plan);
            var check = Assert.Single(plan.Root.Checks);
            // Its field indexes lead to the two inputs the condition names
            Assert.Equal(new[] { "companies", "orders" }, TablesRead(plan, check));
        }

        [Fact]
        public void ARepeatedColumnInAnEmitIsOneKey()
        {
            var root = RootJoin(Optimized(@"
                INSERT INTO output
                SELECT o.orderkey, u.name, c.name
                FROM orders o
                INNER JOIN users u ON o.userkey = u.userkey
                LEFT JOIN companies c ON u.companyid = c.companyid;"));
            var lower = Assert.IsType<MergeJoinRelation>(root.Left);
            Assert.True(lower.EmitSet);
            Assert.True(root.EmitSet);
            int oldLeftLength = lower.OutputLength;
            var leftKey = (FlowtideDotNet.Substrait.Expressions.StructReferenceSegment)((FlowtideDotNet.Substrait.Expressions.DirectFieldReference)root.LeftKeys[0]).ReferenceSegment;
            var rightKey = (FlowtideDotNet.Substrait.Expressions.StructReferenceSegment)((FlowtideDotNet.Substrait.Expressions.DirectFieldReference)root.RightKeys[0]).ReferenceSegment;

            // The lower join emits its key column a second time at the end, everything right of it moves one step
            lower.Emit.Add(lower.Emit[leftKey.Field]);
            rightKey.Field += 1;
            for (int i = 0; i < root.Emit.Count; i++)
            {
                if (root.Emit[i] >= oldLeftLength)
                {
                    root.Emit[i] += 1;
                }
            }
            // And the join above compares both copies with the same right column
            root.LeftKeys.Add(new FlowtideDotNet.Substrait.Expressions.DirectFieldReference()
            {
                ReferenceSegment = new FlowtideDotNet.Substrait.Expressions.StructReferenceSegment() { Field = oldLeftLength }
            });
            root.RightKeys.Add(new FlowtideDotNet.Substrait.Expressions.DirectFieldReference()
            {
                ReferenceSegment = new FlowtideDotNet.Substrait.Expressions.StructReferenceSegment() { Field = rightKey.Field }
            });
            root.ComparisonTypes?.Add(JoinComparisonType.Equal);

            var leftOut = new List<MultiJoinLeftOut>();
            var plan = MultiJoinPlanBuilder.Discover(root, 5, leftOut);

            Assert.Empty(leftOut);
            Assert.NotNull(plan);
            Assert.Single(plan.Root.LeftKey!.Columns);
        }

        [Theory]
        [InlineData("INNER")]
        [InlineData("LEFT")]
        public void KeyFromTwoInputsLeavesTheJoinOut(string type)
        {
            var (plan, leftOut) = Discover($@"
                INSERT INTO output
                SELECT o.orderkey, u.name, e.v
                FROM orders o
                INNER JOIN users u ON o.userkey = u.userkey
                {type} JOIN extra e ON o.amount = e.a AND u.name = e.b;");

            // One lookup of extra would use rows of two inputs, the join stays an ordinary one
            Assert.Null(plan);
            Assert.Contains(leftOut, x => x.Reason == "key from several inputs");
        }

        [Fact]
        public void KeyOfSeveralColumnsFromOneInputStaysWhole()
        {
            var (plan, _) = Discover(@"
                INSERT INTO output
                SELECT o.orderkey, u.name, e.v
                FROM orders o
                INNER JOIN users u ON o.userkey = u.userkey
                LEFT JOIN extra e ON o.amount = e.a AND o.productkey = e.b;");

            Assert.NotNull(plan);
            Assert.Equal(2, plan.Root.LeftKey!.Columns.Length);
            Assert.Equal(2, plan.Root.RightKey!.Columns.Length);
            Assert.Empty(plan.Root.Checks);
        }

        [Fact]
        public void ColumnComparedWithTwoColumnsKeepsOneKeyOnAnInnerJoin()
        {
            var (plan, _) = Discover(@"
                INSERT INTO output
                SELECT o.orderkey, u.name, e.v
                FROM orders o
                INNER JOIN users u ON o.userkey = u.userkey
                INNER JOIN extra e ON o.amount = e.a AND o.amount = e.b;");

            Assert.NotNull(plan);
            Assert.Single(plan.Root.LeftKey!.Columns);
            Assert.Single(plan.Root.Checks);
        }

        [Fact]
        public void ARepeatedKeyConditionIsUsedOnce()
        {
            var root = RootJoin(Optimized(@"
                INSERT INTO output
                SELECT o.orderkey, u.name, c.name
                FROM orders o
                INNER JOIN users u ON o.userkey = u.userkey
                LEFT JOIN companies c ON u.companyid = c.companyid;"));
            // The same condition written twice, as a query can
            root.LeftKeys.Add(root.LeftKeys[0]);
            root.RightKeys.Add(root.RightKeys[0]);
            root.ComparisonTypes?.Add(JoinComparisonType.Equal);

            var plan = MultiJoinPlanBuilder.Discover(root, 5);

            Assert.NotNull(plan);
            Assert.Single(plan.Root.LeftKey!.Columns);
            Assert.Single(plan.Root.RightKey!.Columns);
            Assert.Empty(plan.Root.Checks);
        }

        [Fact]
        public void SingleJoinIsNotAGroup()
        {
            var (plan, leftOut) = Discover(@"
                INSERT INTO output
                SELECT o.orderkey, u.name
                FROM orders o
                INNER JOIN users u ON o.userkey = u.userkey;");

            Assert.Null(plan);
            var entry = Assert.Single(leftOut);
            Assert.Equal("fewer than three inputs", entry.Reason);
        }

        [Fact]
        public void AnUnsupportedJoinBelowBecomesAnInput()
        {
            var (plan, leftOut) = Discover(@"
                INSERT INTO output
                SELECT o.orderkey, u.name, p.name, c.name
                FROM orders o
                INNER JOIN users u ON o.userkey = u.userkey AND o.amount < u.name
                INNER JOIN products p ON o.productkey = p.productkey
                INNER JOIN companies c ON u.companyid = c.companyid;");

            Assert.NotNull(plan);
            Assert.Equal(3, plan.Inputs.Count);
            Assert.IsType<MergeJoinRelation>(plan.Inputs[0].Relation);
            Assert.Contains(leftOut, x => x.Reason == "range key");
        }

        [Fact]
        public void AJoinDroppedAfterItsInputsWereFlattenedLeavesNoTrace()
        {
            // The left join is only found unusable once its inputs are known, by then two inputs were added for it
            var (plan, leftOut) = Discover(@"
                INSERT INTO output
                SELECT o.orderkey, u.name, e.v, p.name, c.name
                FROM orders o
                INNER JOIN users u ON o.userkey = u.userkey
                LEFT JOIN extra e ON o.amount = e.a AND u.name = e.b
                INNER JOIN products p ON o.productkey = p.productkey
                INNER JOIN companies c ON u.companyid = c.companyid;");

            Assert.NotNull(plan);
            Assert.Equal(3, plan.Inputs.Count);
            Assert.IsType<MergeJoinRelation>(plan.Inputs[0].Relation);
            Assert.Equal(new[] { "products", "companies" }, plan.Inputs.Skip(1).Select(x => Table(x.Relation)));
            Assert.Equal(2, plan.Joins.Count);
            Assert.Equal(2, plan.InnerJoinCount);
            Assert.Equal(0, plan.LeftJoinCount);
            for (int i = 0; i < plan.Inputs.Count; i++)
            {
                Assert.Equal(i, plan.Inputs[i].Node.Leaf);
            }
            Assert.All(plan.Output, x => Assert.InRange(x.Input, 0, 2));
            var entry = Assert.Single(leftOut);
            Assert.Equal("key from several inputs", entry.Reason);
        }

        [Fact]
        public void BuildFollowsMarksAndGivesTheSameGroup()
        {
            var root = RootJoin(Optimized(@"
                INSERT INTO output
                SELECT o.orderkey, u.name, p.name, c.name
                FROM orders o
                INNER JOIN users u ON o.userkey = u.userkey
                LEFT JOIN products p ON o.productkey = p.productkey
                INNER JOIN companies c ON u.companyid = c.companyid;"));
            var discovered = MultiJoinPlanBuilder.Discover(root, 5);
            Assert.NotNull(discovered);
            foreach (var join in discovered.Joins)
            {
                join.MultiJoinGroup = 7;
            }

            var built = MultiJoinPlanBuilder.Build(root);

            Assert.Equal(discovered.Inputs.Select(x => Table(x.Relation)), built.Inputs.Select(x => Table(x.Relation)));
            Assert.Equal(discovered.Joins, built.Joins);
            Assert.Equal(discovered.Output, built.Output);
            Assert.Equal(discovered.LeftJoinCount, built.LeftJoinCount);
        }

        [Fact]
        public void BuildStopsWhereTheMarksStop()
        {
            var root = RootJoin(Optimized(@"
                INSERT INTO output
                SELECT o.orderkey, u.name, p.name, c.name
                FROM orders o
                INNER JOIN users u ON o.userkey = u.userkey
                INNER JOIN products p ON o.productkey = p.productkey
                INNER JOIN companies c ON u.companyid = c.companyid;"));
            // Only the two upper joins are in the group
            root.MultiJoinGroup = 1;
            ((MergeJoinRelation)root.Left).MultiJoinGroup = 1;

            var built = MultiJoinPlanBuilder.Build(root);

            Assert.Equal(3, built.Inputs.Count);
            Assert.IsType<MergeJoinRelation>(built.Inputs[0].Relation);
        }

        [Fact]
        public void BuildFailsOnAMarkBelowAnInput()
        {
            var root = RootJoin(Optimized(@"
                INSERT INTO output
                SELECT o.orderkey, u.name, p.name, c.name
                FROM orders o
                INNER JOIN users u ON o.userkey = u.userkey
                INNER JOIN products p ON o.productkey = p.productkey
                INNER JOIN companies c ON u.companyid = c.companyid;"));
            var middle = (MergeJoinRelation)root.Left;
            var bottom = (MergeJoinRelation)middle.Left;
            // The middle join is not in the group, so the bottom one is cut off from it
            root.MultiJoinGroup = 1;
            bottom.MultiJoinGroup = 1;

            var exception = Assert.Throws<InvalidOperationException>(() => MultiJoinPlanBuilder.Build(root));
            Assert.Contains("below one of its inputs", exception.Message);

            // A different group below is fine
            bottom.MultiJoinGroup = 2;
            middle.MultiJoinGroup = 1;
            Assert.Equal(3, MultiJoinPlanBuilder.Build(root).Inputs.Count);
        }

        [Fact]
        public void BuildFailsOnAMarkedJoinThatCannotBePartOfAGroup()
        {
            var root = RootJoin(Optimized(@"
                INSERT INTO output
                SELECT o.orderkey, u.name, c.name
                FROM orders o
                INNER JOIN users u ON o.userkey = u.userkey
                FULL OUTER JOIN companies c ON u.companyid = c.companyid AND o.amount <> c.name;"));
            root.MultiJoinGroup = 1;
            ((MergeJoinRelation)root.Left).MultiJoinGroup = 1;

            var exception = Assert.Throws<InvalidOperationException>(() => MultiJoinPlanBuilder.Build(root));
            Assert.Contains("condition on a Outer join", exception.Message);
        }

        [Fact]
        public void BuildFailsWithoutAMarkOrWithTooFewInputs()
        {
            var root = RootJoin(Optimized(@"
                INSERT INTO output
                SELECT o.orderkey, u.name, c.name
                FROM orders o
                INNER JOIN users u ON o.userkey = u.userkey
                INNER JOIN companies c ON u.companyid = c.companyid;"));

            Assert.Throws<InvalidOperationException>(() => MultiJoinPlanBuilder.Build(root));

            root.MultiJoinGroup = 1;
            Assert.Throws<InvalidOperationException>(() => MultiJoinPlanBuilder.Build(root));
        }

        [Fact]
        public void TheMarkIsPartOfEqualityAndHash()
        {
            var first = RootJoin(Optimized(@"
                INSERT INTO output
                SELECT o.orderkey, u.name FROM orders o INNER JOIN users u ON o.userkey = u.userkey;"));
            var second = RootJoin(Optimized(@"
                INSERT INTO output
                SELECT o.orderkey, u.name FROM orders o INNER JOIN users u ON o.userkey = u.userkey;"));
            Assert.Equal(first, second);
            Assert.Equal(first.GetHashCode(), second.GetHashCode());

            second.MultiJoinGroup = 3;

            Assert.NotEqual(first, second);
            Assert.NotEqual(first.GetHashCode(), second.GetHashCode());
        }
    }
}
