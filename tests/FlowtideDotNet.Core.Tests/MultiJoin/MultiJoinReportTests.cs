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
using FlowtideDotNet.Substrait;
using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Type;
using Xunit.Abstractions;

namespace FlowtideDotNet.Core.Tests.MultiJoin
{
    public class MultiJoinReportTests
    {
        private readonly ITestOutputHelper _output;

        public MultiJoinReportTests(ITestOutputHelper output)
        {
            _output = output;
        }

        private const string StarTables = @"
            CREATE TABLE fact (id any, k1 any, k2 any, k3 any, k4 any, k5 any, k6 any, k7 any, k8 any);
            CREATE TABLE d1 (k any, v any);
            CREATE TABLE d2 (k any, v any);
            CREATE TABLE d3 (k any, v any);
            CREATE TABLE d4 (k any, v any);
            CREATE TABLE d5 (k any, v any);
            CREATE TABLE d6 (k any, v any);
            CREATE TABLE d7 (k any, v any);
            CREATE TABLE d8 (k any, v any);
        ";

        private const string EightJoins = @"
            INSERT INTO output
            SELECT f.id, a1.v, a2.v, a3.v, a4.v, a5.v, a6.v, a7.v, a8.v
            FROM fact f
            INNER JOIN d1 a1 ON f.k1 = a1.k
            INNER JOIN d2 a2 ON f.k2 = a2.k
            INNER JOIN d3 a3 ON f.k3 = a3.k
            INNER JOIN d4 a4 ON f.k4 = a4.k
            INNER JOIN d5 a5 ON f.k5 = a5.k
            INNER JOIN d6 a6 ON f.k6 = a6.k
            INNER JOIN d7 a7 ON f.k7 = a7.k
            INNER JOIN d8 a8 ON f.k8 = a8.k;";

        private MultiJoinReport Report(string sql, int maxInputs = 5, string tables = MultiJoinPlanBuilderTests.Tables)
        {
            var report = MultiJoinReport.Create(MultiJoinPlanBuilderTests.Optimized(sql, tables), maxInputs);
            _output.WriteLine(report.ToString());
            return report;
        }

        [Theory]
        [InlineData(3, new[] { 3, 3, 3, 3 })]
        [InlineData(4, new[] { 3, 4, 4 })]
        [InlineData(5, new[] { 5, 5 })]
        [InlineData(9, new[] { 9 })]
        public void TheInputCapSplitsALongChainIntoGroups(int maxInputs, int[] expectedGroupSizes)
        {
            var report = Report(EightJoins, maxInputs, StarTables);

            Assert.Equal(8, report.MergeJoinCount);
            Assert.Equal(expectedGroupSizes, report.Groups.Select(x => x.InputCount).OrderBy(x => x));
            Assert.Equal(8, report.Groups.Sum(x => x.InnerJoinCount));
            Assert.Empty(report.LeftOut);
        }

        [Fact]
        public void TheGroupAtTheTopHoldsTheLastJoins()
        {
            var report = Report(EightJoins, 5, StarTables);

            // The first group found is the one at the root, its first input is the rest of the chain
            Assert.Equal("join", report.Groups[0].InputNames[0]);
            Assert.Contains(report.Groups[0].InputNames, x => x.Contains("d8"));
            Assert.Contains(report.Groups[1].InputNames, x => x.Contains("fact"));
        }

        [Fact]
        public void ASingleJoinIsLeftOut()
        {
            var report = Report(@"
                INSERT INTO output
                SELECT o.orderkey, u.name FROM orders o INNER JOIN users u ON o.userkey = u.userkey;");

            Assert.Empty(report.Groups);
            Assert.Equal(1, report.LeftOut["fewer than three inputs"]);
        }

        [Fact]
        public void ARightJoinIsPartOfItsGroup()
        {
            var report = Report(@"
                INSERT INTO output
                SELECT o.orderkey, u.name, p.name, c.name
                FROM orders o
                INNER JOIN users u ON o.userkey = u.userkey
                INNER JOIN products p ON o.productkey = p.productkey
                RIGHT JOIN companies c ON u.companyid = c.companyid;");

            Assert.Equal(3, report.MergeJoinCount);
            var group = Assert.Single(report.Groups);
            Assert.Equal(4, group.InputCount);
            Assert.Empty(report.LeftOut);
        }

        [Fact]
        public void EachLeftOutJoinIsCountedOnce()
        {
            var report = Report(@"
                INSERT INTO output
                SELECT o.orderkey, u.name, p.name, c.name
                FROM orders o
                INNER JOIN users u ON o.userkey = u.userkey AND o.amount < u.name
                INNER JOIN products p ON o.productkey = p.productkey
                INNER JOIN companies c ON u.companyid = c.companyid;");

            // The range join is left out of the group above it and is then looked at again as a root
            Assert.Equal(1, report.LeftOut["range key"]);
            Assert.Equal(3, report.MergeJoinCount);
            Assert.Single(report.Groups);
        }

        [Fact]
        public void JoinsInsideALoopBodyAreLeftOut()
        {
            var report = Report(@"
                WITH managed AS (
                    SELECT u.userkey, u.name, u.managerkey, c.name AS company, d.name AS department, 1 AS level
                    FROM people u
                    INNER JOIN firms c ON u.firmid = c.firmid
                    INNER JOIN departments d ON u.departmentid = d.departmentid
                    WHERE u.managerkey IS NULL
                    UNION ALL
                    SELECT u.userkey, u.name, u.managerkey, c.name AS company, d.name AS department, m.level + 1 AS level
                    FROM people u
                    INNER JOIN managed m ON m.userkey = u.managerkey
                    INNER JOIN firms c ON u.firmid = c.firmid
                    INNER JOIN departments d ON u.departmentid = d.departmentid
                )
                INSERT INTO output
                SELECT userkey, name, company, department, level FROM managed;",
                tables: @"
                CREATE TABLE people (userkey any, name any, managerkey any, firmid any, departmentid any);
                CREATE TABLE firms (firmid any, name any);
                CREATE TABLE departments (departmentid any, name any);
                ");

            // The SQL front end puts the whole recursive query into the loop plan, the anchor part included, so all five joins are inside it.
            Assert.Equal(5, report.MergeJoinCount);
            Assert.Equal(5, report.LeftOut[MultiJoinReport.InsideLoopReason]);
            Assert.Empty(report.Groups);
        }

        [Fact]
        public void FullOuterAndMarkJoinsArePartOfTheirGroup()
        {
            var report = Report(@"
                INSERT INTO output
                SELECT o.orderkey, u.name, p.name
                FROM orders o
                INNER JOIN users u ON o.userkey = u.userkey
                INNER JOIN products p ON o.productkey = p.productkey
                FULL OUTER JOIN companies c ON u.companyid = c.companyid
                WHERE EXISTS (SELECT 1 FROM extra e WHERE e.a = o.amount);");

            _output.WriteLine(report.ToString());
            Assert.Empty(report.LeftOut);
        }

        private static ReadRelation Read(string name)
        {
            return new ReadRelation()
            {
                NamedTable = new NamedTable() { Names = new List<string>() { name } },
                BaseSchema = new NamedStruct()
                {
                    Names = new List<string>() { "k", "v" },
                    Struct = new Struct() { Types = new List<SubstraitBaseType>() { new AnyType(), new AnyType() } }
                }
            };
        }

        // Keys are field indexes into the left output followed by the right output
        private static MergeJoinRelation Join(JoinType type, Relation left, Relation right, params (int left, int right)[] keys)
        {
            return new MergeJoinRelation()
            {
                Type = type,
                Left = left,
                Right = right,
                LeftKeys = keys.Select(x => (FieldReference)new DirectFieldReference() { ReferenceSegment = new StructReferenceSegment() { Field = x.left } }).ToList(),
                RightKeys = keys.Select(x => (FieldReference)new DirectFieldReference() { ReferenceSegment = new StructReferenceSegment() { Field = x.right } }).ToList()
            };
        }

        [Fact]
        public void AJoinLeftOutOfOneGroupCanStillFormItsOwn()
        {
            // The SQL parser has no nested joins, so the bushy plan is built by hand. Every table is (k, v).
            var i = Join(JoinType.Inner, Read("a"), Read("b"), (0, 2));
            // a.k = c.k AND b.k = c.v
            var h = Join(JoinType.Left, i, Read("c"), (0, 4), (2, 5));
            var r = Join(JoinType.Inner, Read("d"), Read("e"), (0, 2));
            // a.k = d.k AND c.k = d.v
            var k = Join(JoinType.Left, h, r, (0, 6), (4, 7));
            var q = Join(JoinType.Inner, Read("f"), Read("g"), (0, 2));
            // a.k = f.k
            var top = Join(JoinType.Inner, k, q, (0, 10));
            var plan = new Plan() { Relations = new List<Relation>() { top } };

            // With a cap of four the group at the top reaches h before the cap ends below it, which makes k's key span two inputs, so k is left out there.
            var report = MultiJoinReport.Create(plan, 4);
            _output.WriteLine(report.ToString());

            Assert.Equal(6, report.MergeJoinCount);
            Assert.Equal(2, report.Groups.Count);
            int grouped = report.Groups.Sum(x => x.InnerJoinCount + x.LeftJoinCount);
            Assert.Equal(4, grouped);
            Assert.Equal(2, report.LeftOut.Values.Sum());
            Assert.Equal(1, report.LeftOut["key from several inputs"]);
            Assert.Equal(1, report.LeftOut["fewer than three inputs"]);
        }

        [Fact]
        public void ReadingTheReportDoesNotMarkAnything()
        {
            var plan = MultiJoinPlanBuilderTests.Optimized(EightJoins, StarTables);
            MultiJoinReport.Create(plan, 5);

            var join = MultiJoinPlanBuilderTests.RootJoin(plan);
            Assert.Null(join.MultiJoinGroup);
            Assert.Null(((MergeJoinRelation)join.Left).MultiJoinGroup);
        }
    }
}
