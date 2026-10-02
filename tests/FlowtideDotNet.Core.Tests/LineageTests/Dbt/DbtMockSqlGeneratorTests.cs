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

using FlowtideDotNet.Core.Lineage.Dbt;
using FlowtideDotNet.Core.Lineage.Dbt.Internal;
using FlowtideDotNet.Core.Lineage.Internal;
using FlowtideDotNet.Substrait.Type;
using static FlowtideDotNet.Core.Tests.LineageTests.Dbt.DbtTestData;

namespace FlowtideDotNet.Core.Tests.LineageTests.Dbt
{
    public class DbtMockSqlGeneratorTests
    {
        internal const string WindowSql = @"
            CREATE TABLE input1 (a int, b int, c string);
            CREATE TABLE output (c1 int, c2 int);
            INSERT INTO output
            SELECT SUM(a) OVER (PARTITION BY b ORDER BY c) as c1, b as c2 FROM input1;
            ";

        internal static readonly string WindowCompiledCode = Sql(
            "SELECT",
            "  COALESCE(t0.\"a\", t0.\"b\", t0.\"c\") AS \"c1\",",
            "  t0.\"b\" AS \"c2\"",
            "FROM \"shop\".\"dbo\".\"input1\" AS t0");

        private static void Shop(DbtManifestOptions options)
        {
            options.MapNamespace("mssql", "shop", "dbo");
        }

        [Fact]
        public void WindowExampleGolden()
        {
            var lineage = ExtractWithSql(WindowSql, "mssql");
            var store = Store(Shop);
            store.Register(lineage, "window_stream");

            var model = Node(store.GetManifest(), "model.flowtide.mssql__shop__dbo__output");

            Assert.Equal(WindowCompiledCode, model.GetProperty("compiled_code").GetString());
            // Checksum pins the exact formatting.
            Assert.Equal("8606654d84ff9fd3df3063efedae1d4989eedf5d1045965b66a082753e35101a", model.GetProperty("checksum").GetProperty("checksum").GetString());
        }

        [Fact]
        public void JoinExampleWithRealExtraction()
        {
            var lineage = ExtractWithSql(@"
                CREATE TABLE orders (id int, cid int, status string);
                CREATE TABLE customers (id int, name string);
                CREATE TABLE out (id int, name string);
                INSERT INTO out
                SELECT o.id, c.name FROM orders o JOIN customers c ON o.cid = c.id;
                ", "mssql");

            Assert.Equal(Sql(
                "SELECT",
                "  t0.\"id\" AS \"id\",",
                "  t1.\"name\" AS \"name\"",
                "FROM \"shop\".\"dbo\".\"orders\" AS t0",
                "INNER JOIN \"shop\".\"dbo\".\"customers\" AS t1 ON t0.\"cid\" = t1.\"id\""), CompiledCode(lineage, Shop));
        }

        [Fact]
        public void JoinAndFilterExampleGolden()
        {
            // Read filters never reach the dataset today, so hand built.
            var lineage = Snapshot(
                [Input("mssql", "orders", [Col("id"), Col("cid"), Col("status")]), Input("mssql", "customers", [Col("id"), Col("name")])],
                [Output("mssql", "out", [Col("id"), Col("name")], new()
                {
                    ["id"] = [Identity("mssql", "orders", "id")],
                    ["name"] = [Identity("mssql", "customers", "name")]
                }, [
                    Indirect("mssql", "orders", "cid", LineageTransformationSubtype.Join),
                    Indirect("mssql", "customers", "id", LineageTransformationSubtype.Join),
                    Indirect("mssql", "orders", "status", LineageTransformationSubtype.Filter)
                ], ["orders", "customers"])]);

            Assert.Equal(Sql(
                "SELECT",
                "  t0.\"id\" AS \"id\",",
                "  t1.\"name\" AS \"name\"",
                "FROM \"shop\".\"dbo\".\"orders\" AS t0",
                "INNER JOIN \"shop\".\"dbo\".\"customers\" AS t1 ON t0.\"cid\" = t1.\"id\"",
                "WHERE t0.\"status\" IS NOT NULL"), CompiledCode(lineage, Shop));
        }

        [Fact]
        public void NoInputsGivesNull()
        {
            var lineage = Snapshot(
                [Input("mssql", "t", [Col("a")])],
                [Output("mssql", "out", [Col("c")], new() { ["c"] = [] }, upstream: ["t"])]);

            Assert.Equal(Sql(
                "SELECT",
                "  NULL AS \"c\"",
                "FROM \"t\" AS t0"), CompiledCode(lineage, o => o.MapNamespace("mssql", defaultSchema: "")));
        }

        [Fact]
        public void TableWithoutJoinFieldsCrossJoins()
        {
            var lineage = Snapshot(
                [Input("pg", "a", [Col("x")]), Input("pg", "b", [Col("y")])],
                [Output("pg", "out", [Col("x"), Col("y")], new()
                {
                    ["x"] = [Identity("pg", "a", "x")],
                    ["y"] = [Identity("pg", "b", "y")]
                }, upstream: ["a", "b"])]);

            Assert.Equal(Sql(
                "SELECT",
                "  t0.\"x\" AS \"x\",",
                "  t1.\"y\" AS \"y\"",
                "FROM \"a\" AS t0",
                "CROSS JOIN \"b\" AS t1"), CompiledCode(lineage));
        }

        [Fact]
        public void LaterTableWithoutEarlierJoinRefsUsesIsNotNull()
        {
            var lineage = Snapshot(
                [Input("pg", "a", [Col("x")]), Input("pg", "b", [Col("k"), Col("l")])],
                [Output("pg", "out", [Col("x")], new() { ["x"] = [Identity("pg", "a", "x")] }, [
                    Indirect("pg", "b", "k", LineageTransformationSubtype.Join),
                    Indirect("pg", "b", "l", LineageTransformationSubtype.Join)
                ])]);

            Assert.Equal(Sql(
                "SELECT",
                "  t0.\"x\" AS \"x\"",
                "FROM \"a\" AS t0",
                "INNER JOIN \"b\" AS t1 ON COALESCE(t1.\"k\", t1.\"l\") IS NOT NULL"), CompiledCode(lineage));
        }

        [Fact]
        public void JoinRefsOnlyOnFirstTableGoToWhere()
        {
            var lineage = Snapshot(
                [Input("pg", "a", [Col("x"), Col("k")])],
                [Output("pg", "out", [Col("x")], new() { ["x"] = [Identity("pg", "a", "x")] }, [
                    Indirect("pg", "a", "k", LineageTransformationSubtype.Join)
                ])]);

            Assert.Equal(Sql(
                "SELECT",
                "  t0.\"x\" AS \"x\"",
                "FROM \"a\" AS t0",
                "WHERE t0.\"k\" IS NOT NULL"), CompiledCode(lineage));
        }

        [Fact]
        public void GroupByWithRealExtraction()
        {
            var lineage = ExtractWithSql(@"
                CREATE TABLE input1 (a int, b int);
                CREATE TABLE output (c1 int, c2 int);
                INSERT INTO output
                SELECT b as c1, SUM(a) as c2 FROM input1 GROUP BY b;
                ", "mssql");

            Assert.Equal(Sql(
                "SELECT",
                "  t0.\"b\" AS \"c1\",",
                "  t0.\"a\" AS \"c2\"",
                "FROM \"shop\".\"dbo\".\"input1\" AS t0",
                "GROUP BY t0.\"b\""), CompiledCode(lineage, Shop));
        }

        [Fact]
        public void SelfJoinWithRealExtractionUsesOneAlias()
        {
            var lineage = ExtractWithSql(@"
                CREATE TABLE input1 (id int, parent int, name string);
                CREATE TABLE output (c1 string, c2 string);
                INSERT INTO output
                SELECT c.name as c1, p.name as c2 FROM input1 c JOIN input1 p ON c.parent = p.id;
                ", "mssql");

            Assert.Equal(Sql(
                "SELECT",
                "  t0.\"name\" AS \"c1\",",
                "  t0.\"name\" AS \"c2\"",
                "FROM \"shop\".\"dbo\".\"input1\" AS t0",
                "WHERE t0.\"parent\" IS NOT NULL AND t0.\"id\" IS NOT NULL"), CompiledCode(lineage, Shop));
        }

        [Fact]
        public void AliasOrderIsFieldsThenDatasetThenReachable()
        {
            // Reachable only tables sort by unique id, d before e.
            var lineage = Snapshot(
                [Input("pg", "b", [Col("y")]), Input("pg", "a", [Col("x")]), Input("pg", "c", [Col("f")]), Input("pg", "e", []), Input("pg", "d", [])],
                [Output("pg", "out", [Col("o1"), Col("o2")], new()
                {
                    ["o1"] = [Identity("pg", "b", "y")],
                    ["o2"] = [Identity("pg", "a", "x")]
                }, [Indirect("pg", "c", "f", LineageTransformationSubtype.Filter)], ["e", "d", "c", "b", "a"])]);

            Assert.Equal(Sql(
                "SELECT",
                "  t0.\"y\" AS \"o1\",",
                "  t1.\"x\" AS \"o2\"",
                "FROM \"b\" AS t0",
                "CROSS JOIN \"a\" AS t1",
                "CROSS JOIN \"c\" AS t2",
                "CROSS JOIN \"d\" AS t3",
                "CROSS JOIN \"e\" AS t4",
                "WHERE t2.\"f\" IS NOT NULL"), CompiledCode(lineage));
        }

        [Fact]
        public void SelfReferenceIsDropped()
        {
            var lineage = Snapshot(
                [Input("pg", "out", [Col("a")]), Input("pg", "src", [Col("b")])],
                [Output("pg", "out", [Col("a")], new()
                {
                    ["a"] = [Identity("pg", "out", "a"), Identity("pg", "src", "b")]
                }, upstream: ["out", "src"])]);

            Assert.Equal(Sql(
                "SELECT",
                "  t0.\"b\" AS \"a\"",
                "FROM \"src\" AS t0"), CompiledCode(lineage));
        }

        [Fact]
        public void ExcludedInputIsDropped()
        {
            var lineage = Snapshot(
                [Input("test", "hidden", [Col("a")]), Input("pg", "src", [Col("b")])],
                [Output("pg", "out", [Col("a"), Col("b")], new()
                {
                    ["a"] = [Identity("test", "hidden", "a")],
                    ["b"] = [Identity("test", "hidden", "a"), Identity("pg", "src", "b")]
                }, upstream: ["hidden", "src"])]);

            Assert.Equal(Sql(
                "SELECT",
                "  NULL AS \"a\",",
                "  t0.\"b\" AS \"b\"",
                "FROM \"src\" AS t0"), CompiledCode(lineage));
        }

        [Fact]
        public void EmbeddedQuotesAreDoubled()
        {
            var lineage = Snapshot(
                [Input("pg", "we\"ird", [Col("c\"ol")])],
                [Output("pg", "o\"ut", [Col("a\"b")], new() { ["a\"b"] = [Identity("pg", "we\"ird", "c\"ol")] })]);

            var store = Store();
            store.Register(lineage, "stream");
            var model = Assert.Single(Parse(store.GetManifest()).GetProperty("nodes").EnumerateObject()).Value;

            Assert.Equal(Sql(
                "SELECT",
                "  t0.\"c\"\"ol\" AS \"a\"\"b\"",
                "FROM \"we\"\"ird\" AS t0"), model.GetProperty("compiled_code").GetString());
            Assert.Equal("\"o\"\"ut\"", model.GetProperty("relation_name").GetString());
        }

        [Fact]
        public void DottedKafkaTopicStaysOneIdentifier()
        {
            var lineage = Snapshot(
                [Input("kafka://b1:9092", "orders.v1", [Col("id")])],
                [Output("pg", "out", [Col("id")], new() { ["id"] = [Identity("kafka://b1:9092", "orders.v1", "id")] })]);

            Assert.Equal(Sql(
                "SELECT",
                "  t0.\"id\" AS \"id\"",
                "FROM \"orders.v1\" AS t0"), CompiledCode(lineage));
        }

        [Fact]
        public void CasingIsCanonicalized()
        {
            // Connector casing wins for inputs, outputs and aliases.
            var lineage = Snapshot(
                [Input("pg", "src", [Col("orderid", new Int64Type())], [Col("OrderId", new Int64Type())])],
                [Output("pg", "out", [Col("orderid")], new() { ["orderid"] = [Identity("pg", "src", "orderid")] }, connectorColumns: [Col("OrderId", new Int64Type())])]);

            var store = Store();
            store.Register(lineage, "stream");
            var model = Assert.Single(Parse(store.GetManifest()).GetProperty("nodes").EnumerateObject()).Value;

            Assert.Equal(Sql(
                "SELECT",
                "  t0.\"OrderId\" AS \"OrderId\"",
                "FROM \"src\" AS t0"), model.GetProperty("compiled_code").GetString());
            Assert.Equal(["OrderId"], Keys(model.GetProperty("columns")));
            Assert.Equal("pg:src.OrderId DIRECT/IDENTITY", model.GetProperty("columns").GetProperty("OrderId").GetProperty("meta").GetProperty("flowtide_inputs").GetString());
        }

        [Fact]
        public void CaseVariantOutputColumnsMerge()
        {
            var first = Snapshot([Input("pg", "a", [Col("x")])], [Output("pg", "out", [Col("Id")], new() { ["Id"] = [Identity("pg", "a", "x")] })]);
            var second = Snapshot([Input("pg", "b", [Col("y")])], [Output("pg", "out", [Col("id")], new() { ["id"] = [Identity("pg", "b", "y")] })]);
            var store = Store();
            store.Register(first, "s1");
            store.Register(second, "s2");

            var model = Assert.Single(Parse(store.GetManifest()).GetProperty("nodes").EnumerateObject()).Value;

            Assert.Equal(Sql(
                "SELECT",
                "  COALESCE(t0.\"x\", t1.\"y\") AS \"Id\"",
                "FROM \"a\" AS t0",
                "CROSS JOIN \"b\" AS t1"), model.GetProperty("compiled_code").GetString());
            Assert.Equal(["Id"], Keys(model.GetProperty("columns")));
        }

        [Fact]
        public void LfOnlyWithoutSemicolonOrComments()
        {
            var sql = CompiledCode(ExtractWithSql(WindowSql, "mssql"), Shop);

            Assert.DoesNotContain("\r", sql);
            Assert.DoesNotContain(";", sql);
            Assert.DoesNotContain("--", sql);
            Assert.DoesNotContain("/*", sql);
            Assert.DoesNotContain("WITH", sql);
        }

        [Fact]
        public void EmptyOutputSchemaSelectsNull()
        {
            var lineage = Snapshot([Input("pg", "a", [Col("x")])], [Output("pg", "out", [], new(), upstream: ["a"])]);

            Assert.Equal(Sql("SELECT", "  NULL", "FROM \"a\" AS t0"), CompiledCode(lineage));
        }

        [Fact]
        public void NoTablesHasNoFrom()
        {
            var model = new DbtMergedTable(new DbtTableIdentity("pg", null, "", "out"));
            model.AddOutputColumn("c");

            var result = DbtMockSqlGenerator.Generate(model, x => x.Identifier, DbtSqlDialectInfo.For(DbtSqlDialect.Postgres));

            Assert.Equal(Sql("SELECT", "  NULL AS \"c\""), result.Sql);
            Assert.Empty(result.FromOrder);
            Assert.False(result.SelfReference);
        }

        [Fact]
        public void DuplicateRefsAppearOnce()
        {
            // Same column reached twice through a union.
            var lineage = Snapshot(
                [Input("pg", "a", [Col("x")])],
                [Output("pg", "out", [Col("c")], new()
                {
                    ["c"] = [Identity("pg", "a", "x"), Indirect("pg", "a", "x", LineageTransformationSubtype.Sort)]
                })]);

            Assert.Equal(Sql("SELECT", "  t0.\"x\" AS \"c\"", "FROM \"a\" AS t0"), CompiledCode(lineage));
        }
    }
}
