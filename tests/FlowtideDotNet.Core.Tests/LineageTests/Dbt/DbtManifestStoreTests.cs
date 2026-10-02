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
using FlowtideDotNet.Core.Lineage.Internal.Models;
using FlowtideDotNet.Substrait.Type;
using System.Text;
using System.Text.Json;
using static FlowtideDotNet.Core.Tests.LineageTests.Dbt.DbtTestData;

namespace FlowtideDotNet.Core.Tests.LineageTests.Dbt
{
    public class DbtManifestStoreTests
    {
        private static readonly string[] RootKeys =
        [
            "metadata", "nodes", "sources", "macros", "docs", "exposures", "metrics", "groups", "selectors",
            "disabled", "parent_map", "child_map", "group_map", "saved_queries", "semantic_models", "unit_tests"
        ];

        private static readonly string[] ModelKeys =
        [
            "database", "schema", "name", "resource_type", "package_name", "path", "original_file_path", "unique_id", "fqn", "alias",
            "checksum", "config", "tags", "description", "meta", "columns", "relation_name", "raw_code", "language", "refs", "sources",
            "depends_on", "compiled", "compiled_code"
        ];

        private static readonly string[] SourceKeys =
        [
            "database", "schema", "name", "resource_type", "package_name", "path", "original_file_path", "unique_id", "fqn", "source_name",
            "source_description", "loader", "identifier", "description", "columns", "meta", "source_meta", "tags", "config", "relation_name"
        ];

        private sealed class ManualTimeProvider : TimeProvider
        {
            public long Ticks { get; set; }

            public override long TimestampFrequency => TimeSpan.TicksPerSecond;

            public override long GetTimestamp()
            {
                return Ticks;
            }
        }

        // A writes t from s, b reads t into u.
        private static (StreamLineage A, StreamLineage B) Chain()
        {
            var a = Snapshot(
                [Input("pg", "s", [Col("x", new Int64Type())])],
                [Output("pg", "t", [Col("x", new Int64Type())], new() { ["x"] = [Identity("pg", "s", "x")] }, upstream: ["s"])]);
            var b = Snapshot(
                [Input("pg", "t", [Col("x", new Int64Type())])],
                [Output("pg", "u", [Col("y", new Int64Type())], new() { ["y"] = [Identity("pg", "t", "x")] }, upstream: ["t"])]);
            return (a, b);
        }

        private static StreamLineage Simple(string input, string output, DateTimeOffset? buildTime = null, string? substream = null)
        {
            return Snapshot(
                [Input("pg", input, [Col("a", new StringType())])],
                [Output("pg", output, [Col("a", new StringType())], new() { ["a"] = [Identity("pg", input, "a")] }, upstream: [input])],
                substream,
                buildTime);
        }

        private static string Text(DbtArtifact artifact)
        {
            return Encoding.UTF8.GetString(artifact.Utf8Json.Span);
        }

        // Single quotes keep the golden JSON readable.
        private static string Json(string text)
        {
            return text.Replace('\'', '"');
        }

        [Fact]
        public void EmptyStoreManifestHasEveryRootKey()
        {
            var root = Parse(new DbtManifestStore().GetManifest());

            Assert.Equal(RootKeys, Keys(root));
            var metadata = root.GetProperty("metadata");
            Assert.Equal(["dbt_schema_version", "dbt_version", "generated_at", "invocation_id", "env", "project_name", "adapter_type"], Keys(metadata));
            Assert.Equal("https://schemas.getdbt.com/dbt/manifest/v12.json", metadata.GetProperty("dbt_schema_version").GetString());
            Assert.Equal("1970-01-01T00:00:00.000000Z", metadata.GetProperty("generated_at").GetString());
            Assert.Equal("flowtide", metadata.GetProperty("project_name").GetString());
            Assert.Equal("postgres", metadata.GetProperty("adapter_type").GetString());
            Assert.Empty(Keys(metadata.GetProperty("env")));
            foreach (var key in RootKeys.Skip(1))
            {
                Assert.Equal(JsonValueKind.Object, root.GetProperty(key).ValueKind);
                Assert.Empty(Keys(root.GetProperty(key)));
            }
        }

        [Fact]
        public void EmptyStoreCatalogIsValid()
        {
            var root = Parse(new DbtManifestStore().GetCatalog());

            Assert.Equal(["metadata", "nodes", "sources", "errors"], Keys(root));
            Assert.Equal(["dbt_schema_version", "dbt_version", "generated_at", "invocation_id", "env"], Keys(root.GetProperty("metadata")));
            Assert.Equal("https://schemas.getdbt.com/dbt/catalog/v1.json", root.GetProperty("metadata").GetProperty("dbt_schema_version").GetString());
            Assert.Equal(JsonValueKind.Null, root.GetProperty("errors").ValueKind);
        }

        [Fact]
        public void WindowExampleGolden()
        {
            var store = Store(o => o.MapNamespace("mssql", "shop", "dbo"));
            store.Register(ExtractWithSql(DbtMockSqlGeneratorTests.WindowSql, "mssql"), "window_stream");

            var manifest = Text(store.GetManifest());
            var invocationId = Parse(store.GetManifest()).GetProperty("metadata").GetProperty("invocation_id").GetString()!;
            var code = @"SELECT\n  COALESCE(t0.\'a\', t0.\'b\', t0.\'c\') AS \'c1\',\n  t0.\'b\' AS \'c2\'\nFROM \'shop\'.\'dbo\'.\'input1\' AS t0";
            var expectedManifest =
                @"{'metadata':{'dbt_schema_version':'https://schemas.getdbt.com/dbt/manifest/v12.json','dbt_version':'1.10.0','generated_at':'2026-10-02T12:00:00.000000Z','invocation_id':'<ID>','env':{},'project_name':'flowtide','adapter_type':'postgres'}," +
                @"'nodes':{'model.flowtide.mssql__shop__dbo__output':{'database':'shop','schema':'dbo','name':'mssql__shop__dbo__output','resource_type':'model','package_name':'flowtide'," +
                @"'path':'mssql/mssql__shop__dbo__output.sql','original_file_path':'models/mssql/mssql__shop__dbo__output.sql','unique_id':'model.flowtide.mssql__shop__dbo__output'," +
                @"'fqn':['flowtide','mssql','mssql__shop__dbo__output'],'alias':'output','checksum':{'name':'sha256','checksum':'8606654d84ff9fd3df3063efedae1d4989eedf5d1045965b66a082753e35101a'}," +
                @"'config':{'enabled':true,'materialized':'incremental'},'tags':[],'description':'','meta':{'flowtide_streams':'window_stream'}," +
                @"'columns':{'c1':{'name':'c1','description':'','meta':{'flowtide_inputs':'mssql:shop.dbo.input1.b INDIRECT/GROUP_BY; mssql:shop.dbo.input1.c INDIRECT/SORT; mssql:shop.dbo.input1.a DIRECT/AGGREGATION'},'data_type':'bigint','tags':[]}," +
                @"'c2':{'name':'c2','description':'','meta':{'flowtide_inputs':'mssql:shop.dbo.input1.b DIRECT/IDENTITY'},'data_type':'bigint','tags':[]}}," +
                @"'relation_name':'\'shop\'.\'dbo\'.\'output\'','raw_code':'<CODE>','language':'sql','refs':[],'sources':[['mssql_shop_dbo','input1']]," +
                @"'depends_on':{'macros':[],'nodes':['source.flowtide.mssql_shop_dbo.input1']},'compiled':true,'compiled_code':'<CODE>'}}," +
                @"'sources':{'source.flowtide.mssql_shop_dbo.input1':{'database':'shop','schema':'dbo','name':'input1','resource_type':'source','package_name':'flowtide'," +
                @"'path':'models/flowtide_sources.yml','original_file_path':'models/flowtide_sources.yml','unique_id':'source.flowtide.mssql_shop_dbo.input1'," +
                @"'fqn':['flowtide','mssql_shop_dbo','input1'],'source_name':'mssql_shop_dbo','source_description':'','loader':'flowtide','identifier':'input1','description':''," +
                @"'columns':{'a':{'name':'a','description':'','meta':{},'data_type':'bigint','tags':[]},'b':{'name':'b','description':'','meta':{},'data_type':'bigint','tags':[]}," +
                @"'c':{'name':'c','description':'','meta':{},'data_type':'text','tags':[]}},'meta':{'flowtide_streams':'window_stream'},'source_meta':{},'tags':[],'config':{'enabled':true}," +
                @"'relation_name':'\'shop\'.\'dbo\'.\'input1\''}}," +
                @"'macros':{},'docs':{},'exposures':{},'metrics':{},'groups':{},'selectors':{},'disabled':{}," +
                @"'parent_map':{'model.flowtide.mssql__shop__dbo__output':['source.flowtide.mssql_shop_dbo.input1'],'source.flowtide.mssql_shop_dbo.input1':[]}," +
                @"'child_map':{'model.flowtide.mssql__shop__dbo__output':[],'source.flowtide.mssql_shop_dbo.input1':['model.flowtide.mssql__shop__dbo__output']}," +
                @"'group_map':{},'saved_queries':{},'semantic_models':{},'unit_tests':{}}";
            Assert.Equal(Json(expectedManifest.Replace("<CODE>", code)), manifest.Replace(invocationId, "<ID>"));

            var stats = @"'stats':{'has_stats':{'id':'has_stats','label':'Has Stats?','value':false,'include':false,'description':'Indicates whether there are statistics for this table'}}";
            var expectedCatalog =
                @"{'metadata':{'dbt_schema_version':'https://schemas.getdbt.com/dbt/catalog/v1.json','dbt_version':'1.10.0','generated_at':'2026-10-02T12:00:00.000000Z','invocation_id':'<ID>','env':{}}," +
                @"'nodes':{'model.flowtide.mssql__shop__dbo__output':{'metadata':{'type':'BASE TABLE','schema':'dbo','name':'output','database':'shop','comment':null,'owner':null}," +
                @"'columns':{'c1':{'type':'bigint','index':1,'name':'c1','comment':null},'c2':{'type':'bigint','index':2,'name':'c2','comment':null}},<STATS>,'unique_id':'model.flowtide.mssql__shop__dbo__output'}}," +
                @"'sources':{'source.flowtide.mssql_shop_dbo.input1':{'metadata':{'type':'BASE TABLE','schema':'dbo','name':'input1','database':'shop','comment':null,'owner':null}," +
                @"'columns':{'a':{'type':'bigint','index':1,'name':'a','comment':null},'b':{'type':'bigint','index':2,'name':'b','comment':null},'c':{'type':'text','index':3,'name':'c','comment':null}}," +
                @"<STATS>,'unique_id':'source.flowtide.mssql_shop_dbo.input1'}},'errors':null}";
            Assert.Equal(Json(expectedCatalog.Replace("<STATS>", stats)), Text(store.GetCatalog()).Replace(invocationId, "<ID>"));
        }

        [Fact]
        public void NodesHaveRequiredKeys()
        {
            var store = Store();
            var (a, b) = Chain();
            store.Register(a, "a");
            store.Register(b, "b");

            var root = Parse(store.GetManifest());

            var models = root.GetProperty("nodes").EnumerateObject().ToList();
            Assert.Equal(2, models.Count);
            foreach (var model in models)
            {
                Assert.Equal(ModelKeys, Keys(model.Value));
                Assert.Equal(model.Name, model.Value.GetProperty("unique_id").GetString());
                Assert.Equal("model", model.Value.GetProperty("resource_type").GetString());
                Assert.Equal("sql", model.Value.GetProperty("language").GetString());
                Assert.True(model.Value.GetProperty("compiled").GetBoolean());
                Assert.Equal(model.Value.GetProperty("compiled_code").GetString(), model.Value.GetProperty("raw_code").GetString());
                Assert.Equal(JsonValueKind.String, model.Value.GetProperty("schema").ValueKind);
                Assert.Equal(["enabled", "materialized"], Keys(model.Value.GetProperty("config")));
                Assert.Equal("incremental", model.Value.GetProperty("config").GetProperty("materialized").GetString());
                Assert.Equal(["macros", "nodes"], Keys(model.Value.GetProperty("depends_on")));
            }

            var source = Assert.Single(root.GetProperty("sources").EnumerateObject());
            Assert.Equal(SourceKeys, Keys(source.Value));
            Assert.Equal("source", source.Value.GetProperty("resource_type").GetString());
            Assert.False(source.Value.TryGetProperty("alias", out _));
            Assert.False(source.Value.TryGetProperty("compiled_code", out _));
            Assert.False(source.Value.TryGetProperty("depends_on", out _));
        }

        [Fact]
        public void ColumnsHaveDataTypeAndMetaIsFlat()
        {
            var store = Store();
            var (a, b) = Chain();
            store.Register(a, "a");
            store.Register(b, "b");
            store.Register(Snapshot(
                [Input("pg", "s", [Col("x", new Int64Type())])],
                [Output("pg", "anyout", [Col("z")], new() { ["z"] = [Identity("pg", "s", "x")] }, upstream: ["s"])]), "c");

            var root = Parse(store.GetManifest());

            var nodes = root.GetProperty("nodes").EnumerateObject().Concat(root.GetProperty("sources").EnumerateObject()).ToList();
            Assert.Equal(4, nodes.Count);
            foreach (var node in nodes)
            {
                AssertMetaIsFlat(node.Value.GetProperty("meta"));
                foreach (var column in node.Value.GetProperty("columns").EnumerateObject())
                {
                    Assert.Equal(["name", "description", "meta", "data_type", "tags"], Keys(column.Value));
                    Assert.Equal(column.Name, column.Value.GetProperty("name").GetString());
                    AssertMetaIsFlat(column.Value.GetProperty("meta"));
                }
            }
            var anyColumn = root.GetProperty("nodes").GetProperty("model.flowtide.pg__anyout").GetProperty("columns").GetProperty("z");
            Assert.Equal(JsonValueKind.Null, anyColumn.GetProperty("data_type").ValueKind);
            var catalogColumn = Parse(store.GetCatalog()).GetProperty("nodes").GetProperty("model.flowtide.pg__anyout").GetProperty("columns").GetProperty("z");
            Assert.Equal("unknown", catalogColumn.GetProperty("type").GetString());
        }

        private static void AssertMetaIsFlat(JsonElement meta)
        {
            foreach (var property in meta.EnumerateObject())
            {
                Assert.Equal(JsonValueKind.String, property.Value.ValueKind);
            }
        }

        [Fact]
        public void RegistrationOrderDoesNotChangeBytes()
        {
            var (a, b) = Chain();
            var first = Store();
            first.Register(a, "a");
            first.Register(b, "b");
            var second = Store();
            second.Register(b, "b");
            second.Register(a, "a");

            Assert.Equal(first.GetManifest().Utf8Json.ToArray(), second.GetManifest().Utf8Json.ToArray());
            Assert.Equal(first.GetManifest().ETag, second.GetManifest().ETag);
            Assert.Equal(first.GetCatalog().Utf8Json.ToArray(), second.GetCatalog().Utf8Json.ToArray());
        }

        [Fact]
        public void ArtifactIsCachedUntilRegister()
        {
            var store = Store();
            store.Register(Simple("s1", "t1"), "a");

            var manifest = store.GetManifest();
            Assert.Same(manifest, store.GetManifest());
            Assert.Same(store.GetCatalog(), store.GetCatalog());
            Assert.True(store.TryGetManifest("a", out var perStream));
            Assert.True(store.TryGetManifest("a", out var perStreamAgain));
            Assert.Same(perStream, perStreamAgain);
            Assert.Equal(1, store.Version);

            store.Register(Simple("s2", "t2"), "b");

            Assert.Equal(2, store.Version);
            var updated = store.GetManifest();
            Assert.NotSame(manifest, updated);
            Assert.NotEqual(manifest.ETag, updated.ETag);
            Assert.StartsWith("\"", updated.ETag);
            Assert.Equal($"\"{DbtHashing.Sha256Hex(updated.Utf8Json.Span)}\"", updated.ETag);
        }

        [Fact]
        public void InvocationIdIsUuidV8SharedWithCatalog()
        {
            var store = Store();
            store.Register(Simple("s", "t"), "a");

            var manifest = Parse(store.GetManifest()).GetProperty("metadata");
            var catalog = Parse(store.GetCatalog()).GetProperty("metadata");

            var invocationId = manifest.GetProperty("invocation_id").GetString()!;
            Assert.True(Guid.TryParseExact(invocationId, "D", out _));
            Assert.Equal(invocationId.ToLowerInvariant(), invocationId);
            Assert.Equal('8', invocationId[14]);
            Assert.Contains(invocationId[19], "89ab");
            Assert.Equal(invocationId, catalog.GetProperty("invocation_id").GetString());
            Assert.Equal(manifest.GetProperty("generated_at").GetString(), catalog.GetProperty("generated_at").GetString());

            // Same content in a new store gives the same id.
            var again = Store();
            again.Register(Simple("s", "t"), "a");
            Assert.Equal(invocationId, Parse(again.GetManifest()).GetProperty("metadata").GetProperty("invocation_id").GetString());
        }

        [Fact]
        public void GeneratedAtIsLatestBuildTime()
        {
            var store = Store();
            store.Register(Simple("s1", "t1", BuildTime.AddSeconds(5).AddTicks(1234567)), "late");
            store.Register(Simple("s2", "t2", BuildTime), "early");

            Assert.Equal("2026-10-02T12:00:05.123456Z", Parse(store.GetManifest()).GetProperty("metadata").GetProperty("generated_at").GetString());
            Assert.True(store.TryGetManifest("early", out var early));
            Assert.Equal("2026-10-02T12:00:00.000000Z", Parse(early).GetProperty("metadata").GetProperty("generated_at").GetString());
        }

        [Fact]
        public void SubstreamsUnionUnderLogicalName()
        {
            var store = Store();
            store.Register(Simple("s1", "t1", substream: "sub1"), "orders");
            store.Register(Simple("s2", "t2", substream: "sub2"), "orders");

            Assert.True(store.TryGetManifest("orders", out var manifest));
            Assert.Equal(["model.flowtide.pg__t1", "model.flowtide.pg__t2"], Keys(Parse(manifest).GetProperty("nodes")));
            Assert.False(store.TryGetManifest("sub1", out var missing));
            Assert.Null(missing);
            Assert.False(store.TryGetCatalog("unknown", out _));
            Assert.True(store.TryGetCatalog("orders", out var catalog));
            Assert.Equal(2, Keys(Parse(catalog).GetProperty("nodes")).Count);
        }

        [Fact]
        public void LastRegistrationWinsPerSubstream()
        {
            var store = Store();
            store.Register(Simple("s", "old", substream: "sub"), "orders");
            store.Register(Simple("s", "new", substream: "sub"), "orders");

            Assert.Equal(["model.flowtide.pg__new"], Keys(Parse(store.GetManifest()).GetProperty("nodes")));
        }

        [Fact]
        public void SingleStreamCombinedEqualsPerStream()
        {
            var store = Store();
            var (a, _) = Chain();
            store.Register(a, "a");

            Assert.True(store.TryGetManifest("a", out var manifest));
            Assert.True(store.TryGetCatalog("a", out var catalog));
            Assert.Equal(store.GetManifest().Utf8Json.ToArray(), manifest.Utf8Json.ToArray());
            Assert.Equal(store.GetCatalog().Utf8Json.ToArray(), catalog.Utf8Json.ToArray());
        }

        [Fact]
        public void TableWrittenInScopeIsModel()
        {
            var store = Store();
            var (a, b) = Chain();
            store.Register(a, "a");
            store.Register(b, "b");

            var combined = Parse(store.GetManifest());
            Assert.Equal(["model.flowtide.pg__t", "model.flowtide.pg__u"], Keys(combined.GetProperty("nodes")));
            Assert.Equal(["source.flowtide.pg.s"], Keys(combined.GetProperty("sources")));
            var u = combined.GetProperty("nodes").GetProperty("model.flowtide.pg__u");
            Assert.Equal(["model.flowtide.pg__t"], u.GetProperty("depends_on").GetProperty("nodes").EnumerateArray().Select(x => x.GetString()));
            var reference = Assert.Single(u.GetProperty("refs").EnumerateArray());
            Assert.Equal("pg__t", reference.GetProperty("name").GetString());
            Assert.Equal(JsonValueKind.Null, reference.GetProperty("package").ValueKind);
            Assert.Equal(JsonValueKind.Null, reference.GetProperty("version").ValueKind);
            Assert.Empty(u.GetProperty("sources").EnumerateArray());
            Assert.Equal(["model.flowtide.pg__u"], combined.GetProperty("child_map").GetProperty("model.flowtide.pg__t").EnumerateArray().Select(x => x.GetString()));

            // Another stream's output is a source here.
            Assert.True(store.TryGetManifest("b", out var perStream));
            var streamB = Parse(perStream);
            Assert.Equal(["model.flowtide.pg__u"], Keys(streamB.GetProperty("nodes")));
            Assert.Equal(["source.flowtide.pg.t"], Keys(streamB.GetProperty("sources")));
            var source = Assert.Single(streamB.GetProperty("nodes").GetProperty("model.flowtide.pg__u").GetProperty("sources").EnumerateArray());
            Assert.Equal(["pg", "t"], source.EnumerateArray().Select(x => x.GetString()));
            Assert.Empty(streamB.GetProperty("nodes").GetProperty("model.flowtide.pg__u").GetProperty("refs").EnumerateArray());
        }

        [Fact]
        public void WritersAcrossStreamsMergeIntoOneModel()
        {
            var store = Store();
            store.Register(Snapshot(
                [Input("pg", "s1", [Col("x")])],
                [Output("pg", "t", [Col("a"), Col("b")], new()
                {
                    ["a"] = [Identity("pg", "s1", "x")],
                    ["b"] = [Identity("pg", "s1", "x")]
                }, upstream: ["s1"])]), "b_stream");
            store.Register(Snapshot(
                [Input("pg", "s2", [Col("y")])],
                [Output("pg", "t", [Col("a"), Col("c", new StringType())], new()
                {
                    ["a"] = [Field("pg", "s2", "y", LineageTransformationType.Direct, LineageTransformationSubtype.Transformation)],
                    ["c"] = [Identity("pg", "s2", "y")]
                }, upstream: ["s2"])]), "a_stream");

            var root = Parse(store.GetManifest());

            var model = Assert.Single(root.GetProperty("nodes").EnumerateObject()).Value;
            Assert.Equal("a_stream,b_stream", model.GetProperty("meta").GetProperty("flowtide_streams").GetString());
            Assert.Equal(["a", "c", "b"], Keys(model.GetProperty("columns")));
            Assert.Equal("pg:s2.y DIRECT/TRANSFORMATION; pg:s1.x DIRECT/IDENTITY", model.GetProperty("columns").GetProperty("a").GetProperty("meta").GetProperty("flowtide_inputs").GetString());
            var sql = model.GetProperty("compiled_code").GetString()!;
            Assert.DoesNotContain("UNION", sql);
            Assert.Equal(Sql(
                "SELECT",
                "  COALESCE(t0.\"y\", t1.\"x\") AS \"a\",",
                "  t0.\"y\" AS \"c\",",
                "  t1.\"x\" AS \"b\"",
                "FROM \"s2\" AS t0",
                "CROSS JOIN \"s1\" AS t1"), sql);
            Assert.Equal(["source.flowtide.pg.s2", "source.flowtide.pg.s1"], model.GetProperty("depends_on").GetProperty("nodes").EnumerateArray().Select(x => x.GetString()));
        }

        [Fact]
        public void CycleEdgeIsDroppedAndRecorded()
        {
            var store = Store();
            store.Register(Snapshot(
                [Input("pg", "u", [Col("y")])],
                [Output("pg", "t", [Col("x")], new() { ["x"] = [Identity("pg", "u", "y")] }, upstream: ["u"])]), "a");
            store.Register(Snapshot(
                [Input("pg", "t", [Col("x")])],
                [Output("pg", "u", [Col("y")], new() { ["y"] = [Identity("pg", "t", "x")] }, upstream: ["t"])]), "b");

            var root = Parse(store.GetManifest());

            var t = root.GetProperty("nodes").GetProperty("model.flowtide.pg__t");
            var u = root.GetProperty("nodes").GetProperty("model.flowtide.pg__u");
            Assert.Equal(["model.flowtide.pg__u"], t.GetProperty("depends_on").GetProperty("nodes").EnumerateArray().Select(x => x.GetString()));
            Assert.False(t.GetProperty("meta").TryGetProperty("flowtide_dropped_dependencies", out _));
            Assert.Empty(u.GetProperty("depends_on").GetProperty("nodes").EnumerateArray());
            Assert.Empty(u.GetProperty("refs").EnumerateArray());
            Assert.Equal("model.flowtide.pg__t", u.GetProperty("meta").GetProperty("flowtide_dropped_dependencies").GetString());
            // The SQL still names the table.
            Assert.Contains("FROM \"t\" AS t0", u.GetProperty("compiled_code").GetString());
            Assert.Empty(root.GetProperty("parent_map").GetProperty("model.flowtide.pg__u").EnumerateArray());
            Assert.Empty(root.GetProperty("child_map").GetProperty("model.flowtide.pg__t").EnumerateArray());
        }

        [Fact]
        public void SelfReferenceIsRecorded()
        {
            var store = Store();
            store.Register(Snapshot(
                [Input("pg", "t", [Col("x")]), Input("pg", "s", [Col("y")])],
                [Output("pg", "t", [Col("x")], new() { ["x"] = [Identity("pg", "t", "x"), Identity("pg", "s", "y")] }, upstream: ["t", "s"])]), "a");

            var model = Node(store.GetManifest(), "model.flowtide.pg__t");

            Assert.Equal(["source.flowtide.pg.s"], model.GetProperty("depends_on").GetProperty("nodes").EnumerateArray().Select(x => x.GetString()));
            Assert.Equal("model.flowtide.pg__t", model.GetProperty("meta").GetProperty("flowtide_dropped_dependencies").GetString());
            Assert.Equal("pg:t.x DIRECT/IDENTITY; pg:s.y DIRECT/IDENTITY", model.GetProperty("columns").GetProperty("x").GetProperty("meta").GetProperty("flowtide_inputs").GetString());
        }

        [Fact]
        public void RelationCollisionIsRecorded()
        {
            var store = Store();
            store.Register(Snapshot(
                [Input("mssql", "shop.dbo.orders", [Col("id")]), Input("mssql", "shop.dbo.customers", [Col("name")])],
                [Output("starrocks", "SHOP.dbo.Orders", [Col("id"), Col("name")], new()
                {
                    ["id"] = [Identity("mssql", "shop.dbo.orders", "id")],
                    ["name"] = [Identity("mssql", "shop.dbo.customers", "name")]
                }, upstream: ["shop.dbo.orders", "shop.dbo.customers"])]), "a");

            var manifest = store.GetManifest();
            var model = Node(manifest, "model.flowtide.starrocks__shop__dbo__orders");
            var orders = Node(manifest, "source.flowtide.mssql_shop_dbo.orders");
            var customers = Node(manifest, "source.flowtide.mssql_shop_dbo.customers");

            // Case differs, catalogs still see one relation.
            Assert.Equal("source.flowtide.mssql_shop_dbo.orders", model.GetProperty("meta").GetProperty("flowtide_relation_collision").GetString());
            Assert.Equal("model.flowtide.starrocks__shop__dbo__orders", orders.GetProperty("meta").GetProperty("flowtide_relation_collision").GetString());
            Assert.False(customers.GetProperty("meta").TryGetProperty("flowtide_relation_collision", out _));
        }

        [Fact]
        public void UnreferencedInputIsNotASource()
        {
            var store = Store();
            store.Register(Snapshot(
                [Input("pg", "used", [Col("a")]), Input("pg", "unused", [Col("b")])],
                [Output("pg", "t", [Col("a")], new() { ["a"] = [Identity("pg", "used", "a")] }, upstream: ["used"])]), "a");

            Assert.Equal(["source.flowtide.pg.used"], Keys(Parse(store.GetManifest()).GetProperty("sources")));
        }

        [Fact]
        public void DefaultExcludedNamespacesAreDropped()
        {
            var store = Store();
            store.Register(Snapshot(
                [Input("test", "hidden", [Col("a")]), Input("pg", "s", [Col("b")])],
                [
                    Output("console", "c", [Col("a")], new() { ["a"] = [Identity("pg", "s", "b")] }, upstream: ["s"]),
                    Output("blackhole", "bh", [Col("a")], new() { ["a"] = [Identity("pg", "s", "b")] }, upstream: ["s"]),
                    Output("pg", "t", [Col("a")], new() { ["a"] = [Identity("test", "hidden", "a")] }, upstream: ["hidden"])
                ]), "a");

            var root = Parse(store.GetManifest());

            Assert.Equal(["model.flowtide.pg__t"], Keys(root.GetProperty("nodes")));
            Assert.Empty(Keys(root.GetProperty("sources")));
            var column = root.GetProperty("nodes").GetProperty("model.flowtide.pg__t").GetProperty("columns").GetProperty("a");
            Assert.Equal("test:hidden.a DIRECT/IDENTITY", column.GetProperty("meta").GetProperty("flowtide_inputs").GetString());

            var included = Store(o => o.ExcludedNamespaces.Clear());
            included.Register(Simple("s", "t"), "a");
            Assert.Single(Keys(Parse(included.GetManifest()).GetProperty("nodes")));
        }

        [Theory]
        [InlineData(DbtSqlDialect.Postgres, "postgres")]
        [InlineData(DbtSqlDialect.TSql, "tsql")]
        [InlineData(DbtSqlDialect.Snowflake, "snowflake")]
        public void DialectSetsAdapterType(DbtSqlDialect dialect, string adapterType)
        {
            var store = Store(o => o.SqlDialect = dialect);
            store.Register(Simple("s", "t"), "a");

            var actual = Parse(store.GetManifest()).GetProperty("metadata").GetProperty("adapter_type").GetString();

            Assert.Equal(adapterType, actual);
            Assert.NotEqual("sqlserver", actual);
        }

        [Fact]
        public void EveryDialectHasAnAdapterType()
        {
            foreach (var dialect in Enum.GetValues<DbtSqlDialect>())
            {
                Assert.NotEqual("sqlserver", DbtSqlDialectInfo.For(dialect).AdapterType);
            }
            Assert.Throws<ArgumentOutOfRangeException>(() => Store(o => o.SqlDialect = (DbtSqlDialect)42));
        }

        public static TheoryData<string, string?, string?, string?> TypeMappings()
        {
            return new TheoryData<string, string?, string?, string?>()
            {
                { "string", "text", "nvarchar(max)", "varchar" },
                { "int", "integer", "int", "integer" },
                { "bigint", "bigint", "bigint", "bigint" },
                { "boolean", "boolean", "bit", "boolean" },
                { "date", "date", "date", "date" },
                { "timestamp", "timestamptz", "datetimeoffset", "timestamp_tz" },
                { "float", "real", "real", "float" },
                { "double", "double precision", "float", "double" },
                { "binary", "bytea", "varbinary(max)", "binary" },
                { "array", "jsonb", "nvarchar(max)", "array" },
                { "map", "jsonb", "nvarchar(max)", "object" },
                { "struct", "jsonb", "nvarchar(max)", "object" },
                { "decimal", "numeric(10,2)", "decimal(10,2)", "number(10,2)" },
                { "any", null, null, null },
                { "null", null, null, null }
            };
        }

        [Theory]
        [MemberData(nameof(TypeMappings))]
        public void TypeMappingPerDialect(string typeName, string? postgres, string? tsql, string? snowflake)
        {
            SubstraitBaseType type = typeName switch
            {
                "string" => new StringType(),
                "int" => new Int32Type(),
                "bigint" => new Int64Type(),
                "boolean" => new BoolType(),
                "date" => new DateType(),
                "timestamp" => new TimestampType(),
                "float" => new Fp32Type(),
                "double" => new Fp64Type(),
                "binary" => new BinaryType(),
                "array" => new ListType(new StringType()),
                "map" => new MapType(new StringType(), new StringType()),
                "struct" => new NamedStruct() { Names = ["x"], Struct = new Struct() { Types = [new StringType()] } },
                "decimal" => new DecimalType() { Precision = 10, Scale = 2 },
                "null" => NullType.Instance,
                _ => AnyType.Instance
            };

            Assert.Equal(postgres, DbtSqlDialectInfo.For(DbtSqlDialect.Postgres).MapDataType(type));
            Assert.Equal(tsql, DbtSqlDialectInfo.For(DbtSqlDialect.TSql).MapDataType(type));
            Assert.Equal(snowflake, DbtSqlDialectInfo.For(DbtSqlDialect.Snowflake).MapDataType(type));

            var store = Store(o => o.SqlDialect = DbtSqlDialect.TSql);
            store.Register(Snapshot([], [Output("pg", "t", [Col("c", type)], new())]), "a");
            var column = Node(store.GetManifest(), "model.flowtide.pg__t").GetProperty("columns").GetProperty("c");
            Assert.Equal(tsql, column.GetProperty("data_type").GetString());
        }

        [Fact]
        public void CatalogListsEveryNode()
        {
            var store = Store();
            var (a, b) = Chain();
            store.Register(a, "a");
            store.Register(b, "b");
            store.Register(Snapshot(
                [Input("pg", "s", [Col("x", new Int64Type()), Col("w", new StringType())])],
                [Output("pg", "v", [Col("x", new Int64Type()), Col("w", new StringType()), Col("q")], new() { ["x"] = [Identity("pg", "s", "x")] }, upstream: ["s"])]), "c");

            var manifest = Parse(store.GetManifest());
            var catalog = Parse(store.GetCatalog());

            Assert.Equal(Keys(manifest.GetProperty("nodes")), Keys(catalog.GetProperty("nodes")));
            Assert.Equal(Keys(manifest.GetProperty("sources")), Keys(catalog.GetProperty("sources")));
            var v = catalog.GetProperty("nodes").GetProperty("model.flowtide.pg__v");
            Assert.Equal(["metadata", "columns", "stats", "unique_id"], Keys(v));
            Assert.Equal("BASE TABLE", v.GetProperty("metadata").GetProperty("type").GetString());
            Assert.Equal("v", v.GetProperty("metadata").GetProperty("name").GetString());
            Assert.Equal(JsonValueKind.Null, v.GetProperty("metadata").GetProperty("database").ValueKind);
            Assert.Equal([1, 2, 3], v.GetProperty("columns").EnumerateObject().Select(x => x.Value.GetProperty("index").GetInt32()));
            Assert.Equal(["bigint", "text", "unknown"], v.GetProperty("columns").EnumerateObject().Select(x => x.Value.GetProperty("type").GetString()));
            var source = catalog.GetProperty("sources").GetProperty("source.flowtide.pg.s");
            Assert.Equal(["x", "w"], Keys(source.GetProperty("columns")));
        }

        [Fact]
        public void WarmingUpUntilExpectedStreamsRegister()
        {
            var time = new ManualTimeProvider();
            var store = new DbtManifestStore(new DbtManifestOptions(), time);
            Assert.False(store.IsWarmingUp);

            store.ExpectStream("a");
            store.ExpectStream("b");
            Assert.True(store.IsWarmingUp);

            store.Register(Simple("s", "t"), "a");
            Assert.True(store.IsWarmingUp);

            store.Register(Simple("s", "u"), "b");
            Assert.False(store.IsWarmingUp);
        }

        [Fact]
        public void WarmingUpStopsAfterTimeout()
        {
            var time = new ManualTimeProvider();
            var store = new DbtManifestStore(new DbtManifestOptions() { WarmupTimeout = TimeSpan.FromSeconds(30) }, time);
            store.ExpectStream("a");

            time.Ticks = TimeSpan.FromSeconds(29).Ticks;
            Assert.True(store.IsWarmingUp);

            time.Ticks = TimeSpan.FromSeconds(30).Ticks;
            Assert.False(store.IsWarmingUp);
        }

        [Fact]
        public void ResolverFailureIsNotCached()
        {
            var fail = true;
            var store = Store(o => o.RelationResolver = context => fail ? throw new InvalidDataException("bad") : null);
            store.Register(Simple("s", "t"), "a");

            var e = Assert.Throws<InvalidOperationException>(() => store.GetManifest());
            Assert.IsType<InvalidDataException>(e.InnerException);
            Assert.Throws<InvalidOperationException>(() => store.TryGetCatalog("a", out _));

            fail = false;
            Assert.Single(Keys(Parse(store.GetManifest()).GetProperty("nodes")));
            Assert.True(store.TryGetCatalog("a", out _));
        }

        [Fact]
        public void OptionsChangedAfterCreationAreIgnored()
        {
            var options = new DbtManifestOptions();
            var store = new DbtManifestStore(options);
            options.ProjectName = "changed";
            options.SqlDialect = DbtSqlDialect.TSql;
            options.ExcludedNamespaces.Add("pg");
            options.MapNamespace("pg", "db");
            store.Register(Simple("s", "t"), "a");

            var root = Parse(store.GetManifest());

            Assert.Equal("flowtide", root.GetProperty("metadata").GetProperty("project_name").GetString());
            Assert.Equal("postgres", root.GetProperty("metadata").GetProperty("adapter_type").GetString());
            Assert.Equal(["model.flowtide.pg__t"], Keys(root.GetProperty("nodes")));
        }
    }
}
