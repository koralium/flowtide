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

using FlowtideDotNet.Core.Lineage.Internal;
using FlowtideDotNet.Core.Lineage.Internal.Models;
using FlowtideDotNet.Lineage.DataHub.Internal;
using FlowtideDotNet.Substrait.Type;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;
using static FlowtideDotNet.Lineage.DataHub.Tests.LineageTestData;

namespace FlowtideDotNet.Lineage.DataHub.Tests
{
    public class DataHubLineageStoreTests
    {
        private const string FlowA = "urn:li:dataFlow:(flowtide,a,PROD)";
        private const string DatasetS = "urn:li:dataset:(urn:li:dataPlatform:postgres,db.public.s,PROD)";
        private const string DatasetT = "urn:li:dataset:(urn:li:dataPlatform:postgres,db.public.t,PROD)";
        private const string DatasetU = "urn:li:dataset:(urn:li:dataPlatform:postgres,db.public.u,PROD)";
        private const string JobT = "urn:li:dataJob:(urn:li:dataFlow:(flowtide,a,PROD),postgres.db.public.t)";
        private const string Platform = "urn:li:dataPlatform:flowtide";

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
                [Input("postgres", "s", [Col("x", new Int64Type())])],
                [Output("postgres", "t", [Col("x", new Int64Type())], new() { ["x"] = [Identity("postgres", "s", "x")] }, upstream: ["s"])]);
            var b = Snapshot(
                [Input("postgres", "t", [Col("x", new Int64Type())])],
                [Output("postgres", "u", [Col("y", new Int64Type())], new() { ["y"] = [Identity("postgres", "t", "x")] }, upstream: ["t"])]);
            return (a, b);
        }

        // Lineage only, runs have their own tests.
        private static DataHubLineageStore Store(Action<DataHubLineageOptions>? configure = null)
        {
            var options = new DataHubLineageOptions() { IncludeRuns = false };
            options.MapNamespace("postgres", m =>
            {
                m.Database = "db";
                m.DefaultSchema = "public";
            });
            configure?.Invoke(options);
            return new DataHubLineageStore(options);
        }

        private static string Text(DataHubSnapshot snapshot, string urn)
        {
            Assert.True(snapshot.TryGetEntity(urn, out var json), urn);
            return Encoding.UTF8.GetString(json.Span);
        }

        private static JsonElement Aspects(DataHubSnapshot snapshot, string urn)
        {
            return JsonDocument.Parse(Text(snapshot, urn)).RootElement.GetProperty("aspects");
        }

        private static JsonElement Aspect(DataHubSnapshot snapshot, string urn, string aspectName)
        {
            return Aspects(snapshot, urn).GetProperty(aspectName).GetProperty("value");
        }

        private static List<string> FieldPaths(DataHubSnapshot snapshot, string urn)
        {
            return Aspect(snapshot, urn, "schemaMetadata").GetProperty("fields").EnumerateArray().Select(x => x.GetProperty("fieldPath").GetString()!).ToList();
        }

        private static List<JsonElement> FineGrainedLineages(DataHubSnapshot snapshot, string jobUrn)
        {
            var inputOutput = Aspect(snapshot, jobUrn, "dataJobInputOutput");
            return inputOutput.TryGetProperty("fineGrainedLineages", out var lineages) ? lineages.EnumerateArray().ToList() : [];
        }

        private static List<string> Strings(JsonElement array)
        {
            return array.EnumerateArray().Select(x => x.GetString()!).ToList();
        }

        private static string SchemaField(string datasetUrn, string field)
        {
            return $"urn:li:schemaField:({datasetUrn},{field})";
        }

        // Single quotes keep the golden JSON readable.
        private static string Json(string text)
        {
            return text.Replace('\'', '"');
        }

        [Fact]
        public void EmptyStoreHasNoEntities()
        {
            var snapshot = new DataHubLineageStore().GetSnapshot();

            Assert.Empty(snapshot.Urns);
            Assert.False(snapshot.TryGetEntity(FlowA, out _));
        }

        [Fact]
        public void ChainServesFlowsJobsAndDatasetsInOrdinalOrder()
        {
            var store = Store();
            var (a, b) = Chain();
            store.Register(a, "a");
            store.Register(b, "b");

            var snapshot = store.GetSnapshot();

            Assert.Equal(
                [
                    FlowA,
                    "urn:li:dataFlow:(flowtide,b,PROD)",
                    JobT,
                    "urn:li:dataJob:(urn:li:dataFlow:(flowtide,b,PROD),postgres.db.public.u)",
                    Platform,
                    DatasetS,
                    DatasetT,
                    DatasetU
                ],
                snapshot.Urns);
        }

        [Fact]
        public void EntitiesMatchTheGmsResponseShape()
        {
            var store = Store();
            store.Register(Chain().A, "a");

            var snapshot = store.GetSnapshot();

            var status = "'status':{'name':'status','type':'VERSIONED','version':0,'value':{'removed':false}}";
            Assert.Equal(
                Json($"{{'entityName':'dataFlow','urn':'{FlowA}','aspects':{{" +
                    "'dataFlowInfo':{'name':'dataFlowInfo','type':'VERSIONED','version':0,'value':{'customProperties':{},'name':'a','env':'PROD'}}," +
                    status + "}}"),
                Text(snapshot, FlowA));
            Assert.Equal(
                Json($"{{'entityName':'dataJob','urn':'{JobT}','aspects':{{" +
                    "'dataJobInfo':{'name':'dataJobInfo','type':'VERSIONED','version':0,'value':{'customProperties':{'flowtide.namespace':'postgres','flowtide.table':'t'}," +
                    $"'name':'db.public.t','type':{{'string':'STREAMING'}},'flowUrn':'{FlowA}','env':'PROD'}}}}," +
                    $"'dataJobInputOutput':{{'name':'dataJobInputOutput','type':'VERSIONED','version':0,'value':{{'inputDatasets':['{DatasetS}'],'outputDatasets':['{DatasetT}']," +
                    $"'fineGrainedLineages':[{{'upstreamType':'FIELD_SET','upstreams':['{SchemaField(DatasetS, "x")}'],'downstreamType':'FIELD','downstreams':['{SchemaField(DatasetT, "x")}']," +
                    "'transformOperation':'DIRECT:IDENTITY','confidenceScore':1}]}}," +
                    status + "}}"),
                Text(snapshot, JobT));
            Assert.Equal(
                Json($"{{'entityName':'dataset','urn':'{DatasetS}','aspects':{{" + status + "," +
                    "'schemaMetadata':{'name':'schemaMetadata','type':'VERSIONED','version':0,'value':{'schemaName':'db.public.s','platform':'urn:li:dataPlatform:postgres','version':0,'hash':''," +
                    "'platformSchema':{'com.linkedin.schema.OtherSchema':{'rawSchema':''}}," +
                    "'fields':[{'fieldPath':'x','nullable':false,'type':{'type':{'com.linkedin.schema.NumberType':{}}},'nativeDataType':'bigint'}]}}}}"),
                Text(snapshot, DatasetS));
        }

        [Fact]
        public void PlatformInfoMatchesDataHubPutPlatform()
        {
            var store = Store();
            store.Register(Chain().A, "a");

            // No status aspect, GMS rejects it on a data platform.
            Assert.Equal(
                Json($"{{'entityName':'dataPlatform','urn':'{Platform}','aspects':{{'dataPlatformInfo':{{'name':'dataPlatformInfo','type':'VERSIONED','version':0," +
                    "'value':{'name':'flowtide','displayName':'Flowtide','type':'OTHERS','datasetNameDelimiter':'.'," +
                    "'logoUrl':'https://raw.githubusercontent.com/koralium/flowtide/main/logo/flowtidelogo.svg'}}}}"),
                Text(store.GetSnapshot(), Platform));
        }

        [Theory]
        [InlineData(null)]
        [InlineData("")]
        public void PlatformLogoCanBeLeftOut(string? logoUrl)
        {
            var store = Store(o => o.PlatformLogoUrl = logoUrl);
            store.Register(Chain().A, "a");

            var info = Aspect(store.GetSnapshot(), Platform, "dataPlatformInfo");

            Assert.Equal("Flowtide", info.GetProperty("displayName").GetString());
            Assert.False(info.TryGetProperty("logoUrl", out _));
        }

        [Fact]
        public void PlatformLogoIsConfigurable()
        {
            var store = Store(o => o.PlatformLogoUrl = "https://example.com/flowtide.png");
            store.Register(Chain().A, "a");

            Assert.Equal("https://example.com/flowtide.png", Aspect(store.GetSnapshot(), Platform, "dataPlatformInfo").GetProperty("logoUrl").GetString());
        }

        [Fact]
        public void PlatformInfoCanBeTurnedOff()
        {
            var store = Store(o => o.IncludePlatformInfo = false);
            store.Register(Chain().A, "a");

            Assert.DoesNotContain(Platform, store.GetSnapshot().Urns);
        }

        [Fact]
        public void DatasetPlatformsAreNeverServed()
        {
            // A served mssql platform would replace DataHub's built-in logo and name.
            var store = Store(o => o.MapNamespace("mssql", m => m.PlatformInstance = "prod"));
            store.Register(Snapshot([Input("mssql", "s", [Col("x")]), Input("kafka", "k", [Col("x")])], []), "a");

            Assert.Equal([Platform], store.GetSnapshot().Urns.Where(x => x.StartsWith("urn:li:dataPlatform:", StringComparison.Ordinal)));
        }

        [Fact]
        public void SchemaFieldTypesUsePegasusUnionNames()
        {
            var store = Store();
            store.Register(Snapshot(
                [Input("postgres", "s", [
                    Col("b", new BoolType()),
                    Col("s", new StringType() { Nullable = true }),
                    Col("d", new DateType()),
                    Col("ts", new TimestampType()),
                    Col("bin", new BinaryType()),
                    Col("dec", new DecimalType() { Precision = 10, Scale = 2 }),
                    Col("l", new ListType(new Int64Type())),
                    Col("m", new MapType(new StringType(), new Int64Type())),
                    Col("any")])],
                []), "a");

            var fields = Aspect(store.GetSnapshot(), DatasetS, "schemaMetadata").GetProperty("fields").EnumerateArray().ToList();

            Assert.Equal(
                ["BooleanType", "StringType", "DateType", "TimeType", "BytesType", "NumberType", "ArrayType", "MapType", "NullType"],
                fields.Select(x => x.GetProperty("type").GetProperty("type").EnumerateObject().Single().Name.Replace("com.linkedin.schema.", "")));
            Assert.Equal(["boolean", "string", "date", "timestamp", "binary", "decimal(10, 2)", "array", "map", "any"], fields.Select(x => x.GetProperty("nativeDataType").GetString()));
            Assert.True(fields[1].GetProperty("nullable").GetBoolean());
        }

        [Fact]
        public void StreamWithoutOutputsStillHasAFlow()
        {
            var store = Store();
            store.Register(Snapshot([Input("postgres", "s", [Col("x")])], []), "readonly");

            Assert.Equal(["urn:li:dataFlow:(flowtide,readonly,PROD)", Platform, DatasetS], store.GetSnapshot().Urns);
        }

        [Theory]
        [InlineData("mssql", "orders", "shop.dbo.orders")]
        [InlineData("mssql", "sales.orders", "shop.sales.orders")]
        [InlineData("mssql", "other.sales.orders", "other.sales.orders")]
        [InlineData("kafka://broker:9092", "orders.v1", "orders.v1")]
        [InlineData("elasticsearch", "orders.2026", "orders.2026")]
        [InlineData("mongodb", "shop.orders", "shop.orders")]
        public void DatasetNamesFollowTheNamespaceRules(string ns, string table, string expectedName)
        {
            var store = Store(o => o.MapNamespace("mssql", m => m.Database = "shop"));
            store.Register(Snapshot([Input(ns, table, [Col("x")])], []), "a");

            var platform = LineageRelationNames.ShortNamespace(ns);
            Assert.Contains($"urn:li:dataset:(urn:li:dataPlatform:{platform},{expectedName},PROD)", store.GetSnapshot().Urns);
        }

        [Theory]
        [InlineData("postgresql", "postgres")]
        [InlineData("delta_table", "delta-lake")]
        [InlineData("SharePoint", "sharepoint")]
        public void BuiltInPlatformIds(string ns, string expectedPlatform)
        {
            var store = new DataHubLineageStore();
            store.Register(Snapshot([Input(ns, "tbl", [Col("x")])], []), "a");

            Assert.Contains($"urn:li:dataset:(urn:li:dataPlatform:{expectedPlatform},tbl,PROD)", store.GetSnapshot().Urns);
        }

        [Fact]
        public void NamespaceMappingSetsPlatformInstanceEnvAndCasing()
        {
            var store = Store(o => o.MapNamespace("mssql", m =>
            {
                m.Platform = "sqlserver";
                m.PlatformInstance = "Prod-Sql";
                m.Env = "dev";
                m.Database = "Shop";
                m.LowercaseNames = true;
                m.LowercaseColumns = true;
            }));
            store.Register(Snapshot(
                [Input("mssql", "Orders", [Col("OrderKey", new Int64Type())])],
                [Output("postgres", "t", [Col("Id", new Int64Type())], new() { ["Id"] = [Identity("mssql", "Orders", "OrderKey")] }, upstream: ["Orders"])]), "a");

            var snapshot = store.GetSnapshot();

            // DataHub lowercases the whole name, the instance urn keeps its casing.
            const string orders = "urn:li:dataset:(urn:li:dataPlatform:sqlserver,prod-sql.shop.dbo.orders,DEV)";
            Assert.Contains(orders, snapshot.Urns);
            Assert.Equal(["orderkey"], FieldPaths(snapshot, orders));
            var instance = Aspect(snapshot, orders, "dataPlatformInstance");
            Assert.Equal("urn:li:dataPlatform:sqlserver", instance.GetProperty("platform").GetString());
            Assert.Equal("urn:li:dataPlatformInstance:(urn:li:dataPlatform:sqlserver,Prod-Sql)", instance.GetProperty("instance").GetString());
            var lineage = Assert.Single(FineGrainedLineages(snapshot, JobT));
            Assert.Equal([SchemaField(orders, "orderkey")], Strings(lineage.GetProperty("upstreams")));
            // Output columns keep their casing outside the mapped namespace.
            Assert.Equal([SchemaField(DatasetT, "Id")], Strings(lineage.GetProperty("downstreams")));
        }

        [Fact]
        public void ExcludedNamespacesAreLeftOut()
        {
            var store = Store(o =>
            {
                o.ExcludedNamespaces.Add("test");
                o.ExcludedNamespaces.Add("console");
            });
            store.Register(Snapshot(
                [Input("postgres", "s", [Col("x")]), Input("test", "fixture", [Col("y")])],
                [
                    Output("postgres", "t", [Col("x"), Col("y")], new() { ["x"] = [Identity("postgres", "s", "x")], ["y"] = [Identity("test", "fixture", "y")] }, upstream: ["s", "fixture"]),
                    Output("console", "debug", [Col("x")], new() { ["x"] = [Identity("postgres", "s", "x")] }, upstream: ["s"])
                ]), "a");

            var snapshot = store.GetSnapshot();

            Assert.Equal([FlowA, JobT, Platform, DatasetS, DatasetT], snapshot.Urns);
            Assert.Equal([DatasetS], Strings(Aspect(snapshot, JobT, "dataJobInputOutput").GetProperty("inputDatasets")));
            Assert.Equal([SchemaField(DatasetT, "x")], FineGrainedLineages(snapshot, JobT).Select(x => Strings(x.GetProperty("downstreams")).Single()));
        }

        [Fact]
        public void NothingIsExcludedByDefault()
        {
            var store = new DataHubLineageStore();
            store.Register(Snapshot(
                [Input("test", "in", [Col("x")])],
                [
                    Output("console", "c", [Col("x")], new() { ["x"] = [Identity("test", "in", "x")] }, upstream: ["in"]),
                    Output("blackhole", "b", [Col("x")], new() { ["x"] = [Identity("test", "in", "x")] }, upstream: ["in"])
                ]), "a");

            var snapshot = store.GetSnapshot();

            Assert.Contains("urn:li:dataJob:(urn:li:dataFlow:(flowtide,a,PROD),console.c)", snapshot.Urns);
            Assert.Contains("urn:li:dataJob:(urn:li:dataFlow:(flowtide,a,PROD),blackhole.b)", snapshot.Urns);
            Assert.Contains("urn:li:dataset:(urn:li:dataPlatform:test,in,PROD)", snapshot.Urns);
        }

        [Fact]
        public void ExcludedNamespacesMatchTheShortNamespace()
        {
            var store = Store(o => o.ExcludedNamespaces.Add("kafka"));
            store.Register(Snapshot([Input("kafka://broker:9092", "topic", [Col("x")])], []), "a");

            Assert.Equal([FlowA, Platform], store.GetSnapshot().Urns);
        }

        [Fact]
        public void DatasetMetadataCanBeTurnedOffGloballyOrPerNamespace()
        {
            var (a, _) = Chain();
            var global = Store(o => o.IncludeDatasetMetadata = false);
            global.Register(a, "a");
            var perNamespace = Store(o => o.MapNamespace("postgres", m => m.IncludeDatasetMetadata = false));
            perNamespace.Register(a, "a");
            var overridden = Store(o =>
            {
                o.IncludeDatasetMetadata = false;
                o.MapNamespace("postgres", m => m.IncludeDatasetMetadata = true);
            });
            overridden.Register(a, "a");

            Assert.Equal([FlowA, JobT, Platform], global.GetSnapshot().Urns);
            Assert.Equal([FlowA, JobT, Platform], perNamespace.GetSnapshot().Urns);
            Assert.Equal([FlowA, JobT, Platform, DatasetS, DatasetT], overridden.GetSnapshot().Urns);
            // The job still links the datasets.
            Assert.Equal([DatasetS], Strings(Aspect(global.GetSnapshot(), JobT, "dataJobInputOutput").GetProperty("inputDatasets")));
        }

        [Fact]
        public void DatasetResolverOverridesTheDefault()
        {
            DataHubDatasetContext? seen = null;
            var store = Store(o => o.DatasetResolver = ctx =>
            {
                if (ctx.TableName == "s")
                {
                    seen = ctx;
                    return new DataHubDataset("hive", "warehouse.s", "lake", "QA");
                }
                return null;
            });
            store.Register(Chain().A, "a");

            var snapshot = store.GetSnapshot();

            Assert.NotNull(seen);
            Assert.Equal("postgres", seen.Namespace);
            Assert.Equal(["s"], seen.NameParts);
            Assert.Equal("postgres", seen.DefaultDataset.Platform);
            Assert.Equal("db.public.s", seen.DefaultDataset.Name);
            Assert.Equal("PROD", seen.DefaultDataset.Env);
            Assert.Contains("urn:li:dataset:(urn:li:dataPlatform:hive,lake.warehouse.s,QA)", snapshot.Urns);
            Assert.Contains(DatasetT, snapshot.Urns);
        }

        [Fact]
        public void ResolverFailureFailsTheSnapshot()
        {
            var store = Store(o => o.DatasetResolver = _ => throw new InvalidOperationException("boom"));
            store.Register(Chain().A, "a");

            var ex = Assert.Throws<InvalidOperationException>(() => store.GetSnapshot());

            Assert.Contains("namespace 'postgres' table 's'", ex.Message);
            Assert.Equal("boom", ex.InnerException!.Message);
        }

        [Fact]
        public void InvalidResolverEnvironmentFailsTheSnapshot()
        {
            var store = Store(o => o.DatasetResolver = ctx => new DataHubDataset("postgres", ctx.TableName, env: "local"));
            store.Register(Chain().A, "a");

            var ex = Assert.Throws<InvalidOperationException>(() => store.GetSnapshot());

            Assert.IsType<ArgumentException>(ex.InnerException);
        }

        [Theory]
        [InlineData("local")]
        [InlineData("")]
        public void InvalidEnvironmentThrowsAtConstruction(string env)
        {
            Assert.Throws<ArgumentException>(() => new DataHubLineageStore(new DataHubLineageOptions() { Env = env }));
            Assert.Throws<ArgumentException>(() => new DataHubLineageStore(new DataHubLineageOptions().MapNamespace("mssql", m => m.Env = env)));
        }

        [Fact]
        public void EnvironmentIsUppercased()
        {
            var store = Store(o => o.Env = "dev");
            store.Register(Chain().A, "a");

            var snapshot = store.GetSnapshot();

            Assert.Contains("urn:li:dataFlow:(flowtide,a,DEV)", snapshot.Urns);
            Assert.Equal("DEV", Aspect(snapshot, "urn:li:dataFlow:(flowtide,a,DEV)", "dataFlowInfo").GetProperty("env").GetString());
        }

        [Fact]
        public void ColumnCasingFollowsTheConnectorSchema()
        {
            var store = Store();
            store.Register(Snapshot(
                [Input("postgres", "s", [Col("orderkey")], connectorColumns: [Col("OrderKey", new Int64Type())])],
                [Output("postgres", "t", [Col("id")], new() { ["id"] = [Identity("postgres", "s", "ORDERKEY")] }, upstream: ["s"], connectorColumns: [Col("Id", new Int64Type())])]), "a");

            var snapshot = store.GetSnapshot();

            Assert.Equal(["OrderKey"], FieldPaths(snapshot, DatasetS));
            Assert.Equal(["Id"], FieldPaths(snapshot, DatasetT));
            var lineage = Assert.Single(FineGrainedLineages(snapshot, JobT));
            Assert.Equal([SchemaField(DatasetS, "OrderKey")], Strings(lineage.GetProperty("upstreams")));
            Assert.Equal([SchemaField(DatasetT, "Id")], Strings(lineage.GetProperty("downstreams")));
        }

        [Fact]
        public void FineGrainedLineagesMergeUpstreamsAndTransformations()
        {
            var store = Store();
            store.Register(Snapshot(
                [Input("postgres", "s", [Col("a"), Col("b"), Col("c")])],
                [Output("postgres", "t", [Col("total"), Col("key")], new()
                {
                    ["key"] = [Identity("postgres", "s", "c")],
                    ["total"] =
                    [
                        Field("postgres", "s", "b", LineageTransformationType.Direct, LineageTransformationSubtype.Aggregation),
                        Field("postgres", "s", "a", LineageTransformationType.Indirect, LineageTransformationSubtype.Conditional),
                        Field("postgres", "s", "a", LineageTransformationType.Direct, LineageTransformationSubtype.Aggregation)
                    ]
                },
                dataset: [Indirect("postgres", "s", "c", LineageTransformationSubtype.GroupBy)],
                upstream: ["s"])]), "a");

            var lineages = FineGrainedLineages(store.GetSnapshot(), JobT);

            // Output column order, sorted upstreams, sorted distinct transformations.
            Assert.Equal([SchemaField(DatasetT, "total"), SchemaField(DatasetT, "key")], lineages.Select(x => Strings(x.GetProperty("downstreams")).Single()));
            Assert.Equal([SchemaField(DatasetS, "a"), SchemaField(DatasetS, "b")], Strings(lineages[0].GetProperty("upstreams")));
            Assert.Equal("DIRECT:AGGREGATION,INDIRECT:CONDITIONAL", lineages[0].GetProperty("transformOperation").GetString());
            Assert.Equal("DIRECT:IDENTITY", lineages[1].GetProperty("transformOperation").GetString());
            Assert.All(lineages, x => Assert.Equal("FIELD_SET", x.GetProperty("upstreamType").GetString()));
            Assert.All(lineages, x => Assert.Equal("FIELD", x.GetProperty("downstreamType").GetString()));
        }

        [Fact]
        public void FieldsWithoutResolvableUpstreamsGetNoFineGrainedLineage()
        {
            var store = Store(o => o.ExcludedNamespaces.Add("console"));
            store.Register(Snapshot(
                [Input("postgres", "s", [Col("x")])],
                [Output("postgres", "t", [Col("x"), Col("constant")], new()
                {
                    ["x"] = [Identity("postgres", "s", "x")],
                    ["constant"] = [],
                    ["excluded"] = [Identity("console", "c", "y")]
                }, upstream: ["s"])]), "a");

            var lineages = FineGrainedLineages(store.GetSnapshot(), JobT);

            Assert.Equal([SchemaField(DatasetT, "x")], lineages.Select(x => Strings(x.GetProperty("downstreams")).Single()));
            Assert.All(lineages, x => Assert.NotEmpty(x.GetProperty("upstreams").EnumerateArray()));
        }

        [Fact]
        public void DatasetLevelInputsAreJobInputs()
        {
            var store = Store();
            store.Register(Snapshot(
                [Input("postgres", "s", [Col("x")]), Input("postgres", "filter", [Col("f")])],
                [Output("postgres", "t", [Col("x")], new() { ["x"] = [Identity("postgres", "s", "x")] }, dataset: [Indirect("postgres", "filter", "f", LineageTransformationSubtype.Filter)])]), "a");

            var inputs = Strings(Aspect(store.GetSnapshot(), JobT, "dataJobInputOutput").GetProperty("inputDatasets"));

            Assert.Equal([DatasetS, "urn:li:dataset:(urn:li:dataPlatform:postgres,db.public.filter,PROD)"], inputs);
        }

        [Fact]
        public void SubstreamsMergeIntoOneFlowAndShareJobs()
        {
            var store = Store();
            store.Register(Snapshot(
                [Input("postgres", "s", [Col("x")])],
                [Output("postgres", "t", [Col("x")], new() { ["x"] = [Identity("postgres", "s", "x")] }, upstream: ["s"])],
                substream: "sub1"), "a");
            store.Register(Snapshot(
                [Input("postgres", "u", [Col("y")])],
                [Output("postgres", "t", [Col("y")], new() { ["y"] = [Identity("postgres", "u", "y")] }, upstream: ["u"])],
                substream: "sub2"), "a");

            var snapshot = store.GetSnapshot();

            Assert.Equal([FlowA, JobT, Platform, DatasetS, DatasetT, DatasetU], snapshot.Urns);
            Assert.Equal([DatasetS, DatasetU], Strings(Aspect(snapshot, JobT, "dataJobInputOutput").GetProperty("inputDatasets")));
            Assert.Equal(2, FineGrainedLineages(snapshot, JobT).Count);
        }

        [Fact]
        public void StreamsWritingOneTableGetOneJobEach()
        {
            var store = Store();
            var (a, _) = Chain();
            store.Register(a, "a");
            store.Register(a, "b");

            var snapshot = store.GetSnapshot();

            Assert.Contains(JobT, snapshot.Urns);
            Assert.Contains("urn:li:dataJob:(urn:li:dataFlow:(flowtide,b,PROD),postgres.db.public.t)", snapshot.Urns);
        }

        [Fact]
        public void JobIdsAddTheEnvironmentOnlyWhenItDiffersFromTheFlow()
        {
            var store = Store(o => o.DatasetResolver = ctx => ctx.Namespace == "pg2" ? new DataHubDataset("postgres", ctx.DefaultDataset.Name, env: "DEV") : null);
            store.Register(Snapshot(
                [Input("postgres", "s", [Col("x")])],
                [
                    Output("postgres", "t", [Col("x")], new() { ["x"] = [Identity("postgres", "s", "x")] }, upstream: ["s"]),
                    Output("pg2", "db.public.t", [Col("x")], new() { ["x"] = [Identity("postgres", "s", "x")] }, upstream: ["s"])
                ]), "a");

            var snapshot = store.GetSnapshot();

            Assert.Contains(JobT, snapshot.Urns);
            Assert.Single(snapshot.Urns, x => Regex.IsMatch(x, @"^urn:li:dataJob:\(urn:li:dataFlow:\(flowtide,a,PROD\),postgres\.db\.public\.t~DEV~[0-9a-f]{32}\)$"));
        }

        [Fact]
        public void JobIdDoesNotChangeWhenAnotherJobAppears()
        {
            var store = Store(o => o.DatasetResolver = ctx => ctx.Namespace == "pg2" ? new DataHubDataset("postgres", ctx.DefaultDataset.Name, env: "DEV") : null);
            store.Register(Chain().A, "a");
            var before = store.GetSnapshot().Urns;
            store.Register(Snapshot(
                [Input("postgres", "s", [Col("x")])],
                [Output("pg2", "db.public.t", [Col("x")], new() { ["x"] = [Identity("postgres", "s", "x")] }, upstream: ["s"])],
                substream: "sub2"), "a");

            var after = store.GetSnapshot().Urns;

            Assert.Contains(JobT, before);
            Assert.Contains(JobT, after);
        }

        [Fact]
        public void LookalikeJobIdsNeitherCollideNorRenameExistingJobs()
        {
            const string prodJob = "urn:li:dataJob:(urn:li:dataFlow:(flowtide,a,PROD),kafka.orders.DEV)";
            var store = new DataHubLineageStore(new DataHubLineageOptions().MapNamespace("kafka://dev:9092", m => m.Env = "DEV"));
            store.Register(Snapshot(
                [Input("kafka", "in", [Col("x")])],
                [Output("kafka", "orders.DEV", [Col("x")], new() { ["x"] = [Identity("kafka", "in", "x")] }, upstream: ["in"])],
                substream: "sub1"), "a");
            var before = store.GetSnapshot().Urns;

            // A DEV topic named like the PROD topic's prefix arrives later.
            store.Register(Snapshot(
                [Input("kafka", "in", [Col("x")])],
                [Output("kafka://dev:9092", "orders", [Col("x")], new() { ["x"] = [Identity("kafka", "in", "x")] }, upstream: ["in"])],
                substream: "sub2"), "a");
            var jobs = store.GetSnapshot().Urns.Where(x => x.StartsWith("urn:li:dataJob:", StringComparison.Ordinal)).ToList();

            Assert.Contains(prodJob, before);
            Assert.Equal(2, jobs.Count);
            Assert.Contains(prodJob, jobs);
            Assert.Single(jobs, x => Regex.IsMatch(x, @"^urn:li:dataJob:\(urn:li:dataFlow:\(flowtide,a,PROD\),kafka\.orders~DEV~[0-9a-f]{32}\)$"));
        }

        [Fact]
        public void NameCopyingAnotherJobsHashedIdDoesNotCollide()
        {
            // A PROD topic deliberately named like the DEV topic's hashed id.
            const string devDataset = "urn:li:dataset:(urn:li:dataPlatform:kafka,orders,DEV)";
            var hash = Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(devDataset)), 0, 16).ToLowerInvariant();
            var store = new DataHubLineageStore(new DataHubLineageOptions().MapNamespace("kafka://dev:9092", m => m.Env = "DEV"));
            store.Register(Snapshot(
                [Input("kafka", "in", [Col("x")])],
                [
                    Output("kafka://dev:9092", "orders", [Col("x")], new() { ["x"] = [Identity("kafka", "in", "x")] }, upstream: ["in"]),
                    Output("kafka", "orders~DEV~" + hash, [Col("x")], new() { ["x"] = [Identity("kafka", "in", "x")] }, upstream: ["in"])
                ]), "a");

            var jobs = store.GetSnapshot().Urns.Where(x => x.StartsWith("urn:li:dataJob:", StringComparison.Ordinal)).ToList();

            Assert.Equal(2, jobs.Distinct().Count());
            Assert.Contains($"urn:li:dataJob:(urn:li:dataFlow:(flowtide,a,PROD),kafka.orders~DEV~{hash})", jobs);
        }

        [Fact]
        public void PlatformsWithADotGetAHashedJobId()
        {
            // a.b + c and a + b.c would both read a.b.c.
            var store = new DataHubLineageStore(new DataHubLineageOptions()
                .MapNamespace("one", m => m.Platform = "a.b")
                .MapNamespace("two", m => m.Platform = "a"));
            store.Register(Snapshot(
                [Input("kafka", "in", [Col("x")])],
                [
                    Output("one", "c", [Col("x")], new() { ["x"] = [Identity("kafka", "in", "x")] }, upstream: ["in"]),
                    Output("two", "b.c", [Col("x")], new() { ["x"] = [Identity("kafka", "in", "x")] }, upstream: ["in"])
                ]), "s");

            var jobs = store.GetSnapshot().Urns.Where(x => x.StartsWith("urn:li:dataJob:", StringComparison.Ordinal)).ToList();

            Assert.Equal(2, jobs.Count);
            Assert.Contains("urn:li:dataJob:(urn:li:dataFlow:(flowtide,s,PROD),a.b.c)", jobs);
            Assert.Single(jobs, x => Regex.IsMatch(x, @"^urn:li:dataJob:\(urn:li:dataFlow:\(flowtide,s,PROD\),a\.b\.c~PROD~[0-9a-f]{32}\)$"));
        }

        [Fact]
        public void StreamsWhoseUrnsCollideShareOneFlow()
        {
            var store = Store();
            store.Register(Chain().A, "a,b");
            store.Register(Chain().B, "a%2Cb");

            var snapshot = store.GetSnapshot();

            Assert.Single(snapshot.Urns, x => x.StartsWith("urn:li:dataFlow:", StringComparison.Ordinal));
            Assert.Equal(2, snapshot.Urns.Count(x => x.StartsWith("urn:li:dataJob:", StringComparison.Ordinal)));
        }

        [Fact]
        public void LongJobUrnsAreHashedBelowTheGmsLimit()
        {
            // The dataset urn fits in 512 URL encoded bytes, its job urn would not.
            var name = new string('n', 440);
            var store = new DataHubLineageStore();
            store.Register(Snapshot(
                [Input("kafka", "in", [Col("x")])],
                [Output("kafka", name, [Col("x")], new() { ["x"] = [Identity("kafka", "in", "x")] }, upstream: ["in"])]), "s");

            var snapshot = store.GetSnapshot();

            var dataset = $"urn:li:dataset:(urn:li:dataPlatform:kafka,{name},PROD)";
            Assert.True(DataHubUrns.UrlEncodedLength(dataset) <= DataHubUrns.MaxUrnLength);
            var job = Assert.Single(snapshot.Urns, x => x.StartsWith("urn:li:dataJob:", StringComparison.Ordinal));
            Assert.Matches(@"^urn:li:dataJob:\(urn:li:dataFlow:\(flowtide,s,PROD\),kafka~[0-9a-f]{32}\)$", job);
            Assert.Equal([dataset], Strings(Aspect(snapshot, job, "dataJobInputOutput").GetProperty("outputDatasets")));
            Assert.Equal(name, Aspect(snapshot, job, "dataJobInfo").GetProperty("name").GetString());
        }

        [Theory]
        [InlineData("x", 1)]
        [InlineData("aZ09.-*_ ", 9)]
        [InlineData(":(,)", 12)]
        [InlineData("å", 6)]
        public void UrlEncodedLengthMatchesTheJavaEncoder(string value, int expected)
        {
            Assert.Equal(expected, DataHubUrns.UrlEncodedLength(value));
        }

        [Fact]
        public void ReservedUrnCharactersAreEncoded()
        {
            var store = new DataHubLineageStore(new DataHubLineageOptions() { IncludeRuns = false });
            store.Register(Snapshot(
                [Input("kafka", "in(1)", [Col("a,b")])],
                [Output("kafka", "out,2", [Col("c")], new() { ["c"] = [Identity("kafka", "in(1)", "a,b")] }, upstream: ["in(1)"])]), "s(1)");

            var snapshot = store.GetSnapshot();

            const string flow = "urn:li:dataFlow:(flowtide,s%281%29,PROD)";
            const string input = "urn:li:dataset:(urn:li:dataPlatform:kafka,in%281%29,PROD)";
            const string output = "urn:li:dataset:(urn:li:dataPlatform:kafka,out%2C2,PROD)";
            var job = $"urn:li:dataJob:({flow},kafka.out%2C2)";
            Assert.Equal([flow, job, Platform, input, output], snapshot.Urns);
            Assert.Equal([SchemaField(input, "a%2Cb")], Strings(Assert.Single(FineGrainedLineages(snapshot, job)).GetProperty("upstreams")));
            // Schema field paths stay unencoded.
            Assert.Equal(["a,b"], FieldPaths(snapshot, input));
        }

        [Fact]
        public void AspectProviderAddsAndReplacesAspects()
        {
            var contexts = new List<DataHubEntityContext>();
            var store = Store(o => o.AspectProvider = ctx =>
            {
                contexts.Add(ctx);
                return ctx.EntityType switch
                {
                    DataHubEntityType.DataFlow => [new DataHubAspect("globalTags", JsonNode.Parse("{\"tags\":[{\"tag\":\"urn:li:tag:streaming\"}]}")!)],
                    DataHubEntityType.DataJob => [new DataHubAspect("status", JsonNode.Parse("{\"removed\":true}")!)],
                    _ => null
                };
            });
            store.Register(Chain().A, "a");

            var snapshot = store.GetSnapshot();

            Assert.Equal(["dataFlowInfo", "status", "globalTags"], Aspects(snapshot, FlowA).EnumerateObject().Select(x => x.Name));
            Assert.Equal("urn:li:tag:streaming", Aspect(snapshot, FlowA, "globalTags").GetProperty("tags")[0].GetProperty("tag").GetString());
            Assert.True(Aspect(snapshot, JobT, "status").GetProperty("removed").GetBoolean());
            var flow = Assert.Single(contexts, x => x.EntityType == DataHubEntityType.DataFlow);
            Assert.Equal(("a", (string?)null, (string?)null), (flow.StreamName, flow.Namespace, flow.TableName));
            var job = Assert.Single(contexts, x => x.EntityType == DataHubEntityType.DataJob);
            Assert.Equal((JobT, "a", "postgres", "t"), (job.Urn, job.StreamName!, job.Namespace!, job.TableName!));
            var dataset = Assert.Single(contexts, x => x.Urn == DatasetS);
            Assert.Equal((DataHubEntityType.Dataset, (string?)null, "postgres", "s"), (dataset.EntityType, dataset.StreamName, dataset.Namespace!, dataset.TableName!));
        }

        [Fact]
        public void AspectProviderFailureFailsTheSnapshot()
        {
            var store = Store(o => o.AspectProvider = _ => throw new InvalidOperationException("boom"));
            store.Register(Chain().A, "a");

            var ex = Assert.Throws<InvalidOperationException>(() => store.GetSnapshot());

            Assert.Contains(FlowA, ex.Message);
        }

        [Fact]
        public void SnapshotIsCachedUntilTheNextRegistration()
        {
            var store = Store();
            var (a, b) = Chain();
            store.Register(a, "a");

            var first = store.GetSnapshot();
            Assert.Same(first, store.GetSnapshot());

            store.Register(b, "b");
            var second = store.GetSnapshot();

            Assert.NotSame(first, second);
            Assert.Equal(2, store.Version);
            Assert.Equal(8, second.Urns.Count);
        }

        [Fact]
        public void LastRegistrationPerSubstreamWins()
        {
            var store = Store();
            var (a, b) = Chain();
            store.Register(a, "a");
            store.Register(b, "a");

            var snapshot = store.GetSnapshot();

            Assert.DoesNotContain(JobT, snapshot.Urns);
            Assert.Contains("urn:li:dataJob:(urn:li:dataFlow:(flowtide,a,PROD),postgres.db.public.u)", snapshot.Urns);
        }

        [Fact]
        public void OptionsAreCopiedAtConstruction()
        {
            var options = new DataHubLineageOptions();
            var store = new DataHubLineageStore(options);
            options.ExcludedNamespaces.Add("postgres");
            options.MapNamespace("postgres", m => m.Database = "late");

            store.Register(Chain().A, "a");

            Assert.Contains("urn:li:dataset:(urn:li:dataPlatform:postgres,s,PROD)", store.GetSnapshot().Urns);
        }

        [Fact]
        public void MapNamespaceCallsAddUp()
        {
            var options = new DataHubLineageOptions()
                .MapNamespace("postgres", m => m.Database = "db")
                .MapNamespace("POSTGRES", m => m.DefaultSchema = "public");
            var store = new DataHubLineageStore(options);
            store.Register(Chain().A, "a");

            Assert.Contains(DatasetS, store.GetSnapshot().Urns);
        }

        [Fact]
        public void WarmsUpUntilExpectedStreamsRegisterOrTimeout()
        {
            var time = new ManualTimeProvider();
            var store = new DataHubLineageStore(new DataHubLineageOptions() { WarmupTimeout = TimeSpan.FromSeconds(10) }, time);
            Assert.False(store.IsWarmingUp);

            store.ExpectStream("a");
            store.ExpectStream("b");
            Assert.True(store.IsWarmingUp);

            store.Register(Chain().A, "a");
            Assert.True(store.IsWarmingUp);

            time.Ticks = TimeSpan.FromSeconds(10).Ticks;
            Assert.False(store.IsWarmingUp);

            var registered = new DataHubLineageStore(new DataHubLineageOptions(), time);
            registered.ExpectStream("a");
            registered.Register(Chain().A, "a");
            Assert.False(registered.IsWarmingUp);
        }

        [Fact]
        public void RealExtractionProducesColumnLineage()
        {
            var store = Store(o => o.MapNamespace("mssql", m => m.Database = "shop"));
            store.Register(ExtractWithSql(WindowSql, "mssql"), "window_stream");

            var snapshot = store.GetSnapshot();

            const string job = "urn:li:dataJob:(urn:li:dataFlow:(flowtide,window_stream,PROD),mssql.shop.dbo.output)";
            const string input = "urn:li:dataset:(urn:li:dataPlatform:mssql,shop.dbo.input1,PROD)";
            const string output = "urn:li:dataset:(urn:li:dataPlatform:mssql,shop.dbo.output,PROD)";
            Assert.Equal([input], Strings(Aspect(snapshot, job, "dataJobInputOutput").GetProperty("inputDatasets")));
            var lineages = FineGrainedLineages(snapshot, job);
            Assert.Equal([SchemaField(output, "c1"), SchemaField(output, "c2")], lineages.Select(x => Strings(x.GetProperty("downstreams")).Single()));
            Assert.Equal([SchemaField(input, "a"), SchemaField(input, "b"), SchemaField(input, "c")], Strings(lineages[0].GetProperty("upstreams")));
            Assert.Equal("DIRECT:AGGREGATION,INDIRECT:GROUP_BY,INDIRECT:SORT", lineages[0].GetProperty("transformOperation").GetString());
            Assert.Equal([SchemaField(input, "b")], Strings(lineages[1].GetProperty("upstreams")));
            Assert.Equal("DIRECT:IDENTITY", lineages[1].GetProperty("transformOperation").GetString());
        }

        [Fact]
        public void TablesReachedWithoutColumnLineageAreJobInputsWithoutASchema()
        {
            var store = Store(o => o.MapNamespace("mssql", m => m.Database = "db"));
            store.Register(ExtractWithSql("CREATE TABLE input1 (a int); INSERT INTO output SELECT count(*) AS c FROM input1;", "mssql"), "a");

            var snapshot = store.GetSnapshot();

            const string input = "urn:li:dataset:(urn:li:dataPlatform:mssql,db.dbo.input1,PROD)";
            const string job = "urn:li:dataJob:(urn:li:dataFlow:(flowtide,a,PROD),mssql.db.dbo.output)";
            Assert.Equal([input], Strings(Aspect(snapshot, job, "dataJobInputOutput").GetProperty("inputDatasets")));
            // An empty schema would replace the real one in DataHub.
            Assert.Equal(["status"], Aspects(snapshot, input).EnumerateObject().Select(x => x.Name));
        }

        [Fact]
        public void ExtractedNamePartsReachTheResolverAndFieldLineage()
        {
            DataHubDatasetContext? seen = null;
            var store = Store(o =>
            {
                o.MapNamespace("mssql", m => m.Database = "db");
                o.DatasetResolver = ctx =>
                {
                    seen ??= ctx.Namespace == "mssql" ? ctx : null;
                    return null;
                };
            });
            var input = new StreamLineageInput()
            {
                Key = "dbo.my.orders",
                NameParts = ["dbo", "my.orders"],
                Namespace = "mssql",
                TableName = "dbo.my.orders",
                PlanColumns = [Col("a")]
            };
            store.Register(Snapshot(
                [input],
                [Output("postgres", "t", [Col("a")], new() { ["a"] = [Identity("mssql", "dbo.my.orders", "a")] }, upstream: ["dbo.my.orders"])]), "a");

            var snapshot = store.GetSnapshot();

            const string orders = "urn:li:dataset:(urn:li:dataPlatform:mssql,db.dbo.my.orders,PROD)";
            Assert.Equal(["dbo", "my.orders"], seen!.NameParts);
            Assert.Equal([orders], Strings(Aspect(snapshot, JobT, "dataJobInputOutput").GetProperty("inputDatasets")));
            Assert.Equal([SchemaField(orders, "a")], Strings(Assert.Single(FineGrainedLineages(snapshot, JobT)).GetProperty("upstreams")));
            Assert.Single(snapshot.Urns, x => x.StartsWith("urn:li:dataset:(urn:li:dataPlatform:mssql,", StringComparison.Ordinal));
        }

        [Fact]
        public void FullNamespaceMappingWinsAndMatchingIgnoresCase()
        {
            var store = new DataHubLineageStore(new DataHubLineageOptions()
                .MapNamespace("KAFKA://a:9092", m => m.PlatformInstance = "a")
                .MapNamespace("Kafka", m => m.PlatformInstance = "default"));
            store.Register(Snapshot([Input("kafka://a:9092", "ta", [Col("x")]), Input("kafka://b:9092", "tb", [Col("x")])], []), "s");

            var snapshot = store.GetSnapshot();

            Assert.Contains("urn:li:dataset:(urn:li:dataPlatform:kafka,a.ta,PROD)", snapshot.Urns);
            Assert.Contains("urn:li:dataset:(urn:li:dataPlatform:kafka,default.tb,PROD)", snapshot.Urns);
        }

        [Fact]
        public void FullNamespaceExclusionLeavesOtherClustersAndIgnoresCase()
        {
            var options = new DataHubLineageOptions();
            options.ExcludedNamespaces.Add("KAFKA://a:9092");
            var store = new DataHubLineageStore(options);
            store.Register(Snapshot([Input("kafka://a:9092", "ta", [Col("x")]), Input("kafka://b:9092", "tb", [Col("x")])], []), "s");

            var datasets = store.GetSnapshot().Urns.Where(x => x.StartsWith("urn:li:dataset:", StringComparison.Ordinal));

            Assert.Equal(["urn:li:dataset:(urn:li:dataPlatform:kafka,tb,PROD)"], datasets);
        }

        [Fact]
        public void SnapshotIsGeneratedOnceUntilTheNextRegistration()
        {
            var calls = 0;
            var store = Store(o => o.AspectProvider = _ =>
            {
                Interlocked.Increment(ref calls);
                return null;
            });
            store.Register(Chain().A, "a");

            Parallel.For(0, 8, _ => store.GetSnapshot());
            var once = calls;
            store.GetSnapshot();
            store.Register(Chain().B, "b");
            store.GetSnapshot();

            // One call per flow, job and dataset of one generation.
            Assert.Equal(4, once);
            Assert.Equal(4, calls - 7);
        }

        [Fact]
        public void SchemasCoverAliasesAndReferencedFields()
        {
            var store = Store();
            store.Register(Snapshot(
                [Input("postgres", "s", [Col("x")])],
                [Output("postgres", "t", [Col("x")], new() { ["alias"] = [Identity("postgres", "s", "z")] }, upstream: ["s"])]), "a");

            var snapshot = store.GetSnapshot();

            Assert.Equal(["x", "alias"], FieldPaths(snapshot, DatasetT));
            Assert.Equal(["x", "z"], FieldPaths(snapshot, DatasetS));
        }

        [Fact]
        public void WrittenColumnCasingWinsOverReads()
        {
            var store = Store();
            store.Register(Snapshot(
                [Input("postgres", "t", [Col("ID")])],
                [Output("postgres", "u", [Col("y")], new() { ["y"] = [Identity("postgres", "t", "ID")] }, upstream: ["t"])]), "reader");
            store.Register(Snapshot(
                [Input("postgres", "s", [Col("x")])],
                [Output("postgres", "t", [Col("Id")], new() { ["Id"] = [Identity("postgres", "s", "x")] }, upstream: ["s"])]), "writer");

            var snapshot = store.GetSnapshot();

            Assert.Equal(["Id"], FieldPaths(snapshot, DatasetT));
            var readerJob = "urn:li:dataJob:(urn:li:dataFlow:(flowtide,reader,PROD),postgres.db.public.u)";
            Assert.Equal([SchemaField(DatasetT, "Id")], Strings(Assert.Single(FineGrainedLineages(snapshot, readerJob)).GetProperty("upstreams")));
        }

        [Fact]
        public void AspectProviderRunsForDatasetsWithoutMetadata()
        {
            var store = Store(o =>
            {
                o.MapNamespace("postgres", m => m.IncludeDatasetMetadata = false);
                o.AspectProvider = ctx => ctx.Urn == DatasetS ? [new DataHubAspect("globalTags", JsonNode.Parse("{\"tags\":[]}")!)] : null;
            });
            store.Register(Chain().A, "a");

            var snapshot = store.GetSnapshot();

            // Only datasets that got an aspect are served.
            Assert.Equal([FlowA, JobT, Platform, DatasetS], snapshot.Urns);
            Assert.Equal(["globalTags"], Aspects(snapshot, DatasetS).EnumerateObject().Select(x => x.Name));
        }

        [Fact]
        public void ConnectorColumnsAreIgnoredWhenTheStoreDidNotAskForThem()
        {
            var store = Store(o => o.IncludeConnectorSchema = false);
            store.Register(Snapshot(
                [Input("postgres", "s", [Col("orderkey")], connectorColumns: [Col("OrderKey", new Int64Type())])],
                [Output("postgres", "t", [Col("id")], new() { ["id"] = [Identity("postgres", "s", "orderkey")] }, upstream: ["s"])]), "a");

            var snapshot = store.GetSnapshot();

            Assert.Equal(["orderkey"], FieldPaths(snapshot, DatasetS));
            Assert.Equal([SchemaField(DatasetS, "orderkey")], Strings(Assert.Single(FineGrainedLineages(snapshot, JobT)).GetProperty("upstreams")));
        }

        [Fact]
        public void UnitSeparatorIsEncoded()
        {
            var store = new DataHubLineageStore();
            store.Register(Snapshot([Input("kafka", "a␟b", [Col("x")])], []), "s");

            Assert.Contains("urn:li:dataset:(urn:li:dataPlatform:kafka,a%E2%90%9Fb,PROD)", store.GetSnapshot().Urns);
        }
    }
}
