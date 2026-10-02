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
using FlowtideDotNet.Core.Lineage.Internal;
using FlowtideDotNet.Core.Lineage.Internal.Models;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Type;
using System.Text.Json;

namespace FlowtideDotNet.Core.Tests.LineageTests.Dbt
{
    internal static class DbtTestData
    {
        public static readonly DateTimeOffset BuildTime = new DateTimeOffset(2026, 10, 2, 12, 0, 0, TimeSpan.Zero);

        public static LineageColumn Col(string name, SubstraitBaseType? type = null)
        {
            return new LineageColumn(name, type ?? AnyType.Instance);
        }

        public static StreamLineageInput Input(string ns, string table, IReadOnlyList<LineageColumn> columns, IReadOnlyList<LineageColumn>? connectorColumns = null)
        {
            return new StreamLineageInput()
            {
                Key = table,
                NameParts = table.Split('.'),
                Namespace = ns,
                TableName = table,
                ConnectorColumns = connectorColumns,
                PlanColumns = columns
            };
        }

        public static StreamLineageOutput Output(
            string ns,
            string table,
            IReadOnlyList<LineageColumn> columns,
            Dictionary<string, LineageInputField[]> fields,
            LineageInputField[]? dataset = null,
            string[]? upstream = null,
            IReadOnlyList<LineageColumn>? connectorColumns = null)
        {
            return new StreamLineageOutput()
            {
                Key = table,
                NameParts = table.Split('.'),
                Namespace = ns,
                TableName = table,
                ConnectorColumns = connectorColumns,
                PlanColumns = columns,
                ColumnLineage = new ColumnLineage(
                    fields.ToDictionary(x => x.Key, x => new ColumnLineageField(x.Value)),
                    dataset ?? []),
                UpstreamInputKeys = upstream ?? []
            };
        }

        public static StreamLineage Snapshot(IReadOnlyList<StreamLineageInput> inputs, IReadOnlyList<StreamLineageOutput> outputs, string? substream = null, DateTimeOffset? buildTime = null)
        {
            return new StreamLineage("builder", substream, buildTime ?? BuildTime, inputs, outputs);
        }

        public static LineageInputField Field(string ns, string table, string field, LineageTransformationType type, LineageTransformationSubtype subtype, bool masking = false)
        {
            return new LineageInputField(ns, table, field, [new LineageTransformation(type, subtype, masking: masking)]);
        }

        public static LineageInputField Identity(string ns, string table, string field)
        {
            return Field(ns, table, field, LineageTransformationType.Direct, LineageTransformationSubtype.Identity);
        }

        public static LineageInputField Indirect(string ns, string table, string field, LineageTransformationSubtype subtype)
        {
            return Field(ns, table, field, LineageTransformationType.Indirect, subtype);
        }

        // Real extraction through the SQL planner and optimizer.
        public static StreamLineage ExtractWithSql(string sql, string ns, Func<ReadRelation, NamedStruct?>? sourceSchema = null, Func<WriteRelation, NamedStruct?>? sinkSchema = null, DateTimeOffset? buildTime = null)
        {
            var connectorManager = new ConnectorManager();
            connectorManager.AddSource(new LineageTestSourceFactory(sourceSchema, ns));
            connectorManager.AddSink(new LineageTestSinkFactory(sinkSchema, ns));
            return StreamLineageExtractor.Extract(new StreamLineageExtractionContext()
            {
                Plan = LineageTestHelper.BuildPlan(sql),
                ConnectorManager = connectorManager,
                BuilderStreamName = "builder",
                IncludeConnectorSchema = true,
                BuildTime = buildTime ?? BuildTime
            });
        }

        public static DbtManifestStore Store(Action<DbtManifestOptions>? configure = null)
        {
            var options = new DbtManifestOptions();
            configure?.Invoke(options);
            return new DbtManifestStore(options);
        }

        public static JsonElement Parse(DbtArtifact artifact)
        {
            return JsonDocument.Parse(artifact.Utf8Json).RootElement;
        }

        public static JsonElement Node(DbtArtifact manifest, string uniqueId)
        {
            var root = Parse(manifest);
            var section = uniqueId.StartsWith("source.", StringComparison.Ordinal) ? "sources" : "nodes";
            return root.GetProperty(section).GetProperty(uniqueId);
        }

        // Single model of a single registered lineage.
        public static string CompiledCode(StreamLineage lineage, Action<DbtManifestOptions>? configure = null)
        {
            var store = Store(configure);
            store.Register(lineage, "stream");
            var nodes = Parse(store.GetManifest()).GetProperty("nodes").EnumerateObject().ToList();
            return Assert.Single(nodes).Value.GetProperty("compiled_code").GetString()!;
        }

        public static string Sql(params string[] lines)
        {
            return string.Join("\n", lines);
        }

        public static List<string> Keys(JsonElement element)
        {
            return element.EnumerateObject().Select(x => x.Name).ToList();
        }
    }
}
