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
using FlowtideDotNet.Substrait.Type;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;

namespace FlowtideDotNet.Core.Tests.LineageTests
{
    public class OpenLineageSerializerTests
    {
        private static readonly Guid RunId = Guid.Parse("0f3c6a52-7d1e-4b8a-9c2d-5e6f7a8b9c0d");

        private static readonly DateTime EventTime = new DateTime(2026, 1, 2, 3, 4, 5, DateTimeKind.Utc);

        [Fact]
        public void EveryFacetHasProducerAndSchemaUrl()
        {
            using var document = JsonDocument.Parse(Serialize());

            var facets = new List<(string Name, JsonElement Facet)>();
            CollectFacets(document.RootElement, facets);

            Assert.Equal(["processing_engine", "jobType", "schema", "columnLineage", "schema"], facets.Select(x => x.Name));
            foreach (var (name, facet) in facets)
            {
                Assert.True(Uri.TryCreate(facet.GetProperty("_producer").GetString(), UriKind.Absolute, out _), name);
                Assert.True(Uri.TryCreate(facet.GetProperty("_schemaURL").GetString(), UriKind.Absolute, out _), name);
                Assert.Equal(OpenLineageConstants.Producer, facet.GetProperty("_producer").GetString());
            }
            Assert.Equal(OpenLineageConstants.ProcessingEngineRunFacetSchemaUrl, facets[0].Facet.GetProperty("_schemaURL").GetString());
            Assert.Equal(OpenLineageConstants.JobTypeJobFacetSchemaUrl, facets[1].Facet.GetProperty("_schemaURL").GetString());
            Assert.Equal(OpenLineageConstants.SchemaDatasetFacetSchemaUrl, facets[2].Facet.GetProperty("_schemaURL").GetString());
            Assert.Equal(OpenLineageConstants.ColumnLineageDatasetFacetSchemaUrl, facets[3].Facet.GetProperty("_schemaURL").GetString());
        }

        [Fact]
        public void RootFollowsSpec202()
        {
            using var document = JsonDocument.Parse(Serialize());
            var root = document.RootElement;

            Assert.Equal(["eventTime", "producer", "schemaURL", "eventType", "run", "job", "inputs", "outputs"], root.EnumerateObject().Select(x => x.Name));
            Assert.Equal("https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent", root.GetProperty("schemaURL").GetString());
            Assert.Equal("https://github.com/koralium/flowtide", root.GetProperty("producer").GetString());
            Assert.Equal("RUNNING", root.GetProperty("eventType").GetString());
            Assert.Equal("2026-01-02T03:04:05Z", root.GetProperty("eventTime").GetString());
            Assert.Equal(RunId.ToString("D"), root.GetProperty("run").GetProperty("runId").GetString());
        }

        [Fact]
        public void NoPascalCaseKeys()
        {
            using var document = JsonDocument.Parse(Serialize());

            var names = new List<string>();
            CollectPropertyNames(document.RootElement, null, names);

            var allowed = new Regex("^_?[a-z][A-Za-z0-9_]*$");
            Assert.All(names, name => Assert.Matches(allowed, name));
            // User column names stay verbatim.
            var fields = document.RootElement.GetProperty("outputs")[0].GetProperty("facets").GetProperty("columnLineage").GetProperty("fields");
            Assert.Equal(["CustomerId", "total"], fields.EnumerateObject().Select(x => x.Name));
        }

        [Fact]
        public void JobTypeFacetUsesSpecKey()
        {
            using var document = JsonDocument.Parse(Serialize());
            var job = document.RootElement.GetProperty("job");

            Assert.Equal("flowtide", job.GetProperty("namespace").GetString());
            Assert.Equal("mystream", job.GetProperty("name").GetString());
            var facets = job.GetProperty("facets");
            Assert.False(facets.TryGetProperty("JobType", out _));
            var jobType = facets.GetProperty("jobType");
            Assert.Equal("STREAMING", jobType.GetProperty("processingType").GetString());
            Assert.Equal("flowtide", jobType.GetProperty("integration").GetString());
            Assert.Equal("JOB", jobType.GetProperty("jobType").GetString());
        }

        [Fact]
        public void ProcessingEngineIsCamelCase()
        {
            using var document = JsonDocument.Parse(Serialize());
            var engine = document.RootElement.GetProperty("run").GetProperty("facets").GetProperty("processing_engine");

            Assert.Equal(OpenLineageConstants.EngineVersion, engine.GetProperty("version").GetString());
            Assert.Equal("Flowtide", engine.GetProperty("name").GetString());
            Assert.False(engine.TryGetProperty("openlineageAdapterVersion", out _));
            Assert.False(engine.TryGetProperty("Version", out _));
            Assert.False(engine.TryGetProperty("Name", out _));
            Assert.False(string.IsNullOrEmpty(OpenLineageConstants.EngineVersion));
            Assert.DoesNotContain('+', OpenLineageConstants.EngineVersion);
        }

        [Fact]
        public void GroupByIsSnakeCase()
        {
            var json = Serialize();

            Assert.Contains("\"GROUP_BY\"", json);
            Assert.DoesNotContain("GROUPBY", json);
        }

        [Fact]
        public void OutputIsCompactAndStable()
        {
            var ev = CreateEvent();

            var json = OpenLineageSerializer.Serialize(ev);

            Assert.DoesNotContain('\n', json);
            Assert.DoesNotContain("\": ", json);
            Assert.Equal(json, OpenLineageSerializer.Serialize(ev));
            Assert.Equal(json, Encoding.UTF8.GetString(OpenLineageSerializer.SerializeToUtf8Bytes(ev)));
        }

        [Fact]
        public void SchemaFieldsOmitNullDescription()
        {
            using var document = JsonDocument.Parse(Serialize());
            var fields = document.RootElement.GetProperty("inputs")[0].GetProperty("facets").GetProperty("schema").GetProperty("fields");

            Assert.Equal(["id", "amount", "address"], fields.EnumerateArray().Select(x => x.GetProperty("name").GetString()));
            Assert.Equal(["bigint", "double", "struct"], fields.EnumerateArray().Select(x => x.GetProperty("type").GetString()));
            Assert.All(fields.EnumerateArray(), x => Assert.False(x.TryGetProperty("description", out _)));
        }

        [Fact]
        public void EnumValuesAreInSpecSet()
        {
            var subtypes = Enum.GetValues<LineageTransformationSubtype>();
            var types = Enum.GetValues<LineageTransformationType>();
            var inputFields = new List<LineageInputField>();
            foreach (var type in types)
            {
                foreach (var subtype in subtypes)
                {
                    inputFields.Add(new LineageInputField("ns", "t", "f", [new LineageTransformation(type, subtype)]));
                }
            }
            var columnLineage = new ColumnLineage(new Dictionary<string, ColumnLineageField>() { ["c"] = new ColumnLineageField(inputFields) }, []);

            var serializedSubtypes = new HashSet<string>();
            var serializedTypes = new HashSet<string>();
            var serializedEvents = new HashSet<string>();
            foreach (var eventType in Enum.GetValues<LineageEventType>())
            {
                using var document = JsonDocument.Parse(OpenLineageSerializer.Serialize(CreateEvent(columnLineage).ChangeEventType(eventType, EventTime)));
                serializedEvents.Add(document.RootElement.GetProperty("eventType").GetString()!);
                foreach (var field in document.RootElement.GetProperty("outputs")[0].GetProperty("facets").GetProperty("columnLineage").GetProperty("fields").GetProperty("c").GetProperty("inputFields").EnumerateArray())
                {
                    var transformation = field.GetProperty("transformations")[0];
                    serializedTypes.Add(transformation.GetProperty("type").GetString()!);
                    serializedSubtypes.Add(transformation.GetProperty("subtype").GetString()!);
                }
            }

            Assert.Equal(new HashSet<string>() { "IDENTITY", "TRANSFORMATION", "AGGREGATION", "JOIN", "GROUP_BY", "FILTER", "SORT", "WINDOW", "CONDITIONAL" }, serializedSubtypes);
            Assert.Equal(new HashSet<string>() { "DIRECT", "INDIRECT" }, serializedTypes);
            Assert.Equal(new HashSet<string>() { "START", "RUNNING", "COMPLETE", "ABORT", "FAIL", "OTHER" }, serializedEvents);
        }

        [Theory]
        [InlineData("1.2.3+abc", null, "1.2.3")]
        [InlineData("0.16.0-beta1+abc", null, "0.16.0-beta1")]
        [InlineData("1.2.3", null, "1.2.3")]
        [InlineData(null, "1.2.3.0", "1.2.3")]
        [InlineData("", "4.5.6.0", "4.5.6")]
        [InlineData("+abc", "7.8.9.0", "7.8.9")]
        [InlineData(null, null, "unknown")]
        public void EngineVersionStripsSourceRevision(string? informationalVersion, string? assemblyVersion, string expected)
        {
            Assert.Equal(expected, OpenLineageConstants.ResolveEngineVersion(informationalVersion, assemblyVersion == null ? null : Version.Parse(assemblyVersion)));
        }

        private static string Serialize()
        {
            return OpenLineageSerializer.Serialize(CreateEvent());
        }

        private static OpenLineageEvent CreateEvent(ColumnLineage? columnLineage = null)
        {
            columnLineage ??= new ColumnLineage(new Dictionary<string, ColumnLineageField>()
            {
                ["CustomerId"] = new ColumnLineageField([Field("id", LineageTransformationType.Direct, LineageTransformationSubtype.Identity)]),
                ["total"] = new ColumnLineageField([
                    Field("id", LineageTransformationType.Indirect, LineageTransformationSubtype.GroupBy),
                    Field("amount", LineageTransformationType.Direct, LineageTransformationSubtype.Aggregation)
                    ])
            }, [Field("address", LineageTransformationType.Indirect, LineageTransformationSubtype.Filter)]);

            var address = new NamedStruct()
            {
                Names = ["street"],
                Struct = new Struct() { Types = [new StringType()] }
            };
            var lineage = new StreamLineage(
                "mystream",
                null,
                DateTimeOffset.UnixEpoch,
                [new StreamLineageInput()
                {
                    Key = "orders",
                    NameParts = ["orders"],
                    Namespace = "test_ns",
                    TableName = "orders",
                    PlanColumns = [new LineageColumn("id", new Int64Type()), new LineageColumn("amount", new Fp64Type()), new LineageColumn("address", address)]
                }],
                [new StreamLineageOutput()
                {
                    Key = "totals",
                    NameParts = ["totals"],
                    Namespace = "test_ns",
                    TableName = "totals",
                    PlanColumns = [new LineageColumn("CustomerId", new Int64Type()), new LineageColumn("total", new Fp64Type())],
                    ColumnLineage = columnLineage,
                    UpstreamInputKeys = ["orders"]
                }]);
            return LineageEventCreator.CreateFromLineage(RunId, lineage, true).ChangeEventType(LineageEventType.Running, EventTime);
        }

        private static LineageInputField Field(string field, LineageTransformationType type, LineageTransformationSubtype subtype)
        {
            return new LineageInputField("test_ns", "orders", field, [new LineageTransformation(type, subtype)]);
        }

        private static void CollectFacets(JsonElement element, List<(string Name, JsonElement Facet)> facets)
        {
            if (element.ValueKind == JsonValueKind.Object)
            {
                foreach (var property in element.EnumerateObject())
                {
                    if (property.Name == "facets" && property.Value.ValueKind == JsonValueKind.Object)
                    {
                        foreach (var facet in property.Value.EnumerateObject())
                        {
                            facets.Add((facet.Name, facet.Value));
                        }
                    }
                    else
                    {
                        CollectFacets(property.Value, facets);
                    }
                }
            }
            else if (element.ValueKind == JsonValueKind.Array)
            {
                foreach (var item in element.EnumerateArray())
                {
                    CollectFacets(item, facets);
                }
            }
        }

        private static void CollectPropertyNames(JsonElement element, string? parentName, List<string> names)
        {
            if (element.ValueKind == JsonValueKind.Object)
            {
                foreach (var property in element.EnumerateObject())
                {
                    // Keys under columnLineage.fields are user columns.
                    if (parentName != "fields" || property.Value.ValueKind != JsonValueKind.Object || !property.Value.TryGetProperty("inputFields", out _))
                    {
                        names.Add(property.Name);
                    }
                    CollectPropertyNames(property.Value, property.Name, names);
                }
            }
            else if (element.ValueKind == JsonValueKind.Array)
            {
                foreach (var item in element.EnumerateArray())
                {
                    CollectPropertyNames(item, parentName, names);
                }
            }
        }
    }
}
