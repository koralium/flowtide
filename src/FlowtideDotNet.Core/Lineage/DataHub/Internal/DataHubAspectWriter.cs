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
using FlowtideDotNet.Substrait.Type;
using System.Buffers;
using System.Text.Json;

namespace FlowtideDotNet.Core.Lineage.DataHub.Internal
{
    internal sealed record DataHubFineGrainedLineage(IReadOnlyList<string> Upstreams, string Downstream, string? TransformOperation);

    internal sealed record DataHubSchemaField(string FieldPath, SubstraitBaseType Type);

    // Entity in the GMS entitiesV2 response shape.
    internal sealed class DataHubEntityBuilder
    {
        private readonly List<string> _names = new List<string>();
        private readonly Dictionary<string, Action<Utf8JsonWriter>> _aspects = new Dictionary<string, Action<Utf8JsonWriter>>(StringComparer.Ordinal);

        public DataHubEntityBuilder(string entityName, string urn)
        {
            EntityName = entityName;
            Urn = urn;
        }

        public string EntityName { get; }

        public string Urn { get; }

        public int AspectCount => _names.Count;

        // A later aspect with the same name replaces the earlier one.
        public void Set(string name, Action<Utf8JsonWriter> writeValue)
        {
            if (!_aspects.ContainsKey(name))
            {
                _names.Add(name);
            }
            _aspects[name] = writeValue;
        }

        public byte[] ToUtf8Json()
        {
            var buffer = new ArrayBufferWriter<byte>();
            using (var writer = new Utf8JsonWriter(buffer))
            {
                writer.WriteStartObject();
                writer.WriteString("entityName", EntityName);
                writer.WriteString("urn", Urn);
                writer.WriteStartObject("aspects");
                foreach (var name in _names)
                {
                    writer.WriteStartObject(name);
                    writer.WriteString("name", name);
                    writer.WriteString("type", "VERSIONED");
                    writer.WriteNumber("version", 0);
                    writer.WritePropertyName("value");
                    _aspects[name](writer);
                    writer.WriteEndObject();
                }
                writer.WriteEndObject();
                writer.WriteEndObject();
            }
            return buffer.WrittenSpan.ToArray();
        }
    }

    // Aspect values in Pegasus JSON, the form GMS returns.
    internal static class DataHubAspectWriter
    {
        public static void WriteStatus(Utf8JsonWriter writer)
        {
            writer.WriteStartObject();
            writer.WriteBoolean("removed", false);
            writer.WriteEndObject();
        }

        public static void WriteDataFlowInfo(Utf8JsonWriter writer, string name, string env)
        {
            writer.WriteStartObject();
            writer.WriteStartObject("customProperties");
            writer.WriteEndObject();
            writer.WriteString("name", name);
            writer.WriteString("env", env);
            writer.WriteEndObject();
        }

        public static void WriteDataJobInfo(Utf8JsonWriter writer, string name, string flowUrn, string env, string @namespace, string tableName)
        {
            writer.WriteStartObject();
            writer.WriteStartObject("customProperties");
            writer.WriteString("flowtide.namespace", @namespace);
            writer.WriteString("flowtide.table", tableName);
            writer.WriteEndObject();
            writer.WriteString("name", name);
            writer.WriteStartObject("type");
            writer.WriteString("string", "STREAMING");
            writer.WriteEndObject();
            writer.WriteString("flowUrn", flowUrn);
            writer.WriteString("env", env);
            writer.WriteEndObject();
        }

        public static void WriteDataJobInputOutput(Utf8JsonWriter writer, IReadOnlyList<string> inputs, string output, IReadOnlyList<DataHubFineGrainedLineage> fineGrainedLineages)
        {
            writer.WriteStartObject();
            WriteStringArray(writer, "inputDatasets", inputs);
            WriteStringArray(writer, "outputDatasets", [output]);
            if (fineGrainedLineages.Count > 0)
            {
                writer.WriteStartArray("fineGrainedLineages");
                foreach (var lineage in fineGrainedLineages)
                {
                    writer.WriteStartObject();
                    writer.WriteString("upstreamType", "FIELD_SET");
                    WriteStringArray(writer, "upstreams", lineage.Upstreams);
                    writer.WriteString("downstreamType", "FIELD");
                    WriteStringArray(writer, "downstreams", [lineage.Downstream]);
                    if (lineage.TransformOperation != null)
                    {
                        writer.WriteString("transformOperation", lineage.TransformOperation);
                    }
                    writer.WriteNumber("confidenceScore", 1.0);
                    writer.WriteEndObject();
                }
                writer.WriteEndArray();
            }
            writer.WriteEndObject();
        }

        public static void WriteSchemaMetadata(Utf8JsonWriter writer, string schemaName, string platformUrn, IReadOnlyList<DataHubSchemaField> fields)
        {
            writer.WriteStartObject();
            writer.WriteString("schemaName", schemaName);
            writer.WriteString("platform", platformUrn);
            writer.WriteNumber("version", 0);
            writer.WriteString("hash", string.Empty);
            writer.WriteStartObject("platformSchema");
            writer.WriteStartObject("com.linkedin.schema.OtherSchema");
            writer.WriteString("rawSchema", string.Empty);
            writer.WriteEndObject();
            writer.WriteEndObject();
            writer.WriteStartArray("fields");
            foreach (var field in fields)
            {
                writer.WriteStartObject();
                writer.WriteString("fieldPath", field.FieldPath);
                writer.WriteBoolean("nullable", field.Type.Nullable);
                writer.WriteStartObject("type");
                writer.WriteStartObject("type");
                writer.WriteStartObject("com.linkedin.schema." + GetSchemaFieldType(field.Type));
                writer.WriteEndObject();
                writer.WriteEndObject();
                writer.WriteEndObject();
                writer.WriteString("nativeDataType", LineageSchemaConverter.ToTypeName(field.Type));
                writer.WriteEndObject();
            }
            writer.WriteEndArray();
            writer.WriteEndObject();
        }

        public static void WriteDataPlatformInstance(Utf8JsonWriter writer, string platformUrn, string instanceUrn)
        {
            writer.WriteStartObject();
            writer.WriteString("platform", platformUrn);
            writer.WriteString("instance", instanceUrn);
            writer.WriteEndObject();
        }

        private static string GetSchemaFieldType(SubstraitBaseType type)
        {
            return type switch
            {
                BoolType => "BooleanType",
                Int32Type or Int64Type or Fp32Type or Fp64Type or DecimalType => "NumberType",
                StringType => "StringType",
                BinaryType => "BytesType",
                DateType => "DateType",
                TimestampType => "TimeType",
                ListType => "ArrayType",
                MapType => "MapType",
                NamedStruct => "RecordType",
                _ => "NullType"
            };
        }

        private static void WriteStringArray(Utf8JsonWriter writer, string propertyName, IReadOnlyList<string> values)
        {
            writer.WriteStartArray(propertyName);
            foreach (var value in values)
            {
                writer.WriteStringValue(value);
            }
            writer.WriteEndArray();
        }
    }
}
