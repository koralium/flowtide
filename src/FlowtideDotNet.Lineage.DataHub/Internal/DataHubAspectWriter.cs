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

using FlowtideDotNet.Base.Engine;
using FlowtideDotNet.Core.Lineage.Internal;
using FlowtideDotNet.Substrait.Type;
using System.Buffers;
using System.Globalization;
using System.Text.Json;

namespace FlowtideDotNet.Lineage.DataHub.Internal
{
    // A field set downstream for columns that affect every written column, such as join keys.
    internal sealed record DataHubFineGrainedLineage(IReadOnlyList<string> Upstreams, IReadOnlyList<string> Downstreams, bool DownstreamFieldSet, string? TransformOperation);

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

        // Renders every aspect once, concurrent requests then share no aspect writer.
        public void Freeze()
        {
            foreach (var name in _names)
            {
                var buffer = new ArrayBufferWriter<byte>();
                using (var writer = new Utf8JsonWriter(buffer))
                {
                    _aspects[name](writer);
                }
                var json = buffer.WrittenSpan.ToArray();
                _aspects[name] = w => w.WriteRawValue(json, skipInputValidation: true);
            }
        }

        // The timeseries aspect is written per request, the builder stays unchanged.
        public byte[] ToUtf8Json(string? timeseriesName = null, Action<Utf8JsonWriter>? writeTimeseries = null)
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
                    WriteAspect(writer, name, "VERSIONED", _aspects[name]);
                }
                if (timeseriesName != null && writeTimeseries != null)
                {
                    WriteAspect(writer, timeseriesName, "TIMESERIES", writeTimeseries);
                }
                writer.WriteEndObject();
                writer.WriteEndObject();
            }
            return buffer.WrittenSpan.ToArray();
        }

        private static void WriteAspect(Utf8JsonWriter writer, string name, string type, Action<Utf8JsonWriter> writeValue)
        {
            writer.WriteStartObject(name);
            writer.WriteString("name", name);
            writer.WriteString("type", type);
            writer.WriteNumber("version", 0);
            writer.WritePropertyName("value");
            writeValue(writer);
            writer.WriteEndObject();
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
                    writer.WriteString("downstreamType", lineage.DownstreamFieldSet ? "FIELD_SET" : "FIELD");
                    WriteStringArray(writer, "downstreams", lineage.Downstreams);
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

        // A custom assertion like the dbt and Great Expectations integrations write.
        public static void WriteAssertionInfo(Utf8JsonWriter writer, string datasetUrn, string description, string streamName, IEnumerable<string> checkIds)
        {
            writer.WriteStartObject();
            writer.WriteStartObject("customProperties");
            writer.WriteString("flowtide.stream", streamName);
            writer.WriteString("flowtide.checkIds", string.Join(",", checkIds));
            writer.WriteEndObject();
            writer.WriteString("type", "CUSTOM");
            writer.WriteStartObject("customAssertion");
            writer.WriteString("type", "Flowtide Check");
            writer.WriteString("entity", datasetUrn);
            writer.WriteEndObject();
            writer.WriteStartObject("source");
            writer.WriteString("type", "EXTERNAL");
            writer.WriteEndObject();
            writer.WriteString("description", description);
            writer.WriteString("entityUrn", datasetUrn);
            writer.WriteEndObject();
        }

        // Every result carries the same fields, so an overwritten event never keeps old values.
        public static void WriteAssertionRunEvent(Utf8JsonWriter writer, string assertionUrn, string datasetUrn, string streamName, DataHubAssertionResult result)
        {
            writer.WriteStartObject();
            writer.WriteNumber("timestampMillis", result.TimestampMillis);
            writer.WriteString("runId", streamName);
            writer.WriteString("asserteeUrn", datasetUrn);
            writer.WriteString("status", "COMPLETE");
            writer.WriteStartObject("result");
            writer.WriteString("type", result.State == CheckState.Failed ? "FAILURE" : "SUCCESS");
            // DataHub has three severities, critical and high incidents are both high.
            if (result.State == CheckState.Failed)
            {
                writer.WriteString("severity", result.Priority switch
                {
                    DataHubIncidentPriority.Critical or DataHubIncidentPriority.High => "HIGH",
                    DataHubIncidentPriority.Medium => "MEDIUM",
                    _ => "LOW"
                });
            }
            writer.WriteNumber("unexpectedCount", result.FailingRows);
            writer.WriteStartObject("nativeResults");
            writer.WriteString("activeIssues", result.ActiveIssues.ToString(CultureInfo.InvariantCulture));
            writer.WriteString("failingRows", result.FailingRows.ToString(CultureInfo.InvariantCulture));
            writer.WriteEndObject();
            writer.WriteEndObject();
            writer.WriteString("assertionUrn", assertionUrn);
            writer.WriteStartObject("partitionSpec");
            writer.WriteString("type", "FULL_TABLE");
            writer.WriteString("partition", "FULL_TABLE_SNAPSHOT");
            writer.WriteEndObject();
            writer.WriteEndObject();
        }

        // DataHub's summary hook fails on a CUSTOM incident without customType.
        public static void WriteIncidentInfo(Utf8JsonWriter writer, DataHubIncidentState incident)
        {
            var info = incident.Info;
            writer.WriteStartObject();
            writer.WriteString("type", "CUSTOM");
            writer.WriteString("customType", "Flowtide Check");
            writer.WriteString("title", info.Title);
            writer.WriteString("description", $"Raised by the Flowtide check '{info.Title}' in stream '{info.StreamName}'.");
            WriteStringArray(writer, "entities", [info.DatasetUrn]);
            writer.WriteNumber("priority", (int)info.Priority);
            writer.WriteStartObject("status");
            writer.WriteString("state", incident.Active ? "ACTIVE" : "RESOLVED");
            if (!incident.Active)
            {
                writer.WriteString("message", incident.CheckRemoved ? "Resolved by Flowtide, the check was removed from the stream." : "Resolved by Flowtide, the check reports no failing rows.");
            }
            WriteAuditStamp(writer, "lastUpdated", incident.LastUpdatedMillis);
            writer.WriteEndObject();
            writer.WriteStartObject("source");
            writer.WriteString("type", "ASSERTION_FAILURE");
            writer.WriteString("sourceUrn", info.AssertionUrn);
            writer.WriteEndObject();
            writer.WriteNumber("startedAt", incident.StartedAtMillis);
            WriteAuditStamp(writer, "created", incident.CreatedMillis);
            writer.WriteEndObject();
        }

        public static void WriteDataProcessInstanceProperties(Utf8JsonWriter writer, DataHubRunInfo run, long createdMillis)
        {
            writer.WriteStartObject();
            writer.WriteStartObject("customProperties");
            writer.WriteString("flowtide.stream", run.StreamName);
            if (run.SubstreamName != null)
            {
                writer.WriteString("flowtide.substream", run.SubstreamName);
            }
            writer.WriteEndObject();
            writer.WriteString("name", run.Name);
            writer.WriteString("type", "STREAMING");
            WriteAuditStamp(writer, "created", createdMillis);
            writer.WriteEndObject();
        }

        // DataHub requires upstreamInstances, even when empty.
        public static void WriteDataProcessInstanceRelationships(Utf8JsonWriter writer, string parentUrn)
        {
            writer.WriteStartObject();
            writer.WriteString("parentTemplate", parentUrn);
            WriteStringArray(writer, "upstreamInstances", []);
            writer.WriteEndObject();
        }

        // Plain inputs and outputs fill the runs table, edges would add every run to lineage.
        public static void WriteDataProcessInstanceDatasets(Utf8JsonWriter writer, string propertyName, IReadOnlyList<string> datasetUrns)
        {
            writer.WriteStartObject();
            WriteStringArray(writer, propertyName, datasetUrns);
            writer.WriteEndObject();
        }

        // DataHub shows a run as running until a COMPLETE event, which needs a result to show its outcome.
        public static void WriteDataProcessInstanceRunEvent(Utf8JsonWriter writer, DataHubRunState run)
        {
            writer.WriteStartObject();
            writer.WriteNumber("timestampMillis", run.EventMillis);
            if (run.ResultType == null)
            {
                writer.WriteString("status", "STARTED");
            }
            else
            {
                writer.WriteString("status", "COMPLETE");
                writer.WriteStartObject("result");
                writer.WriteString("type", run.ResultType);
                writer.WriteString("nativeResultType", "flowtide");
                writer.WriteEndObject();
                writer.WriteNumber("durationMillis", run.EventMillis - run.StartedMillis);
            }
            writer.WriteEndObject();
        }

        // Same shape as 'datahub put platform' writes.
        public static void WriteDataPlatformInfo(Utf8JsonWriter writer, string name, string displayName, string? logoUrl)
        {
            writer.WriteStartObject();
            writer.WriteString("name", name);
            writer.WriteString("displayName", displayName);
            writer.WriteString("type", "OTHERS");
            writer.WriteString("datasetNameDelimiter", ".");
            if (logoUrl != null)
            {
                writer.WriteString("logoUrl", logoUrl);
            }
            writer.WriteEndObject();
        }

        public static void WriteDataPlatformInstance(Utf8JsonWriter writer, string platformUrn, string? instanceUrn)
        {
            writer.WriteStartObject();
            writer.WriteString("platform", platformUrn);
            if (instanceUrn != null)
            {
                writer.WriteString("instance", instanceUrn);
            }
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

        private static void WriteAuditStamp(Utf8JsonWriter writer, string propertyName, long timeMillis)
        {
            writer.WriteStartObject(propertyName);
            writer.WriteNumber("time", timeMillis);
            writer.WriteString("actor", "urn:li:corpuser:flowtide");
            writer.WriteEndObject();
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
