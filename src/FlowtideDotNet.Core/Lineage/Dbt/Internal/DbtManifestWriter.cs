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

using System.Buffers;
using System.Globalization;
using System.Text.Encodings.Web;
using System.Text.Json;

namespace FlowtideDotNet.Core.Lineage.Dbt.Internal
{
    // Explicit key order, strict v12 parsers reject unknown keys.
    internal static class DbtManifestWriter
    {
        private static readonly string[] s_emptyRootObjects = ["macros", "docs", "exposures", "metrics", "groups", "selectors", "disabled"];
        private static readonly string[] s_trailingRootObjects = ["group_map", "saved_queries", "semantic_models", "unit_tests"];

        public static JsonWriterOptions WriterOptions { get; } = new JsonWriterOptions()
        {
            Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping,
            Indented = false
        };

        public static string FormatGeneratedAt(DateTimeOffset time)
        {
            return time.UtcDateTime.ToString(DbtConstants.GeneratedAtFormat, CultureInfo.InvariantCulture);
        }

        public static byte[] Write(DbtProject project, Guid invocationId)
        {
            var buffer = new ArrayBufferWriter<byte>();
            using (var writer = new Utf8JsonWriter(buffer, WriterOptions))
            {
                writer.WriteStartObject();

                writer.WriteStartObject("metadata");
                writer.WriteString("dbt_schema_version", DbtConstants.ManifestSchemaUrl);
                writer.WriteString("dbt_version", DbtConstants.DbtVersion);
                writer.WriteString("generated_at", FormatGeneratedAt(project.GeneratedAt));
                writer.WriteString("invocation_id", invocationId.ToString("D"));
                WriteEmptyObject(writer, "env");
                writer.WriteString("project_name", project.ProjectName);
                writer.WriteString("adapter_type", project.AdapterType);
                writer.WriteEndObject();

                writer.WriteStartObject("nodes");
                foreach (var model in project.Models)
                {
                    WriteModel(writer, project, model);
                }
                writer.WriteEndObject();

                writer.WriteStartObject("sources");
                foreach (var source in project.Sources)
                {
                    WriteSource(writer, project, source);
                }
                writer.WriteEndObject();

                foreach (var name in s_emptyRootObjects)
                {
                    WriteEmptyObject(writer, name);
                }
                WriteMap(writer, "parent_map", project.ParentMap);
                WriteMap(writer, "child_map", project.ChildMap);
                foreach (var name in s_trailingRootObjects)
                {
                    WriteEmptyObject(writer, name);
                }

                writer.WriteEndObject();
            }
            return buffer.WrittenSpan.ToArray();
        }

        private static void WriteModel(Utf8JsonWriter writer, DbtProject project, DbtModelNode model)
        {
            writer.WriteStartObject(model.Name.UniqueId);
            WriteDatabaseAndSchema(writer, model.Identity);
            writer.WriteString("name", model.Name.Name);
            writer.WriteString("resource_type", "model");
            writer.WriteString("package_name", project.ProjectName);
            writer.WriteString("path", model.Name.Path);
            writer.WriteString("original_file_path", model.Name.OriginalFilePath);
            writer.WriteString("unique_id", model.Name.UniqueId);
            WriteStringArray(writer, "fqn", model.Name.Fqn);
            writer.WriteString("alias", model.Identity.Identifier);

            writer.WriteStartObject("checksum");
            writer.WriteString("name", "sha256");
            writer.WriteString("checksum", model.Checksum);
            writer.WriteEndObject();

            writer.WriteStartObject("config");
            writer.WriteBoolean("enabled", true);
            writer.WriteString("materialized", "incremental");
            writer.WriteEndObject();

            WriteStringArray(writer, "tags", []);
            writer.WriteString("description", string.Empty);

            writer.WriteStartObject("meta");
            writer.WriteString(DbtConstants.StreamsMetaKey, model.Streams);
            if (model.DroppedDependencies.Count > 0)
            {
                writer.WriteString(DbtConstants.DroppedDependenciesMetaKey, string.Join(",", model.DroppedDependencies));
            }
            WriteRelationCollision(writer, model.RelationCollision);
            writer.WriteEndObject();

            WriteColumns(writer, model.Columns);
            writer.WriteString("relation_name", model.RelationName);
            writer.WriteString("raw_code", model.CompiledCode);
            writer.WriteString("language", "sql");

            writer.WriteStartArray("refs");
            foreach (var refName in model.RefNames)
            {
                writer.WriteStartObject();
                writer.WriteString("name", refName);
                writer.WriteNull("package");
                writer.WriteNull("version");
                writer.WriteEndObject();
            }
            writer.WriteEndArray();

            writer.WriteStartArray("sources");
            foreach (var (sourceName, name) in model.Sources)
            {
                writer.WriteStartArray();
                writer.WriteStringValue(sourceName);
                writer.WriteStringValue(name);
                writer.WriteEndArray();
            }
            writer.WriteEndArray();

            writer.WriteStartObject("depends_on");
            WriteStringArray(writer, "macros", []);
            WriteStringArray(writer, "nodes", model.DependsOn);
            writer.WriteEndObject();

            writer.WriteBoolean("compiled", true);
            writer.WriteString("compiled_code", model.CompiledCode);
            writer.WriteEndObject();
        }

        private static void WriteSource(Utf8JsonWriter writer, DbtProject project, DbtSourceNode source)
        {
            writer.WriteStartObject(source.Name.UniqueId);
            WriteDatabaseAndSchema(writer, source.Identity);
            writer.WriteString("name", source.Name.Name);
            writer.WriteString("resource_type", "source");
            writer.WriteString("package_name", project.ProjectName);
            writer.WriteString("path", source.Name.Path);
            writer.WriteString("original_file_path", source.Name.OriginalFilePath);
            writer.WriteString("unique_id", source.Name.UniqueId);
            WriteStringArray(writer, "fqn", source.Name.Fqn);
            writer.WriteString("source_name", source.Name.SourceName);
            writer.WriteString("source_description", string.Empty);
            writer.WriteString("loader", DbtConstants.Loader);
            writer.WriteString("identifier", source.Identity.Identifier);
            writer.WriteString("description", string.Empty);
            WriteColumns(writer, source.Columns);

            writer.WriteStartObject("meta");
            writer.WriteString(DbtConstants.StreamsMetaKey, source.Streams);
            WriteRelationCollision(writer, source.RelationCollision);
            writer.WriteEndObject();

            WriteEmptyObject(writer, "source_meta");
            WriteStringArray(writer, "tags", []);
            writer.WriteStartObject("config");
            writer.WriteBoolean("enabled", true);
            writer.WriteEndObject();
            writer.WriteString("relation_name", source.RelationName);
            writer.WriteEndObject();
        }

        private static void WriteColumns(Utf8JsonWriter writer, IReadOnlyList<DbtNodeColumn> columns)
        {
            writer.WriteStartObject("columns");
            foreach (var column in columns)
            {
                writer.WriteStartObject(column.Name);
                writer.WriteString("name", column.Name);
                writer.WriteString("description", string.Empty);
                writer.WriteStartObject("meta");
                if (column.Inputs != null)
                {
                    writer.WriteString(DbtConstants.InputsMetaKey, column.Inputs);
                }
                writer.WriteEndObject();
                // Always present, DataHub indexes it directly.
                if (column.DataType == null)
                {
                    writer.WriteNull("data_type");
                }
                else
                {
                    writer.WriteString("data_type", column.DataType);
                }
                WriteStringArray(writer, "tags", []);
                writer.WriteEndObject();
            }
            writer.WriteEndObject();
        }

        private static void WriteRelationCollision(Utf8JsonWriter writer, string? relationCollision)
        {
            if (relationCollision != null)
            {
                writer.WriteString(DbtConstants.RelationCollisionMetaKey, relationCollision);
            }
        }

        private static void WriteDatabaseAndSchema(Utf8JsonWriter writer, DbtTableIdentity identity)
        {
            if (identity.Database == null)
            {
                writer.WriteNull("database");
            }
            else
            {
                writer.WriteString("database", identity.Database);
            }
            writer.WriteString("schema", identity.Schema);
        }

        private static void WriteMap(Utf8JsonWriter writer, string name, SortedDictionary<string, List<string>> map)
        {
            writer.WriteStartObject(name);
            foreach (var kv in map)
            {
                WriteStringArray(writer, kv.Key, kv.Value);
            }
            writer.WriteEndObject();
        }

        private static void WriteStringArray(Utf8JsonWriter writer, string name, IEnumerable<string> values)
        {
            writer.WriteStartArray(name);
            foreach (var value in values)
            {
                writer.WriteStringValue(value);
            }
            writer.WriteEndArray();
        }

        private static void WriteEmptyObject(Utf8JsonWriter writer, string name)
        {
            writer.WriteStartObject(name);
            writer.WriteEndObject();
        }
    }
}
