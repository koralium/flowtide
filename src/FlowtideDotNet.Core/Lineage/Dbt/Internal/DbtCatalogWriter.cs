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
using System.Text.Json;

namespace FlowtideDotNet.Core.Lineage.Dbt.Internal
{
    // Every manifest node is listed, DataHub warns per missing one.
    internal static class DbtCatalogWriter
    {
        public static byte[] Write(DbtProject project, Guid invocationId)
        {
            var buffer = new ArrayBufferWriter<byte>();
            using (var writer = new Utf8JsonWriter(buffer, DbtManifestWriter.WriterOptions))
            {
                writer.WriteStartObject();

                writer.WriteStartObject("metadata");
                writer.WriteString("dbt_schema_version", DbtConstants.CatalogSchemaUrl);
                writer.WriteString("dbt_version", DbtConstants.DbtVersion);
                writer.WriteString("generated_at", DbtManifestWriter.FormatGeneratedAt(project.GeneratedAt));
                writer.WriteString("invocation_id", invocationId.ToString("D"));
                writer.WriteStartObject("env");
                writer.WriteEndObject();
                writer.WriteEndObject();

                writer.WriteStartObject("nodes");
                foreach (var model in project.Models)
                {
                    WriteTable(writer, model.Name.UniqueId, model.Identity, model.Columns);
                }
                writer.WriteEndObject();

                writer.WriteStartObject("sources");
                foreach (var source in project.Sources)
                {
                    WriteTable(writer, source.Name.UniqueId, source.Identity, source.Columns);
                }
                writer.WriteEndObject();

                writer.WriteNull("errors");
                writer.WriteEndObject();
            }
            return buffer.WrittenSpan.ToArray();
        }

        private static void WriteTable(Utf8JsonWriter writer, string uniqueId, DbtTableIdentity identity, IReadOnlyList<DbtNodeColumn> columns)
        {
            writer.WriteStartObject(uniqueId);

            writer.WriteStartObject("metadata");
            writer.WriteString("type", "BASE TABLE");
            writer.WriteString("schema", identity.Schema);
            writer.WriteString("name", identity.Identifier);
            if (identity.Database == null)
            {
                writer.WriteNull("database");
            }
            else
            {
                writer.WriteString("database", identity.Database);
            }
            writer.WriteNull("comment");
            writer.WriteNull("owner");
            writer.WriteEndObject();

            writer.WriteStartObject("columns");
            for (int i = 0; i < columns.Count; i++)
            {
                writer.WriteStartObject(columns[i].Name);
                writer.WriteString("type", columns[i].DataType ?? "unknown");
                writer.WriteNumber("index", i + 1);
                writer.WriteString("name", columns[i].Name);
                writer.WriteNull("comment");
                writer.WriteEndObject();
            }
            writer.WriteEndObject();

            writer.WriteStartObject("stats");
            writer.WriteStartObject("has_stats");
            writer.WriteString("id", "has_stats");
            writer.WriteString("label", "Has Stats?");
            writer.WriteBoolean("value", false);
            writer.WriteBoolean("include", false);
            writer.WriteString("description", "Indicates whether there are statistics for this table");
            writer.WriteEndObject();
            writer.WriteEndObject();

            writer.WriteString("unique_id", uniqueId);
            writer.WriteEndObject();
        }
    }
}
