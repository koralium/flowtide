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

using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Actions;
using Stowage;
using System.Globalization;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    // A JSON replay with its own spec rules, independent of the reconciler and of both Parquet readers
    internal static class CheckpointOracle
    {
        public static readonly TimeSpan DefaultRetention = TimeSpan.FromDays(7);

        /// <summary>
        /// The checkpoint content at <paramref name="version"/>, replayed from version 0, as sorted canonical lines.
        /// </summary>
        public static async Task<List<string>> FullHistory(IFileStorage storage, string table, long version, DateTimeOffset now, bool expire = true)
        {
            var state = new State();
            for (long v = 0; v <= version; v++)
            {
                await state.ApplyCommit(storage, table, v);
            }
            return state.Lines(now, expire);
        }

        /// <summary>
        /// The content readers reconstruct from a root's recorded actions plus the commits after it, as sorted canonical lines.
        /// </summary>
        public static async Task<List<string>> RootPlusTail(IEnumerable<DeltaAction> rootActions, IFileStorage storage, string table, long root, long version, DateTimeOffset now, bool expire = true)
        {
            var state = new State();
            foreach (var action in rootActions)
            {
                state.Apply(JsonNode.Parse(JsonSerializer.Serialize(action))!.AsObject());
            }
            for (long v = root + 1; v <= version; v++)
            {
                await state.ApplyCommit(storage, table, v);
            }
            return state.Lines(now, expire);
        }

        /// <summary>
        /// Canonical lines for actions taken from the reader's model, for comparisons with the oracle.
        /// </summary>
        public static List<string> Canonical(IEnumerable<DeltaAction> actions)
        {
            var lines = new List<string>();
            foreach (var action in actions)
            {
                var node = JsonNode.Parse(JsonSerializer.Serialize(action))!.AsObject();
                var line = CanonicalLine(node);
                if (line != null)
                {
                    lines.Add(line);
                }
            }
            lines.Sort(StringComparer.Ordinal);
            return lines;
        }

        private sealed class State
        {
            private readonly Dictionary<string, JsonObject> _files = new Dictionary<string, JsonObject>();
            private readonly Dictionary<string, JsonObject> _txns = new Dictionary<string, JsonObject>();
            private readonly Dictionary<string, JsonObject> _domains = new Dictionary<string, JsonObject>();
            private JsonObject? _protocol;
            private JsonObject? _metaData;

            public async Task ApplyCommit(IFileStorage storage, string table, long version)
            {
                var text = await storage.ReadText($"/{table}/_delta_log/{version:D20}.json") ?? throw new InvalidOperationException($"Commit {version} of {table} is missing");
                foreach (var line in text.Split('\n'))
                {
                    if (!string.IsNullOrWhiteSpace(line))
                    {
                        // Through the action model, so absent fields read as the reader reads them and unmodeled ones drop
                        var action = JsonSerializer.Deserialize<DeltaAction>(line)!;
                        Apply(JsonNode.Parse(JsonSerializer.Serialize(action))!.AsObject());
                    }
                }
            }

            // The latest action per key wins; adds and removes share the file key
            public void Apply(JsonObject action)
            {
                if (action["add"] is JsonObject add)
                {
                    _files[FileKey(add)] = new JsonObject() { ["add"] = add.DeepClone() };
                }
                else if (action["remove"] is JsonObject remove)
                {
                    _files[FileKey(remove)] = new JsonObject() { ["remove"] = remove.DeepClone() };
                }
                else if (action["txn"] is JsonObject txn)
                {
                    _txns[(string)txn["appId"]!] = new JsonObject() { ["txn"] = txn.DeepClone() };
                }
                else if (action["domainMetadata"] is JsonObject domain)
                {
                    _domains[(string)domain["domain"]!] = new JsonObject() { ["domainMetadata"] = domain.DeepClone() };
                }
                else if (action["protocol"] is JsonObject protocol)
                {
                    _protocol = new JsonObject() { ["protocol"] = protocol.DeepClone() };
                }
                else if (action["metaData"] is JsonObject metaData)
                {
                    _metaData = new JsonObject() { ["metaData"] = metaData.DeepClone() };
                }
            }

            public List<string> Lines(DateTimeOffset now, bool expire)
            {
                var lines = new List<string>();
                // Expires a tombstone only when now exceeds its timestamp plus the retention, or it has no timestamp
                Int128 cutoffTicks = (Int128)(now.UtcTicks - DateTimeOffset.UnixEpoch.UtcTicks) - Retention().Ticks;
                foreach (var entry in new[] { _protocol, _metaData }.Concat(_files.Values).Concat(_txns.Values).Concat(_domains.Values))
                {
                    if (entry == null)
                    {
                        continue;
                    }
                    if (entry["remove"] is JsonObject remove && expire)
                    {
                        var timestamp = remove["deletionTimestamp"];
                        if (timestamp == null || (Int128)timestamp.GetValue<long>() * 10_000 < cutoffTicks)
                        {
                            continue;
                        }
                    }
                    if (entry["domainMetadata"] is JsonObject domain && domain["removed"]?.GetValue<bool>() == true)
                    {
                        continue;
                    }
                    lines.Add(CanonicalLine(entry)!);
                }
                lines.Sort(StringComparer.Ordinal);
                return lines;
            }

            private TimeSpan Retention()
            {
                var value = _metaData?["metaData"]?["configuration"]?["delta.deletedFileRetentionDuration"]?.GetValue<string>();
                return value == null ? DefaultRetention : ParseInterval(value);
            }
        }

        // Path plus the deletion vector's unique id, as the spec defines it
        private static string FileKey(JsonObject file)
        {
            var path = (string)file["path"]!;
            if (file["deletionVector"] is not JsonObject dv)
            {
                return $"{path.Length}:{path}|";
            }
            var id = (string)dv["storageType"]! + (string)dv["pathOrInlineDv"]!;
            if (dv["offset"] is JsonNode offset)
            {
                id += "@" + offset.GetValue<long>().ToString(CultureInfo.InvariantCulture);
            }
            return $"{path.Length}:{path}|{id}";
        }

        private static readonly string[] s_addFields = { "path", "partitionValues", "size", "modificationTime", "dataChange", "stats", "tags", "deletionVector", "baseRowId", "defaultRowCommitVersion", "clusteringProvider" };
        private static readonly string[] s_removeFields = { "path", "deletionTimestamp", "dataChange", "extendedFileMetadata", "partitionValues", "size", "deletionVector", "baseRowId", "defaultRowCommitVersion" };
        private static readonly string[] s_metaDataFields = { "id", "name", "description", "format", "schemaString", "partitionColumns", "configuration", "createdTime" };
        private static readonly string[] s_protocolFields = { "minReaderVersion", "minWriterVersion", "readerFeatures", "writerFeatures" };
        private static readonly string[] s_txnFields = { "appId", "version", "lastUpdated" };
        private static readonly string[] s_domainFields = { "domain", "configuration", "removed" };
        private static readonly string[] s_dvFields = { "storageType", "pathOrInlineDv", "offset", "sizeInBytes", "cardinality" };

        // The checkpoint's view of one action: fixed field order, maps sorted by key, dataChange false, no stats or tags on removes
        private static string? CanonicalLine(JsonObject action)
        {
            foreach (var (kind, fields) in new[] { ("add", s_addFields), ("remove", s_removeFields), ("metaData", s_metaDataFields), ("protocol", s_protocolFields), ("txn", s_txnFields), ("domainMetadata", s_domainFields) })
            {
                if (action[kind] is JsonObject body)
                {
                    var builder = new StringBuilder();
                    builder.Append('{').Append(JsonSerializer.Serialize(kind)).Append(":{");
                    for (int i = 0; i < fields.Length; i++)
                    {
                        var field = fields[i];
                        JsonNode? value = field == "dataChange" ? JsonValue.Create(false) : body[field];
                        if (field == "removed")
                        {
                            value = JsonValue.Create(false);
                        }
                        if (i > 0)
                        {
                            builder.Append(',');
                        }
                        builder.Append(JsonSerializer.Serialize(field)).Append(':');
                        AppendValue(builder, field, value);
                    }
                    builder.Append("}}");
                    return builder.ToString();
                }
            }
            return null;
        }

        private static void AppendValue(StringBuilder builder, string field, JsonNode? value)
        {
            if (value == null)
            {
                builder.Append("null");
            }
            else if (field == "deletionVector" && value is JsonObject dv)
            {
                builder.Append('{');
                for (int i = 0; i < s_dvFields.Length; i++)
                {
                    if (i > 0)
                    {
                        builder.Append(',');
                    }
                    builder.Append(JsonSerializer.Serialize(s_dvFields[i])).Append(':').Append(dv[s_dvFields[i]]?.ToJsonString() ?? "null");
                }
                builder.Append('}');
            }
            else if (field == "format" && value is JsonObject format)
            {
                builder.Append("{\"provider\":").Append(format["provider"]?.ToJsonString() ?? "null").Append(",\"options\":");
                AppendMap(builder, format["options"] as JsonObject);
                builder.Append('}');
            }
            else if (value is JsonObject map)
            {
                AppendMap(builder, map);
            }
            else if (value is JsonArray array && (field == "readerFeatures" || field == "writerFeatures"))
            {
                // Features are a set
                var items = array.Select(x => x!.GetValue<string>()).OrderBy(x => x, StringComparer.Ordinal).Select(x => JsonSerializer.Serialize(x));
                builder.Append('[').Append(string.Join(",", items)).Append(']');
            }
            else
            {
                builder.Append(value.ToJsonString());
            }
        }

        // Maps compare without regard to order; null values are kept
        private static void AppendMap(StringBuilder builder, JsonObject? map)
        {
            if (map == null)
            {
                builder.Append("null");
                return;
            }
            builder.Append('{');
            var first = true;
            foreach (var entry in map.OrderBy(x => x.Key, StringComparer.Ordinal))
            {
                if (!first)
                {
                    builder.Append(',');
                }
                first = false;
                builder.Append(JsonSerializer.Serialize(entry.Key)).Append(':').Append(entry.Value?.ToJsonString() ?? "null");
            }
            builder.Append('}');
        }

        private static readonly Regex s_interval = new Regex(@"^\s*(?:interval\s+)?(-?\d+)\s+([a-z]+)\s*$", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);

        // A minimal reading of the Spark interval forms the tests use, kept apart from the product's parser
        public static TimeSpan ParseInterval(string value)
        {
            var match = s_interval.Match(value);
            if (!match.Success)
            {
                throw new FormatException($"Unsupported interval '{value}'");
            }
            var amount = long.Parse(match.Groups[1].Value, CultureInfo.InvariantCulture);
            var unit = match.Groups[2].Value.ToLowerInvariant().TrimEnd('s');
            long ticksPerUnit = unit switch
            {
                "week" => TimeSpan.TicksPerDay * 7,
                "day" => TimeSpan.TicksPerDay,
                "hour" => TimeSpan.TicksPerHour,
                "minute" => TimeSpan.TicksPerMinute,
                "second" => TimeSpan.TicksPerSecond,
                "millisecond" => TimeSpan.TicksPerMillisecond,
                "microsecond" => 10,
                _ => throw new FormatException($"Unsupported interval unit '{unit}'")
            };
            return TimeSpan.FromTicks(checked(amount * ticksPerUnit));
        }
    }
}
