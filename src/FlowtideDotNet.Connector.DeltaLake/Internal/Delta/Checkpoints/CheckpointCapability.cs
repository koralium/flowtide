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
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance;
using System.Text;
using System.Text.Json;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Checkpoints
{
    /// <summary>
    /// Whether a classic V1 checkpoint can be written for a table, and the per-row rules it needs.
    /// </summary>
    internal static class CheckpointCapability
    {
        /// <summary>
        /// Writer features a classic V1 checkpoint handles correctly, with the reason; rendered into the docs.
        /// </summary>
        public static readonly IReadOnlyDictionary<string, string> Allowed = new Dictionary<string, string>()
        {
            ["appendOnly"] = "Constrains data writes; the checkpoint copies metaData unchanged",
            ["invariants"] = "Constrains data writes; the checkpoint copies metaData unchanged",
            ["checkConstraints"] = "Constrains data writes; the checkpoint copies metaData unchanged",
            ["generatedColumns"] = "Constrains data writes; the checkpoint copies metaData unchanged",
            ["identityColumns"] = "Constrains data writes; the checkpoint copies metaData unchanged",
            ["allowColumnDefaults"] = "Constrains data writes; the checkpoint copies metaData unchanged",
            ["changeDataFeed"] = "The spec excludes cdc actions from checkpoints",
            ["columnMapping"] = "Schema rules live in metaData; JSON stats and partition values are copied as written",
            ["timestampNtz"] = "Type rules live in metaData; JSON stats and partition values are copied as written",
            ["typeWidening"] = "Type rules live in metaData (type widening RFC)",
            ["typeWidening-preview"] = "Type rules live in metaData (type widening RFC)",
            ["variantType"] = "Type rules live in metaData (variant RFC)",
            ["variantType-preview"] = "Type rules live in metaData (variant RFC)",
            ["deletionVectors"] = "Deletion vector descriptors are copied; numRecords is required and checked",
            ["rowTracking"] = "baseRowId and defaultRowCommitVersion are copied; the delta.rowTracking domain is copied",
            ["domainMetadata"] = "Domain actions are written; removed domains are excluded",
            ["clustering"] = "Its domain is written like any other domain",
            ["v2Checkpoint"] = "Writers may always create a classic V1 checkpoint",
            ["inCommitTimestamp"] = "Lives in commitInfo and metaData.configuration; commitInfo is not checkpointed",
            ["icebergCompatV1"] = "Constrains data writes only; no checkpoint rule",
            ["icebergCompatV2"] = "Constrains data writes only; no checkpoint rule",
            ["materializePartitionColumns"] = "Writer-only; requires partition columns in new data files, no checkpoint rule",
            ["vacuumProtocolCheck"] = "Restricts cleanup, not checkpoints"
        };

        /// <summary>
        /// Features a checkpoint must decline, with the reason; every unlisted feature declines too.
        /// </summary>
        public static readonly IReadOnlyDictionary<string, string> Excluded = new Dictionary<string, string>()
        {
            ["checkpointProtection"] = "Checkpoints below delta.requireCheckpointProtectionBeforeVersion need support for every protocol feature",
            ["catalogManaged"] = "Commits may not be in _delta_log",
            ["catalogOwned-preview"] = "Commits may not be in _delta_log",
            ["coordinatedCommits-preview"] = "Commits may not be in _delta_log"
        };

        private static readonly string[][] s_implied =
        {
            Array.Empty<string>(),
            Array.Empty<string>(),
            new[] { "appendOnly", "invariants" },
            new[] { "appendOnly", "invariants", "checkConstraints" },
            new[] { "appendOnly", "invariants", "checkConstraints", "changeDataFeed", "generatedColumns" },
            new[] { "appendOnly", "invariants", "checkConstraints", "changeDataFeed", "generatedColumns", "columnMapping" },
            new[] { "appendOnly", "invariants", "checkConstraints", "changeDataFeed", "generatedColumns", "columnMapping", "identityColumns" }
        };

        /// <summary>
        /// The writer features in effect: implied by writer versions 1 to 6, listed for writer 7. Unknown versions and combinations decline.
        /// </summary>
        public static IReadOnlySet<string> WriterFeatures(DeltaProtocolAction protocol)
        {
            var writer = protocol.MinWriterVersion;
            var reader = protocol.MinReaderVersion;
            if (writer < 1 || writer > 7)
            {
                throw Unknown(protocol, "writer version");
            }
            if (reader < 1 || reader > 3)
            {
                throw Unknown(protocol, "reader version");
            }
            if (writer < 7)
            {
                if (reader == 3)
                {
                    throw Unknown(protocol, "reader 3 needs writer 7");
                }
                return new HashSet<string>(s_implied[writer], StringComparer.Ordinal);
            }
            if (protocol.WriterFeatures == null)
            {
                throw Unknown(protocol, "writer 7 without writerFeatures");
            }
            if (protocol.WriterFeatures.Contains(null!) || (protocol.ReaderFeatures?.Contains(null!) ?? false))
            {
                throw Unknown(protocol, "a feature without a name");
            }
            var features = new HashSet<string>(protocol.WriterFeatures, StringComparer.Ordinal);
            if (reader == 3)
            {
                if (protocol.ReaderFeatures == null)
                {
                    throw Unknown(protocol, "reader 3 without readerFeatures");
                }
                foreach (var feature in protocol.ReaderFeatures)
                {
                    if (!features.Contains(feature))
                    {
                        throw Unknown(protocol, $"reader feature {feature} is not a writer feature");
                    }
                }
            }
            return features;
        }

        private static CheckpointDeclinedException Unknown(DeltaProtocolAction protocol, string detail)
        {
            return new CheckpointDeclinedException(MaintenanceReason.UnknownProtocolVersion, DeclineDependency.Protocol,
                $"reader {protocol.MinReaderVersion}, writer {protocol.MinWriterVersion}: {detail}");
        }

        /// <summary>
        /// Declines unless every feature in effect is allowlisted, and the target layout writes JSON stats and no struct stats.
        /// </summary>
        public static IReadOnlySet<string> Check(DeltaProtocolAction protocol, DeltaMetadataAction metaData)
        {
            var features = WriterFeatures(protocol);
            foreach (var feature in features.Order(StringComparer.Ordinal))
            {
                if (Excluded.TryGetValue(feature, out var reason))
                {
                    throw new CheckpointDeclinedException(MaintenanceReason.ExcludedFeature, DeclineDependency.Protocol, $"{feature}: {reason}");
                }
                if (!Allowed.ContainsKey(feature))
                {
                    throw new CheckpointDeclinedException(MaintenanceReason.UnsupportedFeature, DeclineDependency.Protocol, $"writer feature {feature} is not on the allowlist");
                }
            }
            var configuration = metaData.Configuration;
            if (Setting(configuration, "delta.checkpoint.writeStatsAsJson") == false || Setting(configuration, "delta.checkpoint.writeStatsAsStruct") == true)
            {
                throw new CheckpointDeclinedException(MaintenanceReason.CheckpointLayout, DeclineDependency.Protocol, "the table asks for a checkpoint layout without JSON stats or with struct stats");
            }
            return features;
        }

        private static bool? Setting(IReadOnlyDictionary<string, string>? configuration, string key)
        {
            if (configuration == null || !configuration.TryGetValue(key, out var value) || value == null)
            {
                return null;
            }
            return bool.TryParse(value.Trim(), out var parsed) ? parsed : null;
        }

        /// <summary>
        /// The per-row feature rules on an emitted record.
        /// </summary>
        public static void CheckEmitted(ReadOnlySpan<byte> record, IReadOnlySet<string> writerFeatures)
        {
            var kind = CheckpointRecord.Kind(record);
            if (kind == RecordKind.Domain)
            {
                if (!writerFeatures.Contains("domainMetadata"))
                {
                    throw new CheckpointDeclinedException(MaintenanceReason.DomainWithoutFeature, DeclineDependency.None, "a domainMetadata row without the domainMetadata writer feature");
                }
                return;
            }
            if (kind != RecordKind.Add && kind != RecordKind.Remove)
            {
                return;
            }
            var r = new RecordReader(record);
            r.ReadKind(out _);
            r.SkipString();
            ReadOnlySpan<byte> stats = default;
            var hasStats = false;
            if (kind == RecordKind.Add)
            {
                r.SkipMap();
                r.SkipLong();
                r.SkipLong();
                r.ReadBool();
                hasStats = r.ReadUtf8(out stats);
                r.SkipMap();
            }
            else
            {
                r.SkipLong();
                r.ReadBool();
                r.ReadBool();
                r.SkipMap();
                r.SkipLong();
            }
            if (!r.ReadStruct())
            {
                return;
            }
            if (!writerFeatures.Contains("deletionVectors"))
            {
                throw new CheckpointDeclinedException(MaintenanceReason.DvWithoutFeature, DeclineDependency.None, "a deletion vector without the deletionVectors writer feature");
            }
            if (kind == RecordKind.Add && (!hasStats || !HasRecordCount(stats)))
            {
                throw new CheckpointDeclinedException(MaintenanceReason.DvWithoutRecordCount, DeclineDependency.None, "a file with a deletion vector has no record count");
            }
        }

        // A top level, non negative integer numRecords
        public static bool HasRecordCount(ReadOnlySpan<byte> statistics)
        {
            try
            {
                var reader = new Utf8JsonReader(statistics);
                if (!reader.Read() || reader.TokenType != JsonTokenType.StartObject)
                {
                    return false;
                }
                var found = false;
                while (reader.Read() && reader.TokenType == JsonTokenType.PropertyName)
                {
                    var isCount = reader.ValueTextEquals("numRecords"u8);
                    reader.Read();
                    if (isCount)
                    {
                        found = reader.TokenType == JsonTokenType.Number && reader.TryGetInt64(out var value) && value >= 0;
                    }
                    reader.Skip();
                }
                // One complete object and nothing after it
                return found && reader.TokenType == JsonTokenType.EndObject && !reader.Read();
            }
            catch (JsonException)
            {
                return false;
            }
            catch (InvalidOperationException)
            {
                // A name with a lone surrogate escape
                return false;
            }
        }

        public static bool HasRecordCount(string? statistics)
        {
            return statistics != null && HasRecordCount(Encoding.UTF8.GetBytes(statistics));
        }

        /// <summary>
        /// Parsed-only statistics of a root add: counts convert to JSON stats; min, max or null counts decline.
        /// </summary>
        public static string ParsedStatistics(long? numRecords, bool? tightBounds, bool hasMinValues, bool hasMaxValues, bool hasNullCount)
        {
            if (hasMinValues || hasMaxValues || hasNullCount)
            {
                throw new CheckpointDeclinedException(MaintenanceReason.ParsedOnlyRoot, DeclineDependency.Root, "a root add has parsed min, max or null count statistics but no JSON stats");
            }
            var builder = new StringBuilder("{");
            if (numRecords != null)
            {
                builder.Append("\"numRecords\":").Append(numRecords.Value.ToString(System.Globalization.CultureInfo.InvariantCulture));
            }
            if (tightBounds != null)
            {
                builder.Append(numRecords != null ? "," : "").Append("\"tightBounds\":").Append(tightBounds.Value ? "true" : "false");
            }
            return builder.Append('}').ToString();
        }

        /// <summary>
        /// Top-level checkpoint columns a root may have; any other column with a non-null value declines.
        /// </summary>
        public static readonly IReadOnlySet<string> KnownColumns = new HashSet<string>(StringComparer.Ordinal)
        {
            "add", "remove", "metaData", "protocol", "txn", "domainMetadata", "sidecar", "commitInfo", "cdc", "checkpointMetadata"
        };
    }
}
