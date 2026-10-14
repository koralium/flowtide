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

using System.Text;
using System.Text.Json;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Checkpoints
{
    /// <summary>
    /// History a checkpoint built on an unmarked root may be missing.
    /// </summary>
    internal sealed record CoverageGap(long AtOrBelow, IReadOnlyList<string> Families, long TsBound);

    /// <summary>
    /// The footer marker of our checkpoints, unmarked when unknown or malformed.
    /// </summary>
    internal sealed record CoverageMarker(long Version, long BuildNow, long TombstoneCutoff, CoverageGap? Inherited, bool Sorted)
    {
        public const int CurrentFormat = 1;

        // Parsing is bounded: a marker is a few hundred bytes
        public const int MaxLength = 4096;

        public const string Remove = "remove";
        public const string Txn = "txn";
        public const string Domain = "domainMetadata";

        // The slack added to a gap's timestamp bound
        public static readonly TimeSpan Slack = TimeSpan.FromHours(1);

        public string ToJson()
        {
            using var stream = new MemoryStream();
            using (var writer = new Utf8JsonWriter(stream))
            {
                writer.WriteStartObject();
                writer.WriteNumber("format", CurrentFormat);
                writer.WriteNumber("version", Version);
                writer.WriteNumber("buildNow", BuildNow);
                writer.WriteNumber("tombstoneCutoff", TombstoneCutoff);
                if (Inherited == null)
                {
                    writer.WriteNull("inherited");
                }
                else
                {
                    writer.WriteStartObject("inherited");
                    writer.WriteNumber("atOrBelow", Inherited.AtOrBelow);
                    writer.WriteStartArray("families");
                    foreach (var family in Inherited.Families)
                    {
                        writer.WriteStringValue(family);
                    }
                    writer.WriteEndArray();
                    writer.WriteNumber("tsBound", Inherited.TsBound);
                    writer.WriteEndObject();
                }
                writer.WriteBoolean("sorted", Sorted);
                writer.WriteEndObject();
            }
            return Encoding.UTF8.GetString(stream.ToArray());
        }

        /// <summary>
        /// The marker, or null for unmarked: absent, too long, malformed, an unknown format or values that cannot be ours.
        /// </summary>
        public static CoverageMarker? Parse(string? value)
        {
            if (value == null || value.Length > MaxLength)
            {
                return null;
            }
            try
            {
                using var document = JsonDocument.Parse(value, new JsonDocumentOptions() { MaxDepth = 4 });
                var root = document.RootElement;
                if (root.ValueKind != JsonValueKind.Object
                    || !root.TryGetProperty("format", out var format) || format.ValueKind != JsonValueKind.Number || !format.TryGetInt32(out var formatValue) || formatValue != CurrentFormat
                    || !TryLong(root, "version", out var version) || version < 0
                    || !TryLong(root, "buildNow", out var buildNow)
                    || !TryLong(root, "tombstoneCutoff", out var cutoff)
                    || !root.TryGetProperty("sorted", out var sorted) || (sorted.ValueKind != JsonValueKind.True && sorted.ValueKind != JsonValueKind.False)
                    || !root.TryGetProperty("inherited", out var inherited))
                {
                    return null;
                }
                CoverageGap? gap = null;
                if (inherited.ValueKind != JsonValueKind.Null)
                {
                    if (inherited.ValueKind != JsonValueKind.Object
                        || !TryLong(inherited, "atOrBelow", out var atOrBelow) || atOrBelow < 0 || atOrBelow > version
                        || !TryLong(inherited, "tsBound", out var tsBound)
                        || !inherited.TryGetProperty("families", out var familiesElement) || familiesElement.ValueKind != JsonValueKind.Array)
                    {
                        return null;
                    }
                    var families = new List<string>();
                    foreach (var family in familiesElement.EnumerateArray())
                    {
                        var name = family.ValueKind == JsonValueKind.String ? family.GetString() : null;
                        if (name is not (Remove or Txn or Domain) || families.Contains(name))
                        {
                            return null;
                        }
                        families.Add(name);
                    }
                    gap = new CoverageGap(atOrBelow, families, tsBound);
                }
                return new CoverageMarker(version, buildNow, cutoff, gap, sorted.ValueKind == JsonValueKind.True);
            }
            catch (JsonException)
            {
                return null;
            }
            catch (InvalidOperationException)
            {
                // A string with a lone surrogate escape
                return null;
            }
        }

        private static bool TryLong(JsonElement element, string name, out long value)
        {
            value = 0;
            return element.TryGetProperty(name, out var property) && property.ValueKind == JsonValueKind.Number && property.TryGetInt64(out value);
        }

        /// <summary>
        /// The marker of a new checkpoint at a version.
        /// </summary>
        public static CoverageMarker Compute(long version, long buildNowMs, long cutoffMs, bool hasRoot, long rootVersion, CoverageMarker? rootMarker,
            IReadOnlySet<string> writerFeatures, long? firstTailCommitTimestamp, bool sorted)
        {
            var cutoff = rootMarker == null ? cutoffMs : Math.Max(cutoffMs, rootMarker.TombstoneCutoff);
            CoverageGap? gap = null;
            if (hasRoot && rootMarker != null)
            {
                gap = rootMarker.Inherited;
            }
            else if (hasRoot)
            {
                var families = writerFeatures.Contains("domainMetadata") ? new[] { Remove, Txn, Domain } : new[] { Remove, Txn };
                var start = firstTailCommitTimestamp ?? buildNowMs;
                var slack = (long)Slack.TotalMilliseconds;
                // Saturating: an overflowed bound is uncertainty, never completeness
                var tsBound = start > long.MaxValue - slack ? long.MaxValue : start + slack;
                gap = new CoverageGap(rootVersion, families, tsBound);
            }
            return new CoverageMarker(version, buildNowMs, cutoff, gap, sorted);
        }
    }
}
