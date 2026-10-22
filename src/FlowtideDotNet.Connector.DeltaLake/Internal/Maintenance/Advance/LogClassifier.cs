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

using FlowtideDotNet.Connector.DeltaLake.Internal.Delta;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.Deletion;
using Stowage;
using System.Globalization;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.Advance
{
    /// <summary>
    /// Classifies a log listing's entries by exact name grammar; it reads nothing and adds no multipart or V2 reading.
    /// </summary>
    internal static class LogClassifier
    {
        private const int VersionDigits = 20;
        private const int PartDigits = 10;
        private const string HintName = "_last_checkpoint";
        private const string SidecarFolder = "_sidecars";
        private const string Checkpoint = "checkpoint.";
        private const string Parquet = ".parquet";
        private const string Json = ".json";
        private const string Compacted = ".compacted.json";

        /// <summary>
        /// Every entry of a checked listing in its checked order.
        /// </summary>
        public static LogClassification Classify(LogEvidence log) => Classify(log.Entries.Entries);

        public static LogClassification Classify(IEnumerable<IOEntry> entries)
        {
            var names = new List<ClassifiedName>();
            var stages = new List<ClassifiedName>();
            var v2OrSidecar = false;
            var lowestUnknown = long.MaxValue;
            foreach (var entry in entries)
            {
                var named = Classify(entry);
                names.Add(named);
                switch (named.Kind)
                {
                    case LogNameKind.Stage:
                        stages.Add(named);
                        break;
                    case LogNameKind.UuidCheckpoint:
                        v2OrSidecar = true;
                        break;
                    case LogNameKind.ProtectedDirectory when FolderName(entry) == SidecarFolder:
                        v2OrSidecar = true;
                        break;
                    case LogNameKind.UnknownVersioned:
                        lowestUnknown = Math.Min(lowestUnknown, named.Version);
                        break;
                }
            }
            return new LogClassification() { Names = names, Stages = stages, V2OrSidecar = v2OrSidecar, LowestUnknownVersioned = lowestUnknown };
        }

        /// <summary>
        /// One entry's kind, version and end version; -1 for a name without a version.
        /// </summary>
        public static ClassifiedName Classify(IOEntry entry)
        {
            var name = entry.Name;
            if (entry.Path.IsFolder)
            {
                return FolderName(entry).StartsWith(DeltaTransactionReader.CleanupControlPrefix, StringComparison.Ordinal) ? Unversioned(entry, LogNameKind.Control) : Unversioned(entry, LogNameKind.ProtectedDirectory);
            }
            if (name.StartsWith(DeltaTransactionReader.CleanupControlPrefix, StringComparison.Ordinal))
            {
                var boundary = ControlName.TryParseBoundary(name, out var parsed) ? parsed : -1;
                return new ClassifiedName(entry, LogNameKind.Control, boundary, boundary);
            }
            if (ListingEvidence.TryGetStageVersion(name, out var staged))
            {
                return new ClassifiedName(entry, LogNameKind.Stage, staged, staged);
            }
            if (DeletionExecutor.IsTemporary(name))
            {
                return Unversioned(entry, LogNameKind.Temporary);
            }
            if (name == HintName)
            {
                return Unversioned(entry, LogNameKind.Hint);
            }
            if (name.StartsWith('.'))
            {
                return Unversioned(entry, LogNameKind.HiddenUnversioned);
            }
            if (!TryVersion(name, out var version))
            {
                return Unversioned(entry, LogNameKind.UnknownUnversioned);
            }
            // Past a long no reader takes it as a version
            if (version == long.MaxValue)
            {
                return new ClassifiedName(entry, LogNameKind.UnknownVersioned, version, version);
            }
            var rest = name.AsSpan(VersionDigits + 1);
            if (rest.SequenceEqual("json"))
            {
                return new ClassifiedName(entry, LogNameKind.Commit, version, version);
            }
            if (rest.SequenceEqual("crc"))
            {
                return new ClassifiedName(entry, LogNameKind.Crc, version, version);
            }
            if (rest.SequenceEqual("checkpoint.parquet"))
            {
                return new ClassifiedName(entry, LogNameKind.ClassicCheckpoint, version, version);
            }
            if (IsMultipart(rest))
            {
                return new ClassifiedName(entry, LogNameKind.MultipartCheckpoint, version, version);
            }
            if (IsUuid(rest))
            {
                return new ClassifiedName(entry, LogNameKind.UuidCheckpoint, version, version);
            }
            // {start}.{end}.compacted.json with end at or above start
            if (rest.Length == VersionDigits + Compacted.Length && rest.EndsWith(Compacted) && TryDigits(rest.Slice(0, VersionDigits), out var end) && end >= version && end != long.MaxValue)
            {
                return new ClassifiedName(entry, LogNameKind.Compaction, version, end);
            }
            return new ClassifiedName(entry, LogNameKind.UnknownVersioned, version, version);
        }

        private static ClassifiedName Unversioned(IOEntry entry, LogNameKind kind) => new ClassifiedName(entry, kind, -1, -1);

        private static string FolderName(IOEntry entry) => entry.Name.TrimEnd(IOPath.PathSeparator);

        // {version:D20}. followed by more; a version past a long saturates
        private static bool TryVersion(string name, out long version)
        {
            version = -1;
            if (name.Length <= VersionDigits + 1 || name[VersionDigits] != '.')
            {
                return false;
            }
            return TryDigits(name.AsSpan(0, VersionDigits), out version);
        }

        // ASCII digits only; a value past a long saturates
        private static bool TryDigits(ReadOnlySpan<char> digits, out long value)
        {
            value = -1;
            foreach (var c in digits)
            {
                if (!char.IsAsciiDigit(c))
                {
                    return false;
                }
            }
            value = long.TryParse(digits, NumberStyles.None, CultureInfo.InvariantCulture, out var parsed) ? parsed : long.MaxValue;
            return true;
        }

        // checkpoint.{part:D10}.{parts:D10}.parquet with 1 <= part <= parts
        private static bool IsMultipart(ReadOnlySpan<char> rest)
        {
            if (rest.Length != Checkpoint.Length + PartDigits + 1 + PartDigits + Parquet.Length || !rest.StartsWith(Checkpoint) || !rest.EndsWith(Parquet)
                || rest[Checkpoint.Length + PartDigits] != '.')
            {
                return false;
            }
            return TryDigits(rest.Slice(Checkpoint.Length, PartDigits), out var part) && TryDigits(rest.Slice(Checkpoint.Length + PartDigits + 1, PartDigits), out var parts)
                && part >= 1 && part <= parts;
        }

        // checkpoint.{uuid}.json or checkpoint.{uuid}.parquet
        private static bool IsUuid(ReadOnlySpan<char> rest)
        {
            const int UuidLength = 36;
            if (!rest.StartsWith(Checkpoint) || rest.Length < Checkpoint.Length + UuidLength)
            {
                return false;
            }
            var suffix = rest.Slice(Checkpoint.Length + UuidLength);
            return (suffix.SequenceEqual(Json) || suffix.SequenceEqual(Parquet)) && Guid.TryParseExact(rest.Slice(Checkpoint.Length, UuidLength), "D", out _);
        }
    }
}
