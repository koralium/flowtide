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
using Stowage;
using System.Globalization;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance
{
    /// <summary>
    /// The kind of a history-retention control record, by its name.
    /// </summary>
    internal enum ControlKind : byte
    {
        Part,
        Header,
        Started,
        LogsComplete,
        Progress,
        Done
    }

    /// <summary>
    /// A parsed control record name: .flowtide-cleanup.{C20}.{advanceId} followed by part, header or group fields.
    /// </summary>
    internal readonly record struct ControlName(ControlKind Kind, long Boundary, string AdvanceId, string? Digest, string AttemptId, long Ordinal)
    {
        public const int BoundaryDigits = 20;
        public const int DigestLength = 16;
        public const int OrdinalDigits = 6;
        public const int SequenceDigits = 10;
        public const int MaxTokenLength = 64;

        public const string HeaderSegment = "header";
        public const string PartPrefix = "part";
        public const string StartedSuffix = "started";
        public const string LogsCompleteSuffix = "logs-complete";
        public const string ProgressPrefix = "p";
        public const string DoneSuffix = "done";

        /// <summary>
        /// Parses one of the six record forms exactly; anything else is a record this version cannot interpret.
        /// </summary>
        public static bool TryParse(string name, out ControlName parsed)
        {
            parsed = default;
            if (!TryParseBoundary(name, out var boundary) || name.Length == DeltaTransactionReader.CleanupControlPrefix.Length + BoundaryDigits)
            {
                return false;
            }
            var segments = name.Substring(DeltaTransactionReader.CleanupControlPrefix.Length + BoundaryDigits + 1).Split('.');
            if (segments.Length == 3 && IsToken(segments[0]) && IsToken(segments[2]))
            {
                // {advanceId}.part{ordinal6}.{attemptId} or {advanceId}.header.{attemptId}
                if (segments[1] == HeaderSegment)
                {
                    parsed = new ControlName(ControlKind.Header, boundary, segments[0], null, segments[2], -1);
                    return true;
                }
                if (TryDigits(segments[1], PartPrefix, OrdinalDigits, out var ordinal))
                {
                    parsed = new ControlName(ControlKind.Part, boundary, segments[0], null, segments[2], ordinal);
                    return true;
                }
                return false;
            }
            if (segments.Length == 4 && IsToken(segments[0]) && IsDigest(segments[1]) && IsToken(segments[2]))
            {
                // {advanceId}.{d16}.{attemptId}.{suffix}
                long sequence = -1;
                ControlKind? kind = segments[3] switch
                {
                    StartedSuffix => ControlKind.Started,
                    LogsCompleteSuffix => ControlKind.LogsComplete,
                    DoneSuffix => ControlKind.Done,
                    _ => TryDigits(segments[3], ProgressPrefix, SequenceDigits, out sequence) ? ControlKind.Progress : null
                };
                if (kind is { } known)
                {
                    parsed = new ControlName(known, boundary, segments[0], segments[1], segments[2], sequence);
                    return true;
                }
            }
            return false;
        }

        /// <summary>
        /// The boundary of any name that starts with the prefix and twenty digits ending the name or a segment.
        /// </summary>
        public static bool TryParseBoundary(string name, out long boundary)
        {
            boundary = -1;
            var prefix = DeltaTransactionReader.CleanupControlPrefix.Length;
            if (!name.StartsWith(DeltaTransactionReader.CleanupControlPrefix, StringComparison.Ordinal) || name.Length < prefix + BoundaryDigits
                || (name.Length > prefix + BoundaryDigits && name[prefix + BoundaryDigits] != '.'))
            {
                return false;
            }
            var digits = name.AsSpan(prefix, BoundaryDigits);
            foreach (var c in digits)
            {
                if (!char.IsAsciiDigit(c))
                {
                    return false;
                }
            }
            // Twenty digits can exceed a version
            return long.TryParse(digits, NumberStyles.None, CultureInfo.InvariantCulture, out boundary);
        }

        // An identifier segment: 1 to 64 of [A-Za-z0-9_-]
        private static bool IsToken(string segment)
        {
            if (segment.Length == 0 || segment.Length > MaxTokenLength)
            {
                return false;
            }
            foreach (var c in segment)
            {
                if (!char.IsAsciiLetterOrDigit(c) && c != '_' && c != '-')
                {
                    return false;
                }
            }
            return true;
        }

        private static bool IsDigest(string segment)
        {
            if (segment.Length != DigestLength)
            {
                return false;
            }
            foreach (var c in segment)
            {
                if (!char.IsAsciiHexDigit(c))
                {
                    return false;
                }
            }
            return true;
        }

        // A fixed prefix followed by exactly the given number of digits
        private static bool TryDigits(string segment, string prefix, int digits, out long value)
        {
            value = -1;
            if (segment.Length != prefix.Length + digits || !segment.StartsWith(prefix, StringComparison.Ordinal))
            {
                return false;
            }
            for (int i = prefix.Length; i < segment.Length; i++)
            {
                if (!char.IsAsciiDigit(segment[i]))
                {
                    return false;
                }
            }
            return long.TryParse(segment.AsSpan(prefix.Length), NumberStyles.None, CultureInfo.InvariantCulture, out value);
        }
    }

    /// <summary>
    /// What the control records of one log listing forbid this version to change; records are preserved, never interpreted further or scavenged.
    /// </summary>
    internal sealed class ControlFence
    {
        public const long NoReservation = -1;

        public static ControlFence None { get; } = new ControlFence(NoReservation, 0, 0);

        private ControlFence(long reservedFloor, int reservations, int uninterpreted)
        {
            ReservedFloor = reservedFloor;
            Reservations = reservations;
            Uninterpreted = uninterpreted;
        }

        /// <summary>
        /// No checkpoint is created, repaired, replaced or reclaimed at or below it, and the hint never moves below it; long.MaxValue for an unknown boundary.
        /// </summary>
        public long ReservedFloor { get; }

        // Started, logs-complete and progress records, each evidence that its group started
        public int Reservations { get; }

        // Records this version cannot interpret: unknown forms, unparseable names and folders
        public int Uninterpreted { get; }

        // Destructive maintenance stops
        public bool Fenced => Uninterpreted > 0;

        // No reservation and no uninterpreted record
        public bool Clear => ReservedFloor == NoReservation && Uninterpreted == 0;

        /// <summary>
        /// Derives the fence from the control entries of one listing; a done record releases nothing, and parts and headers carry no authority.
        /// </summary>
        public static ControlFence Of(IReadOnlyList<IOEntry> controls)
        {
            if (controls.Count == 0)
            {
                return None;
            }
            var floor = NoReservation;
            int reservations = 0, uninterpreted = 0;
            foreach (var entry in controls)
            {
                // A folder under a control name is never a record
                if (!entry.Path.IsFolder && ControlName.TryParse(entry.Name, out var name))
                {
                    if (name.Kind is ControlKind.Started or ControlKind.LogsComplete or ControlKind.Progress)
                    {
                        reservations++;
                        floor = Math.Max(floor, name.Boundary);
                    }
                    continue;
                }
                uninterpreted++;
                // An uninterpreted record reserves the boundary its name still shows, or every version
                floor = !entry.Path.IsFolder && ControlName.TryParseBoundary(entry.Name, out var boundary) ? Math.Max(floor, boundary) : long.MaxValue;
            }
            return reservations == 0 && uninterpreted == 0 ? None : new ControlFence(floor, reservations, uninterpreted);
        }

        public static ControlFence Of(DeltaTransactionReader.LogListing listing) => Of(listing.Controls);

        /// <summary>
        /// A new checkpoint where none is listed.
        /// </summary>
        public bool AllowsCreate(long version) => version > ReservedFloor;

        /// <summary>
        /// Rewriting or reclaiming a listed checkpoint.
        /// </summary>
        public bool AllowsReplace(long version) => !Fenced && version > ReservedFloor;

        /// <summary>
        /// Writing the hint at a version; it never moves below the floor.
        /// </summary>
        public bool AllowsHint(long version) => version >= ReservedFloor;

        /// <summary>
        /// Deleting payload, stages or temporaries.
        /// </summary>
        public bool AllowsDeletion => !Fenced;

        /// <summary>
        /// Rebuilding a torn checkpoint; on an ineligible store only with no fence at all and every commit from zero listed.
        /// </summary>
        public bool AllowsTornRepair(long version, bool eligible, bool historyFromZero)
        {
            return AllowsReplace(version) && (eligible || (Clear && historyFromZero));
        }

        // For logs: counts and the floor, never names
        public string Detail
        {
            get
            {
                var floor = ReservedFloor == long.MaxValue ? "every version, as a boundary is unknown" : ReservedFloor == NoReservation ? "no version" : $"versions up to {ReservedFloor}";
                return $"cleanup control records reserve {floor}: {Reservations} from started advances, {Uninterpreted} this version cannot interpret";
            }
        }
    }
}
