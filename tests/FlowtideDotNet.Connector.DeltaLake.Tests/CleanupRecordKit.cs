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
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.Advance;
using Stowage;
using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    // A control record's exact name and complete bytes
    internal readonly record struct PlantedRecord(string Name, byte[] Bytes)
    {
        public byte[] Sha256 => SHA256.HashData(Bytes);
    }

    // An advance's boundary as its records carry it
    internal sealed record RecordFixture(string TableId, long C, long O, long A, long Head, long FirstJson)
    {
        public string AdvanceId { get; init; } = ControlTokens.New();

        public AnchorSummary Anchor { get; init; } = new AnchorSummary(Head, "stage-anchor", 512, CleanupRecordKit.Hash("anchor"), 512);

        public CheckpointSummary Destination { get; init; } = new CheckpointSummary(4096, CleanupRecordKit.Hash("checkpoint"), 4096, CleanupRecordKit.Cutoff.UtcTicks);

        public long CutoffTicks { get; init; } = CleanupRecordKit.Cutoff.UtcTicks;
    }

    internal enum NameField
    {
        Boundary,
        AdvanceId,
        // The part ordinal or the progress sequence
        Ordinal,
        Digest,
        AttemptId
    }

    /// <summary>
    /// Builds valid, torn and tampered control records and plants them in a table's log.
    /// </summary>
    internal static class CleanupRecordKit
    {
        public static readonly DateTimeOffset Cutoff = new DateTimeOffset(2026, 10, 8, 0, 0, 0, TimeSpan.Zero);

        private static readonly Comparer<byte[]> s_byteOrder = Comparer<byte[]>.Create(static (x, y) => x.AsSpan().SequenceCompareTo(y));

        // With a root at O unless the first JSON is given
        public static RecordFixture Fixture(string tableId = "table-a", long c = 8, long o = 4, long a = 0, long head = 10, long? firstJson = null) =>
            new RecordFixture(tableId, c, o, a, head, firstJson ?? o + 1);

        public static byte[] Hash(string text) => SHA256.HashData(Encoding.UTF8.GetBytes(text));

        public static IReadOnlyList<byte[]> Sorted(IEnumerable<string> paths) => paths.Select(Encoding.UTF8.GetBytes).OrderBy(x => x, s_byteOrder).ToList();

        // Sorted by UTF-8 bytes, under a fresh attempt id
        public static PlantedRecord Part(RecordFixture fixture, int ordinal, params string[] paths)
        {
            var sorted = Sorted(paths);
            var bytes = new byte[ControlRecords.PartLength(sorted.Sum(x => x.Length), sorted.Count)];
            ControlRecords.WritePart(bytes, fixture.C, fixture.AdvanceId, ordinal, sorted, out _);
            return new PlantedRecord(ControlName.Part(fixture.C, fixture.AdvanceId, ordinal, ControlTokens.New()), bytes);
        }

        // Parts are given in ordinal order
        public static PlantedRecord Header(RecordFixture fixture, params PlantedRecord[] parts) => Header(fixture, parts.Select(Describe).ToList());

        public static PlantedRecord Header(RecordFixture fixture, IReadOnlyList<PartDescriptor> descriptors)
        {
            var header = HeaderOf(fixture, descriptors);
            var bytes = new byte[ControlRecords.HeaderLength(header, descriptors.Count)];
            ControlRecords.WriteHeader(bytes, header, descriptors, out _);
            return new PlantedRecord(ControlName.Header(fixture.C, fixture.AdvanceId, ControlTokens.New()), bytes);
        }

        public static HeaderRecord HeaderOf(RecordFixture fixture, IReadOnlyList<PartDescriptor> descriptors)
        {
            var emitted = descriptors.Sum(x => (long)x.Paths);
            return new HeaderRecord(fixture.C, fixture.AdvanceId, fixture.TableId, fixture.O, fixture.A, fixture.Head, fixture.FirstJson, fixture.CutoffTicks, fixture.Anchor,
                fixture.Destination, emitted + 3, 2, 1, new RecordTotals(emitted, descriptors.Count, descriptors.Sum(x => (long)x.Bytes)));
        }

        public static PartDescriptor Describe(PlantedRecord part)
        {
            Assert.True(ControlName.TryParse(part.Name, out var name));
            Assert.Equal(RecordStatus.Valid, ControlRecords.ValidatePart(part.Bytes, out var head));
            return new PartDescriptor((int)name.Ordinal, name.AttemptId, head.Count, part.Bytes.Length, part.Sha256);
        }

        public static Association AssociationOf(PlantedRecord header)
        {
            Assert.True(ControlName.TryParse(header.Name, out var name));
            return new Association(name.Boundary, name.AdvanceId, header.Name, header.Sha256);
        }

        public static RecordTotals TotalsOf(PlantedRecord header)
        {
            Assert.Equal(RecordStatus.Valid, ControlRecords.DecodeHeader(header.Bytes, out var decoded, null));
            return decoded!.Totals;
        }

        public static PlantedRecord Started(RecordFixture fixture, PlantedRecord header)
        {
            var association = AssociationOf(header);
            return Group(ControlKind.Started, association, ControlRecords.Encode(new StartedRecord(association, fixture.TableId, fixture.O, fixture.A, fixture.Head, fixture.FirstJson,
                fixture.Anchor, TotalsOf(header))));
        }

        public static PlantedRecord LogsComplete(RecordFixture fixture, PlantedRecord header)
        {
            var association = AssociationOf(header);
            return Group(ControlKind.LogsComplete, association, ControlRecords.Encode(new LogsCompleteRecord(association, fixture.TableId, fixture.O)));
        }

        public static PlantedRecord Progress(PlantedRecord header, long sequence, RecordCursor next = default, long processed = 0)
        {
            var association = AssociationOf(header);
            return Group(ControlKind.Progress, association, ControlRecords.Encode(new ProgressRecord(association, sequence, next, processed)), sequence);
        }

        // Past the last part with every emitted path processed unless given
        public static PlantedRecord Done(RecordFixture fixture, PlantedRecord header, RecordCursor? final = null, long? processed = null)
        {
            var association = AssociationOf(header);
            var totals = TotalsOf(header);
            return Group(ControlKind.Done, association, ControlRecords.Encode(new DoneRecord(association, fixture.TableId, fixture.O, fixture.A, fixture.Head, fixture.Anchor,
                final ?? new RecordCursor(totals.Parts, 0, 0), totals, processed ?? totals.Emitted)));
        }

        private static PlantedRecord Group(ControlKind kind, Association association, byte[] bytes, long sequence = -1) =>
            new PlantedRecord(ControlName.Group(kind, association.C, association.AdvanceId, association.HeaderSha256, ControlTokens.New(), sequence), bytes);

        public static async Task PlantAsync(IFileStorage storage, string table, string name, byte[] bytes)
        {
            await using var stream = await storage.OpenWrite($"/{table}/{DeltaTransactionReader.DeltaLogDirName}{name}");
            await stream.WriteAsync(bytes);
        }

        public static Task PlantAsync(IFileStorage storage, string table, PlantedRecord record) => PlantAsync(storage, table, record.Name, record.Bytes);

        public static void PlantLocal(string logDirectory, string name, byte[] bytes) => File.WriteAllBytes(Path.Combine(logDirectory, name), bytes);

        public static void PlantLocal(string logDirectory, PlantedRecord record) => PlantLocal(logDirectory, record.Name, record.Bytes);

        public static byte[] Truncate(byte[] bytes, int at) => bytes.AsSpan(0, at).ToArray();

        public static byte[] Flip(byte[] bytes, int index)
        {
            var copy = bytes.ToArray();
            copy[index] ^= 0xFF;
            return copy;
        }

        // Rewrites the body length, checksum and end marker around tampered bytes
        public static byte[] Reseal(byte[] bytes, uint? bodyLength = null)
        {
            var copy = bytes.ToArray();
            if (copy.Length >= ControlRecords.EnvelopeBytes)
            {
                var sealedAt = copy.Length - 36;
                BinaryPrimitives.WriteUInt32LittleEndian(copy.AsSpan(8), bodyLength ?? (uint)(copy.Length - ControlRecords.EnvelopeBytes));
                SHA256.HashData(copy.AsSpan(0, sealedAt), copy.AsSpan(sealedAt, 32));
                BinaryPrimitives.WriteUInt32LittleEndian(copy.AsSpan(copy.Length - 4), ControlRecords.EndMarker);
            }
            return copy;
        }

        // The value is the segment's text; an ordinal or sequence keeps its part or p prefix
        public static string Rename(string name, NameField field, string value)
        {
            Assert.True(ControlName.TryParse(name, out var parsed));
            var prefix = DeltaTransactionReader.CleanupControlPrefix;
            // C, advance id, then part, header or d16, the attempt id and a group's suffix
            var segments = name.Substring(prefix.Length).Split('.');
            var group = parsed.Kind is not (ControlKind.Part or ControlKind.Header);
            switch (field)
            {
                case NameField.Boundary:
                    segments[0] = value;
                    break;
                case NameField.AdvanceId:
                    segments[1] = value;
                    break;
                case NameField.Ordinal when parsed.Kind == ControlKind.Part:
                    segments[2] = ControlName.PartPrefix + value;
                    break;
                case NameField.Ordinal when parsed.Kind == ControlKind.Progress:
                    segments[4] = ControlName.ProgressPrefix + value;
                    break;
                case NameField.Digest when group:
                    segments[2] = value;
                    break;
                case NameField.AttemptId:
                    segments[3] = value;
                    break;
                default:
                    throw new ArgumentException($"A {parsed.Kind} name has no {field}");
            }
            return prefix + string.Join('.', segments);
        }
    }
}
