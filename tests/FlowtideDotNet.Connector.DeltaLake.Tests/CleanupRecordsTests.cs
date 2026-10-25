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

using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.Advance;
using Stowage;
using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public class CleanupRecordsTests
    {
        public static TheoryData<string> Kinds => new TheoryData<string>(Enum.GetNames<ControlKind>());

        // One record of the kind for a header with two parts
        internal static PlantedRecord Sample(ControlKind kind, RecordFixture? fixture = null)
        {
            fixture ??= CleanupRecordKit.Fixture();
            var parts = new[]
            {
                CleanupRecordKit.Part(fixture, 0, "/t/b.parquet", "/t/a.parquet"),
                CleanupRecordKit.Part(fixture, 1, "/t/c.parquet")
            };
            var header = CleanupRecordKit.Header(fixture, parts);
            return kind switch
            {
                ControlKind.Part => parts[1],
                ControlKind.Header => header,
                ControlKind.Started => CleanupRecordKit.Started(fixture, header),
                ControlKind.LogsComplete => CleanupRecordKit.LogsComplete(fixture, header),
                ControlKind.Progress => CleanupRecordKit.Progress(header, 3, new RecordCursor(1, 0, 60), 2),
                _ => CleanupRecordKit.Done(fixture, header)
            };
        }

        // A sample whose d16, when its name has one, has an upper-case form
        internal static PlantedRecord Lettered(ControlKind kind)
        {
            while (true)
            {
                var record = Sample(kind);
                if (Parsed(record.Name).Digest is not { } digest || digest.Any(char.IsAsciiLetter))
                {
                    return record;
                }
            }
        }

        // The status of the kind's decoder
        internal static RecordStatus Decode(ControlKind kind, byte[] bytes) => kind switch
        {
            ControlKind.Part => ControlRecords.ValidatePart(bytes, out _),
            ControlKind.Header => ControlRecords.DecodeHeader(bytes, out _, new List<PartDescriptor>()),
            ControlKind.Started => ControlRecords.Decode(bytes, out StartedRecord? _),
            ControlKind.LogsComplete => ControlRecords.Decode(bytes, out LogsCompleteRecord? _),
            ControlKind.Progress => ControlRecords.Decode(bytes, out ProgressRecord? _),
            _ => ControlRecords.Decode(bytes, out DoneRecord? _)
        };

        private static ControlName Parsed(string name)
        {
            Assert.True(ControlName.TryParse(name, out var parsed), name);
            return parsed;
        }

        [Fact]
        public void StartedRoundTrips()
        {
            var fixture = CleanupRecordKit.Fixture();
            var header = CleanupRecordKit.Header(fixture, CleanupRecordKit.Part(fixture, 0, "/t/a.parquet"));
            var started = CleanupRecordKit.Started(fixture, header);

            Assert.Equal(RecordStatus.Valid, ControlRecords.Decode(started.Bytes, out StartedRecord? decoded));
            Assert.Equal(CleanupRecordKit.AssociationOf(header), decoded!.Association);
            Assert.Equal((fixture.TableId, fixture.O, fixture.A, fixture.Head, fixture.FirstJson), (decoded.TableId, decoded.O, decoded.A, decoded.Head, decoded.FirstJson));
            Assert.Equal(CleanupRecordKit.TotalsOf(header), decoded.Totals);
            Assert.Equal((fixture.Anchor.Version, fixture.Anchor.StageId, fixture.Anchor.Length, fixture.Anchor.ListedSize),
                (decoded.Anchor.Version, decoded.Anchor.StageId, decoded.Anchor.Length, decoded.Anchor.ListedSize));
            Assert.Equal(fixture.Anchor.Sha256, decoded.Anchor.Sha256);
            Assert.Equal(RecordStatus.Valid, ControlRecords.Matches(Parsed(started.Name), started.Bytes));
        }

        [Fact]
        public void PartRoundTrips()
        {
            var fixture = CleanupRecordKit.Fixture();
            var paths = new[] { "/t/b.parquet", "/t/é.parquet", "/t/a.parquet" };
            var part = CleanupRecordKit.Part(fixture, 3, paths);

            Assert.Equal(RecordStatus.Valid, ControlRecords.ValidatePart(part.Bytes, out var head));
            Assert.Equal(new PartHead(fixture.C, fixture.AdvanceId, 3, 3), head);
            Assert.Equal(ControlRecords.PartLength(paths.Sum(Encoding.UTF8.GetByteCount), 3), part.Bytes.Length);
            var listed = new List<string>();
            foreach (var path in ControlRecords.Paths(part.Bytes))
            {
                listed.Add(Encoding.UTF8.GetString(path));
            }
            Assert.Equal(new[] { "/t/a.parquet", "/t/b.parquet", "/t/é.parquet" }, listed);
            Assert.Equal(RecordStatus.Valid, ControlRecords.Matches(Parsed(part.Name), part.Bytes));
        }

        // Decodes valid bytes of the kind and encodes what it decoded, with the written object's SHA-256
        private static (byte[] Bytes, byte[] Sha256) Reencode(ControlKind kind, byte[] bytes)
        {
            byte[] again;
            switch (kind)
            {
                case ControlKind.Part:
                    Assert.Equal(RecordStatus.Valid, ControlRecords.ValidatePart(bytes, out var head));
                    var paths = new List<byte[]>();
                    foreach (var path in ControlRecords.Paths(bytes))
                    {
                        paths.Add(path.ToArray());
                    }
                    Assert.Equal(head.Count, paths.Count);
                    again = new byte[ControlRecords.PartLength(paths.Sum(x => x.Length), paths.Count)];
                    Assert.Equal(again.Length, ControlRecords.WritePart(again, head.C, head.AdvanceId, head.Ordinal, paths, out var partSha));
                    return (again, partSha);
                case ControlKind.Header:
                    var descriptors = new List<PartDescriptor>();
                    Assert.Equal(RecordStatus.Valid, ControlRecords.DecodeHeader(bytes, out var header, descriptors));
                    again = new byte[ControlRecords.HeaderLength(header!, descriptors.Count)];
                    Assert.Equal(again.Length, ControlRecords.WriteHeader(again, header!, descriptors, out var headerSha));
                    return (again, headerSha);
                case ControlKind.Started:
                    Assert.Equal(RecordStatus.Valid, ControlRecords.Decode(bytes, out StartedRecord? started));
                    again = ControlRecords.Encode(started!);
                    break;
                case ControlKind.LogsComplete:
                    Assert.Equal(RecordStatus.Valid, ControlRecords.Decode(bytes, out LogsCompleteRecord? logs));
                    again = ControlRecords.Encode(logs!);
                    break;
                case ControlKind.Progress:
                    Assert.Equal(RecordStatus.Valid, ControlRecords.Decode(bytes, out ProgressRecord? progress));
                    again = ControlRecords.Encode(progress!);
                    break;
                default:
                    Assert.Equal(RecordStatus.Valid, ControlRecords.Decode(bytes, out DoneRecord? done));
                    again = ControlRecords.Encode(done!);
                    break;
            }
            return (again, SHA256.HashData(again));
        }

        [Theory]
        [MemberData(nameof(Kinds))]
        public void DecodedRecordReencodesToTheSameBytes(string name)
        {
            var kind = Enum.Parse<ControlKind>(name);
            var record = Sample(kind);

            var (again, sha) = Reencode(kind, record.Bytes);

            Assert.Equal(record.Bytes, again);
            Assert.Equal(record.Sha256, sha);
            Assert.Equal(RecordStatus.Valid, ControlRecords.Matches(Parsed(record.Name), record.Bytes));
        }

        [Fact]
        public void GroupRecordsCarryTheirFields()
        {
            var fixture = CleanupRecordKit.Fixture();
            var header = Sample(ControlKind.Header, fixture);
            var descriptors = new List<PartDescriptor>();

            Assert.Equal(RecordStatus.Valid, ControlRecords.DecodeHeader(header.Bytes, out var decoded, descriptors));
            Assert.Equal((fixture.C, fixture.AdvanceId, fixture.TableId, fixture.O, fixture.A, fixture.Head, fixture.FirstJson, fixture.CutoffTicks),
                (decoded!.C, decoded.AdvanceId, decoded.TableId, decoded.O, decoded.A, decoded.Head, decoded.FirstJson, decoded.CutoffTicks));
            Assert.Equal((fixture.Destination.Length, fixture.Destination.ListedSize, fixture.Destination.ListedModifiedTicks),
                (decoded.Destination.Length, decoded.Destination.ListedSize, decoded.Destination.ListedModifiedTicks));
            Assert.Equal(new RecordTotals(3, 2, descriptors.Sum(x => (long)x.Bytes)), decoded.Totals);
            Assert.Equal(new[] { (0, 2), (1, 1) }, descriptors.Select(x => (x.Ordinal, x.Paths)));
            Assert.All(descriptors, x => Assert.True(ControlTokens.IsCanonical(x.AttemptId)));
            Assert.Equal(RecordStatus.Valid, ControlRecords.Decode(Sample(ControlKind.LogsComplete, fixture).Bytes, out LogsCompleteRecord? logs));
            Assert.Equal((fixture.TableId, fixture.O), (logs!.TableId, logs.O));
            Assert.Equal(RecordStatus.Valid, ControlRecords.Decode(Sample(ControlKind.Progress, fixture).Bytes, out ProgressRecord? progress));
            Assert.Equal((3L, new RecordCursor(1, 0, 60), 2L), (progress!.Sequence, progress.Next, progress.Processed));
            Assert.Equal(RecordStatus.Valid, ControlRecords.Decode(Sample(ControlKind.Done, fixture).Bytes, out DoneRecord? done));
            Assert.Equal((fixture.TableId, fixture.O, fixture.A, fixture.Head, new RecordCursor(2, 0, 0), 3L), (done!.TableId, done.O, done.A, done.Head, done.Final, done.Processed));
            Assert.Equal(fixture.C, done.Association.C);
        }

        [Theory]
        [MemberData(nameof(Kinds))]
        public void DamagedRecordIsRejected(string name)
        {
            var kind = Enum.Parse<ControlKind>(name);
            var bytes = Sample(kind).Bytes;

            Assert.Equal(RecordStatus.Valid, Decode(kind, bytes));
            Assert.Equal(RecordStatus.Torn, Decode(kind, CleanupRecordKit.Truncate(bytes, bytes.Length - 1)));
            Assert.Equal(RecordStatus.Torn, Decode(kind, CleanupRecordKit.Truncate(bytes, 0)));
            Assert.Equal(RecordStatus.Torn, Decode(kind, bytes.Append((byte)0).ToArray()));
            // A body byte, the checksum and the end marker
            foreach (var index in new[] { 30, bytes.Length - 20, bytes.Length - 1 })
            {
                Assert.Equal(RecordStatus.Torn, Decode(kind, CleanupRecordKit.Flip(bytes, index)));
            }
            var over = new byte[ControlRecords.CapOf(kind) + 1];
            bytes.CopyTo(over, 0);
            Assert.Equal(RecordStatus.OverCap, Decode(kind, over));
            // An intact record of another kind
            var other = kind == ControlKind.Done ? ControlKind.Started : ControlKind.Done;
            Assert.Equal(RecordStatus.Mismatch, Decode(other, bytes));

            // Resealed: another magic, version or zero bytes, a body length that disagrees, and a body byte more
            Assert.Equal(RecordStatus.Valid, Decode(kind, CleanupRecordKit.Reseal(bytes)));
            foreach (var index in new[] { 0, 4, 6, 7 })
            {
                Assert.Equal(RecordStatus.Torn, Decode(kind, CleanupRecordKit.Reseal(CleanupRecordKit.Flip(bytes, index))));
            }
            Assert.Equal(RecordStatus.Torn, Decode(kind, CleanupRecordKit.Reseal(bytes, (uint)(bytes.Length - ControlRecords.EnvelopeBytes - 1))));
            var longer = bytes.Take(bytes.Length - 36).Append((byte)0).Concat(bytes.TakeLast(36)).ToArray();
            Assert.Equal(RecordStatus.Torn, Decode(kind, CleanupRecordKit.Reseal(longer)));
        }

        [Fact]
        public void ResealedBodiesThatDoNotAddUpAreTorn()
        {
            var fixture = CleanupRecordKit.Fixture();
            var header = Sample(ControlKind.Header, fixture);
            var association = CleanupRecordKit.AssociationOf(header);
            var totals = CleanupRecordKit.TotalsOf(header);
            StartedRecord Started(long o, long firstJson) => new StartedRecord(association, fixture.TableId, o, fixture.A, fixture.Head, firstJson, fixture.Anchor, totals);

            Assert.Equal(RecordStatus.Valid, ControlRecords.Decode(ControlRecords.Encode(Started(fixture.O, fixture.O)), out StartedRecord? _));
            // O at C, a first JSON two past O, A above C and a head below C
            Assert.Equal(RecordStatus.Torn, ControlRecords.Decode(ControlRecords.Encode(Started(fixture.C, fixture.C)), out StartedRecord? _));
            Assert.Equal(RecordStatus.Torn, ControlRecords.Decode(ControlRecords.Encode(Started(fixture.O, fixture.O + 2)), out StartedRecord? _));
            Assert.Equal(RecordStatus.Torn, ControlRecords.Decode(ControlRecords.Encode(Started(fixture.O, fixture.O) with { A = fixture.C + 1 }), out StartedRecord? _));
            Assert.Equal(RecordStatus.Torn, ControlRecords.Decode(ControlRecords.Encode(Started(fixture.O, fixture.O) with { Head = fixture.C - 1 }), out StartedRecord? _));
            // A header name at another C
            var foreign = association with { HeaderName = CleanupRecordKit.Rename(header.Name, NameField.Boundary, $"{fixture.C + 1:D20}") };
            Assert.Equal(RecordStatus.Torn, ControlRecords.Decode(ControlRecords.Encode(new LogsCompleteRecord(foreign, fixture.TableId, fixture.O)), out LogsCompleteRecord? _));
            Assert.Equal(RecordStatus.Torn, ControlRecords.Decode(ControlRecords.Encode(new ProgressRecord(association, 0, new RecordCursor(0, -1, 0), 0)), out ProgressRecord? _));
            // An upper-case advance id in a part
            var upper = Sample(ControlKind.Part, fixture with { AdvanceId = "0123456789abcdef0123456789abcdef" }).Bytes;
            Assert.Equal(RecordStatus.Valid, ControlRecords.ValidatePart(CleanupRecordKit.Reseal(upper), out _));
            Encoding.ASCII.GetBytes("0123456789ABCDEF0123456789ABCDEF").CopyTo(upper, 20);
            Assert.Equal(RecordStatus.Torn, ControlRecords.ValidatePart(CleanupRecordKit.Reseal(upper), out _));

            // Descriptors that do not add up to the totals
            var descriptors = new List<PartDescriptor>();
            Assert.Equal(RecordStatus.Valid, ControlRecords.DecodeHeader(header.Bytes, out var decoded, descriptors));
            var miscounted = decoded! with { Totals = decoded.Totals with { Emitted = decoded.Totals.Emitted + 1 } };
            var bytes = new byte[ControlRecords.HeaderLength(miscounted, descriptors.Count)];
            ControlRecords.WriteHeader(bytes, miscounted, descriptors, out _);
            var appended = new List<PartDescriptor>();
            Assert.Equal(RecordStatus.Torn, ControlRecords.DecodeHeader(bytes, out _, appended));
            Assert.Empty(appended);

            // Unsorted, duplicate and invalid UTF-8 paths
            foreach (var paths in new[] { new[] { "b", "a" }, new[] { "a", "a" } }.Select(x => x.Select(Encoding.UTF8.GetBytes).ToList()).Append(new List<byte[]>() { new byte[] { 0xC3 } }))
            {
                var part = new byte[ControlRecords.PartLength(paths.Sum(x => x.Length), paths.Count)];
                ControlRecords.WritePart(part, fixture.C, fixture.AdvanceId, 0, paths, out _);
                Assert.Equal(RecordStatus.Torn, ControlRecords.ValidatePart(part, out _));
            }
        }

        [Fact]
        public void TokensAreExactlyThirtyTwoLowercaseHexCharacters()
        {
            const string canonical = "0123456789abcdef0123456789abcdef";

            Assert.True(ControlTokens.IsCanonical(ControlTokens.New()));
            Assert.True(ControlTokens.IsCanonical(canonical));
            Assert.False(ControlTokens.IsCanonical(canonical[..31]));
            Assert.False(ControlTokens.IsCanonical(canonical + "0"));
            Assert.False(ControlTokens.IsCanonical(canonical.ToUpperInvariant()));
        }

        [Fact]
        public void GroupRecordMustNameItsOwnAdvancesHeader()
        {
            var fixture = CleanupRecordKit.Fixture();
            var header = Sample(ControlKind.Header, fixture);
            var association = CleanupRecordKit.AssociationOf(header);
            RecordStatus Logs(string headerName) =>
                ControlRecords.Decode(ControlRecords.Encode(new LogsCompleteRecord(association with { HeaderName = headerName }, fixture.TableId, fixture.O)), out LogsCompleteRecord? _);

            Assert.Equal(RecordStatus.Valid, Logs(header.Name));
            Assert.Equal(RecordStatus.Valid, Logs(CleanupRecordKit.Rename(header.Name, NameField.AttemptId, ControlTokens.New())));
            // Another advance's header, a part or group name, and a short or upper-case attempt id
            Assert.Equal(RecordStatus.Torn, Logs(CleanupRecordKit.Rename(header.Name, NameField.AdvanceId, ControlTokens.New())));
            Assert.Equal(RecordStatus.Torn, Logs(ControlName.Part(fixture.C, fixture.AdvanceId, 0, ControlTokens.New())));
            Assert.Equal(RecordStatus.Torn, Logs(ControlName.Group(ControlKind.Started, fixture.C, fixture.AdvanceId, association.HeaderSha256, ControlTokens.New())));
            Assert.Equal(RecordStatus.Torn, Logs(CleanupRecordKit.Rename(header.Name, NameField.AttemptId, "abc")));
            Assert.Equal(RecordStatus.Torn, Logs(CleanupRecordKit.Rename(header.Name, NameField.AttemptId, "0123456789ABCDEF0123456789ABCDEF")));
            // An association at C zero on a record without a boundary of its own
            var zero = association with { C = 0, HeaderName = CleanupRecordKit.Rename(header.Name, NameField.Boundary, $"{0:D20}") };
            Assert.Equal(RecordStatus.Torn, Decode(ControlKind.Progress, ControlRecords.Encode(new ProgressRecord(zero, 0, default, 0))));
        }

        [Fact]
        public void FieldsOutsideTheirRangesAreTorn()
        {
            var fixture = CleanupRecordKit.Fixture();
            var header = Sample(ControlKind.Header, fixture);
            var association = CleanupRecordKit.AssociationOf(header);
            var totals = CleanupRecordKit.TotalsOf(header);
            var started = new StartedRecord(association, fixture.TableId, fixture.O, fixture.A, fixture.Head, fixture.FirstJson, fixture.Anchor, totals);
            var logs = new LogsCompleteRecord(association, fixture.TableId, fixture.O);
            var progress = new ProgressRecord(association, ControlName.MaxSequence, default, 0);
            var done = new DoneRecord(association, fixture.TableId, fixture.O, fixture.A, fixture.Head, fixture.Anchor, new RecordCursor(totals.Parts, 0, 0), totals, totals.Emitted);

            Assert.Equal(RecordStatus.Valid, Decode(ControlKind.Started, ControlRecords.Encode(started with { Totals = totals with { Parts = ControlRecords.MaxDescriptors } })));
            Assert.Equal(RecordStatus.Valid, Decode(ControlKind.LogsComplete, ControlRecords.Encode(logs)));
            Assert.Equal(RecordStatus.Valid, Decode(ControlKind.Progress, ControlRecords.Encode(progress)));
            Assert.Equal(RecordStatus.Valid, Decode(ControlKind.Done, ControlRecords.Encode(done)));
            // O or A below zero, more parts than a header holds, a negative emitted count and anchor version
            Assert.Equal(RecordStatus.Torn, Decode(ControlKind.Started, ControlRecords.Encode(started with { O = -1, FirstJson = 0 })));
            Assert.Equal(RecordStatus.Torn, Decode(ControlKind.Started, ControlRecords.Encode(started with { A = -1 })));
            Assert.Equal(RecordStatus.Torn, Decode(ControlKind.Started, ControlRecords.Encode(started with { Totals = totals with { Parts = ControlRecords.MaxDescriptors + 1 } })));
            Assert.Equal(RecordStatus.Torn, Decode(ControlKind.Started, ControlRecords.Encode(started with { Totals = totals with { Emitted = -1 } })));
            Assert.Equal(RecordStatus.Torn, Decode(ControlKind.Started, ControlRecords.Encode(started with { Anchor = fixture.Anchor with { Version = -1 } })));
            // O below zero or at C
            Assert.Equal(RecordStatus.Torn, Decode(ControlKind.LogsComplete, ControlRecords.Encode(logs with { O = -1 })));
            Assert.Equal(RecordStatus.Torn, Decode(ControlKind.LogsComplete, ControlRecords.Encode(logs with { O = fixture.C })));
            // A sequence above its name's digits or below zero, and a negative cursor part
            Assert.Equal(RecordStatus.Torn, Decode(ControlKind.Progress, ControlRecords.Encode(progress with { Sequence = ControlName.MaxSequence + 1 })));
            Assert.Equal(RecordStatus.Torn, Decode(ControlKind.Progress, ControlRecords.Encode(progress with { Sequence = -1 })));
            Assert.Equal(RecordStatus.Torn, Decode(ControlKind.Progress, ControlRecords.Encode(progress with { Next = new RecordCursor(-1, 0, 0) })));
            // A negative processed count and O at C
            Assert.Equal(RecordStatus.Torn, Decode(ControlKind.Done, ControlRecords.Encode(done with { Processed = -1 })));
            Assert.Equal(RecordStatus.Torn, Decode(ControlKind.Done, ControlRecords.Encode(done with { O = fixture.C })));
        }

        [Fact]
        public void DoneWhoseCursorOrCountDisagreesWithItsTotalsIsTorn()
        {
            var fixture = CleanupRecordKit.Fixture();
            var header = Sample(ControlKind.Header, fixture);
            var association = CleanupRecordKit.AssociationOf(header);
            var totals = CleanupRecordKit.TotalsOf(header);
            var done = new DoneRecord(association, fixture.TableId, fixture.O, fixture.A, fixture.Head, fixture.Anchor, new RecordCursor(totals.Parts, 0, 0), totals, totals.Emitted);
            Assert.True(totals.Parts > 0);
            Assert.Equal(RecordStatus.Valid, Decode(ControlKind.Done, ControlRecords.Encode(done)));
            // An advance that emitted nothing ends where it began
            Assert.Equal(RecordStatus.Valid, Decode(ControlKind.Done, ControlRecords.Encode(done with { Final = default, Totals = new RecordTotals(0, 0, 0), Processed = 0 })));

            // A final cursor short of, inside or past its last part, and fewer or more paths processed than emitted
            foreach (var final in new[] { new RecordCursor(0, 0, 0), new RecordCursor(totals.Parts, 1, 0), new RecordCursor(totals.Parts, 0, 1), new RecordCursor(totals.Parts + 1, 0, 0) })
            {
                Assert.Equal(RecordStatus.Torn, Decode(ControlKind.Done, ControlRecords.Encode(done with { Final = final })));
            }
            Assert.Equal(RecordStatus.Torn, Decode(ControlKind.Done, ControlRecords.Encode(done with { Processed = totals.Emitted - 1 })));
            Assert.Equal(RecordStatus.Torn, Decode(ControlKind.Done, ControlRecords.Encode(done with { Processed = totals.Emitted + 1 })));
        }

        [Fact]
        public void TotalsTheirPartsCannotHoldAreTorn()
        {
            var fixture = CleanupRecordKit.Fixture();
            var header = Sample(ControlKind.Header, fixture);
            var totals = CleanupRecordKit.TotalsOf(header);
            var started = new StartedRecord(CleanupRecordKit.AssociationOf(header), fixture.TableId, fixture.O, fixture.A, fixture.Head, fixture.FirstJson, fixture.Anchor, totals);
            RecordStatus Decoded(RecordTotals changed) => Decode(ControlKind.Started, ControlRecords.Encode(started with { Totals = changed }));

            Assert.Equal(RecordStatus.Valid, Decoded(new RecordTotals(0, 0, 0)));
            Assert.Equal(RecordStatus.Valid, Decoded(new RecordTotals(2L * ControlRecords.MaxPartPaths, 2, 2L * ControlRecords.PartCap)));
            // Paths or bytes without parts, parts without paths, and more paths or bytes than the parts hold
            Assert.Equal(RecordStatus.Torn, Decoded(new RecordTotals(1, 0, 0)));
            Assert.Equal(RecordStatus.Torn, Decoded(new RecordTotals(0, 0, 1)));
            Assert.Equal(RecordStatus.Torn, Decoded(totals with { Emitted = 0 }));
            Assert.Equal(RecordStatus.Torn, Decoded(new RecordTotals(2L * ControlRecords.MaxPartPaths + 1, 2, 2L * ControlRecords.PartCap)));
            Assert.Equal(RecordStatus.Torn, Decoded(new RecordTotals(2L * ControlRecords.MaxPartPaths, 2, 2L * ControlRecords.PartCap + 1)));
            Assert.Equal(RecordStatus.Torn, Decoded(new RecordTotals(1, 1, long.MaxValue)));
        }

        [Fact]
        public void HeaderOutsideItsBoundsIsTorn()
        {
            var fixture = CleanupRecordKit.Fixture();
            var descriptors = new List<PartDescriptor>();
            Assert.Equal(RecordStatus.Valid, ControlRecords.DecodeHeader(Sample(ControlKind.Header, fixture).Bytes, out var decoded, descriptors));
            var header = decoded!;
            RecordStatus Written(HeaderRecord record, IReadOnlyList<PartDescriptor> parts)
            {
                var bytes = new byte[ControlRecords.HeaderLength(record, parts.Count)];
                ControlRecords.WriteHeader(bytes, record, parts, out _);
                return ControlRecords.DecodeHeader(bytes, out _, null);
            }
            // The first descriptor changed, under totals that add up
            RecordStatus First(Func<PartDescriptor, PartDescriptor> change)
            {
                var parts = descriptors.Select((x, i) => i == 0 ? change(x) : x).ToList();
                return Written(header with { Totals = new RecordTotals(parts.Sum(x => (long)x.Paths), parts.Count, parts.Sum(x => (long)x.Bytes)) }, parts);
            }

            Assert.Equal(RecordStatus.Valid, Written(header, descriptors));
            Assert.Equal(RecordStatus.Valid, First(x => x with { Bytes = ControlRecords.PartCap }));
            Assert.Equal(RecordStatus.Valid, First(x => x with { Bytes = ControlRecords.PartLength(x.Paths, x.Paths) }));
            Assert.Equal(RecordStatus.Valid, First(x => x with { Paths = ControlRecords.MaxPartPaths, Bytes = ControlRecords.PartLength(ControlRecords.MaxPartPaths, ControlRecords.MaxPartPaths) }));
            // Parts out of order, bytes that do not add up, a part over its cap or under the smallest, and too many paths
            Assert.Equal(RecordStatus.Torn, Written(header, new[] { descriptors[0] with { Ordinal = 1 }, descriptors[1] with { Ordinal = 0 } }));
            Assert.Equal(RecordStatus.Torn, Written(header with { Totals = header.Totals with { PartBytes = header.Totals.PartBytes + 1 } }, descriptors));
            Assert.Equal(RecordStatus.Torn, First(x => x with { Bytes = ControlRecords.PartCap + 1 }));
            Assert.Equal(RecordStatus.Torn, First(x => x with { Bytes = ControlRecords.PartLength(x.Paths, x.Paths) - 1 }));
            Assert.Equal(RecordStatus.Torn, First(x => x with { Paths = ControlRecords.MaxPartPaths + 1, Bytes = ControlRecords.PartLength(ControlRecords.MaxPartPaths + 1, ControlRecords.MaxPartPaths + 1) }));
            // Negative candidates and a head below C
            Assert.Equal(RecordStatus.Torn, Written(header with { Candidates = -1 }, descriptors));
            Assert.Equal(RecordStatus.Torn, Written(header with { Head = fixture.C - 1 }, descriptors));
        }

        [Fact]
        public void PartOutsideItsBoundsIsTorn()
        {
            var fixture = CleanupRecordKit.Fixture();
            var path = Encoding.UTF8.GetBytes("/t/a.parquet");
            var many = Enumerable.Range(0, ControlRecords.MaxPartPaths + 1).Select(x => Encoding.ASCII.GetBytes(x.ToString("D5"))).ToArray();
            static RecordStatus Validated(byte[] bytes) => ControlRecords.ValidatePart(bytes, out _);

            Assert.Equal(CleanupRecordKit.Part(fixture, 0, "/t/a.parquet").Bytes, SealedPart(fixture, 0, path));
            Assert.Equal(RecordStatus.Valid, Validated(SealedPart(fixture, ControlRecords.MaxDescriptors - 1, path)));
            Assert.Equal(RecordStatus.Valid, Validated(SealedPart(fixture, 0, many[..ControlRecords.MaxPartPaths])));
            Assert.Equal(RecordStatus.Valid, Validated(SealedPart(fixture, 0, Enumerable.Repeat((byte)'p', ControlRecords.MaxPathBytes).ToArray())));
            // C zero, an ordinal out of range, no paths or too many, and an empty or too long path
            Assert.Equal(RecordStatus.Torn, Validated(SealedPart(fixture with { C = 0 }, 0, path)));
            Assert.Equal(RecordStatus.Torn, Validated(SealedPart(fixture, -1, path)));
            Assert.Equal(RecordStatus.Torn, Validated(SealedPart(fixture, ControlRecords.MaxDescriptors, path)));
            Assert.Equal(RecordStatus.Torn, Validated(SealedPart(fixture, 0)));
            Assert.Equal(RecordStatus.Torn, Validated(SealedPart(fixture, 0, many)));
            Assert.Equal(RecordStatus.Torn, Validated(SealedPart(fixture, 0, Array.Empty<byte>())));
            Assert.Equal(RecordStatus.Torn, Validated(SealedPart(fixture, 0, Enumerable.Repeat((byte)'p', ControlRecords.MaxPathBytes + 1).ToArray())));
        }

        // A part sealed around fields its writer would refuse
        private static byte[] SealedPart(RecordFixture fixture, int ordinal, params byte[][] paths)
        {
            using var stream = new MemoryStream();
            using (var writer = new BinaryWriter(stream, Encoding.ASCII, leaveOpen: true))
            {
                writer.Write(new byte[12]);
                writer.Write(fixture.C);
                writer.Write(Encoding.ASCII.GetBytes(fixture.AdvanceId));
                writer.Write(ordinal);
                writer.Write(paths.Length);
                foreach (var path in paths)
                {
                    writer.Write((uint)path.Length);
                    writer.Write(path);
                }
                writer.Write(new byte[36]);
            }
            var bytes = stream.ToArray();
            BinaryPrimitives.WriteUInt32LittleEndian(bytes, ControlRecords.Magic);
            bytes[4] = ControlRecords.FormatVersion;
            bytes[5] = (byte)ControlKind.Part;
            return CleanupRecordKit.Reseal(bytes);
        }

        [Fact]
        public void RecordOverItsCapDeclines()
        {
            var fixture = CleanupRecordKit.Fixture(tableId: new string('t', ControlRecords.SmallCap));
            var header = Sample(ControlKind.Header);

            var declined = Assert.Throws<CheckpointDeclinedException>(() => CleanupRecordKit.Started(fixture, header));
            Assert.Equal(MaintenanceReason.RecordOverCap, declined.Reason);
            var headerRecord = CleanupRecordKit.HeaderOf(CleanupRecordKit.Fixture(tableId: new string('t', ControlRecords.HeaderCap)), Array.Empty<PartDescriptor>());
            Assert.Equal(MaintenanceReason.RecordOverCap, Assert.Throws<CheckpointDeclinedException>(() => ControlRecords.HeaderLength(headerRecord, 0)).Reason);

            // Thirty-one longest paths fit a part, a thirty-second does not, and a longer path is refused
            var longest = Enumerable.Range(0, 32).Select(x => Encoding.ASCII.GetBytes(x.ToString("D2").PadRight(ControlRecords.MaxPathBytes, 'p'))).ToList();
            var part = new byte[ControlRecords.PartLength(31 * ControlRecords.MaxPathBytes, 31)];
            ControlRecords.WritePart(part, fixture.C, CleanupRecordKit.Fixture().AdvanceId, 0, longest.Take(31).ToList(), out _);
            Assert.Equal(RecordStatus.Valid, ControlRecords.ValidatePart(part, out _));
            var oversized = new byte[ControlRecords.PartLength(32 * ControlRecords.MaxPathBytes, 32)];
            Assert.Equal(MaintenanceReason.RecordOverCap, Assert.Throws<CheckpointDeclinedException>(() => ControlRecords.WritePart(oversized, 8, ControlTokens.New(), 0, longest, out _)).Reason);
            Assert.Throws<ArgumentOutOfRangeException>(() => ControlRecords.WritePart(oversized, 8, ControlTokens.New(), 0, new[] { new byte[ControlRecords.MaxPathBytes + 1] }, out _));

            // A record of exactly its cap encodes and decodes, one byte more declines
            var association = CleanupRecordKit.AssociationOf(header);
            var padding = ControlRecords.SmallCap - ControlRecords.Encode(new LogsCompleteRecord(association, "", fixture.O)).Length;
            byte[]? exact = null;
            Assert.Null(Record.Exception(() => exact = ControlRecords.Encode(new LogsCompleteRecord(association, new string('t', padding), fixture.O))));
            Assert.Equal((ControlRecords.SmallCap, RecordStatus.Valid), (exact!.Length, ControlRecords.Decode(exact, out LogsCompleteRecord? _)));
            Assert.Throws<CheckpointDeclinedException>(() => ControlRecords.Encode(new LogsCompleteRecord(association, new string('t', padding + 1), fixture.O)));
        }

        [Fact]
        public void WritersRefuseFieldsTheirDecodersReject()
        {
            var fixture = CleanupRecordKit.Fixture();
            var header = Sample(ControlKind.Header, fixture);
            var association = CleanupRecordKit.AssociationOf(header);
            var target = new byte[ControlRecords.PartCap];
            var path = new[] { Encoding.UTF8.GetBytes("/t/a.parquet") };
            var many = Enumerable.Range(0, ControlRecords.MaxPartPaths + 1).Select(x => Encoding.ASCII.GetBytes(x.ToString("D5"))).ToList();
            void Part(int ordinal, IReadOnlyList<byte[]> paths) => ControlRecords.WritePart(target, fixture.C, fixture.AdvanceId, ordinal, paths, out _);
            void Logs(Association written) => ControlRecords.Encode(new LogsCompleteRecord(written, fixture.TableId, fixture.O));

            Part(ControlRecords.MaxDescriptors - 1, many.Take(ControlRecords.MaxPartPaths).ToList());
            // An ordinal out of range, no paths or too many, and an empty path
            Assert.Throws<ArgumentOutOfRangeException>(() => Part(-1, path));
            Assert.Throws<ArgumentOutOfRangeException>(() => Part(ControlRecords.MaxDescriptors, path));
            Assert.Throws<ArgumentOutOfRangeException>(() => Part(0, Array.Empty<byte[]>()));
            Assert.Throws<ArgumentOutOfRangeException>(() => Part(0, many));
            Assert.Throws<ArgumentOutOfRangeException>(() => Part(0, new[] { Array.Empty<byte>() }));
            // A short SHA-256 and an upper-case token
            Assert.Throws<ArgumentException>(() => Logs(association with { HeaderSha256 = new byte[31] }));
            Assert.Throws<ArgumentException>(() => Logs(association with { AdvanceId = "0123456789ABCDEF0123456789ABCDEF" }));

            Assert.Equal(RecordStatus.Valid, ControlRecords.DecodeHeader(header.Bytes, out var read, null));
            var decoded = read!;
            var most = decoded with { Totals = decoded.Totals with { Parts = ControlRecords.MaxDescriptors } };
            Assert.True(ControlRecords.HeaderLength(most, ControlRecords.MaxDescriptors) < ControlRecords.HeaderCap);
            // Fewer descriptors than parts, and more descriptors than a header holds
            Assert.Throws<ArgumentOutOfRangeException>(() => ControlRecords.HeaderLength(decoded, decoded.Totals.Parts - 1));
            Assert.Throws<ArgumentOutOfRangeException>(() => ControlRecords.HeaderLength(most with { Totals = most.Totals with { Parts = ControlRecords.MaxDescriptors + 1 } }, ControlRecords.MaxDescriptors + 1));
        }

        [Fact]
        public void NameMustAgreeWithItsBody()
        {
            var record = Lettered(ControlKind.Started);
            var name = record.Name;
            RecordStatus Matches(string renamed) => ControlRecords.Matches(Parsed(renamed), record.Bytes);

            Assert.Equal(RecordStatus.Valid, Matches(name));
            Assert.Equal(RecordStatus.Valid, Matches(CleanupRecordKit.Rename(name, NameField.AttemptId, ControlTokens.New())));
            Assert.Equal(RecordStatus.Mismatch, Matches(CleanupRecordKit.Rename(name, NameField.AttemptId, "A")));
            Assert.Equal(RecordStatus.Mismatch, Matches(CleanupRecordKit.Rename(name, NameField.Boundary, $"{9:D20}")));
            Assert.Equal(RecordStatus.Mismatch, Matches(CleanupRecordKit.Rename(name, NameField.AdvanceId, ControlTokens.New())));
            Assert.Equal(RecordStatus.Mismatch, Matches(CleanupRecordKit.Rename(name, NameField.Digest, Parsed(name).Digest!.ToUpperInvariant())));
            Assert.Equal(RecordStatus.Mismatch, Matches(name[..^"started".Length] + "done"));
            var progress = Sample(ControlKind.Progress);
            Assert.Equal(RecordStatus.Mismatch, ControlRecords.Matches(Parsed(CleanupRecordKit.Rename(progress.Name, NameField.Ordinal, $"{4:D10}")), progress.Bytes));
            var part = Sample(ControlKind.Part);
            Assert.Equal(RecordStatus.Mismatch, ControlRecords.Matches(Parsed(CleanupRecordKit.Rename(part.Name, NameField.Ordinal, $"{0:D6}")), part.Bytes));
        }

        [Fact]
        public void NamesFormatToTheirParsedForms()
        {
            var advance = ControlTokens.New();
            var attempt = ControlTokens.New();
            var sha = CleanupRecordKit.Hash("header");
            var d16 = Convert.ToHexString(sha, 0, 8).ToLowerInvariant();

            Assert.Equal((ControlKind.Part, 7L, advance, (string?)null, attempt, 12L), Fields(ControlName.Part(7, advance, 12, attempt)));
            Assert.Equal((ControlKind.Header, 7L, advance, (string?)null, attempt, -1L), Fields(ControlName.Header(7, advance, attempt)));
            foreach (var kind in new[] { ControlKind.Started, ControlKind.LogsComplete, ControlKind.Done })
            {
                Assert.Equal((kind, 7L, advance, (string?)d16, attempt, -1L), Fields(ControlName.Group(kind, 7, advance, sha, attempt)));
            }
            Assert.Equal((ControlKind.Progress, 7L, advance, (string?)d16, attempt, ControlName.MaxSequence), Fields(ControlName.Group(ControlKind.Progress, 7, advance, sha, attempt, ControlName.MaxSequence)));
            Assert.Throws<ArgumentOutOfRangeException>(() => ControlName.Group(ControlKind.Progress, 7, advance, sha, attempt));
            Assert.Throws<ArgumentOutOfRangeException>(() => ControlName.Group(ControlKind.Header, 7, advance, sha, attempt));
            // An ordinal or sequence past its digits and a negative C
            Assert.Throws<ArgumentOutOfRangeException>(() => ControlName.Part(7, advance, -1, attempt));
            Assert.Throws<ArgumentOutOfRangeException>(() => ControlName.Part(7, advance, ControlName.MaxOrdinal + 1, attempt));
            Assert.Throws<ArgumentOutOfRangeException>(() => ControlName.Group(ControlKind.Progress, 7, advance, sha, attempt, ControlName.MaxSequence + 1));
            Assert.Throws<ArgumentOutOfRangeException>(() => ControlName.Part(-1, advance, 0, attempt));
            Assert.Throws<ArgumentOutOfRangeException>(() => ControlName.Header(-1, advance, attempt));
            Assert.Throws<ArgumentOutOfRangeException>(() => ControlName.Group(ControlKind.Done, -1, advance, sha, attempt));
            Assert.True(ControlTokens.IsCanonical(advance));
            Assert.NotEqual(advance, attempt);

            static (ControlKind, long, string, string?, string, long) Fields(string name)
            {
                Assert.True(ControlName.TryParse(name, out var parsed), name);
                return (parsed.Kind, parsed.Boundary, parsed.AdvanceId, parsed.Digest, parsed.AttemptId, parsed.Ordinal);
            }
        }

        [Fact]
        public void DecodingArbitraryBytesNeverThrowsAndAcceptsOnlyWhatItWouldWrite()
        {
            var random = new Random(1253);
            var accepted = 0;
            foreach (var kind in Enum.GetValues<ControlKind>())
            {
                var record = Sample(kind);
                var name = Parsed(record.Name);
                for (int i = 0; i < 1000; i++)
                {
                    var bytes = record.Bytes.ToArray();
                    // Random bytes, a cut and a fresh envelope, each by chance
                    if (random.Next(2) == 0)
                    {
                        var length = random.Next(1, 9);
                        random.NextBytes(bytes.AsSpan(random.Next(bytes.Length - length + 1), length));
                    }
                    if (random.Next(3) == 0)
                    {
                        bytes = CleanupRecordKit.Truncate(bytes, random.Next(bytes.Length));
                    }
                    if (random.Next(2) == 0)
                    {
                        bytes = CleanupRecordKit.Reseal(bytes);
                    }
                    var status = Decode(kind, bytes);
                    ControlRecords.Matches(name, bytes);
                    foreach (var _ in ControlRecords.Paths(bytes))
                    {
                    }
                    if (status == RecordStatus.Valid && !bytes.AsSpan().SequenceEqual(record.Bytes))
                    {
                        accepted++;
                        Assert.Equal(bytes, Reencode(kind, bytes).Bytes);
                    }
                }
            }
            // Some tampered bodies still decode, and only to what the encoder writes
            Assert.True(accepted > 0);
        }

        [Fact]
        public async Task ReadTakesAtMostTheCapAndReleasesItsBuffer()
        {
            var storage = Files.Of.InternalMemory($"./records-{Guid.NewGuid():N}");
            var memory = new JobMemory();
            var record = Sample(ControlKind.Started);
            await CleanupRecordKit.PlantAsync(storage, "t", record);
            await CleanupRecordKit.PlantAsync(storage, "t", "large.started", new byte[ControlRecords.SmallCap + 1]);

            using (var read = await ControlRecords.ReadAsync(storage, $"/t/_delta_log/{record.Name}", ControlKind.Started, memory, default))
            {
                Assert.Equal((RecordStatus.Valid, record.Bytes.Length), (read.Status, (int)read.Length));
                Assert.Equal(record.Bytes, read.Bytes.ToArray());
                Assert.Equal(record.Sha256, read.Sha256);
            }
            Assert.Equal(0, memory.Managed);
            using (var over = await ControlRecords.ReadAsync(storage, "/t/_delta_log/large.started", ControlKind.Started, memory, default))
            {
                Assert.Equal(RecordStatus.OverCap, over.Status);
            }
            using (var missing = await ControlRecords.ReadAsync(storage, "/t/_delta_log/missing.started", ControlKind.Started, memory, default))
            {
                Assert.Equal(RecordStatus.Missing, missing.Status);
            }
            Assert.Equal(0, memory.Managed);
        }

        [Fact]
        public async Task ReadClosesItsStreamBeforeReturningAndFreesItsBufferOnFailure()
        {
            var memory = new JobMemory();
            var longer = new WatchedStream(new byte[ControlRecords.SmallCap + 100]);
            var failing = new WatchedStream(new byte[ControlRecords.SmallCap], failAt: 100);

            using (var over = await ControlRecords.ReadAsync(new StreamStorage(longer), "/t/_delta_log/x.started", ControlKind.Started, memory, default))
            {
                Assert.True(longer.Disposed);
                Assert.Equal((RecordStatus.OverCap, ControlRecords.SmallCap + 1L, ControlRecords.SmallCap + 1L), (over.Status, over.Length, longer.Handed));
            }
            await Assert.ThrowsAsync<IOException>(() => ControlRecords.ReadAsync(new StreamStorage(failing), "/t/_delta_log/x.started", ControlKind.Started, memory, default));
            Assert.True(failing.Disposed);
            Assert.Equal(0, memory.Managed);
        }

        // Hands out at most seven bytes per read, fails at an offset when given one and records its disposal
        private sealed class WatchedStream : MemoryStream
        {
            private readonly int _failAt;

            public WatchedStream(byte[] bytes, int failAt = -1) : base(bytes)
            {
                _failAt = failAt;
            }

            public bool Disposed { get; private set; }

            public long Handed { get; private set; }

            public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
            {
                if (_failAt >= 0 && Handed >= _failAt)
                {
                    throw new IOException("Read failed");
                }
                var read = await base.ReadAsync(buffer.Slice(0, Math.Min(buffer.Length, 7)), cancellationToken);
                Handed += read;
                return read;
            }

            protected override void Dispose(bool disposing)
            {
                Disposed = true;
                base.Dispose(disposing);
            }
        }

        // Opens the one given stream for every read
        private sealed class StreamStorage : ForwardingStorage, IFileStorage
        {
            private readonly Stream _stream;

            public StreamStorage(Stream stream) : base(Files.Of.InternalMemory($"./stream-{Guid.NewGuid():N}"))
            {
                _stream = stream;
            }

            Task<Stream?> IFileStorage.OpenRead(IOPath path, CancellationToken cancellationToken) => Task.FromResult<Stream?>(_stream);
        }
    }
}
