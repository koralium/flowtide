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
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Checkpoints;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.Advance;
using Microsoft.Extensions.Time.Testing;
using Stowage;
using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;
using static FlowtideDotNet.Connector.DeltaLake.Tests.AdvancePlannerKit;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public class CleanupRecordPublicationTests : IDisposable
    {
        private readonly AdvancePlannerKit _kit = new AdvancePlannerKit();
        private readonly List<AdvancePlan> _plans = new List<AdvancePlan>();

        public void Dispose()
        {
            foreach (var plan in _plans)
            {
                plan.Dispose();
            }
            _kit.Dispose();
        }

        private sealed record Planned(Table Table, AdvanceBoundary Boundary, AdvancePlan Plan, CleanupContext Context);

        // An advance from the marked root at 8 to C at 20 over a seeded history, planned into several parts across windows
        private async Task<Planned> PlanAsync(AdvanceWindowLimits? limits = null)
        {
            var table = _kit.Local();
            await WriteHistory(table, 41, 30);
            await _kit.Build(table, 8);
            await _kit.Build(table, 20);
            // The root at 8 stands in for its own commit: the first JSON is 9
            Truncate(table, 9);
            await RetainedReferenceOracle.CreateReferencedFilesAsync(table.Native);
            var log = await Log(table);
            var context = _kit.Context(true, log.Head);
            var proof = await CommitAnchor.ProveAsync(table.Configuration.Store.Table, table.Local, log, log.Head, context);
            Assert.Equal(AnchorCheck.Proven, proof.Check);
            Assert.Equal(AnchorCheck.Proven, CommitAnchor.TryCreate("table-a", proof.Proof!, null, out var anchor));
            var boundary = Boundary(log, 8, 20) with { Anchor = anchor, FirstJson = 9 };
            var plan = await AdvancePlanner.PlanAsync(table.Configuration, table.Local, table.Configuration.Store.Table, boundary, Classes(log), context,
                limits ?? new AdvanceWindowLimits() { WindowVersions = 4, PartPaths = 2 });
            _plans.Add(plan);
            Assert.True(plan.Status == AdvancePlanStatus.Planned, $"{plan.Reason}: {plan.Detail}");
            Assert.True(plan.Parts.Count > 2, $"{plan.Parts.Count} parts");
            return new Planned(table, boundary, plan, context);
        }

        // Reads the prefixes by hand: each little-endian u32 is the UTF-8 length of the path that follows it, up to the checksum
        private static void AssertPrefixes(byte[] part, IReadOnlyList<byte[]> sorted)
        {
            // The envelope head, C, the advance id, the ordinal and the count
            var position = 12 + 8 + ControlTokens.Length + 4 + 4;
            Assert.Equal(sorted.Count, BinaryPrimitives.ReadInt32LittleEndian(part.AsSpan(position - 4)));
            foreach (var path in sorted)
            {
                Assert.Equal((uint)path.Length, BinaryPrimitives.ReadUInt32LittleEndian(part.AsSpan(position)));
                Assert.Equal(path, part.AsSpan(position + 4, path.Length).ToArray());
                position += 4 + path.Length;
            }
            Assert.Equal(part.Length - 36, position);
        }

        private static List<string> Decoded(byte[] part)
        {
            Assert.Equal(RecordStatus.Valid, ControlRecords.ValidatePart(part, out _));
            var paths = new List<string>();
            foreach (var path in ControlRecords.Paths(part))
            {
                paths.Add(Encoding.UTF8.GetString(path));
            }
            return paths;
        }

        [Fact]
        public async Task PartUsesU32PathLengths()
        {
            // Multi-byte characters and the longest path a part holds
            var fixture = CleanupRecordKit.Fixture();
            var paths = new[] { "/t/a.parquet", "/t/é/ü.parquet", "/t/\U0001F600.parquet", "/t/" + new string('p', ControlRecords.MaxPathBytes - 3) };
            var part = CleanupRecordKit.Part(fixture, 0, paths);
            AssertPrefixes(part.Bytes, CleanupRecordKit.Sorted(paths));
            Assert.Equal(ControlRecords.MaxPathBytes, Decoded(part.Bytes).Max(x => Encoding.UTF8.GetByteCount(x)));

            // Every part the planner writes is a part record of its advance, numbered across windows
            var planned = await PlanAsync();
            for (int ordinal = 0; ordinal < planned.Plan.Parts.Count; ordinal++)
            {
                var written = planned.Plan.Parts[ordinal];
                var bytes = File.ReadAllBytes(written.ScratchPath);
                Assert.Equal(RecordStatus.Valid, ControlRecords.ValidatePart(bytes, out var head));
                Assert.Equal(new PartHead(planned.Boundary.C, planned.Boundary.AdvanceId, ordinal, (int)written.Paths), head);
                Assert.Equal(ordinal, written.Ordinal);
                AssertPrefixes(bytes, Decoded(bytes).Select(Encoding.UTF8.GetBytes).ToList());
            }
            Assert.Contains(planned.Plan.Parts, x => x.Window > 0 && x.Index == 0 && x.Ordinal > 0);
        }

        [Fact]
        public async Task LengthFramingHandlesEscapedControlCharacters()
        {
            // Control characters and quotes inside paths come back as themselves
            var fixture = CleanupRecordKit.Fixture();
            var paths = new[] { "/t/a\nb.parquet", "/t/a\rb.parquet", "/t/a\0b.parquet", "/t/\"q\".parquet", "/t/a\r\n", "/t/\n" };
            var part = CleanupRecordKit.Part(fixture, 0, paths);
            Assert.Equal(CleanupRecordKit.Sorted(paths).Select(Encoding.UTF8.GetString), Decoded(part.Bytes));
            AssertPrefixes(part.Bytes, CleanupRecordKit.Sorted(paths));

            // One byte past the longest path: the writer refuses it and the decoder rejects it sealed
            Assert.Throws<ArgumentOutOfRangeException>(() => ControlRecords.WritePart(new byte[ControlRecords.PartCap], fixture.C, fixture.AdvanceId, 0, new[] { new byte[ControlRecords.MaxPathBytes + 1] }, out _));
            var longest = CleanupRecordKit.Part(fixture, 0, new string('p', ControlRecords.MaxPathBytes)).Bytes;
            var prefixAt = 12 + 8 + ControlTokens.Length + 4 + 4;
            var longer = longest.Take(longest.Length - 36).Append((byte)'p').Concat(longest.TakeLast(36)).ToArray();
            BinaryPrimitives.WriteUInt32LittleEndian(longer.AsSpan(prefixAt), (uint)ControlRecords.MaxPathBytes + 1);
            Assert.Equal(RecordStatus.Valid, ControlRecords.ValidatePart(CleanupRecordKit.Reseal(longest), out _));
            Assert.Equal(RecordStatus.Torn, ControlRecords.ValidatePart(CleanupRecordKit.Reseal(longer), out _));

            // An escaped line feed or carriage return in a candidate is kept and never emitted, on local disk and on an object store
            foreach (var table in new[] { _kit.Local(), MemoryTable() })
            {
                await Commit(table, 0, 0, Create(), DeltaTestKit.Metadata(DeltaTestKit.UserSchema), Add("part-a.parquet"), Add("part-x%0Ay.parquet"), Add("part-x%0Dy.parquet"));
                await Commit(table, 1, 0, Remove("part-a.parquet", 1), Remove("part-x%0Ay.parquet", 1), Remove("part-x%0Dy.parquet", 1), Add("part-b.parquet"));
                await Commit(table, 2, 0, Add("part-c.parquet"));
                await _kit.Build(table, 2);
                var log = await Log(table);
                var plan = await AdvancePlanner.PlanAsync(table.Configuration, table.Local, table.Configuration.Store.Table, Boundary(log, 0, 2), Classes(log), _kit.Context(false, log.Head));
                _plans.Add(plan);
                Assert.True(plan.Status == AdvancePlanStatus.Planned, $"{plan.Reason}: {plan.Detail}");
                Assert.Equal(new[] { "PART-A.PARQUET" }, Emitted(plan, AdvanceWindowLimits.Default));
                Assert.True(plan.Kept >= 2, $"{plan.Kept} kept");
            }
        }

        // An eligible in-memory table at /t, named as an object store names objects
        private static Table MemoryTable()
        {
            var storage = OwnedHookFileStorage.Over(Files.Of.InternalMemory($"./records-{Guid.NewGuid():N}"));
            var configuration = CleanupConfiguration.Validate(storage, "t", true, CleanupOptIns.Off);
            Assert.True(configuration.Store.Eligible);
            return new Table() { Local = storage, Configuration = configuration, Native = "" };
        }

        [Fact]
        public void PathThatFitsNoPartDeclines()
        {
            var context = _kit.Context(false, 10);
            var advanceId = ControlTokens.New();
            // Parts that hold exactly one path of ten bytes
            using (var writer = new AdvancePartWriter(context.Scratch, context.Memory, new AdvanceWindowLimits() { PartBytes = ControlRecords.PartLength(10, 1) }, 8, advanceId))
            {
                writer.Add(new string('p', 10));
                writer.Add(new string('q', 10));
                writer.Flush();
                Assert.Equal(new[] { ControlRecords.PartLength(10, 1), ControlRecords.PartLength(10, 1) }, writer.Parts.Select(x => (int)x.Bytes));
                Assert.Equal(MaintenanceReason.EntriesOverCap, Assert.Throws<CheckpointDeclinedException>(() => writer.Add(new string('p', 11))).Reason);
                writer.DeleteParts();
            }
            // The longest path fits a part, one byte more fits none
            using (var writer = new AdvancePartWriter(context.Scratch, context.Memory, AdvanceWindowLimits.Default, 8, advanceId))
            {
                writer.Add(new string('p', ControlRecords.MaxPathBytes));
                Assert.Equal(MaintenanceReason.EntriesOverCap, Assert.Throws<CheckpointDeclinedException>(() => writer.Add(new string('q', ControlRecords.MaxPathBytes + 1))).Reason);
                writer.Flush();
                Assert.Equal(RecordStatus.Valid, ControlRecords.ValidatePart(File.ReadAllBytes(Assert.Single(writer.Parts).ScratchPath), out var head));
                Assert.Equal(new PartHead(8, advanceId, 0, 1), head);
                writer.DeleteParts();
            }
            Assert.Equal(0, context.Memory.Managed);
            Assert.Empty(context.Scratch.Files);
        }

        [Fact]
        public async Task LocalCheckpointPublicationStillReplacesATornCheckpoint()
        {
            var table = _kit.Local();
            await WriteHistory(table, 43, 10);
            var torn = table.Log($"{6:D20}.checkpoint.parquet");
            File.WriteAllText(torn, "torn");
            var context = _kit.Context(false, 10);

            // Renamed over the torn object at its canonical name
            BuildResult? result = null;
            var error = await Record.ExceptionAsync(async () => result = await CheckpointBuilder.BuildAsync(new CheckpointBuildContext()
            {
                Storage = table.Local,
                Table = "t",
                Scratch = context.Scratch,
                Memory = context.Memory,
                Clock = new FakeTimeProvider(Now)
            }, new BuildRequest() { Version = 6 }));

            Assert.Null(error);
            Assert.Equal(BuildStatus.Completed, result!.Status);
            Assert.True(new FileInfo(torn).Length > 4);
            Assert.Equal(6, (await DeltaTransactionReader.ReadTable(table.Local, "t"))!.StartCheckpointVersion);
            Assert.Empty(Directory.GetFiles(Path.Combine(table.Native, "_delta_log"), ".flowtide-*.checkpoint.tmp"));
        }

        private const string LogPath = "/t/_delta_log/";

        private static ControlName Parsed(string name)
        {
            Assert.True(ControlName.TryParse(name, out var parsed), name);
            return parsed;
        }

        private static HookFileStorage Memory() => new HookFileStorage(Files.Of.InternalMemory($"./publication-{Guid.NewGuid():N}"));

        // The names of the log objects opened for write, in order
        private static List<string> Written(HookFileStorage storage) =>
            storage.Requests.Where(x => x.StartsWith("OpenWrite ", StringComparison.Ordinal)).Select(x => x.Substring(x.LastIndexOf('/') + 1)).ToList();

        // A started record of the header's group, carrying the boundary the header holds
        private static StartedRecord StartedOf(Association association, HeaderRecord header) =>
            new StartedRecord(association, header.TableId, header.O, header.A, header.Head, header.FirstJson, header.Anchor, header.Totals);

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task RecordsAreWriteOnceWithFresh128BitNames(bool landed)
        {
            var planned = await PlanAsync();
            var faulting = new FaultingStorage();
            var storage = new HookFileStorage(faulting);
            // The first attempt of every record fails, after it landed or before
            var seen = new HashSet<(ControlKind, long)>();
            faulting.Fault = path => ControlName.TryParse(path.Name, out var name) && seen.Add((name.Kind, name.Ordinal)) ? new WriteFault(landed, Throws: true) : null;
            var publisher = new ControlPublisher(storage, "/t/", planned.Context);

            var header = await publisher.PublishManifestAsync(planned.Boundary, planned.Plan);
            var descriptors = new List<PartDescriptor>();
            Assert.Equal(RecordStatus.Valid, ControlRecords.DecodeHeader(await DeltaTestKit.ReadBytes(storage, LogPath + header.Name), out var decoded, descriptors));
            var association = new Association(planned.Boundary.C, planned.Boundary.AdvanceId, header.Name, header.Sha256);
            var started = await publisher.PublishAsync(ControlKind.Started, association, -1, () => ControlRecords.Encode(StartedOf(association, decoded!)));

            // Two attempts per record, each name opened for write once, under distinct canonical attempt ids
            var written = Written(storage);
            Assert.Equal(2 * (planned.Plan.Parts.Count + 2), written.Count);
            Assert.Equal(written.Count, written.Distinct().Count());
            var attempts = written.Select(x => Parsed(x).AttemptId).ToList();
            Assert.All(attempts, x => Assert.True(ControlTokens.IsCanonical(x), x));
            Assert.Equal(attempts.Count, attempts.Distinct().Count());
            // The second attempts count: the header names those parts and the started record that header
            var expected = descriptors.Select(x => ControlName.Part(planned.Boundary.C, planned.Boundary.AdvanceId, x.Ordinal, x.AttemptId)).Append(header.Name).Append(started.Name);
            Assert.Equal(expected, written.Where((_, i) => i % 2 == 1));
            // A first attempt that landed stays beside them as an equal copy under its own name
            var listed = (await storage.Ls(LogPath)).Select(x => x.Name).ToHashSet(StringComparer.Ordinal);
            for (int i = 0; i < written.Count; i += 2)
            {
                Assert.Equal(landed, listed.Contains(written[i]));
                Assert.Contains(written[i + 1], listed);
                if (landed)
                {
                    Assert.Equal(await DeltaTestKit.ReadBytes(storage, LogPath + written[i + 1]), await DeltaTestKit.ReadBytes(storage, LogPath + written[i]));
                }
            }

            // A record that never confirms declines after its attempts, each under a new name
            faulting.Fault = _ => new WriteFault(landed, Throws: true);
            storage.ClearRequests();
            var done = new DoneRecord(association, decoded!.TableId, decoded.O, decoded.A, decoded.Head, decoded.Anchor, new RecordCursor(decoded.Totals.Parts, 0, 0), decoded.Totals, decoded.Totals.Emitted);
            var declined = await Assert.ThrowsAsync<CheckpointDeclinedException>(() => publisher.PublishAsync(ControlKind.Done, association, -1, () => ControlRecords.Encode(done)));
            Assert.Equal(MaintenanceReason.PublishFailed, declined.Reason);
            var retried = Written(storage);
            Assert.Equal(ControlPublisher.MaxAttempts, retried.Count);
            Assert.Equal(retried.Count, retried.Distinct().Count());
            Assert.All(retried, x => Assert.Equal(ControlKind.Done, Parsed(x).Kind));
            Assert.Equal(0, planned.Context.Memory.Managed);
            Assert.Equal(planned.Plan.Parts.Select(x => x.ScratchPath).Order(), planned.Context.Scratch.Files.Order());
        }

        [Fact]
        public async Task ImmutableLocalControlPublicationNeverOverwrites()
        {
            var table = _kit.Local();
            var log = Path.Combine(table.Native, "_delta_log");
            Directory.CreateDirectory(log);
            var metered = MeteredFileStorage.Wrap(table.Local, new MaintenanceBuckets(TimeProvider.System), ListingPaging.LocalDisk);
            var context = _kit.Context(false, 10);
            var fixture = CleanupRecordKit.Fixture();
            var header = CleanupRecordKit.Header(fixture, CleanupRecordKit.Part(fixture, 0, "/t/a.parquet"));
            var association = CleanupRecordKit.AssociationOf(header);
            var bytes = CleanupRecordKit.Started(fixture, header).Bytes;
            string Name(char attempt) => ControlName.Group(ControlKind.Started, association.C, association.AdvanceId, association.HeaderSha256, new string(attempt, ControlTokens.Length));
            // Another writer takes the first name between the copy and the rename
            var taken = Path.Combine(log, Name('1'));
            var planted = Encoding.UTF8.GetBytes("another writer's object");
            var tokens = new Queue<string>(new[] { '1', '2', '3' }.Select(x => new string(x, ControlTokens.Length)));
            var points = new List<string>();
            string[]? tempsAtRetry = null;
            long reservedAtRetry = -1;
            var publisher = new ControlPublisher(metered, table.Configuration.Store.Table, context, tokens.Dequeue, faultForTests: point =>
            {
                points.Add(point);
                if (point == "rename" && points.Count == 2)
                {
                    File.WriteAllBytes(taken, planted);
                }
                if (point == "move" && points.Count == 3)
                {
                    tempsAtRetry = Directory.GetFiles(log, ".flowtide-*.checkpoint.tmp");
                    reservedAtRetry = context.Scratch.Ledger.Reserved;
                }
            });

            var published = await publisher.PublishAsync(ControlKind.Started, association, -1, () => bytes);

            // The rename refused the taken name, which keeps its bytes, and the retry took a new name
            Assert.Equal(new[] { "move", "rename", "move", "rename" }, points);
            Assert.Equal(planted, File.ReadAllBytes(taken));
            Assert.Equal(Name('2'), published.Name);
            Assert.Equal(bytes, File.ReadAllBytes(Path.Combine(log, published.Name)));
            // Before the retry the failed temp and its charge were gone
            Assert.Empty(tempsAtRetry!);
            Assert.Equal(2L * bytes.Length, reservedAtRetry);
            Assert.Empty(Directory.GetFiles(log, ".flowtide-*.checkpoint.tmp"));
            Assert.Equal(new[] { Name('1'), Name('2') }, Directory.GetFiles(log).Select(x => Path.GetFileName(x)).Order(StringComparer.Ordinal));
            Assert.Equal(0, context.Scratch.Ledger.Reserved);
            Assert.Empty(context.Scratch.Files);
            Assert.Equal(0, context.Memory.Managed);
        }

        [Theory]
        [InlineData("local")]
        [InlineData("memory")]
        public async Task HeaderlessStartedRetainsAssociation(string kind)
        {
            var planned = await PlanAsync();
            var boundary = planned.Boundary;
            var plan = planned.Plan;
            var buckets = new MaintenanceBuckets(TimeProvider.System);
            // The table's local disk, renamed into place, or an object store
            var storage = kind == "local" ? MeteredFileStorage.Wrap(planned.Table.Local, buckets, ListingPaging.LocalDisk) : MeteredFileStorage.Wrap(Memory(), buckets, ListingPaging.S3);
            var points = new List<string>();
            var publisher = new ControlPublisher(storage, planned.Table.Configuration.Store.Table, planned.Context, faultForTests: points.Add);

            var header = await publisher.PublishManifestAsync(boundary, plan);

            Assert.Equal(kind == "local" ? 2 * (plan.Parts.Count + 1) : 0, points.Count);
            // The header is the object its name and hash describe, naming each part as it was published
            var headerBytes = await DeltaTestKit.ReadBytes(storage, LogPath + header.Name);
            Assert.Equal(header.Length, headerBytes.LongLength);
            Assert.Equal(header.Sha256, SHA256.HashData(headerBytes));
            var descriptors = new List<PartDescriptor>();
            Assert.Equal(RecordStatus.Valid, ControlRecords.DecodeHeader(headerBytes, out var decoded, descriptors));
            Assert.Equal((boundary.C, boundary.AdvanceId, boundary.TableId, boundary.O, boundary.A, boundary.Head, boundary.FirstJson, boundary.Cutoff.UtcTicks),
                (decoded!.C, decoded.AdvanceId, decoded.TableId, decoded.O, decoded.A, decoded.Head, decoded.FirstJson, decoded.CutoffTicks));
            Assert.Equal((plan.Candidates, plan.Protected, plan.Kept), (decoded.Candidates, decoded.Protected, decoded.Kept));
            var proof = plan.Destination!.Proof!;
            Assert.Equal((proof.Length, proof.Listed.Size, proof.Listed.Modified?.UtcTicks ?? -1), (decoded.Destination.Length, decoded.Destination.ListedSize, decoded.Destination.ListedModifiedTicks));
            Assert.Equal(proof.Sha256, decoded.Destination.Sha256);
            Assert.Equal(plan.Parts.Count, descriptors.Count);
            for (int i = 0; i < descriptors.Count; i++)
            {
                var part = plan.Parts[i];
                Assert.Equal((part.Ordinal, (int)part.Paths, (int)part.Bytes), (descriptors[i].Ordinal, descriptors[i].Paths, descriptors[i].Bytes));
                Assert.Equal(part.Sha256, descriptors[i].Sha256);
                Assert.Equal(File.ReadAllBytes(part.ScratchPath), await DeltaTestKit.ReadBytes(storage, LogPath + ControlName.Part(boundary.C, boundary.AdvanceId, part.Ordinal, descriptors[i].AttemptId)));
            }
            var association = new Association(boundary.C, boundary.AdvanceId, header.Name, header.Sha256);
            var started = await publisher.PublishAsync(ControlKind.Started, association, -1, () => ControlRecords.Encode(StartedOf(association, decoded)));
            Assert.Equal(ControlName.D16(header.Sha256), Parsed(started.Name).Digest);

            // Without the header, the started record alone carries the association and the advance
            await storage.Rm(LogPath + header.Name);
            Assert.DoesNotContain(await storage.Ls(LogPath), x => x.Name == header.Name);
            var startedBytes = await DeltaTestKit.ReadBytes(storage, LogPath + started.Name);
            Assert.Equal(RecordStatus.Valid, ControlRecords.Decode(startedBytes, out StartedRecord? record));
            Assert.Equal(RecordStatus.Valid, ControlRecords.Matches(Parsed(started.Name), startedBytes));
            Assert.Equal(association, record!.Association);
            Assert.Equal(header.Sha256, record.Association.HeaderSha256);
            Assert.Equal((boundary.TableId, boundary.O, boundary.C, boundary.A, boundary.Head, 9L), (record.TableId, record.O, record.Association.C, record.A, record.Head, record.FirstJson));
            var anchor = boundary.Anchor!;
            Assert.Equal((anchor.Version, anchor.StageId, anchor.Proof.Length, anchor.Listed.Size), (record.Anchor.Version, record.Anchor.StageId, record.Anchor.Length, record.Anchor.ListedSize));
            Assert.Equal(anchor.Sha256, record.Anchor.Sha256);
            Assert.Equal(new RecordTotals(plan.Emitted, plan.Parts.Count, plan.Parts.Sum(x => x.Bytes)), record.Totals);
            Assert.Equal(0, planned.Context.Memory.Managed);
        }

        [Fact]
        public async Task TornOrTamperedRecordIsInvalid()
        {
            foreach (var kind in Enum.GetValues<ControlKind>())
            {
                var bytes = CleanupRecordsTests.Sample(kind).Bytes;
                Assert.Equal(RecordStatus.Valid, CleanupRecordsTests.Decode(kind, bytes));
                // Every cut, down to nothing
                for (int at = 0; at < bytes.Length; at++)
                {
                    Assert.Equal(RecordStatus.Torn, CleanupRecordsTests.Decode(kind, CleanupRecordKit.Truncate(bytes, at)));
                }
                // Every byte flipped
                for (int index = 0; index < bytes.Length; index++)
                {
                    Assert.Equal(RecordStatus.Torn, CleanupRecordsTests.Decode(kind, CleanupRecordKit.Flip(bytes, index)));
                }
                // Bytes after the end marker, another end marker or a second copy
                foreach (var trailing in new[] { new byte[1], new byte[4], BitConverter.GetBytes(ControlRecords.EndMarker), bytes })
                {
                    Assert.Equal(RecordStatus.Torn, CleanupRecordsTests.Decode(kind, bytes.Concat(trailing).ToArray()));
                }
                // One byte over the kind's cap, sealed or not
                var over = new byte[ControlRecords.CapOf(kind) + 1];
                bytes.CopyTo(over, 0);
                Assert.Equal(RecordStatus.OverCap, CleanupRecordsTests.Decode(kind, over));
                Assert.Equal(RecordStatus.OverCap, CleanupRecordsTests.Decode(kind, CleanupRecordKit.Reseal(over)));
            }

            var fixture = CleanupRecordKit.Fixture();
            var header = CleanupRecordKit.Header(fixture, CleanupRecordKit.Part(fixture, 0, "/t/a.parquet"));
            var association = CleanupRecordKit.AssociationOf(header);
            var started = CleanupRecordKit.Started(fixture, header).Bytes;
            // Valid, of the same length and agreeing with the name: only the hash tells it apart
            var other = CleanupRecordKit.Started(fixture with { TableId = "table-b" }, header).Bytes;
            Assert.Equal((started.Length, RecordStatus.Valid), (other.Length, ControlRecords.Decode(other, out StartedRecord? _)));
            // A write that lands cut, flipped or replaced without an error is read back and published again under a new name
            foreach (var alter in new Func<byte[], byte[]>[] { x => x[..^1], x => CleanupRecordKit.Flip(x, 30), _ => other })
            {
                var faulting = new FaultingStorage();
                var storage = new HookFileStorage(faulting);
                var faults = new Queue<WriteFault>(new[] { new WriteFault(Lands: true, Throws: false, alter) });
                faulting.Fault = _ => faults.TryDequeue(out var fault) ? fault : null;

                var published = await new ControlPublisher(storage, "/t/", _kit.Context(false, 10)).PublishAsync(ControlKind.Started, association, -1, () => started);

                var written = Written(storage);
                Assert.Equal(2, written.Count);
                Assert.Equal(written[1], published.Name);
                Assert.Equal(started, await DeltaTestKit.ReadBytes(storage, LogPath + written[1]));
                Assert.Equal(alter(started), await DeltaTestKit.ReadBytes(storage, LogPath + written[0]));
            }

            // A record its own decoder rejects is never taken as published
            var unreadable = ControlRecords.Encode(new StartedRecord(association, fixture.TableId, fixture.O, fixture.C + 1, fixture.Head, fixture.FirstJson, fixture.Anchor, CleanupRecordKit.TotalsOf(header)));
            Assert.Equal(RecordStatus.Torn, ControlRecords.Decode(unreadable, out StartedRecord? _));
            var memory = Memory();
            var refused = await Assert.ThrowsAsync<CheckpointDeclinedException>(() => new ControlPublisher(memory, "/t/", _kit.Context(false, 10)).PublishAsync(ControlKind.Started, association, -1, () => unreadable));
            Assert.Equal(MaintenanceReason.PublishFailed, refused.Reason);
            Assert.Equal(ControlPublisher.MaxAttempts, Written(memory).Distinct().Count());
        }

        [Fact]
        public async Task RecordNameBodyAssociationMustMatch()
        {
            foreach (var kind in Enum.GetValues<ControlKind>())
            {
                var record = CleanupRecordsTests.Lettered(kind);
                var name = Parsed(record.Name);
                RecordStatus Renamed(NameField field, string value) => ControlRecords.Matches(Parsed(CleanupRecordKit.Rename(record.Name, field, value)), record.Bytes);

                Assert.Equal(RecordStatus.Valid, ControlRecords.Matches(name, record.Bytes));
                // A sibling attempt's copy is the same record
                Assert.Equal(RecordStatus.Valid, Renamed(NameField.AttemptId, ControlTokens.New()));
                Assert.Equal(RecordStatus.Mismatch, Renamed(NameField.Boundary, $"{name.Boundary + 1:D20}"));
                Assert.Equal(RecordStatus.Mismatch, Renamed(NameField.AdvanceId, ControlTokens.New()));
                if (kind == ControlKind.Part)
                {
                    Assert.Equal(RecordStatus.Mismatch, Renamed(NameField.Ordinal, $"{name.Ordinal + 1:D6}"));
                }
                if (kind == ControlKind.Progress)
                {
                    Assert.Equal(RecordStatus.Mismatch, Renamed(NameField.Ordinal, $"{name.Ordinal + 1:D10}"));
                }
                if (name.Digest is { } digest)
                {
                    Assert.Equal(RecordStatus.Mismatch, Renamed(NameField.Digest, ControlName.D16(CleanupRecordKit.Hash("another header"))));
                    // An upper-case d16 names no header this version writes
                    Assert.Equal(RecordStatus.Mismatch, Renamed(NameField.Digest, digest.ToUpperInvariant()));
                }
                if (kind == ControlKind.Started)
                {
                    Assert.Equal(RecordStatus.Mismatch, ControlRecords.Matches(Parsed(record.Name[..^ControlName.StartedSuffix.Length] + ControlName.DoneSuffix), record.Bytes));
                }
            }

            // A body of another advance under this association's names is published under none of them
            var fixture = CleanupRecordKit.Fixture();
            var header = CleanupRecordKit.Header(fixture, CleanupRecordKit.Part(fixture, 0, "/t/a.parquet"));
            var foreignFixture = CleanupRecordKit.Fixture();
            var foreign = CleanupRecordKit.Started(foreignFixture, CleanupRecordKit.Header(foreignFixture, CleanupRecordKit.Part(foreignFixture, 0, "/t/a.parquet"))).Bytes;
            var storage = Memory();
            var declined = await Assert.ThrowsAsync<CheckpointDeclinedException>(() =>
                new ControlPublisher(storage, "/t/", _kit.Context(false, 10)).PublishAsync(ControlKind.Started, CleanupRecordKit.AssociationOf(header), -1, () => foreign));
            Assert.Equal(MaintenanceReason.PublishFailed, declined.Reason);
            var written = Written(storage);
            Assert.Equal(ControlPublisher.MaxAttempts, written.Count);
            Assert.Equal(written.Count, written.Distinct().Count());
            Assert.All(written, x => Assert.Equal(fixture.AdvanceId, Parsed(x).AdvanceId));
        }

        // How one write ends: its bytes, altered or not, land or never do, then its disposal fails or not
        private sealed record WriteFault(bool Lands, bool Throws, Func<byte[], byte[]>? Alter = null);

        // A store that commits on dispose, as a cloud store does, whose writes can be given a fault
        private sealed class FaultingStorage : ForwardingStorage, IFileStorage
        {
            public FaultingStorage() : base(new CommitOnDisposeStorage(Files.Of.InternalMemory($"./faulting-{Guid.NewGuid():N}")))
            {
            }

            // Asked once per write
            public Func<IOPath, WriteFault?>? Fault { get; set; }

            async Task<Stream> IFileStorage.OpenWrite(IOPath path, CancellationToken cancellationToken)
            {
                var fault = Fault?.Invoke(path);
                var inner = await Inner.OpenWrite(path, cancellationToken);
                return fault == null ? inner : new FaultStream(inner, fault);
            }
        }

        // Buffers what is written and ends as its fault says when disposed
        private sealed class FaultStream : MemoryStream
        {
            private readonly Stream _inner;
            private readonly WriteFault _fault;

            public FaultStream(Stream inner, WriteFault fault)
            {
                _inner = inner;
                _fault = fault;
            }

            public override async ValueTask DisposeAsync()
            {
                var written = ToArray();
                await base.DisposeAsync();
                if (_fault.Lands)
                {
                    await _inner.WriteAsync(_fault.Alter?.Invoke(written) ?? written);
                    await _inner.DisposeAsync();
                }
                if (_fault.Throws)
                {
                    throw new IOException(_fault.Lands ? "The write landed, then failed" : "The write failed before landing");
                }
            }
        }
    }
}
