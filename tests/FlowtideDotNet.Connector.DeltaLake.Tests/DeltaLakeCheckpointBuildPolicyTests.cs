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

using Apache.Arrow.Types;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Actions;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Checkpoints;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance;
using ParquetSharp;
using Stowage;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public partial class DeltaLakeCheckpointBuildTests
    {
        // A checkpoint the build itself would not admit as a root: DELTA_BINARY_PACKED sizes
        internal static async Task OutOfEnvelopeRoot(IFileStorage storage, string table, long version)
        {
            var state = (await DeltaTransactionReader.ReadTable(storage, table, version))!;
            using var properties = DeltaTestKit.Properties(b => b.DisableDictionary().Encoding("add.size", ParquetSharp.Encoding.DeltaBinaryPacked));
            await LegacyCheckpointWriter.WriteCheckpoint(storage, table, state, properties);
        }

        // Writes records through our own writer as the classic checkpoint at a version
        private async Task OwnRoot(IFileStorage storage, string table, long version, IEnumerable<byte[]> records, string? marker = null, Compression compression = Compression.Zstd)
        {
            var path = Path.Combine(_directory, Guid.NewGuid().ToString("N") + ".parquet");
            using (var writer = new CheckpointFileWriter(path, new DiskLedger(), new JobMemory(), compression, marker))
            {
                foreach (var record in records)
                {
                    writer.Append(record);
                }
                writer.Close();
            }
            await using (var target = await storage.OpenWrite($"/{table}/_delta_log/{version:D20}.checkpoint.parquet"))
            {
                await target.WriteAsync(await File.ReadAllBytesAsync(path));
            }
            File.Delete(path);
        }

        private static IEnumerable<byte[]> Records(params DeltaAction[] actions) => actions.SelectMany(x => TestRecords.FromAction(x));

        [Fact]
        public async Task OutOfEnvelopeRootFallsBackToAnOlderRoot()
        {
            var storage = Memory(nameof(OutOfEnvelopeRootFallsBackToAnOlderRoot));
            await WriteHistory(storage, "t", 41, 12, 12);
            await LegacyRoot(storage, "t", 5);
            await OutOfEnvelopeRoot(storage, "t", 8);
            var built = await Build(storage, "t", 11);

            Assert.Equal(BuildStatus.Completed, built.Result.Status);
            Assert.Equal(5, built.Result.RootVersion);
            Assert.Contains(built.Result.Skipped, x => x.StartsWith("checkpoint 8"));
        }

        [Fact]
        public async Task OutOfEnvelopeRootWithoutOlderRootDeclines()
        {
            var storage = Memory(nameof(OutOfEnvelopeRootWithoutOlderRootDeclines));
            await WriteHistory(storage, "t", 42, 12, 12);
            await OutOfEnvelopeRoot(storage, "t", 8);

            // Full history does not make an inadmissible root go away on its own
            var built = await Build(storage, "t", 11);
            Assert.Equal(BuildStatus.Declined, built.Result.Status);
            Assert.Equal(MaintenanceReason.NoAdmissibleRoot, built.Result.Reason);
            Assert.Equal(DeclineDependency.Root, built.Result.Dependency);

            // The option replays from 0 instead
            var rebuilt = await Build(storage, "t", 11, rebuildLegacy: true);
            Assert.Equal(BuildStatus.Completed, rebuilt.Result.Status);
            Assert.Null(rebuilt.Result.RootVersion);
            Assert.Equal(await CheckpointOracle.FullHistory(storage, "t", 11, s_now), rebuilt.Lines);
        }

        [Fact]
        public async Task KnownUnsupportedRootIsSkippedWhileItsIdentityHolds()
        {
            var storage = Memory(nameof(KnownUnsupportedRootIsSkippedWhileItsIdentityHolds));
            await WriteHistory(storage, "t", 43, 12, 12);
            await LegacyRoot(storage, "t", 5);
            await LegacyRoot(storage, "t", 8);
            var listing = await DeltaLogSegment.ListAsync(storage, "t");
            var eight = ObjectIdentity.Of("t", listing.Listing.Checkpoints.Single(x => x.Version == 8));

            var built = await Build(storage, "t", 11, unsupported: new HashSet<ObjectIdentity>() { eight });
            Assert.Equal(5, built.Result.RootVersion);
            // Another identity for the same version does not apply
            var stale = await Build(storage, "t", 11, unsupported: new HashSet<ObjectIdentity>() { eight with { Size = eight.Size + 1 } });
            Assert.Equal(8, stale.Result.RootVersion);
        }

        [Fact]
        public async Task UnknownRootCandidateFailsTheBuild()
        {
            var inner = Memory(nameof(UnknownRootCandidateFailsTheBuild));
            await WriteHistory(inner, "t", 44, 12, 12);
            await LegacyRoot(inner, "t", 5);
            await LegacyRoot(inner, "t", 8);
            // The newest candidate disappears between the listing and its read
            var storage = new HookFileStorage(inner)
            {
                Before = async (verb, path) =>
                {
                    if (verb == "OpenRead" && path.Full.EndsWith($"{8:D20}.checkpoint.parquet"))
                    {
                        await inner.Rm(path);
                    }
                }
            };
            var built = await Build(storage, "t", 11);

            Assert.Equal(BuildStatus.Failed, built.Result.Status);
            Assert.Equal(MaintenanceReason.UnknownCheckpoint, built.Result.Reason);
        }

        [Fact]
        public async Task RootDecodeFailureAfterACleanScanIsUnsupported()
        {
            var storage = Memory(nameof(RootDecodeFailureAfterACleanScanIsUnsupported));
            await DeltaTestKit.WriteCommit(storage, "t", 0, DeltaTestKit.Protocol(), DeltaTestKit.Metadata(DeltaTestKit.UserSchema), Add("decode-me.parquet"));
            await DeltaTestKit.WriteCommit(storage, "t", 1, Add("b.parquet"));
            await OwnRoot(storage, "t", 0, Records(DeltaTestKit.Protocol(), DeltaTestKit.Metadata(DeltaTestKit.UserSchema), Add("decode-me.parquet")), compression: Compression.Uncompressed);
            // A byte array length past its page, which only decoding sees
            var path = $"/t/_delta_log/{0:D20}.checkpoint.parquet";
            var bytes = await DeltaTestKit.ReadBytes(storage, path);
            var value = System.Text.Encoding.UTF8.GetBytes("decode-me.parquet");
            var at = bytes.AsSpan().IndexOf(value);
            BitConverter.GetBytes(0x7FFFFFF0).CopyTo(bytes, at - 4);
            await using (var stream = await storage.OpenWrite(path))
            {
                await stream.WriteAsync(bytes);
            }
            var listing = await DeltaLogSegment.ListAsync(storage, "t");
            var built = await Build(storage, "t", 1);

            Assert.Equal(BuildStatus.Failed, built.Result.Status);
            Assert.Equal(MaintenanceReason.RootDecodeFailure, built.Result.Reason);
            Assert.Equal(ObjectIdentity.Of("t", listing.Listing.Checkpoints.Single()), built.Result.RootIdentity);
        }

        [Fact]
        public async Task ParsedOnlyRootAddDeclinesEvenWhenSuperseded()
        {
            var storage = Memory(nameof(ParsedOnlyRootAddDeclinesEvenWhenSuperseded));
            var columns = new Dictionary<string, (string Name, IArrowType Type)[]>(DeltaLogReplayTests.CheckpointColumns)
            {
                ["add"] = new (string, IArrowType)[]
                {
                    ("path", StringType.Default), ("size", Int64Type.Default), ("modificationTime", Int64Type.Default), ("dataChange", BooleanType.Default), ("stats", StringType.Default),
                    ("stats_parsed", new StructType(new List<Apache.Arrow.Field>()
                    {
                        new Apache.Arrow.Field("numRecords", Int64Type.Default, true),
                        new Apache.Arrow.Field("minValues", new StructType(new List<Apache.Arrow.Field>() { new Apache.Arrow.Field("userkey", Int64Type.Default, true) }), true)
                    }))
                }
            };
            await DeltaTestKit.WriteCommit(storage, "t", 0, DeltaTestKit.Protocol(), DeltaTestKit.Metadata(DeltaTestKit.UserSchema), Add("parsed.parquet"));
            await DeltaLogReplayTests.WriteClassicCheckpoint(storage, "t", 0, columns,
                ("protocol", new object?[] { 1, 2 }),
                ("metaData", new object?[] { "table-id", DeltaTestKit.UserSchema, 0L }),
                ("add", new object?[] { "parsed.parquet", 1L, 0L, true, null, new object?[] { 5L, new object?[] { 1L } } }));
            // The add is removed in the tail, yet the root itself carries statistics the checkpoint cannot keep
            await DeltaTestKit.WriteCommit(storage, "t", 1, Remove("parsed.parquet", s_now.ToUnixTimeMilliseconds()));
            var built = await Build(storage, "t", 1);

            Assert.Equal(BuildStatus.Declined, built.Result.Status);
            Assert.Equal(MaintenanceReason.ParsedOnlyRoot, built.Result.Reason);
            Assert.Equal(DeclineDependency.Root, built.Result.Dependency);
        }

        [Fact]
        public async Task CountOnlyParsedStatisticsBecomeJson()
        {
            var storage = Memory(nameof(CountOnlyParsedStatisticsBecomeJson));
            await DeltaTestKit.WriteCommit(storage, "t", 0, DeltaTestKit.Protocol(), DeltaTestKit.Metadata(DeltaTestKit.UserSchema), Add("parsed.parquet"));
            await DeltaLogReplayTests.WriteClassicCheckpoint(storage, "t", 0, DeltaLogReplayTests.AddWithParsedStatistics,
                ("protocol", new object?[] { 1, 2 }),
                ("metaData", new object?[] { "table-id", DeltaTestKit.UserSchema, 0L }),
                ("add", new object?[] { "parsed.parquet", 1L, 0L, true, null, null, new object?[] { 42L, true } }));
            await DeltaTestKit.WriteCommit(storage, "t", 1, Add("b.parquet"));
            var built = await Build(storage, "t", 1);

            Assert.Equal(BuildStatus.Completed, built.Result.Status);
            var parsed = built.Records.Select(x => CheckpointRecord.ToAction(x).Add).Single(x => x?.Path == "parsed.parquet")!;
            Assert.Equal("{\"numRecords\":42,\"tightBounds\":true}", parsed.Statistics);
        }

        [Theory]
        [InlineData(TailReducer.DefaultArenaCap)]
        [InlineData(4096L)]
        public async Task SpecInvalidDomainRowDeclinesInBothModes(long arenaCap)
        {
            var storage = Memory(nameof(SpecInvalidDomainRowDeclinesInBothModes));
            await DeltaTestKit.WriteCommit(storage, "t", 0, DeltaTestKit.Protocol(), DeltaTestKit.Metadata(DeltaTestKit.UserSchema), Add("a.parquet"),
                new DeltaAction() { DomainMetadata = new DeltaDomainMetadataAction() { Domain = "d", Configuration = "c" } });
            var built = await Build(storage, "t", 0, arenaCap: arenaCap);

            Assert.Equal(BuildStatus.Declined, built.Result.Status);
            Assert.Equal(MaintenanceReason.DomainWithoutFeature, built.Result.Reason);
        }

        [Fact]
        public async Task DvCountIsCheckedOnEmittedAddsOnly()
        {
            var storage = Memory(nameof(DvCountIsCheckedOnEmittedAddsOnly));
            var counted = Add("a.parquet", Dv("x"));
            var uncounted = Add("b.parquet", Dv("y"));
            uncounted.Add!.Statistics = null;
            await DeltaTestKit.WriteCommit(storage, "t", 0, DeltaTestKit.ProtocolWith("deletionVectors"), DeltaTestKit.Metadata(DeltaTestKit.UserSchema), counted, uncounted);
            await DeltaTestKit.WriteCommit(storage, "t", 1, Remove("b.parquet", s_now.ToUnixTimeMilliseconds(), Dv("y")));

            // The uncounted add is superseded, so nothing emitted lacks a count
            Assert.Equal(BuildStatus.Completed, (await Build(storage, "t", 1)).Result.Status);
            var declined = await Build(storage, "t", 0);
            Assert.Equal(MaintenanceReason.DvWithoutRecordCount, declined.Result.Reason);
            Assert.Equal(DeclineDependency.None, declined.Result.Dependency);
        }

        [Fact]
        public async Task DeclineReportsItsDependencyClass()
        {
            var protocol = Memory(nameof(DeclineReportsItsDependencyClass));
            await DeltaTestKit.WriteCommit(protocol, "t", 0, DeltaTestKit.ProtocolWith("catalogManaged"), DeltaTestKit.Metadata(DeltaTestKit.UserSchema));
            var excluded = await Build(protocol, "t", 0);
            Assert.Equal((MaintenanceReason.ExcludedFeature, DeclineDependency.Protocol), (excluded.Result.Reason, excluded.Result.Dependency));

            var tail = Memory(nameof(DeclineReportsItsDependencyClass));
            var big = Add("big.parquet");
            big.Add!.Statistics = "{\"numRecords\":1,\"x\":\"" + new string('x', 5 << 20) + "\"}";
            await DeltaTestKit.WriteCommit(tail, "t", 0, DeltaTestKit.Protocol(), DeltaTestKit.Metadata(DeltaTestKit.UserSchema), big);
            var oversized = await Build(tail, "t", 0);
            Assert.Equal((MaintenanceReason.LineOverCap, DeclineDependency.Tail), (oversized.Result.Reason, oversized.Result.Dependency));
        }

        [Fact]
        public async Task OversizedCommitInfoIsSkippedAndTheBoundUsesTheNextTimestamp()
        {
            var storage = Memory(nameof(OversizedCommitInfoIsSkippedAndTheBoundUsesTheNextTimestamp));
            await DeltaTestKit.WriteCommit(storage, "t", 0, DeltaTestKit.Protocol(), DeltaTestKit.Metadata(DeltaTestKit.UserSchema), Add("a.parquet"));
            await LegacyRoot(storage, "t", 0);
            var big = new DeltaAction() { CommitInfo = new DeltaCommitInfoAction() { Data = new Dictionary<string, object>() { ["timestamp"] = 5L, ["x"] = new string('x', 5 << 20) } } };
            await DeltaTestKit.WriteCommit(storage, "t", 1, big, Add("b.parquet"));
            await DeltaTestKit.WriteCommit(storage, "t", 2, CommitInfo(7_000), Add("c.parquet"));
            var built = await Build(storage, "t", 2);

            Assert.Equal(BuildStatus.Completed, built.Result.Status);
            Assert.Equal(1, built.Result.SkippedLines);
            Assert.Equal(7_000 + (long)CoverageMarker.Slack.TotalMilliseconds, CoverageMarker.Parse(built.Marker)!.Inherited!.TsBound);
        }

        [Fact]
        public async Task OversizedHeaderKeepsConservativeBuilderCoverage()
        {
            var storage = Memory(nameof(OversizedHeaderKeepsConservativeBuilderCoverage));
            await DeltaTestKit.WriteCommit(storage, "t", 0, DeltaTestKit.Protocol(), DeltaTestKit.Metadata(DeltaTestKit.UserSchema), Add("a.parquet"));
            await LegacyRoot(storage, "t", 0);
            // An own header whose created files take it over the action cap
            var created = Enumerable.Range(0, 80_000).Select(i => $"part-00000-{i:D36}.zstd.parquet").ToList();
            var own = new DeltaAction() { CommitInfo = new DeltaCommitInfoAction() { StageId = new string('a', 32), AdoptedAt = 0, Timestamp = 5, Operation = "WRITE", CreatedFiles = created } };
            await DeltaTestKit.WriteCommit(storage, "t", 1, own, Add("b.parquet"));
            await DeltaTestKit.WriteCommit(storage, "t", 2, CommitInfo(7_000), Add("c.parquet"));
            var built = await Build(storage, "t", 2);

            // The skipped header's timestamp is unknown, so the bound falls back to the next one and never claims more
            Assert.Equal(BuildStatus.Completed, built.Result.Status);
            Assert.Equal(1, built.Result.SkippedLines);
            Assert.Equal(7_000 + (long)CoverageMarker.Slack.TotalMilliseconds, CoverageMarker.Parse(built.Marker)!.Inherited!.TsBound);
        }

        [Fact]
        public async Task WriterBugIsCaughtBeforePublication()
        {
            var storage = Memory(nameof(WriterBugIsCaughtBeforePublication));
            var configuration = new Dictionary<string, string>() { ["delta.parquet.compression.codec"] = "uncompressed" };
            await DeltaTestKit.WriteCommit(storage, "t", 0, DeltaTestKit.Protocol(), DeltaTestKit.Metadata(DeltaTestKit.UserSchema, configuration), Add("hashed-value.parquet"));
            // One value changed after it was hashed, the file staying valid Parquet with the same row count
            var built = await Build(storage, "t", 0, damage: path =>
            {
                var bytes = File.ReadAllBytes(path);
                var at = bytes.AsSpan().IndexOf(System.Text.Encoding.UTF8.GetBytes("hashed-value.parquet"));
                Assert.True(at > 0, "The value is stored uncompressed");
                bytes[at] = (byte)'H';
                File.WriteAllBytes(path, bytes);
            });

            Assert.Equal(BuildStatus.Failed, built.Result.Status);
            Assert.Equal(MaintenanceReason.ValidationFailed, built.Result.Reason);
        }

        [Fact]
        public async Task MergeWithActionCapRecordsStaysWithinJobMemory()
        {
            var storage = Memory(nameof(MergeWithActionCapRecordsStaysWithinJobMemory));
            var actions = new List<DeltaAction>() { DeltaTestKit.Protocol(), DeltaTestKit.Metadata(DeltaTestKit.UserSchema) };
            for (int i = 0; i < 20; i++)
            {
                var add = Add($"wide{i}.parquet");
                add.Add!.Statistics = "{\"numRecords\":1,\"x\":\"" + new string((char)('a' + i), 3 << 20) + "\"}";
                actions.Add(add);
            }
            for (int i = 0; i < actions.Count; i++)
            {
                await DeltaTestKit.WriteCommit(storage, "t", i, actions[i]);
            }
            var memory = new JobMemory();
            var built = await Build(storage, "t", actions.Count - 1, arenaCap: 4096, memory: memory);

            Assert.Equal(BuildStatus.Completed, built.Result.Status);
            Assert.Equal(BuildMode.Sort, built.Result.Mode);
            Assert.Equal(20, built.Result.Checkpoint!.AddFiles);
            Assert.True(memory.ManagedPeak <= memory.ManagedCap);
        }

        [Fact]
        public void RunCursorGrowsToTheEntryItHolds()
        {
            using var scratch = _owners.Add(new ScratchDirectory(Path.Combine(_directory, ScratchOwners.ShortId()))).CreateJob();
            var record = new byte[RunWriter.BufferSize + 40_000];
            var key = new byte[] { 4, 1, 2, 3 };
            var entry = new byte[ArenaEntry.EncodedLength(key.Length, record.Length)];
            var length = ArenaEntry.Encode(entry, key, 1, 0, record);
            string path;
            using (var writer = new RunWriter(scratch, new JobMemory()))
            {
                writer.Write(entry.AsSpan(0, length));
                writer.Close();
                path = writer.Path;
            }
            var memory = new JobMemory();
            using (var cursor = new RunCursor(path, memory))
            {
                Assert.True(cursor.MoveNext());
                Assert.Equal(record.Length, cursor.Current.Record.Length);
                // The grown buffer is charged whole, the old one released
                Assert.Equal(length, memory.Managed);
            }
            // The read buffer, then one growth to exactly the entry while the old buffer is still held
            Assert.Equal(RunWriter.BufferSize + length, memory.ManagedPeak);
            Assert.Equal(0, memory.Managed);
        }

        [Fact]
        public void RunCursorLongKeyStaysWithinReadBufferPlusEntry()
        {
            using var scratch = _owners.Add(new ScratchDirectory(Path.Combine(_directory, ScratchOwners.ShortId()))).CreateJob();
            // A key longer than the read buffer
            var record = Records(Add(new string('p', RunWriter.BufferSize + 40_000))).Single();
            var key = TestRecords.Key(record);
            var entry = new byte[ArenaEntry.EncodedLength(key.Length, record.Length)];
            var length = ArenaEntry.Encode(entry, key, 1, 0, record);
            string path;
            using (var writer = new RunWriter(scratch, new JobMemory()))
            {
                writer.Write(entry.AsSpan(0, length));
                writer.Close();
                path = writer.Path;
            }
            var memory = new JobMemory();
            using (var cursor = new RunCursor(path, memory))
            {
                Assert.True(cursor.MoveNext());
                Assert.Equal(record.Length, cursor.Current.Record.Length);
            }
            Assert.True(memory.ManagedPeak <= RunWriter.BufferSize + length, $"Peak {memory.ManagedPeak}, permitted {RunWriter.BufferSize + length}, entry {length}");
        }

        [Fact]
        public async Task SortModeBuffersInTheMapAloneWithinTheJobCap()
        {
            var storage = Memory(nameof(SortModeBuffersInTheMapAloneWithinTheJobCap));
            await DeltaTestKit.WriteCommit(storage, "t", 0, DeltaTestKit.Protocol(), DeltaTestKit.Metadata(DeltaTestKit.UserSchema));
            for (int version = 1; version <= 20; version++)
            {
                var adds = Enumerable.Range(0, 1000).Select(i => Add($"v{version}-{i}-{new string('p', 200)}.parquet")).ToArray();
                await DeltaTestKit.WriteCommit(storage, "t", version, adds);
            }
            // Sort mode buffers in the map alone; a second arena held beside it was measured at 11.0 MB
            const long arena = 2L << 20;
            var memory = new JobMemory(managedCap: 10L << 20);
            var built = await Build(storage, "t", 20, arenaCap: arena, memory: memory);

            Assert.True(built.Result.Status == BuildStatus.Completed, $"{built.Result.Reason}: {built.Result.Detail}");
            Assert.Equal(BuildMode.Sort, built.Result.Mode);
            Assert.Equal(20_000, built.Result.Checkpoint!.AddFiles);
        }

        [Fact]
        public void ArenaGrowthReservesOldAndNewIndex()
        {
            var memory = new JobMemory();
            var reserves = new List<(long Before, long Bytes)>();
            memory.BeforeReserveForTests = bytes => reserves.Add((memory.Managed, bytes));
            using (var reducer = new TailReducer(memory))
            {
                for (int i = 0; i < 13; i++)
                {
                    var record = TestRecords.FromAction(Add($"f{i}.parquet"))[0];
                    Assert.True(reducer.TryAdd(TestRecords.Key(record), 1, i, record));
                }
                // 13 keys pass three quarters of 16 slots: 32 slots are reserved while the 16 are still held
                Assert.Contains(reserves, x => x.Bytes == 32 * 16 && x.Before >= 16 * 16);
                Assert.Equal(13, reducer.Count);
            }
            Assert.Equal(0, memory.Managed);
        }

        [Fact]
        public void SpillKeepsTheIndexAndTheMergeHoldsNoReducer()
        {
            using var scratch = _owners.Add(new ScratchDirectory(Path.Combine(_directory, ScratchOwners.ShortId()))).CreateJob();
            var memory = new JobMemory();
            using var reducer = new TailReducer(memory);
            using var sorter = new CheckpointSorter(scratch, memory);
            void Fill(int from)
            {
                for (int i = from; i < from + 13; i++)
                {
                    var record = TestRecords.FromAction(Add($"f{i}.parquet"))[0];
                    Assert.True(reducer.TryAdd(TestRecords.Key(record), 1, i, record));
                }
            }

            Fill(0);
            sorter.Spill(reducer);
            // The next run reuses the grown index instead of growing it again
            Assert.Equal(0, reducer.Count);
            Assert.True(reducer.Bytes >= 32 * 16, $"{reducer.Bytes} bytes held after the spill");

            Fill(13);
            var entries = 0;
            sorter.Reduce(reducer, (in ArenaEntry entry) => entries++);
            Assert.Equal(26, entries);
            // The merge phase holds no reducer memory
            Assert.Equal(0, reducer.Bytes);
        }

        [Fact]
        public async Task MarkedRootPropagatesItsGapAndCutoff()
        {
            var storage = Memory(nameof(MarkedRootPropagatesItsGapAndCutoff));
            await WriteHistory(storage, "t", 51, 12, 12);
            await LegacyRoot(storage, "t", 3);
            // A checkpoint at 6 over the legacy root carries a gap at 3, and a later one over it copies that gap
            var six = await Build(storage, "t", 6);
            var sixMarker = CoverageMarker.Parse(six.Marker)!;
            Assert.Equal(3, sixMarker.Inherited!.AtOrBelow);
            await OwnRoot(storage, "t", 6, six.Records, six.Marker);
            var built = await Build(storage, "t", 11);

            Assert.Equal(6, built.Result.RootVersion);
            var marker = CoverageMarker.Parse(built.Marker)!;
            Assert.Equal(sixMarker.Inherited.AtOrBelow, marker.Inherited!.AtOrBelow);
            Assert.Equal(sixMarker.Inherited.TsBound, marker.Inherited.TsBound);
            Assert.Equal(sixMarker.Inherited.Families, marker.Inherited.Families);
            Assert.Equal(Math.Max(sixMarker.TombstoneCutoff, marker.TombstoneCutoff), marker.TombstoneCutoff);
        }

        [Fact]
        public async Task DuplicateRootKeysResolveLastRowWinsInSortMode()
        {
            var storage = Memory(nameof(DuplicateRootKeysResolveLastRowWinsInSortMode));
            await DeltaTestKit.WriteCommit(storage, "t", 0, DeltaTestKit.Protocol(), DeltaTestKit.Metadata(DeltaTestKit.UserSchema));
            await DeltaTestKit.WriteCommit(storage, "t", 1, Add("other.parquet"));
            var first = Add("same.parquet");
            first.Add!.Size = 1;
            var last = Add("same.parquet");
            last.Add!.Size = 2;
            await OwnRoot(storage, "t", 0, Records(DeltaTestKit.Protocol(), DeltaTestKit.Metadata(DeltaTestKit.UserSchema), first, last));
            var built = await Build(storage, "t", 1, arenaCap: 4096);

            // A root without a marker is always sorted
            Assert.Equal(BuildMode.Sort, built.Result.Mode);
            var same = Assert.Single(built.Records.Select(x => CheckpointRecord.ToAction(x).Add), x => x?.Path == "same.parquet");
            Assert.Equal(2, same!.Size);
        }

        [Fact]
        public async Task HandCheckedLostPreRootTxnsAndDomains()
        {
            var storage = Memory(nameof(HandCheckedLostPreRootTxnsAndDomains));
            await DeltaTestKit.WriteCommit(storage, "t", 0, DeltaTestKit.ProtocolWith("domainMetadata"), DeltaTestKit.Metadata(DeltaTestKit.UserSchema),
                new DeltaAction() { Txn = new DeltaTransactionAction() { AppId = "app", Version = 3 } },
                new DeltaAction() { DomainMetadata = new DeltaDomainMetadataAction() { Domain = "d", Configuration = "c" } });
            await DeltaTestKit.WriteCommit(storage, "t", 1, Add("a.parquet"));
            await LegacyRoot(storage, "t", 1);
            await DeltaTestKit.WriteCommit(storage, "t", 2, CommitInfo(9_000), Add("b.parquet"), new DeltaAction() { Txn = new DeltaTransactionAction() { AppId = "later", Version = 1 } });
            var built = await Build(storage, "t", 2);

            // The legacy root never held the txn or the domain from commit 0, so they are lost, and the marker says so
            var actions = built.Records.Select(x => CheckpointRecord.ToAction(x)).ToList();
            Assert.Equal(new[] { "later" }, actions.Where(x => x.Txn != null).Select(x => x.Txn!.AppId));
            Assert.DoesNotContain(actions, x => x.DomainMetadata != null);
            var gap = CoverageMarker.Parse(built.Marker)!.Inherited!;
            Assert.Equal(1, gap.AtOrBelow);
            Assert.Equal(new[] { CoverageMarker.Remove, CoverageMarker.Txn, CoverageMarker.Domain }, gap.Families);
            Assert.Equal(9_000 + (long)CoverageMarker.Slack.TotalMilliseconds, gap.TsBound);
        }
    }
}
