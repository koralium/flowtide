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
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Actions;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Checkpoints;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.DeletionVectors;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Utils;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance;
using ParquetSharp;
using Stowage;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public partial class DeltaMaintenanceWorkerTests
    {
        // A root add with parsed statistics and no JSON stats: every build over it declines, root-dependent
        private static Task ParsedOnlyRoot(IFileStorage storage, long version)
        {
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
            return DeltaLogReplayTests.WriteClassicCheckpoint(storage, "t", version, columns,
                ("protocol", new object?[] { 1, 2 }),
                ("metaData", new object?[] { "table-id", DeltaTestKit.UserSchema, 0L }),
                ("add", new object?[] { "parsed.parquet", 1L, 0L, true, null, new object?[] { 5L, new object?[] { 1L } } }));
        }

        // Our own checkpoint of every add up to the version, uncompressed so its strings can be found in the bytes
        private byte[] OwnRootBytes(long version)
        {
            var path = Path.Combine(_directory, Guid.NewGuid().ToString("N") + ".parquet");
            Directory.CreateDirectory(_directory);
            using (var writer = new CheckpointFileWriter(path, new DiskLedger(), new JobMemory(), Compression.Uncompressed, null))
            {
                foreach (var action in new[] { DeltaTestKit.Protocol(), DeltaTestKit.Metadata(DeltaTestKit.UserSchema) }.Concat(Enumerable.Range(0, (int)version + 1).Select(i => DeltaLakeCheckpointBuildTests.Add($"f{i}.parquet"))))
                {
                    foreach (var record in TestRecords.FromAction(action))
                    {
                        writer.Append(record);
                    }
                }
                writer.Close();
            }
            var bytes = File.ReadAllBytes(path);
            File.Delete(path);
            return bytes;
        }

        // The same bytes with a byte-array length past its page, which only decoding sees
        private static byte[] Undecodable(byte[] valid, long version)
        {
            var bytes = (byte[])valid.Clone();
            var value = System.Text.Encoding.UTF8.GetBytes($"f{version}.parquet");
            var at = bytes.AsSpan().IndexOf(value);
            Assert.True(at > 4);
            BitConverter.GetBytes(0x7FFFFFF0).CopyTo(bytes, at - 4);
            return bytes;
        }

        private static async Task WriteBytes(IFileStorage storage, string path, byte[] bytes)
        {
            await using var stream = await storage.OpenWrite(path);
            await stream.WriteAsync(bytes);
        }

        private static int RootReads(HookFileStorage hooks) => hooks.Requests.Count(x => x.StartsWith("OpenRead") && x.EndsWith(".checkpoint.parquet"));

        // A root-dependent decline at 5 over the parsed-only root at 3
        private async Task<Rig> DeclinedRig(bool stamp = false)
        {
            var rig = Create(interval: 2, stamp: stamp);
            await Commits(rig.Storage, 0, 5);
            await ParsedOnlyRoot(rig.Storage, 3);
            rig.Worker.Published(5);
            await rig.TickAsync();
            Assert.True(rig.Worker.Skipped);
            Assert.NotNull(rig.Worker.Memo);
            Assert.Equal(MaintenanceReason.ParsedOnlyRoot, rig.Worker.Memo!.Reason);
            return rig;
        }

        [Fact]
        public async Task RepeatedStableDeclineDownloadsNoRoot()
        {
            var rig = await DeclinedRig();
            for (int attempt = 0; attempt < 3; attempt++)
            {
                rig.PastRetry();
                rig.Hooks!.ClearRequests();
                await rig.TickAsync();
                Assert.Equal(0, RootReads(rig.Hooks));
                Assert.NotNull(rig.Worker.Memo);
            }
            Assert.Equal(1, rig.Logs.Warnings(nameof(MaintenanceReason.ParsedOnlyRoot)));
        }

        [Fact]
        public async Task NewProtocolActionClearsTheDeclineMemo()
        {
            var rig = Create(interval: 2);
            await DeltaTestKit.WriteCommit(rig.Storage, "t", 0, DeltaTestKit.ProtocolWith("someFutureFeature"), DeltaTestKit.Metadata(DeltaTestKit.UserSchema), DeltaLakeCheckpointBuildTests.Add("f0.parquet"));
            await Commits(rig.Storage, 1, 2);
            rig.Worker.Published(2);
            await rig.TickAsync();
            Assert.Equal(DeclineDependency.Protocol, rig.Worker.Memo!.Dependency);

            await DeltaTestKit.WriteCommit(rig.Storage, "t", 3, DeltaTestKit.ProtocolWith("deletionVectors"));
            rig.Worker.Published(3);
            rig.PastRetry();
            await rig.TickAsync();
            Assert.Null(rig.Worker.Memo);
            Assert.Equal(new long[] { 3 }, await Checkpoints(rig.Storage));
        }

        private async Task<Rig> ProtocolDeclinedRig(bool stamp)
        {
            var rig = Create(interval: 2, stamp: stamp);
            await DeltaTestKit.WriteCommit(rig.Storage, "t", 0, DeltaTestKit.ProtocolWith("someFutureFeature"), DeltaTestKit.Metadata(DeltaTestKit.UserSchema), DeltaLakeCheckpointBuildTests.Add("f0.parquet"));
            await Commits(rig.Storage, 1, 2);
            rig.Worker.Published(2);
            await rig.TickAsync();
            Assert.Equal(DeclineDependency.Protocol, rig.Worker.Memo!.Dependency);
            return rig;
        }

        [Fact]
        public async Task ProtocolMemoAdmitsCommitBeforeOpeningIt()
        {
            // Local disk lists sizes, which the admission charges
            var root = Path.Combine(_directory, "memo-transfer");
            Directory.CreateDirectory(root);
            var storage = new HookFileStorage(Files.Of.LocalDisk(root));
            var rig = Create(storage, interval: 2);
            await DeltaTestKit.WriteCommit(rig.Storage, "t", 0, DeltaTestKit.ProtocolWith("someFutureFeature"), DeltaTestKit.Metadata(DeltaTestKit.UserSchema), DeltaLakeCheckpointBuildTests.Add("f0.parquet"));
            await Commits(rig.Storage, 1, 2);
            rig.Worker.Published(2);
            await rig.TickAsync();
            Assert.Equal(DeclineDependency.Protocol, rig.Worker.Memo!.Dependency);

            await Commits(rig.Storage, 3, 3);
            long chargeAtOpen = -1;
            storage.Before = (verb, path) =>
            {
                if (verb == "OpenRead" && path.Full.EndsWith($"{3:D20}.json"))
                {
                    chargeAtOpen = rig.Budget.Transfer.Charged;
                }
                return Task.CompletedTask;
            };
            rig.Worker.Published(3);
            rig.PastRetry();
            await rig.TickAsync();

            Assert.NotNull(rig.Worker.Memo);
            Assert.Equal(new FileInfo(Path.Combine(root, "t", "_delta_log", $"{3:D20}.json")).Length, chargeAtOpen);
        }

        [Fact]
        public async Task ProtocolAfterAnotherTopLevelPropertyClearsMemo()
        {
            var rig = await ProtocolDeclinedRig(stamp: true);
            // One accepted line with commitInfo first and a supported protocol after it, which the encoder reads
            await rig.Storage.WriteText($"/t/_delta_log/{3:D20}.json", "{\"commitInfo\":{\"timestamp\":0},\"protocol\":{\"minReaderVersion\":1,\"minWriterVersion\":2}}\n");
            rig.Worker.Published(3);
            rig.PastRetry();
            await rig.TickAsync();
            Assert.Equal(new long[] { 3 }, await Checkpoints(rig.Storage));
        }

        [Fact]
        public async Task ReplacingProtocolDependencyClearsStrongMemo()
        {
            var rig = await ProtocolDeclinedRig(stamp: true);
            Assert.False(rig.Worker.Memo!.Weak);
            Assert.Equal(new long[] { 0 }, rig.Worker.Memo.ProtocolCommits.Select(x => x.Version));

            // Commit 0 repaired at the same version with a supported protocol: its identity changes
            await Commits(rig.Storage, 0, 0);
            rig.PastRetry();
            await rig.TickAsync();
            Assert.Equal(new long[] { 2 }, await Checkpoints(rig.Storage));
        }

        [Theory]
        [InlineData((int)DeclineDependency.Protocol)]
        [InlineData((int)DeclineDependency.Root)]
        [InlineData((int)DeclineDependency.Tail)]
        public async Task ProtocolMemoDoesNotHoldOverAMissingCommit(int dependency)
        {
            var storage = Files.Of.InternalMemory($"./memo-gap-{Guid.NewGuid():N}");
            await Commits(storage, 0, 20);
            // A decline at 10 on a table without checkpoints, so the candidate set is empty
            var tail = ObjectIdentity.Of("t", (await DeltaLogSegment.ListAsync(storage, "t")).Listing.Commits[10]);
            var declined = new BuildResult(BuildStatus.Declined, Reason: MaintenanceReason.UnsupportedFeature, Dependency: (DeclineDependency)dependency, Detail: "feature") { TailCommit = tail };
            var memo = DeclineMemo.For(declined, 10, s_now)!;

            // Missing history is never memoized, and the commit may come back with a change the memo would never read
            await storage.Rm($"/t/_delta_log/{15:D20}.json");
            Assert.Equal(MemoCheck.Changed, await memo.CheckAsync(storage, "t", 20, s_now, new JobMemory(), new TransferAllowance(), CancellationToken.None));
        }

        [Fact]
        public async Task NewCheckpointClearsTheDeclineMemo()
        {
            var rig = await DeclinedRig();
            await DeltaLakeCheckpointBuildTests.LegacyRoot(rig.Storage, "t", 4);
            rig.PastRetry();
            await rig.TickAsync();
            Assert.Null(rig.Worker.Memo);
            Assert.Contains(5L, await Checkpoints(rig.Storage));
        }

        [Fact]
        public async Task DvCountDeclineRecoversAfterTheFileIsRemoved()
        {
            var rig = Create(interval: 2);
            await DeltaTestKit.WriteCommit(rig.Storage, "t", 0, DeltaTestKit.ProtocolWith("deletionVectors"), DeltaTestKit.Metadata(DeltaTestKit.UserSchema), DeltaLakeCheckpointBuildTests.Add("f0.parquet"));
            var dv = new DeletionVector() { StorageType = "u", PathOrInlineDv = Z85.EncodeGuid(Guid.NewGuid()), Offset = 1, SizeInBytes = 40, Cardinality = 1 };
            await DeltaTestKit.WriteCommit(rig.Storage, "t", 1, new DeltaAction() { Add = new DeltaAddAction() { Path = "uncounted.parquet", Size = 1, DataChange = true, DeletionVector = dv } });
            await Commits(rig.Storage, 2, 2);
            rig.Worker.Published(2);
            await rig.TickAsync();
            Assert.True(rig.Worker.Skipped);
            // Output-dependent: never memoized
            Assert.Null(rig.Worker.Memo);

            await DeltaTestKit.WriteCommit(rig.Storage, "t", 3, DeltaLakeCheckpointBuildTests.Remove("uncounted.parquet", s_now.ToUnixTimeMilliseconds(), dv));
            rig.Worker.Published(3);
            rig.PastRetry();
            await rig.TickAsync();
            Assert.False(rig.Worker.Skipped);
            Assert.Equal(new long[] { 3 }, await Checkpoints(rig.Storage));
        }

        [Fact]
        public async Task SlowDeclineWaitsThreeTimesItsDuration()
        {
            var rig = Create(interval: 2);
            await DeltaTestKit.WriteCommit(rig.Storage, "t", 0, DeltaTestKit.ProtocolWith("deletionVectors"), DeltaTestKit.Metadata(DeltaTestKit.UserSchema), DeltaLakeCheckpointBuildTests.Add("f0.parquet"));
            var dv = new DeletionVector() { StorageType = "u", PathOrInlineDv = Z85.EncodeGuid(Guid.NewGuid()), Offset = 1, SizeInBytes = 40, Cardinality = 1 };
            await DeltaTestKit.WriteCommit(rig.Storage, "t", 1, new DeltaAction() { Add = new DeltaAddAction() { Path = "uncounted.parquet", Size = 1, DataChange = true, DeletionVector = dv } });
            await Commits(rig.Storage, 2, 2);
            var hold = HoldAt(rig, "build");
            rig.Worker.Published(2);
            rig.Budget.TickNow();
            await hold.WhenReached;
            var job = rig.Worker.CurrentJob;

            // The attempt takes an hour, then declines on its output, which is never memoized
            rig.Advance(TimeSpan.FromHours(1));
            hold.Release();
            await job;
            Assert.Equal(OutcomeKind.Declined, rig.Worker.LastOutcomeForTests!.Kind);
            Assert.Null(rig.Worker.Memo);
            Assert.Equal(TimeSpan.FromHours(3), rig.Worker.RetryAt - rig.OptionsClock.GetUtcNow());
        }

        [Fact]
        public async Task ASlowMemoizedDeclineStillReprobesAWeakRootAfterSixHours()
        {
            var rig = Create(interval: 2, cached: true);
            await Commits(rig.Storage, 0, 5);
            await DecliningRoot(rig.Storage, "parsed", 3);
            var hold = HoldAt(rig, "build");
            rig.Worker.Published(5);
            rig.Budget.TickNow();
            var job = rig.Worker.CurrentJob;
            try
            {
                await hold.WhenReached;
                // The attempt takes three hours
                rig.OptionsClock.Advance(TimeSpan.FromHours(3));
            }
            finally
            {
                hold.Release();
                await job.WaitAsync(TimeSpan.FromSeconds(30));
            }
            Assert.True(rig.Worker.Memo!.Weak);

            // A memoized decline is not spaced by the attempt's duration, so the weak memo's expiry still rereads the repaired root
            await WriteBytes(rig.Storage, $"/t/_delta_log/{3:D20}.checkpoint.parquet", OwnRootBytes(3));
            rig.OptionsClock.Advance(TimeSpan.FromHours(6));
            await rig.TickAsync();
            Assert.Contains(5L, await Checkpoints(rig.Storage));
        }

        [Fact]
        public async Task FilledHistoryGapPermitsRetry()
        {
            var rig = Create(interval: 2);
            await Commits(rig.Storage, 0, 0);
            await Commits(rig.Storage, 2, 3);
            rig.Worker.Published(3);
            await rig.TickAsync();
            Assert.True(rig.Worker.Skipped);
            Assert.Null(rig.Worker.Memo);

            await Commits(rig.Storage, 1, 1);
            rig.PastRetry();
            await rig.TickAsync();
            Assert.Equal(new long[] { 3 }, await Checkpoints(rig.Storage));
        }

        [Fact]
        public async Task RootMemoClearsWhenAnOlderRootBecomesContiguous()
        {
            var rig = Create(interval: 1);
            await Commits(rig.Storage, 0, 3);
            await DeltaLakeCheckpointBuildTests.LegacyRoot(rig.Storage, "t", 3);
            // Commit 4 is missing, so checkpoint 3 is no candidate for 7
            await Commits(rig.Storage, 5, 7);
            await ParsedOnlyRoot(rig.Storage, 6);
            rig.Worker.Published(7);
            await rig.TickAsync();
            Assert.Equal(MaintenanceReason.ParsedOnlyRoot, rig.Worker.Memo!.Reason);

            rig.PastRetry();
            rig.Hooks!.ClearRequests();
            await rig.TickAsync();
            Assert.Equal(0, RootReads(rig.Hooks));

            await Commits(rig.Storage, 4, 4);
            rig.PastRetry();
            rig.Hooks.ClearRequests();
            await rig.TickAsync();
            // The candidates changed, so the memo cleared and the build read its root again
            Assert.True(RootReads(rig.Hooks) > 0);
        }

        [Fact]
        public async Task SameVersionReplacementIsReprobedAfterSixHours()
        {
            // In-memory listings carry no modification time, so the memo is weak
            var rig = await DeclinedRig();
            Assert.True(rig.Worker.Memo!.Weak);
            var setAt = rig.OptionsClock.GetUtcNow();
            var held = 0;
            while (true)
            {
                rig.PastRetry();
                rig.Hooks!.ClearRequests();
                await rig.TickAsync();
                if (RootReads(rig.Hooks) > 0)
                {
                    break;
                }
                held++;
                Assert.True(rig.OptionsClock.GetUtcNow() - setAt < DeclineMemo.WeakExpiry, "The weak memo held past its expiry");
            }
            Assert.True(held >= 2, $"{held} held declines");
            Assert.True(rig.OptionsClock.GetUtcNow() - setAt >= DeclineMemo.WeakExpiry);
        }

        [Fact]
        public async Task StrongIdentityMemoHasNoTimeExpiry()
        {
            var rig = await DeclinedRig(stamp: true);
            Assert.False(rig.Worker.Memo!.Weak);
            rig.OptionsClock.Advance(TimeSpan.FromHours(7));
            rig.Hooks!.ClearRequests();
            await rig.TickAsync();
            Assert.Equal(0, RootReads(rig.Hooks));
            Assert.NotNull(rig.Worker.Memo);
        }

        [Fact]
        public async Task RepairedCommitClearsATailDependentMemo()
        {
            var rig = Create(interval: 2, stamp: true);
            await Commits(rig.Storage, 0, 1);
            var oversized = "{\"add\":{\"path\":\"big.parquet\",\"size\":1,\"modificationTime\":0,\"dataChange\":true,\"stats\":\"" + new string('x', 5 << 20) + "\"}}\n";
            await rig.Storage.WriteText($"/t/_delta_log/{2:D20}.json", oversized);
            rig.Worker.Published(2);
            await rig.TickAsync();
            Assert.Equal(DeclineDependency.Tail, rig.Worker.Memo!.Dependency);
            Assert.Equal(2, rig.Worker.Memo.TailCommit!.Version);

            await DeltaTestKit.WriteCommit(rig.Storage, "t", 2, DeltaLakeCheckpointBuildTests.Add("f2.parquet"));
            rig.PastRetry();
            await rig.TickAsync();
            Assert.Null(rig.Worker.Memo);
            Assert.Equal(new long[] { 2 }, await Checkpoints(rig.Storage));
        }

        // Root 10 passes the scan and fails to decode, is recorded unsupported, then declines with a memo
        private async Task<(Rig Rig, byte[] Valid)> UndecodableRootRig(bool stamp, bool cached = false)
        {
            var rig = Create(interval: 2, stamp: stamp, cached: cached);
            await Commits(rig.Storage, 0, 12);
            var valid = OwnRootBytes(10);
            await WriteBytes(rig.Storage, $"/t/_delta_log/{10:D20}.checkpoint.parquet", Undecodable(valid, 10));
            rig.Worker.Published(12);
            await rig.TickAsync();
            Assert.Single(rig.Worker.UnsupportedRoots);

            rig.PastRetry();
            await rig.TickAsync();
            Assert.Equal(MaintenanceReason.NoAdmissibleRoot, rig.Worker.Memo!.Reason);
            return (rig, valid);
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task RepairedRootRecoversOnIdentityChange(bool cached)
        {
            var (rig, valid) = await UndecodableRootRig(stamp: true, cached);
            // The repair has the same version and size; only the listing's modification time tells it apart
            await WriteBytes(rig.Storage, $"/t/_delta_log/{10:D20}.checkpoint.parquet", valid);
            rig.PastRetry();
            await rig.TickAsync();
            Assert.Contains(12L, await Checkpoints(rig.Storage));
            Assert.Empty(rig.Worker.UnsupportedRoots);
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task RepairedRootRecoversAfterSixHoursOnWeakIdentity(bool cached)
        {
            var (rig, valid) = await UndecodableRootRig(stamp: false, cached);
            await WriteBytes(rig.Storage, $"/t/_delta_log/{10:D20}.checkpoint.parquet", valid);
            var setAt = rig.OptionsClock.GetUtcNow();
            for (var attempt = 0; !(await Checkpoints(rig.Storage)).Contains(12); attempt++)
            {
                Assert.True(attempt < 1000, "The retry time never moved");
                rig.PastRetry();
                rig.Hooks!.ClearRequests();
                await rig.TickAsync();
                if (rig.OptionsClock.GetUtcNow() - setAt < DeclineMemo.WeakExpiry)
                {
                    // Held declines download no root
                    Assert.Equal(0, RootReads(rig.Hooks));
                }
                Assert.True(rig.OptionsClock.GetUtcNow() - setAt < TimeSpan.FromHours(8), "The repair was not picked up");
            }
            Assert.Empty(rig.Worker.UnsupportedRoots);
        }

        [Fact]
        public async Task TableIdChangeDropsTheMemoAndClassifications()
        {
            var (rig, _) = await UndecodableRootRig(stamp: true);
            Assert.NotNull(rig.Worker.Memo);
            rig.Worker.SetTableId("recreated");
            rig.PastRetry();
            await rig.TickAsync();
            // The facts were dropped at admission, so the root was read and classified again
            Assert.Equal(MaintenanceReason.RootDecodeFailure, rig.Worker.LastOutcomeForTests!.Reason);
        }

        [Fact]
        public async Task MemoIdentityIsStrongOnLocalDisk()
        {
            var root = Path.Combine(_directory, "strong");
            Directory.CreateDirectory(root);
            var storage = Files.Of.LocalDisk(root);
            await Commits(storage, 0, 0);
            var listing = await DeltaLogSegment.ListAsync(storage, "t");
            // Local disk listings carry a modification time
            Assert.True(ObjectIdentity.Of("t", listing.Listing.Commits[0]).Strong);
        }
    }
}
