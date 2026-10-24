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
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;
using Stowage;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public partial class OrphanSweepTests
    {
        // Long before any cutoff a sweep computes
        private static readonly DateTimeOffset s_old = new DateTimeOffset(2020, 1, 1, 0, 0, 0, TimeSpan.Zero);

        // What the frozen-set hook saw
        private sealed record Seen(List<FrozenCandidate> Frozen, OrphanCounts Counts, long H1, long VInventory, long ProtectionReads, DateTimeOffset Cutoff);

        // An eligible local table with own commits 0 to head and old files at the names
        private async Task<OrphanTable> LocalWithOldAsync(long head, params string[] names)
        {
            var table = LocalTable();
            await RetainedTableAsync(table.Raw, table.Name, head);
            foreach (var name in names)
            {
                Plant(table, name);
            }
            return table;
        }

        // A three-byte file at the name below the table, last written long before any cutoff
        private static void Plant(OrphanTable table, string name)
        {
            var path = Path.Combine(table.Folder!, name);
            Directory.CreateDirectory(Path.GetDirectoryName(path)!);
            File.WriteAllBytes(path, new byte[3]);
            File.SetLastWriteTimeUtc(path, s_old.UtcDateTime);
        }

        // The sweep over the table's own meter, whose buckets a test reads
        private static OrphanSweep SweepOver(OrphanTable table, MeteredFileStorage meter, RetentionCounters counters)
        {
            return new OrphanSweep(table.Configuration, meter, new FakeTimeProvider(Now), NullLogger.Instance, table.Name, counters);
        }

        // One settled run and what its hook saw; the run holds no memory, scratch or delete afterwards
        private static async Task<(CleanupResult Result, Seen? Seen)> RunAsync(OrphanSweep sweep, OrphanTable table, CleanupContext context, bool scratchUntilSettled = false)
        {
            Seen? seen = null;
            sweep.FrozenHookForTests = inventory =>
            {
                Assert.Equal(0, inventory.Identity.Owed);
                seen = new Seen(Frozen(inventory, table), inventory.Counts, inventory.H1, inventory.VInventory, inventory.ProtectionReads, inventory.Cutoff);
            };
            CleanupResult result;
            try
            {
                result = await sweep.RunAsync(context);
            }
            finally
            {
                await context.SettleAsync();
            }
            Assert.Equal(0, context.Memory.Managed);
            Assert.Equal(0, context.Memory.Native);
            if (scratchUntilSettled)
            {
                // The worker disposes the job's scratch as it settles the job
                context.Scratch.Dispose();
                Assert.Equal(0, context.Scratch.Ledger.Reserved);
            }
            Assert.Empty(context.Scratch.Files);
            Assert.DoesNotContain(table.Owned.Calls, x => x.StartsWith("Rm ", StringComparison.Ordinal));
            return (result, seen);
        }

        private static void AssertResult(CleanupResult result, OutcomeKind kind, MaintenanceReason reason)
        {
            Assert.True(result.Kind == kind && result.Reason == reason, $"{result.Kind} {result.Reason}: {result.Detail}");
        }

        private static void AssertNoReference(OrphanTable table)
        {
            Assert.False(File.Exists(Path.Combine(table.Folder!, ".flowtide-orphan-sweep")));
        }

        [Fact]
        public async Task L1IsSingleRecursiveListingWithoutCountCap()
        {
            var table = await LocalWithOldAsync(2, "old.parquet");
            // More than short planning reads or one attempt's deletions
            const int synthesized = 100_001;
            table.Owned.AfterRecursiveLs = (folder, listed) =>
            {
                var entries = listed.ToList();
                for (int i = 0; i < synthesized; i++)
                {
                    entries.Add(new IOEntry($"{table.Prefix}_synthesized/f{i:D6}.parquet") { Size = 1, LastModificationTime = s_old });
                }
                return entries;
            };
            var counters = new RetentionCounters();

            var (result, seen) = await RunAsync(Sweep(table.Configuration, table.Owned, counters: counters), table, OrphanContext(2));

            AssertResult(result, OutcomeKind.Completed, MaintenanceReason.None);
            Assert.Equal(new[] { "Ls " + table.Prefix }, table.Owned.Recursive);
            Assert.Equal(1, table.Owned.Listings(table.Prefix));
            Assert.True(counters.OrphanListed >= synthesized, $"{counters.OrphanListed} listed");
            Assert.Equal(synthesized, seen!.Counts.Hidden);
            Assert.Equal("old.parquet", Assert.Single(seen.Frozen).Relative);
        }

        [Fact]
        public async Task RecursiveNamesUseFullPathRatherThanLeaf()
        {
            // Commit names above the head, outside the table's own log
            var table = await LocalWithOldAsync(2, $"sub/{9:D20}.json", $"nested/_delta_log/{9:D20}.json", "nested/part-old.parquet");

            var (result, seen) = await RunAsync(Sweep(table.Configuration, table.Owned), table, OrphanContext(2));

            AssertResult(result, OutcomeKind.Completed, MaintenanceReason.None);
            Assert.Equal(2, seen!.H1);
            Assert.Equal($"sub/{9:D20}.json", Assert.Single(seen.Frozen).Relative);
            Assert.Equal(1, seen.Counts.NestedRoots);
        }

        [Fact]
        public async Task RootInventoryChecksItsOwnHeadAndSuccessor()
        {
            // The worker published past the head the root listing holds
            var ahead = await LocalWithOldAsync(2, "old.parquet");
            var counters = new RetentionCounters();
            var (result, seen) = await RunAsync(Sweep(ahead.Configuration, ahead.Owned, counters: counters), ahead, OrphanContext(2, published: 3));
            AssertResult(result, OutcomeKind.Declined, MaintenanceReason.ListingIneligible);
            Assert.Null(seen);
            Assert.Equal(1, counters.ListingDeclines(ListingCheck.RangeGap));
            Assert.Single(ahead.Owned.Recursive);
            Assert.Empty(ahead.Owned.CallsAfterRecursive());

            // An interior commit missing from the root listing only
            var gap = await LocalWithOldAsync(2, "old.parquet");
            var interior = $"{gap.Prefix}_delta_log/{1:D20}.json";
            gap.Owned.AfterRecursiveLs = (folder, listed) => listed.Where(x => x.Path.Full != interior).ToList();
            counters = new RetentionCounters();
            (result, seen) = await RunAsync(Sweep(gap.Configuration, gap.Owned, counters: counters), gap, OrphanContext(2));
            AssertResult(result, OutcomeKind.Declined, MaintenanceReason.ListingIneligible);
            Assert.Null(seen);
            Assert.Equal(1, counters.ListingDeclines(ListingCheck.RangeGap));
            Assert.Single(gap.Owned.Recursive);

            // The root listing's own successor is probed first after it
            var current = await LocalWithOldAsync(2, "old.parquet");
            counters = new RetentionCounters();
            (result, seen) = await RunAsync(Sweep(current.Configuration, current.Owned, counters: counters), current, OrphanContext(2, published: 2));
            AssertResult(result, OutcomeKind.Completed, MaintenanceReason.None);
            Assert.Equal((2L, 2L), (seen!.H1, seen.VInventory));
            Assert.Equal($"Stat {current.Prefix}_delta_log/{3:D20}.json", current.Owned.CallsAfterRecursive()[0]);
            // The root listing's probe, then L2's and L3's
            Assert.Equal(3, counters.Probes(ProbeResult.Absent));
            Assert.Equal("old.parquet", Assert.Single(seen.Frozen).Relative);
        }

        [Fact]
        public async Task RootInventorySuccessorPublicationDeclinesWithoutDeletion()
        {
            var table = await LocalWithOldAsync(2, "old.parquet");
            // Commit 3 lands while the root listing is delivered
            table.Owned.AfterRecursiveLs = (folder, listed) =>
            {
                File.WriteAllText(Path.Combine(table.Folder!, "_delta_log", $"{3:D20}.json"), "{}");
                return listed;
            };
            var counters = new RetentionCounters();
            var meter = Metered(table);

            var (result, seen) = await RunAsync(SweepOver(table, meter, counters), table, OrphanContext(2));

            AssertResult(result, OutcomeKind.Declined, MaintenanceReason.ListingIneligible);
            Assert.Null(seen);
            Assert.Equal(1, counters.ListingDeclines(ListingCheck.SuccessorPresent));
            Assert.Equal(1, counters.Probes(ProbeResult.Present));
            Assert.Single(table.Owned.Recursive);
            // No candidate was resolved
            Assert.Equal(0, meter.Buckets.NativeRequests);
        }

        [Fact]
        public async Task HiddenFoldersAndPlaceholdersNeverCandidate()
        {
            var table = await LocalWithOldAsync(2, "_x/a.parquet", ".x/a.parquet", "a/_tmp/b.parquet", "a/.c", "dir_$folder$", "x/_change_data/y", "_change_data/cdc.parquet", "a/keep.parquet");
            File.SetLastWriteTimeUtc(Path.Combine(table.Folder!, "_delta_log", $"{0:D20}.json"), s_old.UtcDateTime);
            var listedFolders = new List<string>();
            table.Owned.AfterRecursiveLs = (folder, listed) =>
            {
                listedFolders.AddRange(listed.Where(x => x.Path.IsFolder).Select(x => x.Path.Full.Substring(table.Prefix.Length)));
                // A directory entry with an old time, at its sorted position once checked
                return listed.Append(new IOEntry(table.Prefix + "d/") { LastModificationTime = s_old }).ToList();
            };

            var (result, seen) = await RunAsync(Sweep(table.Configuration, table.Owned), table, OrphanContext(2));

            AssertResult(result, OutcomeKind.Completed, MaintenanceReason.None);
            Assert.Equal(new[] { "_change_data/cdc.parquet", "a/keep.parquet" }, seen!.Frozen.Select(x => x.Relative).Order(StringComparer.Ordinal));
            Assert.Equal(1, seen.Counts.Reference);
            // Counted before the hidden rule: three commits and the log folder when the provider lists it
            Assert.Equal(3 + listedFolders.Count(x => x == "_delta_log/"), seen.Counts.Log);
            // Five files and any hidden folder the provider lists itself
            Assert.Equal(5 + listedFolders.Count(x => x is "_x/" or ".x/" or "a/_tmp/" or "x/_change_data/"), seen.Counts.Hidden);
            // The placeholder, the injected directory and any plain folder the provider lists itself
            Assert.Equal(2 + listedFolders.Count(x => x is "a/" or "x/" or "_change_data/"), seen.Counts.Folder);
            Assert.Equal(2, seen.Counts.Frozen);
        }

        [Fact]
        public async Task MissingTimesDoNotAuthorizeDeletion()
        {
            // The reference's Stat without a time, then without an entry
            foreach (var missing in new Func<IOPath, IOEntry?, IOEntry?>[] { (path, entry) => entry == null ? null : new IOEntry(path.Full) { Size = entry.Size }, (path, entry) => null })
            {
                var table = await LocalWithOldAsync(2, "old.parquet");
                var reference = table.Prefix + ".flowtide-orphan-sweep";
                table.Owned.StatAnswer = (path, entry) => path.Full == reference ? missing(path, entry) : entry;
                var (declined, none) = await RunAsync(Sweep(table.Configuration, table.Owned), table, OrphanContext(2));
                AssertResult(declined, OutcomeKind.Declined, MaintenanceReason.OrphanNotEligible);
                Assert.Null(none);
                Assert.Empty(table.Owned.Recursive);
            }

            // A candidate listed without a time
            var listed = await LocalWithOldAsync(2, "a.parquet", "b.parquet");
            var timeless = listed.Prefix + "a.parquet";
            listed.Owned.AfterRecursiveLs = (folder, entries) => entries.Select(x => x.Path.Full == timeless ? new IOEntry(x.Path.Full) { Size = x.Size } : x).ToList();
            var (result, seen) = await RunAsync(Sweep(listed.Configuration, listed.Owned), listed, OrphanContext(2));
            AssertResult(result, OutcomeKind.Completed, MaintenanceReason.None);
            Assert.Equal("b.parquet", Assert.Single(seen!.Frozen).Relative);
            Assert.Equal(1, seen.Counts.NoTime);
        }

        [Fact]
        public async Task DiskCapDeclinesAfterUncappedListing()
        {
            var table = await LocalWithOldAsync(2, "a.parquet", "b.parquet", "c.parquet");
            var meter = Metered(table);
            // The second candidate spills, and no spill fits the disk
            var sweep = SweepOver(table, meter, new RetentionCounters());
            sweep.ArenaCap = 1;

            // The sorter's run that failed mid-write stays on the job's scratch until the job settles
            var (result, seen) = await RunAsync(sweep, table, OrphanContext(2, diskCap: 1), scratchUntilSettled: true);

            AssertResult(result, OutcomeKind.Declined, MaintenanceReason.DiskCap);
            Assert.True(result.Oversized);
            Assert.Null(seen);
            Assert.Single(table.Owned.Recursive);
            // The table root and the two candidates resolved before the spill were charged
            Assert.Equal(3, meter.Buckets.NativeRequests);
        }

        [Fact]
        public async Task UnresolvedAdvanceDefersOrphanPlanning()
        {
            using var kit = new AdvanceExecutionKit();
            var execution = await kit.ChurnedAsync();
            await kit.PublishAsync(AdvanceExecutionKit.Cleanup(execution), execution);
            var table = new OrphanTable()
            {
                Raw = execution.Raw,
                Owned = new LocalTestStorage(execution.Raw),
                Configuration = execution.Configuration,
                Name = "t",
                Folder = Path.Combine(execution.Root!, "t")
            };

            var (result, seen) = await RunAsync(Sweep(table.Configuration, table.Owned), table, OrphanContext(execution.Head));

            AssertResult(result, OutcomeKind.Declined, MaintenanceReason.OrphanNotEligible);
            Assert.True(result.Defers);
            Assert.Null(seen);
            AssertNoReference(table);
            Assert.Empty(table.Owned.Recursive);
        }

        [Fact]
        public async Task ProtectionReadsOverTheCeilingDeclineBeforeListing()
        {
            var table = await LocalWithOldAsync(2, "old.parquet");
            var sweep = Sweep(table.Configuration, table.Owned);
            sweep.MaxProtectionReads = 2;

            var (result, seen) = await RunAsync(sweep, table, OrphanContext(2));

            AssertResult(result, OutcomeKind.Declined, MaintenanceReason.OrphanNotEligible);
            Assert.Null(seen);
            AssertNoReference(table);
            Assert.Empty(table.Owned.Recursive);

            // Three commits fit a ceiling of three
            sweep.MaxProtectionReads = 3;
            (result, seen) = await RunAsync(sweep, table, OrphanContext(2));
            AssertResult(result, OutcomeKind.Completed, MaintenanceReason.None);
            Assert.Equal(3, seen!.ProtectionReads);
        }

        [Fact]
        public async Task OldestBelowAdoptionDeclines()
        {
            var table = LocalTable();
            // Every commit from 0, adopted at 2
            await BoundarySelectorTests.Create(table.Raw, 3, BoundarySelectorTests.Old, adoptedAt: 2);
            Plant(table, "old.parquet");
            var sweep = Sweep(table.Configuration, table.Owned);
            PreparedAdvance? prepared = null;
            sweep.Prepared = x => prepared = x;

            var (result, seen) = await RunAsync(sweep, table, OrphanContext(3));

            AssertResult(result, OutcomeKind.Declined, MaintenanceReason.OrphanNotEligible);
            Assert.Equal((0L, 2L), (prepared!.O, prepared.A));
            Assert.False(result.Defers);
            Assert.Null(seen);
            AssertNoReference(table);
            Assert.Empty(table.Owned.Recursive);
        }

        [Fact]
        public async Task HugeRetentionSaturatesTheCutoff()
        {
            var largest = TimeSpan.FromTicks(TimeSpan.MaxValue.Ticks / TimeSpan.TicksPerHour * TimeSpan.TicksPerHour);
            var table = LocalTable();
            var configuration = CleanupConfiguration.Validate(table.Raw, table.Name, false, new CleanupOptIns(largest, Interval));
            var published = new Dictionary<string, string>()
            {
                [RetentionProperties.LogKey] = RetentionProperties.Format(largest),
                [RetentionProperties.DeletedFileKey] = RetentionProperties.Format(largest)
            };
            await DeltaTestKit.WriteOwnCommit(table.Raw, table.Name, 0, 0, BoundarySelectorTests.Old(0), DeltaTestKit.Protocol(), DeltaTestKit.Metadata(DeltaTestKit.UserSchema, published));
            Plant(table, "old.parquet");

            var (result, seen) = await RunAsync(Sweep(configuration, table.Owned), table, OrphanContext(0));

            AssertResult(result, OutcomeKind.Completed, MaintenanceReason.None);
            Assert.Equal(DateTimeOffset.MinValue, seen!.Cutoff);
            Assert.Empty(seen.Frozen);
            Assert.Equal(1, seen.Counts.Young);
        }

        [Fact]
        public async Task StorageReplacedBeforeRunDeclines()
        {
            var table = await LocalWithOldAsync(2, "old.parquet");
            IFileStorage holder = table.Raw;
            var configuration = CleanupConfiguration.Validate(() => holder, table.Name, false, new CleanupOptIns(Retention, Interval));
            var counters = new RetentionCounters();
            var sweep = Sweep(configuration, table.Owned, counters: counters);
            // Another instance over the same folder
            using var replacement = Files.Of.LocalDisk(Path.GetDirectoryName(table.Folder!)!);
            holder = replacement;

            var (result, seen) = await RunAsync(sweep, table, OrphanContext(2));

            AssertResult(result, OutcomeKind.Declined, MaintenanceReason.ListingIneligible);
            Assert.Equal(1, counters.ListingDeclines(ListingCheck.StorageChanged));
            Assert.Null(seen);
            AssertNoReference(table);
            Assert.Empty(table.Owned.Recursive);

            holder = table.Raw;
            (result, _) = await RunAsync(sweep, table, OrphanContext(2));
            AssertResult(result, OutcomeKind.Completed, MaintenanceReason.None);
        }

        [Fact]
        public async Task UnsupportedIdentityDeclinesBeforeListing()
        {
            var local = LocalTable();
            using var raw = Files.Of.InternalMemory($"./orphan-{Guid.NewGuid():N}");
            await RetainedTableAsync(raw, "t", 2);
            await raw.WriteText("/t/old.parquet", "old");
            // Local disk's contract over a store without native paths
            var configuration = CleanupConfiguration.Validate(CleanupStoreContract.Create(raw, local.Configuration.Store.Store, "t", false), new CleanupOptIns(Retention, Interval));
            var table = new OrphanTable() { Raw = raw, Owned = new TestStorage(raw), Configuration = configuration, Name = "t" };

            var (result, seen) = await RunAsync(Sweep(configuration, table.Owned), table, OrphanContext(2));

            AssertResult(result, OutcomeKind.Declined, MaintenanceReason.OrphanNotEligible);
            Assert.Null(seen);
            Assert.False(await raw.Exists("/t/.flowtide-orphan-sweep"));
            Assert.Empty(table.Owned.Recursive);
        }

        [Fact]
        public async Task ForeignHeadConcludesForTheOrphanKind()
        {
            var table = await LocalWithOldAsync(2, "old.parquet");
            // Another writer's commit without commitInfo
            await DeltaTestKit.WriteCommit(table.Raw, table.Name, 3, DeltaLakeCheckpointBuildTests.Add("foreign.parquet"));
            var advance = await new AdvanceJob(table.Configuration, Metered(table), new FakeTimeProvider(Now)).RunAsync(OrphanContext(3));
            Assert.True(advance.Kind == OutcomeKind.Declined && advance.ForeignHead, $"{advance.Kind} {advance.Reason}: {advance.Detail}");

            var (result, seen) = await RunAsync(Sweep(table.Configuration, table.Owned), table, OrphanContext(3));

            AssertResult(result, OutcomeKind.Declined, advance.Reason);
            Assert.False(result.ForeignHead);
            Assert.Null(seen);
            AssertNoReference(table);
            Assert.Empty(table.Owned.Recursive);
        }

        [Fact]
        public async Task RootInventoryRequiresJsonAtOldestRetainedVersion()
        {
            // A usable checkpoint at the oldest retained version without that version's commit
            var table = await LocalWithOldAsync(2, "old.parquet");
            using var kit = new AdvanceExecutionKit();
            await kit.BuildAsync(table.Raw, 0);
            File.Delete(Path.Combine(table.Folder!, "_delta_log", $"{0:D20}.json"));
            var sweep = Sweep(table.Configuration, table.Owned);
            PreparedAdvance? prepared = null;
            sweep.Prepared = value => prepared = value;

            var (result, seen) = await RunAsync(sweep, table, OrphanContext(2));

            Assert.Equal((0L, 0L, 1L), (prepared!.O, prepared.A, prepared.FirstJson));
            AssertResult(result, OutcomeKind.Declined, MaintenanceReason.OrphanNotEligible);
            Assert.False(result.Defers);
            Assert.Null(seen);
            AssertNoReference(table);
            Assert.Empty(table.Owned.Recursive);
        }

        [Fact]
        public async Task RootInventoryHeadBelowTheOldestRetainedVersionDeclines()
        {
            // Rooted at checkpoint 2 with commits 0 and 1 gone
            var table = await LocalWithOldAsync(4, "old.parquet");
            using var kit = new AdvanceExecutionKit();
            await kit.BuildAsync(table.Raw, 2);
            var zero = Path.Combine(table.Folder!, "_delta_log", $"{0:D20}.json");
            var zeroBytes = await File.ReadAllBytesAsync(zero);
            File.Delete(zero);
            File.Delete(Path.Combine(table.Folder!, "_delta_log", $"{1:D20}.json"));
            var log = table.Prefix + "_delta_log/";
            var left = $"{log}{0:D20}.json";
            // A stale root listing whose log shows only the commit below the root
            table.Owned.AfterRecursiveLs = (folder, listed) => listed.Where(x => !x.Path.Full.StartsWith(log, StringComparison.Ordinal) || x.Path.Full == left).ToList();
            var counters = new RetentionCounters();
            var sweep = Sweep(table.Configuration, table.Owned, counters: counters);
            PreparedAdvance? prepared = null;
            // Commit 0 is back after preparation
            sweep.Prepared = value =>
            {
                prepared = value;
                File.WriteAllBytes(zero, zeroBytes);
            };

            // The worker knows only version 0 as published
            var (result, seen) = await RunAsync(sweep, table, OrphanContext(0));

            Assert.Equal((2L, 2L), (prepared!.O, prepared.FirstJson));
            AssertResult(result, OutcomeKind.Declined, MaintenanceReason.ListingIneligible);
            Assert.Null(seen);
            Assert.Equal(1, counters.ListingDeclines(ListingCheck.RangeGap));
            Assert.Single(table.Owned.Recursive);
        }

        [Fact]
        public async Task LocalLogSubfolderStaysOutOfTheLogProjection()
        {
            // A sidecar folder another writer left in the log
            var table = await LocalWithOldAsync(2, "old.parquet", "_delta_log/_sidecars/x.parquet");
            var counters = new RetentionCounters();

            var (result, seen) = await RunAsync(Sweep(table.Configuration, table.Owned, counters: counters), table, OrphanContext(2));

            AssertResult(result, OutcomeKind.Completed, MaintenanceReason.None);
            Assert.Equal(2, seen!.H1);
            Assert.Equal("old.parquet", Assert.Single(seen.Frozen).Relative);
            Assert.Equal(0, counters.ListingDeclines(ListingCheck.UnsupportedSubfolder));
            Assert.Single(table.Owned.Recursive);
        }

        [Fact]
        public async Task ManyNestedRootsKeepOnlyTheirMembers()
        {
            var names = new List<string>() { "old.parquet" };
            for (int i = 0; i < 10; i++)
            {
                names.AddRange(new[] { $"n{i}/_delta_log/{0:D20}.json", $"n{i}/part.parquet", $"n{i}/deeper/x.parquet", $"n{i}.parquet" });
            }
            var table = await LocalWithOldAsync(2, names.ToArray());

            var (result, seen) = await RunAsync(Sweep(table.Configuration, table.Owned), table, OrphanContext(2));

            AssertResult(result, OutcomeKind.Completed, MaintenanceReason.None);
            Assert.Equal(10, seen!.Counts.NestedRoots);
            // Each root's two files; a sibling sharing the root's name prefix is no member
            Assert.Equal(Enumerable.Range(0, 10).Select(i => $"n{i}.parquet").Append("old.parquet").Order(StringComparer.Ordinal), seen.Frozen.Select(x => x.Relative).Order(StringComparer.Ordinal));
            Assert.True(seen.Counts.Nested >= 20, $"{seen.Counts.Nested} nested");
        }

        [Fact]
        public async Task NestedRootsFoundOutOfOrdinalOrderKeepTheirMembers()
        {
            // a-b/ lists before a/ while the root a sorts first, and B sorts first by ordinal but last by culture
            var table = await LocalWithOldAsync(2, $"B/_delta_log/{0:D20}.json", "B/z.parquet", $"a/_delta_log/{0:D20}.json", "a/x.parquet", $"a-b/_delta_log/{0:D20}.json", "a-b/y.parquet", "old.parquet");

            var (result, seen) = await RunAsync(Sweep(table.Configuration, table.Owned), table, OrphanContext(2));

            AssertResult(result, OutcomeKind.Completed, MaintenanceReason.None);
            Assert.Equal(3, seen!.Counts.NestedRoots);
            Assert.Equal(0, seen.Counts.Unqualified);
            Assert.Equal("old.parquet", Assert.Single(seen.Frozen).Relative);
        }

        [Fact]
        public async Task NestedRootChargeHoldsUntilTheSetIsSealed()
        {
            var table = await LocalWithOldAsync(2);
            const int roots = 128;
            var memory = new JobMemory();
            long rootBytes = 0;
            var listed = false;
            long? chargedAtSeal = null;
            table.Owned.AfterRecursiveLs = (folder, entries) =>
            {
                var all = entries.ToList();
                for (int i = 0; i < roots; i++)
                {
                    var root = $"child{i:D4}";
                    rootBytes += 64 + 2L * root.Length;
                    all.Add(new IOEntry($"{table.Prefix}{root}/_delta_log/{0:D20}.json") { Size = 1, LastModificationTime = s_old });
                }
                listed = true;
                return all;
            };
            // The empty candidate set's seal takes the first run buffer after the root listing
            memory.BeforeReserveForTests = bytes =>
            {
                if (listed && bytes == 256L * 1024 && chargedAtSeal == null)
                {
                    chargedAtSeal = memory.Managed;
                }
            };

            var (result, seen) = await RunAsync(Sweep(table.Configuration, table.Owned), table, OrphanContext(2, memory: memory));

            AssertResult(result, OutcomeKind.Completed, MaintenanceReason.None);
            Assert.Equal(roots, seen!.Counts.NestedRoots);
            Assert.Empty(seen.Frozen);
            Assert.True(chargedAtSeal >= rootBytes, $"{chargedAtSeal} charged at the seal while the nested roots hold {rootBytes}");
        }
    }
}
