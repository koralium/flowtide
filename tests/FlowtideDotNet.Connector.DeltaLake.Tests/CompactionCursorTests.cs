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

using FlowtideDotNet.Connector.DeltaLake.Internal.Catalog;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Actions;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.DeletionVectors;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.Compaction;
using FlowtideDotNet.Storage.Persistence.Reservoir.Internal;
using FlowtideDotNet.Storage.StateManager;
using Microsoft.Extensions.Logging;
using Stowage;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public class CompactionCursorTests
    {
        private const long Minute = 60_000;
        private const long Now = 1_000 * 60 * Minute;
        private const long ColdAt = Now - 2 * 60 * Minute;
        private const int Class2 = 1 << 2;
        private const int Class3 = 1 << 3;

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task TheCursorWrapsFindsRecycledIdsAndResetsOnAnEpoch(bool treeMode)
        {
            var limits = treeMode
                ? new CompactionLimits() { ScanSlots = 100_000, ScanLeaves = 1, ColdReads = 10_000, Hits = 10_000 }
                : new CompactionLimits() { ScanSlots = 7, ColdReads = 10_000, Hits = 10_000 };
            await using var rig = await Rig.Create($"cursor_wrap_{treeMode}", treeMode, limits, Enumerable.Range(0, 200).Select(x => File(x, 20_000, ColdAt)), pageBytes: treeMode ? 1024 : 32 * 1024);
            var cursor = rig.Catalog.Cursor(DueRule.Count)!;

            var calls = await rig.Lap(DueRule.Count, Class2, null, Now, (inspected, leaves) =>
            {
                Assert.True(treeMode ? leaves <= 1 : inspected <= 7, $"inspected {inspected} leaves {leaves}");
            });
            Assert.True(calls > 2, $"calls {calls}");
            Assert.Equal(Enumerable.Range(0, 200), cursor.Hits.Select(x => x.Id));
            Assert.Equal(0, cursor.Position);

            await rig.Commit(Array.Empty<DeltaAddAction>(), removes: new[] { 10, 42 }, updates: new[] { 77 });
            Assert.DoesNotContain(cursor.Hits, x => x.Id == 10 || x.Id == 42 || x.Id == 77);
            rig.Catalog.Consume(DueRule.Count, cursor.Hits.Select(x => x.Id).ToHashSet());
            var reused = await rig.Commit(new[] { File(1_000, 20_000, ColdAt), File(1_001, 20_000, ColdAt) });
            Assert.Equal(new[] { 42, 10 }, reused);

            await rig.Lap(DueRule.Count, Class2, new HashSet<int>() { 5 }, Now, null);
            var hits = cursor.Hits.ToDictionary(x => x.Id, x => x.Record);
            Assert.Equal(Enumerable.Range(0, 200).Where(x => x != 5).Order(), hits.Keys.Order());
            Assert.Equal("part-1000.parquet", hits[42].Path);
            Assert.Equal("part-1001.parquet", hits[10].Path);
            Assert.Equal("u", hits[77].DeletionVector!.StorageType);

            var fill = rig.Catalog.Cursor(DueRule.Fill)!;
            await rig.Lap(DueRule.Fill, Class3, null, Now, null);
            Assert.True(fill.PausedUntil > Now);
            rig.Catalog.Consume(DueRule.Count, cursor.Hits.Select(x => x.Id).ToHashSet());
            Assert.Equal(CursorStop.Budget, await rig.Catalog.Discover(DueRule.Count, Class2, null, Now));
            Assert.True(cursor.Position > 0);
            rig.Catalog.Declines!.Decline(3, "part-3.parquet", (await rig.Catalog.GetFiles(new[] { 3 }))[3], CompactionReason.Precision);
            var epoch = rig.Catalog.CatalogEpoch;

            await rig.Overwrite(new[] { File(2_000, 20_000, ColdAt) });

            Assert.True(rig.Catalog.CatalogEpoch > epoch);
            Assert.All(new[] { cursor, fill }, x =>
            {
                Assert.Equal(0, x.Position);
                Assert.Empty(x.Hits);
                Assert.Equal(0, x.YoungFiles);
                Assert.Equal(long.MinValue, x.PausedUntil);
            });
            Assert.Equal(0, rig.Catalog.Declines!.Count);
            Assert.Equal(0, rig.Catalog.Declines.DeclinedFiles(2));
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task TheColdFilterDropsYoungHitsAndSetsThePause(bool skipOne)
        {
            var files = new List<DeltaAddAction>();
            for (int i = 0; i < 70; i++)
            {
                var young = i % 7 != 0 ? (i / 7) * 6 + i % 7 - 1 : -1;
                files.Add(File(i, 20_000, young < 0 ? ColdAt : Now - 30 * Minute + (young + 1) * 10_000));
            }
            await using var rig = await Rig.Create($"cursor_cold_{skipOne}", false, new CompactionLimits(), files);
            var cursor = rig.Catalog.Cursor(DueRule.Count)!;
            var youngTimes = files.Where(x => x.ModificationTime != ColdAt).Select(x => x.ModificationTime).Order().ToList();

            var calls = await rig.Lap(DueRule.Count, Class2, null, Now, null);

            Assert.Equal(2, calls);
            Assert.Equal(70, cursor.Reads);
            Assert.Equal(Enumerable.Range(0, 10).Select(x => x * 7), cursor.Hits.Select(x => x.Id).Order());
            Assert.Equal(50, cursor.YoungFiles);
            var pausedUntil = youngTimes[39] + 30 * Minute;
            Assert.Equal(pausedUntil, cursor.PausedUntil);
            Assert.Equal(CursorStop.Paused, await rig.Catalog.Discover(DueRule.Count, Class2, null, pausedUntil - 1));
            Assert.Equal(70, cursor.Reads);

            var dueYoung = files.Single(x => x.ModificationTime == youngTimes[0]);
            var skippedId = int.Parse(dueYoung.Path!.Split('-', '.')[1]);
            var inspected = cursor.Inspected;
            var stop = await rig.Catalog.Discover(DueRule.Count, Class2, skipOne ? new HashSet<int>() { skippedId } : null, pausedUntil);

            if (skipOne)
            {
                Assert.Equal(CursorStop.LapEnd, stop);
                Assert.Equal(120, cursor.Reads);
                Assert.True(cursor.Inspected > inspected);
                Assert.Equal(49, cursor.Hits.Count);
                Assert.DoesNotContain(cursor.Hits, x => x.Id == skippedId);
            }
            else
            {
                Assert.Equal(CursorStop.Full, stop);
                Assert.Equal(110, cursor.Reads);
                Assert.Equal(inspected, cursor.Inspected);
                Assert.Equal(50, cursor.Hits.Count);
            }
        }

        [Fact]
        public async Task AFullBufferStopsJustAfterItsLastHit()
        {
            await using var rig = await Rig.Create("cursor_full", false, new CompactionLimits(), Enumerable.Range(0, 120).Select(x => File(x, 20_000, ColdAt)));
            var cursor = rig.Catalog.Cursor(DueRule.Count)!;

            Assert.Equal(CursorStop.Full, await rig.Catalog.Discover(DueRule.Count, Class2, null, Now));
            Assert.Equal(64, cursor.Reads);
            Assert.Equal(Enumerable.Range(0, 50), cursor.Hits.Select(x => x.Id));
            Assert.Equal(50, cursor.Position);
            await rig.Fold(DueRule.Count, Enumerable.Range(0, 50));

            Assert.Equal(CursorStop.Full, await rig.Catalog.Discover(DueRule.Count, Class2, null, Now));
            Assert.Equal(128, cursor.Reads);
            Assert.Equal(Enumerable.Range(50, 50), cursor.Hits.Select(x => x.Id));
            Assert.Equal(100, cursor.Position);
            cursor.Stall();
            Assert.Empty(cursor.Hits);
            Assert.Equal(1, cursor.Stalls);

            Assert.Equal(CursorStop.LapEnd, await rig.Catalog.Discover(DueRule.Count, Class2, null, Now));
            Assert.Equal(148, cursor.Reads);
            Assert.Equal(Enumerable.Range(100, 20), cursor.Hits.Select(x => x.Id));
            await rig.Fold(DueRule.Count, Enumerable.Range(100, 20));

            Assert.Equal(CursorStop.Full, await rig.Catalog.Discover(DueRule.Count, Class2, null, Now));
            Assert.Equal(Enumerable.Range(50, 50), cursor.Hits.Select(x => x.Id));
        }

        [Fact]
        public async Task ADeclinedHitIsMemoizedAndNotReadAgain()
        {
            var files = Enumerable.Range(0, 20).Select(x => File(x, 20_000, ColdAt, x == 4 ? "i" : null)).ToList();
            await using var rig = await Rig.Create("cursor_declines", false, new CompactionLimits(), files);
            var cursor = rig.Catalog.Cursor(DueRule.Count)!;
            var declines = rig.Catalog.Declines!;
            declines.Decline(7, "part-7.parquet", (await rig.Catalog.GetFiles(new[] { 7 }))[7], CompactionReason.Precision);

            Assert.Equal(CursorStop.LapEnd, await rig.Catalog.Discover(DueRule.Count, Class2, null, Now));
            Assert.Equal(19, cursor.Reads);
            Assert.Equal(18, cursor.Hits.Count);
            Assert.DoesNotContain(cursor.Hits, x => x.Id == 4 || x.Id == 7);
            Assert.Equal(CompactionReason.InlineDv, declines.ReasonOf(4));
            Assert.Equal(2, declines.Count);
            Assert.Equal(2, declines.DeclinedFiles(2));
            Assert.Equal(40_000, declines.DeclinedBytes(2));

            rig.Catalog.Consume(DueRule.Count, cursor.Hits.Select(x => x.Id).ToHashSet());
            Assert.Equal(long.MinValue, cursor.PausedUntil);
            Assert.Equal(CursorStop.LapEnd, await rig.Catalog.Discover(DueRule.Count, Class2, null, Now));
            Assert.Equal(37, cursor.Reads);
            Assert.DoesNotContain(cursor.Hits, x => x.Id == 4 || x.Id == 7);
            Assert.True(cursor.PausedUntil > Now);

            await rig.Commit(Array.Empty<DeltaAddAction>(), removes: new[] { 7 });
            Assert.Equal(1, declines.Count);
            Assert.Equal(1, declines.DeclinedFiles(2));
        }

        [Fact]
        public async Task OneRulesPauseDoesNotBlockAnother()
        {
            var files = Enumerable.Range(0, 5).Select(x => File(x, 20_000, Now - 10 * Minute + x * 1_000))
                .Concat(Enumerable.Range(5, 4).Select(x => File(x, 40_000, ColdAt)))
                .ToList();
            await using var rig = await Rig.Create("cursor_rules", false, new CompactionLimits(), files);
            var count = rig.Catalog.Cursor(DueRule.Count)!;
            var fill = rig.Catalog.Cursor(DueRule.Fill)!;

            Assert.Equal(CursorStop.LapEnd, await rig.Catalog.Discover(DueRule.Count, Class2, null, Now));
            Assert.Equal(Now - 10 * Minute + 4_000 + 30 * Minute, count.PausedUntil);
            Assert.Equal(CursorStop.LapEnd, await rig.Catalog.Discover(DueRule.Fill, Class3, null, Now));
            Assert.Equal(new[] { 5, 6, 7, 8 }, fill.Hits.Select(x => x.Id));
            var reads = count.Reads;
            Assert.Equal(CursorStop.Paused, await rig.Catalog.Discover(DueRule.Count, Class2, null, Now));
            Assert.Equal(reads, count.Reads);

            Assert.Equal(CursorStop.LapEnd, await rig.Catalog.Discover(DueRule.Count, Class2 | Class3, null, Now));
            Assert.Equal(new[] { 5, 6, 7, 8 }, count.Hits.Select(x => x.Id));
            rig.Catalog.Consume(DueRule.Fill, new HashSet<int>() { 5, 6 });
            Assert.Equal(new[] { 7, 8 }, count.Hits.Select(x => x.Id));
            Assert.Equal(new[] { 7, 8 }, fill.Hits.Select(x => x.Id));

            var fillPause = fill.PausedUntil;
            Assert.Equal(5, count.YoungFiles);
            Assert.Equal(CursorStop.Paused, await rig.Catalog.Discover(DueRule.Count, Class3, null, count.PausedUntil - 1));
            Assert.Equal(0, count.YoungFiles);
            Assert.Equal(new[] { 7, 8 }, count.Hits.Select(x => x.Id));
            Assert.Equal(new[] { 7, 8 }, fill.Hits.Select(x => x.Id));
            Assert.Equal(fillPause, fill.PausedUntil);
        }

        [Fact]
        public void TheThousandAndTwentyFifthDeclineSuspendsTheTable()
        {
            var logger = new DeltaMaintenanceWorkerTests.CollectingLogger();
            var declines = new CompactionDeclines(1_024, logger, "t");
            for (int i = 0; i < 1_024; i++)
            {
                declines.Decline(i, $"part-{i}.parquet", Record(i, 20_000), CompactionReason.Precision);
            }
            Assert.False(declines.Suspended);
            Assert.Equal(1_024, declines.Count);
            declines.Decline(5, "part-5.parquet", Record(5, 20_000), CompactionReason.InlineDv);
            Assert.False(declines.Suspended);
            Assert.Equal(CompactionReason.Precision, declines.ReasonOf(5));

            declines.Decline(1_024, "part-1024.parquet", Record(1_024, 20_000), CompactionReason.Precision);
            declines.Decline(1_025, "part-1025.parquet", Record(1_025, 20_000), CompactionReason.Precision);

            Assert.True(declines.Suspended);
            Assert.Equal(1_024, declines.Count);
            Assert.Single(logger.Entries, x => x.Level == LogLevel.Warning);
            declines.Removed(3, "other.parquet");
            Assert.Equal(1_024, declines.Count);
            declines.Removed(3, "part-3.parquet");
            Assert.Equal(1_023, declines.Count);
            Assert.Equal(1_023, declines.DeclinedFiles(2));
            Assert.Equal(1_023 * 20_000L, declines.DeclinedBytes(2));
            declines.Decline(5, "moved.parquet", Record(5, 20_000), CompactionReason.Precision);
            declines.Clear();
            Assert.False(declines.Suspended);
            Assert.Equal(0, declines.DeclinedFiles(2));
            Assert.Equal(0, declines.DeclinedBytes(2));
            for (int i = 0; i <= 1_024; i++)
            {
                declines.Decline(i, $"part-{i}.parquet", Record(i, 20_000), CompactionReason.Precision);
            }
            Assert.True(declines.Suspended);
            Assert.Single(logger.Entries, x => x.Level == LogLevel.Warning);
        }

        [Fact]
        public void ADeclineCountsOnlyItsOwnPathOnce()
        {
            var declines = new CompactionDeclines(4, new DeltaMaintenanceWorkerTests.CollectingLogger(), "t");
            var tagged = new DeltaFileRecord() { Path = "part-9.parquet", Size = 20_000, PartitionValues = new Dictionary<string, string>(), Tags = new Dictionary<string, string>() { ["t"] = "1" } };

            declines.Decline(1, "other.parquet", Record(1, 20_000), CompactionReason.Precision);
            declines.Decline(2, "part-2.parquet", Record(2, 20_000), CompactionReason.Precision);
            declines.Decline(2, "part-2.parquet", Record(2, 20_000), CompactionReason.InlineDv);
            declines.Decline(9, "part-9.parquet", tagged, CompactionReason.Precision);

            Assert.Equal(2, declines.Count);
            Assert.False(declines.Contains(1));
            Assert.True(declines.Contains(9));
            Assert.Equal(CompactionReason.Precision, declines.ReasonOf(2));
            Assert.Equal(1, declines.DeclinedFiles(2));
            Assert.Equal(20_000, declines.DeclinedBytes(2));
            Assert.Equal(1, Enumerable.Range(0, SizeClass.Counted).Sum(x => declines.DeclinedFiles(x)));
        }

        [Fact]
        public async Task ASuspendedTableDiscoversNothingUntilTheNextEpoch()
        {
            await using var rig = await Rig.Create("cursor_suspended", false, new CompactionLimits() { MemoEntries = 1 }, Enumerable.Range(0, 5).Select(x => File(x, 20_000, ColdAt)));
            var records = await rig.Catalog.GetFiles(new[] { 0, 1 });
            rig.Catalog.Declines!.Decline(0, "part-0.parquet", records[0], CompactionReason.Precision);
            rig.Catalog.Declines.Decline(1, "part-1.parquet", records[1], CompactionReason.Precision);

            Assert.Equal(CursorStop.Suspended, await rig.Catalog.Discover(DueRule.Count, Class2, null, Now));
            Assert.Equal(0, rig.Catalog.Cursor(DueRule.Count)!.Reads);
            await rig.Overwrite(Enumerable.Range(10, 3).Select(x => File(x, 20_000, ColdAt)));
            Assert.Equal(CursorStop.LapEnd, await rig.Catalog.Discover(DueRule.Count, Class2, null, Now));
            Assert.Equal(3, rig.Catalog.Cursor(DueRule.Count)!.Hits.Count);
        }

        [Fact]
        public async Task TheColdBacklogStaysWithinOneBinAtOneFilePerCommit()
        {
            const int Base = 5_000;
            await using var rig = await Rig.Create("cursor_sawtooth", false, new CompactionLimits(), Enumerable.Range(0, Base).Select(j => File(j, 20_000, (j + 1) * 1_000L)));
            var cursor = rig.Catalog.Cursor(DueRule.Count)!;
            var written = new Dictionary<int, long>();
            for (int j = 0; j < Base; j++)
            {
                written[j] = (j + 1) * 1_000L;
            }
            var key = Base;
            List<int>? pending = null;
            var bins = 0;
            var peak = 0;
            for (int i = 1; i <= 1_600; i++)
            {
                var now = (Base + i) * 1_000L;
                var adds = new List<DeltaAddAction>() { File(key++, 20_000, now) };
                if (pending != null)
                {
                    adds.Add(File(key++, 64L << 20, now));
                }
                var removes = pending ?? new List<int>();
                var (overlay, ids) = await rig.Stage(adds, removes);
                foreach (var id in removes)
                {
                    written.Remove(id);
                }
                written[ids[0]] = now;
                pending = null;
                if (i >= 900)
                {
                    var reads = cursor.Reads;
                    if (await rig.Catalog.Discover(DueRule.Count, Class2, removes.ToHashSet(), now) == CursorStop.Full)
                    {
                        var bin = cursor.Hits.Select(x => x.Id).ToList();
                        Assert.Equal(50, bin.Distinct().Count());
                        Assert.All(bin, x => Assert.True(written.ContainsKey(x) && !removes.Contains(x), $"commit {i}: bin id {x}"));
                        rig.Catalog.Consume(DueRule.Count, bin.ToHashSet());
                        pending = bin;
                        bins++;
                    }
                    Assert.True(cursor.Reads - reads <= 64, $"commit {i}: {cursor.Reads - reads} reads");
                }
                rig.Catalog.MarkPublished(overlay);
                await rig.Catalog.ApplyPublished();
                if (i >= 1_200)
                {
                    peak = Math.Max(peak, written.Count(x => x.Value <= now - 30 * Minute));
                }
            }

            Assert.True(bins >= 10, $"bins {bins}");
            Assert.True(peak <= 50, $"peak {peak}");
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task AGainedClassMidLapScansSkippedSlotsBeforePausing(bool treeMode)
        {
            var limits = treeMode
                ? new CompactionLimits() { ScanSlots = 100_000, ScanLeaves = 1 }
                : new CompactionLimits() { ScanSlots = 50 };
            var files = Enumerable.Range(0, 6).Select(x => File(x, 40_000, ColdAt))
                .Concat(Enumerable.Range(6, 194).Select(x => File(x, 64L << 20, ColdAt)));
            await using var rig = await Rig.Create($"cursor_gained_mid_lap_{treeMode}", treeMode, limits, files, pageBytes: treeMode ? 1024 : 32 * 1024);
            var fill = rig.Catalog.Cursor(DueRule.Fill)!;

            Assert.Equal(CursorStop.Budget, await rig.Catalog.Discover(DueRule.Fill, Class2, null, Now));
            Assert.True(fill.Position > 6, $"position {fill.Position}");

            for (int calls = 0; calls < 200 && fill.Hits.Count == 0; calls++)
            {
                var stop = await rig.Catalog.Discover(DueRule.Fill, Class3, null, Now);
                Assert.NotEqual(CursorStop.Paused, stop);
            }
            Assert.Equal(Enumerable.Range(0, 6), fill.Hits.Select(x => x.Id));
        }

        [Fact]
        public async Task AClassGainedMidLapIsScannedBeforeAPause()
        {
            var files = Enumerable.Range(0, 5).Select(x => File(x, 40_000, ColdAt))
                .Concat(Enumerable.Range(5, 5).Select(x => File(x, 20_000, ColdAt)))
                .ToList();
            await using var rig = await Rig.Create("cursor_gained", false, new CompactionLimits() { ScanSlots = 5 }, files);
            var count = rig.Catalog.Cursor(DueRule.Count)!;

            Assert.Equal(CursorStop.Budget, await rig.Catalog.Discover(DueRule.Count, Class2, null, Now));
            Assert.Equal(5, count.Position);

            for (int calls = 0; calls < 1_000 && count.Hits.Count < 10; calls++)
            {
                if (await rig.Catalog.Discover(DueRule.Count, Class2 | Class3, null, Now) == CursorStop.Paused)
                {
                    break;
                }
            }

            Assert.Equal(Enumerable.Range(0, 10), count.Hits.Select(x => x.Id).Order());
        }

        [Fact]
        public async Task ALapThatFormedABinDoesNotPause()
        {
            await using var rig = await Rig.Create("cursor_lap_bin", false, new CompactionLimits(), Enumerable.Range(0, 70).Select(x => File(x, 20_000, ColdAt)));
            var count = rig.Catalog.Cursor(DueRule.Count)!;

            Assert.Equal(CursorStop.Full, await rig.Catalog.Discover(DueRule.Count, Class2, null, Now));
            rig.Catalog.Consume(DueRule.Count, count.Hits.Select(x => x.Id).ToHashSet());
            Assert.Equal(CursorStop.LapEnd, await rig.Catalog.Discover(DueRule.Count, Class2, null, Now));

            Assert.Equal(20, count.Hits.Count);
            Assert.Equal(long.MinValue, count.PausedUntil);
            Assert.NotEqual(CursorStop.Paused, await rig.Catalog.Discover(DueRule.Count, Class2, null, Now));
        }

        [Fact]
        public async Task ARemovedYoungFileFreesItsIdForTheScan()
        {
            var files = new[] { File(0, 20_000, Now - 10 * Minute), File(1, 20_000, ColdAt), File(2, 20_000, ColdAt) };
            await using var rig = await Rig.Create("cursor_young_removed", false, new CompactionLimits(), files);
            var count = rig.Catalog.Cursor(DueRule.Count)!;

            Assert.Equal(CursorStop.LapEnd, await rig.Catalog.Discover(DueRule.Count, Class2, null, Now));
            Assert.Equal(1, count.YoungFiles);
            await rig.Commit(Array.Empty<DeltaAddAction>(), removes: new[] { 0 });
            Assert.Equal(0, count.YoungFiles);
            Assert.Equal(new[] { 0 }, await rig.Commit(new[] { File(100, 20_000, ColdAt) }));
            rig.Catalog.Consume(DueRule.Count, count.Hits.Select(x => x.Id).ToHashSet());

            Assert.Equal(CursorStop.LapEnd, await rig.Catalog.Discover(DueRule.Count, Class2, null, Now));

            Assert.Contains(count.Hits, x => x.Id == 0 && x.Record.Path == "part-100.parquet");
        }

        [Fact]
        public async Task AnEvictedYoungFileIsFoundOnceCold()
        {
            var files = Enumerable.Range(0, 8).Select(x => File(x, 20_000, Now - 10 * Minute - x * 1_000));
            await using var rig = await Rig.Create("cursor_evicted_young", false, new CompactionLimits() { Hits = 5 }, files);
            var count = rig.Catalog.Cursor(DueRule.Count)!;

            Assert.Equal(CursorStop.LapEnd, await rig.Catalog.Discover(DueRule.Count, Class2, null, Now));
            Assert.Equal(5, count.YoungFiles);
            Assert.Equal(CursorStop.Full, await rig.Catalog.Discover(DueRule.Count, Class2, null, Now + 30 * Minute));
            Assert.Equal(Enumerable.Range(3, 5), count.Hits.Select(x => x.Id).Order());
            await rig.Fold(DueRule.Count, Enumerable.Range(3, 5));

            Assert.Equal(CursorStop.LapEnd, await rig.Catalog.Discover(DueRule.Count, Class2, null, Now + 30 * Minute));
            Assert.Equal(new[] { 0, 1, 2 }, count.Hits.Select(x => x.Id).Order());
        }

        [Fact]
        public async Task RevalidationSharesTheCallsReadAllowance()
        {
            var files = Enumerable.Range(0, 100).Select(x => File(x, 20_000, Now - 10 * Minute));
            await using var rig = await Rig.Create("cursor_revalidate_budget", false, new CompactionLimits() { Hits = 120 }, files);
            var count = rig.Catalog.Cursor(DueRule.Count)!;
            await rig.Lap(DueRule.Count, Class2, null, Now, null);
            Assert.Equal(100, count.YoungFiles);
            await rig.Commit(Enumerable.Range(100, 50).Select(x => File(x, 20_000, ColdAt)));
            var reads = count.Reads;

            Assert.Equal(CursorStop.Budget, await rig.Catalog.Discover(DueRule.Count, Class2, null, Now + 20 * Minute));
            Assert.Equal(64, count.Reads - reads);
            Assert.Equal(CursorStop.Full, await rig.Catalog.Discover(DueRule.Count, Class2, null, Now + 20 * Minute));
            Assert.Equal(128, count.Reads - reads);
            Assert.Equal(120, count.Hits.Count);
        }

        [Fact]
        public async Task AClassZeroScanSkipsFreeSlots()
        {
            await using var rig = await Rig.Create("cursor_class_zero", false, new CompactionLimits(), Enumerable.Range(0, 10).Select(x => File(x, 4_000, ColdAt)));
            var count = rig.Catalog.Cursor(DueRule.Count)!;
            await rig.Commit(Array.Empty<DeltaAddAction>(), removes: new[] { 3, 7 });

            Assert.Equal(CursorStop.LapEnd, await rig.Catalog.Discover(DueRule.Count, 1 << 0, null, Now));

            Assert.Equal(8, count.Reads);
            Assert.Equal(new[] { 0, 1, 2, 4, 5, 6, 8, 9 }, count.Hits.Select(x => x.Id));
        }

        [Theory]
        [InlineData(100_000, 64)]
        [InlineData(10, 10)]
        public async Task ATreeSliceKeepsItsReadAndSlotBudgets(int slots, int expected)
        {
            var limits = new CompactionLimits() { ScanSlots = slots, Hits = 10_000 };
            await using var rig = await Rig.Create($"cursor_tree_budget_{slots}", true, limits, Enumerable.Range(0, 200).Select(x => File(x, 20_000, ColdAt)), pageBytes: 1024);
            var count = rig.Catalog.Cursor(DueRule.Count)!;

            Assert.Equal(CursorStop.Budget, await rig.Catalog.Discover(DueRule.Count, Class2, null, Now));

            Assert.Equal(expected, count.Reads);
            Assert.Equal(expected, count.Inspected);
            Assert.Equal(expected, count.Position);
        }

        [Fact]
        public async Task SuspensionDuringASliceStopsDiscoveryImmediately()
        {
            var files = Enumerable.Range(0, 5).Select(x => File(x, 20_000, ColdAt, x is 1 or 2 ? "i" : null));
            await using var rig = await Rig.Create("cursor_suspend_mid_slice", false, new CompactionLimits() { MemoEntries = 1 }, files);

            var stop = await rig.Catalog.Discover(DueRule.Count, Class2, null, Now);

            Assert.True(rig.Catalog.Declines!.Suspended);
            Assert.Equal(CursorStop.Suspended, stop);
            Assert.Empty(rig.Catalog.Cursor(DueRule.Count)!.Hits);
        }

        [Fact]
        public async Task ASuspensionWhileRevalidatingStopsTheCall()
        {
            var files = new[] { File(0, 20_000, ColdAt, "i"), File(1, 20_000, Now - 10 * Minute), File(2, 20_000, Now - 10 * Minute), File(3, 20_000, ColdAt) };
            await using var rig = await Rig.Create("cursor_suspend_revalidate", false, new CompactionLimits() { MemoEntries = 1, Hits = 2 }, files);
            var count = rig.Catalog.Cursor(DueRule.Count)!;
            Assert.Equal(CursorStop.LapEnd, await rig.Catalog.Discover(DueRule.Count, Class2, null, Now));
            Assert.Equal(1, rig.Catalog.Declines!.Count);
            Assert.Equal(new[] { 3 }, count.Hits.Select(x => x.Id));
            Assert.Equal(2, count.YoungFiles);
            await rig.Commit(Array.Empty<DeltaAddAction>(), updates: new[] { 1 }, vectorType: "i");

            var stop = await rig.Catalog.Discover(DueRule.Count, Class2, null, count.PausedUntil);

            Assert.True(rig.Catalog.Declines.Suspended);
            Assert.Equal(CursorStop.Suspended, stop);
            Assert.Empty(count.Hits);
        }

        [Fact]
        public async Task ExtremeForeignWriteTimeDoesNotOverflowTheLapPause()
        {
            await using var rig = await Rig.Create("cursor_extreme_written", false, new CompactionLimits(), new[] { File(0, 20_000, long.MaxValue) });

            Assert.Equal(CursorStop.LapEnd, await rig.Catalog.Discover(DueRule.Count, Class2, null, Now));
            Assert.Equal(Now + 30 * Minute, rig.Catalog.Cursor(DueRule.Count)!.PausedUntil);
            Assert.Equal(CursorStop.Paused, await rig.Catalog.Discover(DueRule.Count, Class2, null, Now));
        }

        [Fact]
        public async Task ALostClassTakesItsHeldHits()
        {
            var files = Enumerable.Range(0, 3).Select(x => File(x, 20_000, ColdAt))
                .Concat(Enumerable.Range(3, 3).Select(x => File(x, 40_000, ColdAt)));
            await using var rig = await Rig.Create("cursor_lost_hits", false, new CompactionLimits(), files);
            var count = rig.Catalog.Cursor(DueRule.Count)!;
            Assert.Equal(CursorStop.LapEnd, await rig.Catalog.Discover(DueRule.Count, Class2 | Class3, null, Now));
            Assert.Equal(Enumerable.Range(0, 6), count.Hits.Select(x => x.Id));

            Assert.Equal(CursorStop.Paused, await rig.Catalog.Discover(DueRule.Count, Class3, null, Now));

            Assert.Equal(new[] { 3, 4, 5 }, count.Hits.Select(x => x.Id));
        }

        [Fact]
        public async Task ASkippedIdLeavesTheHeldHits()
        {
            await using var rig = await Rig.Create("cursor_skip_hits", false, new CompactionLimits(), Enumerable.Range(0, 5).Select(x => File(x, 20_000, ColdAt)));
            var count = rig.Catalog.Cursor(DueRule.Count)!;
            Assert.Equal(CursorStop.LapEnd, await rig.Catalog.Discover(DueRule.Count, Class2, null, Now));

            Assert.Equal(CursorStop.Paused, await rig.Catalog.Discover(DueRule.Count, Class2, new HashSet<int>() { 2 }, Now));

            Assert.Equal(new[] { 0, 1, 3, 4 }, count.Hits.Select(x => x.Id));
        }

        [Fact]
        public async Task AYoungFileDeclinedMeanwhileIsNotAHit()
        {
            var files = new[] { File(0, 20_000, Now - 10 * Minute), File(1, 20_000, ColdAt) };
            await using var rig = await Rig.Create("cursor_young_declined", false, new CompactionLimits(), files);
            var count = rig.Catalog.Cursor(DueRule.Count)!;
            Assert.Equal(CursorStop.LapEnd, await rig.Catalog.Discover(DueRule.Count, Class2, null, Now));
            Assert.Equal(1, count.YoungFiles);
            rig.Catalog.Declines!.Decline(0, "part-0.parquet", (await rig.Catalog.GetFiles(new[] { 0 }))[0], CompactionReason.Precision);

            Assert.Equal(CursorStop.LapEnd, await rig.Catalog.Discover(DueRule.Count, Class2, null, count.PausedUntil));

            Assert.Equal(new[] { 1 }, count.Hits.Select(x => x.Id));
            Assert.Equal(0, count.YoungFiles);
        }

        [Fact]
        public async Task ASuspendedTableIsReportedBeforeAPauseOrARead()
        {
            await using var rig = await Rig.Create("cursor_suspended_paused", false, new CompactionLimits() { MemoEntries = 1 }, Enumerable.Range(0, 3).Select(x => File(x, 20_000, Now - 10 * Minute)));
            var count = rig.Catalog.Cursor(DueRule.Count)!;
            Assert.Equal(CursorStop.LapEnd, await rig.Catalog.Discover(DueRule.Count, Class2, null, Now));
            Assert.Equal(3, count.YoungFiles);
            var records = await rig.Catalog.GetFiles(new[] { 0, 1 });
            rig.Catalog.Declines!.Decline(0, "part-0.parquet", records[0], CompactionReason.Precision);
            rig.Catalog.Declines.Decline(1, "part-1.parquet", records[1], CompactionReason.Precision);
            var reads = count.Reads;

            Assert.Equal(CursorStop.Suspended, await rig.Catalog.Discover(DueRule.Count, Class2, null, Now));
            Assert.Equal(CursorStop.Suspended, await rig.Catalog.Discover(DueRule.Count, Class2, null, count.PausedUntil));

            Assert.Equal(reads, count.Reads);
        }

        [Fact]
        public async Task BufferedColdHitsAreRecheckedAfterAClockStepBack()
        {
            var limits = new CompactionLimits() { ScanSlots = 1, Hits = 2, QuietCommits = 1 };
            await using var rig = await Rig.Create("cursor_clock_step_back", false, limits, Array.Empty<DeltaAddAction>());
            var guard = rig.Catalog.Guard!;
            var now = guard.QuietSince + 120 * Minute;
            await rig.Commit(new[] { File(0, 20_000, now - 30 * Minute), File(1, 20_000, now - 60 * Minute) });
            guard.CommitStaged(true, guard.Sequence);
            var cursor = rig.Catalog.Cursor(DueRule.Count)!;

            Assert.True(guard.IsQuiet(now));
            Assert.Equal(CursorStop.Budget, await rig.Catalog.Discover(DueRule.Count, Class2, null, now));
            Assert.Equal(0, Assert.Single(cursor.Hits).Id);

            var steppedBack = now - Minute;
            Assert.True(guard.IsQuiet(steppedBack));
            await rig.Catalog.Discover(DueRule.Count, Class2, null, steppedBack);

            Assert.All(cursor.Hits, hit => Assert.True(hit.Record.ModificationTime <= steppedBack - 30 * Minute,
                $"File {hit.Id} was written at {hit.Record.ModificationTime}, newer than {steppedBack - 30 * Minute}"));
            Assert.Equal(1, Assert.Single(cursor.Hits).Id);
            Assert.Equal(1, cursor.YoungFiles);
        }

        [Fact]
        public async Task APassedPauseDoesNotReturnAfterAClockStepBack()
        {
            await using var rig = await Rig.Create("cursor_pause_passed", false, new CompactionLimits() { ScanSlots = 2 }, Enumerable.Range(0, 6).Select(x => File(x, 40_000, ColdAt)));
            var count = rig.Catalog.Cursor(DueRule.Count)!;
            await rig.Lap(DueRule.Count, Class2, null, Now, null);
            var pausedUntil = count.PausedUntil;
            Assert.Equal(Now + 30 * Minute, pausedUntil);

            Assert.Equal(CursorStop.Budget, await rig.Catalog.Discover(DueRule.Count, Class2, null, pausedUntil));
            Assert.Equal(long.MinValue, count.PausedUntil);

            Assert.Equal(CursorStop.Budget, await rig.Catalog.Discover(DueRule.Count, Class2, null, pausedUntil - Minute));
        }

        [Fact]
        public async Task APausedCallRechecksItsHitsAfterAClockStepBack()
        {
            await using var rig = await Rig.Create("cursor_paused_step_back", false, new CompactionLimits(), new[] { File(0, 20_000, Now - 31 * Minute) });
            var count = rig.Catalog.Cursor(DueRule.Count)!;
            Assert.Equal(CursorStop.LapEnd, await rig.Catalog.Discover(DueRule.Count, Class2, null, Now));
            Assert.Equal(0, Assert.Single(count.Hits).Id);

            Assert.Equal(CursorStop.Paused, await rig.Catalog.Discover(DueRule.Count, Class2, null, Now - 5 * Minute));

            Assert.Empty(count.Hits);
            Assert.Equal(1, count.YoungFiles);
        }

        [Fact]
        public async Task ASuspendedTableDropsTheHitsItHeld()
        {
            await using var rig = await Rig.Create("cursor_suspended_hits", false, new CompactionLimits() { MemoEntries = 1 }, Enumerable.Range(0, 3).Select(x => File(x, 20_000, ColdAt)));
            var count = rig.Catalog.Cursor(DueRule.Count)!;
            Assert.Equal(CursorStop.LapEnd, await rig.Catalog.Discover(DueRule.Count, Class2, null, Now));
            Assert.Equal(3, count.Hits.Count);
            var records = await rig.Catalog.GetFiles(new[] { 0, 1 });
            rig.Catalog.Declines!.Decline(0, "part-0.parquet", records[0], CompactionReason.Precision);
            rig.Catalog.Declines.Decline(1, "part-1.parquet", records[1], CompactionReason.Precision);

            Assert.Equal(CursorStop.Suspended, await rig.Catalog.Discover(DueRule.Count, Class2, null, Now));

            Assert.Empty(count.Hits);
        }

        // A catalog over a table made by its own create commit
        private sealed class Rig : IAsyncDisposable
        {
            private readonly IFileStorage _storage;
            private readonly StateManagerSync<object> _stateManager;
            private readonly ReservoirPersistentStorage _persistence;
            private long _version;
            private int _next = 10_000;

            private Rig(IFileStorage storage, StateManagerSync<object> stateManager, DeltaSinkCatalog catalog, ReservoirPersistentStorage persistence)
            {
                _storage = storage;
                _stateManager = stateManager;
                Catalog = catalog;
                _persistence = persistence;
            }

            public DeltaSinkCatalog Catalog { get; }

            public static async Task<Rig> Create(string name, bool treeMode, CompactionLimits limits, IEnumerable<DeltaAddAction> files, int pageBytes = 32 * 1024)
            {
                var storage = Files.Of.InternalMemory($"./{name}");
                Rig? rig = null;
                try
                {
                    var options = new DeltaLakeOptions() { StorageLocation = storage, ReservationOverride = new PruningReservation(), EnableAutoCompaction = true, CompactionLimitsOverride = limits };
                    if (treeMode)
                    {
                        options.PruningMemoryBytes = 1;
                    }
                    var (stateManager, catalog, persistence) = await CompactionCatalogTests.OpenCatalog(name, options, pageBytes);
                    rig = new Rig(storage, stateManager, catalog, persistence);
                    await catalog.Validate(new DeltaReadOptions() { SkipTombstones = true });
                    var create = catalog.BeginCommit(0, CompactionCatalogTests.NewTable());
                    foreach (var file in files)
                    {
                        create.Add(catalog.AllocateId(), file);
                    }
                    catalog.MarkPublished(create);
                    await catalog.ApplyPublished();
                    Assert.Equal(treeMode, catalog.TreeMode);
                    return rig;
                }
                catch
                {
                    // A failed setup still releases what it opened
                    if (rig != null)
                    {
                        await rig.DisposeAsync();
                    }
                    else
                    {
                        storage.Dispose();
                    }
                    throw;
                }
            }

            public async Task<(CatalogOverlay Overlay, List<int> Ids)> Stage(IEnumerable<DeltaAddAction> adds, IEnumerable<int> removes, IEnumerable<int>? updates = null, string vectorType = "u")
            {
                var overlay = Catalog.BeginCommit(++_version, null);
                var touched = removes.Concat(updates ?? Array.Empty<int>()).ToList();
                var records = await Catalog.GetFiles(touched);
                foreach (var id in removes)
                {
                    overlay.Remove(id, records[id]);
                }
                foreach (var id in updates ?? Array.Empty<int>())
                {
                    var add = records[id].ToAdd();
                    add.DeletionVector = new DeletionVector() { StorageType = vectorType, PathOrInlineDv = $"vector{id}", Offset = 1, SizeInBytes = 10, Cardinality = 1 };
                    overlay.Updates.Add((id, DeltaFileRecord.FromAdd(add, records[id].NumRecords)));
                }
                var ids = new List<int>();
                foreach (var add in adds)
                {
                    var id = Catalog.AllocateId();
                    overlay.Add(id, add);
                    ids.Add(id);
                }
                return (overlay, ids);
            }

            public async Task<List<int>> Commit(IEnumerable<DeltaAddAction> adds, IEnumerable<int>? removes = null, IEnumerable<int>? updates = null, string vectorType = "u")
            {
                var (overlay, ids) = await Stage(adds, removes ?? Array.Empty<int>(), updates, vectorType);
                Catalog.MarkPublished(overlay);
                await Catalog.ApplyPublished();
                return ids;
            }

            // A bin's inputs leave the cursors and then the table
            public async Task Fold(DueRule rule, IEnumerable<int> ids)
            {
                var bin = ids.ToList();
                Catalog.Consume(rule, bin.ToHashSet());
                await Commit(new[] { File(_next++, 64L << 20, ColdAt) }, removes: bin);
            }

            public async Task Overwrite(IEnumerable<DeltaAddAction> adds)
            {
                var overlay = Catalog.BeginCommit(++_version, null);
                overlay.RemoveAll = true;
                Catalog.RestartIds();
                foreach (var add in adds)
                {
                    overlay.Add(Catalog.AllocateId(), add);
                }
                Catalog.MarkPublished(overlay);
                await Catalog.ApplyPublished();
            }

            // Calls to the lap's end, reporting slots and leaves
            public async Task<int> Lap(DueRule rule, int wanted, IReadOnlySet<int>? skip, long now, Action<long, long>? perCall)
            {
                var cursor = Catalog.Cursor(rule)!;
                for (int calls = 1; calls < 10_000; calls++)
                {
                    var inspected = cursor.Inspected;
                    var leaves = cursor.Leaves;
                    var stop = await Catalog.Discover(rule, wanted, skip, now);
                    perCall?.Invoke(cursor.Inspected - inspected, cursor.Leaves - leaves);
                    if (stop == CursorStop.LapEnd)
                    {
                        return calls;
                    }
                    Assert.Equal(CursorStop.Budget, stop);
                }
                throw new InvalidOperationException("The lap never ended");
            }

            public ValueTask DisposeAsync()
            {
                Catalog.Dispose();
                _stateManager.Dispose();
                _persistence.Dispose();
                _storage.Dispose();
                return ValueTask.CompletedTask;
            }
        }

        private static DeltaAddAction File(int key, long size, long written, string? vectorType = null)
        {
            return new DeltaAddAction()
            {
                Path = $"part-{key}.parquet",
                PartitionValues = new Dictionary<string, string>(),
                Size = size,
                ModificationTime = written,
                DataChange = true,
                Statistics = $"{{\"numRecords\":10,\"minValues\":{{\"userkey\":{key}}},\"maxValues\":{{\"userkey\":{key}}},\"nullCount\":{{\"userkey\":0}}}}",
                DeletionVector = vectorType == null ? null : new DeletionVector() { StorageType = vectorType, PathOrInlineDv = "x", Offset = 1, SizeInBytes = 10, Cardinality = 1 }
            };
        }

        private static DeltaFileRecord Record(int key, long size)
        {
            return new DeltaFileRecord() { Path = $"part-{key}.parquet", Size = size, PartitionValues = new Dictionary<string, string>() };
        }
    }
}
