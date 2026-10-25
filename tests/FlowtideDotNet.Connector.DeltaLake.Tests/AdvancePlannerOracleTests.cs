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
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Actions;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.DeletionVectors;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Utils;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.Advance;
using Microsoft.Extensions.Time.Testing;
using Stowage;
using Stowage.Impl;
using Xunit.Abstractions;
using static FlowtideDotNet.Connector.DeltaLake.Tests.AdvancePlannerKit;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public class AdvancePlannerOracleTests : IDisposable
    {
        private readonly AdvancePlannerKit _kit = new AdvancePlannerKit();
        private readonly List<AdvancePlan> _plans = new List<AdvancePlan>();
        private readonly ITestOutputHelper _output;

        public AdvancePlannerOracleTests(ITestOutputHelper output)
        {
            _output = output;
        }

        public void Dispose()
        {
            foreach (var plan in _plans)
            {
                plan.Dispose();
            }
            _kit.Dispose();
        }

        private async Task<(AdvancePlan Plan, CleanupContext Context)> Plan(Table table, AdvanceBoundary boundary, bool longAdmission = true, AdvanceWindowLimits? limits = null,
            long readLimit = MaintenanceWorkerSettings.DefaultShortReads, CleanupContext? context = null, IFileStorage? storage = null)
        {
            // The files the log names exist, as in a written table
            await RetainedReferenceOracle.CreateReferencedFilesAsync(table.Native);
            context ??= _kit.Context(longAdmission, boundary.V, readLimit);
            var plan = await AdvancePlanner.PlanAsync(table.Configuration, storage ?? table.Local, table.Configuration.Store.Table, boundary, Classes(boundary.Log), context,
                limits ?? AdvanceWindowLimits.Default);
            _plans.Add(plan);
            if (plan.Status == AdvancePlanStatus.Planned)
            {
                AssertEmittedExist(table, plan);
            }
            return (plan, context);
        }

        private static Task<RetainedReferenceOracle.Result> Oracle(Table table, AdvanceBoundary boundary, bool tombstones = false)
        {
            var stages = Classes(boundary.Log).Stages.Select(x => x.Entry.Name);
            return RetainedReferenceOracle.ComputeAsync(table.Native, boundary.O, boundary.OldestRoot != null, tombstones, boundary.C, boundary.Head, stages);
        }

        // A declined or promoted plan holds nothing: no part, no scratch file and no memory
        private static void AssertNothingHeld(AdvancePlan plan, CleanupContext context)
        {
            Assert.Empty(plan.Parts);
            Assert.Empty(context.Scratch.Files);
            Assert.Equal(0, context.Memory.Managed);
            Assert.Equal(0, context.Memory.Native);
        }

        private static void WriteStage(Table table, long version, params string[] lines)
        {
            File.WriteAllLines(table.Log($".{version:D20}.json.{Guid.NewGuid():N}.tmp"), lines);
        }

        [Theory]
        [InlineData(1)]
        [InlineData(2)]
        [InlineData(3)]
        [InlineData(4)]
        [InlineData(5)]
        [InlineData(6)]
        [InlineData(7)]
        [InlineData(8)]
        [InlineData(9)]
        public async Task PlanEqualsFullUnionOracle(int seed)
        {
            // Inserts, overwrites, deletion vector churn, copy-on-write and CDC; O at 0, at a marked root or at an unmarked one
            var random = new Random(seed * 7919);
            var table = _kit.Local();
            var versions = random.Next(30, 70);
            var shape = seed % 3;
            long o = shape == 0 ? 0 : random.Next(3, versions / 3);
            long c = random.Next(Math.Max((int)o + 2, versions / 2), versions - 3);
            var snapshot = await WriteHistory(table, seed, versions, snapshotAt: shape == 2 ? o : null);
            if (shape == 1)
            {
                await _kit.Build(table, o);
            }
            await _kit.Build(table, c);
            if (random.Next(2) == 0)
            {
                await _kit.Build(table, random.Next((int)c + 1, versions));
            }
            if (shape == 2)
            {
                // Odd seeds root on the legacy writer's layout, which holds no tombstones
                if (seed % 2 == 1)
                {
                    await LegacyRoot(table, o);
                }
                else
                {
                    await _kit.UnmarkedRoot(table, o, snapshot!);
                }
            }
            if (shape != 0)
            {
                // For even seeds the root's own commit goes too
                Truncate(table, seed % 2 == 0 ? o + 1 : o);
            }
            if (random.Next(2) == 0)
            {
                WriteStage(table, versions, $$$"""{"add":{"path":"part-staged-{{{seed}}}.parquet","partitionValues":{},"size":1,"modificationTime":1,"dataChange":true}}""",
                    """{"remove":{"path":"part-000000-1.parquet","deletionTimestamp":1,"dataChange":true}}""");
            }
            var log = await Log(table);
            var boundary = Boundary(log, o, c, oldestMarked: shape != 2, step: AdvanceStep.Long);
            // Part bytes beyond a part record's own overhead
            var limits = new AdvanceWindowLimits() { WindowVersions = random.Next(2, 12), PartPaths = random.Next(1, 6), PartBytes = ControlRecords.PartLength(0, 0) + random.Next(128, 400) };

            var (plan, context) = await Plan(table, boundary, limits: limits);

            Assert.True(plan.Status == AdvancePlanStatus.Planned, $"{plan.Reason}: {plan.Detail}");
            var oracle = await Oracle(table, boundary, tombstones: shape == 2);
            var emitted = Emitted(plan, limits);
            _output.WriteLine($"O {o}, C {c}, head {log.Head}, {plan.Parts.Select(x => x.Window).Distinct().Count()} windows, {plan.Parts.Count} parts, {plan.Candidates} candidates, {plan.Protected} protected, {plan.Kept} kept, {plan.Emitted} emitted");
            Assert.Equal(oracle.Retired, new SortedSet<string>(emitted, StringComparer.Ordinal));
            Assert.NotEmpty(oracle.Retired);
            // Duplicates appear only across windows
            Assert.All(plan.Parts.GroupBy(x => x.Window), window => Assert.Equal(window.Sum(x => x.Paths), window.SelectMany(PathsOf).Distinct().Count()));
            Assert.Equal(AdvancePlanner.Reads(boundary, Classes(log)), context.ReadsUsed);
            Assert.True(plan.Destination!.GapFree);
            Assert.Equal(c, plan.Destination.Proof!.Version);

            plan.Dispose();
            Assert.Empty(context.Scratch.Files);
            Assert.Equal(0, context.Memory.Managed);
            Assert.Equal(0, context.Memory.Native);
        }

        [Fact]
        public async Task BoundaryCommitReferencesProtectChangeReplay()
        {
            var table = _kit.Local();
            var d1 = Uuid(Guid.NewGuid());
            var b = Add("part-b.parquet", d1).Add!;
            var bAgain = Add("part-b.parquet", Uuid(Guid.NewGuid())).Add!;
            var metadata = DeltaTestKit.Metadata(DeltaTestKit.UserSchema);
            await Commit(table, 0, 0, Create(), metadata, Add("part-a.parquet"), new DeltaAction() { Add = b });
            await Commit(table, 1, 0, Add("part-c.parquet"));
            await Commit(table, 2, 0, Remove("part-a.parquet", 2), Cdc("_change_data/cdc-2.parquet"));
            // C removes c and b's first vector, which change replay from C - 1 reads
            await Commit(table, 3, 0, Remove("part-c.parquet", 3), Cdc("_change_data/cdc-3.parquet"), Remove(b, 3), new DeltaAction() { Add = bAgain });
            await Commit(table, 4, 0, Add("part-e.parquet"));
            await _kit.Build(table, 3);
            var boundary = Boundary(await Log(table), 0, 3);

            var (plan, _) = await Plan(table, boundary, longAdmission: false);

            Assert.True(plan.Status == AdvancePlanStatus.Planned, $"{plan.Reason}: {plan.Detail}");
            var emitted = Emitted(plan, AdvanceWindowLimits.Default);
            Assert.Equal(new[] { "PART-A.PARQUET", "_CHANGE_DATA/CDC-2.PARQUET" }, emitted.OrderBy(x => x, StringComparer.Ordinal));
            Assert.Equal((await Oracle(table, boundary)).Retired, new SortedSet<string>(emitted, StringComparer.Ordinal));
            // c and the first vector of b are candidates only C's commit protects
            var d1File = Upper($"deletion_vector_{Z85.DecodeToGuid(d1.PathOrInlineDv!):D}.bin");
            Assert.DoesNotContain("PART-C.PARQUET", emitted);
            Assert.DoesNotContain(d1File, emitted);
            Assert.Equal(3, plan.Protected);
        }

        [Fact]
        public async Task LegacyUnmarkedRootTombstonesIncludedOnce()
        {
            var table = _kit.Local();
            var snapshot = await WriteHistory(table, 31, 26, snapshotAt: 12);
            await _kit.Build(table, 13);
            await _kit.Build(table, 20);
            await _kit.UnmarkedRoot(table, 12, snapshot!);
            Truncate(table, 12);
            var log = await Log(table);
            var limits = new AdvanceWindowLimits() { WindowVersions = 3, PartPaths = 4 };
            var tombstoneNames = new RetainedReferenceOracleNames(snapshot!.Where(x => x.Remove != null).Select(x => x.Remove!)).Names;

            // Unmarked: every retired tombstone file is a candidate once, in the first window only
            var unmarked = Boundary(log, 12, 20, oldestMarked: false, step: AdvanceStep.Long);
            var (plan, context) = await Plan(table, unmarked, limits: limits);
            Assert.True(plan.Status == AdvancePlanStatus.Planned, $"{plan.Reason}: {plan.Detail}");
            var emitted = Emitted(plan, limits);
            var oracle = await Oracle(table, unmarked, tombstones: true);
            Assert.Equal(oracle.Retired, new SortedSet<string>(emitted, StringComparer.Ordinal));
            var retiredTombstones = tombstoneNames.Where(oracle.Retired.Contains).ToList();
            Assert.True(retiredTombstones.Count >= 3, $"Only {retiredTombstones.Count} tombstone files retire");
            Assert.All(retiredTombstones, name => Assert.Single(emitted, name));
            Assert.Contains(plan.Parts, x => x.Window > 0);

            // Marked: an earlier advance already took them
            var marked = Boundary(log, 12, 20, oldestMarked: true, step: AdvanceStep.Long);
            var (markedPlan, markedContext) = await Plan(table, marked, limits: limits);
            Assert.True(markedPlan.Status == AdvancePlanStatus.Planned, $"{markedPlan.Reason}: {markedPlan.Detail}");
            var markedEmitted = Emitted(markedPlan, limits);
            Assert.Equal((await Oracle(table, marked)).Retired, new SortedSet<string>(markedEmitted, StringComparer.Ordinal));
            Assert.DoesNotContain(markedEmitted, retiredTombstones.Contains);
            Assert.Equal(context.ReadsUsed - 1, markedContext.ReadsUsed);

            // A root right below C without its own commit: no commit is a candidate, the tombstones still are
            File.Delete(table.Log($"{12:D20}.json"));
            var adjacentLog = await Log(table);
            var adjacent = Boundary(adjacentLog, 12, 13, oldestMarked: false, step: AdvanceStep.Long);
            var (adjacentPlan, adjacentContext) = await Plan(table, adjacent, limits: limits);
            Assert.True(adjacentPlan.Status == AdvancePlanStatus.Planned, $"{adjacentPlan.Reason}: {adjacentPlan.Detail}");
            var adjacentEmitted = Emitted(adjacentPlan, limits);
            Assert.Equal((await Oracle(table, adjacent, tombstones: true)).Retired, new SortedSet<string>(adjacentEmitted, StringComparer.Ordinal));
            Assert.Contains(adjacentEmitted, tombstoneNames.Contains);
            Assert.Equal(AdvancePlanner.Reads(adjacent, Classes(adjacentLog)), adjacentContext.ReadsUsed);
            // Marked, the same range has nothing to offer
            var (emptyPlan, emptyContext) = await Plan(table, adjacent with { OldestRootMarked = true }, limits: limits);
            Assert.True(emptyPlan.Status == AdvancePlanStatus.Planned, $"{emptyPlan.Reason}: {emptyPlan.Detail}");
            Assert.Empty(emptyPlan.Parts);
            Assert.Equal(0, emptyPlan.Candidates);
            Assert.Equal(adjacentContext.ReadsUsed - 1, emptyContext.ReadsUsed);
        }

        [Fact]
        public async Task VanishedStageDeclinesTransiently()
        {
            var table = _kit.Local();
            await WriteHistory(table, 19, 12);
            await _kit.Build(table, 8);
            WriteStage(table, 12, """{"add":{"path":"part-staged.parquet","partitionValues":{},"size":1,"modificationTime":1,"dataChange":true}}""");
            var boundary = Boundary(await Log(table), 0, 8);
            Assert.Single(Classes(boundary.Log).Stages);
            // Published or abandoned after the listing
            foreach (var stage in Directory.GetFiles(Path.Combine(table.Native, "_delta_log"), "*.tmp"))
            {
                File.Delete(stage);
            }

            var (plan, context) = await Plan(table, boundary, longAdmission: false);

            Assert.Equal(AdvancePlanStatus.Declined, plan.Status);
            Assert.Equal(MaintenanceReason.Transient, plan.Reason);
            AssertNothingHeld(plan, context);
        }

        [Fact]
        public async Task MissingCandidateRangeDeclines()
        {
            // A commit inside [O, C) the listing does not show
            var table = _kit.Local();
            await WriteHistory(table, 5, 20);
            await _kit.Build(table, 15);
            File.Delete(table.Log($"{7:D20}.json"));
            var (gap, gapContext) = await Plan(table, Boundary(await Log(table), 0, 15), longAdmission: false);
            Assert.Equal(AdvancePlanStatus.Declined, gap.Status);
            Assert.Equal(MaintenanceReason.MissingHistory, gap.Reason);
            Assert.Equal(0, gapContext.ReadsUsed);
            AssertNothingHeld(gap, gapContext);

            // A listed commit gone before its read
            var gone = _kit.Local();
            await WriteHistory(gone, 5, 20);
            await _kit.Build(gone, 15);
            var goneBoundary = Boundary(await Log(gone), 0, 15);
            File.Delete(gone.Log($"{9:D20}.json"));
            var (vanished, vanishedContext) = await Plan(gone, goneBoundary, longAdmission: false);
            Assert.Equal(AdvancePlanStatus.Declined, vanished.Status);
            Assert.Equal(MaintenanceReason.HistoryChanged, vanished.Reason);
            AssertNothingHeld(vanished, vanishedContext);

            // A listed commit whose length changed before its read
            var grown = _kit.Local();
            await WriteHistory(grown, 5, 20);
            await _kit.Build(grown, 15);
            var grownBoundary = Boundary(await Log(grown), 0, 15);
            File.AppendAllText(grown.Log($"{4:D20}.json"), "\n");
            var (changed, changedContext) = await Plan(grown, grownBoundary, longAdmission: false);
            Assert.Equal(AdvancePlanStatus.Declined, changed.Status);
            Assert.Equal(MaintenanceReason.HistoryChanged, changed.Reason);
            AssertNothingHeld(changed, changedContext);
        }

        [Fact]
        public async Task UnlistedBoundaryRootOrTailDeclinesBeforeAnyRead()
        {
            var table = _kit.Local();
            await WriteHistory(table, 23, 20);
            await _kit.Build(table, 10);
            await _kit.Build(table, 15);
            var boundary = Boundary(await Log(table), 0, 10);
            var cases = new List<(AdvanceBoundary Boundary, MaintenanceReason Reason)>()
            {
                // Not a range
                (boundary with { O = 10 }, MaintenanceReason.Unexpected),
                // Adoption after C
                (boundary with { A = 11 }, MaintenanceReason.Unexpected),
                // An unmarked oldest root the boundary does not name
                (boundary with { O = 5, OldestRoot = null, OldestRootMarked = false }, MaintenanceReason.MissingHistory)
            };
            // C's own commit is not listed
            File.Delete(table.Log($"{10:D20}.json"));
            cases.Add((Boundary(await Log(table), 0, 10), MaintenanceReason.MissingHistory));
            // A commit after the newest checkpoint is not listed
            var tail = _kit.Local();
            await WriteHistory(tail, 23, 20);
            await _kit.Build(tail, 10);
            await _kit.Build(tail, 15);
            File.Delete(tail.Log($"{17:D20}.json"));
            var tailBoundary = Boundary(await Log(tail), 0, 10);

            foreach (var (bad, reason) in cases)
            {
                var (plan, context) = await Plan(table, bad, longAdmission: false);
                Assert.Equal(AdvancePlanStatus.Declined, plan.Status);
                Assert.Equal(reason, plan.Reason);
                Assert.Equal(0, context.ReadsUsed);
                AssertNothingHeld(plan, context);
            }
            var (tailPlan, tailContext) = await Plan(tail, tailBoundary, longAdmission: false);
            Assert.Equal(AdvancePlanStatus.Declined, tailPlan.Status);
            Assert.Equal(MaintenanceReason.MissingHistory, tailPlan.Reason);
            Assert.Equal(0, tailContext.ReadsUsed);
            AssertNothingHeld(tailPlan, tailContext);
        }

        [Fact]
        public async Task PhysicalAliasesNeverProduceFalseDifference()
        {
            var table = _kit.Local();
            var id = Guid.NewGuid();
            var vector = $"ab/deletion_vector_{id:D}.bin";
            foreach (var name in new[] { "part-a.parquet", "part-b.parquet", "part-c.parquet", "part-d.parquet", "part-e.parquet", vector })
            {
                var path = Path.Combine(table.Native, name.Replace('/', Path.DirectorySeparatorChar));
                Directory.CreateDirectory(Path.GetDirectoryName(path)!);
                File.WriteAllText(path, "x");
            }
            var forward = table.Native.Replace('\\', '/');
            string Absolute(string relative) => (OperatingSystem.IsWindows() ? "file:///" : "file://") + forward + "/" + relative;
            var uuidVector = Uuid(id, "ab");
            var absoluteVector = new DeletionVector() { StorageType = "p", PathOrInlineDv = Absolute(vector), Offset = 1, SizeInBytes = 10, Cardinality = 2 };
            await Commit(table, 0, 0, Create(), DeltaTestKit.Metadata(DeltaTestKit.UserSchema), Add("part-a.parquet"), Add("part%2Db.parquet"), Add("part-c.parquet"),
                Add("part-d.parquet", uuidVector), Add("part-e.parquet"));
            // Each live file is named again in another spelling: case, escape, absolute path and an absolute vector path
            await Commit(table, 1, 0,
                Remove("part-a.parquet", 1), Add("PART-A.parquet"),
                Remove("part%2Db.parquet", 1), Add("part-b.parquet"),
                Remove("part-c.parquet", 1), Add(Absolute("part-c.parquet")),
                Remove("part-d.parquet", 1, uuidVector), Add("part-d.parquet", absoluteVector),
                Remove("part-e.parquet", 1));
            await Commit(table, 2, 0, Add("part-f.parquet"));
            await _kit.Build(table, 2);
            var boundary = Boundary(await Log(table), 0, 2);

            var (plan, _) = await Plan(table, boundary, longAdmission: false);

            Assert.True(plan.Status == AdvancePlanStatus.Planned, $"{plan.Reason}: {plan.Detail}");
            var emitted = Emitted(plan, AdvanceWindowLimits.Default);
            Assert.Equal(new[] { "PART-E.PARQUET" }, emitted);
            Assert.Equal((await Oracle(table, boundary)).Retired, new SortedSet<string>(emitted, StringComparer.Ordinal));
        }

        [Fact]
        public async Task VectorsAtOtherOffsetsOfOneVectorFileAreOtherFiles()
        {
            // A writer before adoption moves a's vector within its vector file, the add before the remove
            var table = _kit.Local();
            var first = Uuid(Guid.NewGuid());
            var moved = At(first, 100);
            await Commit(table, 0, 2, Create(), DeltaTestKit.Metadata(DeltaTestKit.UserSchema), Add("part-a.parquet", first), Add("part-c.parquet"));
            await Commit(table, 1, 2, Add("part-a.parquet", moved), Remove("part-a.parquet", 1, first), Remove("part-c.parquet", 1));
            await Commit(table, 2, 2, Add("part-b.parquet"));
            await _kit.Build(table, 2);
            var boundary = Boundary(await Log(table), 0, 2, a: 2);

            var (plan, _) = await Plan(table, boundary, longAdmission: false);

            Assert.True(plan.Status == AdvancePlanStatus.Planned, $"{plan.Reason}: {plan.Detail}");
            // a and its vector file stay live at C
            var emitted = Emitted(plan, AdvanceWindowLimits.Default);
            Assert.Equal(new[] { "PART-C.PARQUET" }, emitted);
            Assert.Equal((await Oracle(table, boundary)).Retired, new SortedSet<string>(emitted, StringComparer.Ordinal));
        }

        [Fact]
        public async Task UpgradedTablePlansFirstLongAdvance()
        {
            // History from before adoption at 70, a repair checkpoint at 80 and a newer one at 90
            var table = _kit.Local();
            await WriteHistory(table, 17, 100, adoptedAt: 70);
            await _kit.Build(table, 80);
            await _kit.Build(table, 90);
            var log = await Log(table);
            var boundary = Boundary(log, 0, 80, a: 70, step: AdvanceStep.Long, cap: 30);
            var limits = new AdvanceWindowLimits() { WindowVersions = 21 };

            // Short admission promotes before any counted read
            var (promoted, promotedContext) = await Plan(table, boundary, longAdmission: false, limits: limits);
            Assert.Equal(AdvancePlanStatus.NeedsLongAdmission, promoted.Status);
            Assert.Equal(0, promotedContext.ReadsUsed);
            AssertNothingHeld(promoted, promotedContext);
            // So does a short step whose reads exceed what short admission has
            var (overBudget, overBudgetContext) = await Plan(table, boundary with { Step = AdvanceStep.Short }, longAdmission: false, limits: limits, readLimit: 50);
            Assert.Equal(AdvancePlanStatus.NeedsLongAdmission, overBudget.Status);
            Assert.Equal(0, overBudgetContext.ReadsUsed);

            // Long admission plans [0, 80) in four windows
            var (plan, context) = await Plan(table, boundary, limits: limits);
            Assert.True(plan.Status == AdvancePlanStatus.Planned, $"{plan.Reason}: {plan.Detail}");
            Assert.Equal(new[] { 0, 1, 2, 3 }, plan.Parts.Select(x => x.Window).Distinct());
            var emitted = Emitted(plan, limits);
            Assert.Equal((await Oracle(table, boundary)).Retired, new SortedSet<string>(emitted, StringComparer.Ordinal));
            Assert.Equal(AdvancePlanner.Reads(boundary, Classes(log)), context.ReadsUsed);
            Assert.Equal(80 + 2 + 1 + 9, context.ReadsUsed);
        }

        [Fact]
        public async Task NewestCheckpointAndTailProtectAsDefenseInDepth()
        {
            // Files retired before C come back after it, which our writer never does
            var table = _kit.Local();
            var metadata = DeltaTestKit.Metadata(DeltaTestKit.UserSchema);
            await Commit(table, 0, 0, Create(), metadata, Add("part-f.parquet"), Add("part-g.parquet"), Add("part-h.parquet"));
            await Commit(table, 1, 0, Remove("part-f.parquet", 1), Remove("part-g.parquet", 1), Remove("part-h.parquet", 1));
            await Commit(table, 2, 0, Add("part-x.parquet"));
            await Commit(table, 3, 0, Add("part-g.parquet"));
            await Commit(table, 4, 0, Add("part-y.parquet"));
            await Commit(table, 5, 0, Add("part-f.parquet"));
            await _kit.Build(table, 2);
            await _kit.Build(table, 4);
            // One commit after the newest checkpoint, within an interval of one
            var boundary = Boundary(await Log(table), 0, 2, interval: 1);

            var (plan, context) = await Plan(table, boundary, longAdmission: false);

            Assert.True(plan.Status == AdvancePlanStatus.Planned, $"{plan.Reason}: {plan.Detail}");
            // g is live at the newest checkpoint 4, f is named by the tail after it
            Assert.Equal(new[] { "PART-H.PARQUET" }, Emitted(plan, AdvanceWindowLimits.Default));
            Assert.Equal((await Oracle(table, boundary)).Retired, new SortedSet<string>(Emitted(plan, AdvanceWindowLimits.Default), StringComparer.Ordinal));
            Assert.Equal(2 + 2 + 1 + 1, context.ReadsUsed);
        }

        [Theory]
        [InlineData(null, 0)]
        [InlineData(40L, 0)]
        [InlineData(70L, 1 + 9)]
        public async Task TailIsReadOnlyAfterANewerCheckpointWithinTheInterval(long? newer, long newestReads)
        {
            // C is the newest checkpoint, or the newer one is past the interval of 20 commits from the head, or within it
            var table = _kit.Local();
            await WriteHistory(table, 29, 80);
            await _kit.Build(table, 10);
            if (newer is { } version)
            {
                await _kit.Build(table, version);
            }
            var log = await Log(table);
            var boundary = Boundary(log, 0, 10);

            var (plan, context) = await Plan(table, boundary, longAdmission: false);

            Assert.True(plan.Status == AdvancePlanStatus.Planned, $"{plan.Reason}: {plan.Detail}");
            // The candidates, C and C.json, then the newer checkpoint and the commits after it only within the interval
            Assert.Equal(10 + 2 + newestReads, context.ReadsUsed);
            Assert.Equal(AdvancePlanner.Reads(boundary, Classes(log)), context.ReadsUsed);
            Assert.Equal((await Oracle(table, boundary)).Retired, new SortedSet<string>(Emitted(plan, AdvanceWindowLimits.Default), StringComparer.Ordinal));
        }

        [Fact]
        public async Task ProtectionRejectionDeclinesThePlan()
        {
            var table = _kit.Local();
            await WriteHistory(table, 3, 12);
            await _kit.Build(table, 8);
            // A stage naming a file outside the table leaves protection unresolved
            var outside = (OperatingSystem.IsWindows() ? "file:///" : "file://") + Path.Combine(Path.GetTempPath(), "elsewhere").Replace('\\', '/') + "/x.parquet";
            WriteStage(table, 12, $$$"""{"add":{"path":"{{{outside}}}","partitionValues":{},"size":1,"modificationTime":1,"dataChange":true}}""");

            var (plan, context) = await Plan(table, Boundary(await Log(table), 0, 8), longAdmission: false);

            Assert.Equal(AdvancePlanStatus.Declined, plan.Status);
            Assert.Equal(MaintenanceReason.PathRejected, plan.Reason);
            AssertNothingHeld(plan, context);
        }

        [Fact]
        public async Task UnusableReferenceOfTheNewerCheckpointDeclinesThePlan()
        {
            var table = _kit.Local();
            var outside = (OperatingSystem.IsWindows() ? "file:///" : "file://") + Path.Combine(Path.GetTempPath(), "elsewhere").Replace('\\', '/') + "/x.parquet";
            await Commit(table, 0, 0, Create(), DeltaTestKit.Metadata(DeltaTestKit.UserSchema), Add("part-a.parquet"));
            await Commit(table, 1, 0, Remove("part-a.parquet", 1), Add("part-b.parquet"));
            await Commit(table, 2, 0, Add("part-c.parquet"));
            // Only the newest checkpoint, with one commit after it, names a file outside the table
            await Commit(table, 3, 0, Add(outside));
            await Commit(table, 4, 0, Add("part-d.parquet"));
            await _kit.Build(table, 2);
            await _kit.Build(table, 3);
            var boundary = Boundary(await Log(table), 0, 2);
            Assert.Equal(3, AdvancePlanner.NewestAbove(boundary)!.Version);

            var (plan, context) = await Plan(table, boundary, longAdmission: false);

            Assert.Equal(AdvancePlanStatus.Declined, plan.Status);
            Assert.Equal(MaintenanceReason.PathRejected, plan.Reason);
            AssertNothingHeld(plan, context);
            // C, C.json and the newer checkpoint; the commit after it is not read
            Assert.Equal(2 + 1, context.ReadsUsed);
        }

        [Fact]
        public async Task UnusableTombstoneOfTheUnmarkedOldestRootIsKeptAndCounted()
        {
            var table = _kit.Local();
            var outside = (OperatingSystem.IsWindows() ? "file:///" : "file://") + Path.Combine(Path.GetTempPath(), "elsewhere").Replace('\\', '/') + "/x.parquet";
            await Commit(table, 0, 0, Create(), DeltaTestKit.Metadata(DeltaTestKit.UserSchema), Add("part-a.parquet"));
            await Commit(table, 1, 0, Remove("part-a.parquet", 1), Remove(outside, 1), Add("part-b.parquet"));
            await Commit(table, 2, 0, Add("part-c.parquet"));
            await _kit.Build(table, 2);
            // An unmarked root at 1 whose tombstones name a file outside the table, right below C and without its own commit
            await _kit.UnmarkedRoot(table, 1, new[] { Create(), DeltaTestKit.Metadata(DeltaTestKit.UserSchema), Remove("part-a.parquet", 1), Remove(outside, 1), Add("part-b.parquet") });
            Truncate(table, 2);
            var boundary = Boundary(await Log(table), 1, 2, oldestMarked: false, step: AdvanceStep.Long);

            var (plan, context) = await Plan(table, boundary);

            // A candidate the planner cannot name is kept, never a decline
            Assert.True(plan.Status == AdvancePlanStatus.Planned, $"{plan.Reason}: {plan.Detail}");
            Assert.Equal(new[] { "PART-A.PARQUET" }, Emitted(plan, AdvanceWindowLimits.Default));
            Assert.Equal((await Oracle(table, boundary, tombstones: true)).Retired, new SortedSet<string>(Emitted(plan, AdvanceWindowLimits.Default), StringComparer.Ordinal));
            Assert.Equal(1, plan.Kept);
            Assert.Equal(AdvancePlanner.Reads(boundary, Classes(boundary.Log)), context.ReadsUsed);
        }

        [Fact]
        public async Task TargetsTheExecutorRefusesAreKeptNotEmitted()
        {
            var table = _kit.Local();
            await Commit(table, 0, 0, Create(), DeltaTestKit.Metadata(DeltaTestKit.UserSchema), Add("_delta_log/stray.parquet"), Add("part-a.parquet"));
            await Commit(table, 1, 0, Remove("_delta_log/stray.parquet", 1), Remove("part-a.parquet", 1));
            await Commit(table, 2, 0, Add("part-b.parquet"));
            await _kit.Build(table, 2);

            var (plan, _) = await Plan(table, Boundary(await Log(table), 0, 2), longAdmission: false);

            Assert.True(plan.Status == AdvancePlanStatus.Planned, $"{plan.Reason}: {plan.Detail}");
            // A payload inside the log is never a target
            Assert.Equal(new[] { "PART-A.PARQUET" }, Emitted(plan, AdvanceWindowLimits.Default));
            Assert.Equal(1, plan.Kept);
            Assert.Equal(2, plan.Candidates);
        }

        [Fact]
        public async Task GappedDestinationDeclines()
        {
            // C built over an unmarked root inherits its gap
            var table = _kit.Local();
            var snapshot = await WriteHistory(table, 9, 20, snapshotAt: 8);
            await _kit.UnmarkedRoot(table, 8, snapshot!);
            await _kit.Build(table, 15);

            var (plan, context) = await Plan(table, Boundary(await Log(table), 0, 15), longAdmission: false);

            Assert.Equal(AdvancePlanStatus.Declined, plan.Status);
            Assert.Equal(MaintenanceReason.HistoryChanged, plan.Reason);
            AssertNothingHeld(plan, context);
        }

        [Fact]
        public async Task PlanOverItsPartBoundDeclinesAndLeavesNoParts()
        {
            var table = _kit.Local();
            await WriteHistory(table, 11, 30);
            await _kit.Build(table, 25);
            var boundary = Boundary(await Log(table), 0, 25);
            Assert.True((await Oracle(table, boundary)).Retired.Count > 1);

            var (plan, context) = await Plan(table, boundary, longAdmission: false, limits: new AdvanceWindowLimits() { PartPaths = 1, MaxParts = 1 });

            Assert.Equal(AdvancePlanStatus.Declined, plan.Status);
            Assert.Equal(MaintenanceReason.EntriesOverCap, plan.Reason);
            AssertNothingHeld(plan, context);
        }

        [Fact]
        public async Task StoppedPlanningDeclinesOrCancels()
        {
            var table = _kit.Local();
            await WriteHistory(table, 13, 12);
            await _kit.Build(table, 8);
            var boundary = Boundary(await Log(table), 0, 8);

            // Past the deadline: a decline, nothing held
            var clock = new FakeTimeProvider(Now);
            var late = _kit.Context(false, boundary.V, clock: clock);
            clock.Advance(TimeSpan.FromHours(1));
            var (plan, _) = await Plan(table, boundary, context: late);
            Assert.Equal(AdvancePlanStatus.Declined, plan.Status);
            Assert.Equal(MaintenanceReason.JobDeadline, plan.Reason);
            AssertNothingHeld(plan, late);

            // The worker stopping cancels
            using var stop = new CancellationTokenSource();
            stop.Cancel();
            var stopped = _kit.Context(false, boundary.V, stop: stop.Token);
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => Plan(table, boundary, context: stopped));
            Assert.Empty(stopped.Scratch.Files);
            Assert.Equal(0, stopped.Memory.Managed);
        }

        [Theory]
        [InlineData(false, false, 1)]
        [InlineData(false, false, 8)]
        [InlineData(false, true, 1)]
        [InlineData(false, true, 8)]
        [InlineData(true, false, 1)]
        [InlineData(true, false, 8)]
        [InlineData(true, true, 1)]
        [InlineData(true, true, 8)]
        public async Task PlanningStoppedMidwayHoldsNothingAndPlansAgain(bool cancel, bool afterParts, int readAhead)
        {
            var table = _kit.Local();
            await WriteHistory(table, 37, 30);
            await _kit.Build(table, 25);
            var boundary = Boundary(await Log(table), 0, 25, step: AdvanceStep.Long);
            var limits = new AdvanceWindowLimits() { WindowVersions = 5, PartPaths = 1 };
            var clock = new FakeTimeProvider(Now);
            using var stop = new CancellationTokenSource();
            var context = _kit.Context(true, boundary.V, stop: stop.Token, clock: clock, readAhead: readAhead);
            // The first candidate commit, read once protection is sealed, or the first of the third window, read once two windows emitted their parts
            var stopVersion = afterParts ? 10 : 0;
            var stopAt = $"{stopVersion:D20}.json";
            var storage = TrackedHookFileStorage.Over(table.Local);
            List<string>? held = null;
            var openedAtStop = -1;
            storage.Before = (verb, path) =>
            {
                if (verb == "OpenRead" && path.Name == stopAt && held == null)
                {
                    held = context.Scratch.Files.ToList();
                    openedAtStop = storage.Opened.Count;
                    if (cancel)
                    {
                        stop.Cancel();
                    }
                    else
                    {
                        clock.Advance(TimeSpan.FromHours(7));
                    }
                }
                return Task.CompletedTask;
            };
            Task<AdvancePlan> Stopped() => AdvancePlanner.PlanAsync(table.Configuration, storage, table.Configuration.Store.Table, boundary, Classes(boundary.Log), context, limits);

            if (cancel)
            {
                await Assert.ThrowsAnyAsync<OperationCanceledException>(Stopped);
            }
            else
            {
                var plan = await Stopped();
                _plans.Add(plan);
                Assert.Equal(AdvancePlanStatus.Declined, plan.Status);
                Assert.Equal(MaintenanceReason.JobDeadline, plan.Reason);
                Assert.Empty(plan.Parts);
            }

            // Stopped once protection spilled, and after parts when they were due
            Assert.NotNull(held);
            Assert.NotEmpty(held);
            Assert.Equal(afterParts, held.Any(x => x.EndsWith(".part", StringComparison.Ordinal)));
            Assert.Empty(context.Scratch.Files);
            Assert.Equal(0, context.Memory.Managed);
            Assert.Equal(0, context.Memory.Native);
            // Nothing above the stop was opened once it was seen
            Assert.All(storage.Opened.Skip(openedAtStop), version => Assert.True(version <= stopVersion, $"Commit {version} was opened after the stop at {stopVersion}"));
            Assert.Equal(storage.Returned, storage.Disposed);
            Assert.Equal(0, context.Transfer.Charged);

            // A retry plans what the oracle retires
            var (retry, retryContext) = await Plan(table, boundary, limits: limits);
            Assert.True(retry.Status == AdvancePlanStatus.Planned, $"{retry.Reason}: {retry.Detail}");
            Assert.Equal((await Oracle(table, boundary)).Retired, new SortedSet<string>(Emitted(retry, limits), StringComparer.Ordinal));
            Assert.Equal(AdvancePlanner.Reads(boundary, Classes(boundary.Log)), retryContext.ReadsUsed);
        }

        [Fact]
        public async Task LongPlanKeepsOpensInFlightUpToTheReadAhead()
        {
            var table = _kit.Local();
            await WriteHistory(table, 23, 40);
            await _kit.Build(table, 30);
            var log = await Log(table);
            var boundary = Boundary(log, 0, 30, step: AdvanceStep.Long);
            var classes = Classes(log);
            await RetainedReferenceOracle.CreateReferencedFilesAsync(table.Native);
            var storage = TrackedHookFileStorage.Over(table.Local);
            var rounds = new OpenRounds();
            // The candidate commits below C
            storage.Before = rounds.Hook(path => DeltaTransactionReader.TryGetCommitVersion(path.Name, out var version) && version < 30);
            var context = _kit.Context(true, boundary.V, readAhead: 8);

            var planning = AdvancePlanner.PlanAsync(table.Configuration, storage, table.Configuration.Store.Table, boundary, classes, context);
            var trips = await rounds.DriveAsync(planning, 8, 30);
            var plan = await planning;
            _plans.Add(plan);

            Assert.True(plan.Status == AdvancePlanStatus.Planned, $"{plan.Reason}: {plan.Detail}");
            Assert.Equal(4, trips);
            var emitted = Emitted(plan, AdvanceWindowLimits.Default);
            Assert.Equal((await Oracle(table, boundary)).Retired, new SortedSet<string>(emitted, StringComparer.Ordinal));
            var (sequential, _) = await Plan(table, boundary);
            Assert.True(sequential.Status == AdvancePlanStatus.Planned, $"{sequential.Reason}: {sequential.Detail}");
            Assert.Equal(Emitted(sequential, AdvanceWindowLimits.Default), emitted);
            Assert.Equal(AdvancePlanner.Reads(boundary, classes), context.ReadsUsed);
            Assert.Equal(8, storage.MaxOutstanding);
            plan.Dispose();
            Assert.Equal(0, context.Memory.Managed);
            Assert.Equal(0, context.Memory.Native);
            Assert.Equal(0, context.Transfer.Charged);
        }

        // Guards short admission against a context's read-ahead
        [Fact]
        public async Task ShortPlanReadsOneCommitAtATime()
        {
            var table = _kit.Local();
            await WriteHistory(table, 29, 20);
            await _kit.Build(table, 15);
            var boundary = Boundary(await Log(table), 0, 15);
            var storage = TrackedHookFileStorage.Over(table.Local);

            var (plan, context) = await Plan(table, boundary, longAdmission: false, storage: storage, context: _kit.Context(false, boundary.V, readAhead: 8));

            Assert.True(plan.Status == AdvancePlanStatus.Planned, $"{plan.Reason}: {plan.Detail}");
            Assert.True(storage.Opened.Count > 10, $"{storage.Opened.Count} commits opened");
            Assert.Equal(1, storage.MaxOutstanding);
            Assert.Equal(AdvancePlanner.Reads(boundary, Classes(boundary.Log)), context.ReadsUsed);
        }

        [Fact]
        public async Task LongPlanMissingCandidateDeclinesAtItsTurn()
        {
            var table = _kit.Local();
            await WriteHistory(table, 41, 30);
            await _kit.Build(table, 25);
            var boundary = Boundary(await Log(table), 0, 25, step: AdvanceStep.Long);
            async Task<(AdvancePlan Plan, CleanupContext Context, TrackedHookFileStorage Storage)> Missing(int readAhead)
            {
                var storage = TrackedHookFileStorage.Over(table.Local);
                storage.Hidden = path => path.Name == $"{12:D20}.json";
                var (plan, context) = await Plan(table, boundary, storage: storage, context: _kit.Context(true, boundary.V, readAhead: readAhead));
                return (plan, context, storage);
            }

            var (sequential, _, _) = await Missing(1);
            var (ahead, context, storage) = await Missing(8);

            Assert.Equal(AdvancePlanStatus.Declined, ahead.Status);
            Assert.Equal(MaintenanceReason.HistoryChanged, ahead.Reason);
            Assert.Contains("Missing after", ahead.Detail);
            Assert.Equal(sequential.Detail, ahead.Detail);
            Assert.Contains(13L, storage.Opened);
            AssertNothingHeld(ahead, context);
            Assert.Equal(0, context.Transfer.Charged);
            Assert.Equal(storage.Returned, storage.Disposed);
        }

        [Fact]
        public async Task ShortPlanPromotesBeforeChargingResolutionsItsReadsCannotCount()
        {
            var table = _kit.Local();
            await WriteHistory(table, 11, 40);
            await _kit.Build(table, 20);
            var log = await Log(table);
            var boundary = Boundary(log, 0, 20);
            var reads = AdvancePlanner.Reads(boundary, Classes(log));
            // C and C.json, read before candidate commits 0 to 19
            var ancillary = reads - boundary.C;

            // Long admission charges every native resolution of the plan, the protection's before the first candidate commit is read and each commit's after it
            var (plan, context) = await Plan(table, boundary, storage: Watched(table, out var buckets, out var watch));
            Assert.True(plan.Status == AdvancePlanStatus.Planned, $"{plan.Reason}: {plan.Detail}");
            var charges = buckets.NativeRequests;
            var protection = watch.ChargedBefore[CommitName(0)];
            Assert.InRange(protection, 2, charges - 1);

            // Any fewer reads than the plan and every charge need promotes with nothing charged once its debt outgrows them
            for (var limit = reads + 1; limit < reads + charges; limit++)
            {
                var (fewer, fewerContext) = await Plan(table, boundary, longAdmission: false, readLimit: limit, storage: Metered(table, out var fewerBuckets));
                Assert.True(fewer.Status == AdvancePlanStatus.NeedsLongAdmission, $"{limit} reads: {fewer.Status} {fewer.Reason}: {fewer.Detail}");
                Assert.True(fewerBuckets.NativeRequests == 0, $"{limit} reads, {protection} owed by the protection: {fewerBuckets.NativeRequests} charged");
                if (limit < reads + protection)
                {
                    Assert.InRange(fewerContext.ReadsUsed, 1, ancillary);
                }
                else
                {
                    // Through the first commit whose debt does not fit
                    Assert.Equal(ancillary + Enumerable.Range(0, (int)boundary.C).Count(x => watch.ChargedBefore[CommitName(x)] <= limit - reads), fewerContext.ReadsUsed);
                }
                AssertNothingHeld(fewer, fewerContext);
            }

            // Exactly enough plans and charges its whole debt once its last object is read, every charge a counted read
            var (fits, fitsContext) = await Plan(table, boundary, longAdmission: false, readLimit: reads + charges, storage: Watched(table, out var fitsBuckets, out var fitsWatch));
            Assert.True(fits.Status == AdvancePlanStatus.Planned, $"{fits.Reason}: {fits.Detail}");
            Assert.Equal(charges, fitsBuckets.NativeRequests);
            Assert.True(fitsWatch.ChargedBefore.Values.All(x => x == 0), $"{fitsWatch.ChargedBefore.Values.Max()} of {charges} charged before the last read");
            Assert.Equal(reads + charges, fitsContext.ReadsUsed);
            Assert.Equal(reads + charges, context.ReadsUsed);
            Assert.Equal(Emitted(plan, AdvanceWindowLimits.Default), Emitted(fits, AdvanceWindowLimits.Default));
        }

        [Fact]
        public async Task ShortPlanWhoseNewestCheckpointOutgrowsItsReadsPromotesWithNothingCharged()
        {
            var table = _kit.Local();
            // The protection's resolutions to C at 1 as the table is now, charged by a long plan before its first candidate commit
            async Task<(AdvanceBoundary Boundary, long Owed)> OwedAsync()
            {
                var listed = Boundary(await Log(table), 0, 1);
                var (plan, _) = await Plan(table, listed, storage: Watched(table, out _, out var watch));
                Assert.True(plan.Status == AdvancePlanStatus.Planned, $"{plan.Reason}: {plan.Detail}");
                return (listed, watch.ChargedBefore[CommitName(0)]);
            }
            // Checkpoint 1 holds five live files
            await Commit(table, 0, 0, new[] { Create(), DeltaTestKit.Metadata(DeltaTestKit.UserSchema) }.Concat(Enumerable.Range(0, 4).Select(x => Add($"part-{x}.parquet"))).ToArray());
            await Commit(table, 1, 0, Add("part-x.parquet"));
            await _kit.Build(table, 1);
            var (_, beforeNewest) = await OwedAsync();
            // Checkpoint 2 above C holds forty more
            await Commit(table, 2, 0, Enumerable.Range(0, 40).Select(x => Add($"part-y-{x}.parquet")).ToArray());
            await _kit.Build(table, 2);
            var (_, beforeTail) = await OwedAsync();
            // A tail commit after it adds twenty
            await Commit(table, 3, 0, Enumerable.Range(0, 20).Select(x => Add($"part-z-{x}.parquet")).ToArray());
            var (boundary, owed) = await OwedAsync();
            Assert.Equal(2, AdvancePlanner.NewestAbove(boundary)?.Version);
            Assert.True(1 < beforeNewest && beforeNewest < beforeTail && beforeTail < owed, $"{beforeNewest}, {beforeTail} and {owed} owed");
            var reads = AdvancePlanner.Reads(boundary, Classes(boundary.Log));

            // Reads for the objects and one resolution, what is owed before the newest checkpoint, or before the tail; C, C.json, the newest checkpoint and the tail commit are reads 1 to 4
            foreach (var (fits, read) in new[] { (1L, 1L), (beforeNewest, 3L), (beforeTail, 4L) })
            {
                var (promoted, promotedContext) = await Plan(table, boundary, longAdmission: false, readLimit: reads + fits, storage: Metered(table, out var buckets));

                Assert.True(promoted.Status == AdvancePlanStatus.NeedsLongAdmission, $"{fits} fit: {promoted.Status} {promoted.Reason}: {promoted.Detail}");
                Assert.True(buckets.NativeRequests == 0, $"{fits} fit: {buckets.NativeRequests} charged before the promotion");
                // Promoted by the first object whose debt does not fit
                Assert.Equal(read, promotedContext.ReadsUsed);
                AssertNothingHeld(promoted, promotedContext);
            }
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task ShortPlanWhoseWindowDebtOutgrowsItsReadsPromotesWithNothingCharged(bool churn)
        {
            // O at a marked root at 1, then a wide table replacing one file a commit, or a narrow one replacing all its files every commit
            var table = _kit.Local();
            var width = churn ? 3 : 60;
            var c = churn ? 32 : 8;
            await Commit(table, 0, 0, new[] { Create(), DeltaTestKit.Metadata(DeltaTestKit.UserSchema) }.Concat(Enumerable.Range(0, width).Select(x => Add($"part-0-{x}.parquet"))).ToArray());
            await Commit(table, 1, 0, Add("part-1.parquet"));
            await _kit.Build(table, 1);
            for (var version = 2; version <= c; version++)
            {
                var gone = churn
                    ? Enumerable.Range(0, width).Select(x => version == 2 ? $"part-0-{x}.parquet" : $"part-{version - 1}-{x}.parquet").ToList()
                    : new List<string>() { $"part-0-{version}.parquet" };
                await Commit(table, version, 0, gone.Select(x => Remove(x, version)).Concat(Enumerable.Range(0, gone.Count).Select(x => Add($"part-{version}-{x}.parquet"))).ToArray());
            }
            await _kit.Build(table, c);
            var log = await Log(table);
            var boundary = Boundary(log, 1, c);
            var reads = AdvancePlanner.Reads(boundary, Classes(log));
            // C and C.json, read before candidate commits 1 to c - 1
            var ancillary = reads - (c - 1);

            // Long admission charges the protection before commit 1, the first candidate, is read and each commit's resolutions after it
            var (plan, _) = await Plan(table, boundary, storage: Watched(table, out var buckets, out var watch));
            Assert.True(plan.Status == AdvancePlanStatus.Planned, $"{plan.Reason}: {plan.Detail}");
            var charges = buckets.NativeRequests;
            var protection = watch.ChargedBefore[CommitName(1)];
            var windows = charges - protection;
            Assert.True(churn ? windows > 2 * protection : 2 * windows < protection, $"{protection} owed by the protection and {windows} by the windows");

            // The whole debt fits: planned, charging it once its last object is read
            var (fits, fitsContext) = await Plan(table, boundary, longAdmission: false, readLimit: reads + charges, storage: Watched(table, out var fitsBuckets, out var fitsWatch));
            Assert.True(fits.Status == AdvancePlanStatus.Planned, $"{fits.Reason}: {fits.Detail}");
            Assert.Equal(charges, fitsBuckets.NativeRequests);
            Assert.True(fitsWatch.ChargedBefore.Values.All(x => x == 0), $"{fitsWatch.ChargedBefore.Values.Max()} of {charges} charged before the last read");
            Assert.Equal(reads + charges, fitsContext.ReadsUsed);
            Assert.Equal(Emitted(plan, AdvanceWindowLimits.Default), Emitted(fits, AdvanceWindowLimits.Default));

            // The protection fits and the windows push the debt over: promoted with nothing charged, by the first commit that does not fit
            foreach (var debt in new[] { protection, protection + windows / 2, charges - 1 })
            {
                var (promoted, promotedContext) = await Plan(table, boundary, longAdmission: false, readLimit: reads + debt, storage: Metered(table, out var promotedBuckets));

                Assert.True(promoted.Status == AdvancePlanStatus.NeedsLongAdmission, $"{debt} fit: {promoted.Status} {promoted.Reason}: {promoted.Detail}");
                Assert.True(promotedBuckets.NativeRequests == 0, $"{debt} of {charges} fit: {promotedBuckets.NativeRequests} charged before the promotion");
                Assert.Equal(ancillary + Enumerable.Range(1, c - 1).Count(x => watch.ChargedBefore[CommitName(x)] <= debt), promotedContext.ReadsUsed);
                AssertNothingHeld(promoted, promotedContext);
            }
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task ChargingResolutionsYieldsBetweenCharges(bool deadline)
        {
            var stop = deadline ? IssuanceStop.Deadline : IssuanceStop.BuildDue;
            // Checkpoint 1 holds eleven live files
            var table = _kit.Local();
            await Commit(table, 0, 0, new[] { Create(), DeltaTestKit.Metadata(DeltaTestKit.UserSchema) }.Concat(Enumerable.Range(0, 10).Select(x => Add($"part-{x}.parquet"))).ToArray());
            await Commit(table, 1, 0, Add("part-x.parquet"));
            await _kit.Build(table, 1);
            var boundary = Boundary(await Log(table), 0, 1);
            await RetainedReferenceOracle.CreateReferencedFilesAsync(table.Native);
            // The checkpoint's and C.json's reads and two charges take the burst, the third charge waits for the bucket's clock
            var bucketClock = new FakeTimeProvider(Now);
            var buckets = new MaintenanceBuckets(bucketClock, requestsPerSecond: 1, requestBurst: 4);
            var metered = MeteredFileStorage.Wrap(table.Local, buckets, ListingPaging.LocalDisk);
            var run = new JobRun(0, JobKind.Advance) { Cancellation = new CancellationTokenSource() };
            var clock = new FakeTimeProvider(Now);
            using var cancellation = new CancellationTokenSource();
            var context = _kit.Context(true, boundary.V, stop: cancellation.Token, clock: clock, run: run);

            var planning = AdvancePlanner.PlanAsync(table.Configuration, metered, table.Configuration.Store.Table, boundary, Classes(boundary.Log), context);
            await MaintenanceBucketsTests.Until(() => buckets.Requests.QueueLength == 1);
            Assert.Equal(2, buckets.NativeRequests);
            if (stop == IssuanceStop.BuildDue)
            {
                run.YieldRequested = true;
            }
            else
            {
                clock.Advance(TimeSpan.FromHours(7));
            }
            bucketClock.Advance(TimeSpan.FromSeconds(1));
            var yielded = await Task.WhenAny(planning, Task.Delay(TimeSpan.FromSeconds(10))) == planning;
            if (!yielded)
            {
                cancellation.Cancel();
                await Assert.ThrowsAnyAsync<OperationCanceledException>(() => planning);
            }
            Assert.True(yielded, $"Charging went on past the stop: {buckets.NativeRequests} charges");

            using var plan = await planning;
            Assert.Equal(AdvancePlanStatus.Declined, plan.Status);
            Assert.Equal(stop == IssuanceStop.Deadline ? MaintenanceReason.JobDeadline : MaintenanceReason.Transient, plan.Reason);
            Assert.Contains(stop.ToString(), plan.Detail);
            // The granted charge, and none after it
            Assert.Equal(3, buckets.NativeRequests);
            Assert.Equal(0, buckets.Requests.QueueLength);
            AssertNothingHeld(plan, context);
        }

        [Theory]
        [InlineData(false, false)]
        [InlineData(true, false)]
        [InlineData(false, true)]
        public async Task DeclinedPlanChargesTheResolutionsItMade(bool longAdmission, bool overReads)
        {
            // Checkpoint 1 holds a hundred and one live files
            var table = _kit.Local();
            const int live = 100;
            const int staged = 200;
            await Commit(table, 0, 0, new[] { Create(), DeltaTestKit.Metadata(DeltaTestKit.UserSchema) }.Concat(Enumerable.Range(0, live).Select(x => Add($"part-{x:D4}.parquet"))).ToArray());
            await Commit(table, 1, 0, Add("boundary.parquet"));
            await Commit(table, 2, 0, Add("head.parquet"));
            await _kit.Build(table, 1);
            await RetainedReferenceOracle.CreateReferencedFilesAsync(table.Native);
            // A stage read after C and C.json rejects its last path; with overReads, after more resolutions than short reads leave room for
            var paths = Enumerable.Range(0, overReads ? staged : 0).Select(x => $"part-s-{x:D4}.parquet").ToList();
            foreach (var path in paths)
            {
                await File.WriteAllBytesAsync(Path.Combine(table.Native, path), new byte[] { 1 });
            }
            WriteStage(table, 2, paths.Append("../outside.parquet")
                .Select(x => $"{{\"add\":{{\"path\":\"{x}\",\"partitionValues\":{{}},\"size\":1,\"modificationTime\":1,\"dataChange\":true}}}}").ToArray());
            var log = await Log(table);
            var boundary = Boundary(log, 0, 1);
            var storage = Metered(table, out var buckets);
            var readLimit = overReads ? AdvancePlanner.Reads(boundary, Classes(log)) + live + 10 : MaintenanceWorkerSettings.DefaultShortReads;

            for (var attempt = 0; attempt < 2; attempt++)
            {
                var context = _kit.Context(longAdmission, boundary.V, readLimit);
                using var plan = await AdvancePlanner.PlanAsync(table.Configuration, storage, table.Configuration.Store.Table, boundary, Classes(log), context);
                Assert.Equal((AdvancePlanStatus.Declined, MaintenanceReason.PathRejected), (plan.Status, plan.Reason));
                Assert.Equal(3, context.ReadsUsed);
                AssertNothingHeld(plan, context);
            }

            // Three object reads and at least C's live paths and the staged paths per attempt
            var accounted = buckets.Requests.Charged + buckets.Requests.Debt;
            Assert.True(accounted >= 2 * (3 + live + 1 + paths.Count), $"Two declined plans resolved {live + 1 + paths.Count} paths each, but the bucket charged {buckets.Requests.Charged} requests with {buckets.Requests.Debt} debt");
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task DeclinedLongPlanRepaysItsResolutionsInTurns(bool deadline)
        {
            // Checkpoint 1 holds a hundred and one live files
            var table = _kit.Local();
            const int live = 100;
            await Commit(table, 0, 0, new[] { Create(), DeltaTestKit.Metadata(DeltaTestKit.UserSchema) }.Concat(Enumerable.Range(0, live).Select(x => Add($"part-{x:D4}.parquet"))).ToArray());
            await Commit(table, 1, 0, Add("boundary.parquet"));
            await Commit(table, 2, 0, Add("head.parquet"));
            await _kit.Build(table, 1);
            await RetainedReferenceOracle.CreateReferencedFilesAsync(table.Native);
            // A stage read after C and C.json rejects its path
            WriteStage(table, 2, "{\"add\":{\"path\":\"../outside.parquet\",\"partitionValues\":{},\"size\":1,\"modificationTime\":1,\"dataChange\":true}}");
            var log = await Log(table);
            var boundary = Boundary(log, 0, 1);
            // One request a second, with a burst that covers the object reads but not the resolutions
            var bucketClock = new FakeTimeProvider(Now);
            var buckets = new MaintenanceBuckets(bucketClock, 1, 20, 1L << 50, 1L << 50);
            var storage = MeteredFileStorage.Wrap(table.Local, buckets, ListingPaging.LocalDisk);
            var clock = new FakeTimeProvider(Now);
            using var cancellation = new CancellationTokenSource();
            var context = _kit.Context(true, boundary.V, stop: cancellation.Token, clock: clock);

            var planning = AdvancePlanner.PlanAsync(table.Configuration, storage, table.Configuration.Store.Table, boundary, Classes(log), context);
            await MaintenanceBucketsTests.Until(() => planning.IsCompleted || buckets.Requests.QueueLength == 1);
            Assert.False(planning.IsCompleted, $"Declined with {buckets.NativeRequests} resolutions charged and {buckets.Requests.Debt} debt");
            var turns = buckets.NativeRequests;
            if (deadline)
            {
                clock.Advance(TimeSpan.FromHours(7));
            }
            // A turn at a time until the plan returns or waits again; past the deadline only the waiting turn is granted
            while (!planning.IsCompleted && (!deadline || buckets.NativeRequests == turns))
            {
                var charged = buckets.NativeRequests;
                bucketClock.Advance(TimeSpan.FromSeconds(1));
                await MaintenanceBucketsTests.Until(() => planning.IsCompleted || (buckets.NativeRequests > charged && buckets.Requests.QueueLength == 1));
            }
            if (!planning.IsCompleted)
            {
                var stuck = $"The declined plan went on past the deadline: {buckets.NativeRequests} charges";
                cancellation.Cancel();
                Assert.Fail(stuck);
            }

            using var plan = await planning;
            Assert.Equal((AdvancePlanStatus.Declined, MaintenanceReason.PathRejected), (plan.Status, plan.Reason));
            // The repayment latches no stop over the plan's own reason
            Assert.Equal(IssuanceStop.None, context.Stopped);
            // The repayment counts no reads
            Assert.Equal(3, context.ReadsUsed);
            if (deadline)
            {
                // The granted turn, and the rest left owed
                Assert.Equal(turns + 1, buckets.NativeRequests);
                Assert.True(buckets.Requests.Debt > 0, $"{buckets.NativeRequests} charged with no debt left");
            }
            else
            {
                Assert.True(buckets.NativeRequests >= live + 1, $"{buckets.NativeRequests} charged");
                Assert.Equal(0, buckets.Requests.Debt);
            }
            AssertNothingHeld(plan, context);
        }

        // The table's local disk under fresh unthrottled buckets
        private static IFileStorage Metered(Table table, out MaintenanceBuckets buckets)
        {
            buckets = MaintenanceBucketsTests.Unthrottled();
            return MeteredFileStorage.Wrap(table.Local, buckets, ListingPaging.LocalDisk);
        }

        // As Metered, noting what was charged before each object is read
        private static IFileStorage Watched(Table table, out MaintenanceBuckets buckets, out ChargeWatchStorage watch)
        {
            buckets = MaintenanceBucketsTests.Unthrottled();
            watch = new ChargeWatchStorage(table.Local, buckets);
            return MeteredFileStorage.Wrap(watch, buckets, ListingPaging.LocalDisk);
        }

        private static string CommitName(long version) => $"{version:D20}.json";

        // A connector-owned local disk table that notes the native charges made before each object is first opened
        private sealed class ChargeWatchStorage : ForwardingStorage, IConnectorOwnedStorage, ILocalDiskFileStorage, IFileStorage
        {
            private readonly MaintenanceBuckets _buckets;

            public ChargeWatchStorage(IFileStorage inner, MaintenanceBuckets buckets) : base(inner)
            {
                _buckets = buckets;
            }

            public Dictionary<string, long> ChargedBefore { get; } = new Dictionary<string, long>(StringComparer.Ordinal);

            public string ToNativeLocalPath(IOPath path) => ((ILocalDiskFileStorage)Inner).ToNativeLocalPath(path);

            Task<Stream?> IFileStorage.OpenRead(IOPath path, CancellationToken cancellationToken)
            {
                ChargedBefore.TryAdd(path.Name, _buckets.NativeRequests);
                return Inner.OpenRead(path, cancellationToken);
            }
        }

        // The upper-cased files a list of removes names, by the oracle's own rules
        private sealed class RetainedReferenceOracleNames
        {
            public RetainedReferenceOracleNames(IEnumerable<DeltaRemoveFileAction> removes)
            {
                foreach (var remove in removes)
                {
                    Names.Add(Upper(remove.Path!));
                    if (remove.DeletionVector is { StorageType: "u" } dv)
                    {
                        var value = dv.PathOrInlineDv!;
                        var prefix = value.Substring(0, value.Length - 20);
                        var name = $"deletion_vector_{Z85.DecodeToGuid(value.Substring(value.Length - 20)):D}.bin";
                        Names.Add(Upper(prefix.Length == 0 ? name : prefix + "/" + name));
                    }
                }
            }

            public HashSet<string> Names { get; } = new HashSet<string>(StringComparer.Ordinal);
        }
    }
}
