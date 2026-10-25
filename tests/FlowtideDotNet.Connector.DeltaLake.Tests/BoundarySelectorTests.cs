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
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Checkpoints;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.Advance;
using Microsoft.Extensions.Time.Testing;
using Stowage;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Text.Json;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public class BoundarySelectorTests : IDisposable
    {
        private static readonly DateTimeOffset s_now = new DateTimeOffset(2026, 10, 15, 12, 0, 0, TimeSpan.Zero);
        private static readonly TimeSpan s_retention = TimeSpan.FromDays(7);
        // Midnight UTC of now minus the retention
        private static readonly DateTimeOffset s_cutoff = new DateTimeOffset(2026, 10, 8, 0, 0, 0, TimeSpan.Zero);
        private static readonly TimeSpan s_wait = TimeSpan.FromSeconds(30);

        private readonly string _directory = Path.Combine(Path.GetTempPath(), "bs-" + ScratchOwners.ShortId());
        private readonly ScratchOwners _owners = new ScratchOwners();
        private readonly List<IDisposable> _disposables = new List<IDisposable>();

        public BoundarySelectorTests()
        {
            Directory.CreateDirectory(_directory);
        }

        public void Dispose()
        {
            foreach (var disposable in _disposables)
            {
                disposable.Dispose();
            }
            _owners.Release();
            try
            {
                Directory.Delete(_directory, true);
            }
            catch (IOException)
            {
            }
            catch (UnauthorizedAccessException)
            {
            }
        }

        private sealed class Table
        {
            public required IFileStorage Local { get; init; }

            public required string Root { get; init; }

            public required CleanupConfiguration Configuration { get; init; }

            public string Log(string name) => Path.Combine(Root, "t", "_delta_log", name);
        }

        // An eligible local disk table at /t with retention on
        private Table Local()
        {
            var root = Path.Combine(_directory, ScratchOwners.ShortId());
            Directory.CreateDirectory(root);
            var local = Files.Of.LocalDisk(root);
            _disposables.Add(local);
            var configuration = CleanupConfiguration.Validate(local, "t", false, new CleanupOptIns(s_retention, null));
            Assert.True(configuration.Store.Eligible);
            return new Table() { Local = local, Root = root, Configuration = configuration };
        }

        // Two days before the cutoff, a minute apart
        internal static long Old(long version) => (s_cutoff - TimeSpan.FromDays(2)).ToUnixTimeMilliseconds() + version * 60_000;

        // Within the hour before now
        private static long Young(long version) => (s_now - TimeSpan.FromHours(1)).ToUnixTimeMilliseconds() + version;

        private static Dictionary<string, string> Published() => new Dictionary<string, string>()
        {
            [RetentionProperties.LogKey] = RetentionProperties.Format(s_retention),
            [RetentionProperties.DeletedFileKey] = RetentionProperties.Format(s_retention)
        };

        // Own commits of epoch A from A to head with the table created at 0; commits below A carry a stage id and no epoch
        internal static async Task Create(IFileStorage storage, long head, Func<long, long> timestamp, Dictionary<string, string>? configuration = null, long adoptedAt = 0)
        {
            for (long version = 0; version <= head; version++)
            {
                var actions = version == 0
                    ? new[] { DeltaTestKit.Protocol(), DeltaTestKit.Metadata(DeltaTestKit.UserSchema, configuration ?? Published()), DeltaLakeCheckpointBuildTests.Add("part-0000.parquet") }
                    : new[] { DeltaLakeCheckpointBuildTests.Add($"part-{version:D4}.parquet") };
                if (version < adoptedAt)
                {
                    var legacy = new DeltaAction() { CommitInfo = new DeltaCommitInfoAction() { StageId = $"stage-{version}", Timestamp = timestamp(version), Operation = "WRITE" } };
                    await DeltaTestKit.WriteCommit(storage, "t", version, actions.Prepend(legacy).ToArray());
                }
                else
                {
                    await DeltaTestKit.WriteOwnCommit(storage, "t", version, adoptedAt, timestamp(version), actions);
                }
            }
        }

        private async Task<BuildResult> Build(IFileStorage storage, long version, RootPolicy root = RootPolicy.Auto, IReadOnlySet<ObjectIdentity>? unsupported = null)
        {
            var owner = _owners.Add(new ScratchDirectory(Path.Combine(_directory, ScratchOwners.ShortId())));
            BuildResult result;
            using (var scratch = owner.CreateJob())
            {
                result = await CheckpointBuilder.BuildAsync(new CheckpointBuildContext()
                {
                    Storage = storage,
                    Table = "t",
                    Scratch = scratch,
                    Memory = new JobMemory(),
                    Clock = new FakeTimeProvider(s_now)
                }, new BuildRequest() { Version = version, Root = root, KnownUnsupported = unsupported ?? new HashSet<ObjectIdentity>() });
                Assert.Equal(BuildStatus.Completed, result.Status);
            }
            _owners.Release(owner);
            return result;
        }

        private CleanupContext Context(long head, TimeProvider clock, bool isLong = false, long readLimit = MaintenanceWorkerSettings.DefaultShortReads, int interval = 0,
            IReadOnlySet<ObjectIdentity>? unsupported = null, bool rebuildLegacy = false)
        {
            var owner = _owners.Add(new ScratchDirectory(Path.Combine(_directory, ScratchOwners.ShortId())));
            var scratch = owner.CreateJob();
            _disposables.Add(scratch);
            return new CleanupContext(new JobRun(head, JobKind.Advance) { Cancellation = new CancellationTokenSource() }, clock)
            {
                Kind = JobKind.Advance,
                Long = isLong,
                ReadLimit = isLong ? long.MaxValue : readLimit,
                Deadline = clock.GetUtcNow() + TimeSpan.FromHours(1),
                Head = head,
                TableId = null,
                Scratch = scratch,
                Memory = new JobMemory(),
                Transfer = new TransferAllowance(),
                Stop = default,
                CheckpointInterval = interval,
                KnownUnsupported = unsupported ?? new HashSet<ObjectIdentity>(),
                RebuildLegacyCheckpoints = rebuildLegacy
            };
        }

        private static DeltaTableCleanup Cleanup(Table table, TimeProvider? clock = null, IFileStorage? storage = null, Func<IFileStorage?>? current = null)
        {
            var cleanup = new DeltaTableCleanup(table.Configuration, storage ?? table.Local, clock ?? new FakeTimeProvider(s_now), "t", null, current ?? (() => table.Local), new DiscardCounters(), null, "t");
            Observed(cleanup);
            // These tests observe selection: each advance ends once its hint is raised
            return AdvanceExecutionTests.WithoutExclusions(AdvanceExecutionTests.PlanOnly(cleanup));
        }

        // What a cleanup's advance last selected and planned
        internal sealed class AdvanceObserver
        {
            public AdvanceBoundary? Boundary { get; set; }

            public AdvancePlan? Plan { get; set; }
        }

        private static readonly ConditionalWeakTable<DeltaTableCleanup, AdvanceObserver> s_observers = new ConditionalWeakTable<DeltaTableCleanup, AdvanceObserver>();

        // The cleanup's observer, whose hooks are installed on first use
        internal static AdvanceObserver Observed(DeltaTableCleanup cleanup)
        {
            return s_observers.GetValue(cleanup, static x =>
            {
                var observer = new AdvanceObserver();
                x.AdvanceForTests.SelectedHookForTests = boundary => observer.Boundary = boundary;
                x.AdvanceForTests.PlannedHookForTests = plan => observer.Plan = plan;
                return observer;
            });
        }

        // One advance at V, which releases everything it held
        private async Task<CleanupResult> Run(DeltaTableCleanup cleanup, long head, bool isLong = false, long readLimit = MaintenanceWorkerSettings.DefaultShortReads, int interval = 0)
        {
            return (await RunWith(cleanup, Context(head, new FakeTimeProvider(s_now), isLong, readLimit, interval))).Result;
        }

        private static async Task<(CleanupResult Result, CleanupContext Context)> RunWith(DeltaTableCleanup cleanup, CleanupContext context)
        {
            var result = await cleanup.RunAsync(JobKind.Advance, context);
            Assert.Equal(0, context.Memory.Managed);
            Assert.Equal(0, context.Memory.Native);
            Assert.Empty(context.Scratch.Files);
            return (result, context);
        }

        // The reads a short step to min E spends besides its candidates, measured on a long step, which probes nothing above min E
        private async Task<long> StepSpend(Table table, long head, int interval = 0)
        {
            var cleanup = Cleanup(table);
            // A one-read short admission sets a zero short cap
            Assert.True((await Run(cleanup, head, readLimit: 1, interval: interval)).NeedsLongAdmission);
            long spend = -1;
            var step = AdvanceStep.Short;
            cleanup.AdvanceForTests.PlanForTests = (configuration, storage, path, boundary, classes, context) =>
            {
                step = boundary.Step;
                spend = context.ReadsUsed + AdvancePlanner.Reads(boundary, classes) - (boundary.C - boundary.O);
                return Task.FromResult(new AdvancePlan() { Status = AdvancePlanStatus.NotBuilt });
            };
            Assert.Equal(OutcomeKind.NoWork, (await Run(cleanup, head, isLong: true, interval: interval)).Kind);
            Assert.Equal(AdvanceStep.Long, step);
            return spend;
        }

        private static void AssertDeclined(CleanupResult result, MaintenanceReason reason)
        {
            Assert.True(result.Kind == OutcomeKind.Declined && result.Reason == reason, $"{result.Kind} {result.Reason}: {result.Detail}");
        }

        // The planner read C back gap-free and the advance completed, deleting nothing yet
        private static AdvanceBoundary Selected(DeltaTableCleanup cleanup, CleanupResult result)
        {
            Assert.True(result.Kind == OutcomeKind.Completed && result.Reason == MaintenanceReason.None && !result.NeedsLongAdmission && result.Demand == null, $"{result.Kind} {result.Reason}: {result.Detail}");
            var boundary = Observed(cleanup).Boundary!;
            var plan = Observed(cleanup).Plan!;
            Assert.Equal(AdvancePlanStatus.Planned, plan.Status);
            Assert.Equal(boundary.C, plan.Destination!.Proof!.Version);
            return boundary;
        }

        private static Task<LastCheckpointInfo?> Hint(Table table) => DeltaMaintenanceWorkerTests.Hint(table.Local);

        private static Task WriteHint(Table table, long version) => File.WriteAllTextAsync(table.Log("_last_checkpoint"), JsonSerializer.Serialize(new LastCheckpointInfo() { Version = version, Size = 1 }));

        // A planner that reads C from the destination and plans nothing else
        private static Func<CleanupConfiguration, IFileStorage, IOPath, AdvanceBoundary, LogClassification, CleanupContext, Task<AdvancePlan>> DestinationPlanner(Func<DestinationRead, DestinationRead>? edit = null,
            Func<Task>? planning = null)
        {
            return async (configuration, storage, table, boundary, classes, context) =>
            {
                if (planning != null)
                {
                    await planning();
                }
                var read = await DestinationCheckpoint.ReadAsync(table, storage, boundary.Checkpoint, context.Scratch, context.Memory, context.Transfer, context, null, context.Stop);
                return new AdvancePlan() { Status = AdvancePlanStatus.Planned, Destination = edit?.Invoke(read) ?? read };
            };
        }

        [Fact]
        public async Task OIsCorrectMidAdvance()
        {
            var table = Local();
            await Create(table.Local, 14, Old);
            await Build(table.Local, 4);
            await Build(table.Local, 8);
            await Build(table.Local, 12);
            // A descending log deletion toward 8 stopped after 7, 6 and 5: 0.json is still listed
            for (long version = 5; version <= 7; version++)
            {
                File.Delete(table.Log($"{version:D20}.json"));
            }
            var cleanup = Cleanup(table);

            var boundary = Selected(cleanup, await Run(cleanup, 14));

            Assert.True(File.Exists(table.Log($"{0:D20}.json")));
            Assert.Equal(8, boundary.O);
            Assert.Equal(8, boundary.OldestRoot!.Version);
            Assert.True(boundary.OldestRootMarked);
            Assert.Equal(12, boundary.C);

            // An advance's torn started record suspends the next one
            await File.WriteAllTextAsync(table.Log($".flowtide-cleanup.{12:D20}.advance.0123456789abcdef.attempt.started"), "{}");
            AssertDeclined(await Run(cleanup, 14), MaintenanceReason.ManifestSuspended);
        }

        [Fact]
        public async Task UnmarkedOldestRootIsReported()
        {
            var table = Local();
            await Create(table.Local, 12, Old);
            await DeltaLakeCheckpointBuildTests.LegacyRoot(table.Local, "t", 4);
            await Build(table.Local, 10, RootPolicy.None);
            // Truncated by another tool below its legacy checkpoint
            for (long version = 0; version <= 4; version++)
            {
                File.Delete(table.Log($"{version:D20}.json"));
            }
            var cleanup = Cleanup(table);

            var boundary = Selected(cleanup, await Run(cleanup, 12));

            Assert.Equal(4, boundary.O);
            Assert.False(boundary.OldestRootMarked);
            Assert.Equal(10, boundary.C);
        }

        [Fact]
        public async Task SelectionCarriesTheAnchorAndTheFirstJson()
        {
            var table = Local();
            await Create(table.Local, 14, Old);
            await Build(table.Local, 4);
            await Build(table.Local, 10);
            var cleanup = Cleanup(table);

            // Every commit from zero is listed
            var fromZero = Selected(cleanup, await Run(cleanup, 14));
            Assert.Equal((0L, 0L), (fromZero.O, fromZero.FirstJson));
            Assert.NotNull(fromZero.Anchor);
            Assert.Equal((14L, fromZero.TableId), (fromZero.Anchor.Version, fromZero.Anchor.TableId));
            Assert.True(ControlTokens.IsCanonical(fromZero.AdvanceId));

            // Commits below the root at 4 are gone; its own commit is listed
            for (long version = 0; version < 4; version++)
            {
                File.Delete(table.Log($"{version:D20}.json"));
            }
            var rooted = Selected(cleanup, await Run(cleanup, 14));
            Assert.Equal((4L, 4L), (rooted.O, rooted.FirstJson));
            Assert.Equal(14L, rooted.Anchor!.Version);
            Assert.NotEqual(fromZero.AdvanceId, rooted.AdvanceId);

            // Without its own commit the first JSON follows the root
            File.Delete(table.Log($"{4:D20}.json"));
            var bare = Selected(cleanup, await Run(cleanup, 14));
            Assert.Equal((4L, 5L), (bare.O, bare.FirstJson));
        }

        [Fact]
        public async Task OnlyAForeignHeadDeclinesAsForeign()
        {
            var table = Local();
            await Create(table.Local, 14, Old);
            await Build(table.Local, 4);
            await Build(table.Local, 8);
            var cleanup = Cleanup(table);
            var own = await Run(cleanup, 14);
            Selected(cleanup, own);
            Assert.False(own.ForeignHead);

            // Another advance's authority, valid and resumable
            var fixture = CleanupRecordKit.Fixture("table-id", 8, 0, 0, 14, 0);
            var part = CleanupRecordKit.Part(fixture, 0, "/t/a.parquet");
            var header = CleanupRecordKit.Header(fixture, part);
            var authority = new[] { part, header, CleanupRecordKit.Started(fixture, header) };
            foreach (var record in authority)
            {
                CleanupRecordKit.PlantLocal(table.Log(""), record);
            }
            // It resumes and suspends: the fixture's C is not the content its header names
            var reserved = await Run(cleanup, 14);
            AssertDeclined(reserved, MaintenanceReason.HistoryChanged);
            Assert.False(reserved.ForeignHead);
            foreach (var record in authority)
            {
                File.Delete(table.Log(record.Name));
            }

            // Another writer commits above the own head
            await DeltaTestKit.WriteCommit(table.Local, "t", 15, DeltaLakeCheckpointBuildTests.Add("foreign-15.parquet"));
            var foreign = await Run(cleanup, 15);
            AssertDeclined(foreign, MaintenanceReason.CleanupGate);
            Assert.True(foreign.ForeignHead);
        }

        [Fact]
        public async Task ForeignHeadDeclineLeavesTheDaysAdvanceDue()
        {
            var table = Local();
            await Create(table.Local, 14, Old);
            await Build(table.Local, 4);
            await DeltaTestKit.WriteCommit(table.Local, "t", 15, DeltaLakeCheckpointBuildTests.Add("foreign-15.parquet"));
            var cleanup = Cleanup(table);

            var foreign = await Run(cleanup, 15);

            // Coverage ends, not the day: the advance runs once an own anchor restores it
            AssertDeclined(foreign, MaintenanceReason.CleanupGate);
            Assert.True(foreign.ForeignHead);
            Assert.True(cleanup.IsDue(JobKind.Advance, 15, s_now));
        }

        [Fact]
        public async Task ForeignHeadDeclinesDoNotRepeatAllDay()
        {
            var table = Local();
            await Create(table.Local, 14, Old);
            await Build(table.Local, 4);
            var cleanup = Cleanup(table);
            var head = 14L;
            var attempts = 0;

            // Another writer commits before each attempt, each of which ends coverage in the worker
            while (attempts < 10 && cleanup.IsDue(JobKind.Advance, head + 1, s_now))
            {
                head++;
                await DeltaTestKit.WriteCommit(table.Local, "t", head, DeltaLakeCheckpointBuildTests.Add($"foreign-{head}.parquet"));
                var foreign = await Run(cleanup, head);
                Assert.True(foreign.ForeignHead, $"{foreign.Kind} {foreign.Reason}: {foreign.Detail}");
                attempts++;
            }

            Assert.Equal(2, attempts);
        }

        [Fact]
        public async Task ForeignHeadRetryReturnsTheNextDay()
        {
            var table = Local();
            await Create(table.Local, 14, Old);
            await Build(table.Local, 4);
            var clock = new FakeTimeProvider(s_now);
            var cleanup = Cleanup(table, clock);
            var head = 14L;
            var perDay = new List<int>();
            // Another writer commits before each attempt, for three days
            for (var day = 0; day < 3; day++)
            {
                var attempts = 0;
                while (attempts < 10 && cleanup.IsDue(JobKind.Advance, head + 1, clock.GetUtcNow()))
                {
                    head++;
                    await DeltaTestKit.WriteCommit(table.Local, "t", head, DeltaLakeCheckpointBuildTests.Add($"foreign-{head}.parquet"));
                    var foreign = await Run(cleanup, head);
                    Assert.True(foreign.ForeignHead, $"{foreign.Kind} {foreign.Reason}: {foreign.Detail}");
                    attempts++;
                }
                perDay.Add(attempts);
                clock.Advance(TimeSpan.FromDays(1));
            }
            Assert.Equal(new[] { 2, 2, 2 }, perDay);
        }

        [Fact]
        public async Task SpentShortReadsPromote()
        {
            var table = Local();
            await Create(table.Local, 30, Old);
            await Build(table.Local, 10);
            await Build(table.Local, 25);

            // Too few for the capability replay, then too few for the selection's probes
            foreach (var limit in new long[] { 3, 8 })
            {
                var cleanup = Cleanup(table);
                Assert.True((await Run(cleanup, 30, readLimit: limit)).NeedsLongAdmission, $"limit {limit}");
                // The long admission's cap follows the short limit: the long step to min E
                var boundary = Selected(cleanup, await Run(cleanup, 30, isLong: true));
                Assert.Equal(AdvanceStep.Long, boundary.Step);
                Assert.Equal(10, boundary.C);
            }
        }

        [Fact]
        public async Task BoundaryPreservesAtLeastR()
        {
            var table = Local();
            // 0 to 6 before the cutoff, 7 to 9 after midnight of the cutoff day but before now minus R, 10 on within the last hour
            await Create(table.Local, 12, version => version <= 6 ? Old(version) : version <= 9 ? (s_cutoff + TimeSpan.FromHours(1)).ToUnixTimeMilliseconds() + version : Young(version));
            await Build(table.Local, 4);
            await Build(table.Local, 8);
            await Build(table.Local, 11);
            var cleanup = Cleanup(table);

            var boundary = Selected(cleanup, await Run(cleanup, 12));

            Assert.Equal(s_cutoff, boundary.Cutoff);
            // Checkpoint 8 is older than R but not older than midnight of the cutoff day
            Assert.True((s_cutoff + TimeSpan.FromHours(1)).ToUnixTimeMilliseconds() + 8 <= (s_now - s_retention).ToUnixTimeMilliseconds());
            Assert.Equal(4, boundary.C);
            Assert.True(Old(boundary.C) <= (s_now - s_retention).ToUnixTimeMilliseconds());
            Assert.Equal(s_retention, boundary.Retention);
        }

        [Fact]
        public async Task CapRuleChoosesShortOrLongStep()
        {
            var table = Local();
            await Create(table.Local, 30, Old);
            await Build(table.Local, 10);
            await Build(table.Local, 20);
            await Build(table.Local, 25);

            // A large short limit reaches the largest eligible checkpoint
            var wide = Cleanup(table);
            var largest = Selected(wide, await Run(wide, 30, readLimit: 1000));
            Assert.Equal(25, largest.C);
            Assert.Equal(AdvanceStep.Short, largest.Step);
            // The same data spends the same reads before the cap: the cap follows the limit exactly
            var spent = await StepSpend(table, 30);

            var capped = Cleanup(table);
            var shortStep = Selected(capped, await Run(capped, 30, readLimit: spent + 15));
            Assert.Equal(15, shortStep.EffectiveShortCap);
            Assert.Equal(10, shortStep.C);
            Assert.Equal(AdvanceStep.Short, shortStep.Step);

            // min E - O equal to the cap stays short
            var edge = Cleanup(table);
            var exact = Selected(edge, await Run(edge, 30, readLimit: spent + 10));
            Assert.Equal(10, exact.C);
            Assert.Equal(AdvanceStep.Short, exact.Step);

            // Beyond the cap the short admission promotes once, and the long one plans min E without promoting again
            var promoted = Cleanup(table);
            var result = await Run(promoted, 30, readLimit: spent + 9);
            Assert.True(result.NeedsLongAdmission);
            Assert.Equal(AdvanceStep.Long, Observed(promoted).Boundary!.Step);
            var longStep = Selected(promoted, await Run(promoted, 30, isLong: true));
            Assert.Equal(AdvanceStep.Long, longStep.Step);
            Assert.Equal(10, longStep.C);
            Assert.Equal(9, longStep.EffectiveShortCap);
        }

        [Theory]
        [InlineData(0)]
        [InlineData(20)]
        public async Task SelectionCountsTheReadsThePlannerMakes(int interval)
        {
            // Five commits follow the newer checkpoint 25, which the planner reads with its tail only within the interval
            var table = Local();
            await Create(table.Local, 30, Old);
            await Build(table.Local, 10);
            await Build(table.Local, 25);
            var spent = await StepSpend(table, 30, interval);

            // A short step to 10 with no read to spare: planning uses exactly what the cap left
            var cleanup = Cleanup(table);
            var (result, context) = await RunWith(cleanup, Context(30, new FakeTimeProvider(s_now), readLimit: spent + 10, interval: interval));
            var boundary = Selected(cleanup, result);
            Assert.Equal((10L, AdvanceStep.Short, interval), (boundary.C, boundary.Step, boundary.Interval));
            Assert.Equal(context.ReadLimit, context.ReadsUsed);
        }

        [Fact]
        public async Task ShortStepLeavesTheReadsItsSelectionSpends()
        {
            var table = Local();
            await Create(table.Local, 30, Old);
            await Build(table.Local, 10);
            await Build(table.Local, 20);
            await Build(table.Local, 25);
            var spent = await StepSpend(table, 30);

            // The cap reaches 25, but probing 25 spends a read the plan to it would then lack: 20 is chosen instead of a promotion, and its probe leaves 24
            var cleanup = Cleanup(table);
            var (result, context) = await RunWith(cleanup, Context(30, new FakeTimeProvider(s_now), readLimit: spent + 25));
            var boundary = Selected(cleanup, result);
            Assert.Equal(24, boundary.EffectiveShortCap);
            Assert.Equal((20L, AdvanceStep.Short), (boundary.C, boundary.Step));
            Assert.True(context.ReadsUsed <= context.ReadLimit);

            // The search for the newest eligible checkpoint read 15, 25 and 28 but not 20, whose timestamp costs one more read: the cap passes over 20 to 15
            var many = Local();
            await Create(many.Local, 30, Old);
            foreach (var version in new long[] { 5, 10, 15, 20, 25, 28 })
            {
                await Build(many.Local, version);
            }
            var manySpent = await StepSpend(many, 30);
            var narrow = Cleanup(many);
            var (narrowResult, narrowContext) = await RunWith(narrow, Context(30, new FakeTimeProvider(s_now), readLimit: manySpent + 21));
            var narrowed = Selected(narrow, narrowResult);
            Assert.Equal((15L, AdvanceStep.Short), (narrowed.C, narrowed.Step));
            Assert.True(narrowContext.ReadsUsed <= narrowContext.ReadLimit);
        }

        [Fact]
        public async Task SelectionNeverSpendsTheReadsMinENeeds()
        {
            // An unmarked newest checkpoint at 12, which the plan to 10 reads with its 18 commits after it
            var table = Local();
            await Create(table.Local, 30, Old);
            await Build(table.Local, 10);
            await DeltaLakeCheckpointBuildTests.LegacyRoot(table.Local, "t", 12);
            var spent = await StepSpend(table, 30, 20);

            // Probing 12 would leave the plan to 10 one read short: 12 is not probed
            var cleanup = Cleanup(table);
            var (result, context) = await RunWith(cleanup, Context(30, new FakeTimeProvider(s_now), readLimit: spent + 10, interval: 20));
            var boundary = Selected(cleanup, result);
            Assert.Equal((10L, AdvanceStep.Short), (boundary.C, boundary.Step));
            Assert.Equal(context.ReadLimit, context.ReadsUsed);
        }

        [Fact]
        public async Task TimestampAheadDeclines()
        {
            var table = Local();
            await Create(table.Local, 10, version => version < 8 ? Old(version) : (s_now + TimeSpan.FromHours(1)).ToUnixTimeMilliseconds() + version);
            await Build(table.Local, 4);
            await Build(table.Local, 8);
            var cleanup = Cleanup(table);

            AssertDeclined(await Run(cleanup, 10), MaintenanceReason.TimestampAhead);
            Assert.Null(Observed(cleanup).Boundary);
        }

        [Fact]
        public async Task OtherEpochCommitAtCDeclines()
        {
            var table = Local();
            await Create(table.Local, 10, Old);
            await Build(table.Local, 4);
            await Build(table.Local, 8);
            // Commit 8 claims another epoch than the table's own commits around it
            await DeltaTestKit.WriteOwnCommit(table.Local, "t", 8, 3, Old(8), DeltaLakeCheckpointBuildTests.Add($"part-{8:D4}.parquet"));
            var cleanup = Cleanup(table);

            AssertDeclined(await Run(cleanup, 10), MaintenanceReason.InvalidProvenance);
            Assert.Null(Observed(cleanup).Boundary);
        }

        [Fact]
        public async Task NonmonotonicTimestampsDecline()
        {
            var table = Local();
            await Create(table.Local, 10, version => version == 8 ? Old(2) : Old(version));
            await Build(table.Local, 4);
            await Build(table.Local, 8);
            var cleanup = Cleanup(table);

            AssertDeclined(await Run(cleanup, 10), MaintenanceReason.InvalidProvenance);
            Assert.Null(Observed(cleanup).Boundary);
        }

        [Fact]
        public async Task ReadBackUsesDestinationNotCache()
        {
            // In memory, identities are weak: a cached copy is found under the identity of a replaced object
            var storage = Files.Of.InternalMemory($"./bs-{Guid.NewGuid():N}");
            await Create(storage, 6, Old);
            await Build(storage, 6);
            var gapped = Files.Of.InternalMemory($"./bs-{Guid.NewGuid():N}");
            await Create(gapped, 6, Old);
            await DeltaLakeCheckpointBuildTests.LegacyRoot(gapped, "t", 3);
            await Build(gapped, 6);
            var replacement = await DeltaTestKit.ReadBytes(gapped, $"/t/_delta_log/{6:D20}.checkpoint.parquet");

            var owner = _owners.Add(new ScratchDirectory(Path.Combine(_directory, ScratchOwners.ShortId())));
            var scope = new RootCacheScope(owner.Cache, storage, "table-id", 0);
            var transport = new StowageCheckpointTransport(storage, "t");
            var memory = new JobMemory();
            using var scratch = owner.CreateJob();
            var listed = (await DeltaLogSegment.ListAsync(storage, "t")).Listing.Checkpoints.Single();
            Assert.False(ObjectIdentity.Of("t", listed).Strong);
            Assert.True((await CheckpointProbe.ProbeAsync(transport, scratch, memory, null, listed, scope, default)).MarkedGapFree);
            using (var write = await storage.OpenWrite(listed.IOEntry.Path))
            {
                await write.WriteAsync(replacement);
            }
            var relisted = (await DeltaLogSegment.ListAsync(storage, "t")).Listing.Checkpoints.Single();

            // The cache still answers for the old bytes
            Assert.True((await CheckpointProbe.ProbeAsync(transport, scratch, memory, null, relisted, scope, default)).MarkedGapFree);
            var read = await DestinationCheckpoint.ReadAsync("t", storage, relisted, scratch, memory, new TransferAllowance(), null, null, default);
            Assert.Equal(DestinationStatus.Read, read.Status);
            Assert.False(read.GapFree);
            Assert.NotNull(read.Marker!.Inherited);
            Assert.Equal(SHA256.HashData(replacement), read.Proof!.Sha256);
            Assert.Equal(replacement.Length, read.Proof.Length);

            // The advance relies only on a gap-free destination read of C
            var table = Local();
            await Create(table.Local, 10, Old);
            await Build(table.Local, 4);
            await Build(table.Local, 8);
            await WriteHint(table, 4);
            var cleanup = Cleanup(table);
            var inherited = read.Marker with { Version = 8 };
            cleanup.AdvanceForTests.PlanForTests = DestinationPlanner(edit: x => x with { Marker = inherited });

            AssertDeclined(await Run(cleanup, 10), MaintenanceReason.ValidationFailed);
            Assert.Equal(4, (await Hint(table))!.Version);
        }

        [Fact]
        public async Task ClassifierCoversKnownNamesAndDeclinesUnknown()
        {
            var uuid = Guid.NewGuid().ToString("D");
            var expected = new (string Name, LogNameKind Kind, long Version, long End)[]
            {
                ($"{1:D20}.json", LogNameKind.Commit, 1, 1),
                ($"{1:D20}.crc", LogNameKind.Crc, 1, 1),
                ($"{2:D20}.checkpoint.parquet", LogNameKind.ClassicCheckpoint, 2, 2),
                ($"{3:D20}.checkpoint.{1:D10}.{2:D10}.parquet", LogNameKind.MultipartCheckpoint, 3, 3),
                ($"{4:D20}.checkpoint.{uuid}.json", LogNameKind.UuidCheckpoint, 4, 4),
                ($"{4:D20}.checkpoint.{uuid}.parquet", LogNameKind.UuidCheckpoint, 4, 4),
                ($"{1:D20}.{3:D20}.compacted.json", LogNameKind.Compaction, 1, 3),
                ($".{5:D20}.json.stage-a.tmp", LogNameKind.Stage, 5, 5),
                ($".flowtide-{Guid.NewGuid():N}.checkpoint.tmp", LogNameKind.Temporary, -1, -1),
                ($".flowtide-{Guid.NewGuid():N}.last_checkpoint.tmp", LogNameKind.Temporary, -1, -1),
                ($".flowtide-cleanup.{8:D20}.advance.part000001.attempt", LogNameKind.Control, 8, 8),
                ("_last_checkpoint", LogNameKind.Hint, -1, -1),
                ("_sidecars/", LogNameKind.ProtectedDirectory, -1, -1),
                ($".{1:D20}.crc.crc", LogNameKind.HiddenUnversioned, -1, -1),
                ("README", LogNameKind.UnknownUnversioned, -1, -1),
                ($"{6:D20}.json.bak", LogNameKind.UnknownVersioned, 6, 6),
                ($"{3:D20}.checkpoint.{3:D10}.{2:D10}.parquet", LogNameKind.UnknownVersioned, 3, 3),
                ($"{2:D20}.{1:D20}.compacted.json", LogNameKind.UnknownVersioned, 2, 2),
                ($"{4:D20}.checkpoint.not-a-uuid.json", LogNameKind.UnknownVersioned, 4, 4),
                ("99999999999999999999.json", LogNameKind.UnknownVersioned, long.MaxValue, long.MaxValue),
                ($"{7:D20}.99999999999999999999.compacted.json", LogNameKind.UnknownVersioned, 7, 7)
            };
            var classes = LogClassifier.Classify(expected.Select(x => new IOEntry("/t/_delta_log/" + x.Name)));
            Assert.Equal(expected.Length, classes.Names.Count);
            for (int i = 0; i < expected.Length; i++)
            {
                var named = classes.Names[i];
                Assert.True(expected[i].Kind == named.Kind && expected[i].Version == named.Version && expected[i].End == named.EndVersion,
                    $"{expected[i].Name}: {named.Kind} {named.Version} {named.EndVersion}");
            }
            Assert.Equal($".{5:D20}.json.stage-a.tmp", classes.Stages.Single().Entry.Name);
            Assert.True(classes.V2OrSidecar);
            Assert.Equal(2, classes.LowestUnknownVersioned);
            Assert.False(LogClassifier.Classify(expected.Where(x => x.Kind != LogNameKind.UuidCheckpoint && x.Name != "_sidecars/").Select(x => new IOEntry("/t/_delta_log/" + x.Name))).V2OrSidecar);
            Assert.True(LogClassifier.Classify(new[] { new IOEntry($"/t/_delta_log/{4:D20}.checkpoint.{uuid}.json") }).V2OrSidecar);
            Assert.True(LogClassifier.Classify(new[] { new IOEntry("/t/_delta_log/_sidecars/") }).V2OrSidecar);

            // Unknown names at or above C, unversioned names and hidden checksums do not decline
            var table = Local();
            await Create(table.Local, 14, Old);
            await Build(table.Local, 4);
            await Build(table.Local, 8);
            await File.WriteAllTextAsync(table.Log($"{10:D20}.json.bak"), "x");
            await File.WriteAllTextAsync(table.Log($"{8:D20}.unknown"), "x");
            await File.WriteAllTextAsync(table.Log("README"), "x");
            await File.WriteAllTextAsync(table.Log($".{3:D20}.json.crc"), "x");
            var cleanup = Cleanup(table);
            Assert.Equal(8, Selected(cleanup, await Run(cleanup, 14)).C);

            // An unknown versioned name below C declines
            await File.WriteAllTextAsync(table.Log($"{3:D20}.json.bak"), "x");
            AssertDeclined(await Run(cleanup, 14), MaintenanceReason.UnclassifiedLogFile);
        }

        [Fact]
        public async Task HintRaisedBeforeFirstDeletion()
        {
            var table = Local();
            await Create(table.Local, 12, Old);
            await Build(table.Local, 4);
            await Build(table.Local, 8);
            await WriteHint(table, 4);
            var buckets = new MaintenanceBuckets(TimeProvider.System);
            var cleanup = Cleanup(table, storage: MeteredFileStorage.Wrap(table.Local, buckets, ListingPaging.LocalDisk));
            long? hintWhilePlanning = null;
            long nativeWhilePlanning = 0;
            cleanup.AdvanceForTests.PlanForTests = DestinationPlanner(planning: async () =>
            {
                hintWhilePlanning = (await Hint(table))!.Version;
                nativeWhilePlanning = buckets.NativeRequests;
            });

            var result = await Run(cleanup, 12);

            Assert.True(result.Kind == OutcomeKind.Completed, $"{result.Kind} {result.Reason}: {result.Detail}");
            Assert.Equal(4, hintWhilePlanning);
            var plan = Observed(cleanup).Plan!;
            var destination = plan.Destination!;
            var hint = (await Hint(table))!;
            Assert.Equal(8, hint.Version);
            Assert.Equal(destination.Rows, hint.Size);
            Assert.Equal(destination.Proof!.Length, hint.SizeInBytes);
            Assert.Equal(destination.Adds, hint.NumOfAddFiles);
            Assert.Equal(new FileInfo(table.Log($"{8:D20}.checkpoint.parquet")).Length, hint.SizeInBytes);
            // The native temp write and rename, through the maintenance meter
            Assert.Equal(nativeWhilePlanning + 2, buckets.NativeRequests);

            // A hint above C stays while a listed checkpoint at or after it backs it
            await Build(table.Local, 12);
            var raised = Cleanup(table);
            raised.AdvanceForTests.PlanForTests = DestinationPlanner();
            Assert.Equal(OutcomeKind.Completed, (await Run(raised, 10)).Kind);
            Assert.Equal(12, (await Hint(table))!.Version);

            // Dangling, it is repaired to C
            File.Delete(table.Log($"{12:D20}.checkpoint.parquet"));
            Assert.Equal(OutcomeKind.Completed, (await Run(raised, 10)).Kind);
            var repaired = (await Hint(table))!;
            Assert.Equal(8, repaired.Version);
            Assert.Equal(new FileInfo(table.Log($"{8:D20}.checkpoint.parquet")).Length, repaired.SizeInBytes);

            // A zero-byte checkpoint does not back it
            await File.WriteAllBytesAsync(table.Log($"{12:D20}.checkpoint.parquet"), Array.Empty<byte>());
            await WriteHint(table, 12);
            Assert.Equal(OutcomeKind.Completed, (await Run(raised, 10)).Kind);
            Assert.Equal(8, (await Hint(table))!.Version);
        }

        [Fact]
        public async Task HigherHintOnACheckpointPublishedAfterTheListingIsLowered()
        {
            var table = Local();
            await Create(table.Local, 12, Old);
            await Build(table.Local, 4);
            await Build(table.Local, 8);
            var storage = OwnedHookFileStorage.Over(table.Local);
            var cleanup = Cleanup(table, storage: storage);
            var planner = cleanup.AdvanceForTests.PlanForTests;
            var planning = 0;
            cleanup.AdvanceForTests.PlanForTests = (configuration, metered, path, boundary, classes, context) =>
            {
                Volatile.Write(ref planning, 1);
                return planner(configuration, metered, path, boundary, classes, context);
            };
            // Another process publishes checkpoint 12 and its hint as planning opens its first candidate commit
            storage.Before = async (verb, path) =>
            {
                if (verb == "OpenRead" && path.Name == $"{0:D20}.json" && Interlocked.CompareExchange(ref planning, 2, 1) == 1)
                {
                    await Build(table.Local, 12);
                }
            };

            Assert.Equal(8, Selected(cleanup, await Run(cleanup, 10)).C);

            Assert.Equal(2, planning);
            var hint = (await Hint(table))!;
            Assert.Equal(8, hint.Version);
            Assert.Equal(new FileInfo(table.Log($"{8:D20}.checkpoint.parquet")).Length, hint.SizeInBytes);
        }

        [Fact]
        public async Task HigherHintOnAMultipartCheckpointIsRepaired()
        {
            var table = Local();
            await Create(table.Local, 12, Old);
            await Build(table.Local, 4);
            await Build(table.Local, 8);
            await Build(table.Local, 12);
            // Checkpoint 12 as the first of two parts, with the hint on it
            File.Move(table.Log($"{12:D20}.checkpoint.parquet"), table.Log($"{12:D20}.checkpoint.0000000001.0000000002.parquet"));
            await File.WriteAllTextAsync(table.Log("_last_checkpoint"), "{\"version\":12,\"size\":1,\"parts\":2}");
            var cleanup = Cleanup(table);

            Assert.Equal(8, Selected(cleanup, await Run(cleanup, 12)).C);
            var hint = (await Hint(table))!;
            Assert.Equal(8, hint.Version);
            Assert.Equal(new FileInfo(table.Log($"{8:D20}.checkpoint.parquet")).Length, hint.SizeInBytes);

            // With both parts listed
            await File.WriteAllTextAsync(table.Log($"{12:D20}.checkpoint.0000000002.0000000002.parquet"), "part");
            await File.WriteAllTextAsync(table.Log("_last_checkpoint"), "{\"version\":12,\"size\":1,\"parts\":2}");
            Assert.Equal(8, Selected(cleanup, await Run(cleanup, 12)).C);
            Assert.Equal(8, (await Hint(table))!.Version);
        }

        // A hint with a classic checkpoint's own sizes that claims two parts
        private static Task WriteTwoPartHint(Table table, LastCheckpointInfo classic) => File.WriteAllTextAsync(table.Log("_last_checkpoint"), JsonSerializer.Serialize(new
        {
            version = classic.Version,
            size = classic.Size,
            sizeInBytes = classic.SizeInBytes,
            numOfAddFiles = classic.NumOfAddFiles,
            parts = 2
        }));

        // The hint's raw parts property, absent as null
        private static async Task<long?> HintParts(Table table)
        {
            using var raw = JsonDocument.Parse(await File.ReadAllTextAsync(table.Log("_last_checkpoint")));
            return raw.RootElement.TryGetProperty("parts", out var parts) ? parts.GetInt64() : null;
        }

        [Fact]
        public async Task MultipartHintAtTheValidatedClassicCheckpointIsRepaired()
        {
            var table = Local();
            await Create(table.Local, 12, Old);
            await Build(table.Local, 4);
            await Build(table.Local, 8);
            var classic = (await Hint(table))!;
            Assert.Equal(8, classic.Version);
            await WriteTwoPartHint(table, classic);
            var cleanup = Cleanup(table);

            Assert.Equal(8, Selected(cleanup, await Run(cleanup, 12)).C);

            Assert.Null(await HintParts(table));
            Assert.Equal(new FileInfo(table.Log($"{8:D20}.checkpoint.parquet")).Length, (await Hint(table))!.SizeInBytes);
        }

        [Fact]
        public async Task MultipartHintWithMatchingClassicSizesIsRepaired()
        {
            var table = Local();
            await Create(table.Local, 12, Old);
            await Build(table.Local, 4);
            await Build(table.Local, 8);
            await Build(table.Local, 12);
            // On the classic checkpoint 12, above the only checkpoint the advance reads
            var classic = (await Hint(table))!;
            Assert.Equal(12, classic.Version);
            await WriteTwoPartHint(table, classic);
            var cleanup = Cleanup(table);
            cleanup.AdvanceForTests.PlanForTests = DestinationPlanner();

            Assert.Equal(OutcomeKind.Completed, (await Run(cleanup, 10)).Kind);
            Assert.Equal(8, Observed(cleanup).Boundary!.C);

            Assert.Null(await HintParts(table));
            var hint = (await Hint(table))!;
            Assert.Equal(8, hint.Version);
            Assert.Equal(new FileInfo(table.Log($"{8:D20}.checkpoint.parquet")).Length, hint.SizeInBytes);
        }

        [DuckDbExtensionFact]
        public async Task MultipartHintWithMatchingClassicSizesRemainsReadableInDuckDb()
        {
            var table = Local();
            await Create(table.Local, 12, Old);
            await Build(table.Local, 4);
            await Build(table.Local, 8);
            var classic = (await Hint(table))!;

            void Describe()
            {
                using var connection = new DuckDB.NET.Data.DuckDBConnection("DataSource=:memory:;autoinstall_known_extensions=false;autoload_known_extensions=false");
                connection.Open();
                using var command = connection.CreateCommand();
                command.CommandText = $"SET extension_directory = '{DuckDbTimeTravelProbe.ExtensionDirectory.Replace("\\", "/")}'; LOAD delta;";
                lock (DeltaTestKit.DuckDbInstallLock)
                {
                    command.ExecuteNonQuery();
                }
                command.CommandText = $"DESCRIBE SELECT * FROM delta_scan('{Path.Combine(table.Root, "t").Replace("\\", "/")}')";
                using var rows = command.ExecuteReader();
                Assert.True(rows.Read());
            }

            Describe();
            await WriteTwoPartHint(table, classic);
            var cleanup = Cleanup(table);

            Assert.Equal(8, Selected(cleanup, await Run(cleanup, 12)).C);

            Describe();
        }

        [Fact]
        public async Task UnreadableHintNamesTheNewestCheckpointTheAdvanceRead()
        {
            // C at 8; the newer 11 is too young to be C, and the plan reads it with the commit after it
            var table = Local();
            await Create(table.Local, 12, version => version <= 8 ? Old(version) : Young(version));
            await Build(table.Local, 4);
            await Build(table.Local, 8);
            await Build(table.Local, 11);
            await File.WriteAllTextAsync(table.Log("_last_checkpoint"), "{\"version\":11,");
            var cleanup = Cleanup(table);

            Assert.Equal(8, Selected(cleanup, await Run(cleanup, 12, interval: 20)).C);

            var hint = (await Hint(table))!;
            Assert.Equal(11, hint.Version);
            Assert.Equal(new FileInfo(table.Log($"{11:D20}.checkpoint.parquet")).Length, hint.SizeInBytes);

            // A hint between C and the newest checkpoint read is raised to it
            await WriteHint(table, 9);
            Assert.Equal(8, Selected(cleanup, await Run(cleanup, 12, interval: 20)).C);
            Assert.Equal(11, (await Hint(table))!.Version);

            // A hint on it with wrong sizes is repaired
            await File.WriteAllTextAsync(table.Log("_last_checkpoint"), "{\"version\":11,\"size\":1,\"sizeInBytes\":1}");
            Assert.Equal(8, Selected(cleanup, await Run(cleanup, 12, interval: 20)).C);
            hint = (await Hint(table))!;
            Assert.Equal(11, hint.Version);
            Assert.Equal(new FileInfo(table.Log($"{11:D20}.checkpoint.parquet")).Length, hint.SizeInBytes);
        }

        [Fact]
        public async Task UnsupportedNewerCheckpointIsNotReadForProtection()
        {
            // C at 8 and a newer checkpoint at 11 the worker's builds found unsupported
            var table = Local();
            await Create(table.Local, 12, version => version <= 8 ? Old(version) : Young(version));
            await Build(table.Local, 4);
            await Build(table.Local, 8);
            await File.WriteAllTextAsync(table.Log($"{11:D20}.checkpoint.parquet"), "torn");
            var listing = (await DeltaLogSegment.ListAsync(table.Local, "t")).Listing;
            var unsupported = new HashSet<ObjectIdentity>() { ObjectIdentity.Of(table.Configuration.Store.Table, listing.Checkpoints.Single(x => x.Version == 11)) };
            var cleanup = Cleanup(table);
            var planner = cleanup.AdvanceForTests.PlanForTests;
            long estimated = -1;
            long spent = -1;
            cleanup.AdvanceForTests.PlanForTests = async (configuration, metered, path, boundary, classes, context) =>
            {
                estimated = AdvancePlanner.Reads(boundary, classes);
                var before = context.ReadsUsed;
                var plan = await planner(configuration, metered, path, boundary, classes, context);
                spent = context.ReadsUsed - before;
                return plan;
            };

            var (result, _) = await RunWith(cleanup, Context(12, new FakeTimeProvider(s_now), interval: 20, unsupported: unsupported));

            Assert.Equal(8, Selected(cleanup, result).C);
            Assert.Equal(8, (await Hint(table))!.Version);
            // The selection counts the reads the planner makes
            Assert.Equal(estimated, spent);
        }

        [Fact]
        public async Task AdvanceKeepsNoListingOrPlanAfterItEnds()
        {
            var table = Local();
            await Create(table.Local, 12, Old);
            await Build(table.Local, 4);
            await Build(table.Local, 8);
            await WriteHint(table, 4);
            var cleanup = new DeltaTableCleanup(table.Configuration, table.Local, new FakeTimeProvider(s_now), "t", null, () => table.Local, new DiscardCounters(), null, "t");

            Assert.Equal(OutcomeKind.Completed, (await Run(cleanup, 12)).Kind);
            Assert.Equal(8, (await Hint(table))!.Version);

            // Nothing history-sized outlives an advance on the job the sink run keeps
            var fields = typeof(AdvanceJob).GetFields(BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public);
            Assert.DoesNotContain(fields, x => x.GetValue(cleanup.AdvanceForTests) is AdvanceBoundary or AdvancePlan or LogEvidence or PreparedAdvance);
        }

        [Fact]
        public async Task AdvanceStoppedWhilePlanningStaysDueAndKeepsTheHint()
        {
            var table = Local();
            await Create(table.Local, 12, Old);
            await Build(table.Local, 4);
            await Build(table.Local, 8);
            await WriteHint(table, 4);
            var storage = OwnedHookFileStorage.Over(table.Local);
            var cleanup = Cleanup(table, storage: storage);
            var planner = cleanup.AdvanceForTests.PlanForTests;
            var planning = false;
            cleanup.AdvanceForTests.PlanForTests = (configuration, metered, path, boundary, classes, context) =>
            {
                planning = true;
                return planner(configuration, metered, path, boundary, classes, context);
            };
            // The deadline passes as planning opens its first candidate commit
            var clock = new FakeTimeProvider(s_now);
            storage.Before = (verb, path) =>
            {
                if (planning && verb == "OpenRead" && path.Name == $"{0:D20}.json")
                {
                    planning = false;
                    clock.Advance(TimeSpan.FromHours(2));
                }
                return Task.CompletedTask;
            };

            var (stopped, _) = await RunWith(cleanup, Context(12, clock));

            AssertDeclined(stopped, MaintenanceReason.JobDeadline);
            Assert.Equal(4, (await Hint(table))!.Version);
            Assert.True(cleanup.IsDue(JobKind.Advance, 12, s_now));

            // The retry completes and settles the day
            Assert.Equal(8, Selected(cleanup, await Run(cleanup, 12)).C);
            Assert.Equal(8, (await Hint(table))!.Version);
            Assert.False(cleanup.IsDue(JobKind.Advance, 12, s_now));
        }

        [Fact]
        public async Task ShortStepRecordsTheCapItsBoundaryMet()
        {
            // Checkpoints every ten versions to 80, and five commits after it within an interval of ten
            var table = Local();
            await Create(table.Local, 85, Old);
            for (long version = 10; version <= 80; version += 10)
            {
                await Build(table.Local, version);
            }
            // The reads selecting 80 spends, as planning starts
            var probe = Cleanup(table);
            long selecting = 0;
            probe.AdvanceForTests.PlanForTests = (configuration, storage, path, boundary, classes, context) =>
            {
                selecting = context.ReadsUsed;
                return AdvancePlanner.PlanAsync(configuration, storage, path, boundary, classes, context);
            };
            Assert.Equal(80, Selected(probe, await Run(probe, 85, readLimit: 1000, interval: 10)).C);

            // Then exactly the reads a plan to 80 makes: its 80 candidates, C and C.json
            var cleanup = Cleanup(table);
            var (result, context) = await RunWith(cleanup, Context(85, new FakeTimeProvider(s_now), readLimit: selecting + 82, interval: 10));
            var boundary = Selected(cleanup, result);

            Assert.Equal((80L, AdvanceStep.Short), (boundary.C, boundary.Step));
            Assert.True(boundary.C - boundary.O <= boundary.EffectiveShortCap, $"A short step of {boundary.C - boundary.O} recorded the cap {boundary.EffectiveShortCap}");
            Assert.Equal(context.ReadLimit, context.ReadsUsed);
        }

        [Fact]
        public async Task ShortAdvanceWhoseLastPlanOutgrewItsReadsPromotesBeforeReading()
        {
            // C at 8 holds over two hundred live files, each a native resolution counted as a read
            var table = Local();
            for (long version = 0; version <= 12; version++)
            {
                var actions = new List<DeltaAction>() { DeltaLakeCheckpointBuildTests.Add($"part-{version:D4}.parquet") };
                if (version == 0)
                {
                    actions.InsertRange(0, new[] { DeltaTestKit.Protocol(), DeltaTestKit.Metadata(DeltaTestKit.UserSchema, Published()) }
                        .Concat(Enumerable.Range(0, 200).Select(x => DeltaLakeCheckpointBuildTests.Add($"part-a-{x:D4}.parquet"))));
                }
                await DeltaTestKit.WriteOwnCommit(table.Local, "t", version, 0, Old(version), actions.ToArray());
            }
            await Build(table.Local, 4);
            await Build(table.Local, 8);
            var cleanup = Cleanup(table, storage: MeteredFileStorage.Wrap(table.Local, MaintenanceBucketsTests.Unthrottled(), ListingPaging.LocalDisk));
            var planner = cleanup.AdvanceForTests.PlanForTests;
            long planning = 0;
            cleanup.AdvanceForTests.PlanForTests = async (configuration, storage, path, boundary, classes, context) =>
            {
                var before = context.ReadsUsed;
                var plan = await planner(configuration, storage, path, boundary, classes, context);
                planning = context.ReadsUsed - before;
                return plan;
            };
            async Task<CleanupResult> AdvanceAsync(long readLimit, bool isLong = false)
            {
                planning = 0;
                return await Run(cleanup, 12, isLong, readLimit);
            }

            // The first short plan reads C before its resolutions outgrow the reads left
            Assert.True((await AdvanceAsync(100)).NeedsLongAdmission);
            Assert.True(planning > 0, $"{planning} planning reads");

            // Later short attempts promote before any planning read, also after a long plan
            Assert.True((await AdvanceAsync(100)).NeedsLongAdmission);
            Assert.Equal(0, planning);
            Assert.Equal(8, Selected(cleanup, await AdvanceAsync(long.MaxValue, isLong: true)).C);
            Assert.True((await AdvanceAsync(100)).NeedsLongAdmission);
            Assert.Equal(0, planning);

            // A short admission with reads for them plans
            Assert.Equal(8, Selected(cleanup, await AdvanceAsync(1000)).C);
            Assert.True(planning > 100, $"{planning} planning reads");
        }

        [Fact]
        public async Task UnknownNameBelowEveryBoundaryDeclinesBeforeSelection()
        {
            // No eligible checkpoint over the unmarked root at 8: the advance asks for a cutoff build at 7
            var table = Local();
            await Create(table.Local, 12, version => version <= 8 ? Old(version) : Young(version));
            await DeltaLakeCheckpointBuildTests.LegacyRoot(table.Local, "t", 8);
            var cleanup = Cleanup(table);
            var (demanded, demandedContext) = await RunWith(cleanup, Context(12, new FakeTimeProvider(s_now)));
            Assert.Equal(new BuildDemand(7, RootPolicy.Auto), demanded.Demand);

            // A stray versioned name below every possible C declines before the selection reads
            var stray = table.Log($"{0:D20}.json.bak");
            await File.WriteAllTextAsync(stray, "x");
            var (below, belowContext) = await RunWith(cleanup, Context(12, new FakeTimeProvider(s_now)));
            AssertDeclined(below, MaintenanceReason.UnclassifiedLogFile);
            Assert.Null(below.Demand);
            Assert.True(belowContext.ReadsUsed < demandedContext.ReadsUsed, $"{belowContext.ReadsUsed} reads, the demand took {demandedContext.ReadsUsed}");
            File.Delete(stray);

            // One below the cutoff build's target declines instead of asking for a checkpoint the advance could not use
            await File.WriteAllTextAsync(table.Log($"{5:D20}.json.bak"), "x");
            var target = await Run(cleanup, 12);
            AssertDeclined(target, MaintenanceReason.UnclassifiedLogFile);
            Assert.Null(target.Demand);
        }

        [Fact]
        public async Task AdvancePlansWithTheWindowedPlannerAndRaisesTheHint()
        {
            // a and b are removed before C at 8, c after it
            var table = Local();
            for (long version = 0; version <= 12; version++)
            {
                var actions = new List<DeltaAction>() { DeltaLakeCheckpointBuildTests.Add($"part-{version:D4}.parquet") };
                switch (version)
                {
                    case 0:
                        actions.InsertRange(0, new[] { DeltaTestKit.Protocol(), DeltaTestKit.Metadata(DeltaTestKit.UserSchema, Published()), DeltaLakeCheckpointBuildTests.Add("part-a.parquet"),
                            DeltaLakeCheckpointBuildTests.Add("part-b.parquet"), DeltaLakeCheckpointBuildTests.Add("part-c.parquet") });
                        break;
                    case 2:
                        actions.Add(DeltaLakeCheckpointBuildTests.Remove("part-a.parquet", Old(version)));
                        break;
                    case 5:
                        actions.Add(DeltaLakeCheckpointBuildTests.Remove("part-b.parquet", Old(version)));
                        break;
                    case 10:
                        actions.Add(DeltaLakeCheckpointBuildTests.Remove("part-c.parquet", Old(version)));
                        break;
                }
                await DeltaTestKit.WriteOwnCommit(table.Local, "t", version, 0, Old(version), actions.ToArray());
            }
            await Build(table.Local, 4);
            await Build(table.Local, 8);
            await WriteHint(table, 4);
            var cleanup = Cleanup(table);
            // Read as the planner hands them over, before the job disposes the plan's parts
            var planner = cleanup.AdvanceForTests.PlanForTests;
            List<string>? emitted = null;
            cleanup.AdvanceForTests.PlanForTests = async (configuration, storage, path, boundary, classes, context) =>
            {
                var planned = await planner(configuration, storage, path, boundary, classes, context);
                emitted = AdvancePlannerKit.Emitted(planned, AdvanceWindowLimits.Default);
                return planned;
            };

            var boundary = Selected(cleanup, await Run(cleanup, 12));

            Assert.Equal(8, boundary.C);
            var plan = Observed(cleanup).Plan!;
            Assert.NotEmpty(plan.Parts);
            Assert.Equal(new[] { "PART-A.PARQUET", "PART-B.PARQUET" }, emitted!.Order(StringComparer.Ordinal));
            var destination = plan.Destination!;
            Assert.True(destination.GapFree);
            var hint = (await Hint(table))!;
            Assert.Equal(8, hint.Version);
            Assert.Equal(destination.Rows, hint.Size);
            Assert.Equal(destination.Proof!.Length, hint.SizeInBytes);
            Assert.Equal(destination.Adds, hint.NumOfAddFiles);
        }

        [Fact]
        public async Task CutoffWaitsForYoungRoot()
        {
            var table = Local();
            await Create(table.Local, 12, version => version <= 8 ? Old(version) : Young(version));
            // An unmarked root at 4, and the coverage repair's gap-free root at 10 still younger than the cutoff
            await DeltaLakeCheckpointBuildTests.LegacyRoot(table.Local, "t", 4);
            await Build(table.Local, 10, RootPolicy.None);
            var cleanup = Cleanup(table);

            var waits = await Run(cleanup, 12);

            Assert.True(waits.Kind == OutcomeKind.NoWork && waits.Reason == MaintenanceReason.NoAdmissibleRoot, $"{waits.Kind} {waits.Reason}: {waits.Detail}");
            Assert.Null(waits.Demand);

            // Without the unmarked root an Auto build at 8 replays from zero and is gap-free
            File.Delete(table.Log($"{4:D20}.checkpoint.parquet"));
            var demands = await Run(cleanup, 12);
            Assert.Equal(MaintenanceReason.NoEligibleCheckpoint, demands.Reason);
            Assert.Equal(new BuildDemand(8, RootPolicy.Auto), demands.Demand);
        }

        [Fact]
        public async Task CutoffDemandTargetsNewestUncheckpointedVersion()
        {
            var table = Local();
            await Create(table.Local, 12, version => version <= 8 ? Old(version) : Young(version));
            await DeltaLakeCheckpointBuildTests.LegacyRoot(table.Local, "t", 8);
            var cleanup = Cleanup(table);

            var result = await Run(cleanup, 12);

            Assert.Equal(OutcomeKind.NoWork, result.Kind);
            Assert.Equal(new BuildDemand(7, RootPolicy.Auto), result.Demand);
            Assert.False(result.NeedsLongAdmission);
        }

        [Fact]
        public async Task CutoffDemandFollowsTheRootTheBuildTakes()
        {
            var table = Local();
            // Commits before 7 from before adoption; 7 and 8 before the cutoff, the rest within the last hour
            await Create(table.Local, 12, version => version <= 8 ? Old(version) : Young(version), adoptedAt: 7);
            // An unmarked root at 2 and a gap-free one at 6, both below adoption
            await DeltaLakeCheckpointBuildTests.LegacyRoot(table.Local, "t", 2);
            await Build(table.Local, 6, RootPolicy.None);
            var listing = (await DeltaLogSegment.ListAsync(table.Local, "t")).Listing;
            var six = new HashSet<ObjectIdentity>() { ObjectIdentity.Of(table.Configuration.Store.Table, listing.Checkpoints.Single(x => x.Version == 6)) };
            var cleanup = Cleanup(table);

            // Rooted on 6, an Auto build at 8 is gap-free
            Assert.Equal(new BuildDemand(8, RootPolicy.Auto), (await Run(cleanup, 12)).Demand);

            // The worker's builds skip 6 and would root on the unmarked 2: the advance waits instead
            var skipped = (await RunWith(cleanup, Context(12, new FakeTimeProvider(s_now), unsupported: six))).Result;
            Assert.True(skipped.Kind == OutcomeKind.NoWork && skipped.Reason == MaintenanceReason.NoAdmissibleRoot && skipped.Demand == null,
                $"{skipped.Kind} {skipped.Reason}: {skipped.Detail} {skipped.Demand}");

            // Builds that replay a legacy lineage from zero instead are gap-free again
            var rebuilt = (await RunWith(cleanup, Context(12, new FakeTimeProvider(s_now), unsupported: six, rebuildLegacy: true))).Result;
            Assert.Equal(new BuildDemand(8, RootPolicy.Auto), rebuilt.Demand);

            // The demand the advance did not make would have inherited the gap
            var built = await Build(table.Local, 8, unsupported: six);
            Assert.Equal(2, built.RootVersion);
            Assert.NotNull(built.Checkpoint!.Marker.Inherited);

            // With no other root the build declines, unless the option replays from zero
            var lone = Local();
            await Create(lone.Local, 12, version => version <= 8 ? Old(version) : Young(version), adoptedAt: 7);
            await Build(lone.Local, 6, RootPolicy.None);
            var loneSix = new HashSet<ObjectIdentity>() { ObjectIdentity.Of(lone.Configuration.Store.Table, (await DeltaLogSegment.ListAsync(lone.Local, "t")).Listing.Checkpoints.Single()) };
            var loneCleanup = Cleanup(lone);
            Assert.Null((await RunWith(loneCleanup, Context(12, new FakeTimeProvider(s_now), unsupported: loneSix))).Result.Demand);
            Assert.Equal(new BuildDemand(8, RootPolicy.Auto), (await RunWith(loneCleanup, Context(12, new FakeTimeProvider(s_now), unsupported: loneSix, rebuildLegacy: true))).Result.Demand);
        }

        [Fact]
        public async Task CutoffRootWalkCountsEachFetch()
        {
            var table = Local();
            await Create(table.Local, 12, version => version <= 8 ? Old(version) : Young(version), adoptedAt: 7);
            await DeltaLakeCheckpointBuildTests.LegacyRoot(table.Local, "t", 2);
            await File.WriteAllTextAsync(table.Log($"{6:D20}.checkpoint.parquet"), "torn");
            var listing = (await DeltaLogSegment.ListAsync(table.Local, "t")).Listing;
            var torn = new HashSet<ObjectIdentity>() { ObjectIdentity.Of(table.Configuration.Store.Table, listing.Checkpoints.Single(x => x.Version == 6)) };
            var cleanup = Cleanup(table);

            // The walk to the unmarked 2 fetches the torn 6 too, unless the worker's builds already skip it
            var (walked, walkedContext) = await RunWith(cleanup, Context(12, new FakeTimeProvider(s_now)));
            var (skipped, skippedContext) = await RunWith(cleanup, Context(12, new FakeTimeProvider(s_now), unsupported: torn));

            Assert.True(walked.Kind == OutcomeKind.NoWork && walked.Demand == null, $"{walked.Kind} {walked.Reason}: {walked.Detail}");
            Assert.True(skipped.Kind == OutcomeKind.NoWork && skipped.Demand == null, $"{skipped.Kind} {skipped.Reason}: {skipped.Detail}");
            Assert.Equal(skippedContext.ReadsUsed + 1, walkedContext.ReadsUsed);
        }

        [Fact]
        public async Task PromotionAfterPreparationRepeatsItOnce()
        {
            var table = Local();
            await Create(table.Local, 30, Old);
            await Build(table.Local, 10);
            await Build(table.Local, 20);
            var spent = await StepSpend(table, 30);

            var budget = new DeltaMaintenanceBudget(new MaintenanceBudgetSettings(2, Path.Combine(_directory, ScratchOwners.ShortId())), new FakeTimeProvider(s_now), manualTick: true);
            var options = new FakeTimeProvider(s_now);
            var cleanup = Cleanup(table, options);
            var plans = new List<(bool Long, long ReadsUsed, AdvanceStep Step)>();
            cleanup.AdvanceForTests.PlanForTests = (configuration, storage, path, boundary, classes, context) =>
            {
                lock (plans)
                {
                    plans.Add((context.Long, context.ReadsUsed, boundary.Step));
                }
                return Task.FromResult(new AdvancePlan() { Status = AdvancePlanStatus.NotBuilt });
            };
            var settings = new MaintenanceWorkerSettings(0, null, options) { PeriodicEnabled = false, InspectionEnabled = false, ShortReads = spent + 5 };
            var worker = new DeltaMaintenanceWorker(budget, settings, table.Local, "t", "t", null) { Cleanup = cleanup };
            budget.Register(worker);
            worker.Published(30);

            // The staged sweep runs first, then the advance
            for (int i = 0; i < 10 && plans.Count == 0; i++)
            {
                budget.TickNow();
                await worker.CurrentJob.WaitAsync(s_wait);
            }

            var plan = Assert.Single(plans);
            Assert.True(plan.Long);
            Assert.True(plan.ReadsUsed > 0);
            Assert.Equal(AdvanceStep.Long, plan.Step);
            Assert.Equal(OutcomeKind.NoWork, worker.LastOutcomeForTests!.Kind);
            Assert.Equal(0, worker.CleanupFailures(JobKind.Advance));
            Assert.False(cleanup.IsDue(JobKind.Advance, 30, options.GetUtcNow()));
            await worker.StopAsync(s_wait);
        }

        [Fact]
        public async Task ConflictingAuthorityDeclines()
        {
            var table = Local();
            await Create(table.Local, 10, Old);
            await Build(table.Local, 8);
            var cleanup = Cleanup(table);
            // Parts and headers carry no authority, and with nothing started they are scavenged
            var leftovers = new[] { table.Log($".flowtide-cleanup.{8:D20}.advance.part000001.attempt"), table.Log($".flowtide-cleanup.{8:D20}.advance.header.attempt") };
            foreach (var leftover in leftovers)
            {
                await File.WriteAllTextAsync(leftover, "x");
            }
            Assert.Equal(8, Selected(cleanup, await Run(cleanup, 10)).C);
            Assert.DoesNotContain(leftovers, File.Exists);

            // Torn authority suspends
            foreach (var suffix in new[] { "started", "logs-complete", $"p{1:D10}" })
            {
                var record = table.Log($".flowtide-cleanup.{6:D20}.advance.0123456789abcdef.attempt.{suffix}");
                await File.WriteAllTextAsync(record, "x");
                AssertDeclined(await Run(cleanup, 10), MaintenanceReason.ManifestSuspended);
                Assert.True(File.Exists(record));
                File.Delete(record);
            }

            // A record this version cannot interpret fences the listing itself
            await File.WriteAllTextAsync(table.Log($".flowtide-cleanup.{6:D20}.future-form"), "x");
            AssertDeclined(await Run(cleanup, 10), MaintenanceReason.ListingIneligible);
        }

        [Fact]
        public async Task SidecarFolderDeclinesOnLocalDisk()
        {
            var table = Local();
            await Create(table.Local, 10, Old);
            await Build(table.Local, 8);
            var cleanup = Cleanup(table);
            Assert.Equal(8, Selected(cleanup, await Run(cleanup, 10)).C);

            Directory.CreateDirectory(table.Log("_sidecars"));
            await File.WriteAllTextAsync(Path.Combine(table.Log("_sidecars"), $"{Guid.NewGuid():D}.parquet"), "x");

            AssertDeclined(await Run(cleanup, 10), MaintenanceReason.SidecarRoot);
            Assert.True(Directory.Exists(table.Log("_sidecars")));
        }

        [Fact]
        public async Task CheckpointAboveVIsNeverC()
        {
            var table = Local();
            await Create(table.Local, 12, Old);
            await Build(table.Local, 6);
            await Build(table.Local, 12);
            var cleanup = Cleanup(table);

            var boundary = Selected(cleanup, await Run(cleanup, 10));

            Assert.Equal(6, boundary.C);
            Assert.Equal(10, boundary.V);
            Assert.Equal(12, boundary.Head);
            Assert.Equal(12, Selected(cleanup, await Run(cleanup, 12)).C);
        }

        [Fact]
        public async Task CheckpointBelowAdoptionIsNeverC()
        {
            var table = Local();
            await Create(table.Local, 10, Old, adoptedAt: 5);
            await Build(table.Local, 3);
            var cleanup = Cleanup(table);

            var result = await Run(cleanup, 10);
            Assert.Equal(OutcomeKind.NoWork, result.Kind);
            Assert.Equal(new BuildDemand(10, RootPolicy.Auto), result.Demand);

            await Build(table.Local, 7);
            var boundary = Selected(cleanup, await Run(cleanup, 10));
            Assert.Equal(5, boundary.A);
            Assert.Equal(7, boundary.C);
        }

        [Fact]
        public async Task RetentionNotPublishedDeclines()
        {
            var table = Local();
            var other = new Dictionary<string, string>()
            {
                [RetentionProperties.LogKey] = RetentionProperties.Format(s_retention),
                [RetentionProperties.DeletedFileKey] = "interval 7 days"
            };
            await Create(table.Local, 10, Old, other);
            await Build(table.Local, 8);
            var cleanup = Cleanup(table);

            AssertDeclined(await Run(cleanup, 10), MaintenanceReason.RetentionNotPublished);
            Assert.Null(Observed(cleanup).Boundary);
        }

        [Fact]
        public async Task AdvanceIsDueOncePerDayFromTheTableOffset()
        {
            var table = Local();
            await Create(table.Local, 10, Old);
            await Build(table.Local, 8);
            var offset = TimeSpan.FromHours(3);
            var dayStart = new DateTimeOffset(2026, 10, 15, 3, 0, 0, TimeSpan.Zero);
            var cleanup = AdvanceExecutionTests.PlanOnly(new DeltaTableCleanup(table.Configuration, table.Local, new FakeTimeProvider(s_now), "t", null, () => table.Local, new DiscardCounters(), null, "t")
            {
                AdvanceOffset = offset
            });

            // Stable per table and within the day
            Assert.Equal(Cleanup(table).AdvanceOffset, Cleanup(table).AdvanceOffset);
            Assert.InRange(Cleanup(table).AdvanceOffset, TimeSpan.Zero, TimeSpan.FromDays(1) - TimeSpan.FromTicks(1));

            // Due at the run's first chance, then once a day at the offset
            Assert.True(cleanup.IsDue(JobKind.Advance, 10, s_now));
            Assert.Equal(OutcomeKind.Completed, (await Run(cleanup, 10)).Kind);
            Assert.False(cleanup.IsDue(JobKind.Advance, 10, s_now));
            Assert.False(cleanup.IsDue(JobKind.Advance, 10, dayStart + TimeSpan.FromDays(1) - TimeSpan.FromTicks(1)));
            Assert.True(cleanup.IsDue(JobKind.Advance, 10, dayStart + TimeSpan.FromDays(1)));

            // A run restarted each day after the offset concludes its advance at once
            for (int day = 1; day <= 3; day++)
            {
                var restartClock = new FakeTimeProvider(dayStart + TimeSpan.FromDays(day) + TimeSpan.FromMinutes(30));
                var restarted = AdvanceExecutionTests.PlanOnly(new DeltaTableCleanup(table.Configuration, table.Local, restartClock, "t", null, () => table.Local, new DiscardCounters(), null, "t")
                {
                    AdvanceOffset = offset
                });
                Assert.True(restarted.IsDue(JobKind.Advance, 10, restartClock.GetUtcNow()));
                Assert.Equal(OutcomeKind.Completed, (await Run(restarted, 10)).Kind);
                Assert.False(restarted.IsDue(JobKind.Advance, 10, restartClock.GetUtcNow()));
            }

            // Retention off, or a replaced storage, is never due
            var off = new DeltaTableCleanup(CleanupConfiguration.Validate(table.Local, "t", false, CleanupOptIns.Off), table.Local, new FakeTimeProvider(s_now), "t", null, () => table.Local,
                new DiscardCounters(), null, "t");
            Assert.False(off.IsDue(JobKind.Advance, 10, s_now));
            var replaced = Cleanup(table, current: () => Files.Of.InternalMemory("./bs-replaced"));
            Assert.False(replaced.IsDue(JobKind.Advance, 10, s_now));

            // A decline that waits for the sink stays due
            var unpublished = Local();
            await Create(unpublished.Local, 3, Old, new Dictionary<string, string>());
            var waiting = Cleanup(unpublished);
            AssertDeclined(await Run(waiting, 3), MaintenanceReason.RetentionNotPublished);
            Assert.True(waiting.IsDue(JobKind.Advance, 3, s_now));
        }
    }
}
