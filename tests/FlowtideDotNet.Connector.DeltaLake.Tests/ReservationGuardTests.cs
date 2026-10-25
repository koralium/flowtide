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
using Microsoft.Extensions.Time.Testing;
using Stowage;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public class ReservationGuardTests : IDisposable
    {
        private static readonly DateTimeOffset s_now = new DateTimeOffset(2026, 10, 15, 0, 0, 0, TimeSpan.Zero);

        private const string Log = "/t/_delta_log/";
        private const string Advance = "0123456789abcdef0123456789abcdef";
        private const string Digest = "0123456789abcdef";
        private const string Attempt = "fedcba9876543210fedcba9876543210";

        private readonly string _directory = Path.Combine(Path.GetTempPath(), "rg-" + ScratchOwners.ShortId());

        public void Dispose()
        {
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

        // A record of a started advance's group
        private static string Group(long boundary, string suffix) => $".flowtide-cleanup.{boundary:D20}.{Advance}.{Digest}.{Attempt}.{suffix}";

        private static string Part(long boundary) => $".flowtide-cleanup.{boundary:D20}.{Advance}.part000001.{Attempt}";

        private static string Header(long boundary) => $".flowtide-cleanup.{boundary:D20}.{Advance}.header.{Attempt}";

        private static ControlFence Fence(params string[] names) => ControlFence.Of(names.Select(x => new IOEntry(Log + x) { Size = 1 }).ToList());

        private static string CheckpointPath(long version) => $"{Log}{version:D20}.checkpoint.parquet";

        private sealed class Rig
        {
            public required IFileStorage Storage { get; init; }

            public required DeltaMaintenanceBudget Budget { get; init; }

            public required FakeTimeProvider OptionsClock { get; init; }

            public required DeltaMaintenanceWorker Worker { get; init; }

            public required DeltaMaintenanceWorkerTests.CollectingLogger Logs { get; init; }

            public async Task TickAsync()
            {
                Budget.TickNow();
                await Worker.CurrentJob;
            }

            // Past the latest-head backoff on the options clock
            public void PastRetry()
            {
                var wait = Worker.RetryAt - OptionsClock.GetUtcNow();
                if (wait > TimeSpan.Zero)
                {
                    OptionsClock.Advance(wait);
                }
            }
        }

        // The worker learns the store's eligibility as the sink decides it
        private Rig Create(IFileStorage storage, int interval = 2)
        {
            var budget = new DeltaMaintenanceBudget(new MaintenanceBudgetSettings(2, Path.Combine(_directory, ScratchOwners.ShortId())), new FakeTimeProvider(s_now), manualTick: true)
            {
                Buckets = MaintenanceBucketsTests.Unthrottled()
            };
            var optionsClock = new FakeTimeProvider(s_now);
            var logs = new DeltaMaintenanceWorkerTests.CollectingLogger();
            var eligible = CleanupStoreContract.Classify(storage, "t", false).Eligible;
            var worker = new DeltaMaintenanceWorker(budget, new MaintenanceWorkerSettings(interval, null, optionsClock) { CleanupEligible = eligible }, storage, "t", "t", logs);
            budget.Register(worker);
            return new Rig() { Storage = storage, Budget = budget, OptionsClock = optionsClock, Worker = worker, Logs = logs };
        }

        // Copied reads, so a removed commit stays removed
        private static IFileStorage Memory() => new HookFileStorage(Files.Of.InternalMemory($"./reservation-{Guid.NewGuid():N}"));

        private IFileStorage Local()
        {
            var root = Path.Combine(_directory, "l" + ScratchOwners.ShortId());
            Directory.CreateDirectory(root);
            return Files.Of.LocalDisk(root);
        }

        // A legacy checkpoint without the hint its writer leaves
        private static async Task Root(IFileStorage storage, long version)
        {
            await DeltaLakeCheckpointBuildTests.LegacyRoot(storage, "t", version);
            await storage.Rm(Log + "_last_checkpoint");
        }

        // Commits 0 to 6, a legacy checkpoint at 3 and a torn one at 5; without history, commits 0 to 2 are gone
        private static async Task TornTable(IFileStorage storage, bool fromZero)
        {
            await DeltaMaintenanceWorkerTests.Commits(storage, 0, 6);
            await Root(storage, 3);
            await storage.WriteText(CheckpointPath(5), "torn");
            if (!fromZero)
            {
                for (int version = 0; version < 3; version++)
                {
                    await storage.Rm($"{Log}{version:D20}.json");
                }
            }
        }

        [Theory]
        [InlineData("part000001.A", "Part", 1)]
        [InlineData("header.A", "Header", -1)]
        [InlineData("0123456789abcdef.A.started", "Started", -1)]
        [InlineData("0123456789ABCDEF.A.logs-complete", "LogsComplete", -1)]
        [InlineData("0123456789abcdef.A.p0000000003", "Progress", 3)]
        [InlineData("0123456789abcdef.A.done", "Done", -1)]
        // Future or damaged forms
        [InlineData("0123456789abcdef.A.compacting", null, -1)]
        [InlineData("0123456789abcdef.A.started.tmp", null, -1)]
        [InlineData("0123456789abcdef.A.Started", null, -1)]
        [InlineData("0123456789abcde.A.started", null, -1)]
        [InlineData("0123456789abcdeg.A.started", null, -1)]
        [InlineData("0123456789abcdef..started", null, -1)]
        [InlineData("0123456789abcdef.A+B.done", null, -1)]
        [InlineData("0123456789abcdef.A.p123", null, -1)]
        [InlineData("part01.A", null, -1)]
        [InlineData("part00000x.A", null, -1)]
        [InlineData("headers.A", null, -1)]
        [InlineData("header", null, -1)]
        public void ControlNamesParseOnlyTheirExactForms(string tail, string? kind, long ordinal)
        {
            var name = $".flowtide-cleanup.{7:D20}.{Advance}.{tail}";

            Assert.Equal(kind != null, ControlName.TryParse(name, out var parsed));
            if (kind != null)
            {
                Assert.Equal((Enum.Parse<ControlKind>(kind), 7L, Advance, ordinal), (parsed.Kind, parsed.Boundary, parsed.AdvanceId, parsed.Ordinal));
                Assert.Equal("A", parsed.AttemptId);
            }
            // Every form keeps its boundary, which an uninterpreted record still reserves
            Assert.True(ControlName.TryParseBoundary(name, out var boundary));
            Assert.Equal(7, boundary);
        }

        [Theory]
        [InlineData(".flowtide-cleanup.x")]
        [InlineData(".flowtide-cleanup.0000000000000000007.a.0123456789abcdef.b.started")]
        [InlineData(".flowtide-cleanup.99999999999999999999.a.0123456789abcdef.b.started")]
        [InlineData(".flowtide-cleanup.00000000000000000007x.a.0123456789abcdef.b.started")]
        [InlineData(".flowtide-cleanup.0000000000000000000\uFF17.a.0123456789abcdef.b.started")]
        public void UnparseableBoundaryIsUnknown(string name)
        {
            Assert.False(ControlName.TryParse(name, out _));
            Assert.False(ControlName.TryParseBoundary(name, out _));
        }

        [Fact]
        public void ParseableStartReservesItsBoundary()
        {
            var fence = Fence(Group(5, "started"));

            Assert.Equal(5, fence.ReservedFloor);
            Assert.Equal(1, fence.Reservations);
            Assert.False(fence.Fenced);
            Assert.False(fence.Clear);
            // No checkpoint is created, repaired, replaced or reclaimed at or below the boundary
            Assert.False(fence.AllowsCreate(5));
            Assert.True(fence.AllowsCreate(6));
            Assert.False(fence.AllowsReplace(5));
            Assert.True(fence.AllowsReplace(6));
            Assert.False(fence.AllowsTornRepair(5, true, true));
            Assert.True(fence.AllowsTornRepair(6, true, false));
            // The hint never moves below it
            Assert.False(fence.AllowsHint(4));
            Assert.True(fence.AllowsHint(5));
            // A reservation alone stops no payload deletion
            Assert.True(fence.AllowsDeletion);

            // Logs-complete and progress are evidence their group started; the highest boundary holds
            Assert.Equal(7, Fence(Group(7, "logs-complete")).ReservedFloor);
            Assert.Equal(9, Fence(Group(9, "p0000000001")).ReservedFloor);
            var both = Fence(Group(5, "started"), Group(3, "p0000000001"));
            Assert.Equal((5L, 2), (both.ReservedFloor, both.Reservations));
        }

        [Fact]
        public void NamedDoneReleasesNothing()
        {
            var fence = Fence(Group(5, "started"), Group(5, "done"), Group(8, "done"));
            Assert.Equal(5, fence.ReservedFloor);
            Assert.False(fence.AllowsCreate(5));

            // Done, parts and headers carry no authority of their own
            var settled = Fence(Group(5, "done"), Part(6), Header(6));
            Assert.True(settled.Clear);
            Assert.True(settled.AllowsCreate(0));
            Assert.True(settled.AllowsTornRepair(0, false, true));
            Assert.Same(ControlFence.None, Fence());
        }

        [Fact]
        public async Task UnknownCleanupBoundaryReservesLongMaxValue()
        {
            foreach (var name in new[] { ".flowtide-cleanup.x", ".flowtide-cleanup.99999999999999999999.a.0123456789abcdef.b.started", ".flowtide-cleanup.00000000000000000003" + "x" })
            {
                var unknown = Fence(Group(5, "started"), name);
                Assert.Equal(long.MaxValue, unknown.ReservedFloor);
                Assert.False(unknown.AllowsCreate(long.MaxValue - 1));
                Assert.False(unknown.AllowsHint(long.MaxValue - 1));
                Assert.False(unknown.AllowsDeletion);
            }
            // A folder under a control name is unreadable, whatever its name
            var entry = new IOEntry(Log + Group(5, "done") + "/");
            var folder = ControlFence.Of(new[] { entry });
            Assert.Equal((long.MaxValue, 1), (folder.ReservedFloor, folder.Uninterpreted));
            // Its name keeps the slash, so no record form parses it
            Assert.Equal(Group(5, "done") + "/", entry.Name);
            Assert.False(ControlName.TryParse(entry.Name, out _));

            var rig = Create(Memory());
            await DeltaMaintenanceWorkerTests.Commits(rig.Storage, 0, 6);
            await Root(rig.Storage, 3);
            await rig.Storage.WriteText(Log + ".flowtide-cleanup.x", "{}");
            rig.Worker.Published(6);
            await rig.TickAsync();

            // No hint is written for the valid checkpoint and no checkpoint is built at the head
            Assert.Equal(long.MaxValue, rig.Worker.Fence.ReservedFloor);
            Assert.Null(await DeltaMaintenanceWorkerTests.Hint(rig.Storage));
            Assert.Equal(new long[] { 3 }, await DeltaMaintenanceWorkerTests.Checkpoints(rig.Storage));
            Assert.Equal(OutcomeKind.Declined, rig.Worker.LastOutcomeForTests!.Kind);
            Assert.Equal(MaintenanceReason.CleanupGate, rig.Worker.LastOutcomeForTests.Reason);
            Assert.Equal(1, rig.Logs.Warnings(nameof(MaintenanceReason.CleanupGate)));

            // Nor at any older version
            Assert.True(rig.Worker.RequestBuild(new BuildDemand(1, RootPolicy.Auto)));
            await rig.TickAsync();
            Assert.Equal(MaintenanceReason.CleanupGate, rig.Worker.LastOutcomeForTests!.Reason);
            Assert.Equal(new long[] { 3 }, await DeltaMaintenanceWorkerTests.Checkpoints(rig.Storage));
        }

        [Fact]
        public async Task FutureOrUnreadableControlFencesPr1Mutation()
        {
            var future = Fence(Group(2, "compacting"));
            Assert.True(future.Fenced);
            Assert.Equal(2, future.ReservedFloor);
            Assert.False(future.AllowsDeletion);
            Assert.False(future.AllowsReplace(5));
            Assert.False(future.AllowsTornRepair(5, true, true));
            // Creating a checkpoint above the boundary destroys nothing
            Assert.True(future.AllowsCreate(5));
            Assert.True(future.AllowsHint(2));
            // A bare boundary is no record either
            var bare = Fence($".flowtide-cleanup.{3:D20}");
            Assert.Equal((3L, 1), (bare.ReservedFloor, bare.Uninterpreted));

            // An eligible store with every commit: only the record keeps the torn checkpoint
            var rig = Create(Local());
            await TornTable(rig.Storage, fromZero: true);
            var root = await DeltaTestKit.ReadBytes(rig.Storage, CheckpointPath(3));
            await rig.Storage.WriteText(Log + Group(2, "compacting"), "{}");
            rig.Worker.Published(6);
            await rig.TickAsync();

            Assert.True(rig.Worker.CleanupEligible);
            Assert.Equal("torn", await rig.Storage.ReadText(CheckpointPath(5)));
            Assert.Equal(MaintenanceReason.TornRepairDeclined, rig.Worker.LastOutcomeForTests!.Reason);
            Assert.Contains(nameof(MaintenanceReason.CleanupGate), rig.Worker.LastOutcomeForTests.Detail);
            Assert.Contains("1 this version cannot interpret", rig.Worker.LastOutcomeForTests.Detail);
            rig.PastRetry();
            await rig.TickAsync();
            Assert.Equal(new long[] { 3, 5, 6 }, await DeltaMaintenanceWorkerTests.Checkpoints(rig.Storage));
            Assert.Equal("torn", await rig.Storage.ReadText(CheckpointPath(5)));
            Assert.Equal(root, await DeltaTestKit.ReadBytes(rig.Storage, CheckpointPath(3)));

            // Deletion evidence declines before its successor probe
            var scripted = new ScriptedStorage();
            scripted.Put(Log + $"{0:D20}.json");
            scripted.Put(Log + Group(2, "compacting"));
            var asserted = CleanupConfiguration.Validate(scripted, "t", true, CleanupOptIns.Off);
            var (check, log) = await ListingEvidence.ReadLogAsync(asserted, scripted, -1, default);
            Assert.Equal(ListingCheck.ControlFenced, check);
            Assert.Null(log);
            Assert.DoesNotContain(scripted.Requests, x => x.StartsWith("Stat "));
            // A record this version reads passes, its fence carried with the evidence
            var readable = new ScriptedStorage();
            readable.Put(Log + $"{0:D20}.json");
            readable.Put(Log + Group(0, "started"));
            (check, log) = await ListingEvidence.ReadLogAsync(CleanupConfiguration.Validate(readable, "t", true, CleanupOptIns.Off), readable, -1, default);
            Assert.Equal(ListingCheck.Passed, check);
            Assert.Equal(0, log!.Fence.ReservedFloor);

            // An unreadable folder under a control name on local disk fences deletion and every checkpoint
            var local = Local();
            await DeltaMaintenanceWorkerTests.Commits(local, 0, 1);
            await local.WriteText(Log + ".flowtide-cleanup.x/inner", "{}");
            var listing = await DeltaTransactionReader.ListLog(local, "t");
            Assert.True(Assert.Single(listing.Controls).Path.IsFolder);
            Assert.Equal(long.MaxValue, ControlFence.Of(listing).ReservedFloor);
            Assert.Equal(ListingCheck.ControlFenced, (await ListingEvidence.ReadLogAsync(CleanupConfiguration.Validate(local, "t", false, CleanupOptIns.Off), local, -1, default)).Check);
        }

        [Theory]
        [InlineData(false, false, null, false)]
        [InlineData(false, true, null, true)]
        // On an ineligible store a reservation defers even below the torn checkpoint; a done record does not
        [InlineData(false, true, "started", false)]
        [InlineData(false, true, "done", true)]
        // A version file above the newest commit leaves the head's history unproven
        [InlineData(false, true, "crc", false)]
        [InlineData(true, false, null, true)]
        public async Task IneligibleTornRepairNeedsFullHistory(bool eligible, bool fromZero, string? extra, bool repaired)
        {
            var rig = Create(eligible ? Local() : Memory());
            Assert.Equal(eligible, rig.Worker.CleanupEligible);
            await TornTable(rig.Storage, fromZero);
            var root = await DeltaTestKit.ReadBytes(rig.Storage, CheckpointPath(3));
            switch (extra)
            {
                case "started":
                case "done":
                    await rig.Storage.WriteText(Log + Group(2, extra), "{}");
                    break;
                case "crc":
                    await rig.Storage.WriteText(Log + $"{7:D20}.crc", "{}");
                    break;
            }
            rig.Worker.Published(6);
            await rig.TickAsync();

            var outcome = rig.Worker.LastOutcomeForTests!;
            if (repaired)
            {
                Assert.NotEqual("torn", await rig.Storage.ReadText(CheckpointPath(5)));
                Assert.Equal(5, rig.Worker.LastCompleted);
                Assert.Equal(5, (await DeltaMaintenanceWorkerTests.Hint(rig.Storage))!.Version);
            }
            else
            {
                Assert.Equal("torn", await rig.Storage.ReadText(CheckpointPath(5)));
                Assert.Equal((OutcomeKind.Declined, MaintenanceReason.TornRepairDeclined), (outcome.Kind, outcome.Reason));
                Assert.Contains(nameof(MaintenanceReason.CleanupGate), outcome.Detail);
                Assert.Contains("not eligible for cleanup", outcome.Detail);
                Assert.Null(await DeltaMaintenanceWorkerTests.Hint(rig.Storage));

                // Builds above the torn checkpoint keep working
                rig.PastRetry();
                await rig.TickAsync();
                Assert.Equal(new long[] { 3, 5, 6 }, await DeltaMaintenanceWorkerTests.Checkpoints(rig.Storage));
                Assert.Equal("torn", await rig.Storage.ReadText(CheckpointPath(5)));
                Assert.Equal(6, (await DeltaMaintenanceWorkerTests.Hint(rig.Storage))!.Version);
            }
            // A valid checkpoint is never replaced
            Assert.Equal(root, await DeltaTestKit.ReadBytes(rig.Storage, CheckpointPath(3)));
        }

        [Theory]
        [InlineData(5, false)]
        [InlineData(4, true)]
        public async Task TornRepairWaitsAtOrBelowTheBoundary(long boundary, bool repaired)
        {
            var rig = Create(Local());
            await TornTable(rig.Storage, fromZero: false);
            await rig.Storage.WriteText(Log + Group(boundary, "started"), "{}");
            rig.Worker.Published(6);
            await rig.TickAsync();

            Assert.Equal(boundary, rig.Worker.Fence.ReservedFloor);
            Assert.Equal(repaired, await rig.Storage.ReadText(CheckpointPath(5)) != "torn");
            if (!repaired)
            {
                Assert.Contains("versions up to 5", rig.Worker.LastOutcomeForTests!.Detail);
            }
        }

        [Fact]
        public async Task BuildAboveReservationStillPublishes()
        {
            var rig = Create(Memory());
            await DeltaMaintenanceWorkerTests.Commits(rig.Storage, 0, 8);
            await rig.Storage.WriteText(Log + Group(4, "started"), "{}");
            rig.Worker.Published(8);
            await rig.TickAsync();

            Assert.Equal(new long[] { 8 }, await DeltaMaintenanceWorkerTests.Checkpoints(rig.Storage));
            Assert.Equal(8, (await DeltaMaintenanceWorkerTests.Hint(rig.Storage))!.Version);
            Assert.Equal(4, rig.Worker.Fence.ReservedFloor);

            // A demand at the boundary is declined, one above it is built and the hint stays
            Assert.True(rig.Worker.RequestBuild(new BuildDemand(4, RootPolicy.Auto)));
            await rig.TickAsync();
            Assert.Equal((OutcomeKind.Declined, MaintenanceReason.CleanupGate), (rig.Worker.LastOutcomeForTests!.Kind, rig.Worker.LastOutcomeForTests.Reason));
            Assert.Null(rig.Worker.PendingDemand);
            Assert.True(rig.Worker.RequestBuild(new BuildDemand(5, RootPolicy.Auto)));
            await rig.TickAsync();
            Assert.Equal(OutcomeKind.Completed, rig.Worker.LastOutcomeForTests!.Kind);
            Assert.Equal(new long[] { 5, 8 }, await DeltaMaintenanceWorkerTests.Checkpoints(rig.Storage));
            Assert.Equal(8, (await DeltaMaintenanceWorkerTests.Hint(rig.Storage))!.Version);

            // A build's own listing decides, without a new inspection
            await rig.Storage.WriteText(Log + Group(6, "started"), "{}");
            Assert.True(rig.Worker.RequestBuild(new BuildDemand(6, RootPolicy.Auto)));
            await rig.TickAsync();
            Assert.Equal(MaintenanceReason.CleanupGate, rig.Worker.LastOutcomeForTests!.Reason);
            Assert.Equal(6, rig.Worker.Fence.ReservedFloor);
            Assert.True(rig.Worker.RequestBuild(new BuildDemand(7, RootPolicy.Auto)));
            await rig.TickAsync();
            Assert.Equal(new long[] { 5, 7, 8 }, await DeltaMaintenanceWorkerTests.Checkpoints(rig.Storage));
        }

        [Theory]
        [InlineData(4, false)]
        [InlineData(3, true)]
        public async Task HintBelowReservationIsNeverWritten(long boundary, bool written)
        {
            // No periodic build follows the inspection
            var rig = Create(Memory(), interval: 100);
            await DeltaMaintenanceWorkerTests.Commits(rig.Storage, 0, 6);
            await Root(rig.Storage, 3);
            await rig.Storage.WriteText(Log + Group(boundary, "started"), "{}");
            rig.Worker.Published(6);
            await rig.TickAsync();

            Assert.Equal(3, rig.Worker.LastCompleted);
            Assert.Equal(written ? 3 : null, (await DeltaMaintenanceWorkerTests.Hint(rig.Storage))?.Version);
            Assert.Equal(written ? 0 : 1, rig.Logs.Warnings(nameof(MaintenanceReason.CleanupGate)));

            // A build that finds the checkpoint valid keeps the hint too
            Assert.True(rig.Worker.RequestBuild(new BuildDemand(3, RootPolicy.Auto)));
            await rig.TickAsync();
            var outcome = rig.Worker.LastOutcomeForTests!;
            Assert.Equal((OutcomeKind.AlreadyValid, written ? MaintenanceReason.None : MaintenanceReason.CleanupGate), (outcome.Kind, outcome.Reason));
            Assert.Equal(written ? 3 : null, (await DeltaMaintenanceWorkerTests.Hint(rig.Storage))?.Version);
            // Logged by the inspection and again, at a lower level, by the build
            Assert.Equal(written ? 0 : 2, rig.Logs.Entries.Count(x => x.Message.Contains("The hint stays below checkpoint 3")));
        }

        [Fact]
        public async Task ReservationRestoredWithRetentionOff()
        {
            var storage = Memory();
            await DeltaMaintenanceWorkerTests.Commits(storage, 0, 6);
            await Root(storage, 3);
            // An advance to 4 that started, its records valid
            var fixture = CleanupRecordKit.Fixture(c: 4, o: 0, head: 6, firstJson: 0);
            var part = CleanupRecordKit.Part(fixture, 0, "/t/a.parquet");
            var header = CleanupRecordKit.Header(fixture, part);
            var records = new[] { part, header, CleanupRecordKit.Started(fixture, header) };
            foreach (var record in records)
            {
                await CleanupRecordKit.PlantAsync(storage, "t", record);
            }

            // A first start, then a restart; neither has a cleanup
            foreach (var start in new[] { "first", "restart" })
            {
                var rig = Create(storage, interval: 100);
                rig.Worker.Published(6);
                await rig.TickAsync();

                Assert.Equal(3, rig.Worker.LastCompleted);
                Assert.Equal(4, rig.Worker.Fence.ReservedFloor);
                Assert.Null(await DeltaMaintenanceWorkerTests.Hint(rig.Storage));
                Assert.True(rig.Worker.RequestBuild(new BuildDemand(4, RootPolicy.Auto)));
                await rig.TickAsync();
                Assert.Equal((OutcomeKind.Declined, MaintenanceReason.CleanupGate), (rig.Worker.LastOutcomeForTests!.Kind, rig.Worker.LastOutcomeForTests.Reason));
                Assert.Equal(new long[] { 3 }, await DeltaMaintenanceWorkerTests.Checkpoints(rig.Storage));
                foreach (var record in records)
                {
                    Assert.Equal(record.Bytes, await DeltaTestKit.ReadBytes(storage, Log + record.Name));
                }
                await rig.Worker.StopAsync(TimeSpan.FromSeconds(30));
            }
        }

        [Fact]
        public async Task InspectionFindsControlsWithoutAnotherRequest()
        {
            var root = Path.Combine(_directory, "hinted");
            Directory.CreateDirectory(root);
            var hooks = new HookFileStorage(Files.Of.LocalDisk(root));
            var rig = Create(hooks);
            await DeltaMaintenanceWorkerTests.Commits(hooks, 0, 3);
            await DeltaLakeCheckpointBuildTests.LegacyRoot(hooks, "t", 3);
            var size = (await hooks.Ls(Log)).Single(x => x.Name.EndsWith(".checkpoint.parquet")).Size;
            await DeltaMaintenanceWorkerTests.WriteHint(hooks, 3, size);
            await hooks.WriteText(Log + Group(1, "started"), "{}");
            hooks.ClearRequests();

            rig.Worker.Published(3);
            await rig.TickAsync();

            Assert.Equal(3, rig.Worker.LastCompleted);
            Assert.Equal(1, rig.Worker.Fence.ReservedFloor);
            Assert.Equal(2, hooks.Requests.Count);
            Assert.StartsWith("Ls ", hooks.Requests[0]);
        }

        [Theory]
        [InlineData("local", true)]
        [InlineData("memory", false)]
        [InlineData("replaced", false)]
        public async Task SinkWorkerKnowsWhetherItsStoreIsEligible(string kind, bool eligible)
        {
            var testName = nameof(SinkWorkerKnowsWhetherItsStoreIsEligible) + kind;
            var storage = kind == "memory" ? Files.Of.InternalMemory($"./{testName}-{Guid.NewGuid():N}") : Local();
            await using var stream = new DeltaLakeSinkStream(testName, storage, options =>
            {
                if (kind == "replaced")
                {
                    // Another instance after the classification at construction
                    options.SinkCreatedForTests = _ => options.StorageLocation = new ForwardingStorage(storage);
                }
            });
            stream.Generate(3);
            await stream.StartStream(DeltaTestKit.UserInsert);
            await DeltaTestKit.WaitForVersion(storage, "test", stream, 0);

            Assert.Equal(eligible, Assert.Single(stream.Maintenance.Workers).CleanupEligible);
        }
    }
}
