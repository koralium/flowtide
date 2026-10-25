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
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.Deletion;
using Microsoft.Extensions.Time.Testing;
using Stowage;
using Stowage.Impl;
using System.Text.RegularExpressions;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public class AdvanceRecoveryTests : IDisposable
    {
        private static readonly DateTimeOffset s_now = new DateTimeOffset(2026, 10, 15, 12, 0, 0, TimeSpan.Zero);
        private static readonly TimeSpan s_retention = TimeSpan.FromDays(7);
        private static readonly TimeSpan s_wait = TimeSpan.FromSeconds(30);
        private const string TableId = "table-id";

        private readonly string _directory = Path.Combine(Path.GetTempPath(), "ar-" + ScratchOwners.ShortId());
        private readonly ScratchOwners _owners = new ScratchOwners();
        private readonly List<IDisposable> _disposables = new List<IDisposable>();

        public AdvanceRecoveryTests()
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

            public string LogDirectory => Path.Combine(Root, "t", "_delta_log");

            public string Log(string name) => Path.Combine(LogDirectory, name);
        }

        // Own commits 0 to the head before the cutoff on an eligible local disk table with retention on, checkpointed at every fourth version below it
        private async Task<Table> Checkpointed(long head = 14)
        {
            var root = Path.Combine(_directory, ScratchOwners.ShortId());
            Directory.CreateDirectory(root);
            var local = Files.Of.LocalDisk(root);
            _disposables.Add(local);
            var configuration = CleanupConfiguration.Validate(local, "t", false, new CleanupOptIns(s_retention, null));
            Assert.True(configuration.Store.Eligible);
            await BoundarySelectorTests.Create(local, head, BoundarySelectorTests.Old);
            for (long version = 4; version < head; version += 4)
            {
                Assert.Equal(BuildStatus.Completed, (await TryBuild(local, version)).Status);
            }
            return new Table() { Local = local, Root = root, Configuration = configuration };
        }

        // What an advance to C that ran leaves: no commit or checkpoint below C
        private static void Executed(Table table, long c)
        {
            for (long version = 0; version < c; version++)
            {
                File.Delete(table.Log($"{version:D20}.json"));
                File.Delete(table.Log($"{version:D20}.checkpoint.parquet"));
            }
        }

        private async Task<BuildResult> TryBuild(IFileStorage storage, long version)
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
                }, new BuildRequest() { Version = version, Root = RootPolicy.Auto, KnownUnsupported = new HashSet<ObjectIdentity>() });
            }
            _owners.Release(owner);
            return result;
        }

        private CleanupContext Context(long head, FakeTimeProvider clock, CapacityLedger? capacity = null, CancellationToken owner = default)
        {
            var scratch = _owners.Add(new ScratchDirectory(Path.Combine(_directory, ScratchOwners.ShortId()))).CreateJob();
            _disposables.Add(scratch);
            return new CleanupContext(new JobRun(head, JobKind.Advance) { Cancellation = new CancellationTokenSource() }, clock)
            {
                Kind = JobKind.Advance,
                Long = true,
                ReadLimit = long.MaxValue,
                Deadline = clock.GetUtcNow() + TimeSpan.FromHours(1),
                Head = head,
                TableId = null,
                Scratch = scratch,
                Memory = new JobMemory(),
                Transfer = new TransferAllowance(),
                Stop = default,
                Capacity = capacity,
                CapacityOwner = owner
            };
        }

        private static DeltaTableCleanup Cleanup(Table table, IFileStorage? storage = null)
        {
            return Recovering(new DeltaTableCleanup(table.Configuration, storage ?? table.Local, new FakeTimeProvider(s_now), "t", null, () => table.Local, new DiscardCounters(), null, "t"));
        }

        // New advances end once their hint is raised and a resume starts without exclusions, as these tests observe recovery and selection
        private static DeltaTableCleanup Recovering(DeltaTableCleanup cleanup) => AdvanceExecutionTests.WithoutExclusions(AdvanceExecutionTests.PlanOnly(cleanup));

        // One long advance at V 14, which releases everything it held
        private async Task<CleanupResult> Run(DeltaTableCleanup cleanup, CleanupContext? context = null)
        {
            context ??= Context(14, new FakeTimeProvider(s_now));
            var result = await cleanup.RunAsync(JobKind.Advance, context);
            Assert.Equal(0, context.Memory.Managed);
            Assert.Equal(0, context.Memory.Native);
            Assert.Empty(context.Scratch.Files);
            return result;
        }

        private static void AssertCompleted(CleanupResult result)
        {
            Assert.True(result.Kind == OutcomeKind.Completed, $"{result.Kind} {result.Reason}: {result.Detail}");
        }

        private static void AssertDeclined(CleanupResult result, MaintenanceReason reason)
        {
            Assert.True(result.Kind == OutcomeKind.Declined && result.Reason == reason, $"{result.Kind} {result.Reason}: {result.Detail}");
        }

        // An advance at C begun over every commit from zero
        private static RecordFixture Advance(long c, long o = 0, long firstJson = 0, string tableId = TableId, long head = 14) => CleanupRecordKit.Fixture(tableId, c, o, 0, head, firstJson);

        // One part and the header naming it
        private static (PlantedRecord Part, PlantedRecord Header) Manifest(RecordFixture fixture)
        {
            var part = CleanupRecordKit.Part(fixture, 0, $"/t/f-{fixture.C}.parquet");
            return (part, CleanupRecordKit.Header(fixture, part));
        }

        private static PlantedRecord Torn(PlantedRecord record) => record with { Bytes = CleanupRecordKit.Truncate(record.Bytes, record.Bytes.Length - 1) };

        private static void Plant(Table table, params PlantedRecord[] records)
        {
            foreach (var record in records)
            {
                CleanupRecordKit.PlantLocal(table.LogDirectory, record);
            }
        }

        // Listed control record names in ordinal order
        private static string[] Controls(Table table) => Directory.GetFiles(table.LogDirectory, DeltaTransactionReader.CleanupControlPrefix + "*").Select(x => Path.GetFileName(x)!).Order(StringComparer.Ordinal).ToArray();

        private static string[] Names(params PlantedRecord[] records) => records.Select(x => x.Name).Order(StringComparer.Ordinal).ToArray();

        // Every control record's bytes by name
        private static Dictionary<string, byte[]> Snapshot(Table table) => Controls(table).ToDictionary(x => x, x => File.ReadAllBytes(table.Log(x)));

        private static async Task<ControlFence> Fence(Table table) => ControlFence.Of(await DeltaTransactionReader.ListLog(table.Local, "t"));

        [Fact]
        public async Task LateStartedAfterTeardownReleasedByDone()
        {
            var table = await Checkpointed();
            // A completed advance to 8 with the done an earlier advance to 4 retained
            var earlier = Advance(4);
            var earlierDone = CleanupRecordKit.Done(earlier, Manifest(earlier).Header);
            var fixture = Advance(8);
            var (part, header) = Manifest(fixture);
            var done = CleanupRecordKit.Done(fixture, header);
            Plant(table, earlierDone, part, header, CleanupRecordKit.Started(fixture, header), CleanupRecordKit.LogsComplete(fixture, header), CleanupRecordKit.Progress(header, 1), done);
            Executed(table, 8);
            var cleanup = Cleanup(table);

            AssertCompleted(await Run(cleanup));
            // Only the newest completion is kept
            Assert.Equal(Names(done), Controls(table));

            // A retry of its started lands after the teardown
            Plant(table, CleanupRecordKit.Started(fixture, header));
            Assert.Equal(8, (await Fence(table)).ReservedFloor);
            AssertCompleted(await Run(cleanup));

            Assert.Equal(Names(done), Controls(table));
            Assert.Equal(ControlFence.NoReservation, (await Fence(table)).ReservedFloor);
        }

        [Fact]
        public async Task UncertainStartedAfterSameCReplanReleased()
        {
            var table = await Checkpointed();
            // The replan at the same C completed and kept its done; the first plan's started lands afterwards
            var replan = Advance(8);
            var done = CleanupRecordKit.Done(replan, Manifest(replan).Header);
            var uncertain = Advance(8);
            var started = CleanupRecordKit.Started(uncertain, Manifest(uncertain).Header);
            Plant(table, done, started);
            Executed(table, 8);

            AssertCompleted(await Run(Cleanup(table)));

            Assert.Equal(Names(done), Controls(table));
        }

        [Fact]
        public async Task LateStartedFromEarlierAdvanceSuperseded()
        {
            var table = await Checkpointed();
            var later = Advance(8);
            var done = CleanupRecordKit.Done(later, Manifest(later).Header);
            var earlier = Advance(4);
            var started = CleanupRecordKit.Started(earlier, Manifest(earlier).Header);
            Plant(table, done, started);
            Executed(table, 8);

            AssertCompleted(await Run(Cleanup(table)));

            Assert.Equal(Names(done), Controls(table));
        }

        [Fact]
        public async Task TornDoneDoesNotReleaseReservation()
        {
            var table = await Checkpointed();
            var fixture = Advance(8);
            var header = Manifest(fixture).Header;
            var started = CleanupRecordKit.Started(fixture, header);
            var done = Torn(CleanupRecordKit.Done(fixture, header));
            Plant(table, started, done);
            var hint = (await DeltaMaintenanceWorkerTests.Hint(table.Local))!.Version;

            AssertDeclined(await Run(Cleanup(table)), MaintenanceReason.ManifestSuspended);

            Assert.Equal(Names(started, done), Controls(table));
            Assert.Equal(8, (await Fence(table)).ReservedFloor);
            // Builds at or below C decline; above it they publish
            var below = await TryBuild(table.Local, 7);
            Assert.Equal((BuildStatus.Declined, MaintenanceReason.CleanupGate), (below.Status, below.Reason));
            Assert.Equal(BuildStatus.Completed, (await TryBuild(table.Local, 9)).Status);
            Assert.Equal(hint, (await DeltaMaintenanceWorkerTests.Hint(table.Local))!.Version);
            Assert.True(hint >= 8);
        }

        [Fact]
        public async Task TornHigherDoneDoesNotRelease()
        {
            var table = await Checkpointed();
            var later = Advance(8);
            var done = Torn(CleanupRecordKit.Done(later, Manifest(later).Header));
            var earlier = Advance(4);
            var started = CleanupRecordKit.Started(earlier, Manifest(earlier).Header);
            Plant(table, done, started);
            Executed(table, 8);

            AssertDeclined(await Run(Cleanup(table)), MaintenanceReason.ManifestSuspended);

            Assert.Equal(Names(done, started), Controls(table));
        }

        [Theory]
        [InlineData(false, false)]
        [InlineData(false, true)]
        [InlineData(true, false)]
        [InlineData(true, true)]
        public async Task IncompleteSealedCompletionCannotReleaseAnAdvance(bool subsequent, bool wrongProcessed)
        {
            var table = await Checkpointed();
            var fixture = Advance(8);
            var (part, header) = Manifest(fixture);
            var started = CleanupRecordKit.Started(fixture, header);
            var completed = subsequent ? Advance(12) : fixture;
            var completedHeader = subsequent ? Manifest(completed).Header : header;
            // Sealed and correctly named, short of its final cursor or processed count
            var done = wrongProcessed
                ? CleanupRecordKit.Done(completed, completedHeader, processed: 0)
                : CleanupRecordKit.Done(completed, completedHeader, final: new RecordCursor(0, 0, 0));
            Plant(table, part, started, done);
            if (subsequent)
            {
                Executed(table, 8);
            }
            else
            {
                Plant(table, header);
            }
            var before = Snapshot(table);

            AssertDeclined(await Run(Cleanup(table)), MaintenanceReason.ManifestSuspended);

            Assert.Equal(before, Snapshot(table));
            Assert.Equal(8, (await Fence(table)).ReservedFloor);
        }

        [Theory]
        [InlineData("totals", false)]
        [InlineData("totals", true)]
        [InlineData("oldest", true)]
        [InlineData("adoption", true)]
        [InlineData("head", true)]
        [InlineData("anchor", true)]
        [InlineData("table", true)]
        public async Task CompletionContradictingItsStartCannotReleaseIt(string changed, bool logsComplete)
        {
            var table = await Checkpointed();
            var fixture = Advance(8);
            var (part, header) = Manifest(fixture);
            var association = CleanupRecordKit.AssociationOf(header);
            var totals = CleanupRecordKit.TotalsOf(header);
            var record = new DoneRecord(association, fixture.TableId, fixture.O, fixture.A, fixture.Head, fixture.Anchor, new RecordCursor(totals.Parts, 0, 0), totals, totals.Emitted);
            // Sealed, correctly named and consistent with itself, but not what its started records
            record = changed switch
            {
                "totals" => record with { Final = default, Totals = new RecordTotals(0, 0, 0), Processed = 0 },
                "oldest" => record with { O = 1 },
                "adoption" => record with { A = 1 },
                "head" => record with { Head = fixture.Head - 1 },
                "anchor" => record with { Anchor = fixture.Anchor with { StageId = "other-anchor" } },
                _ => record
            };
            var done = new PlantedRecord(ControlName.Group(ControlKind.Done, fixture.C, fixture.AdvanceId, association.HeaderSha256, ControlTokens.New()), ControlRecords.Encode(record));
            Assert.True(ControlName.TryParse(done.Name, out var name));
            Assert.Equal(RecordStatus.Valid, ControlRecords.Decode(done.Bytes, out DoneRecord? _));
            Assert.Equal(RecordStatus.Valid, ControlRecords.Matches(name, done.Bytes));
            // Another table's start in the table row
            var started = CleanupRecordKit.Started(changed == "table" ? fixture with { TableId = "other-table" } : fixture, header);
            Plant(table, part, header, started, done);
            if (logsComplete)
            {
                Plant(table, CleanupRecordKit.LogsComplete(fixture, header));
            }
            var before = Snapshot(table);

            AssertDeclined(await Run(Cleanup(table)), MaintenanceReason.ManifestSuspended);

            Assert.Equal(before, Snapshot(table));
            Assert.Equal(8, (await Fence(table)).ReservedFloor);
        }

        [Theory]
        [InlineData(2, 0, 0, 0)]
        [InlineData(0, 0, 0, 2)]
        [InlineData(0, ControlRecords.MaxPartPaths + 1, 0, 1)]
        [InlineData(0, 0, ControlRecords.PartCap + 1, 1)]
        [InlineData(1, 0, 0, 0)]
        [InlineData(0, 0, 0, 1)]
        public async Task ImpossibleProgressCannotReduceRestoredDemand(int nextPart, int record, long offset, long processed)
        {
            var table = await Checkpointed();
            var fixture = Advance(8);
            var (part, header) = Manifest(fixture);
            Plant(table, part, header, CleanupRecordKit.Started(fixture, header));
            var ledger = new CapacityLedger(() => default);
            // The fixture's C is not the content its header names, so the resume suspends
            AssertDeclined(await Run(Cleanup(table), Context(14, new FakeTimeProvider(s_now), ledger)), MaintenanceReason.HistoryChanged);
            var expected = ledger.Total(CapacityState.Suspended);
            Assert.True(expected.Requests > 0 && expected.Bytes > 0);
            // The started body declares one emitted path in one part
            var progress = CleanupRecordKit.Progress(header, 1, new RecordCursor(nextPart, record, offset), processed);
            Plant(table, progress);

            var result = await Run(Cleanup(table), Context(14, new FakeTimeProvider(s_now), ledger));

            Assert.Equal(expected, ledger.Total(CapacityState.Runnable) + ledger.Total(CapacityState.Suspended));
            AssertDeclined(result, MaintenanceReason.ManifestSuspended);
            Assert.Contains(progress.Name, Controls(table));
            Assert.Equal(8, (await Fence(table)).ReservedFloor);
        }

        [Theory]
        [InlineData("table")]
        [InlineData("oldest")]
        [InlineData("started")]
        public async Task LogsCompleteOrStartedCopyContradictingItsStartCannotReduceRestoredDemand(string changed)
        {
            var table = await Checkpointed();
            var fixture = Advance(8);
            var (part, header) = Manifest(fixture);
            Plant(table, part, header, CleanupRecordKit.Started(fixture, header));
            var ledger = new CapacityLedger(() => default);
            // The fixture's C is not the content its header names, so the resume suspends
            AssertDeclined(await Run(Cleanup(table), Context(14, new FakeTimeProvider(s_now), ledger)), MaintenanceReason.HistoryChanged);
            var expected = ledger.Total(CapacityState.Suspended);
            Assert.True(expected.Requests > 0 && expected.Bytes > 0);
            // Sealed and named for the started's header, but for another table, oldest version or head
            var contradicting = changed switch
            {
                "table" => CleanupRecordKit.LogsComplete(fixture with { TableId = "other-table" }, header),
                "oldest" => CleanupRecordKit.LogsComplete(fixture with { O = 1 }, header),
                _ => CleanupRecordKit.Started(fixture with { Head = 13 }, header)
            };
            Plant(table, contradicting);

            var result = await Run(Cleanup(table), Context(14, new FakeTimeProvider(s_now), ledger));

            Assert.Equal(expected, ledger.Total(CapacityState.Runnable) + ledger.Total(CapacityState.Suspended));
            AssertDeclined(result, MaintenanceReason.ManifestSuspended);
            Assert.Contains(contradicting.Name, Controls(table));
            Assert.Equal(8, (await Fence(table)).ReservedFloor);
        }

        [Theory]
        [InlineData("logs-complete")]
        [InlineData("started")]
        public async Task ContradictingRecordsOfAnotherHeaderKeepTheAdvanceConflicted(string kind)
        {
            var table = await Checkpointed();
            var fixture = Advance(8);
            var (part, header) = Manifest(fixture);
            var other = Manifest(fixture).Header;
            // Beside a validated completion of the started's header, another header's records for another table or head
            var records = kind == "started"
                ? new[] { CleanupRecordKit.Started(fixture with { Head = 13 }, other), CleanupRecordKit.Done(fixture with { Head = 13 }, other) }
                : new[] { CleanupRecordKit.LogsComplete(fixture with { TableId = "other-table" }, other) };
            Plant(table, records.Concat(new[] { part, header, CleanupRecordKit.Started(fixture, header), CleanupRecordKit.Done(fixture, header) }).ToArray());
            var before = Snapshot(table);

            AssertDeclined(await Run(Cleanup(table)), MaintenanceReason.ManifestSuspended);

            Assert.Equal(before, Snapshot(table));
            Assert.Equal(8, (await Fence(table)).ReservedFloor);
        }

        [Theory]
        // In part 1 with more processed than part 0 holds
        [InlineData(true, 1, 0, 0, 2)]
        // Past part 0's paths
        [InlineData(true, 0, 2, 0, 1)]
        // Past part 0's bytes
        [InlineData(true, 0, 1, ControlRecords.PartCap, 1)]
        // A processed path before the first position, without a header
        [InlineData(false, 0, 0, 0, 1)]
        public async Task ProgressItsPartsCannotBackCannotReduceRestoredDemand(bool header, int nextPart, int record, long offset, long processed)
        {
            var table = await Checkpointed();
            var fixture = Advance(8);
            // One path in part 0 and two in part 1
            var first = CleanupRecordKit.Part(fixture, 0, "/t/f-a.parquet");
            var second = CleanupRecordKit.Part(fixture, 1, "/t/f-b.parquet", "/t/f-c.parquet");
            var manifest = CleanupRecordKit.Header(fixture, first, second);
            // Without its header, logs-complete keeps the start from releasing
            Plant(table, first, second, CleanupRecordKit.Started(fixture, manifest), header ? manifest : CleanupRecordKit.LogsComplete(fixture, manifest));
            var ledger = new CapacityLedger(() => default);
            AssertDeclined(await Run(Cleanup(table), Context(14, new FakeTimeProvider(s_now), ledger)), header ? MaintenanceReason.HistoryChanged : MaintenanceReason.ManifestSuspended);
            var expected = ledger.Total(CapacityState.Runnable) + ledger.Total(CapacityState.Suspended);
            Assert.True(expected.Requests > 0 && expected.Bytes > 0);
            var progress = CleanupRecordKit.Progress(manifest, 1, new RecordCursor(nextPart, record, offset), processed);
            Plant(table, progress);

            var result = await Run(Cleanup(table), Context(14, new FakeTimeProvider(s_now), ledger));

            Assert.Equal(expected, ledger.Total(CapacityState.Runnable) + ledger.Total(CapacityState.Suspended));
            AssertDeclined(result, MaintenanceReason.ManifestSuspended);
            Assert.Contains(progress.Name, Controls(table));
            Assert.Equal(8, (await Fence(table)).ReservedFloor);
        }

        [Theory]
        [InlineData("unsupported")]
        [InlineData("part")]
        [InlineData("foreign")]
        [InlineData("disagreeing")]
        public async Task AHeaderMustDecodeBeforeItsAdvanceIsResumable(string kind)
        {
            var table = await Checkpointed();
            var fixture = Advance(8);
            var (part, header) = Manifest(fixture);
            var invalid = header with
            {
                Bytes = kind switch
                {
                    // Another format version, resealed
                    "unsupported" => CleanupRecordKit.Reseal(CleanupRecordKit.Flip(header.Bytes, 4)),
                    "part" => part.Bytes,
                    // Another advance's header at the same C
                    "foreign" => Manifest(Advance(8)).Header.Bytes,
                    // This advance's header as another table records it
                    _ => Manifest(fixture with { TableId = "other-table" }).Header.Bytes
                }
            };
            // The intact started and logs-complete records name the exact invalid header content
            var association = CleanupRecordKit.AssociationOf(invalid);
            var started = new PlantedRecord(ControlName.Group(ControlKind.Started, fixture.C, fixture.AdvanceId, association.HeaderSha256, ControlTokens.New()),
                ControlRecords.Encode(new StartedRecord(association, fixture.TableId, fixture.O, fixture.A, fixture.Head, fixture.FirstJson, fixture.Anchor, CleanupRecordKit.TotalsOf(header))));
            Plant(table, part, invalid, started, CleanupRecordKit.LogsComplete(fixture, invalid));
            var before = Snapshot(table);

            AssertDeclined(await Run(Cleanup(table)), MaintenanceReason.ManifestSuspended);

            Assert.Equal(before, Snapshot(table));
            Assert.Equal(8, (await Fence(table)).ReservedFloor);
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task LowerCompletionDoesNotReleaseAHigherStart(bool header)
        {
            var table = await Checkpointed();
            // The kept completion of an earlier advance to 4
            var earlier = Advance(4);
            var done = CleanupRecordKit.Done(earlier, Manifest(earlier).Header);
            var fixture = Advance(8);
            var (part, manifest) = Manifest(fixture);
            var started = CleanupRecordKit.Started(fixture, manifest);
            var records = header ? new[] { done, part, manifest, started } : new[] { done, part, started };
            Plant(table, records);
            if (!header)
            {
                Executed(table, 8);
            }

            // With its header the fixture resumes and suspends at its C
            AssertDeclined(await Run(Cleanup(table)), header ? MaintenanceReason.HistoryChanged : MaintenanceReason.ManifestSuspended);

            Assert.Equal(Names(records), Controls(table));
            Assert.Equal(8, (await Fence(table)).ReservedFloor);
        }

        [Fact]
        public async Task UnrelatedTableDoneCannotRelease()
        {
            var table = await Checkpointed();
            // Another table's completion, which is never kept
            var other = Advance(8, tableId: "other-table");
            var done = CleanupRecordKit.Done(other, Manifest(other).Header);
            var earlier = Advance(4);
            var started = CleanupRecordKit.Started(earlier, Manifest(earlier).Header);
            Plant(table, done, started);
            Executed(table, 8);

            AssertDeclined(await Run(Cleanup(table)), MaintenanceReason.ManifestSuspended);

            Assert.Equal(Names(started), Controls(table));
            Assert.Equal(4, (await Fence(table)).ReservedFloor);
        }

        [Fact]
        public async Task OnlyThisTablesNewestCompletionIsKept()
        {
            var table = await Checkpointed();
            var own = Advance(4);
            var ownDone = CleanupRecordKit.Done(own, Manifest(own).Header);
            // Newer, copied from another table
            var other = Advance(8, tableId: "other-table");
            var otherDone = CleanupRecordKit.Done(other, Manifest(other).Header);
            Plant(table, ownDone, otherDone);

            AssertCompleted(await Run(Cleanup(table)));

            Assert.Equal(Names(ownDone), Controls(table));
        }

        [Theory]
        [InlineData("foreign-done")]
        [InlineData("foreign-header")]
        [InlineData("foreign-headerless")]
        [InlineData("torn-started")]
        [InlineData("torn-progress")]
        public async Task ForeignOrUncertainEvidenceNeitherReleasesNorResumes(string kind)
        {
            var table = await Checkpointed();
            var own = Advance(8);
            // The same advance as another table records it
            var foreign = own with { TableId = "other-table" };
            var fixture = kind is "foreign-header" or "foreign-headerless" ? foreign : own;
            var (part, header) = Manifest(fixture);
            var started = CleanupRecordKit.Started(fixture, header);
            // Every original commit is still listed
            var records = kind switch
            {
                "foreign-done" => new[] { part, started, CleanupRecordKit.Done(foreign, header) },
                "foreign-header" => new[] { part, header, started },
                "foreign-headerless" => new[] { part, started },
                "torn-started" => new[] { part, started, Torn(CleanupRecordKit.Started(fixture, header)) },
                _ => new[] { part, header, started, Torn(CleanupRecordKit.Progress(header, 1)) }
            };
            Plant(table, records);
            var before = Snapshot(table);

            AssertDeclined(await Run(Cleanup(table)), MaintenanceReason.ManifestSuspended);

            Assert.Equal(before, Snapshot(table));
            Assert.Equal(8, (await Fence(table)).ReservedFloor);
        }

        [Fact]
        public async Task EqualSequenceProgressWithAnotherBodySuspends()
        {
            var table = await Checkpointed();
            var fixture = Advance(8);
            var part = CleanupRecordKit.Part(fixture, 0, "/t/f-a.parquet", "/t/f-b.parquet");
            var header = CleanupRecordKit.Header(fixture, part);
            // Two claims at sequence one, each valid alone: one path processed, and both
            var records = new[]
            {
                part, header, CleanupRecordKit.Started(fixture, header), CleanupRecordKit.LogsComplete(fixture, header),
                CleanupRecordKit.Progress(header, 1, new RecordCursor(0, 1, 0), 1), CleanupRecordKit.Progress(header, 1, new RecordCursor(1, 0, 0), 2)
            };
            Plant(table, records);
            var before = Snapshot(table);

            AssertDeclined(await Run(Cleanup(table)), MaintenanceReason.ManifestSuspended);

            Assert.Equal(before, Snapshot(table));
            Assert.Equal(8, (await Fence(table)).ReservedFloor);
        }

        [Fact]
        public async Task ConflictedCompletionReleasesNothing()
        {
            var table = await Checkpointed();
            // An advance to 8 whose started and done name different headers
            var conflicted = Advance(8);
            var conflicting = new[] { CleanupRecordKit.Started(conflicted, Manifest(conflicted).Header), CleanupRecordKit.Done(conflicted, Manifest(conflicted).Header) };
            var earlier = Advance(4);
            var started = CleanupRecordKit.Started(earlier, Manifest(earlier).Header);
            Plant(table, conflicting.Append(started).ToArray());
            Executed(table, 8);
            var before = Snapshot(table);

            AssertDeclined(await Run(Cleanup(table)), MaintenanceReason.ManifestSuspended);

            Assert.Equal(before, Snapshot(table));
            Assert.Equal(8, (await Fence(table)).ReservedFloor);
        }

        [Fact]
        public async Task ConflictedCompletionIsNeverKept()
        {
            var table = await Checkpointed();
            var own = Advance(4);
            var ownDone = CleanupRecordKit.Done(own, Manifest(own).Header);
            // Newer completions of one advance that name different headers
            var conflicted = Advance(8);
            var conflicting = new[] { CleanupRecordKit.Done(conflicted, Manifest(conflicted).Header), CleanupRecordKit.Done(conflicted, Manifest(conflicted).Header) };
            Plant(table, conflicting.Append(ownDone).ToArray());

            AssertCompleted(await Run(Cleanup(table)));

            // The conflicting copies stay, reserving nothing
            Assert.Equal(Names(conflicting.Append(ownDone).ToArray()), Controls(table));
            Assert.Equal(ControlFence.NoReservation, (await Fence(table)).ReservedFloor);
        }

        [Fact]
        public async Task ValidatedStartWithCompleteOriginalRangeReleases()
        {
            var table = await Checkpointed();
            // Rooted at 4, whose commit is gone too: the original first JSON is 5
            for (long version = 0; version <= 4; version++)
            {
                File.Delete(table.Log($"{version:D20}.json"));
            }
            var fixture = Advance(8, o: 4, firstJson: 5);
            var (part, header) = Manifest(fixture);
            // Its header never landed
            Plant(table, part, CleanupRecordKit.Started(fixture, header));

            AssertCompleted(await Run(Cleanup(table)));

            Assert.Empty(Controls(table));

            // Under the header's name, another header than the one its started names
            var other = Advance(8, o: 4, firstJson: 5);
            var (otherPart, otherHeader) = Manifest(other);
            var substitute = CleanupRecordKit.Header(other, CleanupRecordKit.Part(other, 0, "/t/g.parquet"));
            Plant(table, otherPart, otherHeader with { Bytes = substitute.Bytes }, CleanupRecordKit.Started(other, otherHeader));

            AssertCompleted(await Run(Cleanup(table)));

            Assert.Empty(Controls(table));
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task HeaderlessStartAboveARootWithoutItsCommitNeedsTheRoot(bool rootListed)
        {
            var table = await Checkpointed();
            Assert.Equal(BuildStatus.Completed, (await TryBuild(table.Local, 7)).Status);
            // Rooted at C - 1 without its commit: the original first JSON is C
            for (long version = 0; version <= 7; version++)
            {
                File.Delete(table.Log($"{version:D20}.json"));
            }
            var fixture = Advance(8, o: 7, firstJson: 8);
            var (part, header) = Manifest(fixture);
            Plant(table, part, CleanupRecordKit.Started(fixture, header));
            if (!rootListed)
            {
                // The advance's first log deletion
                File.Delete(table.Log($"{7:D20}.checkpoint.parquet"));
            }
            var before = Snapshot(table);

            var result = await Run(Cleanup(table));

            if (rootListed)
            {
                AssertCompleted(result);
                Assert.Empty(Controls(table));
                return;
            }
            AssertDeclined(result, MaintenanceReason.ManifestSuspended);
            Assert.Equal(before, Snapshot(table));
            Assert.Equal(8, (await Fence(table)).ReservedFloor);
        }

        [Fact]
        public async Task DoneNamingAnotherHeaderDoesNotRelease()
        {
            var table = await Checkpointed();
            var fixture = Advance(8);
            var started = CleanupRecordKit.Started(fixture, Manifest(fixture).Header);
            // The same advance's completion of another header
            var done = CleanupRecordKit.Done(fixture, Manifest(fixture).Header);
            Plant(table, started, done);

            AssertDeclined(await Run(Cleanup(table)), MaintenanceReason.ManifestSuspended);

            Assert.Equal(Names(started, done), Controls(table));
        }

        [Theory]
        [InlineData("torn")]
        [InlineData("renamed")]
        [InlineData("deleted")]
        [InlineData("hidden")]
        public async Task TornStartOrMissingOriginalRangeSuspends(string kind)
        {
            var table = await Checkpointed();
            var fixture = Advance(8);
            var (part, header) = Manifest(fixture);
            var started = CleanupRecordKit.Started(fixture, header);
            Assert.True(ControlName.TryParse(started.Name, out var name));
            // A valid body under an upper-case digest is not the record its name says
            var renamed = started with { Name = CleanupRecordKit.Rename(started.Name, NameField.Digest, name.Digest!.ToUpperInvariant()) };
            Plant(table, part, kind switch { "torn" => Torn(started), "renamed" => renamed, _ => started });
            IFileStorage storage = table.Local;
            var missing = $"{6:D20}.json";
            if (kind == "deleted")
            {
                File.Delete(table.Log(missing));
            }
            else if (kind == "hidden")
            {
                var hooks = OwnedHookFileStorage.Over(table.Local);
                hooks.Hidden = path => path.Name == missing;
                storage = hooks;
            }
            var before = Snapshot(table);

            AssertDeclined(await Run(Cleanup(table, storage)), MaintenanceReason.ManifestSuspended);

            Assert.Equal(before, Snapshot(table));
        }

        [Theory]
        [InlineData("logs-complete")]
        [InlineData("progress")]
        public async Task LaterEvidenceKeepsAStartWithItsOriginalRange(string kind)
        {
            var table = await Checkpointed();
            var fixture = Advance(8);
            var (part, header) = Manifest(fixture);
            var started = CleanupRecordKit.Started(fixture, header);
            var later = kind == "progress" ? CleanupRecordKit.Progress(header, 1) : CleanupRecordKit.LogsComplete(fixture, header);
            // Every original commit is still listed
            Plant(table, part, started, later);

            AssertDeclined(await Run(Cleanup(table)), MaintenanceReason.ManifestSuspended);

            Assert.Equal(Names(part, started, later), Controls(table));
        }

        [Fact]
        public async Task SuspendedAdvanceWarnsOnce()
        {
            var rig = await CoveredWorker();
            var fixture = CleanupRecordKit.Fixture(TableId, 4, 0, 0, 6, 0);
            var started = Torn(CleanupRecordKit.Started(fixture, Manifest(fixture).Header));
            CleanupRecordKit.PlantLocal(Path.Combine(rig.Root, "t", "_delta_log"), started);

            await rig.DaysAsync(2, 4);

            Assert.Equal(1, rig.Logs.Warnings(nameof(MaintenanceReason.ManifestSuspended)));
            Assert.True(rig.Logs.Entries.Count(x => x.Message.Contains(nameof(MaintenanceReason.ManifestSuspended))) >= 2);
            Assert.True(File.Exists(Path.Combine(rig.Root, "t", "_delta_log", started.Name)));
        }

        [Fact]
        public async Task HeaderlessPartsProtectedWhileStartedExists()
        {
            var table = await Checkpointed();
            Executed(table, 8);
            // A started whose original commits are gone, with its parts and no header
            var started = Advance(8);
            var (startedPart, startedHeader) = Manifest(started);
            var startedRecord = CleanupRecordKit.Started(started, startedHeader);
            // Another advance's parts and header with nothing started
            var leftover = Advance(8);
            var (leftoverPart, leftoverHeader) = Manifest(leftover);
            Plant(table, startedPart, startedRecord, leftoverPart, leftoverHeader);

            AssertDeclined(await Run(Cleanup(table)), MaintenanceReason.ManifestSuspended);

            Assert.Equal(Names(startedPart, startedRecord), Controls(table));

            // A record of unknown association at the same C keeps leftovers there
            var unknown = Advance(8);
            var unknownStarted = Torn(CleanupRecordKit.Started(unknown, Manifest(unknown).Header));
            var kept = Advance(8);
            var (keptPart, keptHeader) = Manifest(kept);
            Plant(table, unknownStarted, keptPart, keptHeader);

            AssertDeclined(await Run(Cleanup(table)), MaintenanceReason.ManifestSuspended);

            Assert.Equal(Names(startedPart, startedRecord, unknownStarted, keptPart, keptHeader), Controls(table));
        }

        // Two valid starteds of one advance that name different headers
        private static PlantedRecord[] ConflictingStarts(RecordFixture fixture, bool headers)
        {
            var first = Manifest(fixture).Header;
            var second = Manifest(fixture).Header;
            var starts = new[] { CleanupRecordKit.Started(fixture, first), CleanupRecordKit.Started(fixture, second) };
            return headers ? starts.Append(first).Append(second).ToArray() : starts;
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task ConflictingStartsSuspend(bool headers)
        {
            var table = await Checkpointed();
            // Every original commit is still listed
            var records = ConflictingStarts(Advance(8), headers);
            Plant(table, records);

            AssertDeclined(await Run(Cleanup(table)), MaintenanceReason.ManifestSuspended);

            Assert.Equal(Names(records), Controls(table));
        }

        [Fact]
        public async Task ConflictedGroupKeepsSameBoundaryLeftovers()
        {
            var table = await Checkpointed();
            // Another advance's parts and header with nothing started
            var (part, header) = Manifest(Advance(8));
            var records = ConflictingStarts(Advance(8), false).Append(part).Append(header).ToArray();
            Plant(table, records);

            AssertDeclined(await Run(Cleanup(table)), MaintenanceReason.ManifestSuspended);

            Assert.Equal(Names(records), Controls(table));
        }

        [Theory]
        [InlineData("successor")]
        [InlineData("probe")]
        [InlineData("duplicate")]
        [InlineData("gap")]
        public async Task FailedRecoveryListingCheckPreservesReservation(string failure)
        {
            var table = await Checkpointed();
            var fixture = Advance(8);
            var (part, header) = Manifest(fixture);
            var done = CleanupRecordKit.Done(fixture, header);
            Plant(table, part, header, CleanupRecordKit.Started(fixture, header), done);
            var storage = new LocalTestStorage(table.Local);
            var successor = $"{15:D20}.json";
            switch (failure)
            {
                case "successor":
                    storage.StatAnswer = (path, entry) => path.Name == successor ? new IOEntry(path.Full) { Size = 1 } : entry;
                    break;
                case "probe":
                    storage.StatAnswer = (path, entry) => path.Name == successor ? throw new IOException("probe") : entry;
                    break;
                case "duplicate":
                    storage.AfterLs = listed => listed.Append(listed.Single(x => x.Name == done.Name)).ToList();
                    break;
                case "gap":
                    // The published head's commit is missing from the listing
                    storage.AfterLs = listed => listed.Where(x => x.Name != $"{14:D20}.json").ToList();
                    break;
            }
            var before = Snapshot(table);

            AssertDeclined(await Run(Cleanup(table, storage)), MaintenanceReason.ListingIneligible);

            Assert.Equal(before, Snapshot(table));
            Assert.Equal(8, (await Fence(table)).ReservedFloor);

            // A listing that passes its checks releases them
            storage.StatAnswer = null;
            storage.AfterLs = null;
            AssertCompleted(await Run(Cleanup(table, storage)));
            Assert.Equal(Names(done), Controls(table));
        }

        // The checkpointed table on an owned in-memory store the assertion makes eligible, with an earlier done at 4 and every record of a completed advance to 8
        private async Task<MemoryTable> CompletedInMemory()
        {
            var hooks = OwnedHookFileStorage.Over(Files.Of.InternalMemory($"./recovery-{Guid.NewGuid():N}"));
            var configuration = CleanupConfiguration.Validate(hooks, "t", true, new CleanupOptIns(s_retention, null));
            Assert.True(configuration.Store.Eligible);
            await BoundarySelectorTests.Create(hooks, 14, BoundarySelectorTests.Old);
            foreach (var version in new long[] { 4, 8, 12 })
            {
                Assert.Equal(BuildStatus.Completed, (await TryBuild(hooks, version)).Status);
            }
            var earlier = Advance(4);
            var fixture = Advance(8);
            var (part, header) = Manifest(fixture);
            var table = new MemoryTable()
            {
                Hooks = hooks,
                Configuration = configuration,
                EarlierDone = CleanupRecordKit.Done(earlier, Manifest(earlier).Header),
                Part = part,
                Header = header,
                Progress = CleanupRecordKit.Progress(header, 1),
                Started = CleanupRecordKit.Started(fixture, header),
                LogsComplete = CleanupRecordKit.LogsComplete(fixture, header),
                Done = CleanupRecordKit.Done(fixture, header)
            };
            foreach (var record in table.All)
            {
                await CleanupRecordKit.PlantAsync(hooks, "t", record);
            }
            hooks.ClearRequests();
            return table;
        }

        private sealed class MemoryTable
        {
            public required HookFileStorage Hooks { get; init; }

            public required CleanupConfiguration Configuration { get; init; }

            public required PlantedRecord EarlierDone { get; init; }

            public required PlantedRecord Part { get; init; }

            public required PlantedRecord Header { get; init; }

            public required PlantedRecord Progress { get; init; }

            public required PlantedRecord Started { get; init; }

            public required PlantedRecord LogsComplete { get; init; }

            public required PlantedRecord Done { get; init; }

            public PlantedRecord[] All => new[] { EarlierDone, Part, Header, Progress, Started, LogsComplete, Done };

            public DeltaTableCleanup Cleanup() => Recovering(new DeltaTableCleanup(Configuration, Hooks, new FakeTimeProvider(s_now), "t", null, () => Hooks, new DiscardCounters(), null, "t"));

            // Control record names the store removed, in order
            public List<string> Removed() => Hooks.Requests.Where(x => x.StartsWith("Rm ")).Select(x => x.Substring(x.LastIndexOf('/') + 1)).ToList();

            public async Task<string[]> Controls() => (await DeltaTransactionReader.ListLog(Hooks, "t")).Controls.Select(x => x.Name).Order(StringComparer.Ordinal).ToArray();
        }

        [Fact]
        public async Task TeardownDeletesInOrderAndKeepsTheNewestDone()
        {
            var table = await CompletedInMemory();

            AssertCompleted(await Run(table.Cleanup()));

            // Header and parts, then progress, then started and logs-complete, then older completions
            var removed = table.Removed();
            Assert.Equal(6, removed.Count);
            Assert.Equal(Names(table.Part, table.Header), removed.Take(2).Order(StringComparer.Ordinal));
            Assert.Equal(table.Progress.Name, removed[2]);
            Assert.Equal(Names(table.Started, table.LogsComplete), removed.Skip(3).Take(2).Order(StringComparer.Ordinal));
            Assert.Equal(table.EarlierDone.Name, removed[5]);
            Assert.Equal(Names(table.Done), await table.Controls());
        }

        [Fact]
        public async Task TeardownStoppedAtTheDeadlineKeepsTheRest()
        {
            var table = await CompletedInMemory();
            var clock = new FakeTimeProvider(s_now);
            // The deadline passes while the first record is removed
            table.Hooks.Before = (verb, path) =>
            {
                if (verb == "Rm")
                {
                    clock.Advance(TimeSpan.FromHours(2));
                }
                return Task.CompletedTask;
            };

            var result = await Run(table.Cleanup(), Context(14, clock));

            Assert.Equal((OutcomeKind.Preempted, MaintenanceReason.JobDeadline), (result.Kind, result.Reason));
            Assert.Single(table.Removed());
            Assert.Equal(table.All.Length - 1, (await table.Controls()).Length);
        }

        [Fact]
        public async Task FailedTeardownDeleteKeepsTheLaterPhases()
        {
            var table = await CompletedInMemory();
            table.Hooks.Before = (verb, path) => verb == "Rm" && path.Name == table.Header.Name ? throw new IOException("Remove failed") : Task.CompletedTask;

            AssertDeclined(await Run(table.Cleanup()), MaintenanceReason.Transient);

            Assert.Equal(Names(table.All), await table.Controls());
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task RecoveryStopsOpeningControlRecordsAfterItsDeadline(bool resumable)
        {
            var table = await CompletedInMemory();
            if (resumable)
            {
                // Only the parts, header and started, whose header is read next
                foreach (var record in new[] { table.EarlierDone, table.Progress, table.LogsComplete, table.Done })
                {
                    await table.Hooks.Rm($"/t/_delta_log/{record.Name}");
                }
                table.Hooks.ClearRequests();
            }
            var clock = new FakeTimeProvider(s_now);
            var reads = 0;
            // The deadline passes while the first record is read
            table.Hooks.Before = (verb, path) =>
            {
                if (verb == "OpenRead" && path.Name.StartsWith(DeltaTransactionReader.CleanupControlPrefix, StringComparison.Ordinal) && ++reads == 1)
                {
                    clock.Advance(TimeSpan.FromHours(2));
                }
                return Task.CompletedTask;
            };
            // This worker's lease of the advance whose records it stops reading
            Assert.True(ControlName.TryParse(table.Started.Name, out var started));
            var ledger = new CapacityLedger(() => default);
            ledger.Restore(default, TableLeases.KeyOf(table.Hooks, "t"), started.AdvanceId, Hours(1), CapacityState.Runnable);

            var result = await Run(table.Cleanup(), Context(14, clock, ledger));

            Assert.Equal((OutcomeKind.Preempted, MaintenanceReason.JobDeadline), (result.Kind, result.Reason));
            Assert.Equal(1, reads);
            Assert.Empty(table.Removed());
            Assert.True(ledger.Current == Hours(1), $"A stopped recovery left {ledger.Current}");
        }

        [Fact]
        public async Task TeardownRetryRemovesAnOlderTornCompletionAfterItsValidSiblingIsGone()
        {
            var table = await CompletedInMemory();
            // The older completion as a valid copy and a torn sibling that sorts after it
            await table.Hooks.Rm($"/t/_delta_log/{table.EarlierDone.Name}");
            var valid = table.EarlierDone with { Name = CleanupRecordKit.Rename(table.EarlierDone.Name, NameField.AttemptId, new string('0', ControlTokens.Length)) };
            var torn = Torn(table.EarlierDone) with { Name = CleanupRecordKit.Rename(table.EarlierDone.Name, NameField.AttemptId, new string('f', ControlTokens.Length)) };
            await CleanupRecordKit.PlantAsync(table.Hooks, "t", valid);
            await CleanupRecordKit.PlantAsync(table.Hooks, "t", torn);
            table.Hooks.Before = (verb, path) => verb == "Rm" && path.Name == torn.Name ? throw new IOException("Remove failed") : Task.CompletedTask;

            AssertDeclined(await Run(table.Cleanup()), MaintenanceReason.Transient);
            Assert.Contains(table.Done.Name, await table.Controls());
            Assert.Contains(torn.Name, await table.Controls());

            table.Hooks.Before = null;
            AssertCompleted(await Run(table.Cleanup()));

            Assert.Equal(Names(table.Done), await table.Controls());
        }

        // Run by a test that owns the deletes switch
        internal async Task ReleasedRecordsFollowTheDeletesSwitch(Action<bool> deletesOff)
        {
            var table = await Checkpointed();
            var fixture = Advance(8);
            var (part, header) = Manifest(fixture);
            var done = CleanupRecordKit.Done(fixture, header);
            Plant(table, part, header, CleanupRecordKit.Started(fixture, header), done);
            var before = Snapshot(table);

            deletesOff(true);
            AssertDeclined(await Run(Cleanup(table)), MaintenanceReason.DeletesDisabled);
            Assert.Equal(before, Snapshot(table));

            deletesOff(false);
            AssertCompleted(await Run(Cleanup(table)));
            Assert.Equal(Names(done), Controls(table));
        }

        private sealed class WorkerRig
        {
            public required FakeTimeProvider Clock { get; init; }

            public required DeltaMaintenanceBudget Budget { get; init; }

            public required DeltaMaintenanceWorker Worker { get; init; }

            public required DeltaTableCleanup Cleanup { get; init; }

            public required DeltaMaintenanceWorkerTests.CollectingLogger Logs { get; init; }

            public required string Root { get; init; }

            public async Task TickAsync()
            {
                Budget.TickNow();
                await Worker.CurrentJob.WaitAsync(s_wait);
            }

            public async Task DaysAsync(int days, int ticks)
            {
                for (var day = 0; day < days; day++)
                {
                    Clock.Advance(TimeSpan.FromDays(1));
                    for (var tick = 0; tick < ticks; tick++)
                    {
                        await TickAsync();
                    }
                }
            }
        }

        // A retention worker with a real cleanup over the table, on a budget of its own
        private WorkerRig Worker(IFileStorage storage, CleanupConfiguration configuration, TimeSpan retention, string root)
        {
            var clock = new FakeTimeProvider(s_now);
            var cleanup = Recovering(new DeltaTableCleanup(configuration, storage, clock, "t", null, () => storage, new DiscardCounters(), null, "t") { AdvanceOffset = TimeSpan.FromHours(12) });
            var budget = new DeltaMaintenanceBudget(new MaintenanceBudgetSettings(2, Path.Combine(_directory, ScratchOwners.ShortId())), clock, manualTick: true)
            {
                Buckets = MaintenanceBucketsTests.Unthrottled()
            };
            var logs = new DeltaMaintenanceWorkerTests.CollectingLogger();
            var worker = new DeltaMaintenanceWorker(budget, new MaintenanceWorkerSettings(0, null, clock)
            {
                PeriodicEnabled = false,
                CleanupEligible = true,
                Retention = retention
            }, storage, "t", "t", logs) { Cleanup = cleanup };
            budget.Register(worker);
            return new WorkerRig() { Clock = clock, Budget = budget, Worker = worker, Cleanup = cleanup, Logs = logs, Root = root };
        }

        // Inspected and covered at the head
        private static async Task CoverAsync(WorkerRig rig, long head)
        {
            rig.Worker.Published(head);
            await rig.TickAsync();
            await rig.TickAsync();
            Assert.True(rig.Worker.CoverageConfirmed);
        }

        // A retention worker with a real cleanup over own commits 0 to 6 and a marked root at 4, inspected and covered
        private async Task<WorkerRig> CoveredWorker()
        {
            var root = Path.Combine(_directory, ScratchOwners.ShortId());
            Directory.CreateDirectory(root);
            var storage = Files.Of.LocalDisk(root);
            _disposables.Add(storage);
            var day = TimeSpan.FromDays(1);
            var rig = Worker(storage, CleanupConfiguration.Validate(storage, "t", false, new CleanupOptIns(day, null)), day, root);
            var published = new Dictionary<string, string>()
            {
                [RetentionProperties.LogKey] = RetentionProperties.Format(day),
                [RetentionProperties.DeletedFileKey] = RetentionProperties.Format(day)
            };
            await BoundarySelectorTests.Create(storage, 6, BoundarySelectorTests.Old, published);
            Assert.Equal(BuildStatus.Completed, (await TryBuild(storage, 4)).Status);
            await CoverAsync(rig, 6);
            return rig;
        }

        // Hours of drain on the request axis
        private static CapacityDemand Hours(double hours) => new CapacityDemand((long)Math.Round(hours * 3600 * MaintenanceBuckets.DefaultRequestsPerSecond), 0);

        // Own commits 0 to 14 before the cutoff, each removing the file the one before added, checkpointed at 4, 8 and 12
        private async Task Churn(IFileStorage storage)
        {
            var published = new Dictionary<string, string>()
            {
                [RetentionProperties.LogKey] = RetentionProperties.Format(s_retention),
                [RetentionProperties.DeletedFileKey] = RetentionProperties.Format(s_retention)
            };
            for (long version = 0; version <= 14; version++)
            {
                var add = DeltaLakeCheckpointBuildTests.Add($"part-{version:D4}.parquet");
                var actions = version == 0
                    ? new[] { DeltaTestKit.Protocol(), DeltaTestKit.Metadata(DeltaTestKit.UserSchema, published), add }
                    : new[] { add, DeltaLakeCheckpointBuildTests.Remove($"part-{version - 1:D4}.parquet", BoundarySelectorTests.Old(version)) };
                await DeltaTestKit.WriteOwnCommit(storage, "t", version, 0, BoundarySelectorTests.Old(version), actions);
            }
            foreach (var version in new long[] { 4, 8, 12 })
            {
                Assert.Equal(BuildStatus.Completed, (await TryBuild(storage, version)).Status);
            }
        }

        // The churned history on an eligible local disk table with retention on
        private async Task<Table> Churned()
        {
            var root = Path.Combine(_directory, ScratchOwners.ShortId());
            Directory.CreateDirectory(root);
            var local = Files.Of.LocalDisk(root);
            _disposables.Add(local);
            var configuration = CleanupConfiguration.Validate(local, "t", false, new CleanupOptIns(s_retention, null));
            Assert.True(configuration.Store.Eligible);
            await Churn(local);
            return new Table() { Local = local, Root = root, Configuration = configuration };
        }

        // What an advance published through the control publisher
        private sealed class Published
        {
            public AdvanceBoundary? Boundary { get; set; }

            public Association? Association { get; set; }

            public StartedRecord? Started { get; set; }

            public ProgressRecord? Progress { get; set; }
        }

        // An inline run's cleanup whose advances start
        private static DeltaTableCleanup Starting(Table table) => new DeltaTableCleanup(table.Configuration, table.Local, new FakeTimeProvider(s_now), "t", null, () => table.Local, new DiscardCounters(), null, "t");

        // Plans in parts of two paths and starts the advance as the job does; with progress, then publishes logs-complete and the first part's progress record as its delete turns will
        private async Task<Published> StartAsync(DeltaTableCleanup cleanup, IFileStorage storage, bool progress = false)
        {
            var published = new Published();
            long firstPart = 0;
            var job = cleanup.AdvanceForTests;
            job.SelectedHookForTests = boundary => published.Boundary = boundary;
            job.PlanForTests = async (configuration, storage, table, boundary, classes, context) =>
            {
                var plan = await AdvancePlanner.PlanAsync(configuration, storage, table, boundary, classes, context, new AdvanceWindowLimits() { PartPaths = 2 });
                Assert.True(plan.Status == AdvancePlanStatus.Planned && plan.Parts.Count > 2, $"{plan.Status} {plan.Reason}: {plan.Detail}, {plan.Parts.Count} parts");
                firstPart = plan.Parts[0].Paths;
                return plan;
            };
            AssertCompleted(await Run(cleanup));
            var active = cleanup.ActiveForTests!;
            published.Association = active.Association;
            published.Started = active.Started;
            if (progress)
            {
                var association = active.Association;
                var context = Context(14, new FakeTimeProvider(s_now));
                var publisher = new ControlPublisher(storage, "t", context);
                await publisher.PublishAsync(ControlKind.LogsComplete, association, -1, () => ControlRecords.Encode(new LogsCompleteRecord(association, active.Started.TableId, active.Started.O)));
                // The first part processed
                var record = new ProgressRecord(association, 1, new RecordCursor(1, 0, ControlRecords.FirstPathOffset), firstPart);
                await publisher.PublishAsync(ControlKind.Progress, association, 1, () => ControlRecords.Encode(record));
                await context.SettleAsync();
                published.Progress = record;
            }
            return published;
        }

        // The shared estimate of what a started advance has left after its progress, as recovery lists the table's log
        private static async Task<CapacityDemand> RemainingAsync(IFileStorage storage, CleanupConfiguration configuration, StartedRecord started, bool logsComplete, ProgressRecord? progress) =>
            CapacityEstimator.Advance(await RemainingInputsAsync(storage, configuration, started, logsComplete, progress), MaintenanceBuckets.DefaultRequestsPerSecond);

        // With progress, the parts left as the started's header describes them
        private static async Task<AdvanceDemandInputs> RemainingInputsAsync(IFileStorage storage, CleanupConfiguration configuration, StartedRecord started, bool logsComplete, ProgressRecord? progress)
        {
            var store = configuration.Store.Store;
            var c = started.Association.C;
            var listed = await storage.Ls($"/t/{DeltaTransactionReader.DeltaLogDirName}");
            var logTargets = logsComplete ? 0 : listed.Count(x => Regex.IsMatch(x.Name, @"^\d{20}\.(json|crc|checkpoint\.parquet)$") && long.Parse(x.Name.Substring(0, 20)) < c);
            var name = $"{c:D20}.checkpoint.parquet";
            // C as listed; an in-memory listing carries no size
            var checkpoint = listed.Single(x => x.Name == name).Size ?? 0;
            // The root at C holds the protocol and metadata establishment read whole
            var root = (await DeltaTestKit.ReadBytes(storage, $"/t/{DeltaTransactionReader.DeltaLogDirName}{name}")).Length;
            var parts = started.Totals.Parts - (progress?.Next.Part ?? 0);
            var partBytes = started.Totals.PartBytes;
            if (progress != null)
            {
                var descriptors = new List<PartDescriptor>();
                var header = await DeltaTestKit.ReadBytes(storage, $"/t/{DeltaTransactionReader.DeltaLogDirName}{started.Association.HeaderName}");
                Assert.Equal(RecordStatus.Valid, ControlRecords.DecodeHeader(header, out _, descriptors));
                partBytes = descriptors.Skip(progress.Next.Part).Sum(x => (long)x.Bytes);
            }
            // The head commit recovery's own evidence anchors at, and the native root a local key holds beyond the table's path
            var anchor = (await DeltaTestKit.ReadBytes(storage, $"/t/{DeltaTransactionReader.DeltaLogDirName}{14:D20}.json")).Length;
            var keyExtra = Math.Max(0, DeletionExecutor.KeyPrefixLength(configuration) - configuration.Store.Table.Full.Length - 1);
            return new AdvanceDemandInputs(logTargets, started.Totals.Emitted - (progress?.Processed ?? 0), parts, partBytes, checkpoint, anchor, root, 1, listed.Count,
                store.Paging, DeletionRun.MaxOutstanding, CapacityEstimator.DefaultLatency, UnknownStore: store.Kind == StoreKind.Unknown, KeyExtra: keyExtra);
        }

        [Fact]
        public async Task CapacityRebuiltAfterRestart()
        {
            var table = await Churned();
            // Before the restart an advance published its parts, header, started record, logs-complete and first progress record
            var published = await StartAsync(Starting(table), table.Local, progress: true);
            var demand = await RemainingAsync(table.Local, table.Configuration, published.Started!, true, published.Progress);
            Assert.True(published.Progress!.Processed > 0 && published.Started!.Totals.Parts > 2);

            // A fresh process whose ledger holds another table's demand, half the advance's short of twelve hours
            var rig = Worker(table.Local, table.Configuration, s_retention, table.Root);
            var other = rig.Budget.Capacity.Restore(default, "other-table", ControlTokens.New(), Hours(12) - new CapacityDemand(demand.Requests / 2, 0), CapacityState.Runnable);
            Assert.True(rig.Budget.Capacity.Admits());
            await CoverAsync(rig, 14);
            await rig.DaysAsync(1, 4);

            // The first recovery restored the advance's remaining demand as runnable
            Assert.Equal(other.Demand + demand, rig.Budget.Capacity.Total(CapacityState.Runnable));
            Assert.Equal(other.Demand + demand, rig.Budget.Capacity.Current);
            Assert.False(rig.Budget.Capacity.Admits());
            // Another table's advance waits on it
            var second = await Checkpointed();
            AssertDeclined(await Run(Cleanup(second), Context(14, new FakeTimeProvider(s_now), rig.Budget.Capacity)), MaintenanceReason.DeleteBacklog);

            // The stopped worker's restored demand is paused, and the other table's stays runnable
            await rig.Worker.StopAsync(s_wait);
            Assert.Equal(demand, rig.Budget.Capacity.Total(CapacityState.Paused));
            Assert.Equal(other.Demand, rig.Budget.Capacity.Current);
        }

        [Fact]
        public async Task ResumedDemandPricesThePartsLeftByTheirOwnBytes()
        {
            var table = await Checkpointed();
            // To the newest checkpoint, which roots the capability
            var fixture = Advance(12);
            // A hundred parts of 10,000 60-byte paths, then a hundred of 1,394 1,500-byte paths
            var descriptors = Enumerable.Range(0, 200).Select(i => i < 100 ? (Paths: 10_000, Length: 60) : (Paths: 1_394, Length: 1_500))
                .Select((x, i) => new PartDescriptor(i, ControlTokens.New(), x.Paths, ControlRecords.PartLength(x.Paths * x.Length, x.Paths), new byte[32])).ToList();
            var header = CleanupRecordKit.Header(fixture, descriptors);
            var started = CleanupRecordKit.Started(fixture, header);
            // Every short path processed
            var progress = CleanupRecordKit.Progress(header, 1, new RecordCursor(100, 0, ControlRecords.FirstPathOffset), 1_000_000);
            Plant(table, header, started, progress);
            var ledger = new CapacityLedger(() => default);

            // The fixture's C is not the content its header names, so the resume suspends
            AssertDeclined(await Run(Cleanup(table), Context(14, new FakeTimeProvider(s_now), ledger)), MaintenanceReason.HistoryChanged);

            Assert.Equal(RecordStatus.Valid, ControlRecords.Decode(started.Bytes, out StartedRecord? startedRecord));
            Assert.Equal(RecordStatus.Valid, ControlRecords.Decode(progress.Bytes, out ProgressRecord? progressRecord));
            var inputs = await RemainingInputsAsync(table.Local, table.Configuration, startedRecord!, false, progressRecord);
            Assert.Equal(CapacityEstimator.Advance(inputs, MaintenanceBuckets.DefaultRequestsPerSecond), ledger.Total(CapacityState.Suspended));
            // Gates priced: what each further checkpoint byte adds
            long Gates(AdvanceDemandInputs x) => CapacityEstimator.Advance(x with { CheckpointBytes = x.CheckpointBytes + 1 }, MaintenanceBuckets.DefaultRequestsPerSecond).Bytes - CapacityEstimator.Advance(x, MaintenanceBuckets.DefaultRequestsPerSecond).Bytes;
            var priced = Gates(inputs) - Gates(inputs with { PayloadPaths = 0 });
            // 344 of the long paths to a 1 MiB buffer
            var perSlice = DeletionRunSettings.DefaultMaxBufferBytes / (DeletionRun.TargetOverheadBytes + 2 * 1_500);
            var slices = (inputs.PayloadPaths + perSlice - 1) / perSlice;
            Assert.True(priced >= slices, $"Priced {priced} payload slices, but {slices} buffered ones remain");
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task RecoveryIsNotBlockedByAdmissionDebt(bool done)
        {
            var table = await Checkpointed();
            var fixture = Advance(8);
            var (part, header) = Manifest(fixture);
            var started = CleanupRecordKit.Started(fixture, header);
            var completion = CleanupRecordKit.Done(fixture, header);
            // Released by its own completion, or headerless with its whole original range listed
            var records = done ? new[] { part, header, started, completion } : new[] { part, started };
            Plant(table, records);
            // Another table's thirteen hours, and this advance's demand as an earlier recovery restored it
            var ledger = new CapacityLedger(() => default);
            var other = ledger.Restore(default, "other-table", ControlTokens.New(), Hours(13), CapacityState.Runnable);
            ledger.Restore(default, TableLeases.KeyOf(table.Local, "t"), fixture.AdvanceId, Hours(1), CapacityState.Suspended);

            AssertDeclined(await Run(Cleanup(table), Context(14, new FakeTimeProvider(s_now), ledger)), MaintenanceReason.DeleteBacklog);

            // Recovery released and tore the advance down before admission declined the next one
            Assert.Equal(done ? Names(completion) : Array.Empty<string>(), Controls(table));
            Assert.Equal(ControlFence.NoReservation, (await Fence(table)).ReservedFloor);
            Assert.Equal(other.Demand, ledger.Current);
            Assert.Equal(default, ledger.Total(CapacityState.Suspended));
        }

        [Fact]
        public async Task RootCountNeverControlsAdmission()
        {
            var crowded = await Checkpointed();
            // Thousands of files in the table's root beside its log
            for (int i = 0; i < 2000; i++)
            {
                File.WriteAllBytes(Path.Combine(crowded.Root, "t", $"orphan-{i:D5}.parquet"), Array.Empty<byte>());
            }
            var bare = await Checkpointed();
            var ledger = new CapacityLedger(() => default);

            // An empty ledger admits the crowded table's advance
            AssertCompleted(await Run(Cleanup(crowded), Context(14, new FakeTimeProvider(s_now), ledger)));
            Assert.Equal(default, ledger.Current);

            // A saturated ledger declines either table
            var other = ledger.Restore(default, "other-table", ControlTokens.New(), Hours(13), CapacityState.Runnable);
            AssertDeclined(await Run(Cleanup(crowded), Context(14, new FakeTimeProvider(s_now), ledger)), MaintenanceReason.DeleteBacklog);
            AssertDeclined(await Run(Cleanup(bare), Context(14, new FakeTimeProvider(s_now), ledger)), MaintenanceReason.DeleteBacklog);

            // On an empty ledger either table's plan declines before its manifest when its own deletes take an hour each
            other.Dispose();
            foreach (var table in new[] { crowded, bare })
            {
                var cleanup = Starting(table);
                for (int i = 0; i < 10; i++)
                {
                    cleanup.ObserveLatency(TimeSpan.FromHours(1), cleanup.Settings.MaxPaths);
                }
                var planned = 0;
                cleanup.AdvanceForTests.PlannedHookForTests = _ => planned++;

                AssertDeclined(await Run(cleanup, Context(14, new FakeTimeProvider(s_now), ledger)), MaintenanceReason.DeleteBacklog);

                // Planned, with no record published and nothing reserved
                Assert.Equal(1, planned);
                Assert.Empty(Controls(table));
                Assert.Equal(default, ledger.Current);
                Assert.Null(cleanup.ActiveForTests);
            }
        }

        [Fact]
        public async Task CapacityAdmissionIsProspectiveAndAtomic()
        {
            var rig = await CoveredWorker();
            var selected = 0;
            var planned = 0;
            rig.Cleanup.AdvanceForTests.SelectedHookForTests = _ => selected++;
            rig.Cleanup.AdvanceForTests.PlannedHookForTests = _ => planned++;
            var other = rig.Budget.Capacity.Restore(default, "other-table", ControlTokens.New(), Hours(13), CapacityState.Runnable);

            await rig.DaysAsync(1, 4);

            // Declined before selection and planning, still due today and still covered
            Assert.Equal((0, 0), (selected, planned));
            Assert.Contains(rig.Logs.Entries, x => x.Message.Contains(nameof(MaintenanceReason.DeleteBacklog)));
            Assert.True(rig.Cleanup.IsDue(JobKind.Advance, 6, rig.Clock.GetUtcNow()));
            Assert.True(rig.Worker.CoverageConfirmed);

            // Below six hours admission reopens and the same day's advance plans
            other.Dispose();
            rig.Clock.Advance(TimeSpan.FromMinutes(30));
            for (int tick = 0; tick < 4; tick++)
            {
                await rig.TickAsync();
            }
            Assert.Equal((1, 1), (selected, planned));
            Assert.False(rig.Cleanup.IsDue(JobKind.Advance, 6, rig.Clock.GetUtcNow()));
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task HeaderlessStartedRetainsAssociation(bool intact)
        {
            var table = await Churned();
            var published = await StartAsync(Starting(table), table.Local);
            var c = published.Boundary!.C;
            var started = published.Started!;
            // The header is gone; only the started record associates the advance
            File.Delete(table.Log(started.Association.HeaderName));
            if (!intact)
            {
                File.Delete(table.Log($"{2:D20}.json"));
            }
            var remaining = await RemainingAsync(table.Local, table.Configuration, started, false, null);
            var ledger = new CapacityLedger(() => default);

            var result = await Run(Cleanup(table), Context(14, new FakeTimeProvider(s_now), ledger));

            if (intact)
            {
                // Its whole original range releases it
                AssertCompleted(result);
                Assert.Empty(Controls(table));
                Assert.Equal(ControlFence.NoReservation, (await Fence(table)).ReservedFloor);
                Assert.Equal(default, ledger.Total(CapacityState.Suspended));
                return;
            }
            // Start evidence without a releasing rule: suspended, reserving C, with its demand reported apart
            AssertDeclined(result, MaintenanceReason.ManifestSuspended);
            Assert.Contains(Controls(table), x => ControlName.TryParse(x, out var name) && name.Kind == ControlKind.Started);
            Assert.Equal(c, (await Fence(table)).ReservedFloor);
            var below = await TryBuild(table.Local, c - 1);
            Assert.Equal((BuildStatus.Declined, MaintenanceReason.CleanupGate), (below.Status, below.Reason));
            Assert.Equal(remaining, ledger.Total(CapacityState.Suspended));
            Assert.Equal(default, ledger.Current);
        }

        [Fact]
        public async Task OriginalRangeStartsAtTheRootsOwnCommit()
        {
            var table = await Churned();
            // Rooted at 4 with its own commit listed
            for (long version = 0; version < 4; version++)
            {
                File.Delete(table.Log($"{version:D20}.json"));
            }
            var published = await StartAsync(Starting(table), table.Local);
            var started = published.Started!;
            // Headerless, with the root's own commit gone
            File.Delete(table.Log(started.Association.HeaderName));
            File.Delete(table.Log($"{4:D20}.json"));
            var records = Snapshot(table);

            AssertDeclined(await Run(Cleanup(table)), MaintenanceReason.ManifestSuspended);

            Assert.Equal((4L, 4L), (started.O, started.FirstJson));
            Assert.Equal(records, Snapshot(table));
        }

        // A user's wrapper that lists in key order, as an object store does
        private sealed class OrderedStorage : ForwardingStorage, IFileStorage
        {
            public OrderedStorage(IFileStorage inner) : base(inner)
            {
            }

            async Task<IReadOnlyCollection<IOEntry>> IFileStorage.Ls(IOPath? path, bool recurse, CancellationToken cancellationToken) =>
                (await Inner.Ls(path, recurse, cancellationToken)).OrderBy(x => x.Path.Full, StringComparer.Ordinal).ToList();
        }

        [Fact]
        public async Task RestoredDemandCountsAnUnknownStoresListingPages()
        {
            // A wrapper of the user's makes the store unknown, paged as S3 pages
            var storage = new OrderedStorage(new HookFileStorage(Files.Of.InternalMemory($"./paged-{Guid.NewGuid():N}")));
            var configuration = CleanupConfiguration.Validate(storage, "t", true, new CleanupOptIns(s_retention, null));
            Assert.True(configuration.Store is { Eligible: true, Store: { Kind: StoreKind.Unknown, Paging: ListingPaging.S3 } });
            await Churn(storage);
            DeltaTableCleanup Cleanup() => new DeltaTableCleanup(configuration, storage, new FakeTimeProvider(s_now), "t", null, () => storage, new DiscardCounters(), null, "t");
            var published = await StartAsync(Cleanup(), storage, progress: true);
            // A log listed in more than one page
            for (int i = 0; i < 1000; i++)
            {
                await CleanupRecordKit.PlantAsync(storage, "t", $".flowtide-{Guid.NewGuid():N}.checkpoint.tmp", new byte[1]);
            }
            var ledger = new CapacityLedger(() => default);

            // A restart resumes it
            AssertCompleted(await Run(Recovering(Cleanup()), Context(14, new FakeTimeProvider(s_now), ledger)));

            // Each remaining gate lists a second page
            var demand = await RemainingAsync(storage, configuration, published.Started!, true, published.Progress);
            Assert.Equal(demand, ledger.Current);
            Assert.Equal(1, MaintenanceBuckets.ExtraPages(ListingPaging.S3, (await storage.Ls("/t/_delta_log/")).Count));
        }

        [Theory]
        [InlineData("local", 200)]
        [InlineData("memory", 200)]
        [InlineData("local", 5000)]
        public async Task PublishedRecordsAreRecoveredByDone(string kind, long requestsPerSecond)
        {
            IFileStorage storage;
            CleanupConfiguration configuration;
            HookFileStorage? hooks = null;
            string? logDirectory = null;
            if (kind == "local")
            {
                var table = await Churned();
                (storage, configuration, logDirectory) = (table.Local, table.Configuration, table.LogDirectory);
            }
            else
            {
                hooks = OwnedHookFileStorage.Over(Files.Of.InternalMemory($"./published-{Guid.NewGuid():N}"));
                configuration = CleanupConfiguration.Validate(hooks, "t", true, new CleanupOptIns(s_retention, null));
                Assert.True(configuration.Store.Eligible);
                await Churn(hooks);
                storage = hooks;
            }
            DeltaTableCleanup Cleanup() => new DeltaTableCleanup(configuration, storage, new FakeTimeProvider(s_now), "t", null, () => storage, new DiscardCounters(), null, "t");
            async Task<string[]> Controls() => (await DeltaTransactionReader.ListLog(storage, "t")).Controls.Select(x => x.Name).Order(StringComparer.Ordinal).ToArray();
            var published = await StartAsync(Cleanup(), storage, progress: true);
            var started = published.Started!;
            var association = published.Association!;

            // After a restart the resumed advance holds what it has left
            var ledger = new CapacityLedger(() => default, requestsPerSecond);
            AssertCompleted(await Run(Recovering(Cleanup()), Context(14, new FakeTimeProvider(s_now), ledger)));
            Assert.Equal(CapacityEstimator.Advance(await RemainingInputsAsync(storage, configuration, started, true, published.Progress), requestsPerSecond), ledger.Current);

            // It completes, and the done's first attempt lands torn under a sibling name
            var tear = false;
            var publisher = new ControlPublisher(storage, configuration.Store.Table, Context(14, new FakeTimeProvider(s_now)), faultForTests: point =>
            {
                if (point == "rename" && tear)
                {
                    tear = false;
                    using var temp = new FileStream(Directory.GetFiles(logDirectory!, ".flowtide-*.checkpoint.tmp").Single(), FileMode.Open);
                    temp.SetLength(40);
                }
            });
            tear = true;
            if (hooks != null)
            {
                hooks.TearWriteOf = ControlName.DoneSuffix;
                hooks.TearAfterBytes = 40;
            }
            var done = await publisher.PublishAsync(ControlKind.Done, association, -1, () => ControlRecords.Encode(new DoneRecord(association, started.TableId, started.O, started.A, started.Head,
                started.Anchor, new RecordCursor(started.Totals.Parts, 0, 0), started.Totals, started.Totals.Emitted)));
            var torn = (await Controls()).Single(x => x != done.Name && x.EndsWith(ControlName.DoneSuffix, StringComparison.Ordinal));
            Assert.Equal(40, (await DeltaTestKit.ReadBytes(storage, $"/t/_delta_log/{torn}")).Length);

            // Its own validated completion tears it down, the torn copy too, keeps the done and releases its demand
            AssertCompleted(await Run(Recovering(Cleanup()), Context(14, new FakeTimeProvider(s_now), ledger)));
            Assert.Equal(new[] { done.Name }, await Controls());
            Assert.Equal(ControlFence.NoReservation, ControlFence.Of(await DeltaTransactionReader.ListLog(storage, "t")).ReservedFloor);
            Assert.Equal(default, ledger.Current);
        }

        [Fact]
        public async Task StoppedTeardownReleasesASupersededAdvancesLease()
        {
            var hooks = OwnedHookFileStorage.Over(Files.Of.InternalMemory($"./superseded-{Guid.NewGuid():N}"));
            var configuration = CleanupConfiguration.Validate(hooks, "t", true, new CleanupOptIns(s_retention, null));
            Assert.True(configuration.Store.Eligible);
            await BoundarySelectorTests.Create(hooks, 14, BoundarySelectorTests.Old);
            foreach (var version in new long[] { 4, 8, 12 })
            {
                Assert.Equal(BuildStatus.Completed, (await TryBuild(hooks, version)).Status);
            }
            // A started advance to 8 between the dones of advances to 4 and 12
            var superseded = Advance(8);
            var (part, header) = Manifest(superseded);
            var newer = Advance(12);
            var newerDone = CleanupRecordKit.Done(newer, Manifest(newer).Header);
            var earlier = Advance(4);
            var earlierDone = CleanupRecordKit.Done(earlier, Manifest(earlier).Header);
            foreach (var record in new[] { part, header, CleanupRecordKit.Started(superseded, header), newerDone, earlierDone })
            {
                await CleanupRecordKit.PlantAsync(hooks, "t", record);
            }
            // The superseded advance's runnable lease as an earlier recovery restored it
            var ledger = new CapacityLedger(() => default);
            ledger.Restore(default, TableLeases.KeyOf(hooks, "t"), superseded.AdvanceId, Hours(1), CapacityState.Runnable);
            DeltaTableCleanup Cleanup() => Recovering(new DeltaTableCleanup(configuration, hooks, new FakeTimeProvider(s_now), "t", null, () => hooks, new DiscardCounters(), null, "t"));
            // The teardown stops at the older done, after the superseded advance's records are gone
            hooks.Before = (verb, path) => verb == "Rm" && path.Name == earlierDone.Name ? throw new IOException("Remove failed") : Task.CompletedTask;

            AssertDeclined(await Run(Cleanup(), Context(14, new FakeTimeProvider(s_now), ledger)), MaintenanceReason.Transient);

            // The rerun finishes the teardown and no lease outlives the superseded advance
            hooks.Before = null;
            AssertCompleted(await Run(Cleanup(), Context(14, new FakeTimeProvider(s_now), ledger)));
            Assert.Equal(new[] { newerDone.Name }, (await DeltaTransactionReader.ListLog(hooks, "t")).Controls.Select(x => x.Name).ToArray());
            Assert.Equal(default, ledger.Current);
        }

        [Fact]
        public async Task InterruptedTeardownOfAConflictedCompletionReleasesNothing()
        {
            var hooks = OwnedHookFileStorage.Over(Files.Of.InternalMemory($"./conflicted-{Guid.NewGuid():N}"));
            var configuration = CleanupConfiguration.Validate(hooks, "t", true, new CleanupOptIns(s_retention, null));
            Assert.True(configuration.Store.Eligible);
            await BoundarySelectorTests.Create(hooks, 14, BoundarySelectorTests.Old);
            foreach (var version in new long[] { 4, 8, 12 })
            {
                Assert.Equal(BuildStatus.Completed, (await TryBuild(hooks, version)).Status);
            }
            // A resumable advance to 8, and two completions of one advance to 12 that name different headers
            var resumable = Advance(8);
            var (part, header) = Manifest(resumable);
            var records = new[] { part, header, CleanupRecordKit.Started(resumable, header) };
            var conflicted = Advance(12);
            var conflicting = new[] { CleanupRecordKit.Done(conflicted, Manifest(conflicted).Header), CleanupRecordKit.Done(conflicted, Manifest(conflicted).Header) };
            foreach (var record in conflicting.Concat(records))
            {
                await CleanupRecordKit.PlantAsync(hooks, "t", record);
            }
            DeltaTableCleanup Cleanup() => Recovering(new DeltaTableCleanup(configuration, hooks, new FakeTimeProvider(s_now), "t", null, () => hooks, new DiscardCounters(), null, "t"));
            // A teardown of the conflicting copies stops after the first
            var removed = 0;
            hooks.Before = (verb, path) => verb == "Rm" && conflicting.Any(x => x.Name == path.Name) && ++removed == 2 ? throw new IOException("Remove failed") : Task.CompletedTask;
            var first = await Run(Cleanup());
            hooks.Before = null;

            // The resumable advance keeps its records
            var rerun = await Run(Cleanup());
            var controls = (await DeltaTransactionReader.ListLog(hooks, "t")).Controls.Select(x => x.Name).ToArray();
            Assert.True(records.All(x => controls.Contains(x.Name)), $"After {first.Kind} {first.Reason} and {rerun.Kind} {rerun.Reason} the resumable advance kept only [{string.Join(", ", controls)}]");
            // Resumed and suspended at the fixture's C
            AssertDeclined(rerun, MaintenanceReason.HistoryChanged);
        }

        [Fact]
        public async Task RestoredLeaseOfAnAdvanceWhoseRecordsAreGoneIsReleased()
        {
            var table = await Checkpointed();
            var fixture = Advance(8);
            var (part, header) = Manifest(fixture);
            Plant(table, part, header, CleanupRecordKit.Started(fixture, header));
            var ledger = new CapacityLedger(() => default);

            // A resumed advance suspended at the fixture's C holds what it has left as suspended demand
            AssertDeclined(await Run(Cleanup(table), Context(14, new FakeTimeProvider(s_now), ledger)), MaintenanceReason.HistoryChanged);
            Assert.NotEqual(default, ledger.Total(CapacityState.Suspended));

            // Its records go without this process's recovery releasing them, and another advance's appear
            foreach (var name in Controls(table))
            {
                File.Delete(table.Log(name));
            }
            var other = Advance(12);
            var (otherPart, otherHeader) = Manifest(other);
            Plant(table, otherPart, otherHeader, CleanupRecordKit.Started(other, otherHeader));
            AssertDeclined(await Run(Cleanup(table), Context(14, new FakeTimeProvider(s_now), ledger)), MaintenanceReason.HistoryChanged);
            var alone = new CapacityLedger(() => default);
            AssertDeclined(await Run(Cleanup(table), Context(14, new FakeTimeProvider(s_now), alone)), MaintenanceReason.HistoryChanged);
            Assert.True(ledger.Total(CapacityState.Suspended) == alone.Total(CapacityState.Suspended), $"The ledger holds {ledger.Total(CapacityState.Suspended)}, the listed advance alone {alone.Total(CapacityState.Suspended)}");

            // No record is left at all
            foreach (var name in Controls(table))
            {
                File.Delete(table.Log(name));
            }
            AssertCompleted(await Run(Cleanup(table), Context(14, new FakeTimeProvider(s_now), ledger)));
            Assert.True(ledger.Current == default && ledger.Total(CapacityState.Suspended) == default, $"The ledger still holds {ledger.Current} for advances no listed record names");

            // Another worker's runnable lease under the table's key survives this worker's recovery
            using var otherWorker = new CancellationTokenSource();
            ledger.Restore(otherWorker.Token, TableLeases.KeyOf(table.Local, "t"), ControlTokens.New(), Hours(1), CapacityState.Runnable);
            AssertCompleted(await Run(Cleanup(table), Context(14, new FakeTimeProvider(s_now), ledger)));
            Assert.True(ledger.Current == Hours(1), $"Another worker's lease became {ledger.Current}");
        }

        [Fact]
        public async Task ReplacementRecoveryReleasesAStoppedWorkersLeasesOfAdvancesNoRecordNames()
        {
            var table = await Checkpointed(30);
            var ledger = new CapacityLedger(() => default);
            var paused = new List<CapacityDemand>();
            PlantedRecord? kept = null;
            for (var episode = 0; episode < 3; episode++)
            {
                // A worker restores a resumable advance and stops
                using var stopped = new CancellationTokenSource();
                var fixture = Advance(8 + 8 * episode, head: 30);
                var (part, header) = Manifest(fixture);
                var records = new[] { part, header, CleanupRecordKit.Started(fixture, header) };
                Plant(table, records);
                AssertDeclined(await Run(Cleanup(table), Context(30, new FakeTimeProvider(s_now), ledger, stopped.Token)), MaintenanceReason.HistoryChanged);
                Assert.NotEqual(default, ledger.Total(CapacityState.Suspended));
                stopped.Cancel();
                ledger.Pause(stopped.Token);

                // Another process completes it and a successor, whose completion is kept
                foreach (var record in kept is { } previous ? records.Append(previous) : records)
                {
                    File.Delete(table.Log(record.Name));
                }
                var successor = Advance(fixture.C + 4, o: fixture.C, firstJson: fixture.C, head: 30);
                kept = CleanupRecordKit.Done(successor, Manifest(successor).Header);
                Plant(table, kept.Value);

                // The replacement worker's recovery of the same table
                using var replacement = new CancellationTokenSource();
                AssertCompleted(await Run(Cleanup(table), Context(30, new FakeTimeProvider(s_now), ledger, replacement.Token)));
                Assert.Equal(default, ledger.Current);
                paused.Add(ledger.Total(CapacityState.Paused) + ledger.Total(CapacityState.Suspended));
            }

            Assert.True(paused.All(x => x == default), $"Paused leases outlived the recovery of their table: {string.Join(", ", paused)}");
        }

        [Fact]
        public async Task PartBytesBeyondWhatThePartsHoldArePricedAsATornStart()
        {
            var table = await Checkpointed();
            var fixture = Advance(8);
            var (part, header) = Manifest(fixture);
            var association = CleanupRecordKit.AssociationOf(header);
            PlantedRecord Started(long partBytes) => new PlantedRecord(ControlName.Group(ControlKind.Started, fixture.C, fixture.AdvanceId, association.HeaderSha256, ControlTokens.New()),
                ControlRecords.Encode(new StartedRecord(association, fixture.TableId, fixture.O, fixture.A, fixture.Head, fixture.FirstJson, fixture.Anchor, new RecordTotals(16, 16, partBytes))));
            (CapacityDemand, CapacityDemand) Held(CapacityLedger ledger) => (ledger.Total(CapacityState.Runnable), ledger.Total(CapacityState.Suspended));
            // Sixteen parts at their cap, torn
            var torn = Torn(Started(16L * ControlRecords.PartCap));
            Plant(table, part, header, torn, CleanupRecordKit.LogsComplete(fixture, header));
            var ledger = new CapacityLedger(() => default);
            Assert.Equal(OutcomeKind.Declined, (await Run(Cleanup(table), Context(14, new FakeTimeProvider(s_now), ledger))).Kind);
            var expected = Held(ledger);

            // Part bytes whose product with the parts passes a long
            File.Delete(table.Log(torn.Name));
            Plant(table, Started(1L << 60));
            var extreme = new CapacityLedger(() => default);
            Assert.Equal(OutcomeKind.Declined, (await Run(Cleanup(table), Context(14, new FakeTimeProvider(s_now), extreme))).Kind);

            Assert.Equal(expected, Held(extreme));
        }

        // A connector-owned local disk table whose read of one named record waits at its end until released
        private sealed class HeldEndStorage : ForwardingStorage, IConnectorOwnedStorage, ILocalDiskFileStorage, IFileStorage
        {
            public HeldEndStorage(IFileStorage inner) : base(inner)
            {
            }

            public string? Held { get; set; }

            public TaskCompletionSource AtEnd { get; } = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

            public TaskCompletionSource Release { get; } = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

            public string ToNativeLocalPath(IOPath path) => ((ILocalDiskFileStorage)Inner).ToNativeLocalPath(path);

            async Task<Stream?> IFileStorage.OpenRead(IOPath path, CancellationToken cancellationToken)
            {
                var stream = await Inner.OpenRead(path, cancellationToken);
                return stream != null && path.Name == Held ? new HeldEndStream(stream, AtEnd, Release) : stream;
            }
        }

        // A remote end of stream that finishes after the caller cancelled
        private sealed class HeldEndStream : Stream
        {
            private readonly Stream _inner;
            private readonly TaskCompletionSource _atEnd;
            private readonly TaskCompletionSource _release;

            public HeldEndStream(Stream inner, TaskCompletionSource atEnd, TaskCompletionSource release)
            {
                _inner = inner;
                _atEnd = atEnd;
                _release = release;
            }

            public override bool CanRead => _inner.CanRead;

            public override bool CanSeek => _inner.CanSeek;

            public override bool CanWrite => false;

            public override long Length => _inner.Length;

            public override long Position { get => _inner.Position; set => _inner.Position = value; }

            public override void Flush() => _inner.Flush();

            public override long Seek(long offset, SeekOrigin origin) => _inner.Seek(offset, origin);

            public override void SetLength(long value) => throw new NotSupportedException();

            public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();

            public override int Read(byte[] buffer, int offset, int count) => _inner.Read(buffer, offset, count);

            public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
            {
                var read = await _inner.ReadAsync(buffer, cancellationToken);
                if (read == 0)
                {
                    _atEnd.TrySetResult();
                    await _release.Task;
                }
                return read;
            }

            protected override void Dispose(bool disposing)
            {
                if (disposing)
                {
                    _inner.Dispose();
                }
                base.Dispose(disposing);
            }
        }

        [Fact]
        public async Task DetachedRecoveryCannotRestoreStoppedWorkerDemandAsRunnable()
        {
            var root = Path.Combine(_directory, ScratchOwners.ShortId());
            Directory.CreateDirectory(root);
            var local = Files.Of.LocalDisk(root);
            _disposables.Add(local);
            var storage = new HeldEndStorage(local);
            var configuration = CleanupConfiguration.Validate(storage, "t", false, new CleanupOptIns(s_retention, null));
            Assert.True(configuration.Store.Eligible);
            await BoundarySelectorTests.Create(storage, 14, BoundarySelectorTests.Old);
            Assert.Equal(BuildStatus.Completed, (await TryBuild(storage, 4)).Status);
            var clock = new FakeTimeProvider(s_now);
            var budget = new DeltaMaintenanceBudget(new MaintenanceBudgetSettings(2, Path.Combine(_directory, ScratchOwners.ShortId())), clock, manualTick: true)
            {
                Buckets = MaintenanceBucketsTests.Unthrottled()
            };
            _owners.Add(budget.Scratch);
            var cleanup = Recovering(new DeltaTableCleanup(configuration, MeteredFileStorage.Wrap(storage, budget.Buckets, configuration.Store.Store.Paging), clock, "t", null, () => storage,
                new DiscardCounters(), null, "t"));
            var worker = new DeltaMaintenanceWorker(budget, new MaintenanceWorkerSettings(0, null, clock)
            {
                PeriodicEnabled = false,
                CleanupEligible = true,
                Retention = s_retention,
                Paging = configuration.Store.Store.Paging
            }, storage, "t", "t", null) { Cleanup = cleanup };
            budget.Register(worker);
            try
            {
                worker.Published(14);
                budget.TickNow();
                await worker.CurrentJob.WaitAsync(s_wait);
                budget.TickNow();
                await worker.CurrentJob.WaitAsync(s_wait);
                Assert.True(worker.CoverageConfirmed);
                // A resumable advance whose header read the store holds at its end
                var fixture = Advance(8);
                var (part, header) = Manifest(fixture);
                foreach (var record in new[] { part, header, CleanupRecordKit.Started(fixture, header) })
                {
                    await CleanupRecordKit.PlantAsync(storage, "t", record);
                }
                storage.Held = header.Name;

                budget.TickNow();
                await storage.AtEnd.Task.WaitAsync(s_wait);
                await worker.StopAsync(TimeSpan.Zero);
                Assert.Equal(default, budget.Capacity.Total(CapacityState.Runnable));

                // The read ends after the stop paused the table
                storage.Release.TrySetResult();
                await worker.CurrentJob.WaitAsync(s_wait);
                Assert.True(worker.Idle);
                Assert.Equal(default, budget.Capacity.Total(CapacityState.Runnable));
                // Paused by the stop, then suspended by the resume, whose capability source is below its C
                Assert.NotEqual(default, budget.Capacity.Total(CapacityState.Paused) + budget.Capacity.Total(CapacityState.Suspended));
            }
            finally
            {
                storage.Release.TrySetResult();
                await worker.StopAsync(s_wait);
                await worker.CurrentJob.WaitAsync(s_wait);
            }
        }
    }

    [Collection(nameof(MaintenanceSwitchCollection))]
    public class AdvanceRecoverySwitchTests : IDisposable
    {
        private readonly AdvanceRecoveryTests _tests = new AdvanceRecoveryTests();

        public AdvanceRecoverySwitchTests()
        {
            Switch(false);
        }

        public void Dispose()
        {
            Switch(false);
            _tests.Dispose();
        }

        private static void Switch(bool on) => AppContext.SetSwitch(MaintenanceSwitches.DisableMaintenanceDeletes, on);

        [Fact]
        public async Task DeletesSwitchedOffKeepReleasedRecords()
        {
            await _tests.ReleasedRecordsFollowTheDeletesSwitch(Switch);
        }
    }
}
