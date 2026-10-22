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

using DuckDB.NET.Data;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Actions;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Checkpoints;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.Advance;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Time.Testing;
using Stowage;
using System.Globalization;
using System.Text;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    // An eligible table at /t that advances execute on, read and written through its raw storage, executed through its meter
    internal sealed class ExecutionTable
    {
        public required IFileStorage Raw { get; init; }

        public required IFileStorage Metered { get; init; }

        public required CleanupConfiguration Configuration { get; init; }

        // The in-memory variant's hooks
        public HookFileStorage? Hooks { get; init; }

        // The local disk variant's root
        public string? Root { get; init; }

        // The newest commit
        public long Head { get; set; }

        public string LogPath(string name) => $"/t/{DeltaTransactionReader.DeltaLogDirName}{name}";

        // Control record names in ordinal order
        public async Task<string[]> ControlsAsync() => (await DeltaTransactionReader.ListLog(Raw, "t")).Controls.Select(x => x.Name).Order(StringComparer.Ordinal).ToArray();

        // Every control record's bytes by name
        public async Task<Dictionary<string, byte[]>> SnapshotAsync()
        {
            var snapshot = new Dictionary<string, byte[]>();
            foreach (var name in await ControlsAsync())
            {
                snapshot[name] = await DeltaTestKit.ReadBytes(Raw, LogPath(name));
            }
            return snapshot;
        }
    }

    // An owned wrapper below the meter whose listings a test rewrites
    internal sealed class RelistedStorage : ForwardingStorage, IFileStorage, IConnectorOwnedStorage
    {
        public RelistedStorage(IFileStorage inner) : base(inner)
        {
        }

        public Func<IReadOnlyCollection<IOEntry>, IReadOnlyCollection<IOEntry>>? Relist { get; set; }

        public new async Task<IReadOnlyCollection<IOEntry>> Ls(IOPath? path = null, bool recurse = false, CancellationToken cancellationToken = default)
        {
            var entries = await Inner.Ls(path, recurse, cancellationToken);
            return Relist is { } relist ? relist(entries) : entries;
        }
    }

    // What the kit published for one advance
    internal sealed class PublishedAdvance
    {
        public required AdvanceBoundary Boundary { get; init; }

        public required Association Association { get; init; }

        public required StartedRecord Started { get; init; }

        public long C => Association.C;
    }

    // Tables, contexts, publication, adoption and turns for executing started advances
    internal sealed class AdvanceExecutionKit : IDisposable
    {
        public static readonly DateTimeOffset Now = new DateTimeOffset(2026, 10, 15, 12, 0, 0, TimeSpan.Zero);
        public static readonly TimeSpan Retention = TimeSpan.FromDays(7);
        // Ends the advance job that published, before anything after its planning
        private const string Sentinel = "The kit's publication ended the advance";

        private readonly string _directory = Path.Combine(Path.GetTempPath(), "ae-" + ScratchOwners.ShortId());
        private readonly ScratchOwners _owners = new ScratchOwners();
        private readonly List<IDisposable> _disposables = new List<IDisposable>();

        public AdvanceExecutionKit()
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

        // A new empty directory the kit removes
        public string NewRoot()
        {
            var root = Path.Combine(_directory, ScratchOwners.ShortId());
            Directory.CreateDirectory(root);
            return root;
        }

        // The eligible table at /t on local disk at the root, or on a fresh owned in-memory store without one
        public ExecutionTable Open(string? root, long head)
        {
            IFileStorage raw;
            HookFileStorage? hooks = null;
            if (root != null)
            {
                raw = Files.Of.LocalDisk(root);
                _disposables.Add(raw);
            }
            else
            {
                hooks = OwnedHookFileStorage.Over(Files.Of.InternalMemory($"./execution-{Guid.NewGuid():N}"));
                raw = hooks;
            }
            var configuration = CleanupConfiguration.Validate(raw, "t", root == null, new CleanupOptIns(Retention, null));
            Assert.True(configuration.Store.Eligible);
            var metered = MeteredFileStorage.Wrap(raw, MaintenanceBucketsTests.Unthrottled(), configuration.Store.Store.Paging);
            return new ExecutionTable() { Raw = raw, Metered = metered, Configuration = configuration, Hooks = hooks, Root = root, Head = head };
        }

        // Own commits 0 to 14 before the cutoff, each removing the file the one before added, checkpointed at 4, 8 and 12; on local disk or an owned in-memory store
        public async Task<ExecutionTable> ChurnedAsync(bool local = true)
        {
            var table = Open(local ? NewRoot() : null, 14);
            var raw = table.Raw;
            var published = new Dictionary<string, string>()
            {
                [RetentionProperties.LogKey] = RetentionProperties.Format(Retention),
                [RetentionProperties.DeletedFileKey] = RetentionProperties.Format(Retention)
            };
            for (long version = 0; version <= 14; version++)
            {
                var add = DeltaLakeCheckpointBuildTests.Add($"part-{version:D4}.parquet");
                var actions = version == 0
                    ? new[] { DeltaTestKit.Protocol(), DeltaTestKit.Metadata(DeltaTestKit.UserSchema, published), add }
                    : new[] { add, DeltaLakeCheckpointBuildTests.Remove($"part-{version - 1:D4}.parquet", BoundarySelectorTests.Old(version)) };
                await DeltaTestKit.WriteOwnCommit(raw, "t", version, 0, BoundarySelectorTests.Old(version), actions);
            }
            foreach (var version in new long[] { 4, 8, 12 })
            {
                await BuildAsync(raw, version);
            }
            return table;
        }

        // Own commits after the head, each adding one file, in the anchor's epoch
        public static async Task CommitAsync(ExecutionTable table, int commits)
        {
            for (int i = 0; i < commits; i++)
            {
                var version = table.Head + 1;
                await DeltaTestKit.WriteOwnCommit(table.Raw, "t", version, 0, BoundarySelectorTests.Old(version), DeltaLakeCheckpointBuildTests.Add($"part-{version:D4}.parquet"));
                table.Head = version;
            }
        }

        public async Task BuildAsync(IFileStorage storage, long version)
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
                    Clock = new FakeTimeProvider(Now)
                }, new BuildRequest() { Version = version, Root = RootPolicy.Auto, KnownUnsupported = new HashSet<ObjectIdentity>() });
            }
            _owners.Release(owner);
            Assert.True(result.Status == BuildStatus.Completed, $"{result.Status} {result.Reason}");
        }

        // An inline run's cleanup over the table's meter
        public static DeltaTableCleanup Cleanup(ExecutionTable table, ILogger? logger = null, TimeProvider? clock = null)
        {
            return new DeltaTableCleanup(table.Configuration, table.Metered, clock ?? new FakeTimeProvider(Now), "t", null, () => table.Raw, new DiscardCounters(), logger, "t");
        }

        // A long advance admission
        public CleanupContext AdvanceContext(long head, CapacityLedger? capacity = null, CancellationToken owner = default, JobMemory? memory = null, CancellationToken stop = default)
        {
            var clock = new FakeTimeProvider(Now);
            return new CleanupContext(new JobRun(head, JobKind.Advance) { Cancellation = new CancellationTokenSource() }, clock)
            {
                Kind = JobKind.Advance,
                Long = true,
                ReadLimit = long.MaxValue,
                Deadline = clock.GetUtcNow() + TimeSpan.FromHours(1),
                Head = head,
                TableId = null,
                Scratch = Scratch(),
                Memory = memory ?? new JobMemory(),
                Transfer = new TransferAllowance(),
                Stop = stop,
                Capacity = capacity,
                CapacityOwner = owner
            };
        }

        // A short delete admission, as a worker's delete turn takes it
        public CleanupContext DeletionContext(long head, CapacityLedger? capacity = null, CancellationToken owner = default, JobRun? run = null, TimeProvider? clock = null, CancellationToken stop = default,
            JobMemory? memory = null)
        {
            clock ??= new FakeTimeProvider(Now);
            return new CleanupContext(run ?? new JobRun(head, JobKind.Deletion) { Cancellation = new CancellationTokenSource() }, clock)
            {
                Kind = JobKind.Deletion,
                Long = false,
                ReadLimit = MaintenanceWorkerSettings.DefaultShortReads,
                Deadline = clock.GetUtcNow() + TimeSpan.FromMinutes(15),
                Head = head,
                TableId = null,
                Scratch = Scratch(),
                Memory = memory ?? new JobMemory(),
                Transfer = new TransferAllowance(),
                Stop = stop,
                Capacity = capacity,
                CapacityOwner = owner
            };
        }

        private JobScratch Scratch()
        {
            var scratch = _owners.Add(new ScratchDirectory(Path.Combine(_directory, ScratchOwners.ShortId()))).CreateJob();
            _disposables.Add(scratch);
            return scratch;
        }

        // Plans in parts of the given paths, publishes the parts, header, started record and, if asked, logs-complete, then ends the job before anything after its planning
        public async Task<PublishedAdvance> PublishAsync(DeltaTableCleanup cleanup, ExecutionTable table, int partPaths = 2, bool logsComplete = false)
        {
            PublishedAdvance? published = null;
            var job = cleanup.AdvanceForTests;
            var planner = job.PlanForTests;
            job.PlanForTests = async (configuration, storage, path, boundary, classes, context) =>
            {
                using (var plan = await AdvancePlanner.PlanAsync(configuration, storage, path, boundary, classes, context, new AdvanceWindowLimits() { PartPaths = partPaths }))
                {
                    Assert.True(plan.Status == AdvancePlanStatus.Planned && plan.Emitted > 0, $"{plan.Status} {plan.Reason}: {plan.Detail}, {plan.Emitted} paths");
                    var publisher = new ControlPublisher(storage, path, context);
                    var header = await publisher.PublishManifestAsync(boundary, plan);
                    var association = new Association(boundary.C, boundary.AdvanceId, header.Name, header.Sha256);
                    var started = new StartedRecord(association, boundary.TableId, boundary.O, boundary.A, boundary.Head, boundary.FirstJson, AnchorSummary.Of(boundary.Anchor!),
                        new RecordTotals(plan.Emitted, plan.Parts.Count, plan.Parts.Sum(x => x.Bytes)));
                    await publisher.PublishAsync(ControlKind.Started, association, -1, () => ControlRecords.Encode(started));
                    if (logsComplete)
                    {
                        await publisher.PublishAsync(ControlKind.LogsComplete, association, -1, () => ControlRecords.Encode(new LogsCompleteRecord(association, started.TableId, started.O)));
                    }
                    published = new PublishedAdvance() { Boundary = boundary, Association = association, Started = started };
                }
                throw new AdvanceEndedException(new CleanupResult(OutcomeKind.NoWork, MaintenanceReason.None, Sentinel));
            };
            try
            {
                var context = AdvanceContext(table.Head);
                var result = await cleanup.RunAsync(JobKind.Advance, context);
                Assert.True(result.Kind == OutcomeKind.NoWork && result.Detail == Sentinel, $"{result.Kind} {result.Reason}: {result.Detail}");
                AssertReleased(context);
            }
            finally
            {
                job.PlanForTests = planner;
            }
            return published!;
        }

        // Recovery's one resumable group, summarized with evidence anchored at its planned head and handed to the cleanup's delete turns; no policy check runs
        public async Task<ActiveAdvance> AdoptAsync(DeltaTableCleanup cleanup, ExecutionTable table, CapacityLedger? capacity = null, CancellationToken owner = default)
        {
            var configuration = table.Configuration;
            var context = AdvanceContext(table.Head, capacity, owner);
            var log = await AdvancePreparation.ListAsync(configuration, table.Metered, context.Head, context);
            var established = await CleanupEvidence.EstablishAsync(configuration, table.Metered, log, log.Head, null, durable: true, context);
            Assert.True(established.Passed, $"{established.Verdict}: {established.Detail}");
            var recovered = await AdvanceRecovery.RecoverAsync(configuration, table.Metered, log, established.Evidence!, context);
            var group = Assert.Single(recovered.Unresolved);
            Assert.Equal(GroupState.Resumable, group.State);
            var started = group.Started!;
            var anchored = await CleanupEvidence.EstablishAsync(configuration, table.Metered, recovered.Log, started.Anchor.Version, started.Anchor.StageId, durable: true, context);
            Assert.True(anchored.Passed, $"{anchored.Verdict}: {anchored.Detail}");
            var active = ActiveAdvance.From(group, anchored.Evidence!);
            active.LogEntries = recovered.Log.Entries.Count;
            active.LogTargetsLeft = active.Phase == AdvancePhase.Log ? LogTargets.Count(LogClassifier.Classify(recovered.Log), active.C) : 0;
            if (capacity != null)
            {
                var demand = CapacityEstimator.Advance(AdvanceDemand.Of(configuration, active, cleanup.Latency), MaintenanceBuckets.DefaultRequestsPerSecond);
                active.Lease = capacity.Restore(owner, TableLeases.KeyOf(RawStorage.Unwrap(table.Raw), configuration.Store.Table), active.Association.AdvanceId, demand, CapacityState.Runnable);
            }
            cleanup.Adopt(active);
            await context.SettleAsync();
            AssertReleased(context);
            return active;
        }

        // One delete turn as a worker runs it: the hint drain, the slice, then settlement
        public static async Task<CleanupResult> TurnAsync(DeltaTableCleanup cleanup, CleanupContext context)
        {
            CleanupResult result;
            try
            {
                await cleanup.DrainHintsAsync(context, DeltaMaintenanceWorker.MaxHintsPerTurn);
                result = await cleanup.RunAsync(JobKind.Deletion, context);
            }
            finally
            {
                await context.SettleAsync();
            }
            AssertReleased(context);
            return result;
        }

        // One advance job under the context, which releases everything it held
        public static async Task<CleanupResult> JobAsync(DeltaTableCleanup cleanup, CleanupContext context)
        {
            CleanupResult result;
            try
            {
                result = await cleanup.RunAsync(JobKind.Advance, context);
            }
            finally
            {
                await context.SettleAsync();
            }
            AssertReleased(context);
            return result;
        }

        // Delete turns until the handed-off advance is done, each completed; returns how many ran
        public async Task<int> ExecuteAsync(DeltaTableCleanup cleanup, ExecutionTable table, CapacityLedger? ledger = null)
        {
            var active = cleanup.ActiveForTests;
            Assert.NotNull(active);
            for (int turns = 1; turns <= 40; turns++)
            {
                var result = await TurnAsync(cleanup, DeletionContext(table.Head, ledger));
                Assert.True(result.Kind == OutcomeKind.Completed, $"{result.Kind} {result.Reason}: {result.Detail}");
                if (active.Phase == AdvancePhase.Done)
                {
                    Assert.Null(cleanup.ActiveForTests);
                    return turns;
                }
            }
            Assert.Fail("The advance never published its done");
            return 0;
        }

        // A one-byte file for each commit's added path
        public static async Task PayloadFilesAsync(ExecutionTable table)
        {
            for (long version = 0; version <= table.Head; version++)
            {
                await using var stream = await table.Raw.OpenWrite($"/t/part-{version:D4}.parquet");
                await stream.WriteAsync(new byte[1]);
            }
        }

        // The churned table's newest own commit, whose added path is the last with a file
        private const long ChurnedHead = 14;

        // Every log name below C gone if its log phase ran, C and the commits above it kept, and each planned path's file gone unless kept
        public static async Task AssertAdvancedAsync(ExecutionTable table, long c, bool logsDeleted = true, params string[] kept)
        {
            var entries = await table.Raw.Ls($"/t/{DeltaTransactionReader.DeltaLogDirName}");
            var listed = entries.Select(x => x.Name).ToHashSet();
            Assert.Equal(logsDeleted, LogTargets.Count(LogClassifier.Classify(entries), c) == 0);
            for (var version = c; version <= table.Head; version++)
            {
                Assert.Contains($"{version:D20}.json", listed);
            }
            Assert.Contains($"{c:D20}.checkpoint.parquet", listed);
            for (long version = 0; version <= ChurnedHead; version++)
            {
                var path = $"/t/part-{version:D4}.parquet";
                Assert.True(await table.Raw.Exists(path) == (version >= c - 1 || kept.Contains(path)), path);
            }
            // Only the done stays, with no reservation
            var done = Assert.Single(await table.ControlsAsync());
            Assert.True(ControlName.TryParse(done, out var name) && name.Kind == ControlKind.Done && name.Boundary == c, done);
            Assert.Equal(ControlFence.NoReservation, await FloorAsync(table));
        }

        public static async Task<long> FloorAsync(ExecutionTable table) => ControlFence.Of(await DeltaTransactionReader.ListLog(table.Raw, "t")).ReservedFloor;

        public static string DataName(long version) => $"part-{version:D4}.parquet";

        // One row keyed by the version; returns the file's length
        private static long WriteRow(string path, long key)
        {
            using (var file = new ParquetSharp.ParquetFileWriter(path,new ParquetSharp.Column[] { new ParquetSharp.Column<long?>("userkey"), new ParquetSharp.Column<string>("name") }))
            {
                using (var rowGroup = file.AppendRowGroup())
                {
                    using (var writer = rowGroup.NextColumn().LogicalWriter<long?>())
                    {
                        writer.WriteBatch(new long?[] { key });
                    }
                    using (var writer = rowGroup.NextColumn().LogicalWriter<string>())
                    {
                        writer.WriteBatch(new[] { $"user-{key}" });
                    }
                }
                file.Close();
            }
            return new FileInfo(path).Length;
        }

        // Own commits 0 to 14 before the cutoff on local disk, each replacing the data file before it with one row of its own version, checkpointed at 4, 8 and 12
        public async Task<ExecutionTable> ReadableAsync(string root)
        {
            var raw = Files.Of.LocalDisk(root);
            var configuration = CleanupConfiguration.Validate(raw, "t", false, new CleanupOptIns(Retention, null));
            Assert.True(configuration.Store.Eligible);
            var published = new Dictionary<string, string>()
            {
                [RetentionProperties.LogKey] = RetentionProperties.Format(Retention),
                [RetentionProperties.DeletedFileKey] = RetentionProperties.Format(Retention)
            };
            Directory.CreateDirectory(Path.Combine(root, "t"));
            for (long version = 0; version <= 14; version++)
            {
                var add = new DeltaAction()
                {
                    Add = new DeltaAddAction()
                    {
                        Path = DataName(version),
                        PartitionValues = new Dictionary<string, string>(),
                        Size = WriteRow(Path.Combine(root, "t", DataName(version)), version),
                        ModificationTime = 1,
                        DataChange = true
                    }
                };
                var actions = version == 0
                    ? new[] { DeltaTestKit.Protocol(), DeltaTestKit.Metadata(DeltaTestKit.UserSchema, published), add }
                    : new[] { add, DeltaLakeCheckpointBuildTests.Remove(DataName(version - 1), BoundarySelectorTests.Old(version)) };
                await DeltaTestKit.WriteOwnCommit(raw, "t", version, 0, BoundarySelectorTests.Old(version), actions);
            }
            foreach (var version in new long[] { 4, 8, 12 })
            {
                await BuildAsync(raw, version);
            }
            var metered = MeteredFileStorage.Wrap(raw, MaintenanceBucketsTests.Unthrottled(), configuration.Store.Store.Paging);
            return new ExecutionTable() { Raw = raw, Metered = metered, Configuration = configuration, Root = root, Head = 14 };
        }

        // Each version's version and data files through our reader, null where it failed; never version 0, the reader's oldest-version sentinel
        public static async Task<string?[]> ReaderSnapshotsAsync(IFileStorage storage, long head)
        {
            var snapshots = new string?[head + 1];
            for (long version = 1; version <= head; version++)
            {
                try
                {
                    var table = await DeltaTransactionReader.ReadTable(storage, "t", version);
                    snapshots[version] = table == null ? null : $"{table.Version}:{string.Join(",", table.AddFiles.Select(x => x.Path).Order(StringComparer.Ordinal))}";
                }
                catch (Exception)
                {
                    snapshots[version] = null;
                }
            }
            return snapshots;
        }

        // Each version's keys through DuckDB on a fresh connection, null where the query failed
        public static string?[] DuckDbSnapshots(string tablePath, long head)
        {
            using var connection = new DuckDBConnection("DataSource=:memory:;autoinstall_known_extensions=false;autoload_known_extensions=false");
            connection.Open();
            using var command = connection.CreateCommand();
            lock (DeltaTestKit.DuckDbInstallLock)
            {
                command.CommandText = $"SET extension_directory = '{DuckDbTimeTravelProbe.ExtensionDirectory.Replace("\\", "/")}'; LOAD delta;";
                command.ExecuteNonQuery();
            }
            command.CommandText = $"ATTACH '{Path.GetFullPath(tablePath).Replace("\\", "/")}' AS probe (TYPE delta)";
            command.ExecuteNonQuery();
            var snapshots = new string?[head + 1];
            for (long version = 0; version <= head; version++)
            {
                try
                {
                    command.CommandText = $"SELECT userkey FROM probe AT (VERSION => {version}) ORDER BY userkey";
                    using var reader = command.ExecuteReader();
                    var keys = new List<string>();
                    while (reader.Read())
                    {
                        keys.Add(Convert.ToString(reader.GetValue(0), CultureInfo.InvariantCulture)!);
                    }
                    snapshots[version] = string.Join(",", keys);
                }
                catch (DuckDBException)
                {
                    snapshots[version] = null;
                }
            }
            return snapshots;
        }

        private static void AssertReleased(CleanupContext context)
        {
            Assert.Equal(0, context.Memory.Managed);
            Assert.Equal(0, context.Memory.Native);
            Assert.Empty(context.Scratch.Files);
        }

        // A compaction of the commits from start to end, their lines in order
        public static async Task PlantCompactionAsync(ExecutionTable table, long start, long end)
        {
            var lines = new StringBuilder();
            for (var version = start; version <= end; version++)
            {
                lines.Append(Encoding.UTF8.GetString(await DeltaTestKit.ReadBytes(table.Raw, table.LogPath($"{version:D20}.json"))).TrimEnd('\n')).Append('\n');
            }
            await CleanupRecordKit.PlantAsync(table.Raw, "t", $"{start:D20}.{end:D20}.compacted.json", Encoding.UTF8.GetBytes(lines.ToString()));
        }

        public static Task PlantCrcAsync(ExecutionTable table, long version) => CleanupRecordKit.PlantAsync(table.Raw, "t", $"{version:D20}.crc", Encoding.UTF8.GetBytes("{}"));
    }
}
