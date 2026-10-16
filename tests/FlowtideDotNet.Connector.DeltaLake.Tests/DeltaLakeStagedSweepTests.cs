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
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.Deletion;
using Microsoft.Extensions.Time.Testing;
using Stowage;
using Stowage.Impl;
using System.Collections.Concurrent;
using System.Text;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public class DeltaLakeStagedSweepTests : IDisposable
    {
        private static readonly DateTimeOffset s_now = new DateTimeOffset(2026, 10, 15, 12, 0, 0, TimeSpan.Zero);
        private static readonly TimeSpan s_wait = TimeSpan.FromSeconds(30);

        // The id of the stage most tests sweep
        private const string Attempt = "0f8fad5bd9cb469fa16570867728950e";

        private static readonly string[] s_owned = { "own-a.parquet", "own-b.parquet" };

        // Local disk classified as listable but without absence evidence
        private static readonly StoreDescriptor s_unvouchedLocal = new StoreDescriptor
        {
            Kind = StoreKind.LocalDisk,
            Pinned = true,
            Delivery = ListingDelivery.UnpagedSnapshot,
            NeedsAssertion = true,
            ExactDeletes = true,
            DirectoryExclusion = true,
            Detail = "local disk nobody vouched for"
        };

        // Local disk whose targets must stay within the qualified characters, as an object store's do
        private static readonly StoreDescriptor s_qualifiedLocal = new StoreDescriptor
        {
            Kind = StoreKind.LocalDisk,
            Pinned = true,
            Delivery = ListingDelivery.UnpagedSnapshot,
            QualifiedDomain = true,
            ExactDeletes = true,
            DirectoryExclusion = true,
            Detail = "local disk with qualified names"
        };

        private readonly string _directory = Path.Combine(Path.GetTempPath(), "ss-" + ScratchOwners.ShortId());
        private readonly ScratchOwners _owners = new ScratchOwners();
        private readonly List<IDisposable> _disposables = new List<IDisposable>();

        public DeltaLakeStagedSweepTests()
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

        // A local table at t seen through a hook view and the maintenance meter
        private sealed class Table
        {
            public required string Dir { get; init; }

            public required IFileStorage Local { get; init; }

            public required SweepStorage Hooks { get; init; }

            public required MeteredFileStorage Metered { get; init; }

            public required CleanupConfiguration Configuration { get; init; }

            public string Native(string relative) => Path.Combine(Dir, relative.Replace('/', Path.DirectorySeparatorChar));

            public string Log(string name) => Native("_delta_log/" + name);

            public string StorePath(string relative) => "/t/" + relative;

            public void Put(string relative, string content = "x")
            {
                var path = Native(relative);
                Directory.CreateDirectory(Path.GetDirectoryName(path)!);
                File.WriteAllText(path, content);
            }

            public bool Exists(string relative) => File.Exists(Native(relative));
        }

        // A fast meter for tests that issue thousands of requests
        private Table NewTable(bool eligible = true, TimeSpan? retention = null, bool fastMeter = false, StoreDescriptor? descriptor = null)
        {
            var root = Path.Combine(_directory, ScratchOwners.ShortId());
            Directory.CreateDirectory(root);
            var local = Files.Of.LocalDisk(root);
            _disposables.Add(local);
            var contract = descriptor != null ? CleanupStoreContract.Create(local, descriptor, "t", false)
                : eligible ? CleanupStoreContract.Classify(local, "t", false) : CleanupStoreContract.Create(local, s_unvouchedLocal, "t", false);
            var configuration = CleanupConfiguration.Validate(contract, new CleanupOptIns(retention, null));
            Assert.Equal(eligible, configuration.Store.Eligible);
            var hooks = new SweepStorage(local);
            var dir = Path.Combine(root, "t");
            Directory.CreateDirectory(Path.Combine(dir, "_delta_log"));
            return new Table()
            {
                Dir = dir,
                Local = local,
                Hooks = hooks,
                Metered = MeteredFileStorage.Wrap(hooks, fastMeter ? new MaintenanceBuckets(TimeProvider.System, 1_000_000, 1_000_000) : new MaintenanceBuckets(TimeProvider.System), ListingPaging.LocalDisk),
                Configuration = configuration
            };
        }

        private static StagedSweep Sweep(Table table, DeletionRunSettings? settings = null) => new StagedSweep(table.Configuration, table.Metered, TimeProvider.System, settings ?? new DeletionRunSettings());

        private CleanupContext Context(long head, long readLimit = MaintenanceWorkerSettings.DefaultShortReads, JobRun? run = null, TimeProvider? clock = null, CancellationToken stop = default,
            JobMemory? memory = null)
        {
            clock ??= new FakeTimeProvider(s_now);
            var owner = _owners.Add(new ScratchDirectory(Path.Combine(_directory, ScratchOwners.ShortId())));
            var scratch = owner.CreateJob();
            _disposables.Add(scratch);
            return new CleanupContext(run ?? new JobRun(0, JobKind.StagedSweep) { Cancellation = new CancellationTokenSource() }, clock)
            {
                Kind = JobKind.StagedSweep,
                Long = false,
                ReadLimit = readLimit,
                Deadline = clock.GetUtcNow() + TimeSpan.FromMinutes(15),
                Head = head,
                TableId = null,
                Scratch = scratch,
                Memory = memory ?? new JobMemory(),
                Transfer = new TransferAllowance(),
                Stop = stop
            };
        }

        private static DeltaAction Info(string? stageId, long? adoptedAt, long timestamp, IEnumerable<string>? created) => new DeltaAction()
        {
            CommitInfo = new DeltaCommitInfoAction() { StageId = stageId, AdoptedAt = adoptedAt, Timestamp = timestamp, Operation = "WRITE", CreatedFiles = created?.ToList() }
        };

        private static DeltaAction Own(long version, long epoch = 0) => Info($"stage-{version}", epoch, 1000 + version, new List<string>());

        private static DeltaAction Foreign(long version) => new DeltaAction() { CommitInfo = new DeltaCommitInfoAction() { Timestamp = 1000 + version, Operation = "WRITE" } };

        // A data file, or a change file under _change_data
        private static DeltaAction Reference(string path) => path.StartsWith("_change_data/", StringComparison.Ordinal)
            ? new DeltaAction() { Cdc = new DeltaCdcAction() { Path = path, PartitionValues = new Dictionary<string, string>(), Size = 1, DataChange = false } }
            : DeltaLakeCheckpointBuildTests.Add(path);

        private static Task Write(Table table, long version, params DeltaAction[] actions) => DeltaTestKit.WriteCommit(table.Local, "t", version, actions);

        // Own commits 0 to head of epoch 0, each after the first adding one file
        private static async Task Create(Table table, long head)
        {
            var metaData = DeltaTestKit.Metadata(DeltaTestKit.UserSchema);
            metaData.MetaData!.Id = "table-a";
            await Write(table, 0, Own(0), DeltaTestKit.Protocol(), metaData);
            for (long version = 1; version <= head; version++)
            {
                await Write(table, version, Own(version), DeltaLakeCheckpointBuildTests.Add($"part-{version}.parquet"));
            }
        }

        // The writer's stage at a version, its first line carrying the given ownership, and the owned files on disk
        private static async Task<string> Stage(Table table, long version, string? stageId, IEnumerable<string>? created, params string[] owned)
        {
            foreach (var name in owned)
            {
                table.Put(name);
            }
            var actions = new List<DeltaAction>() { Info(stageId, stageId == null ? null : 0, 2000 + version, created) };
            actions.AddRange(owned.Select(Reference));
            var staged = await DeltaTransactionWriter.StageCommit(table.Local, "t", version, actions);
            return staged.FileName;
        }

        private static string StageName(long version, string id) => $".{version:D20}.json.{id}.tmp";

        private static string CommitName(long version) => $"{version:D20}.json";

        private static Task<string> OwnStage(Table table, long version) => Stage(table, version, Attempt, s_owned, s_owned);

        // Another attempt's commit at the version
        private static Task WriteOther(Table table, long version) => Write(table, version, Info("other-attempt", 0, 3000 + version, new[] { $"other-{version}.parquet" }), DeltaLakeCheckpointBuildTests.Add($"other-{version}.parquet"));

        private static void WriteRaw(Table table, string relative, byte[] bytes)
        {
            File.WriteAllBytes(table.Native(relative), bytes);
        }

        private static byte[] Utf8(string text) => Encoding.UTF8.GetBytes(text);

        // A hand-written stage: its first line as given, then one add per owned file
        private static void RawStage(Table table, long version, string id, string firstLine, params string[] owned)
        {
            foreach (var name in owned)
            {
                table.Put(name);
            }
            var lines = new StringBuilder(firstLine).Append('\n');
            foreach (var name in owned)
            {
                lines.Append($"{{\"add\":{{\"path\":\"{name}\",\"partitionValues\":{{}},\"size\":1,\"modificationTime\":1,\"dataChange\":true}}}}\n");
            }
            WriteRaw(table, "_delta_log/" + StageName(version, id), Utf8(lines.ToString()));
        }

        // Expected results per scenario on an eligible store: verdict (null when not considered), stage removed, owned output removed, dead stages leaked
        private static readonly Dictionary<string, (string? Verdict, bool StageRemoved, bool PayloadRemoved, int Leaked)> s_matrix = new Dictionary<string, (string?, bool, bool, int)>()
        {
            ["Published"] = ("Published", true, false, 0),
            ["DeadWithSuccessor"] = ("Dead", true, true, 0),
            ["DeadWithoutSuccessor"] = ("Keep", false, false, 0),
            ["SameLengthOwnPrefix"] = ("Keep", false, false, 0),
            ["SameLengthOtherPrefix"] = ("Dead", true, false, 0),
            ["RepairablePrefix"] = ("Keep", false, false, 0),
            ["LongerWithOwnPrefix"] = ("Keep", false, false, 0),
            ["EmptyTarget"] = ("Keep", false, false, 0),
            ["ShortForeignTarget"] = ("Keep", false, false, 0),
            ["BelowOldest"] = ("BelowOldest", true, false, 0),
            ["AtOldest"] = ("Keep", false, false, 0),
            ["LegacyPublished"] = ("Published", true, false, 0),
            ["LegacyDead"] = ("Keep", false, false, 0),
            ["InvalidOwnership"] = ("Dead", true, false, 1),
            ["NoOwnership"] = ("Dead", true, false, 1),
            ["ReorderedHeader"] = ("Keep", false, false, 0),
            ["EpochAboveVersion"] = ("Dead", true, false, 1),
            ["AboveV"] = (null, false, false, 0)
        };

        [Theory]
        [InlineData("Published", true)]
        [InlineData("Published", false)]
        [InlineData("DeadWithSuccessor", true)]
        [InlineData("DeadWithSuccessor", false)]
        [InlineData("DeadWithoutSuccessor", true)]
        [InlineData("DeadWithoutSuccessor", false)]
        [InlineData("SameLengthOwnPrefix", true)]
        [InlineData("SameLengthOwnPrefix", false)]
        [InlineData("SameLengthOtherPrefix", true)]
        [InlineData("SameLengthOtherPrefix", false)]
        [InlineData("RepairablePrefix", true)]
        [InlineData("RepairablePrefix", false)]
        [InlineData("LongerWithOwnPrefix", true)]
        [InlineData("LongerWithOwnPrefix", false)]
        [InlineData("EmptyTarget", true)]
        [InlineData("EmptyTarget", false)]
        [InlineData("ShortForeignTarget", true)]
        [InlineData("ShortForeignTarget", false)]
        [InlineData("BelowOldest", true)]
        [InlineData("BelowOldest", false)]
        [InlineData("AtOldest", true)]
        [InlineData("AtOldest", false)]
        [InlineData("LegacyPublished", true)]
        [InlineData("LegacyPublished", false)]
        [InlineData("LegacyDead", true)]
        [InlineData("LegacyDead", false)]
        [InlineData("InvalidOwnership", true)]
        [InlineData("InvalidOwnership", false)]
        [InlineData("NoOwnership", true)]
        [InlineData("NoOwnership", false)]
        [InlineData("ReorderedHeader", true)]
        [InlineData("ReorderedHeader", false)]
        [InlineData("EpochAboveVersion", true)]
        [InlineData("EpochAboveVersion", false)]
        [InlineData("AboveV", true)]
        [InlineData("AboveV", false)]
        public async Task PublicationConflictPrefixRevivalMatrix(string scenario, bool eligible)
        {
            var table = NewTable(eligible);
            var (published, stageName) = await Scenario(table, scenario);
            var target = table.Log(CommitName(3));
            var targetBefore = File.Exists(target) ? File.ReadAllBytes(target) : null;
            var sweep = Sweep(table);

            var result = await sweep.RunAsync(Context(published));

            var (verdict, stageRemoved, payloadRemoved, leaked) = s_matrix[scenario];
            // Without absence evidence a stage whose commit is not listed is never seen, and no output is deleted or planned
            if (!eligible && scenario is "BelowOldest" or "AtOldest")
            {
                (verdict, stageRemoved) = (null, false);
            }
            payloadRemoved &= eligible;
            leaked = eligible ? leaked : 0;
            var report = sweep.LastReport!;
            Assert.Equal(leaked, report.LeakedStages);
            if (leaked > 0)
            {
                Assert.Equal(MaintenanceReason.CreatedFilesUnreadable, report.LeakReason);
                Assert.Equal(MaintenanceReason.CreatedFilesUnreadable, result.Reason);
            }
            if (verdict == null)
            {
                Assert.False(report.Verdicts.ContainsKey(stageName));
            }
            else
            {
                Assert.Equal(Enum.Parse<StageVerdict>(verdict), report.Verdicts[stageName]);
            }
            Assert.Equal(stageRemoved, !File.Exists(table.Log(stageName)));
            foreach (var name in s_owned)
            {
                Assert.Equal(payloadRemoved, !table.Exists(name));
            }
            Assert.Equal(payloadRemoved ? 2 : 0, report.PayloadDeleted);
            Assert.NotEqual(OutcomeKind.Failed, result.Kind);
            // The commit at the stage's version is never touched
            if (targetBefore != null)
            {
                Assert.Equal(targetBefore, File.ReadAllBytes(target));
            }
            Assert.True(File.Exists(table.Log(CommitName(published))));
        }

        [Fact]
        public async Task SweptLegacyStageNeverMakesItsWriterAcknowledge()
        {
            var table = NewTable();
            var (published, stageName) = await Scenario(table, "LegacyDead");
            var pending = new Internal.DeltaLakePendingCommit() { Version = 3, StagedFile = stageName, StageId = null };
            // A writer still holding this legacy pending commit fails closed: version 3 is another attempt's
            Assert.Equal(PublishOutcome.FailedClosed, (await DeltaTransactionWriter.PublishCommit(table.Local, "t", pending, null, checkSuccessor: true)).Outcome);

            await Sweep(table).RunAsync(Context(published));

            // Removing the stage must not turn that refusal into an acknowledgement that drops its rows
            Assert.Equal(PublishOutcome.FailedClosed, (await DeltaTransactionWriter.PublishCommit(table.Local, "t", pending, null, checkSuccessor: true)).Outcome);
        }

        [Fact]
        public async Task UnqualifiedPlannedLogFilesAreKeptAndCounted()
        {
            // Local disk whose targets must stay in the qualified characters
            var table = NewTable(descriptor: s_qualifiedLocal);
            await Create(table, 2);
            var unqualified = await Stage(table, 3, "attempt+3", Array.Empty<string>());
            WriteRaw(table, "_delta_log/" + CommitName(3), File.ReadAllBytes(table.Log(unqualified)));
            var qualified = await Stage(table, 4, "attempt-4", Array.Empty<string>());
            WriteRaw(table, "_delta_log/" + CommitName(4), File.ReadAllBytes(table.Log(qualified)));
            var sweep = Sweep(table);

            var result = await sweep.RunAsync(Context(4));

            Assert.NotEqual(OutcomeKind.Failed, result.Kind);
            Assert.Equal(StageVerdict.Published, sweep.LastReport!.Verdicts[unqualified]);
            Assert.Equal(StageVerdict.Published, sweep.LastReport.Verdicts[qualified]);
            Assert.True(File.Exists(table.Log(unqualified)));
            Assert.False(File.Exists(table.Log(qualified)));
            Assert.Equal(1, sweep.LastReport.LogFilesDeleted);
            Assert.Equal(1, sweep.LastReport.LogFilesKept);

            // A stage the run keeps at issuance is counted too: the sink's storage is replaced after the first delete
            var replaced = NewTable();
            await Create(replaced, 2);
            var stages = new List<string>();
            for (long version = 3; version <= 4; version++)
            {
                var stage = await Stage(replaced, version, $"attempt-{version}", Array.Empty<string>());
                WriteRaw(replaced, "_delta_log/" + CommitName(version), File.ReadAllBytes(replaced.Log(stage)));
                stages.Add(stage);
            }
            IFileStorage current = replaced.Local;
            var live = CleanupConfiguration.Validate(() => current, "t", false, CleanupOptIns.Off);
            var replacing = new StagedSweep(live, replaced.Metered, TimeProvider.System, new DeletionRunSettings() { MaxOutstanding = 1 });
            using var other = Files.Of.LocalDisk(Path.GetDirectoryName(replaced.Dir)!);
            replacing.BeforeDeleteForTests = _ =>
            {
                current = other;
                return Task.CompletedTask;
            };

            await replacing.RunAsync(Context(4));

            Assert.Equal(1, replacing.LastReport!.LogFilesDeleted);
            Assert.Equal(1, replacing.LastReport.LogFilesKept);
            Assert.Single(stages, x => File.Exists(replaced.Log(x)));
        }

        // Builds one matrix scenario around a stage at version 3; returns V and the stage's name
        private static async Task<(long Published, string StageName)> Scenario(Table table, string scenario)
        {
            var prefixLength = DeltaTransactionWriter.StagePrefix(Attempt).Length;
            switch (scenario)
            {
                case "BelowOldest":
                case "AtOldest":
                {
                    // Commits from zero to six, the early ones removed after a checkpoint
                    var root = scenario == "BelowOldest" ? 5 : 3;
                    await Create(table, 6);
                    var stage = await OwnStage(table, 3);
                    for (int version = 0; version < (scenario == "BelowOldest" ? 5 : 4); version++)
                    {
                        File.Delete(table.Log(CommitName(version)));
                    }
                    table.Put($"_delta_log/{root:D20}.checkpoint.parquet", "a checkpoint no sweep reads");
                    return (6, stage);
                }
                case "DeadWithoutSuccessor":
                {
                    await Create(table, 2);
                    var stage = await OwnStage(table, 3);
                    await WriteOther(table, 3);
                    return (3, stage);
                }
            }

            await Create(table, 2);
            string stageName;
            switch (scenario)
            {
                case "LegacyPublished":
                case "LegacyDead":
                    stageName = await Stage(table, 3, null, null, s_owned);
                    break;
                case "InvalidOwnership":
                    stageName = StageName(3, Attempt);
                    RawStage(table, 3, Attempt, $"{{\"commitInfo\":{{\"flowtide.stageId\":\"{Attempt}\",\"flowtide.adoptedAt\":0,\"timestamp\":2003,\"operation\":\"WRITE\",\"flowtide.createdFiles\":[\"own-a.parquet\",\"\"]}}}}", s_owned);
                    break;
                case "NoOwnership":
                    stageName = await Stage(table, 3, Attempt, null, s_owned);
                    break;
                case "ReorderedHeader":
                    stageName = StageName(3, Attempt);
                    RawStage(table, 3, Attempt, $"{{\"commitInfo\":{{\"timestamp\":2003,\"flowtide.stageId\":\"{Attempt}\",\"flowtide.adoptedAt\":0,\"operation\":\"WRITE\",\"flowtide.createdFiles\":[\"own-a.parquet\",\"own-b.parquet\"]}}}}", s_owned);
                    break;
                case "EpochAboveVersion":
                    stageName = StageName(3, Attempt);
                    RawStage(table, 3, Attempt, $"{{\"commitInfo\":{{\"flowtide.stageId\":\"{Attempt}\",\"flowtide.adoptedAt\":4,\"timestamp\":2003,\"operation\":\"WRITE\",\"flowtide.createdFiles\":[\"own-a.parquet\",\"own-b.parquet\"]}}}}", s_owned);
                    break;
                case "ShortForeignTarget":
                    // A legacy stage without commitInfo, against a commit shorter than the stage prefix
                    stageName = StageName(3, Attempt);
                    RawStage(table, 3, Attempt, "{\"protocol\":{\"minReaderVersion\":1,\"minWriterVersion\":2}}", s_owned);
                    break;
                default:
                    stageName = await OwnStage(table, 3);
                    break;
            }
            var stageBytes = File.ReadAllBytes(table.Log(stageName));
            var target = "_delta_log/" + CommitName(3);
            switch (scenario)
            {
                case "Published":
                case "LegacyPublished":
                    WriteRaw(table, target, stageBytes);
                    break;
                case "SameLengthOwnPrefix":
                {
                    var changed = (byte[])stageBytes.Clone();
                    changed[prefixLength + 3] = changed[prefixLength + 3] == (byte)'0' ? (byte)'1' : (byte)'0';
                    WriteRaw(table, target, changed);
                    break;
                }
                case "SameLengthOtherPrefix":
                {
                    // The last character of the stage id differs; every add still names the owned files
                    var changed = (byte[])stageBytes.Clone();
                    changed[prefixLength - 2] = changed[prefixLength - 2] == (byte)'e' ? (byte)'f' : (byte)'e';
                    WriteRaw(table, target, changed);
                    break;
                }
                case "RepairablePrefix":
                    WriteRaw(table, target, stageBytes.AsSpan(0, prefixLength + 10).ToArray());
                    break;
                case "LongerWithOwnPrefix":
                    WriteRaw(table, target, stageBytes.Concat(Utf8("{\"add\":{\"path\":\"extra.parquet\",\"partitionValues\":{},\"size\":1,\"modificationTime\":1,\"dataChange\":true}}\n")).ToArray());
                    break;
                case "EmptyTarget":
                    WriteRaw(table, target, Array.Empty<byte>());
                    break;
                case "ShortForeignTarget":
                    WriteRaw(table, target, DeltaTransactionWriter.StagePrefix(Attempt).AsSpan(0, 18).ToArray());
                    break;
                default:
                    await WriteOther(table, 3);
                    break;
            }
            await Write(table, 4, Own(4), DeltaLakeCheckpointBuildTests.Add("part-4.parquet"));
            // Above V the stage is the running sink's, even with a later commit listed
            return (scenario == "AboveV" ? 2 : 4, stageName);
        }

        // A classic checkpoint at the version, as the maintenance builder writes it
        private async Task BuildCheckpoint(Table table, long version)
        {
            var owner = _owners.Add(new ScratchDirectory(Path.Combine(_directory, ScratchOwners.ShortId())));
            using (var scratch = owner.CreateJob())
            {
                var result = await CheckpointBuilder.BuildAsync(new CheckpointBuildContext()
                {
                    Storage = table.Local,
                    Table = "t",
                    Scratch = scratch,
                    Memory = new JobMemory(),
                    Clock = new FakeTimeProvider(s_now)
                }, new BuildRequest() { Version = version });
                Assert.Equal(BuildStatus.Completed, result.Status);
            }
            _owners.Release(owner);
        }

        // A dead stage at 3 owning the two files, another attempt's commit at 3 and an own head at 4
        private async Task<(Table Table, string Stage)> DeadStage(bool eligible = true)
        {
            var table = NewTable(eligible);
            await Create(table, 2);
            var stage = await OwnStage(table, 3);
            await WriteOther(table, 3);
            await Write(table, 4, Own(4), DeltaLakeCheckpointBuildTests.Add("part-4.parquet"));
            return (table, stage);
        }

        [Fact]
        public async Task DeadStageRemovalPrecedesPayload()
        {
            var (table, stage) = await DeadStage();
            var stagePath = table.StorePath("_delta_log/" + stage);
            var events = new ConcurrentQueue<string>();
            table.Hooks.StatAnswer = (path, entry) =>
            {
                events.Enqueue("stat " + path.Full);
                return entry;
            };
            var sweep = Sweep(table);
            sweep.BeforeDeleteForTests = target =>
            {
                events.Enqueue("delete " + Path.GetFileName(target.Key));
                return Task.CompletedTask;
            };

            var result = await sweep.RunAsync(Context(4));

            Assert.Equal(OutcomeKind.Completed, result.Kind);
            Assert.False(File.Exists(table.Log(stage)));
            Assert.False(table.Exists("own-a.parquet"));
            Assert.False(table.Exists("own-b.parquet"));
            var order = events.ToList();
            var removed = order.IndexOf("delete " + stage);
            var confirmed = order.IndexOf("stat " + stagePath);
            var firstPayload = order.FindIndex(x => x.StartsWith("delete own-", StringComparison.Ordinal));
            Assert.True(removed >= 0 && removed < confirmed && confirmed < firstPayload, string.Join(", ", order));
            Assert.Equal(2, order.Count(x => x.StartsWith("delete own-", StringComparison.Ordinal)));

            // A stage delete that fails in every retry leaves the stage and every owned file
            var (failing, failingStage) = await DeadStage();
            var failingSweep = Sweep(failing, new DeletionRunSettings() { RetryDelay = TimeSpan.Zero });
            var payloadDeletes = 0;
            failingSweep.BeforeDeleteForTests = target =>
            {
                if (target.Kind == TargetKind.Stage)
                {
                    throw new IOException("injected");
                }
                Interlocked.Increment(ref payloadDeletes);
                return Task.CompletedTask;
            };
            var failed = await failingSweep.RunAsync(Context(4));
            Assert.Equal(MaintenanceReason.DeleteFailed, failed.Reason);
            Assert.True(File.Exists(failing.Log(failingStage)));
            Assert.True(failing.Exists("own-a.parquet"));
            Assert.True(failing.Exists("own-b.parquet"));
            Assert.Equal(0, payloadDeletes);
            Assert.Equal(0, failingSweep.LastReport!.LeakedStages);

            // A confirming Stat that fails deletes no output, and the output is counted as left behind
            var (erring, erringStage) = await DeadStage();
            var erringPath = erring.StorePath("_delta_log/" + erringStage);
            erring.Hooks.StatAnswer = (path, entry) => path.Full == erringPath ? throw new IOException("injected") : entry;
            var erringSweep = Sweep(erring);
            await erringSweep.RunAsync(Context(4));
            Assert.False(File.Exists(erring.Log(erringStage)));
            Assert.True(erring.Exists("own-a.parquet"));
            Assert.Equal(1, erringSweep.LastReport!.LeakedStages);

            // A removal the confirming Stat does not see deletes no output either
            var (unconfirmed, unconfirmedStage) = await DeadStage();
            var unconfirmedPath = unconfirmed.StorePath("_delta_log/" + unconfirmedStage);
            unconfirmed.Hooks.StatAnswer = (path, entry) => path.Full == unconfirmedPath ? new IOEntry(path.Full) { Size = 1 } : entry;
            var unconfirmedSweep = Sweep(unconfirmed);
            await unconfirmedSweep.RunAsync(Context(4));
            Assert.False(File.Exists(unconfirmed.Log(unconfirmedStage)));
            Assert.True(unconfirmed.Exists("own-a.parquet"));
            Assert.True(unconfirmed.Exists("own-b.parquet"));
            Assert.Equal(0, unconfirmedSweep.LastReport!.PayloadDeleted);
        }

        [Fact]
        public async Task PayloadDeleteThatAlwaysFailsLeaksOnlyThatFile()
        {
            var table = NewTable(fastMeter: true);
            await Create(table, 2);
            // Two dead stages whose output takes two slices of a thousand
            var first = Enumerable.Range(0, 600).Select(i => $"own-3-{i:D4}.parquet").ToArray();
            var second = Enumerable.Range(0, 500).Select(i => $"own-4-{i:D4}.parquet").ToArray();
            var firstStage = await Stage(table, 3, Attempt, first, first);
            await WriteOther(table, 3);
            var secondStage = await Stage(table, 4, "1f8fad5bd9cb469fa16570867728950e", second, second);
            await WriteOther(table, 4);
            await Write(table, 5, Own(5));
            const string Failing = "own-3-0005.parquet";
            var sweep = Sweep(table, new DeletionRunSettings() { RetryDelay = TimeSpan.Zero });
            var attempts = 0;
            sweep.BeforeDeleteForTests = target =>
            {
                if (Path.GetFileName(target.Key) == Failing)
                {
                    Interlocked.Increment(ref attempts);
                    throw new IOException("injected");
                }
                return Task.CompletedTask;
            };

            var result = await sweep.RunAsync(Context(5));

            var report = sweep.LastReport!;
            Assert.Equal(StageVerdict.Dead, report.Verdicts[firstStage]);
            Assert.Equal(StageVerdict.Dead, report.Verdicts[secondStage]);
            Assert.True(table.Exists(Failing));
            Assert.Equal(DeletionRun.MaxFailedSlices, attempts);
            Assert.All(first.Concat(second).Where(x => x != Failing), x => Assert.False(table.Exists(x), x));
            Assert.Equal(first.Length + second.Length - 1, report.PayloadDeleted);
            Assert.Equal(1, report.PayloadLeaked);
            Assert.Equal(0, report.LeakedStages);
            Assert.Equal(MaintenanceReason.DeleteFailed, result.Reason);
        }

        [Fact]
        public async Task TransientPayloadFailureIsRetriedAfterABackoff()
        {
            var (table, _) = await DeadStage();
            var clock = new TimerClock(s_now);
            var sweep = new StagedSweep(table.Configuration, table.Metered, clock, new DeletionRunSettings() { RetryDelay = TimeSpan.FromSeconds(1) });
            var once = 1;
            sweep.BeforeDeleteForTests = target => Path.GetFileName(target.Key) == "own-a.parquet" && Interlocked.Exchange(ref once, 0) == 1 ? throw new IOException("injected") : Task.CompletedTask;

            var running = sweep.RunAsync(Context(4, clock: clock));
            // The retry waits on the clock
            await clock.NextTimerAsync(s_wait);
            Assert.True(table.Exists("own-a.parquet"));
            clock.Fake.Advance(TimeSpan.FromSeconds(1));
            var result = await running.WaitAsync(s_wait);

            Assert.Equal(OutcomeKind.Completed, result.Kind);
            Assert.False(table.Exists("own-a.parquet"));
            Assert.False(table.Exists("own-b.parquet"));
            Assert.Equal(2, sweep.LastReport!.PayloadDeleted);
            Assert.Equal(0, sweep.LastReport.PayloadLeaked);
            Assert.Equal(0, sweep.LastReport.LeakedStages);

            // A build due during the backoff ends the run; only what it did not delete is counted, and only the stage that owns it
            foreach (var countable in new[] { true, false })
            {
                var yielding = NewTable();
                await Create(yielding, 2);
                await Stage(yielding, 3, Attempt, new[] { "own-a.parquet" }, "own-a.parquet");
                await WriteOther(yielding, 3);
                await Stage(yielding, 4, "1f8fad5bd9cb469fa16570867728950e", new[] { "own-b.parquet" }, "own-b.parquet");
                await WriteOther(yielding, 4);
                await Write(yielding, 5, Own(5));
                var yieldClock = new TimerClock(s_now);
                var yieldSweep = new StagedSweep(yielding.Configuration, yielding.Metered, yieldClock, new DeletionRunSettings() { RetryDelay = TimeSpan.FromSeconds(10) });
                yieldSweep.BeforeDeleteForTests = target => Path.GetFileName(target.Key) == "own-b.parquet" ? throw new IOException("injected") : Task.CompletedTask;
                var job = new JobRun(0, JobKind.StagedSweep) { Cancellation = new CancellationTokenSource() };
                var memory = new JobMemory();
                var yieldRunning = yieldSweep.RunAsync(Context(5, run: job, clock: yieldClock, memory: memory));
                await yieldClock.NextTimerAsync(s_wait);
                if (!countable)
                {
                    // What remains cannot be read back, so every confirmed stage is counted
                    memory.BeforeReserveForTests = _ => throw new CheckpointDeclinedException(MaintenanceReason.ManagedMemoryCap, DeclineDependency.None, "injected");
                }
                job.YieldRequested = true;
                yieldClock.Fake.Advance(TimeSpan.FromSeconds(1));
                await yieldRunning.WaitAsync(s_wait);
                Assert.False(yielding.Exists("own-a.parquet"));
                Assert.True(yielding.Exists("own-b.parquet"));
                Assert.Equal(1, yieldSweep.LastReport!.PayloadDeleted);
                Assert.Equal(countable ? 1 : 0, yieldSweep.LastReport.PayloadLeaked);
                Assert.Equal(countable ? 1 : 2, yieldSweep.LastReport.LeakedStages);
            }
        }

        [Fact]
        public async Task OutputDeletedBehindAFailureIsNotCountedAsLeft()
        {
            var table = NewTable();
            await Create(table, 2);
            await Stage(table, 3, Attempt, new[] { "own-a.parquet" }, "own-a.parquet");
            await WriteOther(table, 3);
            await Stage(table, 4, "1f8fad5bd9cb469fa16570867728950e", new[] { "own-b.parquet" }, "own-b.parquet");
            await WriteOther(table, 4);
            await Write(table, 5, Own(5));
            var clock = new TimerClock(s_now);
            var sweep = new StagedSweep(table.Configuration, table.Metered, clock, new DeletionRunSettings() { RetryDelay = TimeSpan.FromSeconds(10) });
            // own-a's delete fails once own-b's was submitted beside it in the same slice
            var submitted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            sweep.BeforeDeleteForTests = async target =>
            {
                var name = Path.GetFileName(target.Key);
                if (name == "own-b.parquet")
                {
                    submitted.TrySetResult();
                }
                else if (name == "own-a.parquet")
                {
                    await submitted.Task.WaitAsync(s_wait);
                    throw new IOException("injected");
                }
            };
            var job = new JobRun(0, JobKind.StagedSweep) { Cancellation = new CancellationTokenSource() };
            var running = sweep.RunAsync(Context(5, run: job, clock: clock));

            // A build due during the retry's backoff ends the run at own-a
            await clock.NextTimerAsync(s_wait);
            job.YieldRequested = true;
            clock.Fake.Advance(TimeSpan.FromSeconds(1));
            await running.WaitAsync(s_wait);

            // Only own-a and its stage are counted, not own-b deleted behind it
            var report = sweep.LastReport!;
            Assert.True(table.Exists("own-a.parquet"));
            Assert.False(table.Exists("own-b.parquet"));
            Assert.Equal(1, report.PayloadDeleted);
            Assert.Equal(1, report.PayloadLeaked);
            Assert.Equal(1, report.LeakedStages);
        }

        [Fact]
        public async Task StopSkipsCountingTheOutputLeft()
        {
            var table = NewTable();
            await Create(table, 2);
            var first = new[] { "own-3-a.parquet", "own-3-b.parquet" };
            var second = new[] { "own-4-a.parquet", "own-4-b.parquet" };
            await Stage(table, 3, Attempt, first, first);
            await WriteOther(table, 3);
            await Stage(table, 4, "1f8fad5bd9cb469fa16570867728950e", second, second);
            await WriteOther(table, 4);
            await Write(table, 5, Own(5));
            var sweep = Sweep(table, new DeletionRunSettings() { MaxOutstanding = 1 });
            using var stop = new CancellationTokenSource();
            var memory = new JobMemory();
            var reservedAfterStop = 0;
            // The worker stops during the first payload delete; after it only a count of what is left reads the path sets
            sweep.BeforeDeleteForTests = target =>
            {
                if (target.Kind == TargetKind.Payload && !stop.IsCancellationRequested)
                {
                    stop.Cancel();
                    memory.BeforeReserveForTests = _ => Interlocked.Increment(ref reservedAfterStop);
                }
                return Task.CompletedTask;
            };

            await sweep.RunAsync(Context(5, stop: stop.Token, memory: memory));

            // The names left are not read back, so both stages count and no name does
            var report = sweep.LastReport!;
            Assert.Equal(1, report.PayloadDeleted);
            Assert.Equal(3, first.Concat(second).Count(table.Exists));
            Assert.Equal(0, reservedAfterStop);
            Assert.Equal(0, report.PayloadLeaked);
            Assert.Equal(2, report.LeakedStages);
        }

        [Fact]
        public async Task SweepLeaksReachTheSinkCountersAndWarnOnce()
        {
            var (table, _) = await DeadStage();
            var counters = new SweepCounters();
            var logs = new TestLogCollector();
            var clock = new TimerClock(s_now);
            var cleanup = new DeltaTableCleanup(table.Configuration, table.Metered, clock, "t", null, () => table.Local, new DiscardCounters(), logs.CreateLogger("t"), "t", counters);
            // own-a's and own-c's deletes fail in every retry
            cleanup.SweepForTests.BeforeDeleteForTests = target => Path.GetFileName(target.Key) is "own-a.parquet" or "own-c.parquet" ? throw new IOException("injected") : Task.CompletedTask;

            // A turn below the dead stage leaves nothing behind and stays quiet
            await cleanup.RunAsync(JobKind.StagedSweep, Context(2, clock: clock));
            Assert.Equal((0L, 0L), (counters.PayloadLeaked, counters.LeakedStages));
            Assert.Empty(logs.Warnings);
            for (int turn = 1; turn <= 2; turn++)
            {
                if (turn == 2)
                {
                    // A second dead stage for the second turn
                    await Stage(table, 5, "2f8fad5bd9cb469fa16570867728950e", new[] { "own-c.parquet" }, "own-c.parquet");
                    await WriteOther(table, 5);
                    await Write(table, 6, Own(6));
                }
                var running = cleanup.RunAsync(JobKind.StagedSweep, Context(turn == 1 ? 4 : 6, clock: clock));
                for (int retry = 1; retry < DeletionRun.MaxFailedSlices; retry++)
                {
                    await clock.NextTimerAsync(s_wait);
                    clock.Fake.Advance(DeletionRun.DefaultRetryDelay * (1 << (retry - 1)));
                }
                var result = await running.WaitAsync(s_wait);
                Assert.Equal(MaintenanceReason.DeleteFailed, result.Reason);
                Assert.Equal(1, cleanup.LastSweepForTests!.PayloadLeaked);

                // Each turn's leak is counted, and only the run's first logs a warning
                Assert.Equal(turn, counters.PayloadLeaked);
                Assert.Equal(0, counters.LeakedStages);
                Assert.Single(logs.Warnings, x => x.Contains("left output behind"));
            }
            Assert.True(table.Exists("own-a.parquet"));
            Assert.False(table.Exists("own-b.parquet"));
            Assert.True(table.Exists("own-c.parquet"));
        }

        [Fact]
        public async Task SweepLeaksAreShownByTheSinkGauges()
        {
            await using var run = SinkRun.Start(nameof(SweepLeaksAreShownByTheSinkGauges), _directory);
            var logs = new TestLogCollector();
            run.Stream.AddLoggerProvider(logs);
            await run.Started();
            run.Stream.Generate(100);
            await DeltaTestKit.WaitForVersion(run.Local, "test", run.Stream, 1);
            var worker = Assert.Single(run.Stream.Maintenance.Workers);
            var cleanup = (DeltaTableCleanup)worker.Cleanup!;
            Assert.Equal(0, DiscardHandOffTests.SinkGauge(run.Stream, "delta_cleanup_sweep_leaked_stages"));

            // A dead stage at 0 whose first line proves no ownership is removed alone, its output counted as left behind
            var log = Path.Combine(run.TableDir, "_delta_log");
            var id = Guid.NewGuid().ToString("N");
            var stage = StageName(0, id);
            File.WriteAllText(Path.Combine(log, stage), $"{{\"commitInfo\":{{\"flowtide.stageId\":\"{id}\",\"timestamp\":1,\"operation\":\"WRITE\"}}}}\n");
            await run.Stream.Maintenance.TickAndSettleAsync(1);
            Assert.Equal(StageVerdict.Dead, cleanup.LastSweepForTests!.Verdicts[stage]);
            Assert.False(File.Exists(Path.Combine(log, stage)));
            Assert.Equal(1, cleanup.LastSweepForTests.LeakedStages);
            Assert.Equal(1, DiscardHandOffTests.SinkGauge(run.Stream, "delta_cleanup_sweep_leaked_stages"));
            Assert.Equal(0, DiscardHandOffTests.SinkGauge(run.Stream, "delta_cleanup_sweep_leaked"));
            Assert.Single(logs.Warnings, x => x.Contains("left output behind"));
            Assert.Equal(0, run.Stream.FailureNotificationCount);
        }

        [Fact]
        public async Task StageRetryPassesItsOwnEarlierDeletions()
        {
            var table = NewTable();
            await Create(table, 0);
            var stages = new List<string>();
            for (long version = 1; version <= 3; version++)
            {
                var stage = await Stage(table, version, $"attempt-{version}", Array.Empty<string>());
                WriteRaw(table, $"_delta_log/{CommitName(version)}", File.ReadAllBytes(table.Log(stage)));
                stages.Add(stage);
            }
            // The second stage's first delete fails once the third's was submitted beside it
            var thirdSubmitted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var once = 1;
            var sawThird = false;
            var sweep = Sweep(table, new DeletionRunSettings() { RetryDelay = TimeSpan.Zero });
            sweep.BeforeDeleteForTests = async target =>
            {
                var name = Path.GetFileName(target.Key);
                if (name == stages[2])
                {
                    thirdSubmitted.TrySetResult();
                }
                else if (name == stages[1] && Interlocked.Exchange(ref once, 0) == 1)
                {
                    sawThird = await Task.WhenAny(thirdSubmitted.Task, Task.Delay(s_wait)) == thirdSubmitted.Task;
                    throw new IOException("injected");
                }
            };

            var result = await sweep.RunAsync(Context(3));

            // The failure came after the third stage's delete was submitted, so the retry follows a deletion ahead of it
            Assert.True(sawThird);
            Assert.Equal(OutcomeKind.Completed, result.Kind);
            Assert.All(stages, x => Assert.False(File.Exists(table.Log(x)), x));
            Assert.Equal(3, sweep.LastReport!.LogFilesDeleted);
        }

        [Fact]
        public async Task ForeignReferenceProtectsDeadStageOutput()
        {
            var table = NewTable();
            await Create(table, 2);
            var owned = new[] { "own-a.parquet", "own-b.parquet", "own-c.parquet", "_change_data/own-d.parquet", "own-e.parquet" };
            var stage = await Stage(table, 3, Attempt, owned, owned);
            await WriteOther(table, 3);
            // Another writer adds one owned file and records another as change data
            await Write(table, 4, Foreign(4), Reference("own-a.parquet"), Reference("_change_data/own-d.parquet"));
            // The next own commit starts a new epoch and removes a third
            await Write(table, 5, Own(5, epoch: 5), DeltaLakeCheckpointBuildTests.Remove("own-e.parquet", 1));
            // A stage above V that the running sink may still publish names a fourth
            await Stage(table, 6, "live-attempt", new[] { "live.parquet" }, "live.parquet");
            await DeltaTransactionWriter.StageCommit(table.Local, "t", 6, new List<DeltaAction>() { Info("live-2", 5, 2006, new List<string>()), Reference("own-c.parquet") });

            var sweep = Sweep(table);
            var context = Context(5);
            var result = await sweep.RunAsync(context);

            Assert.Equal(OutcomeKind.Completed, result.Kind);
            // The turn's buffers, path sets and transfer charges are all returned
            Assert.Equal(0, context.Memory.Managed);
            Assert.True(context.Memory.ManagedPeak > 0);
            Assert.Equal(0, context.Scratch.Ledger.Reserved);
            Assert.Empty(context.Scratch.Files);
            Assert.Equal(0, context.Transfer.Charged);
            Assert.Equal(StageVerdict.Dead, sweep.LastReport!.Verdicts[stage]);
            Assert.False(File.Exists(table.Log(stage)));
            Assert.False(table.Exists("own-b.parquet"));
            Assert.True(table.Exists("own-a.parquet"));
            Assert.True(table.Exists("own-c.parquet"));
            Assert.True(table.Exists("_change_data/own-d.parquet"));
            Assert.True(table.Exists("own-e.parquet"));
            Assert.True(table.Exists("live.parquet"));
            Assert.Equal(1, sweep.LastReport.PayloadDeleted);
        }

        [Fact]
        public async Task TruncatedListingCannotAuthorizePayload()
        {
            const string Log = "/t/_delta_log/";

            // A listing that hides the commit after its head fails the successor probe; nothing is read or deleted
            var (hidden, hiddenStage) = await DeadStage();
            await Write(hidden, 5, Own(5));
            hidden.Hooks.AfterList = (_, entries) => entries.Where(x => x.Name != CommitName(5)).ToList();
            var hiddenSweep = Sweep(hidden);
            var declined = await hiddenSweep.RunAsync(Context(4));
            Assert.Equal(OutcomeKind.Declined, declined.Kind);
            Assert.Equal(MaintenanceReason.ListingIneligible, declined.Reason);
            Assert.Equal(0, hidden.Hooks.Reads);
            Assert.True(File.Exists(hidden.Log(hiddenStage)));
            Assert.True(hidden.Exists("own-a.parquet"));

            // A planning listing missing a commit between the stage and a checkpoint that still roots the head removes the stage alone and leaves its output behind
            var gap = NewTable();
            await Create(gap, 2);
            var gapStage = await OwnStage(gap, 3);
            await WriteOther(gap, 3);
            await Write(gap, 4, Own(4));
            await Write(gap, 5, Foreign(5), Reference("own-a.parquet"));
            await Write(gap, 6, Own(6, epoch: 6));
            await BuildCheckpoint(gap, 6);
            await Write(gap, 7, Own(7, epoch: 6));
            gap.Hooks.AfterList = (_, entries) => entries.Where(x => x.Name != CommitName(5)).ToList();
            var gapSweep = Sweep(gap);
            var leaked = await gapSweep.RunAsync(Context(7));
            Assert.Equal(StageVerdict.Dead, gapSweep.LastReport!.Verdicts[gapStage]);
            Assert.False(File.Exists(gap.Log(gapStage)));
            Assert.True(gap.Exists("own-a.parquet"));
            Assert.True(gap.Exists("own-b.parquet"));
            Assert.Equal(1, gapSweep.LastReport.LeakedStages);
            Assert.Equal(MaintenanceReason.MissingHistory, gapSweep.LastReport.LeakReason);
            Assert.Equal(OutcomeKind.Declined, leaked.Kind);
            Assert.Equal(MaintenanceReason.MissingHistory, leaked.Reason);

            // A sound planning listing, then payload gates whose listings miss a relied-on commit, delete no output
            var (late, lateStage) = await DeadStage();
            await Write(late, 5, Foreign(5));
            await Write(late, 6, Own(6, epoch: 6));
            var lateSweep = Sweep(late);
            // The planning listing and the stage gate's listing are whole
            late.Hooks.AfterList = (n, entries) => n <= 2 ? entries : entries.Where(x => x.Name != CommitName(5)).ToList();
            await lateSweep.RunAsync(Context(6));
            Assert.False(File.Exists(late.Log(lateStage)));
            Assert.True(late.Exists("own-a.parquet"));
            Assert.True(late.Exists("own-b.parquet"));
            Assert.Equal(0, lateSweep.LastReport!.PayloadDeleted);
            Assert.Equal(1, lateSweep.LastReport.LeakedStages);
            Assert.Equal(2, lateSweep.LastReport.PayloadLeaked);
            Assert.True(late.Hooks.Listings(Log) > 2);
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task StageOnlyRemovalChecksItsListing(bool eligible)
        {
            const string Log = "/t/_delta_log/";

            async Task<(Table Table, string Stage)> PublishedStage()
            {
                var table = NewTable(eligible);
                await Create(table, 2);
                var stage = await OwnStage(table, 3);
                WriteRaw(table, "_delta_log/" + CommitName(3), File.ReadAllBytes(table.Log(stage)));
                await Write(table, 4, Own(4));
                return (table, stage);
            }

            // The successor of the listed head exists
            var (successor, successorStage) = await PublishedStage();
            successor.Hooks.StatAnswer = (path, entry) => path.Full.EndsWith(CommitName(5), StringComparison.Ordinal) ? new IOEntry(path.Full) { Size = 1 } : entry;
            var successorResult = await Sweep(successor).RunAsync(Context(4));
            Assert.Equal(MaintenanceReason.ListingIneligible, successorResult.Reason);
            Assert.True(File.Exists(successor.Log(successorStage)));
            Assert.Equal(0, successor.Hooks.Reads);

            // A stage listed twice
            var (repeated, repeatedStage) = await PublishedStage();
            repeated.Hooks.AfterList = (_, entries) => entries.Concat(entries.Where(x => x.Name == repeatedStage)).ToList();
            Assert.Equal(MaintenanceReason.ListingIneligible, (await Sweep(repeated).RunAsync(Context(4))).Reason);
            Assert.True(File.Exists(repeated.Log(repeatedStage)));

            // A cleanup record this version cannot interpret
            var (fenced, fencedStage) = await PublishedStage();
            fenced.Put("_delta_log/.flowtide-cleanup.unknown");
            Assert.Equal(MaintenanceReason.ListingIneligible, (await Sweep(fenced).RunAsync(Context(4))).Reason);
            Assert.True(File.Exists(fenced.Log(fencedStage)));

            // Planning passes, and every slice gate's own listing fails its check
            var (gate, gateStage) = await PublishedStage();
            gate.Hooks.AfterList = (n, entries) => n == 1 ? entries : entries.Concat(entries.Where(x => x.Name == gateStage)).ToList();
            var gateSweep = Sweep(gate);
            var gateResult = await gateSweep.RunAsync(Context(4));
            Assert.Equal(StageVerdict.Published, gateSweep.LastReport!.Verdicts[gateStage]);
            Assert.Equal(OutcomeKind.Declined, gateResult.Kind);
            Assert.Equal(MaintenanceReason.ListingIneligible, gateResult.Reason);
            Assert.True(File.Exists(gate.Log(gateStage)));
            Assert.True(gate.Hooks.Listings(Log) > 1);

            // The same stage is removed once its listings pass
            var (sound, soundStage) = await PublishedStage();
            var soundSweep = Sweep(sound);
            Assert.Equal(OutcomeKind.Completed, (await soundSweep.RunAsync(Context(4))).Kind);
            Assert.False(File.Exists(sound.Log(soundStage)));
            Assert.True(File.Exists(sound.Log(CommitName(3))));
            Assert.Equal(1, soundSweep.LastReport!.LogFilesDeleted);
        }

        [Fact]
        public async Task PinnedGcsDeclinesListingBasedRemoval()
        {
            using var gcs = StoreFixtures.Gcs();
            var objects = new ScriptedStorage();
            objects.Put("/sales/t/_delta_log/" + CommitName(0));
            objects.Put("/sales/t/_delta_log/" + StageName(0, Attempt));
            // The pinned client has no Stat
            objects.StatAnswer = (_, _) => throw new NotImplementedException();
            var configuration = CleanupConfiguration.Validate(CleanupStoreContract.Create(objects, StoreDescriptor.Of(gcs), "sales/t", false), CleanupOptIns.Off);
            Assert.False(configuration.Store.Eligible);
            var sweep = new StagedSweep(configuration, MeteredFileStorage.Wrap(objects, new MaintenanceBuckets(TimeProvider.System), ListingPaging.S3), TimeProvider.System, new DeletionRunSettings());

            var result = await sweep.RunAsync(Context(0));

            Assert.Equal(OutcomeKind.Declined, result.Kind);
            Assert.Equal(MaintenanceReason.ListingIneligible, result.Reason);
            Assert.Contains(nameof(ListingCheck.ProbeError), result.Detail);
            Assert.DoesNotContain(objects.Requests, x => x.StartsWith("Rm", StringComparison.Ordinal));
        }

        [Fact]
        public async Task CleanupRecordsAreNeverTemporaries()
        {
            var controls = new[]
            {
                ".flowtide-cleanup.00000000000000000003.adv.header.attempt",
                ".flowtide-cleanup.00000000000000000003.adv.part000001.attempt",
                ".flowtide-cleanup.00000000000000000003.adv.0123456789abcdef.attempt.started",
                ".flowtide-cleanup.00000000000000000003.adv.0123456789abcdef.attempt.done"
            };
            var old = ".flowtide-0f8fad5bd9cb469fa16570867728950e.checkpoint.tmp";
            var oldHint = ".flowtide-1f8fad5bd9cb469fa16570867728950e.last_checkpoint.tmp";
            var young = ".flowtide-2f8fad5bd9cb469fa16570867728950e.checkpoint.tmp";
            var unaged = ".flowtide-3f8fad5bd9cb469fa16570867728950e.checkpoint.tmp";

            async Task<Table> Prepared(bool eligible = true, TimeSpan? retention = null)
            {
                var table = NewTable(eligible, retention);
                await Create(table, 2);
                foreach (var name in controls.Append(old).Append(oldHint).Append(young).Append(unaged))
                {
                    table.Put("_delta_log/" + name);
                    File.SetLastWriteTimeUtc(table.Log(name), (s_now - TimeSpan.FromDays(30)).UtcDateTime);
                }
                for (int version = 0; version <= 2; version++)
                {
                    File.SetLastWriteTimeUtc(table.Log(CommitName(version)), (s_now - TimeSpan.FromDays(2 - version)).UtcDateTime);
                }
                File.SetLastWriteTimeUtc(table.Log(old), (s_now - TimeSpan.FromDays(1)).UtcDateTime);
                File.SetLastWriteTimeUtc(table.Log(oldHint), (s_now - TimeSpan.FromDays(1.5)).UtcDateTime);
                File.SetLastWriteTimeUtc(table.Log(young), (s_now - TimeSpan.FromHours(12)).UtcDateTime);
                return table;
            }

            // Only the two temporaries a day older than the newest commit go; every control record stays however old
            var table = await Prepared();
            table.Hooks.AfterList = (_, entries) => entries.Select(x => x.Name == unaged ? Altered(x, e => e.LastModificationTime = null) : x).ToList();
            var sweep = Sweep(table);
            Assert.Equal(OutcomeKind.Completed, (await sweep.RunAsync(Context(2))).Kind);
            Assert.False(File.Exists(table.Log(old)));
            Assert.False(File.Exists(table.Log(oldHint)));
            Assert.True(File.Exists(table.Log(young)));
            Assert.True(File.Exists(table.Log(unaged)));
            Assert.All(controls, x => Assert.True(File.Exists(table.Log(x))));
            Assert.Equal(2, sweep.LastReport!.LogFilesDeleted);

            // Names shaped like a temporary under the control prefix are records, never temporaries
            Assert.True(DeletionExecutor.IsTemporary(old));
            Assert.False(DeletionExecutor.IsTemporary(".flowtide-cleanup.0123456789abcdef01234567.checkpoint.tmp"));
            Assert.False(DeletionExecutor.IsTemporary(".flowtide-cleanup.00000000000000000003.adv.0123456789abcdef.attempt.checkpoint.tmp"));

            // Retention longer than a day is the age
            var retained = await Prepared(retention: TimeSpan.FromDays(2));
            await Sweep(retained).RunAsync(Context(2));
            Assert.True(File.Exists(retained.Log(old)));
            Assert.True(File.Exists(retained.Log(oldHint)));

            // Commits listed without times age nothing
            var untimed = await Prepared();
            untimed.Hooks.AfterList = (_, entries) => entries.Select(x => x.Name.EndsWith(".json", StringComparison.Ordinal) ? Altered(x, e => e.LastModificationTime = null) : x).ToList();
            Assert.Equal(OutcomeKind.NoWork, (await Sweep(untimed).RunAsync(Context(2))).Kind);
            Assert.True(File.Exists(untimed.Log(old)));

            // Without absence evidence temporaries and records stay
            var unvouched = await Prepared(eligible: false);
            await Sweep(unvouched).RunAsync(Context(2));
            Assert.True(File.Exists(unvouched.Log(old)));
            Assert.All(controls, x => Assert.True(File.Exists(unvouched.Log(x))));
        }

        private static IOEntry Altered(IOEntry entry, Action<IOEntry> change)
        {
            var copy = new IOEntry(entry.Path.Full)
            {
                Size = entry.Size,
                LastModificationTime = entry.LastModificationTime,
                CreatedTime = entry.CreatedTime,
                MD5 = entry.MD5
            };
            change(copy);
            return copy;
        }

        [Theory]
        [InlineData("StageSize")]
        [InlineData("StageTime")]
        [InlineData("StageMd5")]
        [InlineData("StageGone")]
        [InlineData("TargetSize")]
        [InlineData("TargetTime")]
        [InlineData("TargetMd5")]
        [InlineData("SuccessorGone")]
        [InlineData("OldestCommitListed")]
        [InlineData("OldestRootGone")]
        [InlineData("TemporaryTime")]
        public async Task EverySliceFindsThePlannedEvidence(string change)
        {
            var table = NewTable();
            string removed;
            long published;
            if (change.StartsWith("Oldest", StringComparison.Ordinal))
            {
                (published, removed) = await Scenario(table, "BelowOldest");
            }
            else if (change == "TemporaryTime")
            {
                await Create(table, 2);
                removed = ".flowtide-0f8fad5bd9cb469fa16570867728950e.checkpoint.tmp";
                table.Put("_delta_log/" + removed);
                File.SetLastWriteTimeUtc(table.Log(removed), (s_now - TimeSpan.FromDays(3)).UtcDateTime);
                for (int version = 0; version <= 2; version++)
                {
                    File.SetLastWriteTimeUtc(table.Log(CommitName(version)), s_now.UtcDateTime);
                }
                published = 2;
            }
            else
            {
                await Create(table, 2);
                removed = await OwnStage(table, 3);
                await WriteOther(table, 3);
                await Write(table, 4, Own(4));
                await Write(table, 5, Own(5));
                published = 5;
            }
            var stage = removed;
            var target = CommitName(change.StartsWith("Oldest", StringComparison.Ordinal) ? 4 : 3);
            Func<IOEntry, IOEntry> alter = change switch
            {
                "StageSize" => x => x.Name == stage ? Altered(x, e => e.Size++) : x,
                "StageTime" => x => x.Name == stage ? Altered(x, e => e.LastModificationTime = e.LastModificationTime!.Value.AddSeconds(1)) : x,
                "StageMd5" => x => x.Name == stage ? Altered(x, e => e.MD5 = "changed") : x,
                "TargetSize" => x => x.Name == target ? Altered(x, e => e.Size++) : x,
                "TargetTime" => x => x.Name == target ? Altered(x, e => e.LastModificationTime = e.LastModificationTime!.Value.AddSeconds(1)) : x,
                "TargetMd5" => x => x.Name == target ? Altered(x, e => e.MD5 = "changed") : x,
                // The newest commit now looks only half a day newer than the temporary
                "TemporaryTime" => x => x.Name.EndsWith(".json", StringComparison.Ordinal) ? Altered(x, e => e.LastModificationTime = s_now.AddDays(-2.5)) : x,
                _ => x => x
            };
            // The planning listing is whole; every later one shows the change
            table.Hooks.AfterList = (n, entries) =>
            {
                if (n == 1)
                {
                    return entries;
                }
                var altered = entries.Select(alter).ToList();
                return change switch
                {
                    "StageGone" => altered.Where(x => x.Name != stage).ToList(),
                    "SuccessorGone" => altered.Where(x => x.Name != CommitName(4)).ToList(),
                    "OldestCommitListed" => altered.Append(new IOEntry(table.StorePath("_delta_log/" + CommitName(3))) { Size = 1, LastModificationTime = s_now }).ToList(),
                    "OldestRootGone" => altered.Where(x => !x.Name.EndsWith(".checkpoint.parquet", StringComparison.Ordinal)).ToList(),
                    _ => altered
                };
            };
            var sweep = Sweep(table);

            var result = await sweep.RunAsync(Context(published));

            Assert.True(File.Exists(table.Log(removed)), change);
            Assert.Equal(0, sweep.LastReport!.LogFilesDeleted);
            Assert.Equal(OutcomeKind.Declined, result.Kind);
            Assert.True(table.Hooks.Listings("/t/_delta_log/") > 1);

            // Without the change the same layout is removed
            table.Hooks.AfterList = null;
            Assert.Equal(OutcomeKind.Completed, (await Sweep(table).RunAsync(Context(published))).Kind);
            Assert.False(File.Exists(table.Log(removed)), change);
        }

        [Fact]
        public async Task SweepNeedsAPublishedVersionAndDeletesSwitchedOn()
        {
            var (table, stage) = await DeadStage();
            var old = ".flowtide-0f8fad5bd9cb469fa16570867728950e.checkpoint.tmp";
            table.Put("_delta_log/" + old);
            File.SetLastWriteTimeUtc(table.Log(old), (s_now - TimeSpan.FromDays(30)).UtcDateTime);

            // Nothing published to this worker yet
            Assert.Equal(OutcomeKind.NoWork, (await Sweep(table).RunAsync(Context(-1))).Kind);
            // Switched off, or a switch that cannot be read
            var off = await Sweep(table, new DeletionRunSettings() { DeletesDisabled = () => true }).RunAsync(Context(4));
            Assert.Equal(MaintenanceReason.DeletesDisabled, off.Reason);
            var unreadable = await Sweep(table, new DeletionRunSettings() { DeletesDisabled = () => throw new InvalidOperationException() }).RunAsync(Context(4));
            Assert.Equal(MaintenanceReason.DeletesDisabled, unreadable.Reason);
            // Storage outside the maintenance meter
            var unmetered = await new StagedSweep(table.Configuration, table.Hooks, TimeProvider.System, new DeletionRunSettings()).RunAsync(Context(4));
            Assert.Equal(MaintenanceReason.Unexpected, unmetered.Reason);
            Assert.Equal(0, table.Hooks.Listings("/t/_delta_log/"));
            Assert.True(File.Exists(table.Log(stage)));
            Assert.True(File.Exists(table.Log(old)));
            Assert.True(table.Exists("own-a.parquet"));

            // Switched off after planning, before the first delete
            var off2 = false;
            var late = Sweep(table, new DeletionRunSettings() { DeletesDisabled = () => off2 });
            table.Hooks.AfterList = (n, entries) =>
            {
                off2 = true;
                return entries;
            };
            Assert.Equal(MaintenanceReason.DeletesDisabled, (await late.RunAsync(Context(4))).Reason);
            Assert.True(File.Exists(table.Log(stage)));
            Assert.True(table.Exists("own-a.parquet"));
        }

        [Fact]
        public async Task SweepPromotesBeforeItsFirstRead()
        {
            var table = NewTable();
            await Create(table, 4);
            for (long version = 1; version <= 3; version++)
            {
                var stage = await Stage(table, version, $"attempt-{version}", Array.Empty<string>());
                WriteRaw(table, $"_delta_log/{CommitName(version)}", File.ReadAllBytes(table.Log(stage)));
            }

            // Three stages with listed commits read six objects
            var promoted = await Sweep(table).RunAsync(Context(4, readLimit: 5));
            Assert.True(promoted.NeedsLongAdmission);
            Assert.Equal(0, table.Hooks.Reads);

            var context = Context(4, readLimit: 6);
            Assert.Equal(OutcomeKind.Completed, (await Sweep(table).RunAsync(context)).Kind);
            Assert.Equal(6, context.ReadsUsed);
            Assert.Equal(6, table.Hooks.Reads);
        }

        [Fact]
        public async Task OwnershipBeyondItsBoundsLeavesTheStageAlone()
        {
            // Two owned names under a one-name limit
            var (names, namesStage) = await DeadStage();
            var namesSweep = new StagedSweep(names.Configuration, names.Metered, TimeProvider.System, new DeletionRunSettings()) { OwnedFileLimit = 1 };
            var namesResult = await namesSweep.RunAsync(Context(4));
            Assert.Equal(MaintenanceReason.CreatedFilesUnreadable, namesResult.Reason);
            Assert.False(File.Exists(names.Log(namesStage)));
            Assert.True(names.Exists("own-a.parquet"));
            Assert.True(names.Exists("own-b.parquet"));
            Assert.Equal(1, namesSweep.LastReport!.LeakedStages);

            // A first line longer than the byte limit
            var (bytes, bytesStage) = await DeadStage();
            var lineLength = File.ReadAllLines(bytes.Log(bytesStage))[0].Length;
            var tight = new StagedSweep(bytes.Configuration, bytes.Metered, TimeProvider.System, new DeletionRunSettings()) { OwnershipByteLimit = lineLength - 1 };
            Assert.Equal(MaintenanceReason.CreatedFilesUnreadable, (await tight.RunAsync(Context(4))).Reason);
            Assert.True(bytes.Exists("own-a.parquet"));

            // Exactly at both limits the output goes
            var (exact, _) = await DeadStage();
            var exactSweep = new StagedSweep(exact.Configuration, exact.Metered, TimeProvider.System, new DeletionRunSettings()) { OwnedFileLimit = 2, OwnershipByteLimit = lineLength };
            Assert.Equal(OutcomeKind.Completed, (await exactSweep.RunAsync(Context(4))).Kind);
            Assert.False(exact.Exists("own-a.parquet"));
            Assert.False(exact.Exists("own-b.parquet"));

            // The ledger refuses the first owned name's buffer
            var (refused, refusedStage) = await DeadStage();
            var memory = new JobMemory();
            var injected = 0;
            memory.BeforeReserveForTests = bytes =>
            {
                if (bytes == 256 && Interlocked.Exchange(ref injected, 1) == 0)
                {
                    throw new CheckpointDeclinedException(MaintenanceReason.ManagedMemoryCap, DeclineDependency.None, "injected");
                }
            };
            var refusedSweep = Sweep(refused);
            Assert.Equal(MaintenanceReason.CreatedFilesUnreadable, (await refusedSweep.RunAsync(Context(4, memory: memory))).Reason);
            Assert.Equal(1, injected);
            Assert.False(File.Exists(refused.Log(refusedStage)));
            Assert.True(refused.Exists("own-a.parquet"));
            Assert.Equal(0, memory.Managed);
        }

        [Fact]
        public async Task LargeStageOwnershipSpansReadChunks()
        {
            var table = NewTable(fastMeter: true);
            await Create(table, 2);
            // Names that were never written, enough to carry the first line past one read chunk
            var phantoms = Enumerable.Range(0, 1600).Select(i => $"phantom-{i:D6}-{Attempt}.parquet");
            var created = s_owned.Concat(phantoms).ToList();
            foreach (var name in s_owned)
            {
                table.Put(name);
            }
            // Later lines span more chunks
            var actions = new List<DeltaAction>() { Info(Attempt, 0, 2003, created) };
            actions.AddRange(s_owned.Select(Reference));
            actions.AddRange(Enumerable.Range(0, 1500).Select(i => Reference($"pad-{i:D6}-{Attempt}.parquet")));
            var stage = (await DeltaTransactionWriter.StageCommit(table.Local, "t", 3, actions)).FileName;
            await WriteOther(table, 3);
            await Write(table, 4, Own(4));
            var lines = File.ReadAllLines(table.Log(stage));
            Assert.True(lines[0].Length > ProofReader.BufferSize);
            Assert.True(new FileInfo(table.Log(stage)).Length > lines[0].Length + 2 * ProofReader.BufferSize);
            var sweep = Sweep(table);

            Assert.Equal(OutcomeKind.Completed, (await sweep.RunAsync(Context(4))).Kind);

            Assert.False(File.Exists(table.Log(stage)));
            Assert.False(table.Exists("own-a.parquet"));
            Assert.False(table.Exists("own-b.parquet"));
            Assert.Equal(2, sweep.LastReport!.PayloadDeleted);
            Assert.Equal(0, sweep.LastReport.LeakedStages);
        }

        [Fact]
        public async Task OwnedNamesWithoutATargetAreKeptAndCounted()
        {
            var table = NewTable();
            await Create(table, 2);
            // Local disk identity takes only ASCII names
            var only = await Stage(table, 3, "only-unusable", new[] { "pärt-1.parquet" }, "pärt-1.parquet");
            var mixed = await Stage(table, 3, "mixed", new[] { "own-a.parquet", "pärt-2.parquet" }, "own-a.parquet", "pärt-2.parquet");
            await WriteOther(table, 3);
            await Write(table, 4, Own(4));
            var sweep = Sweep(table);

            Assert.Equal(OutcomeKind.Completed, (await sweep.RunAsync(Context(4))).Kind);

            Assert.False(File.Exists(table.Log(only)));
            Assert.False(File.Exists(table.Log(mixed)));
            Assert.False(table.Exists("own-a.parquet"));
            Assert.True(table.Exists("pärt-1.parquet"));
            Assert.True(table.Exists("pärt-2.parquet"));
            Assert.Equal(2, sweep.LastReport!.PayloadKept);
            Assert.Equal(1, sweep.LastReport.PayloadDeleted);
        }

        [Fact]
        public async Task EmptyOwnershipNeedsNoProtection()
        {
            var table = NewTable();
            await Create(table, 2);
            var stage = await Stage(table, 3, Attempt, Array.Empty<string>());
            await WriteOther(table, 3);
            await Write(table, 4, Own(4));
            var sweep = Sweep(table);
            var context = Context(4);

            Assert.Equal(OutcomeKind.Completed, (await sweep.RunAsync(context)).Kind);

            Assert.False(File.Exists(table.Log(stage)));
            Assert.Equal(0, sweep.LastReport!.LeakedStages);
            // Only the stage and its commit were read
            Assert.Equal(2, context.ReadsUsed);
        }

        [Fact]
        public async Task UnreadableStageWaitsWhileOthersProceed()
        {
            var table = NewTable();
            await Create(table, 1);
            var published = await Stage(table, 2, "published-attempt", Array.Empty<string>());
            WriteRaw(table, "_delta_log/" + CommitName(2), File.ReadAllBytes(table.Log(published)));
            var dead = await OwnStage(table, 3);
            await WriteOther(table, 3);
            await Write(table, 4, Own(4));
            var once = 0;
            table.Hooks.BeforeOpenRead = path => path.Name == published && Interlocked.Exchange(ref once, 1) == 0 ? throw new IOException("injected") : Task.CompletedTask;
            var sweep = Sweep(table);

            var result = await sweep.RunAsync(Context(4));

            Assert.Equal(OutcomeKind.Declined, result.Kind);
            Assert.Equal(MaintenanceReason.Transient, result.Reason);
            Assert.Equal(StageVerdict.Retry, sweep.LastReport!.Verdicts[published]);
            Assert.True(File.Exists(table.Log(published)));
            Assert.False(File.Exists(table.Log(dead)));
            Assert.False(table.Exists("own-a.parquet"));
        }

        [Fact]
        public async Task OutputWithoutAnOwnHeadOrWithinBudgetStaysBehind()
        {
            // A foreign head proves nothing about references, so only the stage goes
            var foreign = NewTable();
            await Create(foreign, 2);
            var foreignStage = await OwnStage(foreign, 3);
            await WriteOther(foreign, 3);
            await Write(foreign, 4, Foreign(4));
            var foreignSweep = Sweep(foreign);
            var foreignResult = await foreignSweep.RunAsync(Context(4));
            Assert.Equal(OutcomeKind.Declined, foreignResult.Kind);
            Assert.False(File.Exists(foreign.Log(foreignStage)));
            Assert.True(foreign.Exists("own-a.parquet"));
            Assert.Equal(1, foreignSweep.LastReport!.LeakedStages);

            // Protection that needs more reads than the short budget has left
            var (budget, budgetStage) = await DeadStage();
            var budgetSweep = Sweep(budget);
            // Two to classify and five to establish the head, then two protecting commits and three for the one payload gate
            var budgetResult = await budgetSweep.RunAsync(Context(4, readLimit: 11));
            Assert.Equal(MaintenanceReason.EntriesOverCap, budgetResult.Reason);
            Assert.False(File.Exists(budget.Log(budgetStage)));
            Assert.True(budget.Exists("own-a.parquet"));

            // One more read is enough; the gate reads one less, since the head is the anchor
            var (enough, _) = await DeadStage();
            var context = Context(4, readLimit: 12);
            Assert.Equal(OutcomeKind.Completed, (await Sweep(enough).RunAsync(context)).Kind);
            Assert.False(enough.Exists("own-a.parquet"));
            Assert.Equal(11, context.ReadsUsed);
        }

        [Fact]
        public async Task ChangedObjectsWaitForALaterTurn()
        {
            // The stage grows after the listing
            var (grown, grownStage) = await DeadStage();
            var once = 0;
            grown.Hooks.BeforeOpenRead = path =>
            {
                if (path.Name == grownStage && Interlocked.Exchange(ref once, 1) == 0)
                {
                    File.AppendAllText(grown.Log(grownStage), "\n");
                }
                return Task.CompletedTask;
            };
            var grownSweep = Sweep(grown);
            var grownResult = await grownSweep.RunAsync(Context(4));
            Assert.Equal(StageVerdict.Retry, grownSweep.LastReport!.Verdicts[grownStage]);
            Assert.Equal(MaintenanceReason.Transient, grownResult.Reason);
            Assert.True(File.Exists(grown.Log(grownStage)));
            Assert.True(grown.Exists("own-a.parquet"));
            // The next turn sees it as listed
            Assert.Equal(OutcomeKind.Completed, (await Sweep(grown).RunAsync(Context(4))).Kind);
            Assert.False(grown.Exists("own-a.parquet"));

            // The commit at its version grows after the listing; without absence evidence nothing else would read it again
            var (target, targetStage) = await DeadStage(eligible: false);
            var targetOnce = 0;
            target.Hooks.BeforeOpenRead = path =>
            {
                if (path.Name == CommitName(3) && Interlocked.Exchange(ref targetOnce, 1) == 0)
                {
                    File.AppendAllText(target.Log(CommitName(3)), "\n");
                }
                return Task.CompletedTask;
            };
            var targetSweep = Sweep(target);
            await targetSweep.RunAsync(Context(4));
            Assert.Equal(StageVerdict.Retry, targetSweep.LastReport!.Verdicts[targetStage]);
            Assert.True(File.Exists(target.Log(targetStage)));

            // The head changes while the evidence is established, so the dead stage keeps its ownership
            var (head, headStage) = await DeadStage();
            var headOnce = 0;
            head.Hooks.BeforeOpenRead = path =>
            {
                if (path.Name == CommitName(4) && Interlocked.Exchange(ref headOnce, 1) == 0)
                {
                    File.AppendAllText(head.Log(CommitName(4)), "\n");
                }
                return Task.CompletedTask;
            };
            var headSweep = Sweep(head);
            var headResult = await headSweep.RunAsync(Context(4));
            Assert.Equal(StageVerdict.Retry, headSweep.LastReport!.Verdicts[headStage]);
            Assert.Equal(0, headSweep.LastReport.LeakedStages);
            Assert.True(File.Exists(head.Log(headStage)));
            Assert.True(head.Exists("own-a.parquet"));
            Assert.Equal(OutcomeKind.Declined, headResult.Kind);
            Assert.Equal(OutcomeKind.Completed, (await Sweep(head).RunAsync(Context(4))).Kind);
            Assert.False(head.Exists("own-a.parquet"));

            // A protecting commit changes while it is read
            var (protecting, protectingStage) = await DeadStage();
            await Write(protecting, 5, Own(5));
            var protectingReads = 0;
            protecting.Hooks.BeforeOpenRead = path =>
            {
                // The second read of commit 5: the first is the evidence's replay
                if (path.Name == CommitName(5) && Interlocked.Increment(ref protectingReads) == 2)
                {
                    File.AppendAllText(protecting.Log(CommitName(5)), "\n");
                }
                return Task.CompletedTask;
            };
            var protectingSweep = Sweep(protecting);
            await protectingSweep.RunAsync(Context(5));
            Assert.Equal(StageVerdict.Retry, protectingSweep.LastReport!.Verdicts[protectingStage]);
            Assert.True(File.Exists(protecting.Log(protectingStage)));
            Assert.True(protecting.Exists("own-a.parquet"));
        }

        [Fact]
        public async Task UnusableProtectionKeepsTheDeadStage()
        {
            var (table, stage) = await DeadStage();
            // A stage above V whose line protection cannot read
            table.Put("_delta_log/" + StageName(5, "torn"), "{\"add\":{\"path\":\"own-a.parquet\"");
            var sweep = Sweep(table);

            var result = await sweep.RunAsync(Context(4));

            Assert.Equal(MaintenanceReason.PathRejected, result.Reason);
            Assert.Equal(StageVerdict.Retry, sweep.LastReport!.Verdicts[stage]);
            Assert.True(File.Exists(table.Log(stage)));
            Assert.True(table.Exists("own-a.parquet"));
            Assert.True(table.Exists("own-b.parquet"));
        }

        [Fact]
        public async Task StageShapedFoldersAreNotStages()
        {
            var (table, stage) = await DeadStage();
            var folder = StageName(2, "folder");
            Directory.CreateDirectory(table.Log(folder));
            var listed = new ConcurrentQueue<IOEntry>();
            table.Hooks.AfterList = (_, entries) =>
            {
                foreach (var entry in entries.Where(x => x.Path.IsFolder))
                {
                    listed.Enqueue(entry);
                }
                return entries;
            };

            var sweep = Sweep(table);
            Assert.Equal(OutcomeKind.Completed, (await sweep.RunAsync(Context(4))).Kind);

            // The folder is listed, and its name keeps the slash that tells it from a stage
            Assert.Contains(listed, x => x.Name == folder + "/");
            Assert.DoesNotContain(sweep.LastReport!.Verdicts.Keys, x => x.StartsWith(folder, StringComparison.Ordinal));
            Assert.True(Directory.Exists(table.Log(folder)));
            Assert.False(File.Exists(table.Log(stage)));
        }

        [Fact]
        public void SweepRunsOneDeleteAtATimeOnPinnedS3()
        {
            using var s3 = StoreFixtures.S3();
            using var azure = StoreFixtures.Azure();
            var settings = new DeletionRunSettings() { MaxOutstanding = 4 };
            Assert.Equal(1, StagedSweep.RunSettings(CleanupStoreContract.Classify(s3, "c/t", false), settings).MaxOutstanding);
            Assert.Equal(4, StagedSweep.RunSettings(CleanupStoreContract.Classify(azure, "c/t", false), settings).MaxOutstanding);
            Assert.Equal(4, settings.MaxOutstanding);
        }

        [Fact]
        public void PayloadTargetsStayInsideTheTable()
        {
            using var s3 = StoreFixtures.S3();
            var configuration = CleanupConfiguration.Validate(CleanupStoreContract.Classify(s3, "c/t", false), CleanupOptIns.Off);
            var executor = new DeletionExecutor(configuration, s3);

            Assert.Equal(new DeletionTarget("/c/t/part-1.parquet", TargetKind.Payload), StagedSweep.PayloadTarget(executor, "/c/t/part-1.parquet"));
            // A path outside the table is handed on whole, and the executor's check keeps it
            var outside = StagedSweep.PayloadTarget(executor, "/c/u/t/part-1.parquet");
            Assert.Equal("/c/u/t/part-1.parquet", outside.Key);
            Assert.Equal(KeepReason.Unqualified, executor.Check(outside));

            // On local disk the target is the native path
            var table = NewTable();
            var local = new DeletionExecutor(table.Configuration, table.Metered);
            Assert.Equal(table.Native("part-1.parquet"), StagedSweep.PayloadTarget(local, "/t/part-1.parquet").Key);
            Assert.Equal(KeepReason.Unqualified, local.Check(StagedSweep.PayloadTarget(local, "/u/t/part-1.parquet")));
        }

        [Fact]
        public async Task PreemptionBeforeAnyDeleteKeepsEverything()
        {
            var (table, stage) = await DeadStage();
            var run = new JobRun(0, JobKind.StagedSweep) { Cancellation = new CancellationTokenSource() };
            table.Hooks.BeforeOpenRead = _ =>
            {
                run.YieldRequested = true;
                return Task.CompletedTask;
            };

            var result = await Sweep(table).RunAsync(Context(4, run: run));

            Assert.Equal(OutcomeKind.Preempted, result.Kind);
            Assert.True(File.Exists(table.Log(stage)));
            Assert.True(table.Exists("own-a.parquet"));

            // A build due by the time the listing returns stops the turn before its first read
            var (listed, listedStage) = await DeadStage();
            var listedRun = new JobRun(0, JobKind.StagedSweep) { Cancellation = new CancellationTokenSource() };
            listed.Hooks.AfterList = (_, entries) =>
            {
                listedRun.YieldRequested = true;
                return entries;
            };
            Assert.Equal(OutcomeKind.Preempted, (await Sweep(listed).RunAsync(Context(4, run: listedRun))).Kind);
            Assert.Equal(0, listed.Hooks.Reads);
            Assert.True(File.Exists(listed.Log(listedStage)));

            // A stop of the worker surfaces as cancellation, here found between chunks rather than by a request
            var (stopped, stoppedStage) = await DeadStage();
            using var worker = new CancellationTokenSource();
            stopped.Hooks.BeforeOpenRead = path =>
            {
                if (path.Name == CommitName(3))
                {
                    worker.Cancel();
                }
                return Task.CompletedTask;
            };
            var cancelled = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => Sweep(stopped).RunAsync(Context(4, stop: worker.Token)));
            Assert.Equal(worker.Token, cancelled.CancellationToken);
            Assert.IsType<CleanupPreemptedException>(cancelled.InnerException);
            Assert.True(File.Exists(stopped.Log(stoppedStage)));
        }
    }

    /// <summary>
    /// A connector-owned view of local disk whose listings, Stat answers and reads a test can rewrite, hold or count.
    /// </summary>
    internal sealed class SweepStorage : IFileStorage, IConnectorOwnedStorage, ILocalDiskFileStorage
    {
        private int _listings;

        public SweepStorage(IFileStorage inner)
        {
            Inner = inner;
        }

        public IFileStorage Inner { get; }

        // Calls as "Verb path", in order
        public ConcurrentQueue<string> Calls { get; } = new ConcurrentQueue<string>();

        // Rewrites a listing, given its number counted from one
        public Func<int, List<IOEntry>, List<IOEntry>>? AfterList { get; set; }

        // Replaces a Stat answer; it may throw
        public Func<IOPath, IOEntry?, IOEntry?>? StatAnswer { get; set; }

        public Func<IOPath, Task>? BeforeOpenRead { get; set; }

        public int Reads => Calls.Count(x => x.StartsWith("OpenRead ", StringComparison.Ordinal));

        public int Listings(string folder) => Calls.Count(x => x == "Ls " + folder);

        public async Task<IReadOnlyCollection<IOEntry>> Ls(IOPath? path = null, bool recurse = false, CancellationToken cancellationToken = default)
        {
            Calls.Enqueue("Ls " + (path ?? IOPath.Root).Full);
            var entries = (await Inner.Ls(path, recurse, cancellationToken)).ToList();
            var number = Interlocked.Increment(ref _listings);
            return AfterList != null ? AfterList(number, entries) : entries;
        }

        public async Task<IOEntry?> Stat(IOPath path, CancellationToken cancellationToken = default)
        {
            Calls.Enqueue("Stat " + path.Full);
            var entry = await Inner.Stat(path, cancellationToken);
            return StatAnswer != null ? StatAnswer(path, entry) : entry;
        }

        public async Task<Stream?> OpenRead(IOPath path, CancellationToken cancellationToken = default)
        {
            Calls.Enqueue("OpenRead " + path.Full);
            if (BeforeOpenRead != null)
            {
                await BeforeOpenRead(path);
            }
            return await Inner.OpenRead(path, cancellationToken);
        }

        public Task Rm(IOPath path, CancellationToken cancellationToken = default)
        {
            Calls.Enqueue("Rm " + path.Full);
            return Inner.Rm(path, cancellationToken);
        }

        public string ToNativeLocalPath(IOPath path) => ((ILocalDiskFileStorage)Inner).ToNativeLocalPath(path);

        public Task<Stream> OpenWrite(IOPath path, CancellationToken cancellationToken = default) => Inner.OpenWrite(path, cancellationToken);

        public Task Ren(IOPath name, IOPath newName, CancellationToken cancellationToken = default) => Inner.Ren(name, newName, cancellationToken);

        public Task<string?> ReadText(IOPath path, Encoding? encoding = null, CancellationToken cancellationToken = default) => Inner.ReadText(path, encoding, cancellationToken);

        public Task WriteText(IOPath path, string contents, Encoding? encoding = null, CancellationToken cancellationToken = default) => Inner.WriteText(path, contents, encoding, cancellationToken);

        public Task<T?> ReadAsJson<T>(IOPath path, CancellationToken cancellationToken = default) => Inner.ReadAsJson<T>(path, cancellationToken);

        public Task WriteAsJson(IOPath path, object value, bool writeIndented = true, CancellationToken cancellationToken = default) => Inner.WriteAsJson(path, value, writeIndented, cancellationToken);

        public Task<bool> Exists(IOPath path, CancellationToken cancellationToken = default) => Inner.Exists(path, cancellationToken);

        public void Dispose()
        {
        }
    }
}
