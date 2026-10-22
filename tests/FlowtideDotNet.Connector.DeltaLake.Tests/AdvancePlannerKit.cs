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
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.DeletionVectors;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Utils;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.Advance;
using Microsoft.Extensions.Time.Testing;
using Stowage;
using System.Diagnostics;
using System.Security.Cryptography;
using System.Text;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    // Local tables, seeded histories, hand-built boundaries and contexts for advance planning
    internal sealed class AdvancePlannerKit : IDisposable
    {
        public static readonly DateTimeOffset Now = new DateTimeOffset(2026, 10, 17, 12, 0, 0, TimeSpan.Zero);

        // One second per version, ending well before now
        public static readonly long BaseTimestamp = Now.ToUnixTimeMilliseconds() - (long)TimeSpan.FromDays(2).TotalMilliseconds;

        private readonly string _directory;
        private readonly ScratchOwners _owners = new ScratchOwners();
        private readonly List<IDisposable> _disposables = new List<IDisposable>();

        public AdvancePlannerKit(string? directory = null)
        {
            _directory = directory ?? Path.Combine(Path.GetTempPath(), "ap-" + ScratchOwners.ShortId());
            Directory.CreateDirectory(_directory);
        }

        public void Dispose()
        {
            for (int i = _disposables.Count - 1; i >= 0; i--)
            {
                _disposables[i].Dispose();
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

        internal sealed class Table
        {
            public required IFileStorage Local { get; init; }

            public required CleanupConfiguration Configuration { get; init; }

            // The table's own folder
            public required string Native { get; init; }

            public string Log(string name) => Path.Combine(Native, "_delta_log", name);
        }

        // An eligible local disk table at /t
        public Table Local()
        {
            var root = Path.Combine(_directory, ScratchOwners.ShortId());
            Directory.CreateDirectory(Path.Combine(root, "t"));
            var local = Files.Of.LocalDisk(root);
            _disposables.Add(local);
            var configuration = CleanupConfiguration.Validate(local, "t", false, CleanupOptIns.Off);
            Assert.True(configuration.Store.Eligible);
            return new Table() { Local = local, Configuration = configuration, Native = Path.Combine(root, "t") };
        }

        public CleanupContext Context(bool longAdmission, long head, long readLimit = MaintenanceWorkerSettings.DefaultShortReads, CancellationToken stop = default, TimeProvider? clock = null,
            JobRun? run = null, int readAhead = 1)
        {
            clock ??= new FakeTimeProvider(Now);
            var scratch = _owners.Add(new ScratchDirectory(Path.Combine(_directory, ScratchOwners.ShortId()))).CreateJob();
            _disposables.Add(scratch);
            return new CleanupContext(run ?? new JobRun(0, JobKind.Advance) { Cancellation = new CancellationTokenSource() }, clock)
            {
                Kind = JobKind.Advance,
                Long = longAdmission,
                ReadLimit = longAdmission ? long.MaxValue : readLimit,
                Deadline = clock.GetUtcNow() + (longAdmission ? TimeSpan.FromHours(6) : TimeSpan.FromMinutes(15)),
                Head = head,
                TableId = "table-a",
                Scratch = scratch,
                Memory = new JobMemory(),
                Transfer = new TransferAllowance(),
                Stop = stop,
                ReadAhead = readAhead
            };
        }

        // A published Phase 3 build at the version
        public async Task Build(Table table, long version, RootPolicy root = RootPolicy.Auto)
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
                    Clock = new FakeTimeProvider(Now)
                }, new BuildRequest() { Version = version, Root = root });
                Assert.True(result.Status == BuildStatus.Completed, $"{result.Reason}: {result.Detail}");
            }
            _owners.Release(owner);
        }

        // A legacy root without tombstones, as the writer before Phase 3 made it
        public static async Task LegacyRoot(Table table, long version)
        {
            var state = (await DeltaTransactionReader.ReadTable(table.Local, "t", version))!;
            await LegacyCheckpointWriter.WriteCheckpoint(table.Local, "t", state);
        }

        // A root without a coverage marker, written from the actions it holds
        public async Task UnmarkedRoot(Table table, long version, IEnumerable<DeltaAction> actions)
        {
            var path = Path.Combine(_directory, ScratchOwners.ShortId() + ".parquet");
            using (var writer = new CheckpointFileWriter(path, new DiskLedger(), new JobMemory(), ParquetSharp.Compression.Zstd, null))
            {
                foreach (var record in actions.SelectMany(x => TestRecords.FromAction(x)))
                {
                    writer.Append(record);
                }
                writer.Close();
            }
            File.Copy(path, table.Log($"{version:D20}.checkpoint.parquet"));
            File.Delete(path);
        }

        // Removes every commit below the version, as a cleanup of older history would
        public static void Truncate(Table table, long below)
        {
            for (long version = 0; version < below; version++)
            {
                File.Delete(table.Log($"{version:D20}.json"));
            }
        }

        public static async Task<LogEvidence> Log(Table table)
        {
            var (check, log) = await ListingEvidence.ReadLogAsync(table.Configuration, table.Local, -1, default);
            Assert.Equal(ListingCheck.Passed, check);
            return log!;
        }

        // What preparation and selection would hand the planner, built by hand; the interval is the sink's default
        public static AdvanceBoundary Boundary(LogEvidence log, long o, long c, long a = 0, bool? oldestMarked = null, AdvanceStep step = AdvanceStep.Short, long cap = 20_000, int interval = 20)
        {
            var oldest = log.Listing.Checkpoints.FirstOrDefault(x => x.Version == o);
            var root = o > 0 || !log.Listing.Commits.ContainsKey(o) ? oldest : null;
            return new AdvanceBoundary("table-a", o, root, oldestMarked ?? true, c, log.Listing.Checkpoints.Single(x => x.Version == c), a, log.Head, log.Head,
                Now - TimeSpan.FromDays(1), log, step, cap, TimeSpan.FromDays(1), interval);
        }

        public static LogClassification Classes(LogEvidence log)
        {
            var names = new List<ClassifiedName>();
            for (int i = 0; i < log.Entries.Count; i++)
            {
                var entry = log.Entries[i];
                if (ListingEvidence.TryGetStageVersion(entry.Name, out var stage))
                {
                    names.Add(new ClassifiedName(entry, LogNameKind.Stage, stage, stage));
                }
                else if (DeltaTransactionReader.TryGetCommitVersion(entry.Name, out var commit))
                {
                    names.Add(new ClassifiedName(entry, LogNameKind.Commit, commit, commit));
                }
                else
                {
                    var checkpoint = log.Listing.Checkpoints.FirstOrDefault(x => x.IOEntry.Path.Full == entry.Path.Full);
                    names.Add(new ClassifiedName(entry, checkpoint != null ? LogNameKind.ClassicCheckpoint : entry.Name == "_last_checkpoint" ? LogNameKind.Hint : LogNameKind.UnknownUnversioned,
                        checkpoint?.Version ?? -1, checkpoint?.Version ?? -1));
                }
            }
            return new LogClassification() { Names = names, Stages = names.Where(x => x.Kind == LogNameKind.Stage).ToList() };
        }

        /// <summary>
        /// The emitted deletion paths of a plan's parts, table-relative and upper-cased as local disk compares them, after checking each part's bounds, order, count, length and hash.
        /// </summary>
        public static List<string> Emitted(AdvancePlan plan, AdvanceWindowLimits limits)
        {
            var emitted = new List<string>();
            var window = -1;
            var index = 0;
            for (int ordinal = 0; ordinal < plan.Parts.Count; ordinal++)
            {
                var part = plan.Parts[ordinal];
                index = part.Window == window ? index + 1 : 0;
                Assert.True(part.Window >= window, "Parts follow their windows");
                window = part.Window;
                Assert.Equal(index, part.Index);
                Assert.Equal(ordinal, part.Ordinal);
                var bytes = File.ReadAllBytes(part.ScratchPath);
                Assert.Equal(part.Bytes, bytes.Length);
                Assert.Equal(part.Sha256, SHA256.HashData(bytes));
                Assert.True(bytes.Length <= limits.PartBytes);
                var paths = PathsOf(part);
                Assert.True(paths.Count <= limits.PartPaths);
                foreach (var path in paths)
                {
                    Assert.StartsWith("/t/", path);
                    emitted.Add(Upper(path.Substring(3)));
                }
            }
            Assert.Equal(plan.Emitted, emitted.Count);
            return emitted;
        }

        /// <summary>
        /// The paths of a part record its decoder accepts, which checks their order, framing, count and checksum.
        /// </summary>
        public static List<string> PathsOf(AdvancePart part)
        {
            var bytes = File.ReadAllBytes(part.ScratchPath);
            Assert.Equal(RecordStatus.Valid, ControlRecords.ValidatePart(bytes, out var head));
            Assert.Equal((part.Ordinal, part.Paths), (head.Ordinal, (long)head.Count));
            var paths = new List<string>();
            foreach (var path in ControlRecords.Paths(bytes))
            {
                paths.Add(Encoding.UTF8.GetString(path));
            }
            Assert.Equal(head.Count, paths.Count);
            return paths;
        }

        public static string Upper(string name) => new string(name.Select(c => c is >= 'a' and <= 'z' ? (char)(c - 32) : c).ToArray());

        // Every emitted deletion path names a file of the table in its exact spelling, segment by segment
        public static void AssertEmittedExist(Table table, AdvancePlan plan)
        {
            var listings = new Dictionary<string, HashSet<string>>(StringComparer.Ordinal);
            foreach (var part in plan.Parts)
            {
                foreach (var path in PathsOf(part))
                {
                    Assert.StartsWith("/t/", path);
                    var folder = table.Native;
                    foreach (var segment in path.Substring(3).Split('/'))
                    {
                        if (!listings.TryGetValue(folder, out var names))
                        {
                            names = Directory.Exists(folder) ? Directory.GetFileSystemEntries(folder).Select(x => Path.GetFileName(x)).ToHashSet(StringComparer.Ordinal) : new HashSet<string>(StringComparer.Ordinal);
                            listings.Add(folder, names);
                        }
                        Assert.True(names.Contains(segment), $"Emitted {path} names no entry spelled {segment} in {folder}");
                        folder = Path.Combine(folder, segment);
                    }
                    Assert.True(File.Exists(folder), $"Emitted {path} is not a file");
                }
            }
        }

        // Our sink's leading commitInfo after adoption, a pre-adoption one before it
        public static Task Commit(Table table, long version, long adoptedAt, params DeltaAction[] actions)
        {
            var timestamp = BaseTimestamp + version * 1000;
            if (version >= adoptedAt)
            {
                return DeltaTestKit.WriteOwnCommit(table.Local, "t", version, adoptedAt, timestamp, actions);
            }
            var commitInfo = new DeltaAction() { CommitInfo = new DeltaCommitInfoAction() { StageId = $"stage-{version}", Timestamp = timestamp, Operation = "WRITE" } };
            return DeltaTestKit.WriteCommit(table.Local, "t", version, actions.Prepend(commitInfo).ToArray());
        }

        public static DeltaAction Create() => new DeltaAction()
        {
            Protocol = new DeltaProtocolAction() { MinReaderVersion = 3, MinWriterVersion = 7, ReaderFeatures = new List<string>() { "deletionVectors" }, WriterFeatures = new List<string>() { "deletionVectors" } }
        };

        public static DeltaAction Add(string path, DeletionVector? dv = null) => DeltaLakeCheckpointBuildTests.Add(path, dv);

        public static DeltaAction Remove(DeltaAddAction add, long version) => DeltaLakeCheckpointBuildTests.Remove(add.Path!, BaseTimestamp + version * 1000, add.DeletionVector);

        public static DeltaAction Remove(string path, long version, DeletionVector? dv = null) => DeltaLakeCheckpointBuildTests.Remove(path, BaseTimestamp + version * 1000, dv);

        public static DeltaAction Cdc(string path) => new DeltaAction() { Cdc = new DeltaCdcAction() { Path = path, PartitionValues = new Dictionary<string, string>(), Size = 10, DataChange = false } };

        public static DeletionVector Uuid(Guid id, string prefix = "", long offset = 1) => new DeletionVector() { StorageType = "u", PathOrInlineDv = prefix + Z85.EncodeGuid(id), Offset = offset, SizeInBytes = 10, Cardinality = 2 };

        // Another vector in the same vector file
        public static DeletionVector At(DeletionVector dv, long offset) => new DeletionVector() { StorageType = dv.StorageType, PathOrInlineDv = dv.PathOrInlineDv, Offset = offset, SizeInBytes = 10, Cardinality = 3 };

        /// <summary>
        /// A seeded own-writer history of inserts, copy-on-write, overwrites, deletion vector churn, deletes, CDC and, opted in, schema and transaction changes under mixed-case names; returns the state at the snapshot version as root actions.
        /// </summary>
        public static async Task<List<DeltaAction>?> WriteHistory(Table table, int seed, long versions, long adoptedAt = 0, long? snapshotAt = null, bool schemaAndTxn = false)
        {
            var random = new Random(seed);
            // Vector offsets, shared vector files and action order, drawn apart from the operations
            var vectors = new Random(seed * 31 + 7);
            var changes = schemaAndTxn ? new Random(seed * 131 + 17) : null;
            var live = new List<DeltaAddAction>();
            // Removed file keys and their tombstones, newest last
            var tombstones = new Dictionary<string, DeltaRemoveFileAction>(StringComparer.Ordinal);
            // The newest transaction per application
            var transactions = new SortedDictionary<string, DeltaTransactionAction>(StringComparer.Ordinal);
            var metadata = DeltaTestKit.Metadata(DeltaTestKit.UserSchema);
            metadata.MetaData!.Id = "table-a";
            List<DeltaAction>? snapshot = null;
            for (long version = 0; version < versions; version++)
            {
                var actions = new List<DeltaAction>();
                if (version == 0)
                {
                    actions.Add(Create());
                    actions.Add(metadata);
                }
                void Insert(string name, DeletionVector? dv = null)
                {
                    var add = Add(name, dv).Add!;
                    live.Add(add);
                    tombstones.Remove(FileKey(add.Path!, add.DeletionVector));
                    actions.Add(new DeltaAction() { Add = add });
                }
                void Retire(DeltaAddAction victim)
                {
                    live.Remove(victim);
                    var remove = Remove(victim, version);
                    tombstones[FileKey(victim.Path!, victim.DeletionVector)] = remove.Remove!;
                    actions.Add(remove);
                }
                var operation = live.Count == 0 ? 0 : random.Next(20) switch { < 7 => 0, < 10 => 3, 10 => 5, < 15 => 6, < 17 => 8, _ => 9 };
                switch (operation)
                {
                    case 0:
                        {
                            DeletionVector? shared = null;
                            for (int i = random.Next(1, 4); i > 0; i--)
                            {
                                var dv = random.Next(5) == 0 ? Uuid(NextGuid(random), offset: vectors.Next(1, 1000)) : null;
                                // Some files of one commit share a vector file
                                if (dv != null && shared != null && vectors.Next(2) == 0)
                                {
                                    dv = At(shared, shared.Offset!.Value + 10 * i);
                                }
                                shared ??= dv;
                                Insert($"part-{version:D6}-{i}.parquet", dv);
                            }
                            break;
                        }
                    case 3:
                        // Copy-on-write
                        Retire(live[random.Next(live.Count)]);
                        Insert($"Part-{version:D6}-Cow.parquet");
                        break;
                    case 5:
                        // Overwrite
                        foreach (var victim in live.ToList())
                        {
                            Retire(victim);
                        }
                        Insert($"PART-{version:D6}-OVERWRITE.parquet");
                        break;
                    case 6:
                        {
                            // A new deletion vector on the same file, in a new vector file or its old one at another offset, sometimes added first
                            var victim = live[random.Next(live.Count)];
                            var dv = Uuid(NextGuid(random), random.Next(3) == 0 ? "dv" : "", vectors.Next(1, 1000));
                            if (victim.DeletionVector is { } old && vectors.Next(2) == 0)
                            {
                                dv = At(old, old.Offset!.Value + 1000);
                            }
                            if (vectors.Next(2) == 0)
                            {
                                Insert(victim.Path!, dv);
                                Retire(victim);
                            }
                            else
                            {
                                Retire(victim);
                                Insert(victim.Path!, dv);
                            }
                            break;
                        }
                    case 8:
                        // A delete with its change data
                        Retire(live[random.Next(live.Count)]);
                        actions.Add(Cdc($"_change_data/Cdc-{version:D6}.parquet"));
                        break;
                    default:
                        Insert($"part-{version:D6}-cdf.parquet");
                        actions.Add(Cdc($"_change_data/Cdc-{version:D6}.parquet"));
                        break;
                }
                if (changes != null && version > 0)
                {
                    if (changes.Next(4) == 0)
                    {
                        metadata = Evolved(metadata.MetaData!, version);
                        actions.Add(metadata);
                    }
                    if (changes.Next(3) == 0)
                    {
                        var txn = new DeltaTransactionAction() { AppId = $"app-{changes.Next(3)}", Version = version, LastUpdated = BaseTimestamp + version * 1000 };
                        transactions[txn.AppId] = txn;
                        actions.Add(new DeltaAction() { Txn = txn });
                    }
                }
                await Commit(table, version, adoptedAt, actions.ToArray());
                if (version == snapshotAt)
                {
                    snapshot = new List<DeltaAction>() { Create(), metadata };
                    snapshot.AddRange(live.Select(x => new DeltaAction() { Add = x }));
                    snapshot.AddRange(tombstones.Values.Select(x => new DeltaAction() { Remove = x }));
                    snapshot.AddRange(transactions.Values.Select(x => new DeltaAction() { Txn = x }));
                }
            }
            return snapshot;
        }

        // The metadata with one more nullable column and the version as a configuration value
        private static DeltaAction Evolved(DeltaMetadataAction current, long version)
        {
            var schema = current.SchemaString!;
            var configuration = new Dictionary<string, string>(current.Configuration ?? new Dictionary<string, string>()) { ["flowtide.test.generation"] = $"{version}" };
            return new DeltaAction()
            {
                MetaData = new DeltaMetadataAction()
                {
                    Id = current.Id,
                    Name = current.Name,
                    Description = current.Description,
                    Format = current.Format,
                    SchemaString = schema.Substring(0, schema.Length - 2) + $",{{\"name\":\"extra_{version}\",\"type\":\"string\",\"nullable\":true,\"metadata\":{{}}}}]}}",
                    PartitionColumns = current.PartitionColumns,
                    CreatedTime = current.CreatedTime,
                    Configuration = configuration
                }
            };
        }

        private static string FileKey(string path, DeletionVector? dv) => $"{path}|{dv?.StorageType}{dv?.PathOrInlineDv}@{dv?.Offset}";

        /// <summary>
        /// The manual fixture: an upgraded table whose first long advance plans more than 100,000 versions in default windows, checked against the oracle.
        /// </summary>
        public static async Task<int> RunFixtureAsync(string directory, long versions, TextWriter output)
        {
            using var kit = new AdvancePlannerKit(Path.Combine(directory, "advance-" + ScratchOwners.ShortId()));
            var table = kit.Local();
            var adoptedAt = versions - 40;
            var c = versions - 30;
            var watch = Stopwatch.StartNew();
            await WriteHistory(table, 1, versions, adoptedAt);
            output.WriteLine($"Wrote {versions} commits, adoption at {adoptedAt}, in {watch.Elapsed}");
            watch.Restart();
            await kit.Build(table, c);
            await kit.Build(table, versions - 10);
            output.WriteLine($"Built checkpoints {c} and {versions - 10} in {watch.Elapsed}");
            var log = await Log(table);
            var boundary = Boundary(log, 0, c, a: adoptedAt, step: AdvanceStep.Long);
            var context = kit.Context(true, boundary.V);
            watch.Restart();
            using var plan = await AdvancePlanner.PlanAsync(table.Configuration, table.Local, table.Configuration.Store.Table, boundary, Classes(log), context);
            output.WriteLine($"Plan {plan.Status} {plan.Reason} {plan.Detail}: {plan.Parts.Select(x => x.Window).Distinct().Count()} windows, {plan.Parts.Count} parts, {plan.Candidates} candidates, "
                + $"{plan.Protected} protected, {plan.Kept} kept, {plan.Emitted} emitted, {context.ReadsUsed} reads, {context.Memory.ManagedPeak} managed peak bytes, in {watch.Elapsed}");
            if (plan.Status != AdvancePlanStatus.Planned)
            {
                return 1;
            }
            var emitted = new SortedSet<string>(Emitted(plan, AdvanceWindowLimits.Default), StringComparer.Ordinal);
            var oracle = await RetainedReferenceOracle.ComputeAsync(table.Native, 0, false, false, c, log.Head, Classes(log).Stages.Select(x => x.Entry.Name));
            var retired = oracle.Retired;
            if (!retired.SetEquals(emitted))
            {
                output.WriteLine($"MISMATCH: {retired.Except(emitted).Count()} retired files not emitted, {emitted.Except(retired).Count()} emitted files not retired");
                return 1;
            }
            output.WriteLine($"Plan equals the oracle: {retired.Count} retired files");
            return 0;
        }

        private static Guid NextGuid(Random random)
        {
            var bytes = new byte[16];
            random.NextBytes(bytes);
            return new Guid(bytes);
        }
    }
}
