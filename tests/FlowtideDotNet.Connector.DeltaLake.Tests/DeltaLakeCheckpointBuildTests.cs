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
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat.Bounded;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance;
using Microsoft.Extensions.Time.Testing;
using Stowage;
using System.Text.Json;
using Xunit.Abstractions;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public partial class DeltaLakeCheckpointBuildTests : IDisposable
    {
        private static readonly DateTimeOffset s_now = new DateTimeOffset(2026, 10, 1, 12, 0, 0, TimeSpan.Zero);
        private readonly string _directory;
        private readonly ITestOutputHelper _output;

        public DeltaLakeCheckpointBuildTests(ITestOutputHelper output)
        {
            _output = output;
            _directory = Path.Combine(Path.GetTempPath(), "bt-" + ScratchOwners.ShortId());
            Directory.CreateDirectory(_directory);
        }

        private readonly ScratchOwners _owners = new ScratchOwners();

        public void Dispose()
        {
            _owners.Release();
            try
            {
                Directory.Delete(_directory, true);
            }
            catch (IOException)
            {
            }
        }

        [Fact]
        public async Task DisposeRemovesEveryBuildsScratchFolder()
        {
            var storage = Memory(nameof(DisposeRemovesEveryBuildsScratchFolder));
            await DeltaTestKit.WriteCommit(storage, "t", 0, DeltaTestKit.Protocol(), DeltaTestKit.Metadata(DeltaTestKit.UserSchema));
            var built = await Build(storage, "t", 0);
            Assert.Equal(BuildStatus.Completed, built.Result.Status);

            Dispose();
            Assert.False(Directory.Exists(_directory));
        }

        [Fact]
        public void DisposeReleasesScratchOwnersRetainedUntilTeardown()
        {
            var owner = _owners.Add(new ScratchDirectory(Path.Combine(_directory, ScratchOwners.ShortId())));
            Assert.True(owner.LockHeldForTests);
            try
            {
                Dispose();
                Assert.False(owner.LockHeldForTests);
                Assert.False(Directory.Exists(_directory));
            }
            finally
            {
                owner.ReleaseLockForTests();
            }
        }

        internal sealed record Built(BuildResult Result, List<string> Lines, List<byte[]> Records, string? Marker)
        {
            // The output's length on disk before the scratch folder went
            public long OutputLength { get; init; }
        }

        // Builds into a fresh scratch folder, reads the output back, and checks every ledger is empty afterwards
        private async Task<Built> Build(IFileStorage storage, string table, long version, RootPolicy root = RootPolicy.Auto, long arenaCap = TailReducer.DefaultArenaCap,
            bool rebuildLegacy = false, IReadOnlySet<ObjectIdentity>? unsupported = null, Action<string>? damage = null, JobMemory? memory = null, int readAhead = 1, TransferAllowance? transfer = null)
        {
            var directory = _owners.Add(new ScratchDirectory(Path.Combine(_directory, ScratchOwners.ShortId())));
            memory ??= new JobMemory();
            BuildResult result;
            var lines = new List<string>();
            var records = new List<byte[]>();
            string? marker = null;
            long outputLength = 0;
            using (var scratch = directory.CreateJob())
            {
                var context = new CheckpointBuildContext()
                {
                    Storage = storage,
                    Table = table,
                    Scratch = scratch,
                    Memory = memory,
                    Clock = new FakeTimeProvider(s_now),
                    ArenaCap = arenaCap,
                    RebuildLegacyCheckpoints = rebuildLegacy,
                    BeforeValidationForTests = damage,
                    Publish = false,
                    ReadAhead = readAhead,
                    Transfer = transfer
                };
                result = await CheckpointBuilder.BuildAsync(context, new BuildRequest() { Version = version, Root = root, KnownUnsupported = unsupported ?? new HashSet<ObjectIdentity>() });
                if (result.Checkpoint != null)
                {
                    outputLength = new FileInfo(result.Checkpoint.LocalPath).Length;
                    var scan = CheckpointPageScan.Scan(result.Checkpoint.LocalPath, memory);
                    marker = scan.Footer.Marker;
                    using (var reader = new BoundedCheckpointReader(result.Checkpoint.LocalPath, scan, memory))
                    {
                        reader.Read(record => records.Add(record.ToArray()));
                    }
                    memory.Release(scan.ModelBytes);
                    lines = CheckpointOracle.Canonical(records.Select(x => CheckpointRecord.ToAction(x)));
                }
            }
            Assert.Equal(0, memory.Managed);
            Assert.Equal(0, memory.Native);
            Assert.Equal(0, directory.Ledger.Reserved);
            _owners.Release(directory);
            return new Built(result, lines, records, marker) { OutputLength = outputLength };
        }

        internal static DeltaAction Add(string path, DeletionVector? dv = null, long records = 10) => new DeltaAction()
        {
            Add = new DeltaAddAction()
            {
                Path = path,
                PartitionValues = new Dictionary<string, string>(),
                Size = 100,
                ModificationTime = 1,
                DataChange = true,
                Statistics = $"{{\"numRecords\":{records}}}",
                DeletionVector = dv
            }
        };

        internal static DeltaAction Remove(string path, long? deleted, DeletionVector? dv = null) => new DeltaAction()
        {
            Remove = new DeltaRemoveFileAction() { Path = path, DeletionTimestamp = deleted, DataChange = true, DeletionVector = dv }
        };

        internal static DeletionVector Dv(string id) => new DeletionVector() { StorageType = "u", PathOrInlineDv = id, Offset = 1, SizeInBytes = 10, Cardinality = 2 };

        internal static DeltaAction CommitInfo(long timestamp) => new DeltaAction() { CommitInfo = new DeltaCommitInfoAction() { Data = new Dictionary<string, object>() { ["timestamp"] = timestamp } } };

        /// <summary>
        /// A seeded history: adds with and without DVs, removes on both sides of the retention cutoff, txns, domains, metadata changes.
        /// </summary>
        internal static async Task WriteHistory(IFileStorage storage, string table, int seed, int commits, int files)
        {
            var random = new Random(seed);
            var live = new List<DeltaAddAction>();
            var expired = s_now.ToUnixTimeMilliseconds() - (long)TimeSpan.FromDays(8).TotalMilliseconds;
            var recent = s_now.ToUnixTimeMilliseconds() - 1000;
            for (int version = 0; version < commits; version++)
            {
                var actions = new List<DeltaAction>() { CommitInfo(s_now.ToUnixTimeMilliseconds() - (commits - version) * 1000L) };
                if (version == 0)
                {
                    actions.Add(DeltaTestKit.ProtocolWith("deletionVectors", "domainMetadata"));
                    actions.Add(DeltaTestKit.Metadata(DeltaTestKit.UserSchema));
                }
                var count = random.Next(1, 6);
                for (int i = 0; i < count; i++)
                {
                    switch (random.Next(9))
                    {
                        case 0:
                        case 1:
                        case 2:
                            {
                                var add = Add($"f{random.Next(files)}.parquet", random.Next(4) == 0 ? Dv($"dv{random.Next(1000)}") : null).Add!;
                                live.Add(add);
                                actions.Add(new DeltaAction() { Add = add });
                                break;
                            }
                        case 3:
                        case 4:
                            if (live.Count > 0)
                            {
                                var victim = live[random.Next(live.Count)];
                                live.Remove(victim);
                                var timestamp = random.Next(3) switch { 0 => expired, 1 => recent, _ => (long?)null };
                                actions.Add(Remove(victim.Path!, timestamp, victim.DeletionVector));
                            }
                            break;
                        case 5:
                            // A deletion vector update: the old file key goes, the same path with a new vector comes
                            if (live.Count > 0)
                            {
                                var victim = live[random.Next(live.Count)];
                                live.Remove(victim);
                                actions.Add(Remove(victim.Path!, recent, victim.DeletionVector));
                                var updated = Add(victim.Path!, Dv($"dv{random.Next(1000)}")).Add!;
                                live.Add(updated);
                                actions.Add(new DeltaAction() { Add = updated });
                            }
                            break;
                        case 6:
                            actions.Add(new DeltaAction() { Txn = new DeltaTransactionAction() { AppId = $"app{random.Next(3)}", Version = random.Next(100), LastUpdated = random.Next(2) == 0 ? null : random.Next() } });
                            break;
                        case 7:
                            actions.Add(new DeltaAction() { DomainMetadata = new DeltaDomainMetadataAction() { Domain = $"domain{random.Next(3)}", Configuration = $"c{random.Next(100)}", Removed = random.Next(3) == 0 } });
                            break;
                        default:
                            if (version > 0 && random.Next(3) == 0)
                            {
                                var configuration = new Dictionary<string, string>() { ["delta.deletedFileRetentionDuration"] = random.Next(2) == 0 ? "interval 7 days" : "interval 30 days" };
                                actions.Add(DeltaTestKit.Metadata(DeltaTestKit.UserSchema, configuration));
                            }
                            break;
                    }
                }
                await DeltaTestKit.WriteCommit(storage, table, version, actions.ToArray());
            }
        }

        private static IFileStorage Memory(string name) => Files.Of.InternalMemory($"./build-{name}-{Guid.NewGuid():N}");

        [Theory]
        [InlineData(1)]
        [InlineData(2)]
        [InlineData(3)]
        public async Task BuildEqualsFullHistoryOracle(int seed)
        {
            var storage = Memory(nameof(BuildEqualsFullHistoryOracle));
            await WriteHistory(storage, "t", seed, 25, 40);
            var built = await Build(storage, "t", 24);

            Assert.Equal(BuildStatus.Completed, built.Result.Status);
            Assert.Equal(BuildMode.Probe, built.Result.Mode);
            Assert.Equal(await CheckpointOracle.FullHistory(storage, "t", 24, s_now), built.Lines);
        }

        [Fact]
        public async Task RandomSequencesMatchOracleInBothModes()
        {
            // Seeds share nothing, so they run in parallel; most of a seed's time is file churn from the tiny-arena runs
            await Parallel.ForEachAsync(Enumerable.Range(0, 1000), new ParallelOptions() { MaxDegreeOfParallelism = Math.Max(2, Environment.ProcessorCount / 2) }, async (seed, _) =>
            {
                var storage = Memory(nameof(RandomSequencesMatchOracleInBothModes));
                var random = new Random(seed);
                var commits = random.Next(1, 31);
                await WriteHistory(storage, "t", seed, commits, random.Next(1, 51));
                var target = commits - 1;
                var expected = await CheckpointOracle.FullHistory(storage, "t", target, s_now);
                // A third of the seeds build over a root of ours, a third over a legacy one, at the middle of the history
                var rootKind = commits >= 3 ? seed % 3 : 0;
                var middle = target / 2;
                if (rootKind == 1)
                {
                    var own = await Build(storage, "t", middle);
                    await OwnRoot(storage, "t", middle, own.Records, own.Marker);
                    // A retention that grows after the root cannot bring back what the root dropped, so the oracle starts from the root's own rows
                    expected = await CheckpointOracle.RootPlusTail(own.Records.Select(x => CheckpointRecord.ToAction(x)), storage, "t", middle, target, s_now);
                }
                else if (rootKind == 2)
                {
                    var legacy = await LegacyRoot(storage, "t", middle);
                    var rootActions = new List<DeltaAction>() { new DeltaAction() { Protocol = legacy.Protocol }, new DeltaAction() { MetaData = legacy.Metadata } };
                    rootActions.AddRange(legacy.AddFiles.Select(x => new DeltaAction() { Add = x }));
                    expected = await CheckpointOracle.RootPlusTail(rootActions, storage, "t", middle, target, s_now);
                }

                var probe = await Build(storage, "t", target);
                Assert.True(probe.Result.Status == BuildStatus.Completed, $"seed {seed}: {probe.Result.Reason} {probe.Result.Detail}");
                AssertLines(expected, probe.Lines, $"seed {seed}, root kind {rootKind}, probe");

                // An arena of one chunk spills almost every record into its own run, with merge passes
                var sorted = await Build(storage, "t", target, arenaCap: 4096);
                Assert.True(sorted.Result.Status == BuildStatus.Completed, $"seed {seed}: {sorted.Result.Reason} {sorted.Result.Detail}");
                // Over a root of ours a short tail can fit even a tiny arena; longer ones overflow it
                Assert.True(rootKind == 1 || sorted.Result.Mode == BuildMode.Sort, $"seed {seed}: {sorted.Result.Mode}");
                AssertLines(expected, sorted.Lines, $"seed {seed}, root kind {rootKind}, sort");

                // A bigger arena holds several entries per run, so a later entry replaces an earlier one inside the map between spills
                var batched = await Build(storage, "t", target, arenaCap: 16 << 10);
                Assert.True(batched.Result.Status == BuildStatus.Completed, $"seed {seed}: {batched.Result.Reason} {batched.Result.Detail}");
                AssertLines(expected, batched.Lines, $"seed {seed}, root kind {rootKind}, sort with a 16 KiB arena");

                if (rootKind != 0)
                {
                    Assert.Equal(middle, probe.Result.RootVersion);
                    return;
                }
                // The reader's own replay agrees once its tombstones are expired the same way
                var table = (await DeltaTransactionReader.ReadTable(storage, "t", options: new DeltaReadOptions() { SkipTombstones = false }))!;
                var cutoff = CheckpointExpiry.CutoffTicks(s_now, CheckpointExpiry.Retention(table.Metadata.Configuration));
                var replayed = new List<DeltaAction>() { new DeltaAction() { Protocol = table.Protocol }, new DeltaAction() { MetaData = table.Metadata } };
                replayed.AddRange(table.AddFiles.Select(x => new DeltaAction() { Add = x }));
                replayed.AddRange(table.Tombstones.Where(x => !CheckpointExpiry.Expired(x.DeletionTimestamp, cutoff)).Select(x => new DeltaAction() { Remove = x }));
                replayed.AddRange(table.Transactions.Values.Select(x => new DeltaAction() { Txn = x }));
                replayed.AddRange(table.DomainMetadata.Values.Select(x => new DeltaAction() { DomainMetadata = x }));
                Assert.Equal(expected, CheckpointOracle.Canonical(replayed));
            });
        }

        // Names the case and shows only the lines that differ
        private static void AssertLines(List<string> expected, List<string> actual, string what)
        {
            if (!expected.SequenceEqual(actual))
            {
                Assert.Fail($"{what}: only expected {string.Join(" | ", expected.Except(actual))}; only actual {string.Join(" | ", actual.Except(expected))}");
            }
        }

        [Fact]
        public async Task TinyArenaForcesSortModeWithIdenticalContent()
        {
            var storage = Memory(nameof(TinyArenaForcesSortModeWithIdenticalContent));
            await WriteHistory(storage, "t", 11, 30, 50);
            var probe = await Build(storage, "t", 29);
            var sorted = await Build(storage, "t", 29, arenaCap: 4096);

            Assert.Equal(BuildMode.Probe, probe.Result.Mode);
            Assert.Equal(BuildMode.Sort, sorted.Result.Mode);
            Assert.True(sorted.Result.SpillBytes > 0);
            Assert.Equal(probe.Lines, sorted.Lines);
            Assert.Equal(probe.Result.Checkpoint!.Actions, sorted.Result.Checkpoint!.Actions);
        }

        [Fact]
        public async Task SpillBytesCountEveryRunWritten()
        {
            var storage = Memory(nameof(SpillBytesCountEveryRunWritten));
            await DeltaTestKit.WriteCommit(storage, "t", 0, DeltaTestKit.Protocol(), DeltaTestKit.Metadata(DeltaTestKit.UserSchema));
            for (int version = 1; version <= 5; version++)
            {
                await DeltaTestKit.WriteCommit(storage, "t", version, Enumerable.Range(0, 200).Select(i => Add($"v{version}-{i}-{new string('p', 100)}.parquet")).ToArray());
            }
            // Runs whose deletion fails stay on disk, so every run the build wrote can be measured afterwards
            var directory = _owners.Add(new ScratchDirectory(Path.Combine(_directory, ScratchOwners.ShortId())) { DeleteFailsForTests = path => path.EndsWith(".run") });
            BuildResult result;
            using (var scratch = directory.CreateJob())
            {
                result = await CheckpointBuilder.BuildAsync(new CheckpointBuildContext()
                {
                    Storage = storage,
                    Table = "t",
                    Scratch = scratch,
                    Memory = new JobMemory(),
                    Clock = new FakeTimeProvider(s_now),
                    ArenaCap = 16L << 10,
                    Publish = false
                }, new BuildRequest() { Version = 5 });
            }

            Assert.Equal(BuildStatus.Completed, result.Status);
            Assert.Equal(BuildMode.Sort, result.Mode);
            var runs = Directory.GetFiles(directory.ProcessPath, "*.run", SearchOption.AllDirectories);
            // More runs than the fan-in, so a merge pass wrote some, and the first is the tail map drained on overflow
            Assert.True(runs.Length > CheckpointSorter.FanIn + 1, $"{runs.Length} runs");
            Assert.True(new FileInfo(runs.Min()!).Length > 0);
            Assert.Equal(runs.Sum(x => new FileInfo(x).Length), result.SpillBytes);
        }

        // A fault before an intermediate merge, or a cancellation inside one, leaves no run, charge or memory behind
        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task AFailedIntermediateMergeLeavesNothingBehind(bool cancel)
        {
            var storage = Memory(nameof(AFailedIntermediateMergeLeavesNothingBehind));
            await DeltaTestKit.WriteCommit(storage, "t", 0, DeltaTestKit.Protocol(), DeltaTestKit.Metadata(DeltaTestKit.UserSchema));
            for (int version = 1; version <= 5; version++)
            {
                await DeltaTestKit.WriteCommit(storage, "t", version, Enumerable.Range(0, 2000).Select(i => Add($"v{version}-{i}-{new string('p', 100)}.parquet")).ToArray());
            }
            var directory = _owners.Add(new ScratchDirectory(Path.Combine(_directory, ScratchOwners.ShortId())));
            var memory = new JobMemory();
            using var cancellation = new CancellationTokenSource();
            var merges = 0;
            var runsAtFault = 0;
            string jobFolder;
            Exception? error = null;
            using (var scratch = directory.CreateJob())
            {
                jobFolder = scratch.DirectoryPath;
                try
                {
                    await CheckpointBuilder.BuildAsync(new CheckpointBuildContext()
                    {
                        Storage = storage,
                        Table = "t",
                        Scratch = scratch,
                        Memory = memory,
                        Clock = new FakeTimeProvider(s_now),
                        ArenaCap = 64L << 10,
                        Publish = false,
                        BuildFaultForTests = point =>
                        {
                            // The first pass's second merge, after the first merged run replaced its inputs
                            if (point == "merge" && ++merges == 2)
                            {
                                runsAtFault = Directory.GetFiles(scratch.DirectoryPath, "*.run").Length;
                                if (!cancel)
                                {
                                    throw new IOException("merge fault");
                                }
                                cancellation.Cancel();
                            }
                        }
                    }, new BuildRequest() { Version = 5 }, cancellation.Token);
                }
                catch (Exception e)
                {
                    error = e;
                }
            }

            if (cancel)
            {
                Assert.IsAssignableFrom<OperationCanceledException>(error);
            }
            else
            {
                Assert.Equal("merge fault", error?.Message);
            }
            // More runs than the fan-in remained, so the failed merge was not the final one; a cancellation stopped it midway
            Assert.True(runsAtFault > CheckpointSorter.FanIn, $"{runsAtFault} runs");
            Assert.Equal(2, merges);
            Assert.Equal(0, directory.Ledger.Reserved);
            Assert.Equal(0, directory.PendingCount);
            Assert.False(Directory.Exists(jobFolder));
            Assert.Equal(0, memory.Managed);
            Assert.Equal(0, memory.Native);
        }

        // A root of ours stays in probe mode with the default arena; a legacy root is always sorted
        [Theory]
        [InlineData(TailReducer.DefaultArenaCap, false)]
        [InlineData(TailReducer.DefaultArenaCap, true)]
        [InlineData(4096L, false)]
        [InlineData(4096L, true)]
        public async Task ProtocolAndMetadataAreWrittenExactlyOnce(long arenaCap, bool ownRoot)
        {
            var storage = Memory(nameof(ProtocolAndMetadataAreWrittenExactlyOnce));
            await WriteHistory(storage, "t", 5, 10, 10);
            // Both again in a later commit, and a root holding both too
            await DeltaTestKit.WriteCommit(storage, "t", 10, DeltaTestKit.ProtocolWith("deletionVectors", "domainMetadata"), DeltaTestKit.Metadata(DeltaTestKit.UserSchema));
            await RootAt(storage, 10, ownRoot);
            await DeltaTestKit.WriteCommit(storage, "t", 11, Add("late.parquet"), DeltaTestKit.Metadata(DeltaTestKit.UserSchema, new Dictionary<string, string>() { ["k"] = "v" }));
            var built = await Build(storage, "t", 11, arenaCap: arenaCap);

            Assert.Equal(1, built.Records.Count(x => CheckpointRecord.Kind(x) == RecordKind.Protocol));
            var metaData = Assert.Single(built.Records, x => CheckpointRecord.Kind(x) == RecordKind.MetaData);
            Assert.Equal("v", CheckpointRecord.ToAction(metaData).MetaData!.Configuration!["k"]);
        }

        [Fact]
        public async Task SortModeOutputKeysAreStrictlyIncreasing()
        {
            var storage = Memory(nameof(SortModeOutputKeysAreStrictlyIncreasing));
            await WriteHistory(storage, "t", 7, 30, 50);
            var built = await Build(storage, "t", 29, arenaCap: 4096);

            var keys = built.Records.Select(x => TestRecords.Key(x)).ToList();
            for (int i = 1; i < keys.Count; i++)
            {
                Assert.True(keys[i - 1].AsSpan().SequenceCompareTo(keys[i]) < 0);
            }
            Assert.Equal(RecordKind.Protocol, CheckpointRecord.Kind(built.Records[0]));
            Assert.Equal(RecordKind.MetaData, CheckpointRecord.Kind(built.Records[1]));
            Assert.True(CoverageMarker.Parse(built.Marker)!.Sorted);
        }

        [Fact]
        public async Task ProbeOutputIsMarkedUnsorted()
        {
            var storage = Memory(nameof(ProbeOutputIsMarkedUnsorted));
            await WriteHistory(storage, "t", 8, 5, 5);
            var built = await Build(storage, "t", 4);

            Assert.Equal(BuildMode.Probe, built.Result.Mode);
            Assert.False(CoverageMarker.Parse(built.Marker)!.Sorted);
        }

        [Theory]
        [InlineData(TailReducer.DefaultArenaCap)]
        [InlineData(4096L)]
        public async Task RemovedDomainsAreNotWritten(long arenaCap)
        {
            var storage = Memory(nameof(RemovedDomainsAreNotWritten));
            await DeltaTestKit.WriteCommit(storage, "t", 0, DeltaTestKit.ProtocolWith("domainMetadata"), DeltaTestKit.Metadata(DeltaTestKit.UserSchema),
                new DeltaAction() { DomainMetadata = new DeltaDomainMetadataAction() { Domain = "kept", Configuration = "a" } },
                new DeltaAction() { DomainMetadata = new DeltaDomainMetadataAction() { Domain = "gone", Configuration = "b" } });
            await DeltaTestKit.WriteCommit(storage, "t", 1, new DeltaAction() { DomainMetadata = new DeltaDomainMetadataAction() { Domain = "gone", Configuration = "b", Removed = true } });
            var built = await Build(storage, "t", 1, arenaCap: arenaCap);

            var domains = built.Records.Where(x => CheckpointRecord.Kind(x) == RecordKind.Domain).Select(x => CheckpointRecord.ToAction(x).DomainMetadata!).ToList();
            Assert.Equal("kept", Assert.Single(domains).Domain);
        }

        [Theory]
        [InlineData(TailReducer.DefaultArenaCap, false)]
        [InlineData(TailReducer.DefaultArenaCap, true)]
        [InlineData(4096L, false)]
        [InlineData(4096L, true)]
        public async Task ExpiredTombstoneSupersedesAnOlderAdd(long arenaCap, bool ownRoot)
        {
            var storage = Memory(nameof(ExpiredTombstoneSupersedesAnOlderAdd));
            var expired = s_now.ToUnixTimeMilliseconds() - (long)TimeSpan.FromDays(8).TotalMilliseconds;
            await DeltaTestKit.WriteCommit(storage, "t", 0, DeltaTestKit.Protocol(), DeltaTestKit.Metadata(DeltaTestKit.UserSchema), Add("a.parquet"), Add("b.parquet"));
            await RootAt(storage, 0, ownRoot);
            // A second tail record overflows the 4 KiB arena, so those rows sort over either root
            await DeltaTestKit.WriteCommit(storage, "t", 1, Remove("a.parquet", expired), Remove("gone.parquet", expired));
            var built = await Build(storage, "t", 1, arenaCap: arenaCap);

            if (arenaCap == 4096)
            {
                Assert.Equal(BuildMode.Sort, built.Result.Mode);
            }
            // The expired tombstones are not written, and the root's add one removed is not either
            Assert.Equal(0, built.Result.RootVersion);
            Assert.Equal(new[] { "b.parquet" }, built.Records.Where(x => CheckpointRecord.Kind(x) is RecordKind.Add or RecordKind.Remove).Select(x => CheckpointRecord.ToAction(x).Add?.Path ?? "remove"));
        }

        // Our own marked checkpoint at the version, or a legacy one
        private async Task RootAt(IFileStorage storage, long version, bool own)
        {
            if (!own)
            {
                await LegacyRoot(storage, "t", version);
                return;
            }
            var root = await Build(storage, "t", version);
            await OwnRoot(storage, "t", version, root.Records, root.Marker);
        }

        // A legacy checkpoint at the version: adds, protocol and metaData only, no marker
        internal static async Task<DeltaTable> LegacyRoot(IFileStorage storage, string table, long version)
        {
            var state = (await DeltaTransactionReader.ReadTable(storage, table, version))!;
            await LegacyCheckpointWriter.WriteCheckpoint(storage, table, state);
            return state;
        }

        [Theory]
        [InlineData(TailReducer.DefaultArenaCap)]
        [InlineData(4096L)]
        public async Task BuildOverLegacyRootEqualsRootPlusTailOracle(long arenaCap)
        {
            var storage = Memory(nameof(BuildOverLegacyRootEqualsRootPlusTailOracle));
            await WriteHistory(storage, "t", 21, 20, 30);
            // The state the root was written from: the Arrow reader would read its null maps back as empty ones
            var root = await LegacyRoot(storage, "t", 12);
            var built = await Build(storage, "t", 19, arenaCap: arenaCap);

            Assert.Equal(12, built.Result.RootVersion);
            var rootActions = new List<DeltaAction>() { new DeltaAction() { Protocol = root.Protocol }, new DeltaAction() { MetaData = root.Metadata } };
            rootActions.AddRange(root.AddFiles.Select(x => new DeltaAction() { Add = x }));
            var expectedLines = await CheckpointOracle.RootPlusTail(rootActions, storage, "t", 12, 19, s_now);
            if (!expectedLines.SequenceEqual(built.Lines))
            {
                Assert.Fail("ONLY EXPECTED: " + string.Join(" | ", expectedLines.Except(built.Lines)) + " ONLY ACTUAL: " + string.Join(" | ", built.Lines.Except(expectedLines)));
            }
            // The legacy root is unmarked, so the gap it leaves is recorded
            var marker = CoverageMarker.Parse(built.Marker)!;
            Assert.Equal(12, marker.Inherited!.AtOrBelow);
        }

        [Fact]
        public async Task FirstCheckpointReplaysFromZeroWithoutTheOption()
        {
            var storage = Memory(nameof(FirstCheckpointReplaysFromZeroWithoutTheOption));
            await WriteHistory(storage, "t", 31, 5, 5);
            var built = await Build(storage, "t", 4);

            Assert.Equal(BuildStatus.Completed, built.Result.Status);
            Assert.Null(built.Result.RootVersion);
            Assert.Null(CoverageMarker.Parse(built.Marker)!.Inherited);
        }

        [Fact]
        public async Task ExactBuildAtVersionZeroReplaysCommitZero()
        {
            var storage = Memory(nameof(ExactBuildAtVersionZeroReplaysCommitZero));
            await WriteHistory(storage, "t", 32, 3, 5);
            var built = await Build(storage, "t", 0);

            Assert.Equal(BuildStatus.Completed, built.Result.Status);
            Assert.Equal(await CheckpointOracle.FullHistory(storage, "t", 0, s_now), built.Lines);
        }

        [Fact]
        public async Task TornOnlyRootsReplayFromZero()
        {
            var storage = Memory(nameof(TornOnlyRootsReplayFromZero));
            await WriteHistory(storage, "t", 33, 8, 8);
            await LegacyRoot(storage, "t", 5);
            var path = $"/t/_delta_log/{5:D20}.checkpoint.parquet";
            var bytes = await DeltaTestKit.ReadBytes(storage, path);
            await storage.WriteText(path, "torn");
            var built = await Build(storage, "t", 7);

            Assert.Equal(BuildStatus.Completed, built.Result.Status);
            Assert.Null(built.Result.RootVersion);
            Assert.Empty(built.Result.Skipped);
            Assert.Equal(await CheckpointOracle.FullHistory(storage, "t", 7, s_now), built.Lines);
        }

        [Fact]
        public async Task MissingCommitNeverBecomesEmptyInput()
        {
            var storage = Memory(nameof(MissingCommitNeverBecomesEmptyInput));
            await WriteHistory(storage, "t", 34, 6, 6);
            await storage.Rm($"/t/_delta_log/{3:D20}.json");
            var built = await Build(storage, "t", 5);

            Assert.Equal(BuildStatus.Declined, built.Result.Status);
            Assert.Equal(MaintenanceReason.MissingHistory, built.Result.Reason);
            Assert.Equal(DeclineDependency.None, built.Result.Dependency);
        }

        [Fact]
        public async Task TargetAboveTheHeadFails()
        {
            var storage = Memory(nameof(TargetAboveTheHeadFails));
            await WriteHistory(storage, "t", 35, 3, 3);
            var built = await Build(storage, "t", 3);

            Assert.Equal(BuildStatus.Failed, built.Result.Status);
            Assert.Equal(MaintenanceReason.TargetNotInLog, built.Result.Reason);
        }
    }
}
