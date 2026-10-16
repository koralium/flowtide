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
using Microsoft.Extensions.Time.Testing;
using Stowage;
using System.Security.Cryptography;
using System.Text;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public class CleanupCapabilityTests : IDisposable
    {
        private static readonly DateTimeOffset s_now = new DateTimeOffset(2026, 10, 15, 12, 0, 0, TimeSpan.Zero);
        private static readonly TimeSpan s_wait = TimeSpan.FromSeconds(30);
        private const string Ict = CleanupCapability.IctEnablement;

        private readonly string _directory = Path.Combine(Path.GetTempPath(), "cc-" + ScratchOwners.ShortId());
        private readonly ScratchOwners _owners = new ScratchOwners();
        private readonly List<IDisposable> _disposables = new List<IDisposable>();

        public CleanupCapabilityTests()
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

            public string Native(long version) => Path.Combine(Root, "t", "_delta_log", $"{version:D20}.json");
        }

        // An eligible local disk table at /t
        private Table Local()
        {
            var root = Path.Combine(_directory, ScratchOwners.ShortId());
            Directory.CreateDirectory(root);
            var local = Files.Of.LocalDisk(root);
            _disposables.Add(local);
            var configuration = CleanupConfiguration.Validate(local, "t", false, CleanupOptIns.Off);
            Assert.True(configuration.Store.Eligible);
            return new Table() { Local = local, Root = root, Configuration = configuration };
        }

        private static JobRun Run() => new JobRun(0, JobKind.Deletion) { Cancellation = new CancellationTokenSource() };

        private CleanupContext Context(JobRun? run = null, TimeProvider? clock = null, long readLimit = MaintenanceWorkerSettings.DefaultShortReads, string? advisoryTableId = null,
            CancellationToken stop = default, JobMemory? memory = null, long diskCap = DiskLedger.DefaultCap)
        {
            clock ??= new FakeTimeProvider(s_now);
            var owner = _owners.Add(new ScratchDirectory(Path.Combine(_directory, ScratchOwners.ShortId()), diskCap));
            var scratch = owner.CreateJob();
            _disposables.Add(scratch);
            return new CleanupContext(run ?? Run(), clock)
            {
                Kind = JobKind.Deletion,
                Long = false,
                ReadLimit = readLimit,
                Deadline = clock.GetUtcNow() + TimeSpan.FromMinutes(15),
                Head = -1,
                TableId = advisoryTableId,
                Scratch = scratch,
                Memory = memory ?? new JobMemory(),
                Transfer = new TransferAllowance(),
                Stop = stop
            };
        }

        private static async Task<LogEvidence> Log(Table table, IFileStorage? storage = null)
        {
            var (check, log) = await ListingEvidence.ReadLogAsync(table.Configuration, storage ?? table.Local, -1, default);
            Assert.Equal(ListingCheck.Passed, check);
            return log!;
        }

        private static DeltaAction Own(long version, long adoptedAt = 0, string? stageId = null) => new DeltaAction()
        {
            CommitInfo = new DeltaCommitInfoAction() { StageId = stageId ?? $"stage-{version}", AdoptedAt = adoptedAt, Timestamp = 1000 + version, Operation = "WRITE" }
        };

        // A commitInfo another writer wrote
        private static DeltaAction Foreign(long version) => new DeltaAction()
        {
            CommitInfo = new DeltaCommitInfoAction() { Timestamp = 1000 + version, Operation = "WRITE" }
        };

        private static DeltaAction MetaData(string id, Dictionary<string, string>? configuration = null)
        {
            var action = DeltaTestKit.Metadata(DeltaTestKit.UserSchema, configuration);
            action.MetaData!.Id = id;
            return action;
        }

        private static Task Commit(Table table, long version, params DeltaAction[] actions) => DeltaTestKit.WriteCommit(table.Local, "t", version, actions);

        // Own commits 0 to head of one epoch: the table at 0, then one added file per commit
        private static async Task Create(Table table, long head, string id = "table-a", DeltaAction? protocol = null, Dictionary<string, string>? configuration = null)
        {
            await Commit(table, 0, Own(0), protocol ?? DeltaTestKit.Protocol(), MetaData(id, configuration));
            for (long version = 1; version <= head; version++)
            {
                await Commit(table, version, Own(version), DeltaLakeCheckpointBuildTests.Add($"part-{version:D4}.parquet"));
            }
        }

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

        private async Task<CleanupEvidence> Establish(Table table, long anchor, bool durable = false, IFileStorage? storage = null, CleanupContext? context = null)
        {
            storage ??= table.Local;
            var log = await Log(table, storage);
            var result = await CleanupEvidence.EstablishAsync(table.Configuration, storage, log, anchor, null, durable, context ?? Context());
            Assert.True(result.Passed, $"{result.Verdict}: {result.Detail}");
            return result.Evidence!;
        }

        private async Task<GateResult> Validate(Table table, CleanupEvidence evidence, IFileStorage? storage = null, CleanupContext? context = null)
        {
            storage ??= table.Local;
            return await evidence.ValidateAsync(table.Configuration, storage, await Log(table, storage), context ?? Context());
        }

        // Rewrites a file in place with its old modification time
        private static void Rewrite(string native, byte[] content)
        {
            var modified = File.GetLastWriteTimeUtc(native);
            File.WriteAllBytes(native, content);
            File.SetLastWriteTimeUtc(native, modified);
        }

        [Fact]
        public async Task RetentionOffPayloadUsesCleanupCapability()
        {
            // No retention option, no retention properties and no cutoff: capability alone decides
            var table = Local();
            await Create(table, 2);
            await BuildCheckpoint(table, 2);
            for (long version = 3; version <= 4; version++)
            {
                await Commit(table, version, Own(version), DeltaLakeCheckpointBuildTests.Add($"part-{version:D4}.parquet"));
            }
            var log = await Log(table);
            var checkpoint = Path.Combine(table.Root, "t", "_delta_log", $"{2:D20}.checkpoint.parquet");

            var result = await CleanupCapability.CheckAsync(table.Configuration, table.Local, log, Context());

            Assert.Equal(CapabilityCheck.Capable, result.Check);
            var capability = result.Capability!;
            Assert.Equal(4, capability.Version);
            Assert.Equal("table-a", capability.TableId);
            Assert.DoesNotContain(capability.Configuration.Keys, x => x.Contains("Retention", StringComparison.Ordinal));
            // The destination root and the two commits after it, the root's complete bytes hashed as its source
            Assert.Equal(3, result.Reads);
            Assert.True(capability.ProtocolSource.IsCheckpoint);
            Assert.Same(capability.ProtocolSource, capability.MetaDataSource);
            Assert.Equal(2, capability.ProtocolSource.Version);
            Assert.Equal(SHA256.HashData(File.ReadAllBytes(checkpoint)), capability.ProtocolSource.Sha256);
            Assert.Single(capability.Sources);
            var evidence = await Establish(table, 4);
            Assert.Equal(GateVerdict.Passed, (await Validate(table, evidence)).Verdict);

            // Settings that decline a checkpoint build or a retention advance are not cleanup capability
            var other = Local();
            var configuration = new Dictionary<string, string>()
            {
                [CheckpointExpiry.RetentionProperty] = "not an interval",
                ["delta.logRetentionDuration"] = "interval -1 days",
                ["delta.checkpoint.writeStatsAsJson"] = "false"
            };
            await Create(other, 1, configuration: configuration);
            var metaData = MetaData("table-a", configuration).MetaData!;
            Assert.Equal(MaintenanceReason.InvalidRetention, Assert.Throws<CheckpointDeclinedException>(() => CheckpointExpiry.Retention(metaData.Configuration)).Reason);
            Assert.Equal(MaintenanceReason.CheckpointLayout,
                Assert.Throws<CheckpointDeclinedException>(() => CheckpointCapability.Check(DeltaTestKit.Protocol().Protocol!, metaData)).Reason);
            var otherResult = await CleanupCapability.CheckAsync(other.Configuration, other.Local, await Log(other), Context());
            Assert.Equal(CapabilityCheck.Capable, otherResult.Check);
            // Both commits replayed from version 0, each hashed in full
            Assert.Equal(2, otherResult.Reads);
            Assert.Equal(SHA256.HashData(File.ReadAllBytes(other.Native(0))), otherResult.Capability!.MetaDataSource.Sha256);
            Assert.Equal(GateVerdict.Passed, (await Validate(other, await Establish(other, 1))).Verdict);
        }

        [Fact]
        public async Task IctEnabledWithoutItsWriterFeatureDeclines()
        {
            var table = Local();
            await Create(table, 2, configuration: new Dictionary<string, string>() { [Ict] = "true" });
            var log = await Log(table);

            var capability = await CleanupCapability.CheckAsync(table.Configuration, table.Local, log, Context());

            Assert.Equal(CapabilityCheck.IctEnabled, capability.Check);
        }

        [Theory]
        [InlineData("true", null, null, "IctEnabled", false)]
        [InlineData("TRUE", null, null, "IctEnabled", false)]
        [InlineData("true", null, null, "IctEnabled", true)]
        [InlineData("yes", null, null, "IctMalformed", false)]
        [InlineData(null, "delta.inCommitTimestampEnablementVersion", "3", "IctInconsistent", false)]
        [InlineData("false", "delta.inCommitTimestampEnablementTimestamp", "1", "IctInconsistent", true)]
        [InlineData("false", "DELTA.ENABLEINCOMMITTIMESTAMPS", "false", "IctInconsistent", false)]
        [InlineData("false", "DELTA.ENABLEINCOMMITTIMESTAMPS", "true", "IctEnabled", false)]
        [InlineData("false", "delta.enableInCommitTimestamps-preview", "true", "IctInconsistent", false)]
        [InlineData("false", null, null, "Capable", true)]
        [InlineData(null, null, null, "Capable", false)]
        public async Task EnabledAndInconsistentIctDeclinesPayload(string? enabled, string? otherKey, string? otherValue, string outcome, bool rooted)
        {
            var expected = Enum.Parse<CapabilityCheck>(outcome);
            var configuration = new Dictionary<string, string>();
            if (enabled != null)
            {
                configuration[Ict] = enabled;
            }
            if (otherKey != null)
            {
                configuration[otherKey] = otherValue!;
            }
            var table = Local();
            // The feature in the protocol, so only its enablement decides
            await Create(table, 2, protocol: DeltaTestKit.ProtocolWith("inCommitTimestamp"), configuration: configuration);
            if (rooted)
            {
                // The metadata then comes from a root, not from a replayed commit
                await BuildCheckpoint(table, 2);
                await Commit(table, 3, Own(3), DeltaLakeCheckpointBuildTests.Add("part-0003.parquet"));
            }
            var log = await Log(table);

            var capability = await CleanupCapability.CheckAsync(table.Configuration, table.Local, log, Context());
            var established = await CleanupEvidence.EstablishAsync(table.Configuration, table.Local, log, log.Head, null, false, Context());

            Assert.Equal(expected, capability.Check);
            if (rooted)
            {
                // The root and the one commit after it
                Assert.Equal(2, capability.Reads);
            }
            if (expected == CapabilityCheck.Capable)
            {
                Assert.Equal(rooted, capability.Capability!.MetaDataSource.IsCheckpoint);
                Assert.Equal(GateVerdict.Passed, established.Verdict);
                return;
            }
            // Payload cleanup declines before any anchor is proven, and the decline stops a volatile run
            Assert.Equal(GateVerdict.Incapable, established.Verdict);
            Assert.Equal(expected, established.Capability);
            Assert.Equal(MaintenanceReason.ExcludedFeature, established.Reason);
            Assert.Equal(GateAction.Stop, established.Action);
            Assert.Null(established.Evidence);
        }

        [Theory]
        // Protected and catalog-managed variants, by feature or by property
        [InlineData(new[] { "checkpointProtection" }, null, "ExcludedFeature")]
        [InlineData(new[] { "catalogManaged" }, null, "ExcludedFeature")]
        [InlineData(new[] { "coordinatedCommits-preview" }, null, "ExcludedFeature")]
        [InlineData(new string[0], "delta.coordinatedCommits.commitCoordinator-preview", "ExcludedFeature")]
        [InlineData(new string[0], "delta.requireCheckpointProtectionBeforeVersion", "ExcludedFeature")]
        // vacuumProtocolCheck is honored by checking every writer feature
        [InlineData(new[] { "vacuumProtocolCheck", "futureWriterFeature" }, null, "UnsupportedFeature")]
        [InlineData(new[] { "vacuumProtocolCheck", "deletionVectors", "changeDataFeed" }, null, "Capable")]
        [InlineData(new[] { "futureWriterFeature" }, null, "UnsupportedFeature")]
        public void ProtectedCatalogManagedAndUnsupportedFeaturesDecline(string[] features, string? property, string outcome)
        {
            var expected = Enum.Parse<CapabilityCheck>(outcome);
            var protocol = DeltaTestKit.ProtocolWith(features).Protocol!;
            var configuration = property == null ? null : new Dictionary<string, string>() { [property] = "1" };

            Assert.Equal(expected, CleanupCapability.CheckFeatures(protocol, configuration, out var inEffect, out var detail));
            Assert.Equal(expected == CapabilityCheck.Capable, inEffect != null);
            Assert.Equal(expected == CapabilityCheck.Capable, detail == null);
            // A reader version this connector does not know
            Assert.Equal(CapabilityCheck.UnknownProtocol, CleanupCapability.CheckFeatures(new DeltaProtocolAction() { MinReaderVersion = 4, MinWriterVersion = 7, WriterFeatures = features }, null, out _, out _));
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task LaterProtocolActionDecidesCapability(bool rooted)
        {
            // The table starts capable, then a later commit adds a writer feature cleanup does not support
            var table = Local();
            await Create(table, 2);
            if (rooted)
            {
                await BuildCheckpoint(table, 2);
            }
            await Commit(table, 3, Own(3), DeltaTestKit.ProtocolWith("futureWriterFeature"));
            await Commit(table, 4, Own(4), DeltaLakeCheckpointBuildTests.Add("part-0004.parquet"));

            var result = await CleanupCapability.CheckAsync(table.Configuration, table.Local, await Log(table), Context());

            Assert.Equal(CapabilityCheck.UnsupportedFeature, result.Check);
            Assert.Null(result.Capability);
        }

        [Fact]
        public async Task StaleAdvisoryTableIdCannotAuthorizeDelete()
        {
            var table = Local();
            await Create(table, 2);
            var advisory = Context(advisoryTableId: "table-a");
            var durable = await Establish(table, 2, durable: true, context: advisory);
            var volatileRun = await Establish(table, 2);
            Assert.Equal("table-a", durable.TableId);

            // Another writer replaces the table's identity; the worker's advisory id still names the old one
            await Commit(table, 3, Foreign(3), MetaData("table-b"));

            var changed = await Validate(table, durable, context: Context(advisoryTableId: "table-a"));
            Assert.Equal(GateVerdict.TableChanged, changed.Verdict);
            Assert.Equal(GateAction.Suspend, changed.Action);
            Assert.Equal(GateVerdict.ForeignHead, (await Validate(table, volatileRun, context: Context(advisoryTableId: "table-a"))).Verdict);

            // An own commit adopting the new identity: evidence names the storage's id, never the advisory one
            await Commit(table, 4, Own(4, adoptedAt: 4), DeltaLakeCheckpointBuildTests.Add("part-0004.parquet"));
            var log = await Log(table);
            var adopted = await CleanupEvidence.EstablishAsync(table.Configuration, table.Local, log, 4, "stage-4", false, Context(advisoryTableId: "table-a"));
            Assert.True(adopted.Passed, adopted.Detail);
            Assert.Equal("table-b", adopted.Evidence!.TableId);
            Assert.Equal("table-b", adopted.Evidence.Anchor.TableId);
            // A claim carrying the stale id, or no id, never matches; only the storage's id does
            Assert.Equal(GateVerdict.TableChanged, adopted.Evidence.CheckClaim("table-a", 4));
            Assert.Equal(GateVerdict.TableChanged, adopted.Evidence.CheckClaim(null, 4));
            Assert.Equal(GateVerdict.NewEpoch, adopted.Evidence.CheckClaim("table-b", 0));
            Assert.Equal(GateVerdict.NewEpoch, adopted.Evidence.CheckClaim("table-b", null));
            Assert.Equal(GateVerdict.Passed, adopted.Evidence.CheckClaim("table-b", 4));
            // The foreign commit itself proves no ownership
            var foreign = await CleanupEvidence.EstablishAsync(table.Configuration, table.Local, log, 3, null, false, Context(advisoryTableId: "table-a"));
            Assert.Equal(GateVerdict.NotOwn, foreign.Verdict);
        }

        [Fact]
        public async Task GateStopsOnAnchorReplacement()
        {
            var table = Local();
            await Create(table, 3);
            var original = File.ReadAllBytes(table.Native(3));
            var volatileRun = await Establish(table, 3);
            var durable = await Establish(table, 3, durable: true);
            Assert.Equal("stage-3", volatileRun.Anchor.StageId);
            Assert.Equal(0, volatileRun.Anchor.AdoptedAt);
            Assert.Equal($"{3:D20}.json", volatileRun.Anchor.Name);

            // Another commit at the anchor's version
            await Commit(table, 3, Own(3, stageId: "stage-x"), DeltaLakeCheckpointBuildTests.Add("part-other.parquet"));

            // Another listed size stops the gate before any byte is read
            var buckets = new MaintenanceBuckets(new FakeTimeProvider(s_now));
            var metered = MeteredFileStorage.Wrap(table.Local, buckets, ListingPaging.LocalDisk);
            var replaced = await Validate(table, volatileRun, metered);
            Assert.Equal(GateVerdict.AnchorReplaced, replaced.Verdict);
            Assert.Equal(GateAction.Stop, replaced.Action);
            Assert.Equal(MaintenanceReason.HistoryChanged, replaced.Reason);
            Assert.Equal(0, buckets.Bytes.Charged);
            var suspended = await Validate(table, durable);
            Assert.Equal(GateVerdict.AnchorReplaced, suspended.Verdict);
            Assert.Equal(GateAction.Suspend, suspended.Action);

            // A stop is final: the original bytes coming back do not reopen the run
            Rewrite(table.Native(3), original);
            Assert.Equal(GateVerdict.AnchorReplaced, (await Validate(table, volatileRun)).Verdict);
            Assert.Equal(GateVerdict.AnchorReplaced, volatileRun.StoppedBy);

            // A missing anchor below the head stops too, before its gap is read as a listing anomaly
            var below = await Establish(table, 2);
            File.Delete(table.Native(2));
            var missing = await Validate(table, below);
            Assert.Equal(GateVerdict.AnchorMissing, missing.Verdict);
            Assert.Equal(GateAction.Stop, missing.Action);
        }

        [Fact]
        public async Task EqualSizeEqualMtimeChangedActionsStopsRun()
        {
            var table = Local();
            await Create(table, 3);
            var evidence = await Establish(table, 3);
            var listed = (await Log(table)).Listing.Commits[3].IOEntry;

            // Same first line, same length and modification time, another file added
            var original = File.ReadAllBytes(table.Native(3));
            var text = Encoding.UTF8.GetString(original);
            Assert.Contains("part-0003.parquet", text);
            Rewrite(table.Native(3), Encoding.UTF8.GetBytes(text.Replace("part-0003.parquet", "part-9999.parquet")));
            var relisted = (await Log(table)).Listing.Commits[3].IOEntry;
            Assert.Equal(listed.Size, relisted.Size);
            Assert.Equal(listed.LastModificationTime, relisted.LastModificationTime);
            Assert.Equal(listed.MD5, relisted.MD5);
            Assert.True(evidence.Anchor.Proof.ListedUnchanged((await Log(table)).Listing.Commits[3]));

            var result = await Validate(table, evidence);

            Assert.Equal(GateVerdict.AnchorReplaced, result.Verdict);
            Assert.Equal(GateAction.Stop, result.Action);

            // The same holds for the object the cached metadata came from
            var source = Local();
            await Create(source, 2, configuration: new Dictionary<string, string>() { ["k"] = "a" });
            var sourced = await Establish(source, 2);
            Assert.Equal(0, sourced.Capability.MetaDataSource.Version);
            var line = Encoding.UTF8.GetString(File.ReadAllBytes(source.Native(0)));
            Rewrite(source.Native(0), Encoding.UTF8.GetBytes(line.Replace("\"k\":\"a\"", "\"k\":\"b\"")));
            Assert.Equal(GateVerdict.SourceReplaced, (await Validate(source, sourced)).Verdict);
        }

        [Fact]
        public async Task SameTableIdRollbackStopsRun()
        {
            var table = Local();
            await Create(table, 5);
            var evidence = await Establish(table, 3, durable: true);
            Assert.Equal(3, evidence.Frontier);
            Assert.Equal(5, evidence.LastValidatedHead);

            // The table rolls back to 3 under the same id
            File.Delete(table.Native(5));
            File.Delete(table.Native(4));
            var rollback = await Validate(table, evidence);
            Assert.Equal(GateVerdict.Rollback, rollback.Verdict);
            Assert.Equal(GateAction.Suspend, rollback.Action);
            // Regrowing past the old head does not reopen it
            for (long version = 4; version <= 6; version++)
            {
                await Commit(table, version, Own(version, stageId: $"again-{version}"), DeltaLakeCheckpointBuildTests.Add($"again-{version}.parquet"));
            }
            Assert.Equal(GateVerdict.Rollback, (await Validate(table, evidence)).Verdict);

            // A rollback hidden by regrowth between two gates is caught at an extended frontier
            var regrown = Local();
            await Create(regrown, 5);
            var extended = await Establish(regrown, 3);
            var passed = await Validate(regrown, extended);
            Assert.True(passed.Passed, passed.Detail);
            // Only an own commit of the epoch above X and at or below the validated head can become X
            Assert.False(extended.ExtendFrontier(extended.Anchor.Proof));
            Assert.False(extended.ExtendFrontier(extended.Capability.MetaDataSource));
            await Commit(regrown, 6, Own(6), DeltaLakeCheckpointBuildTests.Add("part-0006.parquet"));
            var ahead = await CommitAnchor.ProveAsync("t", regrown.Local, await Log(regrown), 6, Context());
            Assert.False(extended.ExtendFrontier(ahead.Proof!));
            File.Delete(regrown.Native(6));
            var unproven = passed.HeadProof!;
            Assert.False(extended.ExtendFrontier(new ContentProof(unproven.Path, unproven.Version, false, unproven.Listed, unproven.Length, unproven.Sha256) { Provenance = CommitProvenance.Unknown(5) }));
            Assert.True(extended.ExtendFrontier(passed.HeadProof!));
            Assert.Equal(5, extended.Frontier);
            var unextended = await Establish(regrown, 3);
            File.Delete(regrown.Native(5));
            File.Delete(regrown.Native(4));
            for (long version = 4; version <= 6; version++)
            {
                await Commit(regrown, version, Own(version, stageId: $"again-{version}"), DeltaLakeCheckpointBuildTests.Add($"again-{version}.parquet"));
            }
            Assert.Equal(GateVerdict.Rollback, (await Validate(regrown, extended)).Verdict);
            // Above an unchanged frontier the run's own epoch keeps its protection argument
            Assert.Equal(GateVerdict.Passed, (await Validate(regrown, unextended)).Verdict);

            // Rolling back below the anchor replaces it
            var below = Local();
            await Create(below, 5);
            var anchored = await Establish(below, 4);
            File.Delete(below.Native(5));
            File.Delete(below.Native(4));
            for (long version = 4; version <= 6; version++)
            {
                await Commit(below, version, Own(version, stageId: $"again-{version}"), DeltaLakeCheckpointBuildTests.Add($"again-{version}.parquet"));
            }
            Assert.Equal(GateVerdict.AnchorReplaced, (await Validate(below, anchored)).Verdict);
        }

        [Fact]
        public async Task CompleteAnchorHashIsRequired()
        {
            var table = Local();
            await Create(table, 1);
            // An anchor several read buffers long
            var adds = Enumerable.Range(0, 2000).Select(x => DeltaLakeCheckpointBuildTests.Add($"part-{x:D6}-{new string('x', 64)}.parquet")).ToArray();
            await Commit(table, 2, new[] { Own(2) }.Concat(adds).ToArray());
            var bytes = File.ReadAllBytes(table.Native(2));
            Assert.True(bytes.Length > 4 * ProofReader.BufferSize);

            var evidence = await Establish(table, 2);
            Assert.Equal(bytes.Length, evidence.Anchor.Proof.Length);
            Assert.Equal(SHA256.HashData(bytes), evidence.Anchor.Sha256);

            // One byte changed near the end, past every leading buffer
            var tampered = (byte[])bytes.Clone();
            var at = Array.LastIndexOf(tampered, (byte)'x');
            tampered[at] = (byte)'y';
            Rewrite(table.Native(2), tampered);
            Assert.Equal(GateVerdict.AnchorReplaced, (await Validate(table, evidence)).Verdict);

            // A read shorter than the listed size proves nothing, whether it ends between lines or inside one
            var lineEnd = Array.LastIndexOf(bytes, (byte)'\n', bytes.Length / 2) + 1;
            foreach (var cut in new[] { lineEnd, bytes.Length / 2 })
            {
                var truncated = Local();
                await Create(truncated, 1);
                await Commit(truncated, 2, new[] { Own(2) }.Concat(adds).ToArray());
                var log = await Log(truncated);
                File.WriteAllBytes(truncated.Native(2), bytes.AsSpan(0, cut).ToArray());
                var established = await CleanupEvidence.EstablishAsync(truncated.Configuration, truncated.Local, log, 2, null, false, Context());
                Assert.Equal(GateVerdict.Changed, established.Verdict);
                Assert.Equal(GateAction.Retry, established.Action);
                Assert.Null(established.Evidence);
                var proof = await CommitAnchor.ProveAsync("t", truncated.Local, log, 2, Context());
                Assert.Equal(AnchorCheck.Changed, proof.Check);
                Assert.Null(proof.Proof);
            }
        }

        [Fact]
        public async Task HashValidationChargesAllBytesAndCanBePreempted()
        {
            var table = Local();
            var adds = Enumerable.Range(0, 1500).Select(x => DeltaLakeCheckpointBuildTests.Add($"part-{x:D6}-{new string('z', 64)}.parquet")).ToArray();
            await Commit(table, 0, new[] { Own(0), DeltaTestKit.Protocol(), MetaData("table-a") }.Concat(adds).ToArray());
            var length = new FileInfo(table.Native(0)).Length;
            Assert.True(length > 3 * ProofReader.BufferSize);

            // Every request and byte goes through the metered storage
            var buckets = new MaintenanceBuckets(new FakeTimeProvider(s_now));
            var metered = MeteredFileStorage.Wrap(table.Local, buckets, ListingPaging.LocalDisk);
            var log = await Log(table, metered);
            Assert.Equal(2, buckets.Requests.Charged);
            var established = await CleanupEvidence.EstablishAsync(table.Configuration, metered, log, 0, "stage-0", false, Context());
            Assert.True(established.Passed, established.Detail);
            // One complete read proves the capability source, the anchor and the head at once
            Assert.Equal(3, buckets.Requests.Charged);
            Assert.Equal(length, buckets.Bytes.Charged);
            var evidence = established.Evidence!;
            // A gate takes its own listing and probe, then rehashes the whole object
            Assert.Equal(GateVerdict.Passed, (await evidence.ValidateAsync(table.Configuration, metered, await Log(table, metered), Context())).Verdict);
            Assert.Equal(6, buckets.Requests.Charged);
            Assert.Equal(2 * length, buckets.Bytes.Charged);

            // A read parks at its second buffer while the stop condition arrives
            var gated = new GatedStorage(table.Local);
            var parkedBuckets = new MaintenanceBuckets(new FakeTimeProvider(s_now));
            var parked = MeteredFileStorage.Wrap(gated, parkedBuckets, ListingPaging.LocalDisk);
            var clock = new FakeTimeProvider(s_now);
            var cancellation = new CancellationTokenSource();
            _disposables.Add(cancellation);
            foreach (var stop in new[] { IssuanceStop.BuildDue, IssuanceStop.Deadline, IssuanceStop.Stopped })
            {
                var run = Run();
                var stopToken = stop == IssuanceStop.Stopped ? cancellation.Token : default;
                var context = Context(run, clock, stop: stopToken);
                var reached = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                gated.BeforeRead = read => read == 2 ? Release(reached, release) : Task.CompletedTask;
                var fresh = await Log(table, parked);
                var before = parkedBuckets.Bytes.Charged;

                var validation = evidence.ValidateAsync(table.Configuration, parked, fresh, context);
                await reached.Task.WaitAsync(s_wait);
                switch (stop)
                {
                    case IssuanceStop.BuildDue:
                        run.YieldRequested = true;
                        break;
                    case IssuanceStop.Deadline:
                        clock.Advance(TimeSpan.FromMinutes(15));
                        break;
                    default:
                        cancellation.Cancel();
                        break;
                }
                release.SetResult();
                var result = await validation.WaitAsync(s_wait);

                Assert.Equal(GateVerdict.Preempted, result.Verdict);
                Assert.Equal(stop, result.Stop);
                Assert.Equal(GateAction.Retry, result.Action);
                Assert.Equal(stop == IssuanceStop.Deadline ? MaintenanceReason.JobDeadline : MaintenanceReason.None, result.Reason);
                // Charged for exactly what was read: two buffers, or one when the cancelled read never ran
                var charged = parkedBuckets.Bytes.Charged - before;
                if (stop == IssuanceStop.Stopped)
                {
                    Assert.InRange(charged, ProofReader.BufferSize, 2L * ProofReader.BufferSize);
                }
                else
                {
                    Assert.Equal(2L * ProofReader.BufferSize, charged);
                }
                // No partial state: the evidence, the ledgers and the file are as before
                Assert.Null(evidence.StoppedBy);
                Assert.Equal(0, evidence.LastValidatedHead);
                Assert.Equal(0, context.Memory.Managed);
                Assert.Equal(0, context.Transfer.Charged);
                Assert.Equal(stop, context.CheckStop());
                using (File.Open(table.Native(0), FileMode.Open, FileAccess.ReadWrite, FileShare.None))
                {
                }
            }

            // Nothing stuck: a later gate validates the whole object again
            gated.BeforeRead = null;
            Assert.Equal(GateVerdict.Passed, (await evidence.ValidateAsync(table.Configuration, parked, await Log(table, parked), Context())).Verdict);
        }

        private static async Task Release(TaskCompletionSource reached, TaskCompletionSource release)
        {
            reached.TrySetResult();
            await release.Task;
        }

        [Theory]
        [InlineData("torn")]
        [InlineData("duplicate")]
        [InlineData("noEpoch")]
        [InlineData("epochAhead")]
        [InlineData("foreign")]
        [InlineData("otherStage")]
        [InlineData("unpublished")]
        [InlineData("duplicateCreatedFiles")]
        [InlineData("duplicateTimestamp")]
        [InlineData("fractionalTimestamp")]
        public async Task AnchorOwnershipNeedsStrictProof(string anchor)
        {
            var table = Local();
            await Create(table, 1);
            var protocolLine = "{\"add\":{\"path\":\"p.parquet\",\"partitionValues\":{},\"size\":1,\"modificationTime\":1,\"dataChange\":true}}\n";
            var line = anchor switch
            {
                // No terminator after the first line, which a strict proof requires
                "torn" => "{\"commitInfo\":{\"flowtide.stageId\":\"stage-2\",\"flowtide.adoptedAt\":0}}",
                "duplicate" => "{\"commitInfo\":{\"flowtide.stageId\":\"stage-2\",\"flowtide.stageId\":\"stage-2\",\"flowtide.adoptedAt\":0}}\n" + protocolLine,
                // Other fixed fields duplicated or nonintegral, which cleanup rejects as it rejects a duplicated stage id
                "duplicateCreatedFiles" => "{\"commitInfo\":{\"flowtide.stageId\":\"stage-2\",\"flowtide.adoptedAt\":0,\"flowtide.createdFiles\":[\"a.parquet\"],\"flowtide.createdFiles\":[\"b.parquet\"]}}\n" + protocolLine,
                "duplicateTimestamp" => "{\"commitInfo\":{\"timestamp\":1,\"timestamp\":2,\"flowtide.stageId\":\"stage-2\",\"flowtide.adoptedAt\":0}}\n" + protocolLine,
                "fractionalTimestamp" => "{\"commitInfo\":{\"timestamp\":1.5,\"flowtide.stageId\":\"stage-2\",\"flowtide.adoptedAt\":0}}\n" + protocolLine,
                "noEpoch" => "{\"commitInfo\":{\"flowtide.stageId\":\"stage-2\"}}\n" + protocolLine,
                "epochAhead" => "{\"commitInfo\":{\"flowtide.stageId\":\"stage-2\",\"flowtide.adoptedAt\":3}}\n" + protocolLine,
                "foreign" => "{\"commitInfo\":{\"operation\":\"WRITE\"}}\n" + protocolLine,
                _ => "{\"commitInfo\":{\"flowtide.stageId\":\"stage-2\",\"flowtide.adoptedAt\":0}}\n" + protocolLine
            };
            if (anchor != "unpublished")
            {
                Directory.CreateDirectory(Path.GetDirectoryName(table.Native(2))!);
                File.WriteAllText(table.Native(2), line);
            }
            var log = await Log(table);
            var version = anchor == "unpublished" ? 2 : log.Head;

            var result = await CleanupEvidence.EstablishAsync(table.Configuration, table.Local, log, version, anchor == "otherStage" ? "stage-other" : null, false, Context());

            var expected = anchor switch
            {
                "otherStage" => GateVerdict.StageMismatch,
                "unpublished" => GateVerdict.NotPublished,
                _ => GateVerdict.NotOwn
            };
            Assert.Equal(expected, result.Verdict);
            Assert.Null(result.Evidence);
            Assert.Equal(anchor == "unpublished" ? GateAction.Retry : GateAction.Stop, result.Action);
        }

        [Fact]
        public async Task ForeignHeadOrNewEpochStopsVolatileWork()
        {
            var table = Local();
            await Create(table, 2);
            var evidence = await Establish(table, 2);
            await Commit(table, 3, Own(3), DeltaLakeCheckpointBuildTests.Add("part-0003.parquet"));
            Assert.Equal(GateVerdict.Passed, (await Validate(table, evidence)).Verdict);
            Assert.Equal(3, evidence.LastValidatedHead);

            // A foreign head, then an own commit that starts a new epoch after it
            var foreign = await Establish(table, 3);
            var epoch = await Establish(table, 3, durable: true);
            await Commit(table, 4, Foreign(4), DeltaLakeCheckpointBuildTests.Add("part-0004.parquet"));
            var head = await Validate(table, foreign);
            Assert.Equal(GateVerdict.ForeignHead, head.Verdict);
            Assert.Equal(GateAction.Stop, head.Action);
            // No new run is anchored across it either
            Assert.Equal(GateVerdict.ForeignHead, (await CleanupEvidence.EstablishAsync(table.Configuration, table.Local, await Log(table), 3, null, false, Context())).Verdict);
            await Commit(table, 5, Own(5, adoptedAt: 5), DeltaLakeCheckpointBuildTests.Add("part-0005.parquet"));
            Assert.Equal(GateVerdict.NewEpoch, (await Validate(table, evidence)).Verdict);
            Assert.Equal(GateVerdict.NewEpoch, (await CleanupEvidence.EstablishAsync(table.Configuration, table.Local, await Log(table), 3, null, false, Context())).Verdict);
            Assert.Equal(GateVerdict.Passed, (await CleanupEvidence.EstablishAsync(table.Configuration, table.Local, await Log(table), 5, null, false, Context())).Verdict);
            // An advance keeps its authority and handles the change itself
            var rebase = await Validate(table, epoch);
            Assert.Equal(GateVerdict.NewEpoch, rebase.Verdict);
            Assert.Equal(GateAction.Rebase, rebase.Action);
            Assert.Null(epoch.StoppedBy);
        }

        [Fact]
        public async Task GapsAndMissingSourcesNeverPass()
        {
            var table = Local();
            await Create(table, 5);
            var evidence = await Establish(table, 3);
            var relying = await Establish(table, 3);

            // A gap the decision relies on below the anchor declines without stopping the run
            File.Delete(table.Native(1));
            var below = await relying.ValidateAsync(table.Configuration, table.Local, await Log(table), Context(), relyFrom: 1);
            Assert.Equal(GateVerdict.ListingFailed, below.Verdict);
            Assert.Equal(ListingCheck.RangeGap, below.Listing);
            Assert.Equal(GateAction.Retry, below.Action);
            Assert.Null(relying.StoppedBy);
            Assert.Equal(GateVerdict.Passed, (await Validate(table, evidence)).Verdict);

            // A gap between the anchor and the head
            File.Delete(table.Native(4));
            var interior = await Validate(table, evidence);
            Assert.Equal(GateVerdict.ListingFailed, interior.Verdict);
            Assert.Equal(ListingCheck.RangeGap, interior.Listing);
            Assert.Null(evidence.StoppedBy);

            // The object the cached protocol and metadata came from is gone
            var source = Local();
            await Create(source, 3);
            var sourced = await Establish(source, 3);
            File.Delete(source.Native(0));
            var missing = await Validate(source, sourced);
            Assert.Equal(GateVerdict.SourceReplaced, missing.Verdict);
            Assert.Equal(GateAction.Stop, missing.Action);
        }

        [Fact]
        public async Task OneCapabilityServesManyAnchorsAtOneHead()
        {
            var table = Local();
            await Create(table, 4);
            var buckets = new MaintenanceBuckets(new FakeTimeProvider(s_now));
            var metered = MeteredFileStorage.Wrap(table.Local, buckets, ListingPaging.LocalDisk);
            var log = await Log(table, metered);
            var capability = await CleanupCapability.CheckAsync(table.Configuration, metered, log, Context());
            Assert.Equal(CapabilityCheck.Capable, capability.Check);
            Assert.Equal(4, capability.Capability!.HeadProof!.Version);
            var requests = buckets.Requests.Charged;

            // The sources hashed at this listing are not read again; each anchor and the head are
            foreach (var anchor in new[] { 2L, 3L })
            {
                var established = await CleanupEvidence.EstablishAsync(table.Configuration, metered, log, anchor, $"stage-{anchor}", false, Context(), capability.Capability);
                Assert.True(established.Passed, established.Detail);
                Assert.Same(capability.Capability, established.Evidence!.Capability);
                Assert.Equal(anchor, established.Evidence.Frontier);
                Assert.Equal(4, established.Evidence.LastValidatedHead);
            }
            Assert.Equal(requests + 4, buckets.Requests.Charged);
            Assert.Equal(GateVerdict.Passed, (await CleanupEvidence.EstablishAsync(table.Configuration, metered, log, 4, null, false, Context(), capability.Capability)).Verdict);
            Assert.Equal(requests + 5, buckets.Requests.Charged);

            // Facts from another head never stand in for this listing's
            await Commit(table, 5, Own(5), DeltaLakeCheckpointBuildTests.Add("part-0005.parquet"));
            var moved = await CleanupEvidence.EstablishAsync(table.Configuration, metered, await Log(table, metered), 3, null, false, Context(), capability.Capability);
            Assert.Equal(GateVerdict.Changed, moved.Verdict);
            Assert.Null(moved.Evidence);
        }

        [Fact]
        public async Task ChangedStorageInstanceNeverValidates()
        {
            var table = Local();
            await Create(table, 2);
            var volatileRun = await Establish(table, 2);
            var durable = await Establish(table, 2, durable: true);
            var log = await Log(table);
            // The same folder through another instance than the one classified at construction
            var replacement = Files.Of.LocalDisk(table.Root);
            _disposables.Add(replacement);

            var established = await CleanupEvidence.EstablishAsync(table.Configuration, replacement, log, 2, null, false, Context());
            Assert.Equal(GateVerdict.StorageChanged, established.Verdict);
            Assert.Null(established.Evidence);
            var stopped = await volatileRun.ValidateAsync(table.Configuration, replacement, log, Context());
            Assert.Equal(GateVerdict.StorageChanged, stopped.Verdict);
            Assert.Equal(GateAction.Stop, stopped.Action);
            // Durable authority stays for a replacement sink
            var held = await durable.ValidateAsync(table.Configuration, replacement, log, Context());
            Assert.Equal(GateAction.Retry, held.Action);
            Assert.Equal(MaintenanceReason.ListingIneligible, held.Reason);
            Assert.Null(durable.StoppedBy);
            var (check, _) = await ListingEvidence.ReadLogAsync(table.Configuration, replacement, -1, default);
            Assert.Equal(GateVerdict.StorageChanged, GateResult.OfListing(check, durable: false).Verdict);
            Assert.Equal(GateVerdict.ListingFailed, GateResult.OfListing(ListingCheck.SuccessorPresent, durable: true).Verdict);
        }

        [Fact]
        public async Task EstablishmentChecksTheAnchorAndItsRange()
        {
            var table = Local();
            await Create(table, 3);
            File.Delete(table.Native(2));
            var log = await Log(table);

            // An anchor the head has passed but the listing lacks is gone, not a listing anomaly
            var missing = await CleanupEvidence.EstablishAsync(table.Configuration, table.Local, log, 2, null, false, Context());
            Assert.Equal(GateVerdict.AnchorMissing, missing.Verdict);
            Assert.Equal(GateAction.Stop, missing.Action);
            // A gap between a listed anchor and the head declines without concluding anything
            var gap = await CleanupEvidence.EstablishAsync(table.Configuration, table.Local, log, 1, null, false, Context());
            Assert.Equal(GateVerdict.ListingFailed, gap.Verdict);
            Assert.Equal(ListingCheck.RangeGap, gap.Listing);
            Assert.Equal(GateAction.Retry, gap.Action);
        }

        [Fact]
        public async Task TableWithoutIdOrProtocolDeclines()
        {
            var table = Local();
            var noId = MetaData("table-a");
            noId.MetaData!.Id = null;
            await Commit(table, 0, Own(0), DeltaTestKit.Protocol(), noId);
            var result = await CleanupEvidence.EstablishAsync(table.Configuration, table.Local, await Log(table), 0, null, false, Context());
            Assert.Equal(GateVerdict.Incapable, result.Verdict);
            Assert.Equal(CapabilityCheck.NoTableId, result.Capability);

            var noProtocol = Local();
            await Commit(noProtocol, 0, Own(0), MetaData("table-a"));
            Assert.Equal(CapabilityCheck.NoProtocolOrMetadata, (await CleanupCapability.CheckAsync(noProtocol.Configuration, noProtocol.Local, await Log(noProtocol), Context())).Check);

            var empty = Local();
            var emptyLog = await Log(empty);
            Assert.Equal(-1, emptyLog.Head);
            Assert.Equal(CapabilityCheck.NoProtocolOrMetadata, (await CleanupCapability.CheckAsync(empty.Configuration, empty.Local, emptyLog, Context())).Check);
            Assert.Equal(GateVerdict.NotPublished, (await CleanupEvidence.EstablishAsync(empty.Configuration, empty.Local, emptyLog, 0, null, false, Context())).Verdict);
        }

        [Fact]
        public async Task StorageErrorsAndCapsRetryWithoutStopping()
        {
            var table = Local();
            await Create(table, 2);
            var durable = await Establish(table, 2, durable: true);
            var volatileRun = await Establish(table, 2);
            var gated = new GatedStorage(table.Local) { BeforeRead = _ => throw new IOException("network") };
            var log = await Log(table);

            var capability = await CleanupCapability.CheckAsync(table.Configuration, gated, log, Context());
            Assert.Equal(CapabilityCheck.Unavailable, capability.Check);
            Assert.Equal(MaintenanceReason.Transient, capability.Reason);
            var established = await CleanupEvidence.EstablishAsync(table.Configuration, gated, log, 2, null, false, Context());
            Assert.Equal(GateVerdict.Unavailable, established.Verdict);
            Assert.Equal(GateAction.Retry, established.Action);
            foreach (var evidence in new[] { durable, volatileRun })
            {
                var failed = await evidence.ValidateAsync(table.Configuration, gated, log, Context());
                Assert.Equal(GateVerdict.Unavailable, failed.Verdict);
                Assert.Equal(GateAction.Retry, failed.Action);
                Assert.Null(evidence.StoppedBy);
            }

            // A memory cap is a resource limit, never a verdict on the table
            var capped = await CleanupEvidence.EstablishAsync(table.Configuration, table.Local, log, 2, null, false, Context(memory: new JobMemory(managedCap: 16 << 10)));
            Assert.Equal(GateVerdict.Unavailable, capped.Verdict);
            Assert.Equal(GateAction.Retry, capped.Action);
            var cappedCapability = await CleanupCapability.CheckAsync(table.Configuration, table.Local, log, Context(memory: new JobMemory(managedCap: 16 << 10)));
            Assert.Equal(CapabilityCheck.Unavailable, cappedCapability.Check);
            Assert.Equal(GateVerdict.Passed, (await durable.ValidateAsync(table.Configuration, table.Local, await Log(table), Context())).Verdict);
        }

        [Fact]
        public async Task TornRootFallsBackOrDeclines()
        {
            var table = Local();
            await Create(table, 3);
            await BuildCheckpoint(table, 3);
            await Commit(table, 4, Own(4), DeltaLakeCheckpointBuildTests.Add("part-0004.parquet"));
            var checkpoint = Path.Combine(table.Root, "t", "_delta_log", $"{3:D20}.checkpoint.parquet");
            File.WriteAllBytes(checkpoint, new byte[new FileInfo(checkpoint).Length]);
            var log = await Log(table);

            // The torn root is read and skipped; the whole history replays instead
            var context = Context();
            var fallback = await CleanupCapability.CheckAsync(table.Configuration, table.Local, log, context);
            Assert.Equal(CapabilityCheck.Capable, fallback.Check);
            Assert.Equal(1 + 5, fallback.Reads);
            Assert.False(fallback.Capability!.MetaDataSource.IsCheckpoint);
            // Its download left job scratch, and every charged buffer was returned
            Assert.Equal(0, context.Memory.Managed);
            Assert.Equal(0, context.Memory.Native);
            Assert.Empty(context.Scratch.Files);
            Assert.Empty(Directory.GetFiles(context.Scratch.DirectoryPath));

            // Falling back past it needs more reads than the admission allows, and it already read the root
            var limited = await CleanupCapability.CheckAsync(table.Configuration, table.Local, log, Context(readLimit: 2));
            Assert.Equal(CapabilityCheck.OverReadLimit, limited.Check);
            Assert.Equal(1, limited.Reads);
            Assert.Equal(MaintenanceReason.NoAdmissibleRoot, limited.Reason);

            // Without the history before it, nothing roots the head
            File.Delete(table.Native(0));
            var missing = await CleanupCapability.CheckAsync(table.Configuration, table.Local, await Log(table), Context());
            Assert.Equal(CapabilityCheck.MissingHistory, missing.Check);
            Assert.Contains("checkpoint 3", missing.Detail);

            // A root rewritten to another length after the listing is a change, not a root to skip
            var rewritten = Local();
            await Create(rewritten, 3);
            await BuildCheckpoint(rewritten, 3);
            var rewrittenLog = await Log(rewritten);
            var rewrittenPath = Path.Combine(rewritten.Root, "t", "_delta_log", $"{3:D20}.checkpoint.parquet");
            File.WriteAllBytes(rewrittenPath, File.ReadAllBytes(rewrittenPath).Concat(new byte[16]).ToArray());
            var changed = await CleanupCapability.CheckAsync(rewritten.Configuration, rewritten.Local, rewrittenLog, Context());
            Assert.Equal(CapabilityCheck.Changed, changed.Check);
            Assert.Equal(1, changed.Reads);
        }

        [Fact]
        public async Task ReplayPromotesBeforeAnyRead()
        {
            var table = Local();
            await Create(table, 4);
            var buckets = new MaintenanceBuckets(new FakeTimeProvider(s_now));
            var metered = MeteredFileStorage.Wrap(table.Local, buckets, ListingPaging.LocalDisk);
            var log = await Log(table, metered);
            var requests = buckets.Requests.Charged;

            // Five commits to replay against a limit of four
            var promoted = await CleanupEvidence.EstablishAsync(table.Configuration, metered, log, 4, null, false, Context(readLimit: 4));

            Assert.Equal(GateVerdict.NeedsLongAdmission, promoted.Verdict);
            Assert.Equal(GateAction.Promote, promoted.Action);
            Assert.Equal(requests, buckets.Requests.Charged);
            Assert.Equal(0, buckets.Bytes.Charged);
            Assert.Equal(GateVerdict.Passed, (await CleanupEvidence.EstablishAsync(table.Configuration, metered, log, 4, null, false, Context(readLimit: 5))).Verdict);
        }

        [Fact]
        public async Task CachedCapabilityNeverStandsInForTheCurrentListing()
        {
            var table = Local();
            await Create(table, 4);
            var buckets = new MaintenanceBuckets(new FakeTimeProvider(s_now));
            var metered = MeteredFileStorage.Wrap(table.Local, buckets, ListingPaging.LocalDisk);
            var log = await Log(table, metered);
            var capability = (await CleanupCapability.CheckAsync(table.Configuration, metered, log, Context())).Capability!;

            // The head rewritten in place after the check, with its size and modification time kept
            var original = File.ReadAllBytes(table.Native(4));
            Rewrite(table.Native(4), Encoding.UTF8.GetBytes(Encoding.UTF8.GetString(original).Replace("part-0004.parquet", "part-9999.parquet")));
            foreach (var anchor in new[] { 2L, 4L })
            {
                var stale = await CleanupEvidence.EstablishAsync(table.Configuration, metered, log, anchor, null, false, Context(), capability);
                Assert.Equal(GateVerdict.Changed, stale.Verdict);
                Assert.Equal(GateAction.Retry, stale.Action);
                Assert.Null(stale.Evidence);
            }
            Rewrite(table.Native(4), original);
            Assert.Equal(GateVerdict.Passed, (await CleanupEvidence.EstablishAsync(table.Configuration, metered, log, 2, null, false, Context(), capability)).Verdict);

            // Another body at the head, relisted: the listing alone declines it before any byte is read
            await Commit(table, 4, Foreign(4), DeltaLakeCheckpointBuildTests.Add("part-foreign-0004.parquet"));
            var relisted = await Log(table, metered);
            Assert.Equal(4, relisted.Head);
            var bytes = buckets.Bytes.Charged;
            var foreign = await CleanupEvidence.EstablishAsync(table.Configuration, metered, relisted, 3, null, false, Context(), capability);
            Assert.Equal(GateVerdict.Changed, foreign.Verdict);
            Assert.Null(foreign.Evidence);
            Assert.Equal(bytes, buckets.Bytes.Charged);

            // A metadata source rewritten in place between two listings is rehashed
            var source = Local();
            await Create(source, 2, configuration: new Dictionary<string, string>() { ["k"] = "a" });
            var sourced = (await CleanupCapability.CheckAsync(source.Configuration, source.Local, await Log(source), Context())).Capability!;
            Assert.Equal(0, sourced.MetaDataSource.Version);
            var line = Encoding.UTF8.GetString(File.ReadAllBytes(source.Native(0)));
            Rewrite(source.Native(0), Encoding.UTF8.GetBytes(line.Replace("\"k\":\"a\"", "\"k\":\"b\"")));
            var rewritten = await CleanupEvidence.EstablishAsync(source.Configuration, source.Local, await Log(source), 2, null, false, Context(), sourced);
            Assert.Equal(GateVerdict.Changed, rewritten.Verdict);
            Assert.Null(rewritten.Evidence);
        }

        [Fact]
        public async Task GatesNeverPassOnAStaleListing()
        {
            var table = Local();
            await Create(table, 2);
            var store = table.Configuration.Store;
            var folder = store.Table.Combine(DeltaTransactionReader.DeltaLogDirName);

            // A listing that passed the structural checks but never the head, fence and successor checks
            Assert.Equal(ListingCheck.Passed, ListingEvidence.Check(store, folder, false, (await table.Local.Ls(folder)).ToList(), out var listing));
            var unprobed = LogEvidence.From(listing!);
            var refused = await CleanupEvidence.EstablishAsync(table.Configuration, table.Local, unprobed, 2, null, false, Context());
            Assert.Equal(GateVerdict.StaleListing, refused.Verdict);
            Assert.Equal(GateAction.Retry, refused.Action);
            Assert.Null(refused.Evidence);
            // A stage-only listing never serves a gate
            Assert.Equal(ListingCheck.Passed, ListingEvidence.Check(store, folder, false, (await table.Local.Ls(folder)).ToList(), out var stageListing, purpose: EvidencePurpose.StageOnly));
            var stageOnly = await CleanupEvidence.EstablishAsync(table.Configuration, table.Local, LogEvidence.From(stageListing!), 2, null, false, Context());
            Assert.Equal(GateVerdict.ListingFailed, stageOnly.Verdict);
            Assert.Equal(ListingCheck.StoreIneligible, stageOnly.Listing);

            var early = await Log(table);
            var evidence = await Establish(table, 2);
            Assert.Equal(GateVerdict.StaleListing, (await evidence.ValidateAsync(table.Configuration, table.Local, unprobed, Context())).Verdict);
            // Probed before the last gate
            Assert.Equal(GateVerdict.StaleListing, (await evidence.ValidateAsync(table.Configuration, table.Local, early, Context())).Verdict);

            // A foreign commit lands between two gates of a volatile run
            var used = await Log(table);
            Assert.Equal(GateVerdict.Passed, (await evidence.ValidateAsync(table.Configuration, table.Local, used, Context())).Verdict);
            await Commit(table, 3, Foreign(3), DeltaLakeCheckpointBuildTests.Add("part-0003.parquet"));
            var reused = await evidence.ValidateAsync(table.Configuration, table.Local, used, Context());
            Assert.Equal(GateVerdict.StaleListing, reused.Verdict);
            Assert.Equal(GateAction.Retry, reused.Action);
            Assert.Null(evidence.StoppedBy);
            var current = await Validate(table, evidence);
            Assert.Equal(GateVerdict.ForeignHead, current.Verdict);
            Assert.Equal(GateAction.Stop, current.Action);
        }

        [Fact]
        public async Task ReadLimitIsPerAdmission()
        {
            var table = Local();
            await Create(table, 4);
            var log = await Log(table);

            // Two capability checks in one admission share its five reads
            var context = Context(readLimit: 5);
            var first = await CleanupCapability.CheckAsync(table.Configuration, table.Local, log, context);
            Assert.Equal(CapabilityCheck.Capable, first.Check);
            Assert.Equal(5, first.Reads);
            Assert.Equal(5, context.ReadsUsed);
            Assert.Equal(0, context.ReadsRemaining);
            var second = await CleanupCapability.CheckAsync(table.Configuration, table.Local, log, context);
            Assert.Equal(CapabilityCheck.ReadsSpent, second.Check);
            Assert.Equal(0, second.Reads);
            Assert.Equal(5, context.ReadsUsed);

            // A spent admission retries in the next one; only more than a fresh admission allows is a promotion
            var shared = Context(readLimit: 6);
            var established = await CleanupEvidence.EstablishAsync(table.Configuration, table.Local, log, 3, null, false, shared);
            Assert.True(established.Passed, established.Detail);
            Assert.Equal(5, shared.ReadsUsed);
            var spent = await CleanupEvidence.EstablishAsync(table.Configuration, table.Local, log, 3, null, false, shared);
            Assert.Equal(GateVerdict.ReadsSpent, spent.Verdict);
            Assert.Equal(GateAction.Retry, spent.Action);
            Assert.Equal(MaintenanceReason.Transient, spent.Reason);
            Assert.Null(spent.Evidence);
            // Declined before its first read, as the remainder could not cover the replay
            Assert.Equal(5, shared.ReadsUsed);
            Assert.Equal(GateVerdict.NeedsLongAdmission, (await CleanupEvidence.EstablishAsync(table.Configuration, table.Local, log, 3, null, false, Context(readLimit: 4))).Verdict);

            // A gate's rehash reads count too: the anchor, the metadata source and the head
            var evidence = established.Evidence!;
            var starved = Context(readLimit: 2);
            var partial = await evidence.ValidateAsync(table.Configuration, table.Local, await Log(table), starved);
            Assert.Equal(GateVerdict.ReadsSpent, partial.Verdict);
            Assert.Equal(GateAction.Retry, partial.Action);
            Assert.Null(evidence.StoppedBy);
            Assert.Equal(2, starved.ReadsUsed);
            var enough = Context(readLimit: 3);
            Assert.Equal(GateVerdict.Passed, (await evidence.ValidateAsync(table.Configuration, table.Local, await Log(table), enough)).Verdict);
            Assert.Equal(3, enough.ReadsUsed);
        }

        [Fact]
        public async Task ResourceLimitsPromoteOrRetryNeverStop()
        {
            // Falling back past a torn root needs more reads than a short admission allows
            var table = Local();
            await Create(table, 3);
            await BuildCheckpoint(table, 3);
            await Commit(table, 4, Own(4), DeltaLakeCheckpointBuildTests.Add("part-0004.parquet"));
            var checkpoint = Path.Combine(table.Root, "t", "_delta_log", $"{3:D20}.checkpoint.parquet");
            File.WriteAllBytes(checkpoint, new byte[new FileInfo(checkpoint).Length]);
            var promoted = await CleanupEvidence.EstablishAsync(table.Configuration, table.Local, await Log(table), 4, null, false, Context(readLimit: 2));
            Assert.Equal(GateVerdict.NeedsLongAdmission, promoted.Verdict);
            Assert.Equal(CapabilityCheck.OverReadLimit, promoted.Capability);
            Assert.Equal(GateAction.Promote, promoted.Action);
            Assert.Null(promoted.Evidence);

            // A durable run's foreign-head gate rereads the capability under the same limit
            var advance = Local();
            await Create(advance, 3);
            var durable = await Establish(advance, 3, durable: true);
            await BuildCheckpoint(advance, 3);
            await Commit(advance, 4, Own(4), DeltaLakeCheckpointBuildTests.Add("part-0004.parquet"));
            await Commit(advance, 5, Foreign(5), DeltaLakeCheckpointBuildTests.Add("part-0005.parquet"));
            var root = Path.Combine(advance.Root, "t", "_delta_log", $"{3:D20}.checkpoint.parquet");
            File.WriteAllBytes(root, new byte[new FileInfo(root).Length]);
            var gate = await durable.ValidateAsync(advance.Configuration, advance.Local, await Log(advance), Context(readLimit: 6));
            Assert.Equal(GateVerdict.NeedsLongAdmission, gate.Verdict);
            Assert.Equal(GateAction.Promote, gate.Action);
            Assert.Null(durable.StoppedBy);

            // A root over the disk cap is a resource limit, not missing history
            var capped = Local();
            await Create(capped, 3);
            await BuildCheckpoint(capped, 3);
            await Commit(capped, 4, Own(4), DeltaLakeCheckpointBuildTests.Add("part-0004.parquet"));
            File.Delete(capped.Native(0));
            var size = new FileInfo(Path.Combine(capped.Root, "t", "_delta_log", $"{3:D20}.checkpoint.parquet")).Length;
            var cappedLog = await Log(capped);
            Assert.Equal(CapabilityCheck.Unavailable, (await CleanupCapability.CheckAsync(capped.Configuration, capped.Local, cappedLog, Context(diskCap: size - 1))).Check);
            var retry = await CleanupEvidence.EstablishAsync(capped.Configuration, capped.Local, cappedLog, 4, null, true, Context(diskCap: size - 1));
            Assert.Equal(GateVerdict.Unavailable, retry.Verdict);
            Assert.Equal(GateAction.Retry, retry.Action);
            Assert.Equal(GateVerdict.Passed, (await CleanupEvidence.EstablishAsync(capped.Configuration, capped.Local, cappedLog, 4, null, true, Context())).Verdict);
        }

        [Fact]
        public async Task GateActionFollowsTheEvidenceDurability()
        {
            var table = Local();
            await Create(table, 2);
            var durable = await Establish(table, 2, durable: true);
            var volatileRun = await Establish(table, 2);
            await Commit(table, 3, Foreign(3), DeltaLakeCheckpointBuildTests.Add("part-0003.parquet"));

            var held = await Validate(table, durable);
            Assert.Equal(GateVerdict.ForeignHead, held.Verdict);
            Assert.True(held.Durable);
            Assert.Equal(GateAction.Rebase, held.Action);
            Assert.Null(durable.StoppedBy);
            var stopped = await Validate(table, volatileRun);
            Assert.False(stopped.Durable);
            Assert.Equal(GateAction.Stop, stopped.Action);
            Assert.Equal(GateVerdict.ForeignHead, volatileRun.StoppedBy);

            // Establishment and listing failures carry the run's kind too
            var anchored = await CleanupEvidence.EstablishAsync(table.Configuration, table.Local, await Log(table), 2, null, true, Context());
            Assert.Equal(GateVerdict.ForeignHead, anchored.Verdict);
            Assert.Equal(GateAction.Rebase, anchored.Action);
            Assert.Equal(GateAction.Retry, GateResult.OfListing(ListingCheck.StorageChanged, durable: true).Action);
            Assert.Equal(GateAction.Stop, GateResult.OfListing(ListingCheck.StorageChanged, durable: false).Action);
        }

        /// <summary>
        /// A pass-through the connector unwraps like its own wrappers; its read streams can park before a numbered read.
        /// </summary>
        private sealed class GatedStorage : IFileStorage, IConnectorOwnedStorage
        {
            public GatedStorage(IFileStorage inner)
            {
                Inner = inner;
            }

            public IFileStorage Inner { get; }

            // Awaited before each read of a stream, numbered from one
            public Func<int, Task>? BeforeRead { get; set; }

            public async Task<Stream?> OpenRead(IOPath path, CancellationToken cancellationToken = default)
            {
                var stream = await Inner.OpenRead(path, cancellationToken);
                return stream == null ? null : new GatedStream(stream, this);
            }

            public Task<IReadOnlyCollection<IOEntry>> Ls(IOPath? path = null, bool recurse = false, CancellationToken cancellationToken = default) => Inner.Ls(path, recurse, cancellationToken);

            public Task<IOEntry?> Stat(IOPath path, CancellationToken cancellationToken = default) => Inner.Stat(path, cancellationToken);

            public Task Rm(IOPath path, CancellationToken cancellationToken = default) => Inner.Rm(path, cancellationToken);

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

        private sealed class GatedStream : Stream
        {
            private readonly Stream _inner;
            private readonly GatedStorage _owner;
            private int _reads;

            public GatedStream(Stream inner, GatedStorage owner)
            {
                _inner = inner;
                _owner = owner;
            }

            public override bool CanRead => true;

            public override bool CanSeek => false;

            public override bool CanWrite => false;

            public override long Length => _inner.Length;

            public override long Position
            {
                get => _inner.Position;
                set => throw new NotSupportedException();
            }

            public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();

            public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
            {
                if (_owner.BeforeRead is { } before)
                {
                    await before(++_reads);
                }
                return await _inner.ReadAsync(buffer, cancellationToken);
            }

            public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) => ReadAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();

            public override void Flush()
            {
            }

            public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

            public override void SetLength(long value) => throw new NotSupportedException();

            public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();

            protected override void Dispose(bool disposing)
            {
                if (disposing)
                {
                    _inner.Dispose();
                }
                base.Dispose(disposing);
            }
        }
    }
}
