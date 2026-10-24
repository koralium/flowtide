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
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.Advance;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.References;
using Microsoft.Extensions.Logging.Abstractions;
using Stowage;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public partial class OrphanSweepTests
    {
        // An own commit at the version adding the names, as the sink publishes it
        private static Task PublishAsync(OrphanTable table, long version, params string[] names)
        {
            return DeltaTestKit.WriteOwnCommit(table.Raw, table.Name, version, 0, BoundarySelectorTests.Old(version), names.Select(x => DeltaLakeCheckpointBuildTests.Add(x)).ToArray());
        }

        // The sink's stage of an own commit at the version adding the names; returns its local path
        private static async Task<string> StageAsync(OrphanTable table, long version, params string[] names)
        {
            var actions = names.Select(x => DeltaLakeCheckpointBuildTests.Add(x)).Prepend(CommitInfo($"stage-{version}", version)).ToList();
            var staged = await DeltaTransactionWriter.StageCommit(table.Raw, table.Name, version, actions);
            return Path.Combine(table.Folder!, "_delta_log", staged.FileName);
        }

        private static DeltaAction CommitInfo(string stageId, long version)
        {
            return new DeltaAction() { CommitInfo = new DeltaCommitInfoAction() { StageId = stageId, AdoptedAt = 0, Timestamp = BoundarySelectorTests.Old(version), Operation = "WRITE" } };
        }

        // Rewrites only the given log listing after the root listing, 2 for L2 and 3 for L3, and answers its successor probe
        private static void AtListing(OrphanTable table, int listing, Func<List<IOEntry>, List<IOEntry>>? rewrite, Func<IOPath, IOEntry?, IOEntry?>? successor = null)
        {
            var log = table.Prefix + "_delta_log/";
            var probe = $"{log}{3:D20}.json";
            var listings = 0;
            var current = false;
            table.Owned.BeforeLs = folder =>
            {
                current = folder.Full == log && table.Owned.Recursive.Count == 1 && ++listings == listing - 1;
                return Task.CompletedTask;
            };
            if (rewrite != null)
            {
                table.Owned.AfterLs = listed => current ? rewrite(listed.ToList()) : listed;
            }
            if (successor != null)
            {
                table.Owned.StatAnswer = (path, entry) => current && path.Full == probe ? successor(path, entry) : entry;
            }
        }

        // Run files a finished sweep left in the process's scratch folder
        private static string[] RunFiles(CleanupContext context)
        {
            var runs = Path.Combine(context.Scratch.Owner.ProcessPath, "runs");
            return Directory.Exists(runs) ? Directory.GetFiles(runs) : Array.Empty<string>();
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task PublishBetweenL2AndL3IsProtected(bool staged)
        {
            var table = await LocalWithOldAsync(2, "old.parquet", "pub.parquet");
            var stage = staged ? await StageAsync(table, 3, "pub.parquet") : null;
            var sweep = Sweep(table.Configuration, table.Owned);
            var runs = Registered(sweep);
            sweep.StepForTests = async step =>
            {
                if (step == OrphanSweepStep.SecondListing)
                {
                    await PublishAsync(table, 3, "pub.parquet");
                    if (stage != null)
                    {
                        // Published and removed after L2 listed it, before it is read
                        File.Delete(stage);
                    }
                }
            };

            var (result, seen) = await RunAsync(sweep, table, OrphanContext(2));

            AssertResult(result, OutcomeKind.Completed, MaintenanceReason.None);
            Assert.Equal(new[] { "old.parquet", "pub.parquet" }, seen!.Frozen.Select(x => x.Relative).Order(StringComparer.Ordinal));
            Assert.Equal(new[] { "old.parquet" }, TargetsOf(Assert.Single(runs), table));
        }

        [Theory]
        [InlineData(StoreFlavor.S3)]
        // Local disk
        [InlineData(null)]
        public async Task L2SuccessorProbePrecedesL3(StoreFlavor? flavor)
        {
            if (flavor == null)
            {
                var table = await LocalWithOldAsync(2, "old.parquet");

                var (result, _) = await RunAsync(Sweep(table.Configuration, table.Owned), table, OrphanContext(2));

                AssertResult(result, OutcomeKind.Completed, MaintenanceReason.None);
                var successor = $"Stat {table.Prefix}_delta_log/{3:D20}.json";
                var log = $"Ls {table.Prefix}_delta_log/";
                Assert.Equal(new[] { successor, log, successor, log, successor }, table.Owned.CallsAfterRecursive().Take(5));
                return;
            }
            await using var server = new LoopbackObjectStore(flavor.Value);
            var run = await PinnedOrphanRun.SeedAsync(this, server);
            run.PutOld("part-old.parquet", 3);

            var swept = await run.SweepAsync();

            Assert.Equal(OutcomeKind.Completed, swept.Kind);
            var requests = run.Requests();
            var steps = new List<string>();
            foreach (var request in requests.Skip(run.RecursiveListing(requests)[^1] + 1))
            {
                var step = request == PinnedOrphanRun.Successor ? "HEAD"
                    : (request.Contains("comp=list") || request.Contains("list-type=2")) && request.Contains("delimiter=") ? "LIST"
                    : "OTHER";
                // A listing's pages are one step
                if (step != "LIST" || steps.LastOrDefault() != "LIST")
                {
                    steps.Add(step);
                }
            }
            Assert.Equal(new[] { "HEAD", "LIST", "HEAD", "LIST", "HEAD" }, steps.Take(5));
        }

        [Theory]
        [InlineData(2, "duplicate")]
        [InlineData(3, "duplicate")]
        [InlineData(2, "gap")]
        [InlineData(3, "gap")]
        [InlineData(2, "descendant")]
        [InlineData(3, "descendant")]
        [InlineData(2, "present")]
        [InlineData(3, "present")]
        [InlineData(2, "error")]
        [InlineData(3, "error")]
        [InlineData(2, "fence")]
        [InlineData(3, "fence")]
        public async Task L2AndL3EachValidateBeforeUnion(int listing, string fault)
        {
            var table = await LocalWithOldAsync(2, "old.parquet");
            var log = table.Prefix + "_delta_log/";
            ListingCheck? check = fault switch
            {
                "duplicate" => ListingCheck.OrderOrDuplicate,
                "gap" => ListingCheck.RangeGap,
                "descendant" => ListingCheck.UnsupportedSubfolder,
                "present" => ListingCheck.SuccessorPresent,
                "error" => ListingCheck.ProbeError,
                _ => null
            };
            switch (fault)
            {
                case "duplicate":
                    AtListing(table, listing, listed => listed.Append(listed.Single(x => x.Name == $"{0:D20}.json")).ToList());
                    break;
                case "gap":
                    AtListing(table, listing, listed => listed.Where(x => x.Name != $"{1:D20}.json").ToList());
                    break;
                case "descendant":
                    AtListing(table, listing, listed => listed.Append(new IOEntry(log + "sub/x.json") { Size = 1, LastModificationTime = s_old }).ToList());
                    break;
                case "present":
                    AtListing(table, listing, null, (path, _) => new IOEntry(path.Full) { Size = 1 });
                    break;
                case "error":
                    AtListing(table, listing, null, (_, _) => throw new IOException("probe"));
                    break;
                default:
                    // Another advance's started record
                    var started = ControlName.Group(ControlKind.Started, 1, "a", new byte[32], "b");
                    AtListing(table, listing, listed => listed.Append(new IOEntry(log + started) { Size = 10, LastModificationTime = s_old }).ToList());
                    break;
            }
            var counters = new RetentionCounters();
            var sweep = Sweep(table.Configuration, table.Owned, counters: counters);
            var runs = Registered(sweep);

            var (result, _) = await RunAsync(sweep, table, OrphanContext(2));

            AssertResult(result, OutcomeKind.Declined, check == null ? MaintenanceReason.CleanupGate : MaintenanceReason.ListingIneligible);
            if (check is { } failed)
            {
                Assert.Equal(1, counters.ListingDeclines(failed));
            }
            Assert.Empty(runs);
            // Declined at the faulty listing: an L2 fault is never followed by L3
            Assert.Equal(listing - 1, table.Owned.CallsAfterRecursive().Count(x => x == "Ls " + log));
        }

        [Theory]
        [InlineData("own")]
        [InlineData("none")]
        [InlineData("foreign")]
        [InlineData("mismatch")]
        public async Task VanishedStageRequiresMatchingPublishedCommit(string commit)
        {
            var table = await LocalWithOldAsync(2, "old.parquet", "staged.parquet");
            var stage = await StageAsync(table, 3, "staged.parquet");
            var sweep = Sweep(table.Configuration, table.Owned);
            var runs = Registered(sweep);
            sweep.StepForTests = async step =>
            {
                if (step != OrphanSweepStep.SecondListing)
                {
                    return;
                }
                // Gone after L2 listed it, before it is read
                File.Delete(stage);
                switch (commit)
                {
                    case "own":
                        await PublishAsync(table, 3, "staged.parquet");
                        break;
                    case "foreign":
                        await DeltaTestKit.WriteCommit(table.Raw, table.Name, 3, DeltaLakeCheckpointBuildTests.Add("staged.parquet"));
                        break;
                    case "mismatch":
                        // An own commit of another attempt in the same epoch
                        await DeltaTransactionWriter.WriteCommit(table.Raw, table.Name, 3, new List<DeltaAction>() { CommitInfo("stage-other", 3), DeltaLakeCheckpointBuildTests.Add("staged.parquet") });
                        break;
                }
            };

            var (result, seen) = await RunAsync(sweep, table, OrphanContext(2));

            Assert.Equal(new[] { "old.parquet", "staged.parquet" }, seen!.Frozen.Select(x => x.Relative).Order(StringComparer.Ordinal));
            if (commit == "own")
            {
                AssertResult(result, OutcomeKind.Completed, MaintenanceReason.None);
                Assert.Equal(new[] { "old.parquet" }, TargetsOf(Assert.Single(runs), table));
            }
            else
            {
                AssertResult(result, OutcomeKind.Declined, MaintenanceReason.Transient);
                Assert.Empty(runs);
            }
        }

        [Theory]
        [InlineData(2)]
        [InlineData(3)]
        public async Task TruncatedProtectionListingCannotAuthorizeDeletion(int listing)
        {
            var table = await LocalWithOldAsync(2, "old.parquet", "pub.parquet");
            var counters = new RetentionCounters();
            var sweep = Sweep(table.Configuration, table.Owned, counters: counters);
            var runs = Registered(sweep);
            var publishAt = listing == 2 ? OrphanSweepStep.Drained : OrphanSweepStep.SecondListing;
            sweep.StepForTests = async step =>
            {
                if (step == publishAt)
                {
                    await PublishAsync(table, 3, "pub.parquet");
                }
            };
            // The listing omits the commit its successor probe then finds
            AtListing(table, listing, listed => listed.Where(x => x.Name != $"{3:D20}.json").ToList());

            var (result, _) = await RunAsync(sweep, table, OrphanContext(2));

            AssertResult(result, OutcomeKind.Declined, MaintenanceReason.ListingIneligible);
            Assert.Equal(1, counters.ListingDeclines(ListingCheck.SuccessorPresent));
            Assert.Equal(1, counters.Probes(ProbeResult.Present));
            Assert.Empty(runs);
            Assert.True(File.Exists(Path.Combine(table.Folder!, "pub.parquet")));
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task LiveAtOldestRootIsProtected(bool rootRemoved)
        {
            var table = LocalTable();
            var published = new Dictionary<string, string>()
            {
                [RetentionProperties.LogKey] = RetentionProperties.Format(Retention),
                [RetentionProperties.DeletedFileKey] = RetentionProperties.Format(Retention)
            };
            // keep.parquet is added once by commit 0 and stays live
            for (long version = 0; version <= 4; version++)
            {
                var add = DeltaLakeCheckpointBuildTests.Add($"part-{version:D4}.parquet");
                var actions = version == 0
                    ? new[] { DeltaTestKit.Protocol(), DeltaTestKit.Metadata(DeltaTestKit.UserSchema, published), DeltaLakeCheckpointBuildTests.Add("keep.parquet"), add }
                    : new[] { add, DeltaLakeCheckpointBuildTests.Remove($"part-{version - 1:D4}.parquet", BoundarySelectorTests.Old(version)) };
                await DeltaTestKit.WriteOwnCommit(table.Raw, table.Name, version, 0, BoundarySelectorTests.Old(version), actions);
            }
            using var kit = new AdvanceExecutionKit();
            await kit.BuildAsync(table.Raw, 2);
            if (rootRemoved)
            {
                // Establishment roots at the newest checkpoint
                await kit.BuildAsync(table.Raw, 3);
            }
            // Only the root at 2 still names keep.parquet
            File.Delete(Path.Combine(table.Folder!, "_delta_log", $"{0:D20}.json"));
            File.Delete(Path.Combine(table.Folder!, "_delta_log", $"{1:D20}.json"));
            Plant(table, "keep.parquet");
            Plant(table, "old.parquet");
            var sweep = Sweep(table.Configuration, table.Owned);
            long? oldest = null;
            sweep.Prepared = prepared => oldest = prepared.O;
            var runs = Registered(sweep);
            if (rootRemoved)
            {
                sweep.StepForTests = step =>
                {
                    if (step == OrphanSweepStep.SecondListing)
                    {
                        File.Delete(Path.Combine(table.Folder!, "_delta_log", $"{2:D20}.checkpoint.parquet"));
                    }
                    return Task.CompletedTask;
                };
            }

            var (result, seen) = await RunAsync(sweep, table, OrphanContext(4));

            Assert.Equal(2, oldest);
            Assert.Equal(new[] { "keep.parquet", "old.parquet" }, seen!.Frozen.Select(x => x.Relative).Order(StringComparer.Ordinal));
            if (rootRemoved)
            {
                AssertResult(result, OutcomeKind.Declined, MaintenanceReason.HistoryChanged);
                Assert.Contains("checkpoint 2 is not listed", result.Detail);
                Assert.Empty(runs);
            }
            else
            {
                AssertResult(result, OutcomeKind.Completed, MaintenanceReason.None);
                Assert.Equal(new[] { "old.parquet" }, TargetsOf(Assert.Single(runs), table));
            }
        }

        [Fact]
        public async Task StageFirstSeenInL3ProtectsFrozenCandidate()
        {
            var table = await LocalWithOldAsync(2, "old.parquet", "stage-only.parquet");
            var sweep = Sweep(table.Configuration, table.Owned);
            var runs = Registered(sweep);
            sweep.StepForTests = async step =>
            {
                if (step == OrphanSweepStep.SecondListing)
                {
                    // Staged after L2 listed the log; only L3 shows it
                    await StageAsync(table, 3, "stage-only.parquet");
                }
            };

            var (result, seen) = await RunAsync(sweep, table, OrphanContext(2));

            AssertResult(result, OutcomeKind.Completed, MaintenanceReason.None);
            Assert.Equal(new[] { "old.parquet", "stage-only.parquet" }, seen!.Frozen.Select(x => x.Relative).Order(StringComparer.Ordinal));
            Assert.Equal(new[] { "old.parquet" }, TargetsOf(Assert.Single(runs), table));
        }

        [Fact]
        public async Task MissingEntryWithoutAStageIdDeclines()
        {
            var table = await LocalWithOldAsync(2);
            var context = OrphanContext(2);
            var metered = Metered(table);
            var l3 = await OrphanProtection.ListAsync(table.Configuration, metered, 2, 0, "third log listing", new RetentionCounters(), context);
            using var protection = new OrphanProtection(table.Configuration, metered, PathCanonicalizer.Create(table.Configuration.Store, metered), context, PathSet.DefaultArenaCap);
            // The stage form around a name that is not a commit, gone before its read
            await protection.ReadStagesAsync(new List<IOEntry>() { new IOEntry($"{table.Prefix}_delta_log/.{0:D20}.jsox.x.tmp") { Size = 3 } }, context);

            var ended = await Assert.ThrowsAsync<AdvanceEndedException>(() => protection.ResolveAsync(l3, "t", context));

            AssertResult(ended.Result, OutcomeKind.Declined, MaintenanceReason.Transient);
            Assert.Contains("names no stage id", ended.Result.Detail);
            await context.SettleAsync();
        }

        [Fact]
        public async Task RegistrationStopsAtMaxDeletions()
        {
            var table = await LocalWithOldAsync(2, "e.parquet", "c.parquet", "a.parquet", "d.parquet", "b.parquet");
            var sweep = Sweep(table.Configuration, table.Owned);
            sweep.MaxDeletions = 3;
            var runs = Registered(sweep);

            var (result, seen) = await RunAsync(sweep, table, OrphanContext(2));

            AssertResult(result, OutcomeKind.Completed, MaintenanceReason.None);
            Assert.Equal(5, seen!.Frozen.Count);
            Assert.Equal(new[] { "a.parquet", "b.parquet", "c.parquet" }, TargetsOf(Assert.Single(runs), table));
            Assert.Contains("2 over", result.Detail);
        }

        [Fact]
        public async Task ProtectionCeilingRecheckedWithStages()
        {
            var table = await LocalWithOldAsync(2, "old.parquet");
            var sweep = Sweep(table.Configuration, table.Owned);
            // The three commits preparation counted fit; one more stage does not
            sweep.MaxProtectionReads = 3;
            var runs = Registered(sweep);
            sweep.StepForTests = async step =>
            {
                if (step == OrphanSweepStep.Drained)
                {
                    await StageAsync(table, 3, "x.parquet");
                }
            };

            var (result, seen) = await RunAsync(sweep, table, OrphanContext(2));

            Assert.Equal(3, seen!.ProtectionReads);
            AssertResult(result, OutcomeKind.Declined, MaintenanceReason.OrphanNotEligible);
            Assert.Empty(runs);
        }

        [Fact]
        public async Task CapacityReservationDeclinesWithoutRegistering()
        {
            var table = await LocalWithOldAsync(2, "old.parquet");
            var ledger = new CapacityLedger(() => default);
            var sweep = Sweep(table.Configuration, table.Owned);
            var runs = Registered(sweep);
            var warned = new List<MaintenanceReason>();
            sweep.Warn = (reason, _) => warned.Add(reason);
            sweep.StepForTests = step =>
            {
                if (step == OrphanSweepStep.Protected)
                {
                    // Other runnable demand passes twelve hours after the sweep was admitted
                    ledger.Restore(default, "other", "advance", new CapacityDemand((long)CapacityLedger.CloseSeconds * ledger.RequestsPerSecond + 1, 0), CapacityState.Runnable);
                }
                return Task.CompletedTask;
            };
            var context = OrphanContext(2, capacity: ledger);

            var (result, _) = await RunAsync(sweep, table, context);

            AssertResult(result, OutcomeKind.Declined, MaintenanceReason.DeleteBacklog);
            Assert.Equal(new[] { MaintenanceReason.DeleteBacklog }, warned);
            Assert.Empty(runs);
            Assert.Null(ledger.StateOf(default, OrphanRun.LeaseKeyOf(ProducerKey(table))));
            Assert.Equal(0, context.Scratch.Ledger.Reserved);
            Assert.Empty(RunFiles(context));
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task ForeignOrNewEpochHeadAtProtectionDeclines(bool foreign)
        {
            var table = await LocalWithOldAsync(2, "old.parquet");
            var sweep = Sweep(table.Configuration, table.Owned);
            var runs = Registered(sweep);
            sweep.StepForTests = async step =>
            {
                if (step != OrphanSweepStep.Drained)
                {
                    return;
                }
                if (foreign)
                {
                    await DeltaTestKit.WriteCommit(table.Raw, table.Name, 3, DeltaLakeCheckpointBuildTests.Add("x.parquet"));
                }
                else
                {
                    // An own commit of a later adoption
                    await DeltaTestKit.WriteOwnCommit(table.Raw, table.Name, 3, 3, BoundarySelectorTests.Old(3), DeltaLakeCheckpointBuildTests.Add("x.parquet"));
                }
            };

            var (result, seen) = await RunAsync(sweep, table, OrphanContext(2));

            Assert.Equal("old.parquet", Assert.Single(seen!.Frozen).Relative);
            AssertResult(result, OutcomeKind.Declined, foreign ? MaintenanceReason.CleanupGate : MaintenanceReason.HistoryChanged);
            Assert.False(result.ForeignHead);
            Assert.Empty(runs);
        }

        [Theory]
        [InlineData("Drained", "deadline")]
        [InlineData("SecondListing", "deadline")]
        [InlineData("ThirdListing", "deadline")]
        [InlineData("Protected", "deadline")]
        [InlineData("Emitted", "deadline")]
        [InlineData("Drained", "stop")]
        [InlineData("SecondListing", "stop")]
        [InlineData("ThirdListing", "stop")]
        [InlineData("Protected", "stop")]
        [InlineData("Emitted", "stop")]
        [InlineData("Emitted", "register")]
        // Stopped while the barrier waits for a writer, before it drains
        [InlineData("Drained", "barrier")]
        [InlineData("Emitted", "finish")]
        [InlineData("Emitted", "settings")]
        public async Task EveryEndingAfterTheFreezeReleasesEverything(string at, string ending)
        {
            var table = await LocalWithOldAsync(2, "old.parquet", "other.parquet");
            var clock = new TimerClock(Now);
            using var stop = new CancellationTokenSource();
            var ledger = new CapacityLedger(() => default);
            var memory = new JobMemory();
            var producers = new ProducerGenerations();
            using var holder = producers.Acquire(ProducerKey(table));
            // A writer of the old generation keeps the barrier waiting
            ProducerToken? writer = ending == "barrier" ? holder.Enter() : null;
            var run = new JobRun(2, JobKind.Orphan) { Cancellation = new CancellationTokenSource() };
            using var cancellation = run.Cancellation;
            var context = new CleanupContext(run, clock)
            {
                Kind = JobKind.Orphan,
                Long = true,
                ReadLimit = long.MaxValue,
                Deadline = clock.GetUtcNow() + TimeSpan.FromHours(6),
                Head = 2,
                TableId = null,
                Scratch = Scratch(DiskLedger.DefaultCap),
                Memory = memory,
                Transfer = new TransferAllowance(),
                Stop = stop.Token,
                Producers = producers,
                Capacity = ledger
            };
            var key = OrphanRun.LeaseKeyOf(ProducerKey(table));
            // A small arena spills the path sets to scratch
            var sweep = new OrphanSweep(table.Configuration, Metered(table), clock, NullLogger.Instance, table.Name, new RetentionCounters()) { ArenaCap = EndingArena };
            OrphanInventory? frozen = null;
            sweep.FrozenHookForTests = inventory => frozen = inventory;
            var step = Enum.Parse<OrphanSweepStep>(at);
            var reached = false;
            sweep.StepForTests = current =>
            {
                if (current == step)
                {
                    switch (ending)
                    {
                        case "deadline":
                            reached = true;
                            clock.Fake.Advance(TimeSpan.FromHours(7));
                            break;
                        case "stop":
                            reached = true;
                            stop.Cancel();
                            break;
                        case "finish":
                            reached = true;
                            // A file where the runs folder goes fails the move out of the job
                            File.WriteAllText(Path.Combine(context.Scratch.Owner.ProcessPath, "runs"), "blocked");
                            break;
                    }
                }
                return Task.CompletedTask;
            };
            switch (ending)
            {
                case "register":
                    sweep.Register = registered =>
                    {
                        // The targets file has left the job and the lease is held
                        reached = File.Exists(registered.Targets.Path) && ledger.StateOf(default, key) == CapacityState.Runnable;
                        throw new InvalidOperationException("register");
                    };
                    break;
                case "settings":
                    sweep.RunSettings = () =>
                    {
                        reached = true;
                        throw new InvalidOperationException("settings");
                    };
                    break;
            }

            CleanupResult? result = null;
            OperationCanceledException? canceled = null;
            var sweeping = sweep.RunAsync(context);
            try
            {
                if (writer != null)
                {
                    await BarrierWaitsAsync(clock, sweeping);
                    reached = true;
                    stop.Cancel();
                }
                result = await sweeping;
            }
            catch (OperationCanceledException e)
            {
                canceled = e;
            }
            finally
            {
                writer?.Exit();
                await context.SettleAsync();
            }

            Assert.True(reached);
            switch (ending)
            {
                case "deadline":
                    AssertResult(result!, OutcomeKind.Preempted, MaintenanceReason.JobDeadline);
                    break;
                case "stop":
                case "barrier":
                    Assert.Equal(stop.Token, canceled!.CancellationToken);
                    break;
                default:
                    AssertResult(result!, OutcomeKind.Failed, MaintenanceReason.Transient);
                    Assert.Contains(ending == "finish" ? "already exists" : ending, result!.Detail);
                    break;
            }
            Assert.True(frozen!.Candidates.SpilledRuns > 0);
            Assert.Equal(0, memory.Managed);
            Assert.Equal(0, memory.Native);
            Assert.Empty(context.Scratch.Files);
            Assert.Equal(0, context.Scratch.Ledger.Reserved);
            Assert.Empty(RunFiles(context));
            Assert.Null(ledger.StateOf(default, key));
            Assert.Equal(default, ledger.Total(CapacityState.Runnable));
            Assert.Equal(0, frozen!.Identity.Owed);
            Assert.DoesNotContain(table.Owned.Calls, x => x.StartsWith("Rm ", StringComparison.Ordinal));
        }
    }
}
