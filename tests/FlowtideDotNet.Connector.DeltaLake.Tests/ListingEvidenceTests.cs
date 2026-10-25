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
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance;
using Stowage;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    [Collection(nameof(PinnedClientCollection))]
    public class ListingEvidenceTests
    {
        private const string Log = "/t/_delta_log/";

        private static string Commit(long version) => $"{version:D20}.json";

        private static IOEntry Entry(string full, long size = 1) => new IOEntry(full) { Size = size };

        // An unknown store whose user asserted its listing contract
        private static CleanupConfiguration Asserted(IFileStorage storage) => CleanupConfiguration.Validate(storage, "t", true, CleanupOptIns.Off);

        private static ScriptedStorage LogWith(params long[] versions)
        {
            var storage = new ScriptedStorage();
            foreach (var version in versions)
            {
                storage.Put(Log + Commit(version));
            }
            return storage;
        }

        private static List<string> Stats(ScriptedStorage storage) => storage.Requests.Where(x => x.StartsWith("Stat ")).ToList();

        [Fact]
        public async Task ListingDuplicateOrRegressionDeclinesBeforeClassification()
        {
            var storage = LogWith(0, 1, 2, 3);
            var configuration = Asserted(storage);
            List<IOEntry>? delivered = null;
            storage.AfterList = entries =>
            {
                entries.Insert(2, Entry(entries[2].Path.Full));
                delivered = entries;
                return entries;
            };

            var (duplicate, log) = await ListingEvidence.ReadLogAsync(configuration, storage, -1, default);

            Assert.Equal(ListingCheck.OrderOrDuplicate, duplicate);
            Assert.Null(log);
            // The foreground classification would have hidden the repeat in its dictionary
            Assert.Equal(4, DeltaTransactionReader.Classify(delivered!).Commits.Count);
            // No probe follows a failed structural check
            Assert.Empty(Stats(storage));

            storage.AfterList = entries => new List<IOEntry> { entries[0], entries[2], entries[1], entries[3] };
            Assert.Equal(ListingCheck.OrderOrDuplicate, (await ListingEvidence.ReadLogAsync(configuration, storage, -1, default)).Check);

            // Pinned cloud deliveries get the same checks and no sorting
            var s3 = CleanupStoreContract.Classify(StoreFixtures.S3(), "c/t", false);
            var folder = "/c/t/_delta_log/";
            Assert.Equal(ListingCheck.Passed, ListingEvidence.Check(s3, folder, false, new List<IOEntry> { Entry(folder + Commit(0)), Entry(folder + Commit(1)) }, out _));
            Assert.Equal(ListingCheck.OrderOrDuplicate, ListingEvidence.Check(s3, folder, false, new List<IOEntry> { Entry(folder + Commit(0)), Entry(folder + Commit(1)), Entry(folder + Commit(1)) }, out _));
            Assert.Equal(ListingCheck.OrderOrDuplicate, ListingEvidence.Check(s3, folder, false, new List<IOEntry> { Entry(folder + Commit(1)), Entry(folder + Commit(0)) }, out _));
        }

        [Theory]
        // Advance preparation and orphan protection rely on [O, head]
        [InlineData("advance", 0, 9, false)]
        [InlineData("orphan", 2, 9, false)]
        // Discard hints and dead-stage payload rely on [N, head]
        [InlineData("discard", 4, 9, false)]
        [InlineData("dead-stage", 6, 9, true)]
        // Continuation relies on [C, head]; deletions below C are intended
        [InlineData("continuation", 6, 9, true)]
        [InlineData("continuation", 5, 9, false)]
        // Foreign rebase relies on (H_m, head]
        [InlineData("rebase", 5, 9, true)]
        [InlineData("rebase", 4, 9, false)]
        // Recovery relies on [O_old, C]
        [InlineData("recovery", 1, 7, false)]
        [InlineData("recovery", 6, 8, true)]
        // Positive stage-only removal relies only on its own proof's range
        [InlineData("stage-only", 7, 7, true)]
        [InlineData("stage-only", 5, 5, false)]
        // Nothing beyond the head or below zero is established
        [InlineData("beyond-head", 8, 10, false)]
        [InlineData("negative", -1, 3, false)]
        public async Task MissingInteriorCommitDeclinesEveryReliedRange(string decision, long from, long to, bool passes)
        {
            var storage = LogWith(0, 1, 2, 3, 4, 6, 7, 8, 9);
            var (check, log) = await ListingEvidence.ReadLogAsync(Asserted(storage), storage, -1, default);
            Assert.Equal(ListingCheck.Passed, check);
            Assert.Equal(9, log!.Head);

            var start = decision == "rebase" ? from + 1 : from;

            Assert.Equal(passes ? ListingCheck.Passed : ListingCheck.RangeGap, log.CheckRange(start, to));
        }

        [Fact]
        public async Task ListedHeadPlusOneExistsDeclines()
        {
            // A listing that looks complete but misses the newest commit
            var storage = LogWith(0, 1, 2, 3, 4);
            storage.AfterList = entries => entries.Where(x => x.Name != Commit(4)).ToList();

            var (check, log) = await ListingEvidence.ReadLogAsync(Asserted(storage), storage, -1, default);

            Assert.Equal(ListingCheck.SuccessorPresent, check);
            Assert.Null(log);
            Assert.Equal(new[] { $"Stat {Log}{Commit(4)}" }, Stats(storage));
        }

        [Fact]
        public async Task SuccessorStatErrorIsNotAbsence()
        {
            var storage = LogWith(0, 1);
            var configuration = Asserted(storage);
            storage.StatAnswer = (_, _) => throw new IOException("Injected");
            Assert.Equal(ListingCheck.ProbeError, (await ListingEvidence.ReadLogAsync(configuration, storage, -1, default)).Check);
            storage.StatAnswer = (_, _) => throw new OperationCanceledException();
            Assert.Equal(ListingCheck.ProbeError, (await ListingEvidence.ReadLogAsync(configuration, storage, -1, default)).Check);
            storage.StatAnswer = null;
            // A stopped probe is no answer at all, so it surfaces as the stop
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => ListingEvidence.ProbeSuccessor(storage, "t", 1, new CancellationToken(true)));
            Assert.Equal(ListingCheck.Passed, (await ListingEvidence.ReadLogAsync(configuration, storage, -1, default)).Check);

            // The pinned clients return null only for a 404 and throw on every other status
            foreach (var flavor in new[] { StoreFlavor.Azure, StoreFlavor.S3 })
            {
                await using var server = new LoopbackObjectStore(flavor);
                using var cloud = server.CreateStorage();
                server.Put("t/_delta_log/" + Commit(0));
                Assert.Equal(ListingCheck.Passed, await ListingEvidence.ProbeSuccessor(cloud, "/c/t", 0, default));
                Assert.Equal(ListingCheck.SuccessorPresent, await ListingEvidence.ProbeSuccessor(cloud, "/c/t", -1, default));
                foreach (var status in new[] { 400, 403, 500, 503 })
                {
                    server.Intercept = (method, _) => method == "HEAD" ? (status, "") : null;
                    Assert.Equal(ListingCheck.ProbeError, await ListingEvidence.ProbeSuccessor(cloud, "/c/t", 0, default));
                }
            }
        }

        [Fact]
        public async Task StopDuringSuccessorProbeIsNotAProbeError()
        {
            var storage = LogWith(0, 1);
            var configuration = Asserted(storage);
            using var stop = new CancellationTokenSource();
            // The worker stops while the probe is in flight
            storage.StatAnswer = (_, _) =>
            {
                stop.Cancel();
                throw new OperationCanceledException(stop.Token);
            };

            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => ListingEvidence.ReadLogAsync(configuration, storage, -1, stop.Token));
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => ListingEvidence.ProbeSuccessor(storage, "t", 1, stop.Token));
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => ListingEvidence.ReadStagesAsync(configuration, storage, -1, stop.Token));
            // A cancellation the stop token did not cause is still a failed probe
            storage.StatAnswer = (_, _) => throw new OperationCanceledException();
            Assert.Equal(ListingCheck.ProbeError, (await ListingEvidence.ReadLogAsync(configuration, storage, -1, default)).Check);
        }

        [Fact]
        public async Task IneligibleListableStoreGivesStageOnlyEvidence()
        {
            // An unknown store without the assertion: absence proves nothing, but a listed commit still proves its stage
            var storage = LogWith(0, 1, 2);
            var staged = "." + Commit(1) + ".s1.tmp";
            storage.Put(Log + staged);
            storage.Put(Log + "." + Commit(3) + ".s3.tmp");
            storage.Put(Log + ".flowtide-0123456789abcdef0123456789abcdef.checkpoint.tmp");
            var unasserted = CleanupConfiguration.Validate(storage, "t", false, CleanupOptIns.Off);
            Assert.False(unasserted.Store.Eligible);
            Assert.Equal(ListingCheck.StoreIneligible, (await ListingEvidence.ReadLogAsync(unasserted, storage, -1, default)).Check);

            var (check, stages) = await ListingEvidence.ReadStagesAsync(unasserted, storage, -1, default);

            Assert.Equal(ListingCheck.Passed, check);
            Assert.Equal(2, stages!.Head);
            // Only the stage whose commit is listed; the stage above the head and the temporary are not shown
            var stage = Assert.Single(stages.Stages);
            Assert.Equal(1, stage.Version);
            Assert.Equal(staged, stage.Stage.Name);
            Assert.Equal(Commit(1), stage.Target.IOEntry.Name);
            Assert.Equal(ListingCheck.Passed, stages.CheckRange(1, 2));
            Assert.Null(stages.ListedCommit(3));
            Assert.Equal(new[] { $"Stat {Log}{Commit(3)}" }, Stats(storage));

            // Every check ReadLogAsync makes still applies
            var entries = (await storage.Ls(Log)).ToList();
            Assert.Equal(ListingCheck.StoreIneligible, ListingEvidence.Check(unasserted.Store, Log, false, entries, out _));
            Assert.Equal(ListingCheck.Passed, ListingEvidence.Check(unasserted.Store, Log, false, entries, out var listing, purpose: EvidencePurpose.StageOnly));
            Assert.Equal(EvidencePurpose.StageOnly, listing!.Purpose);
            Assert.Equal(ListingCheck.RangeGap, (await ListingEvidence.ReadStagesAsync(unasserted, storage, 3, default)).Check);
            storage.AfterList = x => x.AsEnumerable().Reverse().ToList();
            Assert.Equal(ListingCheck.OrderOrDuplicate, (await ListingEvidence.ReadStagesAsync(unasserted, storage, -1, default)).Check);
            storage.AfterList = null;
            storage.Put(Log + Commit(3));
            storage.AfterList = x => x.Where(e => e.Name != Commit(3)).ToList();
            Assert.Equal(ListingCheck.SuccessorPresent, (await ListingEvidence.ReadStagesAsync(unasserted, storage, -1, default)).Check);
            storage.AfterList = null;
            storage.Put(Log + ".flowtide-cleanup.future");
            Assert.Equal(ListingCheck.ControlFenced, (await ListingEvidence.ReadStagesAsync(unasserted, storage, -1, default)).Check);

            // A store without Stat fails the successor probe, so even stage-only removal declines on it
            var gcs = LogWith(0);
            gcs.StatAnswer = (_, _) => throw new NotImplementedException();
            var noStat = CleanupConfiguration.Validate(CleanupStoreContract.Create(gcs, new StoreDescriptor { Kind = StoreKind.Gcs, KnownFailed = true, ExactDeletes = true, Detail = "no Stat" }, "t", true), CleanupOptIns.Off);
            Assert.Equal(ListingCheck.ProbeError, (await ListingEvidence.ReadStagesAsync(noStat, gcs, -1, default)).Check);
            // An unqualified table path is never listed for evidence
            var unqualified = LogWith(0);
            Assert.Equal(ListingCheck.StoreIneligible, (await ListingEvidence.ReadStagesAsync(CleanupConfiguration.Validate(unqualified, "x/../t", false, CleanupOptIns.Off), unqualified, -1, default)).Check);
            Assert.Empty(unqualified.Requests);

            // Stage-only evidence shows nothing a gate, a payload or an absence decision takes
            var exposed = typeof(StageEvidence).GetProperties().Select(x => x.PropertyType).Concat(typeof(StageEvidence).GetMethods().Select(x => x.ReturnType)).ToList();
            Assert.DoesNotContain(exposed, x => x == typeof(LogEvidence) || x == typeof(EvidenceListing) || x == typeof(DeltaTransactionReader.LogListing) || x == typeof(ControlFence)
                || x == typeof(IOEntry) || x == typeof(IReadOnlyList<IOEntry>) || x == typeof(IEnumerable<IOEntry>));
        }

        [Fact]
        public async Task SuccessorPublishedBetweenListAndStatRetriesWithoutDeletion()
        {
            var storage = LogWith(0, 1, 2);
            var configuration = Asserted(storage);
            // A commit lands after the listing and before the probe
            storage.OnListed = () =>
            {
                storage.OnListed = null;
                storage.Put(Log + Commit(3));
            };

            var (first, firstLog) = await ListingEvidence.ReadLogAsync(configuration, storage, -1, default);

            Assert.Equal(ListingCheck.SuccessorPresent, first);
            Assert.Null(firstLog);
            // The retry sees the new head and its absent successor
            var (second, secondLog) = await ListingEvidence.ReadLogAsync(configuration, storage, -1, default);
            Assert.Equal(ListingCheck.Passed, second);
            Assert.Equal(3, secondLog!.Head);
            Assert.Equal(new[] { $"Stat {Log}{Commit(3)}", $"Stat {Log}{Commit(4)}" }, Stats(storage));
            Assert.DoesNotContain(storage.Requests, x => x.StartsWith("Rm "));
        }

        [Fact]
        public async Task EmptyListingProbesVersionZero()
        {
            var storage = new ScriptedStorage();
            storage.Put("/t/part-1.parquet");
            var configuration = Asserted(storage);

            var (check, log) = await ListingEvidence.ReadLogAsync(configuration, storage, -1, default);

            Assert.Equal(ListingCheck.Passed, check);
            Assert.Equal(-1, log!.Head);
            Assert.Equal(new[] { $"Stat {Log}{Commit(0)}" }, Stats(storage));
            Assert.Equal(ListingCheck.Passed, log.CheckRange(0, -1));
            Assert.Equal(ListingCheck.RangeGap, log.CheckRange(0, 0));

            // A table whose version 0 hides from the listing is not empty
            storage.Put(Log + Commit(0));
            storage.AfterList = entries => entries.Where(x => x.Name != Commit(0)).ToList();
            Assert.Equal(ListingCheck.SuccessorPresent, (await ListingEvidence.ReadLogAsync(configuration, storage, -1, default)).Check);

            // A published version forbids reading the listing as an empty table, before any probe
            storage.Requests.Clear();
            Assert.Equal(ListingCheck.RangeGap, (await ListingEvidence.ReadLogAsync(configuration, storage, 0, default)).Check);
            Assert.Empty(Stats(storage));
        }

        [Fact]
        public async Task HeadOverflowDeclines()
        {
            var storage = LogWith(long.MaxValue);
            var configuration = Asserted(storage);

            var (check, log) = await ListingEvidence.ReadLogAsync(configuration, storage, -1, default);

            Assert.Equal(ListingCheck.Overflow, check);
            Assert.Null(log);
            Assert.Empty(Stats(storage));
            Assert.Equal(ListingCheck.Overflow, await ListingEvidence.ProbeSuccessor(storage, "t", long.MaxValue, default));
            Assert.Empty(Stats(storage));
            // The largest representable successor is still probed, and a head below an empty log is no head
            Assert.Equal(ListingCheck.SuccessorPresent, await ListingEvidence.ProbeSuccessor(storage, "t", long.MaxValue - 1, default));
            Assert.Equal(ListingCheck.ProbeError, await ListingEvidence.ProbeSuccessor(storage, "t", -2, default));
            Assert.Single(Stats(storage));
        }

        [Fact]
        public async Task LocalUnorderedSnapshotGetsOrderedEvidenceView()
        {
            var directory = StoreFixtures.TempDirectory(nameof(LocalUnorderedSnapshotGetsOrderedEvidenceView));
            using var local = Files.Of.LocalDisk(directory);
            var contract = CleanupStoreContract.Classify(local, "t", false);
            Assert.True(contract.Eligible);
            var unordered = new List<IOEntry> { Entry(Log + Commit(2)), Entry(Log + Commit(0)), Entry(Log + "_commits/"), Entry(Log + Commit(1)) };

            Assert.Equal(ListingCheck.Passed, ListingEvidence.Check(contract, Log, false, unordered, out var listing));

            Assert.Equal(new[] { Commit(0), Commit(1), Commit(2), "_commits/" }, Enumerable.Range(0, listing!.Count).Select(listing.RelativeName));
            // The sorted view keeps a repeated name for rejection
            var repeated = new List<IOEntry> { Entry(Log + Commit(1)), Entry(Log + Commit(0)), Entry(Log + Commit(1)) };
            Assert.Equal(ListingCheck.OrderOrDuplicate, ListingEvidence.Check(contract, Log, false, repeated, out _));
            // The view is in UTF-8 order, which the uniqueness check uses
            var foreign = new List<IOEntry> { Entry(Log + "a\U0001F600"), Entry(Log + "a！") };
            Assert.Equal(ListingCheck.Passed, ListingEvidence.Check(contract, Log, false, foreign, out listing));
            Assert.Equal(new[] { "a！", "a\U0001F600" }, Enumerable.Range(0, listing!.Count).Select(listing.RelativeName));
            // A cloud listing is never sorted into validity
            var s3 = CleanupStoreContract.Classify(StoreFixtures.S3(), "c/t", false);
            var cloud = new List<IOEntry> { Entry("/c/t/_delta_log/" + Commit(1)), Entry("/c/t/_delta_log/" + Commit(0)) };
            Assert.Equal(ListingCheck.OrderOrDuplicate, ListingEvidence.Check(s3, "/c/t/_delta_log/", false, cloud, out _));

            // A real local log written newest first reads as an ordered log
            foreach (var version in new[] { 3, 1, 2, 0 })
            {
                await local.WriteText($"/t/_delta_log/{Commit(version)}", "{}");
            }
            // A completion record fences nothing, so the listing still serves as evidence
            const string done = ".flowtide-cleanup.00000000000000000002.a.0123456789abcdef.b.done";
            await local.WriteText("/t/_delta_log/" + done, "{}");
            await local.WriteText("/t/_delta_log/_last_checkpoint", "{}");
            await local.WriteText("/t/_delta_log/." + Commit(4) + ".abc.tmp", "{}");
            var configuration = CleanupConfiguration.Validate(local, "t", false, CleanupOptIns.Off);
            var (check, log) = await ListingEvidence.ReadLogAsync(configuration, local, 3, default);
            Assert.Equal(ListingCheck.Passed, check);
            Assert.Equal(3, log!.Head);
            Assert.Equal(ListingCheck.Passed, log.CheckRange(0, 3));
            Assert.Equal(done, Assert.Single(log.Listing.Controls).Name);
        }

        [Fact]
        public void MixedComparatorOnlyPerPairWouldPassButWholeSequenceDeclines()
        {
            var azure = CleanupStoreContract.Classify(StoreFixtures.Azure(), "c/t", false);
            // The block the pinned client puts first for a/c, then provider entries
            IReadOnlyCollection<IOEntry> Delivered(params string[] names) => new[] { new IOEntry("/c/a/") }.Concat(names.Select(x => Entry("/c/t/" + x))).ToList();
            var mixed = new[] { "a-b", "a/c", "a-d" };
            // Each pair is increasing under one of the orders
            Assert.True(ListingEvidence.CompareUtf8("/c/t/a-b", "/c/t/a/c") < 0);
            Assert.True(ListingEvidence.CompareSlashLowest("/c/t/a/c", "/c/t/a-d") < 0);

            Assert.Equal(ListingCheck.OrderOrDuplicate, ListingEvidence.Check(azure, "/c/t/", true, Delivered(mixed), out _));

            // Either order alone holds for a whole sequence
            Assert.Equal(ListingCheck.Passed, ListingEvidence.Check(azure, "/c/t/", true, Delivered("a-b", "a-d", "a/c"), out _));
            Assert.Equal(ListingCheck.Passed, ListingEvidence.Check(azure, "/c/t/", true, Delivered("a/c", "a-b", "a-d"), out _));
            // Equality fails both
            Assert.Equal(ListingCheck.OrderOrDuplicate, ListingEvidence.Check(azure, "/c/t/", true, Delivered("a-b", "a-b", "a/c"), out _));
            // Nonrecursive leaf names and S3 use the ordinary order only
            var s3 = CleanupStoreContract.Classify(StoreFixtures.S3(), "c/t", false);
            Assert.Equal(ListingCheck.OrderOrDuplicate, ListingEvidence.Check(s3, "/c/t/", true, Delivered("a/c", "a-b", "a-d"), out _));
        }

        [Fact]
        public void EntryOutsideFolderDeclines()
        {
            var s3 = CleanupStoreContract.Classify(StoreFixtures.S3(), "c/t", false);
            var folder = "/c/t/_delta_log/";
            var inside = Entry(folder + Commit(0));
            foreach (var outside in new[] { "/c/t/_delta_logx/" + Commit(1), "/c/t/" + Commit(1), "/d/t/_delta_log/" + Commit(1), folder })
            {
                Assert.Equal(ListingCheck.Containment, ListingEvidence.Check(s3, folder, false, new List<IOEntry> { inside, Entry(outside) }, out _));
            }
            // A descendant of a nonrecursive folder is not one of its entries
            Assert.Equal(ListingCheck.UnsupportedSubfolder, ListingEvidence.Check(s3, folder, false, new List<IOEntry> { inside, Entry(folder + "x/" + Commit(1)) }, out _));

            // Recursive pinned listings check provider entries after the synthetic block, even ones the block recomputation cannot place
            var block = new IOEntry("/c/_delta_log/");
            var logEntry = Entry("/c/t/_delta_log/" + Commit(0));
            Assert.Equal(ListingCheck.Passed, ListingEvidence.Check(s3, "/c/t/", true, new List<IOEntry> { block, logEntry }, out _));
            Assert.Equal(ListingCheck.Containment, ListingEvidence.Check(s3, "/c/t/", true, new List<IOEntry> { block, logEntry, Entry("/c/tx/" + Commit(1)) }, out _));
            Assert.Equal(ListingCheck.Containment, ListingEvidence.Check(s3, "/c/t/", true, new List<IOEntry> { block, logEntry, Entry("/d/t/q/" + Commit(1)) }, out _));
            Assert.Equal(ListingCheck.Containment, ListingEvidence.Check(s3, "/c/t/", true, new List<IOEntry> { block, Entry("/c/x"), logEntry }, out _));

            // Local and asserted stores are held to the same containment
            var local = CleanupStoreContract.Classify(Files.Of.LocalDisk(StoreFixtures.TempDirectory(nameof(EntryOutsideFolderDeclines))), "t", false);
            Assert.Equal(ListingCheck.Containment, ListingEvidence.Check(local, Log, false, new List<IOEntry> { Entry(Log + Commit(0)), Entry("/t/_delta_logx") }, out _));
            var asserted = Asserted(new ScriptedStorage()).Store;
            Assert.Equal(ListingCheck.Containment, ListingEvidence.Check(asserted, Log, false, new List<IOEntry> { Entry(Log + Commit(0)), Entry("/u/_delta_log/" + Commit(1)) }, out _));
        }

        [Fact]
        public async Task IneligibleOrChangedStorageDeclinesBeforeAnyRequest()
        {
            var storage = LogWith(0);
            // Not asserted, so an unknown store is not eligible
            var unasserted = CleanupConfiguration.Validate(storage, "t", false, CleanupOptIns.Off);
            Assert.Equal(ListingCheck.StoreIneligible, (await ListingEvidence.ReadLogAsync(unasserted, storage, -1, default)).Check);
            var other = LogWith(0);
            Assert.Equal(ListingCheck.StorageChanged, (await ListingEvidence.ReadLogAsync(Asserted(storage), other, -1, default)).Check);
            Assert.Empty(storage.Requests);
            Assert.Empty(other.Requests);
            // An ineligible store's listing is never evidence, whatever it holds
            var entries = new List<IOEntry> { Entry(Log + Commit(0)) };
            Assert.Equal(ListingCheck.StoreIneligible, ListingEvidence.Check(unasserted.Store, Log, false, entries, out _));
            Assert.Equal(ListingCheck.StoreIneligible, ListingEvidence.Check(CleanupStoreContract.Classify(StoreFixtures.Gcs(), "t", true), Log, false, entries, out _));
            Assert.Equal(ListingCheck.Passed, ListingEvidence.Check(Asserted(storage).Store, Log, false, entries, out _));
            // A listing outside the table's bucket or under an unqualified prefix is never evidence
            var s3 = CleanupStoreContract.Classify(StoreFixtures.S3(), "c/t", false);
            Assert.Equal(ListingCheck.StoreIneligible, ListingEvidence.Check(s3, "/d/t/_delta_log/", false, new List<IOEntry>(), out _));
            Assert.Equal(ListingCheck.StoreIneligible, ListingEvidence.Check(s3, "/c/t x/_delta_log/", false, new List<IOEntry>(), out _));
            Assert.Equal(ListingCheck.StoreIneligible, ListingEvidence.Check(s3, "/c/", false, new List<IOEntry>(), out _));
        }

        [Fact]
        public async Task CheckpointAboveNewestCommitDeclines()
        {
            // Higher checkpoint or compaction evidence never stands in for missing JSON
            var storage = LogWith(0, 1);
            storage.Put(Log + "00000000000000000002.checkpoint.parquet");
            Assert.Equal(ListingCheck.RangeGap, (await ListingEvidence.ReadLogAsync(Asserted(storage), storage, -1, default)).Check);
            var compacted = LogWith(0, 1);
            compacted.Put(Log + "00000000000000000000.00000000000000000002.compacted.json");
            Assert.Equal(ListingCheck.RangeGap, (await ListingEvidence.ReadLogAsync(Asserted(compacted), compacted, -1, default)).Check);
            // Stage files never establish a head
            var staged = LogWith(0);
            staged.Put(Log + "." + Commit(1) + ".abc.tmp");
            var (check, log) = await ListingEvidence.ReadLogAsync(Asserted(staged), staged, -1, default);
            Assert.Equal(ListingCheck.Passed, check);
            Assert.Equal(0, log!.Head);
        }
    }
}
