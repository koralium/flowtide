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

using FlowtideDotNet.Connector.DeltaLake.Internal;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.Deletion;
using Microsoft.Extensions.Time.Testing;
using Stowage;
using System.Collections.Concurrent;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public partial class DeletionExecutorTests
    {
        private static TaskCompletionSource Signal() => new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        private static DeletionRun NewRun(DeletionExecutor executor, ChunkGate gate, IEnumerable<DeletionTarget> targets, DeletionRunSettings? settings = null, TimeProvider? clock = null, long cursor = 0)
        {
            return new DeletionRun(executor, gate, new ListDeletionSource(targets.ToList()), clock ?? TimeProvider.System, settings, cursor);
        }

        // A local table with own commits 0 to 2 and evidence anchored at 2
        private async Task<(LocalTable Table, CleanupEvidence Evidence)> Anchored(bool durable, params string[] files)
        {
            var table = Local();
            await Create(table.Local, "t", 2);
            foreach (var file in files)
            {
                table.Put(file);
            }
            return (table, await Establish(table.Configuration, table.Metered, 2, durable, Context()));
        }

        [Fact]
        public async Task UnsupportedDurableTargetSuspendsWithoutPassingCursor()
        {
            await using var server = new LoopbackObjectStore(StoreFlavor.S3);
            var cloud = Cloud(server);
            await Create(cloud.Client, "c/t", 2);
            foreach (var name in new[] { "a.parquet", "b.parquet", "part#x.parquet", "c.parquet", "part" })
            {
                server.Put("t/" + name);
            }
            var targets = new[] { cloud.Target("a.parquet"), cloud.Target("b.parquet"), cloud.Target("part#x.parquet"), cloud.Target("c.parquet") };
            var progress = new ConcurrentQueue<long>();
            // One outstanding delete, since the pinned S3 signer is not safe for concurrent requests
            var settings = new DeletionRunSettings()
            {
                MaxOutstanding = 1,
                SettleProgress = (cursor, _) =>
                {
                    progress.Enqueue(cursor);
                    return Task.FromResult(true);
                }
            };
            var evidence = await Establish(cloud.Configuration, cloud.Metered, 2, durable: true, Context());
            var run = NewRun(cloud.Executor, ChunkGate.ForEvidence(cloud.Configuration, cloud.Metered, evidence), targets, settings);

            var first = await run.RunSliceAsync(Context());

            Assert.Equal(SliceStatus.Suspended, first.Status);
            Assert.Equal(MaintenanceReason.PathRejected, first.Reason);
            Assert.Equal(2, first.Cursor);
            Assert.Equal(2, run.Cursor);
            Assert.Equal(new long[] { 2 }, progress);
            Assert.False(server.Contains("t/a.parquet"));
            Assert.False(server.Contains("t/b.parquet"));
            Assert.True(server.Contains("t/part#x.parquet"));
            Assert.True(server.Contains("t/part"));
            Assert.True(server.Contains("t/c.parquet"));
            Assert.DoesNotContain(server.Requests, x => x.StartsWith("DELETE /c/t/part", StringComparison.Ordinal) || x.StartsWith("DELETE /c/t/c.parquet", StringComparison.Ordinal));
            Assert.Equal(1, run.Tally.KeptFor(KeepReason.Unqualified));

            // The suspension holds: a later slice issues nothing and makes no request
            var requests = server.Requests.Count;
            var second = await run.RunSliceAsync(Context());
            Assert.Equal(SliceStatus.Suspended, second.Status);
            Assert.Equal(2, second.Cursor);
            Assert.Equal(requests, server.Requests.Count);

            // A restart from the recorded cursor suspends at the same target, and records no progress it did not make
            var restartedExecutor = new DeletionExecutor(cloud.Configuration, cloud.Metered);
            var restarted = NewRun(restartedExecutor, ChunkGate.ForEvidence(cloud.Configuration, cloud.Metered, await Establish(cloud.Configuration, cloud.Metered, 2, true, Context())), targets, settings, cursor: 2);
            var third = await restarted.RunSliceAsync(Context());
            Assert.Equal(SliceStatus.Suspended, third.Status);
            Assert.Equal(2, third.Cursor);
            Assert.Equal(new long[] { 2 }, progress);
            Assert.True(server.Contains("t/c.parquet"));

            // A volatile run counts the same target and passes it
            var volatileExecutor = new DeletionExecutor(cloud.Configuration, cloud.Metered);
            var volatileRun = NewRun(volatileExecutor, ChunkGate.ForEvidence(cloud.Configuration, cloud.Metered, await Establish(cloud.Configuration, cloud.Metered, 2, false, Context())), targets,
                new DeletionRunSettings() { MaxOutstanding = 1 });
            var passed = await volatileRun.RunSliceAsync(Context());
            Assert.Equal(SliceStatus.Completed, passed.Status);
            Assert.Equal(4, passed.Cursor);
            Assert.Equal(1, volatileExecutor.Tally.KeptFor(KeepReason.Unqualified));
            Assert.False(server.Contains("t/c.parquet"));
            Assert.True(server.Contains("t/part#x.parquet"));
            Assert.True(server.Contains("t/part"));
        }

        [Fact]
        public async Task SliceIssuesNothingUnlessItsGatePasses()
        {
            const string Log = "/t/_delta_log/";

            // A successor commit visible to the probe fails the listing; nothing is deleted and the run retries
            var (table, evidence) = await Anchored(false, "a.parquet", "b.parquet");
            var run = NewRun(table.Executor, ChunkGate.ForEvidence(table.Configuration, table.Metered, evidence), new[] { table.Target("a.parquet"), table.Target("b.parquet") });
            table.Owned.StatAnswer = (path, entry) => path.Full.EndsWith($"{3:D20}.json", StringComparison.Ordinal) ? new IOEntry(path.Full) { Size = 1 } : entry;
            var listingFailed = await run.RunSliceAsync(Context());
            Assert.Equal(SliceStatus.Retry, listingFailed.Status);
            Assert.Equal(MaintenanceReason.ListingIneligible, listingFailed.Reason);
            Assert.Equal(ListingCheck.SuccessorPresent, listingFailed.Gate!.Listing);
            Assert.Equal(0, listingFailed.Issued);
            Assert.True(File.Exists(table.Native("a.parquet")));
            // A listing that throws is transient
            table.Owned.StatAnswer = null;
            table.Owned.BeforeLs = _ => throw new IOException("injected");
            var listingThrew = await run.RunSliceAsync(Context());
            Assert.Equal(SliceStatus.Retry, listingThrew.Status);
            Assert.Equal(GateVerdict.Unavailable, listingThrew.Gate!.Verdict);
            Assert.True(File.Exists(table.Native("a.parquet")));
            // A stop or a due build found before the gate preempts the listing
            table.Owned.BeforeLs = null;
            var listings = table.Owned.Listings(Log);
            var stopped = await run.RunSliceAsync(Context(stop: new CancellationToken(true)));
            Assert.Equal(SliceStatus.Retry, stopped.Status);
            Assert.Equal(IssuanceStop.Stopped, stopped.Stop);
            var buildDue = Run();
            buildDue.YieldRequested = true;
            var yielded = await run.RunSliceAsync(Context(run: buildDue));
            Assert.Equal(SliceStatus.Retry, yielded.Status);
            Assert.Equal(IssuanceStop.BuildDue, yielded.Stop);
            Assert.Equal(listings, table.Owned.Listings(Log));
            // The same run passes its gate once the listing is sound
            var passed = await run.RunSliceAsync(Context());
            Assert.Equal(SliceStatus.Completed, passed.Status);
            Assert.False(File.Exists(table.Native("a.parquet")));
            Assert.False(File.Exists(table.Native("b.parquet")));

            foreach (var durable in new[] { false, true })
            {
                // A replaced anchor stops volatile work and suspends a durable run; the end is kept without another listing
                var (replaced, replacedEvidence) = await Anchored(durable, "a.parquet");
                var replacedRun = NewRun(replaced.Executor, ChunkGate.ForEvidence(replaced.Configuration, replaced.Metered, replacedEvidence), new[] { replaced.Target("a.parquet") });
                File.AppendAllText(replaced.Native($"_delta_log/{2:D20}.json"), "\n");
                var anchorReplaced = await replacedRun.RunSliceAsync(Context());
                Assert.Equal(durable ? SliceStatus.Suspended : SliceStatus.Stopped, anchorReplaced.Status);
                Assert.Equal(GateVerdict.AnchorReplaced, anchorReplaced.Gate!.Verdict);
                var replacedListings = replaced.Owned.Listings(Log);
                Assert.Equal(anchorReplaced, await replacedRun.RunSliceAsync(Context()));
                Assert.Equal(replacedListings, replaced.Owned.Listings(Log));
                Assert.True(File.Exists(replaced.Native("a.parquet")));

                // A foreign head stops volatile work and hands a durable run to its rebase
                var (foreign, foreignEvidence) = await Anchored(durable, "a.parquet");
                var foreignRun = NewRun(foreign.Executor, ChunkGate.ForEvidence(foreign.Configuration, foreign.Metered, foreignEvidence), new[] { foreign.Target("a.parquet") });
                await DeltaTestKit.WriteCommit(foreign.Local, "t", 3, Foreign(3));
                var foreignHead = await foreignRun.RunSliceAsync(Context());
                Assert.Equal(durable ? SliceStatus.Rebase : SliceStatus.Stopped, foreignHead.Status);
                Assert.Equal(GateVerdict.ForeignHead, foreignHead.Gate!.Verdict);
                Assert.True(File.Exists(foreign.Native("a.parquet")));
            }
        }

        [Fact]
        public async Task EverySliceListsAfresh()
        {
            const string Log = "/t/_delta_log/";
            var (table, evidence) = await Anchored(false, "a.parquet", "b.parquet", "c.parquet");
            var run = NewRun(table.Executor, ChunkGate.ForEvidence(table.Configuration, table.Metered, evidence), new[] { "a.parquet", "b.parquet", "c.parquet" }.Select(x => table.Target(x)),
                new DeletionRunSettings() { MaxPaths = 1 });

            var statuses = new List<SliceStatus>();
            for (int slice = 1; slice <= 3; slice++)
            {
                var before = table.Owned.Listings(Log);
                var result = await run.RunSliceAsync(Context());
                statuses.Add(result.Status);
                Assert.Equal(before + 1, table.Owned.Listings(Log));
                Assert.Equal(slice, result.Cursor);
            }

            Assert.Equal(new[] { SliceStatus.Progressed, SliceStatus.Progressed, SliceStatus.Completed }, statuses);
            Assert.Equal(3, run.Tally.Deleted);
            // A completed run lists nothing more
            var listings = table.Owned.Listings(Log);
            Assert.Equal(SliceStatus.Completed, (await run.RunSliceAsync(Context())).Status);
            Assert.Equal(listings, table.Owned.Listings(Log));
        }

        [Fact]
        public async Task StageGateNeedsItsOwnCheckedListing()
        {
            const string Log = "/c/t/_delta_log/";
            var table = Scripted();
            table.Put("_delta_log/" + $"{0:D20}.json", "_delta_log/" + $".{0:D20}.json.stage-0.tmp", "a.parquet", "b.parquet");
            var decided = new List<int>();
            var verdict = GateVerdict.Passed;
            var gate = ChunkGate.ForStages(table.Configuration, table.Metered, (stages, _) =>
            {
                decided.Add(stages.Stages.Count);
                return Task.FromResult(new GateResult(verdict));
            });
            var run = NewRun(table.Executor, gate, new[] { table.Target("a.parquet"), table.Target("b.parquet") }, new DeletionRunSettings() { MaxPaths = 1 });

            // A listing that fails its checks never reaches the decision
            table.Owned.StatAnswer = (path, entry) => path.Full.EndsWith($"{1:D20}.json", StringComparison.Ordinal) ? new IOEntry(path.Full) { Size = 1 } : entry;
            var failed = await run.RunSliceAsync(Context());
            Assert.Equal(SliceStatus.Retry, failed.Status);
            Assert.Equal(ListingCheck.SuccessorPresent, failed.Gate!.Listing);
            Assert.Empty(decided);

            // Each slice decides on a listing of its own
            table.Owned.StatAnswer = null;
            var before = table.Owned.Listings(Log);
            Assert.Equal(SliceStatus.Progressed, (await run.RunSliceAsync(Context())).Status);
            Assert.Equal(before + 1, table.Owned.Listings(Log));
            Assert.Equal(new[] { 1 }, decided);
            Assert.False(await table.Contains("a.parquet"));

            // A decision against the slice stops the volatile run
            verdict = GateVerdict.NotOwn;
            var refused = await run.RunSliceAsync(Context());
            Assert.Equal(SliceStatus.Stopped, refused.Status);
            Assert.Equal(new[] { 1, 1 }, decided);
            Assert.Equal(before + 2, table.Owned.Listings(Log));
            Assert.True(await table.Contains("b.parquet"));
        }

        [Fact]
        public async Task SliceBoundsPathsBytesAndIssuanceTime()
        {
            // Paths: two a slice
            var table = Scripted();
            var names = Enumerable.Range(0, 5).Select(i => $"p{i}.parquet").ToArray();
            table.Put(names);
            var byPaths = NewRun(table.Executor, OpenGate(table.Configuration, table.Metered), names.Select(x => table.Target(x)), new DeletionRunSettings() { MaxPaths = 2 });
            var issued = new List<int>();
            SliceResult result;
            do
            {
                var memory = new JobMemory();
                result = await byPaths.RunSliceAsync(Context(memory: memory));
                issued.Add(result.Issued);
                // The buffer is charged while the slice holds it, and released after settlement
                Assert.True(memory.ManagedPeak >= result.Issued * 2L * "/c/t/p0.parquet".Length);
                Assert.Equal(0, memory.Managed);
            }
            while (result.Status == SliceStatus.Progressed);
            Assert.Equal(SliceStatus.Completed, result.Status);
            Assert.Equal(new[] { 2, 2, 1 }, issued);

            // Bytes: one path a slice when two do not fit
            var bytesTable = Scripted();
            bytesTable.Put("b0.parquet", "b1.parquet");
            var oneTarget = 48 + 2L * "/c/t/b0.parquet".Length;
            var byBytes = NewRun(bytesTable.Executor, OpenGate(bytesTable.Configuration, bytesTable.Metered), new[] { bytesTable.Target("b0.parquet"), bytesTable.Target("b1.parquet") },
                new DeletionRunSettings() { MaxBufferBytes = 2 * oneTarget - 1 });
            Assert.Equal(1, (await byBytes.RunSliceAsync(Context())).Issued);
            Assert.Equal(1, (await byBytes.RunSliceAsync(Context())).Issued);
            Assert.Equal(2, byBytes.Cursor);

            // A memory ledger that cannot hold one path reads nothing and retries
            var capped = NewRun(bytesTable.Executor, OpenGate(bytesTable.Configuration, bytesTable.Metered), new[] { bytesTable.Target("b0.parquet") });
            var refused = await capped.RunSliceAsync(Context(memory: new JobMemory(managedCap: 16)));
            Assert.Equal(SliceStatus.Retry, refused.Status);
            Assert.Equal(MaintenanceReason.ManagedMemoryCap, refused.Reason);
            Assert.Equal(0, refused.Issued);

            // Time: issuance stops thirty seconds after it began
            var timed = Scripted();
            timed.Put("t0.parquet", "t1.parquet", "t2.parquet");
            var clock = new FakeTimeProvider(s_now);
            timed.Owned.BeforeRm = (path, _) =>
            {
                clock.Advance(TimeSpan.FromSeconds(30));
                return Task.CompletedTask;
            };
            var byTime = NewRun(timed.Executor, OpenGate(timed.Configuration, timed.Metered), new[] { "t0.parquet", "t1.parquet", "t2.parquet" }.Select(x => timed.Target(x)),
                new DeletionRunSettings() { MaxOutstanding = 1 }, clock);
            var first = await byTime.RunSliceAsync(Context());
            Assert.Equal(SliceStatus.Progressed, first.Status);
            Assert.Equal(1, first.Issued);
            Assert.Equal(1, first.Cursor);
            Assert.True(await timed.Contains("t1.parquet"));
        }

        [Fact]
        public async Task OutstandingDeletesAreCappedAndLogsRunOneAtATime()
        {
            // Pinned Azure, whose runs may have four deletes outstanding
            var table = Scripted(azure: true);
            var names = Enumerable.Range(0, 10).Select(i => $"p{i}.parquet").ToArray();
            table.Put(names);
            var current = 0;
            var peak = 0;
            var fourStarted = Signal();
            var release = Signal();
            table.Owned.BeforeRm = async (path, _) =>
            {
                var now = Interlocked.Increment(ref current);
                InterlockedMax(ref peak, now);
                if (now == 4)
                {
                    fourStarted.TrySetResult();
                }
                await release.Task;
                Interlocked.Decrement(ref current);
            };
            // Eight asked for, four allowed
            var run = NewRun(table.Executor, OpenGate(table.Configuration, table.Metered), names.Select(x => table.Target(x)), new DeletionRunSettings() { MaxOutstanding = 8 });
            var context = Context();
            var slice = run.RunSliceAsync(context);
            await fourStarted.Task.WaitAsync(s_wait);
            Assert.Equal(4, context.Tracked);
            release.SetResult();
            var result = await slice.WaitAsync(s_wait);
            Assert.Equal(SliceStatus.Completed, result.Status);
            Assert.Equal(10, result.Issued);
            Assert.Equal(4, peak);

            // Log files: one outstanding, and a failure ends the slice's log deletes behind it and holds the cursor
            var logs = Scripted(azure: true);
            logs.Put(Enumerable.Range(0, 4).Select(i => $"_delta_log/{i:D20}.json").ToArray());
            var crcs = Crcs(4);
            logs.Put(crcs);
            var logCurrent = 0;
            var logPeak = 0;
            var firstStarted = Signal();
            var firstRelease = Signal();
            logs.Owned.BeforeRm = async (path, _) =>
            {
                InterlockedMax(ref logPeak, Interlocked.Increment(ref logCurrent));
                if (path.Full.EndsWith($"{0:D20}.crc", StringComparison.Ordinal))
                {
                    firstStarted.TrySetResult();
                    await firstRelease.Task;
                }
                Interlocked.Decrement(ref logCurrent);
                if (path.Full.EndsWith($"{2:D20}.crc", StringComparison.Ordinal))
                {
                    throw new IOException("injected");
                }
            };
            var logRun = NewRun(logs.Executor, OpenGate(logs.Configuration, logs.Metered), crcs.Select(x => logs.Target(x, TargetKind.Log)));
            var logContext = Context(head: 3);
            var logSlice = logRun.RunSliceAsync(logContext);
            await firstStarted.Task.WaitAsync(s_wait);
            Assert.Equal(1, logContext.Tracked);
            firstRelease.SetResult();
            var logResult = await logSlice.WaitAsync(s_wait);
            Assert.Equal(1, logPeak);
            Assert.Equal(3, logResult.Issued);
            Assert.Equal(2, logResult.Cursor);
            Assert.Equal(SliceStatus.Failed, logResult.Status);
            Assert.True(await logs.Contains(crcs[2]));
            Assert.True(await logs.Contains(crcs[3]));
            Assert.Equal(1, logs.Executor.Tally.Failed);
        }

        // Log files 0 to 3 with their commits, and the run's log targets in the given order
        private (ScriptedTable Table, DeletionRun Run) LogRun(params string[] names)
        {
            var table = Scripted();
            table.Put(Enumerable.Range(0, 4).Select(i => $"_delta_log/{i:D20}.json").ToArray());
            table.Put(names);
            return (table, NewRun(table.Executor, OpenGate(table.Configuration, table.Metered), names.Select(x => table.Target(x, TargetKind.Log))));
        }

        private static string[] Crcs(int count) => Enumerable.Range(0, count).Select(i => $"_delta_log/{i:D20}.crc").ToArray();

        [Fact]
        public async Task VolatileLogFailureKeepsTheCursorOnItsTarget()
        {
            var crcs = Crcs(4);
            var (logs, run) = LogRun(crcs);
            Assert.False(run.Durable);
            var failing = true;
            logs.Owned.BeforeRm = (path, _) => failing && path.Full.EndsWith($"{2:D20}.crc", StringComparison.Ordinal) ? throw new IOException("injected") : Task.CompletedTask;

            // However many slices fail on it, the dependent delete behind it is never issued
            for (int slice = 1; slice <= 4; slice++)
            {
                var result = await run.RunSliceAsync(Context(head: 3));
                Assert.Equal(SliceStatus.Failed, result.Status);
                Assert.Equal(MaintenanceReason.DeleteFailed, result.Reason);
                Assert.Equal(2, result.Cursor);
                Assert.True(await logs.Contains(crcs[2]));
                Assert.True(await logs.Contains(crcs[3]));
                Assert.Equal(slice, run.Tally.Failed);
            }
            Assert.DoesNotContain(logs.Owned.Calls, x => x == "Rm /c/t/" + crcs[3]);
            Assert.False(await logs.Contains(crcs[1]));

            failing = false;
            var done = await run.RunSliceAsync(Context(head: 3));
            Assert.Equal(SliceStatus.Completed, done.Status);
            Assert.Equal(4, done.Cursor);
            Assert.False(await logs.Contains(crcs[3]));
        }

        [Fact]
        public async Task KeptVolatileLogTargetStopsTheRun()
        {
            // A name the log kind refuses, between two deletable log files
            var names = new[] { $"_delta_log/{0:D20}.crc", "_delta_log/x.parquet", $"_delta_log/{1:D20}.crc" };
            var (logs, run) = LogRun(names);

            var result = await run.RunSliceAsync(Context(head: 3));
            Assert.Equal(SliceStatus.Stopped, result.Status);
            Assert.Equal(MaintenanceReason.PathRejected, result.Reason);
            Assert.Equal(1, result.Cursor);
            Assert.Equal(1, run.Tally.KeptFor(KeepReason.Misplaced));
            // The run has ended; nothing behind the kept target is ever issued
            Assert.Equal(result, await run.RunSliceAsync(Context(head: 3)));
            Assert.False(await logs.Contains(names[0]));
            Assert.True(await logs.Contains(names[2]));
        }

        [Fact]
        public async Task LogTargetsAbovePublishedVersionAreNeverIssued()
        {
            // A compaction's end version bounds it too
            var names = new[] { $"_delta_log/{0:D20}.crc", $"_delta_log/{1:D20}.crc", $"_delta_log/{1:D20}.{2:D20}.compacted.json", $"_delta_log/{2:D20}.crc", $"_delta_log/{3:D20}.crc" };
            var (logs, run) = LogRun(names);

            var first = await run.RunSliceAsync(Context(head: 1));
            Assert.Equal(SliceStatus.Progressed, first.Status);
            Assert.Equal(2, first.Cursor);
            Assert.True(await logs.Contains(names[2]));

            // Nothing below the bound remains, so the slice concludes nothing and keeps the run
            var held = await run.RunSliceAsync(Context(head: 1));
            Assert.Equal(SliceStatus.Retry, held.Status);
            Assert.Equal(MaintenanceReason.CleanupGate, held.Reason);
            Assert.Equal(0, held.Issued);
            Assert.Equal(2, held.Cursor);

            var second = await run.RunSliceAsync(Context(head: 2));
            Assert.Equal(SliceStatus.Progressed, second.Status);
            Assert.Equal(4, second.Cursor);
            Assert.False(await logs.Contains(names[2]));
            Assert.True(await logs.Contains(names[4]));

            Assert.Equal(SliceStatus.Completed, (await run.RunSliceAsync(Context(head: 3))).Status);
            Assert.False(await logs.Contains(names[4]));
            Assert.Equal(0, run.Tally.Kept);
            Assert.Equal(5, run.Tally.Deleted);
        }

        [Fact]
        public async Task VolatilePayloadFailureIsRetriedThenCountedAsALeak()
        {
            // Failing on every attempt: the cursor holds through three failed slices, then the target is counted and passed
            var table = Scripted(azure: true);
            var names = new[] { "a.parquet", "b.parquet", "c.parquet" };
            table.Put(names);
            table.Owned.BeforeRm = (path, _) => path.Full.EndsWith("/b.parquet", StringComparison.Ordinal) ? throw new IOException("injected") : Task.CompletedTask;
            var run = NewRun(table.Executor, OpenGate(table.Configuration, table.Metered), names.Select(x => table.Target(x)));
            for (int slice = 1; slice <= 2; slice++)
            {
                var failed = await run.RunSliceAsync(Context());
                Assert.Equal(SliceStatus.Failed, failed.Status);
                Assert.Equal(MaintenanceReason.DeleteFailed, failed.Reason);
                Assert.Equal(1, failed.Cursor);
                Assert.Equal(slice, run.Tally.Failed);
            }
            Assert.Equal(0, run.Tally.Leaked);
            var passed = await run.RunSliceAsync(Context());
            Assert.Equal(SliceStatus.Completed, passed.Status);
            Assert.Equal(3, passed.Cursor);
            Assert.Equal(DeletionRun.MaxFailedSlices, run.Tally.Failed);
            Assert.Equal(1, run.Tally.Leaked);
            Assert.True(await table.Contains("b.parquet"));
            Assert.False(await table.Contains("a.parquet"));
            Assert.False(await table.Contains("c.parquet"));

            // Failing once: the next slice deletes it
            var transient = Scripted(azure: true);
            transient.Put(names);
            var once = 1;
            transient.Owned.BeforeRm = (path, _) => path.Full.EndsWith("/b.parquet", StringComparison.Ordinal) && Interlocked.Exchange(ref once, 0) == 1 ? throw new IOException("injected") : Task.CompletedTask;
            var retried = NewRun(transient.Executor, OpenGate(transient.Configuration, transient.Metered), names.Select(x => transient.Target(x)));
            Assert.Equal(SliceStatus.Failed, (await retried.RunSliceAsync(Context())).Status);
            Assert.True(await transient.Contains("b.parquet"));
            var repeat = await retried.RunSliceAsync(Context());
            Assert.Equal(SliceStatus.Completed, repeat.Status);
            Assert.False(await transient.Contains("b.parquet"));
            Assert.Equal(1, retried.Tally.Failed);
            Assert.Equal(0, retried.Tally.Leaked);

            // The count belongs to one target: once the cursor moves on, a later target's failure holds it again
            var moving = Scripted(azure: true);
            moving.Put(names);
            var failures = new Dictionary<string, int>() { ["a.parquet"] = DeletionRun.MaxFailedSlices - 1, ["c.parquet"] = DeletionRun.MaxFailedSlices - 1 };
            moving.Owned.BeforeRm = (path, _) =>
            {
                lock (failures)
                {
                    var name = path.Name;
                    if (failures.TryGetValue(name, out var left) && left > 0)
                    {
                        failures[name] = left - 1;
                        throw new IOException("injected");
                    }
                }
                return Task.CompletedTask;
            };
            var movingRun = NewRun(moving.Executor, OpenGate(moving.Configuration, moving.Metered), names.Select(x => moving.Target(x)), new DeletionRunSettings() { MaxOutstanding = 1 });
            var statuses = new List<(SliceStatus, long)>();
            SliceResult moved;
            do
            {
                moved = await movingRun.RunSliceAsync(Context());
                statuses.Add((moved.Status, moved.Cursor));
            }
            while (moved.Status == SliceStatus.Failed);
            Assert.Equal(new[] { (SliceStatus.Failed, 0L), (SliceStatus.Failed, 0L), (SliceStatus.Failed, 2L), (SliceStatus.Failed, 2L), (SliceStatus.Completed, 3L) }, statuses);
            Assert.Equal((DeletionRun.MaxFailedSlices - 1) * 2, movingRun.Tally.Failed);
            Assert.Equal(0, movingRun.Tally.Leaked);
            Assert.False(await moving.Contains("a.parquet"));
            Assert.False(await moving.Contains("c.parquet"));

            // A deterministic refusal is counted and passed at once
            var refused = Scripted(azure: true);
            refused.Put("a.parquet", "_delta_log/x.parquet", "c.parquet");
            var refusing = NewRun(refused.Executor, OpenGate(refused.Configuration, refused.Metered), new[] { refused.Target("a.parquet"), refused.Target("_delta_log/x.parquet"), refused.Target("c.parquet") });
            var counted = await refusing.RunSliceAsync(Context());
            Assert.Equal(SliceStatus.Completed, counted.Status);
            Assert.Equal(1, refusing.Tally.KeptFor(KeepReason.Misplaced));
            Assert.False(await refused.Contains("c.parquet"));
        }

        [Fact]
        public async Task DeletesBehindAFailureAreKnownUntilTheCursorPasses()
        {
            // a's first delete fails once c's was submitted beside it, so b and c are done behind the cursor
            var names = new[] { "a.parquet", "b.parquet", "c.parquet" };
            var table = Scripted(azure: true);
            table.Put(names);
            var submitted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var once = 1;
            table.Owned.BeforeRm = async (path, _) =>
            {
                if (path.Name == "c.parquet")
                {
                    submitted.TrySetResult();
                }
                else if (path.Name == "a.parquet" && Interlocked.Exchange(ref once, 0) == 1)
                {
                    await submitted.Task.WaitAsync(TimeSpan.FromSeconds(30));
                    throw new IOException("injected");
                }
            };
            var run = NewRun(table.Executor, OpenGate(table.Configuration, table.Metered), names.Select(x => table.Target(x)));

            var failed = await run.RunSliceAsync(Context());
            Assert.Equal(SliceStatus.Failed, failed.Status);
            Assert.Equal(0, failed.Cursor);
            Assert.Equal(new[] { false, true, true }, Enumerable.Range(0, names.Length).Select(x => run.DoneAhead(x)));
            Assert.Equal(2, run.DoneAheadCountForTests);

            // Once the cursor passes them they are forgotten
            var retried = await run.RunSliceAsync(Context());
            Assert.Equal(SliceStatus.Completed, retried.Status);
            Assert.Equal(0, run.DoneAheadCountForTests);
            Assert.All(names, x => Assert.False(run.DoneAhead(Array.IndexOf(names, x))));
        }

        [Fact]
        public async Task RetrySliceSkipsTargetsSettledAheadOfItsCursor()
        {
            // a's first delete fails once c's was submitted, b is kept in place, so b and c are settled ahead of the cursor
            var table = Scripted(azure: true);
            table.Put("a.parquet", "_delta_log/b.parquet", "c.parquet");
            var submitted = Signal();
            var once = 1;
            table.Owned.BeforeRm = async (path, _) =>
            {
                if (path.Name == "c.parquet")
                {
                    submitted.TrySetResult();
                }
                else if (path.Name == "a.parquet" && Interlocked.Exchange(ref once, 0) == 1)
                {
                    await submitted.Task.WaitAsync(s_wait);
                    throw new IOException("injected");
                }
            };
            var run = NewRun(table.Executor, OpenGate(table.Configuration, table.Metered), new[] { table.Target("a.parquet"), table.Target("_delta_log/b.parquet"), table.Target("c.parquet") });

            var failed = await run.RunSliceAsync(Context());
            Assert.Equal(SliceStatus.Failed, failed.Status);
            Assert.Equal(0, failed.Cursor);
            Assert.True(submitted.Task.IsCompleted);

            // The retry issues only the failed target and counts nothing twice
            var retried = await run.RunSliceAsync(Context());
            Assert.Equal(SliceStatus.Completed, retried.Status);
            Assert.Equal(3, retried.Cursor);
            Assert.Equal(1, retried.Issued);
            Assert.Equal(new[] { 2, 1 }, new[] { "a.parquet", "c.parquet" }.Select(x => table.Owned.Calls.Count(c => c == "Rm /c/t/" + x)));
            Assert.Equal(2, run.Tally.Deleted);
            Assert.Equal(1, run.Tally.Failed);
            Assert.Equal(1, run.Tally.KeptFor(KeepReason.Misplaced));
            Assert.Equal(0, run.Tally.Absent);
            Assert.True(await table.Contains("_delta_log/b.parquet"));
            Assert.False(await table.Contains("a.parquet"));
        }

        [Fact]
        public async Task VolatileFailureStopsIssuanceUntilItsRetrySlice()
        {
            // One outstanding on pinned S3, so the slice sees the failure before it would issue the next target
            var names = new[] { "a.parquet", "b.parquet", "c.parquet", "d.parquet", "e.parquet" };
            var table = Scripted();
            table.Put(names);
            var once = 1;
            table.Owned.BeforeRm = (path, _) => path.Name == "c.parquet" && Interlocked.Exchange(ref once, 0) == 1 ? throw new IOException("injected") : Task.CompletedTask;
            var run = NewRun(table.Executor, OpenGate(table.Configuration, table.Metered), names.Select(x => table.Target(x)));

            var failed = await run.RunSliceAsync(Context());
            Assert.Equal(SliceStatus.Failed, failed.Status);
            Assert.Equal(3, failed.Issued);
            Assert.Equal(2, failed.Cursor);
            Assert.True(await table.Contains("d.parquet"));
            Assert.True(await table.Contains("e.parquet"));

            // The retry slice issues each target behind the failure once
            var retried = await run.RunSliceAsync(Context());
            Assert.Equal(SliceStatus.Completed, retried.Status);
            Assert.Equal(new[] { 1, 1, 2, 1, 1 }, names.Select(x => table.Owned.Calls.Count(c => c == "Rm /c/t/" + x)));

            // The failure its leak limit passes keeps issuance open behind it
            var leaking = Scripted();
            leaking.Put(names);
            leaking.Owned.BeforeRm = (path, _) => path.Name == "c.parquet" ? throw new IOException("injected") : Task.CompletedTask;
            var leakRun = NewRun(leaking.Executor, OpenGate(leaking.Configuration, leaking.Metered), names.Select(x => leaking.Target(x)));
            var issued = new List<(SliceStatus, int)>();
            SliceResult slice;
            do
            {
                slice = await leakRun.RunSliceAsync(Context());
                issued.Add((slice.Status, slice.Issued));
            }
            while (slice.Status == SliceStatus.Failed);
            Assert.Equal(new[] { (SliceStatus.Failed, 3), (SliceStatus.Failed, 1), (SliceStatus.Completed, 3) }, issued);
            Assert.Equal(1, leakRun.Tally.Leaked);
            Assert.True(await leaking.Contains("c.parquet"));
            Assert.False(await leaking.Contains("d.parquet"));
            Assert.False(await leaking.Contains("e.parquet"));
        }

        [Fact]
        public async Task StageTargetsAbovePublishedVersionAreNeverIssued()
        {
            var table = Scripted();
            table.Put(Enumerable.Range(0, 6).Select(i => $"_delta_log/{i:D20}.json").ToArray());
            var stages = new[] { 1, 2, 5 }.Select(v => $"_delta_log/.{v:D20}.json.stage-{v}.tmp").ToArray();
            table.Put(stages);
            var run = NewRun(table.Executor, OpenGate(table.Configuration, table.Metered), stages.Select(x => table.Target(x, TargetKind.Stage)));

            var first = await run.RunSliceAsync(Context(head: 3));
            Assert.Equal(SliceStatus.Progressed, first.Status);
            Assert.Equal(2, first.Cursor);
            Assert.False(await table.Contains(stages[1]));
            Assert.True(await table.Contains(stages[2]));

            // Nothing at or below the bound remains, so the slice concludes nothing and keeps the run
            var held = await run.RunSliceAsync(Context(head: 3));
            Assert.Equal(SliceStatus.Retry, held.Status);
            Assert.Equal(MaintenanceReason.CleanupGate, held.Reason);
            Assert.Equal(0, held.Issued);
            Assert.DoesNotContain(table.Owned.Calls, x => x == "Rm /c/t/" + stages[2]);

            Assert.Equal(SliceStatus.Completed, (await run.RunSliceAsync(Context(head: 5))).Status);
            Assert.False(await table.Contains(stages[2]));
        }

        [Fact]
        public async Task BackOffWaitsOnTheClockAndEndsWhenIssuanceStops()
        {
            var table = Scripted();
            var none = Array.Empty<DeletionTarget>();
            var settings = new DeletionRunSettings() { RetryDelay = TimeSpan.FromSeconds(1) };

            // The second failed slice waits twice the delay
            var clock = new TimerClock(s_now);
            var run = NewRun(table.Executor, OpenGate(table.Configuration, table.Metered), none, settings, clock);
            var waited = run.BackOffAsync(2, Context(clock: clock));
            await clock.NextTimerAsync(s_wait);
            clock.Fake.Advance(TimeSpan.FromSeconds(1));
            // Still waiting: the wait set another timer
            await clock.NextTimerAsync(s_wait);
            Assert.False(waited.IsCompleted);
            clock.Fake.Advance(TimeSpan.FromSeconds(1));
            Assert.True(await waited.WaitAsync(s_wait));

            // However long the delay, one wait is bounded
            var cappedClock = new TimerClock(s_now);
            var cappedRun = NewRun(table.Executor, OpenGate(table.Configuration, table.Metered), none, settings with { RetryDelay = TimeSpan.FromMinutes(1) }, cappedClock);
            var capped = cappedRun.BackOffAsync(3, Context(clock: cappedClock));
            await cappedClock.NextTimerAsync(s_wait);
            cappedClock.Fake.Advance(TimeSpan.FromSeconds(30));
            Assert.True(await capped.WaitAsync(s_wait));

            // A due build ends a long wait at its next step
            var job = Run();
            var buildClock = new TimerClock(s_now);
            var buildRun = NewRun(table.Executor, OpenGate(table.Configuration, table.Metered), none, settings with { RetryDelay = TimeSpan.FromSeconds(10) }, buildClock);
            var yielding = buildRun.BackOffAsync(1, Context(run: job, clock: buildClock));
            await buildClock.NextTimerAsync(s_wait);
            job.YieldRequested = true;
            buildClock.Fake.Advance(TimeSpan.FromSeconds(1));
            Assert.False(await yielding.WaitAsync(s_wait));

            // So does the job's deadline
            var deadlineClock = new TimerClock(s_now);
            var deadlineContext = Context(clock: deadlineClock);
            deadlineClock.Fake.Advance(TimeSpan.FromMinutes(15) - TimeSpan.FromSeconds(1));
            var deadlineRun = NewRun(table.Executor, OpenGate(table.Configuration, table.Metered), none, settings with { RetryDelay = TimeSpan.FromSeconds(10) }, deadlineClock);
            var expiring = deadlineRun.BackOffAsync(1, deadlineContext);
            await deadlineClock.NextTimerAsync(s_wait);
            deadlineClock.Fake.Advance(TimeSpan.FromSeconds(1));
            Assert.False(await expiring.WaitAsync(s_wait));

            // And a stop of the worker
            using var worker = new CancellationTokenSource();
            var stopClock = new TimerClock(s_now);
            var stopRun = NewRun(table.Executor, OpenGate(table.Configuration, table.Metered), none, settings, stopClock);
            var stopping = stopRun.BackOffAsync(1, Context(clock: stopClock, stop: worker.Token));
            await stopClock.NextTimerAsync(s_wait);
            worker.Cancel();
            Assert.False(await stopping.WaitAsync(s_wait));

            // Without a delay nothing waits
            var immediateClock = new TimerClock(s_now);
            var immediate = NewRun(table.Executor, OpenGate(table.Configuration, table.Metered), none, new DeletionRunSettings() { RetryDelay = TimeSpan.Zero }, immediateClock);
            Assert.True(await immediate.BackOffAsync(1, Context(clock: immediateClock)));
            await Assert.ThrowsAsync<TimeoutException>(() => immediateClock.NextTimerAsync(TimeSpan.Zero));
        }

        [Fact]
        public async Task FailedDiscardDeleteKeepsItsHint()
        {
            await using var server = new LoopbackObjectStore(StoreFlavor.Azure);
            var cloud = Cloud(server);
            await Create(cloud.Client, "c/t", 2);
            var paths = new[] { "_change_data/a.parquet", "_change_data/b.parquet" };
            foreach (var path in paths)
            {
                server.Put("t/" + path);
            }
            var handOff = new DiscardHandOff();
            var counters = new DiscardCounters();
            var clock = new FakeTimeProvider(s_now);
            var cleanup = new DeltaTableCleanup(cloud.Configuration, cloud.Metered, clock, "c/t", handOff, () => cloud.Owned, counters, null, "t");
            using var producer = handOff.Acquire(DiscardRoute.Of(cloud.Owned, "c/t"));
            producer.Entry.Enqueue(new DiscardGroup("table-a", 0, 2, "stage-2", paths));
            var once = 1;
            cloud.Owned.BeforeRm = (path, _) => path.Full.EndsWith("/a.parquet", StringComparison.Ordinal) && Interlocked.Exchange(ref once, 0) == 1 ? throw new IOException("injected") : Task.CompletedTask;

            // The failed delete holds only its own hint; the one behind it settles and is not issued again
            await cleanup.DrainHintsAsync(Context(), 128);
            Assert.True(server.Contains("t/_change_data/a.parquet"));
            Assert.False(server.Contains("t/_change_data/b.parquet"));
            Assert.Equal(1, producer.Entry.Hints);
            Assert.Equal(1, counters.Failed);

            // Retried once its wait passed
            clock.Advance(TimeSpan.FromMinutes(1));
            var retried = await cleanup.DrainHintsAsync(Context(), 128);
            Assert.Equal(1, retried.Drained);
            Assert.False(server.Contains("t/_change_data/a.parquet"));
            Assert.Equal(0, producer.Entry.Hints);
            Assert.Single(cloud.Owned.Calls, x => x == "Rm /c/t/_change_data/b.parquet");
        }

        [Fact]
        public async Task PinnedS3RunDeletesOneAtATimeWhateverItsSettings()
        {
            var table = Scripted();
            var names = new[] { "a.parquet", "b.parquet", "c.parquet" };
            table.Put(names);
            // The switch turns on inside the first delete, so a second delete issued beside it would be seen
            var off = false;
            table.Owned.BeforeRm = (_, _) =>
            {
                off = true;
                return Task.CompletedTask;
            };
            var run = NewRun(table.Executor, OpenGate(table.Configuration, table.Metered), names.Select(x => table.Target(x)), new DeletionRunSettings() { DeletesDisabled = () => off });

            var result = await run.RunSliceAsync(Context());

            Assert.Equal(SliceStatus.Disabled, result.Status);
            Assert.Equal(1, result.Issued);
            Assert.True(await table.Contains("b.parquet"));
            Assert.True(await table.Contains("c.parquet"));
            // One predicate for every caller
            Assert.Equal(1, DeletionRun.OutstandingLimit(table.Configuration.Store.Store));
            Assert.Equal(DeletionRun.MaxOutstanding, DeletionRun.OutstandingLimit(Scripted(azure: true).Configuration.Store.Store));
            Assert.Equal(DeletionRun.MaxOutstanding, DeletionRun.OutstandingLimit(Local().Configuration.Store.Store));
        }

        [Fact]
        public async Task RunKeepsEverythingOnceTheSinkStorageIsReplaced()
        {
            var root = Path.Combine(_directory, ScratchOwners.ShortId());
            Directory.CreateDirectory(Path.Combine(root, "t"));
            var local = Files.Of.LocalDisk(root);
            _disposables.Add(local);
            var options = new DeltaLakeOptions() { StorageLocation = local };
            var sink = new DeltaLakeSink(options, CleanupConfigurationTests.Relation("t"), new ExecutionDataflowBlockOptions());
            var configuration = sink.CleanupForTests;
            await Create(local, "t", 2);
            var native = Path.Combine(root, "t", "a.parquet");
            File.WriteAllText(native, "a");
            // The run's meter wraps the storage the sink had when its run started
            var owned = new LocalTestStorage(local);
            var metered = MeteredFileStorage.Wrap(owned, new MaintenanceBuckets(TimeProvider.System), ListingPaging.LocalDisk);
            var executor = new DeletionExecutor(configuration, metered);
            var target = new DeletionTarget(native, TargetKind.Payload);
            Assert.Equal(KeepReason.None, executor.Check(target));
            var run = NewRun(executor, OpenGate(configuration, metered), new[] { target });

            var replaced = Files.Of.LocalDisk(root);
            _disposables.Add(replaced);
            options.StorageLocation = replaced;

            Assert.Equal(KeepReason.StorageChanged, executor.Check(target));
            var result = await run.RunSliceAsync(Context());
            Assert.Equal(SliceStatus.Stopped, result.Status);
            Assert.Equal(MaintenanceReason.ListingIneligible, result.Reason);
            Assert.Equal(0, result.Issued);
            // Not even the gate's listing goes through the old instance
            Assert.Empty(owned.Calls);
            Assert.True(File.Exists(native));
            Assert.Equal(DeleteOutcome.Keep(KeepReason.StorageChanged), await executor.DeleteAsync(target, false, default));
            Assert.True(File.Exists(native));
        }

        private static void InterlockedMax(ref int target, int value)
        {
            var seen = Volatile.Read(ref target);
            while (value > seen)
            {
                var previous = Interlocked.CompareExchange(ref target, value, seen);
                if (previous == seen)
                {
                    return;
                }
                seen = previous;
            }
        }

        [Theory]
        [InlineData("BuildDue")]
        [InlineData("Deadline")]
        [InlineData("Stopped")]
        public async Task IssuanceStopsWithoutCancellingSubmittedDeletes(string stopName)
        {
            var stop = Enum.Parse<IssuanceStop>(stopName);
            var table = Scripted();
            var names = new[] { "a.parquet", "b.parquet", "c.parquet" };
            table.Put(names);
            var started = Signal();
            var release = Signal();
            table.Owned.BeforeRm = async (path, token) =>
            {
                if (path.Full.EndsWith("/a.parquet", StringComparison.Ordinal))
                {
                    started.TrySetResult();
                    // A submitted delete that honored a stop would end here as a failure
                    await release.Task.WaitAsync(token);
                }
            };
            var job = Run();
            var clock = new FakeTimeProvider(s_now);
            using var worker = new CancellationTokenSource();
            var context = Context(run: job, clock: clock, stop: worker.Token);
            var run = NewRun(table.Executor, OpenGate(table.Configuration, table.Metered), names.Select(x => table.Target(x)), new DeletionRunSettings() { MaxOutstanding = 1 });

            var slice = run.RunSliceAsync(context);
            await started.Task.WaitAsync(s_wait);
            switch (stop)
            {
                case IssuanceStop.BuildDue:
                    job.YieldRequested = true;
                    break;
                case IssuanceStop.Deadline:
                    clock.Advance(TimeSpan.FromMinutes(16));
                    break;
                default:
                    worker.Cancel();
                    break;
            }
            release.SetResult();
            var result = await slice.WaitAsync(s_wait);

            Assert.Equal(SliceStatus.Progressed, result.Status);
            Assert.Equal(stop, result.Stop);
            Assert.Equal(stop == IssuanceStop.Deadline ? MaintenanceReason.JobDeadline : MaintenanceReason.None, result.Reason);
            Assert.Equal(1, result.Issued);
            Assert.Equal(1, result.Cursor);
            Assert.Equal(1, run.Tally.Deleted);
            Assert.False(await table.Contains("a.parquet"));
            Assert.True(await table.Contains("b.parquet"));
            Assert.True(await table.Contains("c.parquet"));
        }

        [Fact]
        public async Task FailedDeleteKeepsTheCursorForItsRetry()
        {
            // Durable and one outstanding: the failed delete stops issuance and holds the cursor
            var (serial, serialEvidence) = await Anchored(true, "a.parquet", "b.parquet", "c.parquet");
            serial.Executor.BeforeDeleteForTests = target => target.Key.EndsWith("b.parquet", StringComparison.Ordinal) ? throw new IOException("injected") : Task.CompletedTask;
            var serialRun = NewRun(serial.Executor, ChunkGate.ForEvidence(serial.Configuration, serial.Metered, serialEvidence), new[] { "a.parquet", "b.parquet", "c.parquet" }.Select(x => serial.Target(x)),
                new DeletionRunSettings() { MaxOutstanding = 1 });
            var failed = await serialRun.RunSliceAsync(Context());
            Assert.Equal(SliceStatus.Failed, failed.Status);
            Assert.Equal(MaintenanceReason.DeleteFailed, failed.Reason);
            Assert.Equal(1, failed.Cursor);
            Assert.Equal(2, failed.Issued);
            Assert.True(File.Exists(serial.Native("b.parquet")));
            Assert.True(File.Exists(serial.Native("c.parquet")));

            // Four outstanding, durable or volatile: deletes already submitted settle, and the cursor still stays on the failure
            foreach (var durable in new[] { true, false })
            {
                var names = new[] { "a.parquet", "b.parquet", "c.parquet", "d.parquet" };
                var (table, evidence) = await Anchored(durable, names);
                var started = 0;
                var allStarted = Signal();
                var release = Signal();
                table.Executor.BeforeDeleteForTests = async target =>
                {
                    if (Interlocked.Increment(ref started) == 4)
                    {
                        allStarted.TrySetResult();
                    }
                    await release.Task;
                    if (target.Key.EndsWith("b.parquet", StringComparison.Ordinal))
                    {
                        throw new IOException("injected");
                    }
                };
                var run = NewRun(table.Executor, ChunkGate.ForEvidence(table.Configuration, table.Metered, evidence), names.Select(x => table.Target(x)));
                var slice = run.RunSliceAsync(Context());
                await allStarted.Task.WaitAsync(s_wait);
                release.SetResult();
                var result = await slice.WaitAsync(s_wait);

                Assert.Equal(4, result.Issued);
                Assert.Equal(1, run.Tally.Failed);
                Assert.True(File.Exists(table.Native("b.parquet")));
                Assert.False(File.Exists(table.Native("d.parquet")));
                Assert.Equal(SliceStatus.Failed, result.Status);
                Assert.Equal(MaintenanceReason.DeleteFailed, result.Reason);
                Assert.Equal(1, run.Cursor);
                // The repeat issues only the failed delete, the ones settled behind it standing
                table.Executor.BeforeDeleteForTests = null;
                var repeat = await run.RunSliceAsync(Context());
                Assert.Equal(SliceStatus.Completed, repeat.Status);
                Assert.Equal(4, repeat.Cursor);
                Assert.Equal(1, repeat.Issued);
                Assert.Equal(0, run.Tally.Absent);
                Assert.Equal(4, run.Tally.Deleted);
                Assert.Equal(0, run.Tally.Leaked);
                Assert.False(File.Exists(table.Native("b.parquet")));
            }
        }

        [Fact]
        public async Task ProgressSettlesAfterDeletesWithinTheSlice()
        {
            var table = Scripted();
            var names = new[] { "a.parquet", "b.parquet", "c.parquet" };
            table.Put(names);
            var cStarted = Signal();
            var cRelease = Signal();
            table.Owned.BeforeRm = async (path, _) =>
            {
                if (path.Full.EndsWith("/c.parquet", StringComparison.Ordinal))
                {
                    cStarted.TrySetResult();
                    await cRelease.Task;
                }
            };
            var recorded = new ConcurrentQueue<long>();
            var unsettled = 0;
            var write = false;
            var run = NewRun(table.Executor, OpenGate(table.Configuration, table.Metered), names.Select(x => table.Target(x)), new DeletionRunSettings()
            {
                SettleProgress = async (cursor, context) =>
                {
                    foreach (var name in names)
                    {
                        if (await table.Contains(name))
                        {
                            Interlocked.Increment(ref unsettled);
                        }
                    }
                    recorded.Enqueue(cursor);
                    return write;
                }
            });

            // An unwritten record keeps the previous cursor, though the deletes happened
            var slice = run.RunSliceAsync(Context());
            await cStarted.Task.WaitAsync(s_wait);
            cRelease.SetResult();
            var unwritten = await slice.WaitAsync(s_wait);
            Assert.Equal(SliceStatus.Retry, unwritten.Status);
            Assert.Equal(MaintenanceReason.Transient, unwritten.Reason);
            Assert.Equal(0, run.Cursor);
            Assert.Equal(0, unsettled);
            Assert.Equal(new long[] { 3 }, recorded);

            // The repeat deletes the same names again and records the cursor
            write = true;
            var written = await run.RunSliceAsync(Context());
            Assert.Equal(SliceStatus.Completed, written.Status);
            Assert.Equal(3, run.Cursor);
            Assert.Equal(new long[] { 3, 3 }, recorded);
            Assert.Equal(6, run.Tally.Deleted);
        }

        [Fact]
        public async Task SliceRequiresDeleteAdmissionMeterAndSwitch()
        {
            const string Log = "/c/t/_delta_log/";
            var table = Scripted();
            table.Put("a.parquet", "b.parquet", "c.parquet");
            var targets = new[] { table.Target("a.parquet"), table.Target("b.parquet"), table.Target("c.parquet") };

            // Planning holds the main gate, not a delete permit
            var planning = NewRun(table.Executor, OpenGate(table.Configuration, table.Metered), targets);
            Assert.Equal(SliceStatus.NotAdmitted, (await planning.RunSliceAsync(Context(JobKind.Advance))).Status);
            // Deletes outside the maintenance meter are never issued by a run
            var unmetered = new DeletionExecutor(table.Configuration, table.Owned);
            Assert.Equal(SliceStatus.NotAdmitted, (await NewRun(unmetered, OpenGate(table.Configuration, table.Owned), targets).RunSliceAsync(Context())).Status);
            Assert.Equal(0, table.Owned.Listings(Log));

            // The switch is read before the gate, and a switch that cannot be read is on
            var disabled = NewRun(table.Executor, OpenGate(table.Configuration, table.Metered), targets, new DeletionRunSettings() { DeletesDisabled = () => true });
            Assert.Equal(new SliceResult(SliceStatus.Disabled, MaintenanceReason.DeletesDisabled, 0), await disabled.RunSliceAsync(Context()));
            var unreadable = NewRun(table.Executor, OpenGate(table.Configuration, table.Metered), targets, new DeletionRunSettings() { DeletesDisabled = () => throw new InvalidOperationException() });
            Assert.Equal(SliceStatus.Disabled, (await unreadable.RunSliceAsync(Context())).Status);
            Assert.Equal(0, table.Owned.Listings(Log));
            Assert.Equal(0, table.Executor.Tally.Deleted);

            // and again before every issuance
            var off = false;
            table.Owned.BeforeRm = (_, _) =>
            {
                off = true;
                return Task.CompletedTask;
            };
            var switched = NewRun(table.Executor, OpenGate(table.Configuration, table.Metered), targets, new DeletionRunSettings() { MaxOutstanding = 1, DeletesDisabled = () => off });
            var result = await switched.RunSliceAsync(Context());
            Assert.Equal(SliceStatus.Disabled, result.Status);
            Assert.Equal(1, result.Issued);
            Assert.Equal(1, result.Cursor);
            Assert.True(await table.Contains("b.parquet"));
        }

        [Fact]
        public async Task ChangedStorageStopsTheRunAtItsNextTarget()
        {
            const string Log = "/c/t/_delta_log/";
            var table = Scripted();
            table.Put("a.parquet", "b.parquet", "c.parquet");
            var replacement = new ScriptedStorage();
            // The storage below the meter is replaced while the first delete is in flight
            table.Owned.BeforeRm = (_, _) =>
            {
                table.Owned.Inner = replacement;
                return Task.CompletedTask;
            };
            var run = NewRun(table.Executor, OpenGate(table.Configuration, table.Metered), new[] { table.Target("a.parquet"), table.Target("b.parquet"), table.Target("c.parquet") },
                new DeletionRunSettings() { MaxOutstanding = 1 });

            var result = await run.RunSliceAsync(Context());

            Assert.Equal(SliceStatus.Stopped, result.Status);
            Assert.Equal(MaintenanceReason.ListingIneligible, result.Reason);
            Assert.Equal(1, result.Cursor);
            Assert.Equal(1, run.Tally.KeptFor(KeepReason.StorageChanged));
            Assert.True(await table.Contains("b.parquet"));
            Assert.True(await table.Contains("c.parquet"));
            Assert.Empty(replacement.Requests.Where(x => x.StartsWith("Rm", StringComparison.Ordinal) && !x.EndsWith("/a.parquet", StringComparison.Ordinal)));
            var listings = table.Owned.Listings(Log);
            Assert.Equal(result, await run.RunSliceAsync(Context()));
            Assert.Equal(listings, table.Owned.Listings(Log));
        }

        [Fact]
        public async Task DurableRunChecksATargetKeptForAChangedStorageAgain()
        {
            var (table, evidence) = await Anchored(true, "a.parquet", "b.parquet", "c.parquet");
            var local = table.Owned.Inner;
            // The storage below the meter is replaced while the first delete is in flight
            table.Executor.BeforeDeleteForTests = target =>
            {
                if (target.Key == table.Native("a.parquet"))
                {
                    table.Owned.Inner = new ScriptedStorage();
                }
                return Task.CompletedTask;
            };
            var run = NewRun(table.Executor, ChunkGate.ForEvidence(table.Configuration, table.Metered, evidence), new[] { table.Target("a.parquet"), table.Target("b.parquet"), table.Target("c.parquet") },
                new DeletionRunSettings() { MaxOutstanding = 1 });

            var changed = await run.RunSliceAsync(Context());
            Assert.Equal((SliceStatus.Retry, MaintenanceReason.ListingIneligible, 1L), (changed.Status, changed.Reason, changed.Cursor));
            Assert.Equal(1, run.Tally.KeptFor(KeepReason.StorageChanged));
            // A keep for the storage is no settled outcome a later slice may reuse
            Assert.False(run.DoneAhead(1));

            // Once the sink's storage is the classified instance again, the durable run deletes the rest
            table.Owned.Inner = local;
            var cleared = await run.RunSliceAsync(Context());
            Assert.Equal((SliceStatus.Completed, 3L), (cleared.Status, cleared.Cursor));
            Assert.False(File.Exists(table.Native("b.parquet")));
            Assert.False(File.Exists(table.Native("c.parquet")));
        }

        [Fact]
        public async Task SliceHoldsItsDeletePermitThroughGateIssuanceAndSettlement()
        {
            var table = Local();
            await Create(table.Local, "t", 2);
            table.Put("a.parquet");
            table.Put("b.parquet");
            var budget = new DeltaMaintenanceBudget(new MaintenanceBudgetSettings(2, Path.Combine(_directory, "scratch-" + ScratchOwners.ShortId())), new FakeTimeProvider(s_now), manualTick: true);
            var seen = new ConcurrentQueue<(string Point, int Delete, int Main, int Leases)>();
            void See(string point) => seen.Enqueue((point, budget.DeleteGate.Running, budget.Gate.Running, budget.Leases.Count));
            var gating = false;
            table.Owned.BeforeLs = path =>
            {
                if (gating)
                {
                    See("gate");
                }
                return Task.CompletedTask;
            };
            table.Executor.BeforeDeleteForTests = _ =>
            {
                See("issue");
                return Task.CompletedTask;
            };
            SliceResult? slice = null;
            var cleanup = new SliceCleanup(async context =>
            {
                var evidence = await Establish(table.Configuration, table.Metered, 2, false, context);
                var run = NewRun(table.Executor, ChunkGate.ForEvidence(table.Configuration, table.Metered, evidence), new[] { table.Target("a.parquet"), table.Target("b.parquet") }, new DeletionRunSettings()
                {
                    SettleProgress = (_, _) =>
                    {
                        See("progress");
                        return Task.FromResult(true);
                    }
                });
                gating = true;
                slice = await run.RunSliceAsync(context);
                gating = false;
                See("returned");
            });
            var options = new FakeTimeProvider(s_now);
            var worker = new DeltaMaintenanceWorker(budget, new MaintenanceWorkerSettings(100, null, options), table.Local, "t", "t", null) { Cleanup = cleanup };
            budget.Register(worker);
            try
            {
                worker.Published(2);
                for (int i = 0; i < 10 && slice == null; i++)
                {
                    budget.TickNow();
                    await worker.CurrentJob.WaitAsync(s_wait);
                }
            }
            finally
            {
                await worker.StopAsync(s_wait);
                if (budget.ScratchIfCreated is { } scratch)
                {
                    _owners.Add(scratch);
                }
            }

            Assert.NotNull(slice);
            Assert.Equal(SliceStatus.Completed, slice!.Status);
            Assert.Equal(new[] { "gate", "issue", "issue", "progress", "returned" }, seen.Select(x => x.Point));
            // A delete permit and the table lease, never a main slot, at every point
            Assert.All(seen, x => Assert.Equal((1, 0, 1), (x.Delete, x.Main, x.Leases)));
            Assert.Equal(0, budget.DeleteGate.Running);
            Assert.Equal(0, budget.Leases.Count);
            Assert.False(File.Exists(table.Native("a.parquet")));
        }

        // Runs one delete slice in the worker's delete turn
        private sealed class SliceCleanup : IMaintenanceCleanup
        {
            private readonly Func<CleanupContext, Task> _slice;
            private bool _done;

            public SliceCleanup(Func<CleanupContext, Task> slice)
            {
                _slice = slice;
            }

            public bool HasWork(JobKind kind) => kind == JobKind.Deletion && !_done;

            public Task<DrainResult> DrainHintsAsync(CleanupContext context, int max) => Task.FromResult(new DrainResult(0));

            public async Task<CleanupResult> RunAsync(JobKind kind, CleanupContext context)
            {
                if (kind != JobKind.Deletion)
                {
                    return new CleanupResult(OutcomeKind.NoWork);
                }
                _done = true;
                await _slice(context);
                return new CleanupResult(OutcomeKind.Completed);
            }
        }
    }
}
