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

using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance;
using Microsoft.Extensions.Time.Testing;
using Stowage;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public partial class OrphanSweepTests
    {
        // A nested table listed after the synthesized entries
        private const string EndingRoot = "zz-nested/";

        // Synthesized entries, past the filter's second stop check
        private const int EndingEntries = 10_000;

        // Entries the filter classifies between stop checks
        private const int EndingCheckEvery = 4_096;

        // A candidate arena that spills between stop checks
        private const long EndingArena = 4 << 10;

        // A long orphan admission on the test's clock and stop
        private CleanupContext EndingContext(TimeProvider clock, CancellationToken stop, JobMemory memory)
        {
            return new CleanupContext(new JobRun(2, JobKind.Orphan) { Cancellation = new CancellationTokenSource() }, clock)
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
                Stop = stop
            };
        }

        // Whether the projection probed the head's successor after the root listing
        private static Func<bool> SuccessorProbed(OrphanTable table)
        {
            var successor = $"{table.Prefix}_delta_log/{3:D20}.json";
            var probed = false;
            table.Owned.StatAnswer = (path, entry) =>
            {
                if (path.Full == successor && table.Owned.Recursive.Count == 1)
                {
                    probed = true;
                }
                return entry;
            };
            return () => probed;
        }

        // Synthesized names, every tenth an old candidate, or all hidden, then a nested table, in the root listing; reports the resolutions made before the first and second stop checks
        private static void SynthesizeEndingListing(OrphanTable table, bool hidden, Action<long, long>? resolved = null)
        {
            table.Owned.AfterRecursiveLs = (_, listed) =>
            {
                var entries = listed.ToList();
                var old = new HashSet<string>(StringComparer.Ordinal);
                for (int i = 0; i < EndingEntries; i++)
                {
                    var name = hidden ? $"{table.Prefix}_h/f{i:D6}.parquet" : $"{table.Prefix}f{i:D6}.parquet";
                    var candidate = !hidden && i % 10 == 0;
                    if (candidate)
                    {
                        old.Add(name);
                    }
                    // Young names are never resolved
                    entries.Add(new IOEntry(name) { Size = 1, LastModificationTime = hidden || candidate ? s_old : DateTimeOffset.MaxValue });
                }
                entries.Add(new IOEntry($"{table.Prefix}{EndingRoot}_delta_log/{0:D20}.json") { Size = 1, LastModificationTime = s_old });
                var sorted = entries.Select(x => x.Path.Full).Order(StringComparer.Ordinal).ToList();
                long Before(int count)
                {
                    var candidates = sorted.Take(count).Count(old.Contains);
                    // The table root resolves with the first candidate
                    return candidates == 0 ? 0 : candidates + 1;
                }
                resolved?.Invoke(Before(EndingCheckEvery - 1), Before(2 * EndingCheckEvery - 1));
                return entries;
            };
        }

        [Theory]
        [InlineData("reference")]
        [InlineData("candidates")]
        [InlineData("hidden")]
        public async Task DeadlineStopsTheInventoryAtItsNextCheck(string crossed)
        {
            var table = await LocalWithOldAsync(2);
            var clock = new FakeTimeProvider(Now);
            var memory = new JobMemory();
            var meter = Metered(table);
            var sweep = SweepOver(table, meter, new RetentionCounters());
            sweep.ArenaCap = EndingArena;
            long owed = 0;
            long? chargedAtDeadline = null;
            if (crossed == "reference")
            {
                var reference = table.Prefix + ".flowtide-orphan-sweep";
                table.Owned.StatAnswer = (path, entry) =>
                {
                    if (path.Full == reference)
                    {
                        clock.Advance(TimeSpan.FromHours(6));
                    }
                    return entry;
                };
            }
            else
            {
                var probed = SuccessorProbed(table);
                SynthesizeEndingListing(table, crossed == "hidden", (first, _) => owed = first);
                memory.BeforeReserveForTests = _ =>
                {
                    // The first reserve after the projection's probe, before the filter
                    if (probed() && chargedAtDeadline == null)
                    {
                        chargedAtDeadline = meter.Buckets.Requests.Charged;
                        clock.Advance(TimeSpan.FromHours(6));
                    }
                };
            }

            var (result, seen) = await RunAsync(sweep, table, EndingContext(clock, default, memory));

            AssertResult(result, OutcomeKind.Preempted, MaintenanceReason.JobDeadline);
            Assert.Null(seen);
            if (crossed == "reference")
            {
                Assert.Empty(table.Owned.Recursive);
                return;
            }
            Assert.Single(table.Owned.Recursive);
            Assert.NotNull(chargedAtDeadline);
            // Each resolution made before the stop is owed to the request bucket
            Assert.Equal(owed, meter.Buckets.Requests.Charged - chargedAtDeadline.Value);
            Assert.True(crossed == "hidden" || owed > 1, $"{owed} owed");
        }

        [Fact]
        public async Task DeadlineAfterAChargeLeavesOnlyLaterResolutionsOwed()
        {
            var table = await LocalWithOldAsync(2);
            var clock = new FakeTimeProvider(Now);
            var memory = new JobMemory();
            var meter = Metered(table);
            var sweep = SweepOver(table, meter, new RetentionCounters());
            sweep.ArenaCap = EndingArena;
            long first = 0;
            long second = 0;
            var probed = SuccessorProbed(table);
            SynthesizeEndingListing(table, false, (x, y) => (first, second) = (x, y));
            long nativeAtProbe = -1;
            long chargedAtProbe = -1;
            var crossed = false;
            memory.BeforeReserveForTests = _ =>
            {
                // The first reserve after the projection's probe, before the filter
                if (probed() && nativeAtProbe < 0)
                {
                    nativeAtProbe = meter.Buckets.NativeRequests;
                    chargedAtProbe = meter.Buckets.Requests.Charged;
                }
                // The first reserve after the filter charged what it owed
                else if (nativeAtProbe >= 0 && !crossed && meter.Buckets.NativeRequests > nativeAtProbe)
                {
                    crossed = true;
                    clock.Advance(TimeSpan.FromHours(6));
                }
            };

            var (result, seen) = await RunAsync(sweep, table, EndingContext(clock, default, memory));

            AssertResult(result, OutcomeKind.Preempted, MaintenanceReason.JobDeadline);
            Assert.Null(seen);
            Assert.True(crossed);
            Assert.True(first > 1 && second > first, $"{first} then {second} resolved");
            // Charged in turns at the first check, owed at the second
            Assert.Equal(first, meter.Buckets.NativeRequests - nativeAtProbe);
            Assert.Equal(second, meter.Buckets.Requests.Charged - chargedAtProbe);
        }

        [Theory]
        [InlineData("write")]
        [InlineData("listing")]
        [InlineData("probe")]
        public async Task DeadlinePassedDuringARequestStopsTheSweepBeforeItsNextStep(string crossed)
        {
            await using var server = new LoopbackObjectStore(StoreFlavor.S3);
            server.Now = Now;
            var table = LoopbackTable(server);
            await RetainedTableAsync(table.Raw, table.Name, 0);
            server.Put("tables/t/old.parquet", new byte[3], s_old);
            var clock = new FakeTimeProvider(Now);
            var run = new JobRun(0, JobKind.Orphan) { Cancellation = new CancellationTokenSource() };
            using var cancellation = run.Cancellation;
            var context = new CleanupContext(run, clock)
            {
                Kind = JobKind.Orphan,
                Long = true,
                ReadLimit = long.MaxValue,
                Deadline = Now.AddHours(6),
                Head = 0,
                TableId = null,
                Scratch = Scratch(DiskLedger.DefaultCap),
                Memory = new JobMemory(),
                Transfer = new TransferAllowance(),
                Stop = cancellation.Token
            };
            var cleanup = new DeltaTableCleanup(table.Configuration, Metered(table), clock, table.Name, null,
                () => table.Raw, new DiscardCounters(), null, table.Name);
            Assert.Equal(OrphanSweepRequestResult.Scheduled, cleanup.RequestOrphanSweep());
            var reference = table.Prefix + OrphanSweep.ReferenceName;
            var successor = $"{table.Prefix}_delta_log/{1:D20}.json";
            // The provider completes the request after the deadline stops new issuance
            switch (crossed)
            {
                case "write":
                    server.Intercept = (method, target) =>
                    {
                        if (method == "PUT" && target.Contains(OrphanSweep.ReferenceName, StringComparison.Ordinal))
                        {
                            clock.Advance(TimeSpan.FromHours(6));
                        }
                        return null;
                    };
                    break;
                case "listing":
                    table.Owned.AfterRecursiveLs = (_, entries) =>
                    {
                        clock.Advance(TimeSpan.FromHours(6));
                        return entries;
                    };
                    break;
                default:
                    table.Owned.StatAnswer = (path, entry) =>
                    {
                        if (path.Full == successor && table.Owned.Recursive.Count == 1)
                        {
                            clock.Advance(TimeSpan.FromHours(6));
                        }
                        return entry;
                    };
                    break;
            }

            CleanupResult result;
            try
            {
                result = await cleanup.RunAsync(JobKind.Orphan, context);
            }
            finally
            {
                await context.SettleAsync();
            }

            Assert.Equal(0, context.Memory.Managed);
            Assert.Equal(0, context.Memory.Native);
            Assert.Empty(context.Scratch.Files);
            var after = table.Owned.CallsAfterRecursive();
            Assert.True(result.Kind == OutcomeKind.Preempted && result.Reason == MaintenanceReason.JobDeadline,
                $"{result.Kind} {result.Reason}: {result.Detail}; calls after the root listing: {string.Join(", ", after)}");
            switch (crossed)
            {
                case "write":
                    Assert.DoesNotContain("Stat " + reference, table.Owned.Calls);
                    Assert.Empty(table.Owned.Recursive);
                    break;
                case "listing":
                    Assert.Empty(after);
                    break;
                default:
                    Assert.Equal(new[] { "Stat " + successor }, after);
                    break;
            }
            Assert.True(cleanup.IsDue(JobKind.Orphan, 0, clock.GetUtcNow()));
            Assert.Equal(OrphanSweepRequestResult.AlreadyPending, cleanup.RequestOrphanSweep());
        }

        [Theory]
        [InlineData("reference")]
        [InlineData("listing")]
        [InlineData("filter")]
        public async Task StopDuringTheInventoryCancelsAndReleasesEverything(string stopped)
        {
            var table = await LocalWithOldAsync(2);
            using var stop = new CancellationTokenSource();
            var memory = new JobMemory();
            var sweep = Sweep(table.Configuration, table.Owned);
            sweep.ArenaCap = EndingArena;
            var frozen = false;
            sweep.FrozenHookForTests = _ => frozen = true;
            SynthesizeEndingListing(table, false);
            switch (stopped)
            {
                case "reference":
                    var reference = table.Prefix + ".flowtide-orphan-sweep";
                    table.Owned.StatAnswer = (path, entry) =>
                    {
                        if (path.Full == reference)
                        {
                            stop.Cancel();
                            stop.Token.ThrowIfCancellationRequested();
                        }
                        return entry;
                    };
                    break;
                case "listing":
                    // The provider observes the stop
                    table.Owned.AfterRecursiveLs = (_, _) =>
                    {
                        stop.Cancel();
                        stop.Token.ThrowIfCancellationRequested();
                        return Array.Empty<IOEntry>();
                    };
                    break;
                default:
                    var probed = SuccessorProbed(table);
                    // The first reserve after the projection's probe, before the filter
                    memory.BeforeReserveForTests = _ =>
                    {
                        if (probed())
                        {
                            stop.Cancel();
                        }
                    };
                    break;
            }
            var context = EndingContext(new FakeTimeProvider(Now), stop.Token, memory);

            OperationCanceledException canceled;
            try
            {
                canceled = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => sweep.RunAsync(context));
            }
            finally
            {
                await context.SettleAsync();
            }

            Assert.Equal(stop.Token, canceled.CancellationToken);
            Assert.False(frozen);
            Assert.Equal(stopped == "reference" ? 0 : 1, table.Owned.Recursive.Count);
            Assert.Equal(0, memory.Managed);
            Assert.Equal(0, memory.Native);
            Assert.Empty(context.Scratch.Files);
            Assert.DoesNotContain(table.Owned.Calls, x => x.StartsWith("Rm ", StringComparison.Ordinal));
        }
    }
}
