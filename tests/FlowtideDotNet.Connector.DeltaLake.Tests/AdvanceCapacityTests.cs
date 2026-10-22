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
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.Advance;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.Deletion;
using Microsoft.Extensions.Time.Testing;
using Stowage;
using System.Diagnostics.Metrics;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public class AdvanceCapacityTests
    {
        private static readonly DateTimeOffset s_now = new DateTimeOffset(2026, 10, 17, 0, 0, 0, TimeSpan.Zero);

        // Bounded: a reservation that never returns fails the test
        private static readonly TimeSpan s_wait = TimeSpan.FromSeconds(30);

        // Hours of drain on the request or the byte axis alone
        private static CapacityDemand Hours(double hours) => new CapacityDemand((long)Math.Round(hours * 3600 * MaintenanceBuckets.DefaultRequestsPerSecond), 0);

        private static CapacityDemand ByteHours(double hours) => new CapacityDemand(0, (long)Math.Round(hours * 3600 * MaintenanceBuckets.DefaultBytesPerSecond));

        private static CapacityLedger Ledger() => new CapacityLedger(() => default);

        // An advance with nothing to do on local disk without latency; tests add the work they price
        private static readonly AdvanceDemandInputs s_empty = new AdvanceDemandInputs(LogTargets: 0, PayloadPaths: 0, Parts: 0, PartBytes: 0, CheckpointBytes: 0, AnchorBytes: 0,
            CapabilityBytes: 0, CapabilityObjects: 0, LogEntries: 0, Paging: ListingPaging.LocalDisk, Outstanding: 4, Latency: TimeSpan.Zero);

        // Admitted alone on an empty ledger; the lease goes back
        private static bool AdmittedAlone(CapacityDemand demand, CapacityDemand current = default)
        {
            var ledger = Ledger();
            if (current != default)
            {
                ledger.Restore(default, "table-0", "advance-0", current, CapacityState.Runnable);
            }
            using var lease = ledger.TryReserve(default, "table-a", "advance-a", demand);
            return lease != null;
        }

        [Fact]
        public void CapacityUsesRequestAndByteDebt()
        {
            // Two 1 TiB parts, written and read back, the header read for each: bytes alone pass twelve hours
            var bytes = CapacityEstimator.Advance(s_empty with { Parts = 2, PartBytes = 2L << 40 }, MaintenanceBuckets.DefaultRequestsPerSecond);
            Assert.Equal(new CapacityDemand(2 * 2 + 2 + 8, (4L << 40) + 2 * (4096 + 2 * 76) + 2 * (4 * 4096 + 2 * 76)), bytes);
            Assert.False(AdmittedAlone(bytes));

            // A million serial log deletes at the default latency, six hundred to a slice whose gate also reads the hint: requests alone pass twelve hours
            var requests = CapacityEstimator.Advance(s_empty with { LogTargets = 1_000_000, Latency = CapacityEstimator.DefaultLatency }, MaintenanceBuckets.DefaultRequestsPerSecond);
            Assert.Equal(new CapacityDemand(1667 * 5 + 8 + 10_000_000, 2 * 4 * 4096), requests);
            Assert.False(AdmittedAlone(requests));

            // About eight hours on each axis drains in eight, not sixteen
            var both = CapacityEstimator.Advance(s_empty with { LogTargets = 576_000, Latency = CapacityEstimator.DefaultLatency, Parts = 1, PartBytes = (long)(8 * 1800 * MaintenanceBuckets.DefaultBytesPerSecond) }, MaintenanceBuckets.DefaultRequestsPerSecond);
            Assert.InRange(both.Requests / (double)MaintenanceBuckets.DefaultRequestsPerSecond, 8 * 3600, 8.01 * 3600);
            Assert.InRange(both.Bytes / (double)MaintenanceBuckets.DefaultBytesPerSecond, 8 * 3600, 8.01 * 3600);
            Assert.InRange(Ledger().DrainSeconds(both), 8 * 3600, 8.01 * 3600);
            Assert.True(AdmittedAlone(both));
        }

        [Fact]
        public void HashValidationCostEntersDebt()
        {
            // A thousand payload slices of a slice's paths and 52 a full buffer may end, each gate rehashing C, the anchor and the capability sources
            var paths = s_empty with { PayloadPaths = 1_000_000 };
            var small = CapacityEstimator.Advance(paths with { CheckpointBytes = 32L << 20 }, MaintenanceBuckets.DefaultRequestsPerSecond);
            Assert.Equal(1052 * (32L << 20) + 2 * 1056 * 4096, small.Bytes);
            Assert.True(AdmittedAlone(small));

            // The same paths with a 3 GiB checkpoint, anchor or capability source decline
            Assert.False(AdmittedAlone(CapacityEstimator.Advance(paths with { CheckpointBytes = 3L << 30 }, MaintenanceBuckets.DefaultRequestsPerSecond)));
            Assert.False(AdmittedAlone(CapacityEstimator.Advance(paths with { AnchorBytes = 3L << 30 }, MaintenanceBuckets.DefaultRequestsPerSecond)));
            Assert.False(AdmittedAlone(CapacityEstimator.Advance(paths with { CapabilityBytes = 3L << 30 }, MaintenanceBuckets.DefaultRequestsPerSecond)));

            // Each rehashed object is a request per gate
            var objects = CapacityEstimator.Advance(paths with { CapabilityObjects = 3 }, MaintenanceBuckets.DefaultRequestsPerSecond);
            Assert.Equal(3 * 1052, objects.Requests - CapacityEstimator.Advance(paths, MaintenanceBuckets.DefaultRequestsPerSecond).Requests);
        }

        [Fact]
        public void ListingAndCandidateProbeCostEntersDebt()
        {
            // 1,052 gates, each listing 64,000 log entries: S3 pages 63 more times, Azure 12, local disk none
            var paths = s_empty with { PayloadPaths = 1_000_000, LogEntries = 64_000 };
            var local = CapacityEstimator.Advance(paths, MaintenanceBuckets.DefaultRequestsPerSecond);
            var azure = CapacityEstimator.Advance(paths with { Paging = ListingPaging.Azure }, MaintenanceBuckets.DefaultRequestsPerSecond);
            var s3 = CapacityEstimator.Advance(paths with { Paging = ListingPaging.S3 }, MaintenanceBuckets.DefaultRequestsPerSecond);
            // Deletes, gates of a listing, a successor probe and two rehashes, records written and read back, and each slice's part read and predecessor delete
            Assert.Equal(1_000_000 + 1052 * 4 + 2 * 1056 + 2 * 1052, local.Requests);
            Assert.Equal(local.Requests + 1052 * 12, azure.Requests);
            Assert.Equal(local.Requests + 1052 * 63, s3.Requests);

            // Near the threshold the paging decides
            var current = new CapacityDemand(7_600_000, 0);
            Assert.True(AdmittedAlone(local, current));
            Assert.True(AdmittedAlone(azure, current));
            Assert.False(AdmittedAlone(s3, current));

            // Each Azure orphan candidate's positive-size probe is a request
            var orphans = CapacityEstimator.Advance(paths with { Paging = ListingPaging.Azure, OrphanCandidates = 60_000 }, MaintenanceBuckets.DefaultRequestsPerSecond);
            Assert.Equal(azure.Requests + 60_000, orphans.Requests);
            Assert.False(AdmittedAlone(orphans, current));

            // An unknown store probes each payload delete first
            Assert.Equal(local.Requests + 1_000_000, CapacityEstimator.Advance(paths with { UnknownStore = true }, MaintenanceBuckets.DefaultRequestsPerSecond).Requests);
        }

        [Fact]
        public void LatencyBoundDeletesCountAsTheRequestsTheyWaitOut()
        {
            // Serial log deletes at 50 ms hold ten request turns each and fill two slices, each gate with its hint read; payload deletes share their wait across the outstanding limit
            var latency = s_empty with { Latency = CapacityEstimator.DefaultLatency };
            var none = CapacityEstimator.Advance(latency, MaintenanceBuckets.DefaultRequestsPerSecond).Requests;
            Assert.Equal(1000 * 10 + 2 * 4 + 2, CapacityEstimator.Advance(latency with { LogTargets = 1000 }, MaintenanceBuckets.DefaultRequestsPerSecond).Requests - none);
            Assert.Equal(1000 * 10 / 4 + 4 + 2 + 2, CapacityEstimator.Advance(latency with { PayloadPaths = 1000 }, MaintenanceBuckets.DefaultRequestsPerSecond).Requests - none);
            // One outstanding at a time also fills two slices, each with its gate, progress record, part read and predecessor delete
            Assert.Equal(1000 * 10 + 2 * 4 + 2 * 2 + 2 * 2, CapacityEstimator.Advance(latency with { PayloadPaths = 1000, Outstanding = 1 }, MaintenanceBuckets.DefaultRequestsPerSecond).Requests - none);
            // Without latency each delete is its own request
            Assert.Equal(1000 + 4 + 2 + 2, CapacityEstimator.Advance(s_empty with { PayloadPaths = 1000 }, MaintenanceBuckets.DefaultRequestsPerSecond).Requests - CapacityEstimator.Advance(s_empty, MaintenanceBuckets.DefaultRequestsPerSecond).Requests);
        }

        [Fact]
        public void LatencyBoundDeletesScaleWithTheRequestRate()
        {
            // At 5,000 requests a second a serial 50 ms delete holds 250 request turns
            var latency = s_empty with { Latency = CapacityEstimator.DefaultLatency };
            var deletes = latency with { LogTargets = 1000 };
            var raised = CapacityEstimator.Advance(deletes, 5000);
            Assert.Equal(1000 * 250 + 2 * 4 + 2, raised.Requests - CapacityEstimator.Advance(latency, 5000).Requests);

            // The wait drains as long at either rate, about fifty seconds
            var atDefault = Ledger().DrainSeconds(CapacityEstimator.Advance(deletes, MaintenanceBuckets.DefaultRequestsPerSecond));
            var atRaised = new CapacityLedger(() => default, 5000).DrainSeconds(raised);
            Assert.InRange(atDefault, 50, 51);
            Assert.InRange(atRaised, atDefault - 0.1, atDefault + 0.1);
        }

        [Fact]
        public void LedgerDrainsAtItsConfiguredRates()
        {
            var raised = new CapacityLedger(() => default, 5000, 256L << 20);
            Assert.Equal(1.44, raised.DrainSeconds(new CapacityDemand(7200, 0)), 9);
            Assert.Equal(5000, raised.RequestsPerSecond);
            Assert.Equal(256L << 20, raised.BytesPerSecond);

            // Thirteen hours at the defaults is about half an hour of requests or three and a quarter hours of bytes at the raised rates
            foreach (var demand in new[] { Hours(13), ByteHours(13) })
            {
                using var admitted = raised.TryReserve(default, "table-a", $"advance-{demand.Requests}", demand);
                Assert.NotNull(admitted);
                Assert.False(AdmittedAlone(demand));
            }
            Assert.InRange(raised.DrainSeconds(Hours(13)) / 3600, 0.51, 0.53);
            Assert.InRange(raised.DrainSeconds(ByteHours(13)) / 3600, 3.24, 3.26);
        }

        [Fact]
        public void SerialLogIssuanceTimeEntersRepeatedHashDebt()
        {
            var inputs = s_empty with { LogTargets = 576_000, CheckpointBytes = 3L << 30, Latency = CapacityEstimator.DefaultLatency };
            var demand = CapacityEstimator.Advance(inputs, MaintenanceBuckets.DefaultRequestsPerSecond);
            // Serial 50 ms deletes issue at most 600 times in a slice's thirty seconds
            var perSlice = new DeletionRunSettings().IssuanceTime.Ticks / inputs.Latency.Ticks;
            var slices = (inputs.LogTargets + perSlice - 1) / perSlice;
            var admitted = AdmittedAlone(demand);

            Assert.True(demand.Bytes >= slices * inputs.CheckpointBytes && !admitted,
                $"Admission={admitted}: priced {demand.Bytes} bytes, but {slices} serial log slices rehash at least {slices * inputs.CheckpointBytes}");
        }

        [Fact]
        public void PayloadBufferPackingEntersRepeatedHashDebt()
        {
            // 700,000 qualified 1 KiB keys in parts of 2,000, deleted serially
            const int keyLength = 1024;
            const int perPart = 2000;
            Assert.Null(CleanupStoreContract.CheckSegments("t/" + new string('a', keyLength - 2), qualifiedDomain: true));
            var partLength = ControlRecords.PartLength(perPart * keyLength, perPart);
            var inputs = s_empty with { PayloadPaths = 700_000, Parts = 350, PartBytes = 350L * partLength, CheckpointBytes = 2L << 30, Outstanding = 1, Latency = CapacityEstimator.DefaultLatency };
            Assert.True(partLength <= ControlRecords.PartCap && inputs.Parts <= ControlRecords.MaxDescriptors);
            var demand = CapacityEstimator.Advance(inputs, MaintenanceBuckets.DefaultRequestsPerSecond);
            // A slice buffers at most 1 MiB, each target charged its overhead and its key's UTF-16 characters
            var perSlice = DeletionRunSettings.DefaultMaxBufferBytes / (48 + 2L * keyLength);
            var slices = (inputs.PayloadPaths + perSlice - 1) / perSlice;
            var admitted = AdmittedAlone(demand);

            Assert.True(demand.Bytes >= slices * inputs.CheckpointBytes && !admitted,
                $"Admission={admitted}: priced {demand.Bytes} bytes, but {slices} buffered payload slices rehash at least {slices * inputs.CheckpointBytes}");
        }

        [Fact]
        public void SortedPayloadLengthRegionsCannotHideRepeatedHashDebt()
        {
            // A sorted native inventory: 350,000 long keys under a/, then 350,000 short keys under z/
            const int regionPaths = 350_000;
            const int longLength = 1900;
            const int shortLength = 148;
            const int longPartPaths = 1000;
            const int shortPartPaths = 2000;
            var longPart = ControlRecords.PartLength(longPartPaths * longLength, longPartPaths);
            var shortPart = ControlRecords.PartLength(shortPartPaths * shortLength, shortPartPaths);
            Assert.True(longPart <= ControlRecords.PartCap && shortPart <= ControlRecords.PartCap);
            var parts = regionPaths / longPartPaths + regionPaths / shortPartPaths;
            Assert.True(parts <= ControlRecords.MaxDescriptors);
            var partBytes = (long)regionPaths / longPartPaths * longPart + (long)regionPaths / shortPartPaths * shortPart;

            // Distinct ASCII paths within the native full-path limit, every segment below 255 characters
            var longExample = "t/a000000/" + string.Join("/", Enumerable.Repeat(new string('a', 199), 9)) + "/" + new string('a', 90);
            var shortExample = "t/z000000/" + new string('z', 138);
            Assert.Equal(longLength, longExample.Length);
            Assert.Equal(shortLength, shortExample.Length);
            Assert.Null(CleanupStoreContract.CheckSegments(longExample, qualifiedDomain: true));
            Assert.Null(CleanupStoreContract.CheckSegments(shortExample, qualifiedDomain: true));

            // Sequential 1 MiB packing; four outstanding 50 ms deletes never cut a 1,000-path slice short
            long slices = 0;
            long charged = 0;
            var buffered = 0;
            for (var position = 0; position < 2 * regionPaths; position++)
            {
                var size = DeletionRun.TargetOverheadBytes + 2L * (position < regionPaths ? longLength : shortLength);
                if (buffered == DeletionRunSettings.DefaultMaxPaths || charged + size > DeletionRunSettings.DefaultMaxBufferBytes)
                {
                    slices++;
                    charged = 0;
                    buffered = 0;
                }
                charged += size;
                buffered++;
            }
            if (buffered > 0)
            {
                slices++;
            }
            Assert.True(slices >= 1636);
            var inputs = s_empty with { PayloadPaths = 2L * regionPaths, Parts = parts, PartBytes = partBytes, CheckpointBytes = 7L << 28, Latency = CapacityEstimator.DefaultLatency };
            var required = slices * inputs.CheckpointBytes;
            Assert.True(required / (double)MaintenanceBuckets.DefaultBytesPerSecond > CapacityLedger.CloseSeconds);
            var demand = CapacityEstimator.Advance(inputs, MaintenanceBuckets.DefaultRequestsPerSecond);
            var admitted = AdmittedAlone(demand);

            Assert.True(demand.Bytes >= required && !admitted, $"Admission={admitted}: priced {demand.Bytes} bytes, but {slices} sorted-region payload slices rehash at least {required}");
        }

        [Fact]
        public void LongestPathBuffersEnterRepeatedHashDebt()
        {
            // 2,000 parts of 31 keys at the longest path a part holds, seven to a 1 MiB buffer, each part ending its own slice
            const int perPart = 31;
            var partLength = ControlRecords.PartLength(perPart * ControlRecords.MaxPathBytes, perPart);
            Assert.True(partLength <= ControlRecords.PartCap);
            var inputs = s_empty with { PayloadPaths = 2000 * perPart, Parts = 2000, PartBytes = 2000L * partLength, CheckpointBytes = 32L << 20 };
            var perSlice = DeletionRunSettings.DefaultMaxBufferBytes / (DeletionRun.TargetOverheadBytes + 2L * ControlRecords.MaxPathBytes);
            var slices = inputs.Parts * ((perPart + perSlice - 1) / perSlice);
            var demand = CapacityEstimator.Advance(inputs, MaintenanceBuckets.DefaultRequestsPerSecond);

            Assert.True(demand.Bytes >= slices * inputs.CheckpointBytes, $"Priced {demand.Bytes} bytes, but {slices} buffered payload slices rehash at least {slices * inputs.CheckpointBytes}");
        }

        [Fact]
        public void FullPartsCannotHideTheirPartialPayloadSlices()
        {
            // Full native Windows parts with qualified segments, within the native 32,767-character limit
            const int pathLength = 16_384;
            const int keyExtra = 64;
            const int pathsPerPart = 127;
            const int parts = 800;
            var example = "t/" + string.Join("/", Enumerable.Repeat(new string('a', 199), 81)) + "/" + new string('a', 182);
            Assert.Equal(pathLength, example.Length);
            Assert.True(example.Length + keyExtra < 32_767);
            Assert.Null(CleanupStoreContract.CheckSegments(example, qualifiedDomain: true));
            var partLength = ControlRecords.PartLength(pathsPerPart * pathLength, pathsPerPart);
            Assert.InRange(partLength, 1, ControlRecords.PartCap);
            Assert.True(ControlRecords.PartLength((pathsPerPart + 1) * pathLength, pathsPerPart + 1) > ControlRecords.PartCap);
            var partBytes = (long)parts * partLength;
            Assert.True(2 * partBytes < 4L << 30);

            // Each part ends its own last slice, which cannot pack the next part's paths
            var perSlice = DeletionRunSettings.DefaultMaxBufferBytes / (DeletionRun.TargetOverheadBytes + 2L * (pathLength + keyExtra));
            var slices = parts * ((pathsPerPart + perSlice - 1) / perSlice);
            Assert.Equal(31, perSlice);
            Assert.Equal(4000, slices);
            var inputs = new AdvanceDemandInputs(LogTargets: 0, PayloadPaths: (long)parts * pathsPerPart, Parts: parts, PartBytes: partBytes,
                CheckpointBytes: 720L << 20, AnchorBytes: 0, CapabilityBytes: 0, CapabilityObjects: 0, LogEntries: 0, Paging: ListingPaging.LocalDisk,
                Outstanding: 4, Latency: CapacityEstimator.DefaultLatency, KeyExtra: keyExtra);
            var requiredHashes = slices * inputs.CheckpointBytes;
            Assert.True(requiredHashes / (double)MaintenanceBuckets.DefaultBytesPerSecond > CapacityLedger.CloseSeconds);
            var demand = CapacityEstimator.Advance(inputs, MaintenanceBuckets.DefaultRequestsPerSecond);
            var admitted = AdmittedAlone(demand);

            Assert.True(demand.Bytes >= requiredHashes && !admitted,
                $"Admission={admitted}: priced {demand.Bytes} bytes, but {slices} part-bounded payload slices rehash at least {requiredHashes}");
        }

        [Fact]
        public void CapacityHysteresis()
        {
            var ledger = Ledger();
            var current = ledger.Restore(default, "table-a", "advance-a", Hours(8), CapacityState.Runnable);
            Assert.True(ledger.Admits());

            // Eight hours and five more pass twelve: declined, and admission closes
            Assert.Null(ledger.TryReserve(default, "table-b", "advance-b", Hours(5)));
            Assert.True(ledger.Closed);

            // One more hour fits under twelve, but stays declined while the current drain is at least six hours
            Assert.Null(ledger.TryReserve(default, "table-b", "advance-c", Hours(1)));
            Assert.False(ledger.Admits());
            current.Update(Hours(6));
            Assert.Null(ledger.TryReserve(default, "table-b", "advance-c", Hours(1)));
            Assert.True(ledger.Closed);

            // Validated progress leaves less than six hours: admission reopens
            current.Update(Hours(5.9));
            var admitted = ledger.TryReserve(default, "table-b", "advance-c", Hours(1));
            Assert.NotNull(admitted);
            Assert.False(ledger.Closed);
            Assert.Equal(Hours(6.9), ledger.Current);
            Assert.True(ledger.Admits());

            // The byte axis closes and reopens the same way
            var bytes = Ledger();
            var held = bytes.Restore(default, "table-a", "advance-a", ByteHours(8), CapacityState.Runnable);
            Assert.Null(bytes.TryReserve(default, "table-b", "advance-b", ByteHours(5)));
            Assert.Null(bytes.TryReserve(default, "table-b", "advance-c", ByteHours(1)));
            held.Update(ByteHours(5));
            Assert.NotNull(bytes.TryReserve(default, "table-b", "advance-c", ByteHours(1)));
        }

        [Fact]
        public void RunnableDemandPastTwelveHoursClosesAdmissionHoweverItArrives()
        {
            // Restored past twelve hours: closed at once, and still closed once it drains to eight
            var restored = Ledger();
            var lease = restored.Restore(default, "table-a", "advance-a", Hours(13), CapacityState.Runnable);
            Assert.True(restored.Closed);
            lease.Update(Hours(8));
            Assert.False(restored.Admits());

            // Re-estimated past twelve hours, then back to eight
            var updated = Ledger();
            var raised = updated.Restore(default, "table-a", "advance-a", Hours(1), CapacityState.Runnable);
            raised.Update(Hours(13));
            raised.Update(Hours(8));
            Assert.False(updated.Admits());

            // Below six hours both reopen
            lease.Update(Hours(5.9));
            raised.Update(Hours(5.9));
            Assert.True(restored.Admits());
            Assert.True(updated.Admits());
        }

        [Fact]
        public async Task CapacityAdmissionIsProspectiveAndAtomic()
        {
            // Ten hours current and three proposed: under twelve now, over twelve with the proposal
            var ledger = Ledger();
            ledger.Restore(default, "table-a", "advance-a", Hours(10), CapacityState.Runnable);
            Assert.True(ledger.Admits());
            Assert.Null(ledger.TryReserve(default, "table-b", "advance-b", Hours(3)));
            Assert.True(AdmittedAlone(Hours(1.9), Hours(10)));

            // Two seven-hour reservations race on an empty ledger; the second waits between the first's check and add
            var race = Ledger();
            var firstAtCheck = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var bothBetween = false;
            var between = 0;
            using var release = new ManualResetEventSlim();
            race.BetweenCheckAndAddForTests = () =>
            {
                if (Interlocked.Increment(ref between) == 1)
                {
                    firstAtCheck.SetResult();
                    release.Wait();
                }
                else
                {
                    bothBetween = true;
                }
            };
            var first = Reserve(race, "table-a", "advance-a", Hours(7), out var firstThread);
            await firstAtCheck.Task.WaitAsync(s_wait);
            var second = Reserve(race, "table-b", "advance-b", Hours(7), out var secondThread);
            try
            {
                // The second either passed its own check or waits on the ledger's lock
                var deadline = DateTime.UtcNow + s_wait;
                while (Volatile.Read(ref between) < 2 && (secondThread.ThreadState & ThreadState.WaitSleepJoin) == 0)
                {
                    Assert.True(DateTime.UtcNow < deadline, "The second reservation neither checked nor waited");
                    await Task.Delay(1);
                }
            }
            finally
            {
                release.Set();
            }
            var leases = await Task.WhenAll(first, second).WaitAsync(s_wait);

            Assert.False(bothBetween, "Both reservations passed their check before either added");
            Assert.Single(leases, x => x != null);
            Assert.Equal(Hours(7), race.Current);
        }

        // A reservation on a thread of its own, which the test's gate may block
        private static Task<CapacityLease?> Reserve(CapacityLedger ledger, string leaseKey, string advanceId, CapacityDemand demand, out Thread thread)
        {
            var done = new TaskCompletionSource<CapacityLease?>(TaskCreationOptions.RunContinuationsAsynchronously);
            thread = new Thread(() =>
            {
                try
                {
                    done.SetResult(ledger.TryReserve(default, leaseKey, advanceId, demand));
                }
                catch (Exception e)
                {
                    done.SetException(e);
                }
            });
            thread.IsBackground = true;
            thread.Start();
            return done.Task;
        }

        [Fact]
        public void RestoredDemandIsExemptAndReplacesItsKey()
        {
            var ledger = Ledger();
            // Exempt from admission, even above twelve hours
            var first = ledger.Restore(default, "table-a", "advance-a", Hours(13), CapacityState.Runnable);
            Assert.Equal(Hours(13), ledger.Current);
            Assert.False(ledger.Admits());

            // A restart restores the same advance: replaced, not added, and the replaced lease changes nothing
            using var stopped = new CancellationTokenSource();
            var second = ledger.Restore(stopped.Token, "table-a", "advance-a", Hours(2), CapacityState.Runnable);
            first.Update(Hours(20));
            first.Dispose();
            Assert.Equal(Hours(2), ledger.Current);
            Assert.Equal(Hours(2), second.Demand);
            Assert.True(ledger.Admits());

            // Paused and suspended demand is reported apart and never counts toward admission; a stop pauses only its own worker's leases
            var suspended = ledger.Restore(default, "table-b", "advance-b", Hours(20), CapacityState.Suspended);
            var other = ledger.Restore(default, "table-c", "advance-c", Hours(1), CapacityState.Runnable);
            stopped.Cancel();
            ledger.Pause(stopped.Token);
            Assert.Equal(CapacityState.Paused, second.State);
            Assert.Equal(CapacityState.Runnable, other.State);
            Assert.Equal(Hours(1), ledger.Current);
            Assert.Equal(Hours(2), ledger.Total(CapacityState.Paused));
            Assert.Equal(Hours(20), ledger.Total(CapacityState.Suspended));
            Assert.True(ledger.Admits());

            // A stopped owner's runnable demand installs paused
            using (var late = ledger.Restore(stopped.Token, "table-d", "advance-d", Hours(1), CapacityState.Runnable))
            {
                Assert.Equal(CapacityState.Paused, late.State);
                Assert.Equal(Hours(1), ledger.Current);
            }

            // Resumed by restoring it runnable
            var resumed = ledger.Restore(default, "table-a", "advance-a", Hours(2), CapacityState.Runnable);
            Assert.Equal(Hours(3), ledger.Current);
            Assert.Equal(default, ledger.Total(CapacityState.Paused));

            // Disposed leases leave nothing
            resumed.Dispose();
            other.Dispose();
            suspended.Dispose();
            second.Dispose();
            Assert.Equal(default, ledger.Current);
            Assert.Equal(default, ledger.Total(CapacityState.Suspended));
        }

        private static DeltaMaintenanceBudget Budget(MaintenanceBuckets buckets)
        {
            var settings = new MaintenanceBudgetSettings(2, Path.Combine(Path.GetTempPath(), "cap-" + ScratchOwners.ShortId()));
            return new DeltaMaintenanceBudget(settings, new FakeTimeProvider(s_now), manualTick: true) { Buckets = buckets };
        }

        [Fact]
        public async Task AmbientDemandIsLiveHintsAndBucketDebt()
        {
            var clock = new FakeTimeProvider(s_now);
            var budget = Budget(new MaintenanceBuckets(clock, requestsPerSecond: 1, requestBurst: 1, bytesPerSecond: 1, byteBurst: 1));
            var route = new DiscardRoute(Files.Of.InternalMemory($"./capacity-{Guid.NewGuid():N}"), "t");
            using (var handle = budget.Discards.Acquire(route))
            {
                Assert.Equal(0, handle.Entry.Enqueue(new DiscardGroup("table-a", 0, 1, "stage-1", new[] { "a.parquet", "b.parquet", "c.parquet" })));
                Assert.Equal(new CapacityDemand(3, 0), budget.Capacity.Current);

                // Hints dropped without a delete no longer count
                Assert.Equal(3, budget.Discards.DropQueued(route));
                Assert.Equal(3, budget.Discards.Dropped);
                Assert.Equal(default, budget.Capacity.Current);
            }

            // Debt the buckets are owed counts until it is repaid
            await budget.Buckets.Requests.AcquireAsync(1);
            await budget.Buckets.Bytes.AcquireAsync(1);
            using var cancel = new CancellationTokenSource();
            var requests = budget.Buckets.Requests.RepayAsync(Hours(12).Requests + 1, 1, cancel.Token);
            var bytes = budget.Buckets.Bytes.RepayAsync(5, 1, cancel.Token);
            Assert.Equal(new CapacityDemand(Hours(12).Requests + 1, 5), budget.Capacity.Current);
            Assert.False(budget.Capacity.Admits());
            cancel.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => requests);
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => bytes);
        }

        [Fact]
        public void GaugesReportRunnableDemandAndDrainByState()
        {
            var budget = Budget(MaintenanceBucketsTests.Unthrottled());
            using var meter = new Meter("capacity-" + Guid.NewGuid().ToString("N"));
            DeltaMaintenanceBudget.SharedBudget.RegisterGauges(meter, budget);
            budget.Capacity.Restore(default, "table-a", "advance-a", new CapacityDemand(7199, 3 * MaintenanceBuckets.DefaultBytesPerSecond), CapacityState.Runnable);
            budget.Capacity.Restore(default, "table-b", "advance-b", Hours(2), CapacityState.Paused);
            budget.Capacity.Restore(default, "table-c", "advance-c", ByteHours(1), CapacityState.Suspended);
            // A queued hint is runnable demand outside the ledger
            using var handle = budget.Discards.Acquire(new DiscardRoute(Files.Of.InternalMemory($"./capacity-{Guid.NewGuid():N}"), "t"));
            Assert.Equal(0, handle.Entry.Enqueue(new DiscardGroup("table-a", 0, 1, "stage-1", new[] { "a.parquet" })));

            var measured = Measure(meter);
            Assert.Equal(7200.0, measured.GetValueOrDefault("flowtide_delta_maintenance_runnable_request_equivalents", double.NaN));
            Assert.Equal(3.0 * MaintenanceBuckets.DefaultBytesPerSecond, measured.GetValueOrDefault("flowtide_delta_maintenance_runnable_bytes", double.NaN));
            Assert.Equal(36.0, measured.GetValueOrDefault("flowtide_delta_maintenance_estimated_drain_seconds{state=runnable}", double.NaN));
            Assert.Equal(7200.0, measured.GetValueOrDefault("flowtide_delta_maintenance_estimated_drain_seconds{state=paused}", double.NaN));
            Assert.Equal(3600.0, measured.GetValueOrDefault("flowtide_delta_maintenance_estimated_drain_seconds{state=suspended}", double.NaN));
        }

        [Fact]
        public void GaugesReportDrainAtTheConfiguredRates()
        {
            // Unthrottled buckets: a drain priced at the buckets' rates would be about zero
            var settings = new MaintenanceBudgetSettings(2, Path.Combine(Path.GetTempPath(), "cap-" + ScratchOwners.ShortId())) { RequestsPerSecond = 400, BytesPerSecond = 128L << 20 };
            var budget = new DeltaMaintenanceBudget(settings, new FakeTimeProvider(s_now), manualTick: true) { Buckets = MaintenanceBucketsTests.Unthrottled() };
            using var meter = new Meter("capacity-" + Guid.NewGuid().ToString("N"));
            DeltaMaintenanceBudget.SharedBudget.RegisterGauges(meter, budget);
            budget.Capacity.Restore(default, "table-a", "advance-a", new CapacityDemand(7200, 3 * MaintenanceBuckets.DefaultBytesPerSecond), CapacityState.Runnable);
            budget.Capacity.Restore(default, "table-b", "advance-b", Hours(2), CapacityState.Paused);
            budget.Capacity.Restore(default, "table-c", "advance-c", ByteHours(1), CapacityState.Suspended);

            var measured = Measure(meter);
            Assert.Equal(18.0, measured.GetValueOrDefault("flowtide_delta_maintenance_estimated_drain_seconds{state=runnable}", double.NaN));
            Assert.Equal(3600.0, measured.GetValueOrDefault("flowtide_delta_maintenance_estimated_drain_seconds{state=paused}", double.NaN));
            Assert.Equal(1800.0, measured.GetValueOrDefault("flowtide_delta_maintenance_estimated_drain_seconds{state=suspended}", double.NaN));
        }

        // One observation of every gauge on the meter, keyed by name and state tag
        private static Dictionary<string, double> Measure(Meter meter)
        {
            var values = new Dictionary<string, double>();
            using var listener = new MeterListener();
            listener.InstrumentPublished = (instrument, owner) =>
            {
                if (ReferenceEquals(instrument.Meter, meter))
                {
                    owner.EnableMeasurementEvents(instrument);
                }
            };
            void Record(Instrument instrument, double value, ReadOnlySpan<KeyValuePair<string, object?>> tags)
            {
                var name = instrument.Name;
                foreach (var tag in tags)
                {
                    name += $"{{{tag.Key}={tag.Value}}}";
                }
                values[name] = value;
            }
            listener.SetMeasurementEventCallback<long>((instrument, value, tags, _) => Record(instrument, value, tags));
            listener.SetMeasurementEventCallback<double>((instrument, value, tags, _) => Record(instrument, value, tags));
            listener.SetMeasurementEventCallback<int>((instrument, value, tags, _) => Record(instrument, value, tags));
            listener.Start();
            listener.RecordObservableInstruments();
            return values;
        }
    }
}
