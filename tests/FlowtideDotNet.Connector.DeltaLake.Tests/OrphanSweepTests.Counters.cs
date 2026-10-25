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
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.Deletion;
using System.Diagnostics.Metrics;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public partial class OrphanSweepTests
    {
        // Each measurement's only tag value under the key, and its count, in measurement order
        private static List<(string Tag, long Value)> Tagged(IEnumerable<Measurement<long>> measurements, string key) => measurements.Select(x => (TagOf(x, key), x.Value)).ToList();

        private static string TagOf(Measurement<long> measurement, string key)
        {
            var tag = Assert.Single(measurement.Tags.ToArray());
            Assert.Equal(key, tag.Key);
            return Assert.IsType<string>(tag.Value);
        }

        [Fact]
        public void BarrierCountsOnlyTimeoutAndStuckProducer()
        {
            var counters = new RetentionCounters();
            foreach (var outcome in Enum.GetValues<BarrierOutcome>())
            {
                counters.AddBarrier(outcome);
            }
            counters.AddBarrier(BarrierOutcome.Timeout);

            Assert.Equal(2, counters.Barrier(BarrierOutcome.Timeout));
            Assert.Equal(1, counters.Barrier(BarrierOutcome.StuckProducer));
            Assert.All(new[] { BarrierOutcome.Drained, BarrierOutcome.Busy, BarrierOutcome.Exhausted, BarrierOutcome.Cancelled }, x => Assert.Equal(0, counters.Barrier(x)));
            Assert.Equal(new[] { ("timeout", 2L), ("stuck_producer", 1L) }, Tagged(counters.BarrierMeasurements(), "outcome"));
        }

        [Fact]
        public void CandidateProbesCountApartFromSuccessorProbes()
        {
            var counters = new RetentionCounters();
            counters.AddCandidateProbe(ProbeResult.Absent);
            counters.AddCandidateProbe(ProbeResult.Present);
            counters.AddCandidateProbe(ProbeResult.Present);
            counters.AddCandidateProbe(ProbeResult.Error);
            counters.AddCandidateProbe(ProbeResult.Error);
            counters.AddCandidateProbe(ProbeResult.Error);

            Assert.Equal(2, counters.CandidateProbes(ProbeResult.Present));
            Assert.Equal(new[] { ("absent", 1L), ("present", 2L), ("error", 3L) }, Tagged(counters.CandidateProbeMeasurements(), "result"));
            Assert.All(Enum.GetValues<ProbeResult>(), x => Assert.Equal(0, counters.Probes(x)));
        }

        [Fact]
        public void OrphanKeptCountsTheSubmissionReasons()
        {
            var counters = new RetentionCounters();
            foreach (var reason in Enum.GetValues<KeepReason>())
            {
                counters.AddOrphanKept(reason);
            }
            counters.AddOrphanKept(KeepReason.IdentityChanged);
            counters.AddOrphanKept(KeepReason.ProbeError);
            counters.AddOrphanKept(KeepReason.ProbeError);
            counters.AddOrphanKept(KeepReason.Directory);
            counters.AddOrphanKept(KeepReason.Directory);
            counters.AddOrphanKept(KeepReason.Directory);

            Assert.Equal(3, counters.OrphanKept(KeepReason.ProbeError));
            Assert.Equal(0, counters.OrphanKept(KeepReason.Alias));
            Assert.Equal(new[] { ("unqualified", 1L), ("unknown_size", 1L), ("identity_changed", 2L), ("probe_error", 3L), ("directory", 4L) },
                Tagged(counters.OrphanKeptMeasurements(), "reason"));
        }

        [Fact]
        public void TallyCountsEveryKeepReason()
        {
            var tally = new DeletionTally();
            foreach (var reason in Enum.GetValues<KeepReason>())
            {
                tally.Add(DeleteOutcome.Keep(reason));
            }

            Assert.Equal(Enum.GetValues<KeepReason>().Length, tally.Kept);
            Assert.Equal(1, tally.KeptFor(KeepReason.IdentityChanged));
            Assert.Equal(1, tally.KeptFor(KeepReason.ProbeError));
        }
    }
}
