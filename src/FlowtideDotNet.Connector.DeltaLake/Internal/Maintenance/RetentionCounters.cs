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

using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.Deletion;
using System.Diagnostics.Metrics;
using System.Text;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance
{
    /// <summary>
    /// A table's retention state, reported by its code.
    /// </summary>
    internal enum RetentionState : byte
    {
        Off = 0,
        Idle = 1,
        // Coverage is not confirmed or its repair is pending
        Coverage = 2,
        Runnable = 3,
        Paused = 4,
        Rebasing = 5,
        Suspended = 6,
        RepairBlocked = 7
    }

    /// <summary>
    /// What a successor probe found.
    /// </summary>
    internal enum ProbeResult : byte
    {
        Absent = 0,
        Present = 1,
        // The probe failed, or the head has no representable successor
        Error = 2
    }

    /// <summary>
    /// The state gauge's value: off, then the started advance's state, then the coverage state.
    /// </summary>
    internal static class RetentionStates
    {
        public static RetentionState Of(RetentionState coverage, RetentionState advance) => coverage == RetentionState.Off ? RetentionState.Off : advance != RetentionState.Idle ? advance : coverage;
    }

    /// <summary>
    /// Per-sink counts of cleanup deletes by class and outcome, path rejections, listing declines, successor probes, orphan keeps, barriers and candidate probes, and the latest advance slice's timings; they outlive restarts.
    /// </summary>
    internal sealed class RetentionCounters
    {
        private const int Kinds = (int)TargetKind.Log + 1;
        private const int Outcomes = (int)DeleteStatus.Failed + 1;
        private const int Checks = (int)ListingCheck.ControlFenced + 1;
        private const int Results = (int)ProbeResult.Error + 1;

        // Tag values by enum value, made once
        private static readonly KeyValuePair<string, object?>[] s_kinds = Tags<TargetKind>("class", Kinds);
        private static readonly KeyValuePair<string, object?>[] s_outcomes = Tags<DeleteStatus>("outcome", Outcomes);
        private static readonly KeyValuePair<string, object?>[] s_checks = Tags<ListingCheck>("check", Checks);
        private static readonly KeyValuePair<string, object?>[] s_results = Tags<ProbeResult>("result", Results);

        // The keep reasons an orphan listing or run counts, in measurement order
        private static readonly KeepReason[] s_orphanReasons = [KeepReason.Unqualified, KeepReason.UnknownSize, KeepReason.IdentityChanged, KeepReason.ProbeError, KeepReason.Directory];
        private static readonly KeyValuePair<string, object?>[] s_orphanKept = Array.ConvertAll(s_orphanReasons, x => new KeyValuePair<string, object?>("reason", TagOf(x)));

        // The orphan barrier outcomes counted, in measurement order
        private static readonly BarrierOutcome[] s_barrierOutcomes = [BarrierOutcome.Timeout, BarrierOutcome.StuckProducer];
        private static readonly KeyValuePair<string, object?>[] s_barrier = Array.ConvertAll(s_barrierOutcomes, x => new KeyValuePair<string, object?>("outcome", TagOf(x)));

        private readonly long[] _deletes = new long[Kinds * Outcomes];
        private readonly long[] _declines = new long[Checks];
        private readonly long[] _probes = new long[Results];
        private readonly long[] _orphanKept = new long[s_orphanReasons.Length];
        private readonly long[] _barrier = new long[s_barrierOutcomes.Length];
        private readonly long[] _candidateProbes = new long[Results];
        private long _pathRejections;
        private long _lastGateTicks;
        private long _lastSliceTicks;
        private long _orphanListed;

        public long Deletes(TargetKind kind, DeleteStatus status) => Interlocked.Read(ref _deletes[(int)kind * Outcomes + (int)status]);

        public long ListingDeclines(ListingCheck check) => Interlocked.Read(ref _declines[(int)check]);

        public long Probes(ProbeResult result) => Interlocked.Read(ref _probes[(int)result]);

        public long PathRejections => Interlocked.Read(ref _pathRejections);

        // Entries the latest orphan root listing delivered
        public long OrphanListed => Interlocked.Read(ref _orphanListed);

        public long OrphanKept(KeepReason reason)
        {
            var index = Array.IndexOf(s_orphanReasons, reason);
            return index >= 0 ? Interlocked.Read(ref _orphanKept[index]) : 0;
        }

        public long Barrier(BarrierOutcome outcome)
        {
            var index = Array.IndexOf(s_barrierOutcomes, outcome);
            return index >= 0 ? Interlocked.Read(ref _barrier[index]) : 0;
        }

        // Pinned Azure orphan submission probes
        public long CandidateProbes(ProbeResult result) => Interlocked.Read(ref _candidateProbes[(int)result]);

        public double LastGateMs => TimeSpan.FromTicks(Interlocked.Read(ref _lastGateTicks)).TotalMilliseconds;

        // From the slice's first issuance until its deletes settled
        public double LastSliceMs => TimeSpan.FromTicks(Interlocked.Read(ref _lastSliceTicks)).TotalMilliseconds;

        /// <summary>
        /// One settled or withheld delete by its target's class and its outcome; never throws.
        /// </summary>
        public void Add(DeletionTarget target, DeleteOutcome outcome)
        {
            if ((uint)target.Kind < Kinds && (uint)outcome.Status < Outcomes)
            {
                Interlocked.Increment(ref _deletes[(int)target.Kind * Outcomes + (int)outcome.Status]);
            }
        }

        /// <summary>
        /// A tally whose targets are all of the one class.
        /// </summary>
        public void Add(TargetKind kind, DeletionTally tally)
        {
            var row = (int)kind * Outcomes;
            Interlocked.Add(ref _deletes[row + (int)DeleteStatus.Deleted], tally.Deleted);
            Interlocked.Add(ref _deletes[row + (int)DeleteStatus.Absent], tally.Absent);
            Interlocked.Add(ref _deletes[row + (int)DeleteStatus.Kept], tally.Kept);
            Interlocked.Add(ref _deletes[row + (int)DeleteStatus.Failed], tally.Failed);
        }

        public void AddPathRejection() => Interlocked.Increment(ref _pathRejections);

        public void AddDecline(ListingCheck check)
        {
            if (check != ListingCheck.Passed)
            {
                Interlocked.Increment(ref _declines[(int)check]);
            }
        }

        /// <summary>
        /// A checked listing's result: its failed check, and the successor probe that a passed, present, failed or impossible probe shows.
        /// </summary>
        public void AddListing(ListingCheck check)
        {
            ProbeResult? probe = check switch
            {
                ListingCheck.Passed => ProbeResult.Absent,
                ListingCheck.SuccessorPresent => ProbeResult.Present,
                ListingCheck.ProbeError or ListingCheck.Overflow => ProbeResult.Error,
                _ => null
            };
            if (probe is { } result)
            {
                Interlocked.Increment(ref _probes[(int)result]);
            }
            AddDecline(check);
        }

        public void AddSlice(TimeSpan gate, TimeSpan slice)
        {
            Interlocked.Exchange(ref _lastGateTicks, gate.Ticks);
            Interlocked.Exchange(ref _lastSliceTicks, slice.Ticks);
        }

        public void SetOrphanListed(long count) => Interlocked.Exchange(ref _orphanListed, count);

        /// <summary>
        /// One entry an orphan listing or run kept for a counted reason; other reasons are not counted.
        /// </summary>
        public void AddOrphanKept(KeepReason reason)
        {
            var index = Array.IndexOf(s_orphanReasons, reason);
            if (index >= 0)
            {
                Interlocked.Increment(ref _orphanKept[index]);
            }
        }

        /// <summary>
        /// One orphan barrier that timed out or found a stuck producer; other outcomes are not counted.
        /// </summary>
        public void AddBarrier(BarrierOutcome outcome)
        {
            var index = Array.IndexOf(s_barrierOutcomes, outcome);
            if (index >= 0)
            {
                Interlocked.Increment(ref _barrier[index]);
            }
        }

        /// <summary>
        /// One pinned Azure orphan submission probe's result.
        /// </summary>
        public void AddCandidateProbe(ProbeResult result)
        {
            if ((uint)result < Results)
            {
                Interlocked.Increment(ref _candidateProbes[(int)result]);
            }
        }

        public IEnumerable<Measurement<long>> DeleteMeasurements()
        {
            var measurements = new Measurement<long>[Kinds * Outcomes];
            for (int kind = 0; kind < Kinds; kind++)
            {
                for (int outcome = 0; outcome < Outcomes; outcome++)
                {
                    var index = kind * Outcomes + outcome;
                    measurements[index] = new Measurement<long>(Interlocked.Read(ref _deletes[index]), s_kinds[kind], s_outcomes[outcome]);
                }
            }
            return measurements;
        }

        /// <summary>
        /// Every check but Passed, from the drain's turn listing, each started advance's slice gate, the orphan sweep's root listing, log projection and second and third log listings, and the storage-changed declines; preparation, rebase, staged sweep and drain slice listings are not counted.
        /// </summary>
        public IEnumerable<Measurement<long>> DeclineMeasurements()
        {
            var measurements = new Measurement<long>[Checks - 1];
            for (int check = 1; check < Checks; check++)
            {
                measurements[check - 1] = new Measurement<long>(Interlocked.Read(ref _declines[check]), s_checks[check]);
            }
            return measurements;
        }

        /// <summary>
        /// Successor probes of the drain's turn listing, each started advance's slice gate and the orphan sweep's root, second and third log listings, absent once the listing or gate passed; an advance gate that declined after its listing passed counts none, and preparation, rebase, staged sweep and drain slice probes are not counted.
        /// </summary>
        public IEnumerable<Measurement<long>> ProbeMeasurements()
        {
            var measurements = new Measurement<long>[Results];
            for (int result = 0; result < Results; result++)
            {
                measurements[result] = new Measurement<long>(Interlocked.Read(ref _probes[result]), s_results[result]);
            }
            return measurements;
        }

        public IEnumerable<Measurement<long>> OrphanKeptMeasurements()
        {
            var measurements = new Measurement<long>[s_orphanReasons.Length];
            for (int index = 0; index < measurements.Length; index++)
            {
                measurements[index] = new Measurement<long>(Interlocked.Read(ref _orphanKept[index]), s_orphanKept[index]);
            }
            return measurements;
        }

        public IEnumerable<Measurement<long>> BarrierMeasurements()
        {
            var measurements = new Measurement<long>[s_barrierOutcomes.Length];
            for (int index = 0; index < measurements.Length; index++)
            {
                measurements[index] = new Measurement<long>(Interlocked.Read(ref _barrier[index]), s_barrier[index]);
            }
            return measurements;
        }

        public IEnumerable<Measurement<long>> CandidateProbeMeasurements()
        {
            var measurements = new Measurement<long>[Results];
            for (int result = 0; result < Results; result++)
            {
                measurements[result] = new Measurement<long>(Interlocked.Read(ref _candidateProbes[result]), s_results[result]);
            }
            return measurements;
        }

        /// <summary>
        /// An enum value's name in snake case, the only form a tag value takes.
        /// </summary>
        internal static string TagOf<T>(T value) where T : struct, Enum
        {
            var name = value.ToString();
            var tag = new StringBuilder(name.Length + 4);
            for (int i = 0; i < name.Length; i++)
            {
                if (i > 0 && char.IsUpper(name[i]))
                {
                    tag.Append('_');
                }
                tag.Append(char.ToLowerInvariant(name[i]));
            }
            return tag.ToString();
        }

        private static KeyValuePair<string, object?>[] Tags<T>(string key, int count) where T : struct, Enum
        {
            var tags = new KeyValuePair<string, object?>[count];
            for (int i = 0; i < count; i++)
            {
                tags[i] = new KeyValuePair<string, object?>(key, TagOf((T)Enum.ToObject(typeof(T), i)));
            }
            return tags;
        }
    }
}
