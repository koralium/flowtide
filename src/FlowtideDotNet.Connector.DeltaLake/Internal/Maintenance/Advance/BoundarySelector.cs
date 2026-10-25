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
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Checkpoints;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Utils;
using Stowage;
using System.Collections.Frozen;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.Advance
{
    /// <summary>
    /// Chooses the boundary checkpoint C among the eligible checkpoints E, or the cutoff build an empty E asks for.
    /// </summary>
    internal static class BoundarySelector
    {
        /// <summary>
        /// The largest eligible checkpoint at or below max(min E, O + the effective short cap), less the reads selecting it spends; with E empty, ends the job with the cutoff demand or the young-root wait.
        /// </summary>
        public static async Task<AdvanceBoundary> SelectAsync(CleanupConfiguration configuration, IFileStorage storage, PreparedAdvance prepared, LogClassification classes, TimeSpan retention,
            long shortReads, DateTimeOffset now, CleanupContext context)
        {
            var log = prepared.Log;
            var head = log.Head;
            var midnight = Midnight(now, retention);
            var timestamps = new CommitTimestamps(storage, log, prepared.A, now.ToUnixTimeMilliseconds(), context);
            // C > O, C >= A and C <= V
            var lo = Math.Max(prepared.A, prepared.O + 1);
            var hi = Math.Min(prepared.V, head);
            if (lo > hi)
            {
                throw AdvanceJob.NoWork(MaintenanceReason.NoEligibleCheckpoint, $"No version from {lo} to {hi} can be the boundary");
            }
            if (classes.LowestUnknownVersioned < lo)
            {
                throw UnknownBelow(classes, lo);
            }
            var transport = new StowageCheckpointTransport(storage, configuration.Store.Table);
            // Oldest first
            var listed = log.Listing.Checkpoints.Where(x => x.Version >= lo && x.Version <= hi).OrderBy(x => x.Version).ToList();
            var last = await timestamps.LastAtOrBeforeAsync(listed.Count, i => listed[(int)i].Version, midnight);

            var first = -1;
            for (int i = 0; i <= last; i++)
            {
                if ((await AdvanceJob.ProbeAsync(transport, listed[i], context)).MarkedGapFree)
                {
                    first = i;
                    break;
                }
            }
            if (first < 0)
            {
                throw await CutoffAsync(configuration.Store.Table, storage, prepared, classes, transport, timestamps, lo, hi, midnight, context);
            }
            var lowest = listed[first].Version;
            await timestamps.EligibleAsync(lowest, midnight);

            var tombstones = prepared.O > 0 && !prepared.OldestRootMarked;
            var unsupported = Unsupported(configuration.Store.Table, log, context);
            // The short cap of a step to the version once the reads so far and the given ones are spent
            long CapAfter(long version, long spend) => AdvanceBudget.EffectiveShortCap(Math.Min(context.ReadsRemaining, shortReads - context.ReadsUsed) - spend,
                AdvancePlanner.Ancillary(log.Listing, version, head, context.CheckpointInterval, classes.Stages.Count, tombstones, unsupported));
            var cap = CapAfter(lowest, 0);
            var step = lowest - prepared.O > cap ? AdvanceStep.Long : AdvanceStep.Short;
            var chosen = first;
            // A long step goes to min E
            for (var i = (int)last; i > first && step == AdvanceStep.Short; i--)
            {
                var version = listed[i].Version;
                // Its timestamp unless already read, then its probe
                var spend = (timestamps.Known(version) ? 0 : 1) + 1;
                // Read only while a step to it and to min E both still fit afterwards
                if (version - prepared.O > CapAfter(version, spend) || lowest - prepared.O > CapAfter(lowest, spend))
                {
                    continue;
                }
                await timestamps.EligibleAsync(version, midnight);
                if ((await AdvanceJob.ProbeAsync(transport, listed[i], context)).MarkedGapFree)
                {
                    chosen = i;
                    break;
                }
            }
            var checkpoint = listed[chosen];
            // The cap left for the chosen step once its selection reads are spent
            return new AdvanceBoundary(prepared.TableId, prepared.O, prepared.OldestRoot, prepared.OldestRootMarked, checkpoint.Version, checkpoint, prepared.A, prepared.V, head,
                CutoffOf(midnight), log, step, CapAfter(checkpoint.Version, 0), retention, context.CheckpointInterval)
            {
                Unsupported = unsupported,
                Anchor = prepared.Anchor,
                FirstJson = prepared.FirstJson
            };
        }

        // The listed checkpoints the worker's builds skip, by version
        private static IReadOnlySet<long> Unsupported(IOPath table, LogEvidence log, CleanupContext context)
        {
            if (context.KnownUnsupported.Count == 0)
            {
                return FrozenSet<long>.Empty;
            }
            return log.Listing.Checkpoints.Where(x => context.KnownUnsupported.Contains(ObjectIdentity.Of(table, x))).Select(x => x.Version).ToHashSet();
        }

        /// <summary>
        /// Midnight UTC of now minus the retention, in ticks since the Unix epoch, saturating instead of wrapping.
        /// </summary>
        internal static Int128 Midnight(DateTimeOffset now, TimeSpan retention)
        {
            var cutoff = CheckpointExpiry.CutoffTicks(now, retention);
            var days = cutoff / TimeSpan.TicksPerDay;
            if (days * TimeSpan.TicksPerDay > cutoff)
            {
                days--;
            }
            return days * TimeSpan.TicksPerDay;
        }

        // An unknown versioned name below the version declines, as it would below C
        private static AdvanceEndedException UnknownBelow(LogClassification classes, long version) =>
            AdvanceJob.Declined(MaintenanceReason.UnclassifiedLogFile, $"The log lists an unknown name at version {classes.LowestUnknownVersioned}, below version {version}");

        // A commit at or before the cutoff
        internal static bool Eligible(long timestampMs, Int128 midnight) => (Int128)timestampMs * TimeSpan.TicksPerMillisecond <= midnight;

        // The cutoff as a time, clamped to the representable range
        private static DateTimeOffset CutoffOf(Int128 midnight)
        {
            var ticks = midnight + DateTimeOffset.UnixEpoch.UtcTicks;
            var clamped = ticks < DateTimeOffset.MinValue.UtcTicks ? DateTimeOffset.MinValue.UtcTicks : ticks > DateTimeOffset.MaxValue.UtcTicks ? DateTimeOffset.MaxValue.UtcTicks : (long)ticks;
            return new DateTimeOffset(clamped, TimeSpan.Zero);
        }

        // With E empty: exact Auto at the newest uncheckpointed version at or before the cutoff, when that build would be gap-free
        private static async Task<AdvanceEndedException> CutoffAsync(IOPath table, IFileStorage storage, PreparedAdvance prepared, LogClassification classes, ICheckpointTransport transport,
            CommitTimestamps timestamps, long lo, long hi, Int128 midnight, CleanupContext context)
        {
            var index = await timestamps.LastAtOrBeforeAsync(hi - lo + 1, i => lo + i, midnight);
            if (index < 0)
            {
                return AdvanceJob.NoWork(MaintenanceReason.NoEligibleCheckpoint, $"No version from {lo} to {hi} is at or before the cutoff");
            }
            var checkpointed = classes.Names.Where(x => x.Kind is LogNameKind.ClassicCheckpoint or LogNameKind.MultipartCheckpoint or LogNameKind.UuidCheckpoint).Select(x => x.Version).ToHashSet();
            var target = lo + index;
            while (target >= lo && checkpointed.Contains(target))
            {
                target--;
            }
            if (target < lo)
            {
                return AdvanceJob.NoWork(MaintenanceReason.NoAdmissibleRoot, $"Every version from {lo} to {lo + index} has a checkpoint and none is eligible");
            }
            // An unknown name below the cutoff build's target
            if (classes.LowestUnknownVersioned < target)
            {
                return UnknownBelow(classes, target);
            }
            await timestamps.EligibleAsync(target, midnight);
            var segment = new DeltaLogSegment(storage, table, prepared.Log.Listing);
            // The root the worker's Auto build would take, each candidate fetched a counted read
            var prediction = await CheckpointProbe.PredictAutoRootAsync(transport, context.Scratch, context.Memory, context.Transfer, segment.RootCandidates(target, target - 1),
                context.KnownUnsupported, null, () => AdvanceJob.CountRead(context), context.Stop);
            if (prediction.Unknown != null)
            {
                return AdvanceJob.Declined(MaintenanceReason.Transient, prediction.Unknown);
            }
            var history = segment.HasHistoryFromZero(target);
            var rebuild = context.RebuildLegacyCheckpoints;
            // Without a root it uses, the build replays every commit from zero, or declines when a root was skipped
            if (!(prediction.Used(rebuild, history) is { } used ? used.MarkedGapFree : history && (!prediction.Blocked || rebuild)))
            {
                return AdvanceJob.NoWork(MaintenanceReason.NoAdmissibleRoot, $"No checkpoint is eligible and a build at {target} would not be gap-free; the advance waits for a gap-free root to pass the cutoff");
            }
            return AdvanceJob.NoWork(MaintenanceReason.NoEligibleCheckpoint, $"No checkpoint is eligible; a build at {target} is wanted", new BuildDemand(target, RootPolicy.Auto));
        }

        /// <summary>
        /// A listed commit's own timestamp in the epoch from its strict first line, one counted read; another epoch or a timestamp ahead of the clock ends the job.
        /// </summary>
        internal static async Task<long> ReadTimestampAsync(IFileStorage storage, LogEvidence log, long version, long adoptedAt, long nowMs, CleanupContext context)
        {
            if (!log.Listing.Commits.TryGetValue(version, out var commit))
            {
                throw AdvanceJob.Declined(MaintenanceReason.MissingHistory, $"Commit {version} is not listed");
            }
            AdvanceJob.CountRead(context);
            var header = await CommitHeaderReader.ReadAsync(storage, commit.IOEntry.Path, HeaderMode.Strict, null, context.Stop);
            if (header == null)
            {
                throw AdvanceJob.Declined(MaintenanceReason.Transient, $"Commit {version} was removed after the listing");
            }
            var provenance = CommitProvenance.FromStrict(version, header);
            if (!provenance.IsOwn || provenance.AdoptedAt != adoptedAt || provenance.Timestamp is not { } timestamp)
            {
                throw AdvanceJob.Declined(MaintenanceReason.InvalidProvenance, $"Commit {version} does not prove an own timestamp of epoch {adoptedAt}");
            }
            if (timestamp > nowMs)
            {
                throw AdvanceJob.Declined(MaintenanceReason.TimestampAhead, $"Commit {version} has timestamp {timestamp}, ahead of the clock's {nowMs}");
            }
            return timestamp;
        }

        /// <summary>
        /// Own commit timestamps from strict first lines, each one counted read; rejects other epochs, timestamps ahead of the clock and any that do not rise with the version.
        /// </summary>
        private sealed class CommitTimestamps
        {
            private readonly Dictionary<long, long> _read = new Dictionary<long, long>();
            private readonly IFileStorage _storage;
            private readonly LogEvidence _log;
            private readonly long _adoptedAt;
            private readonly long _nowMs;
            private readonly CleanupContext _context;

            public CommitTimestamps(IFileStorage storage, LogEvidence log, long adoptedAt, long nowMs, CleanupContext context)
            {
                _storage = storage;
                _log = log;
                _adoptedAt = adoptedAt;
                _nowMs = nowMs;
                _context = context;
            }

            // Already read; a further read counts nothing
            public bool Known(long version) => _read.ContainsKey(version);

            public async Task<long> ReadAsync(long version)
            {
                if (_read.TryGetValue(version, out var known))
                {
                    return known;
                }
                var timestamp = await ReadTimestampAsync(_storage, _log, version, _adoptedAt, _nowMs, _context);
                foreach (var (other, value) in _read)
                {
                    if ((other < version && value >= timestamp) || (other > version && value <= timestamp))
                    {
                        throw AdvanceJob.Declined(MaintenanceReason.InvalidProvenance, $"Commits {Math.Min(other, version)} and {Math.Max(other, version)} have timestamps that do not rise with the version");
                    }
                }
                _read[version] = timestamp;
                return timestamp;
            }

            // A version the selection relies on, read strictly and at or before the cutoff
            public async Task EligibleAsync(long version, Int128 midnight)
            {
                if (!Eligible(await ReadAsync(version), midnight))
                {
                    throw AdvanceJob.Declined(MaintenanceReason.InvalidProvenance, $"Commit {version} is after the cutoff although a later commit is not");
                }
            }

            /// <summary>
            /// The largest index whose commit is at or before the cutoff, -1 for none, by binary search over rising timestamps.
            /// </summary>
            public async Task<long> LastAtOrBeforeAsync(long count, Func<long, long> versionAt, Int128 midnight)
            {
                long low = 0;
                var high = count - 1;
                long found = -1;
                while (low <= high)
                {
                    var middle = low + (high - low) / 2;
                    if (Eligible(await ReadAsync(versionAt(middle)), midnight))
                    {
                        found = middle;
                        low = middle + 1;
                    }
                    else
                    {
                        high = middle - 1;
                    }
                }
                return found;
            }
        }
    }
}
