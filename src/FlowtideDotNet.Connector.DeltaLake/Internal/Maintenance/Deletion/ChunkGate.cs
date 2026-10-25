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

using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.Advance;
using Stowage;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.Deletion
{
    /// <summary>
    /// What one delete slice passes before it issues anything: a fresh checked listing through the current storage, then the run's evidence judged against it.
    /// </summary>
    internal sealed class ChunkGate
    {
        private readonly Func<CleanupContext, Task<GateResult>> _pass;

        private ChunkGate(bool durable, Func<CleanupContext, Task<GateResult>> pass)
        {
            Durable = durable;
            _pass = pass;
        }

        // A durable run suspends where a volatile run stops and drops
        public bool Durable { get; }

        /// <summary>
        /// Revalidates absence evidence: ReadLogAsync above the admission's published version, then ValidateAsync with that listing.
        /// </summary>
        public static ChunkGate ForEvidence(CleanupConfiguration configuration, IFileStorage storage, CleanupEvidence evidence, long? relyFrom = null)
        {
            return new ChunkGate(evidence.Durable, async context =>
            {
                var (check, log) = await ListingEvidence.ReadLogAsync(configuration, storage, context.Head, context.Stop, context.Memory);
                if (check != ListingCheck.Passed)
                {
                    return GateResult.OfListing(check, evidence.Durable);
                }
                return await evidence.ValidateAsync(configuration, storage, log!, context, relyFrom);
            });
        }

        /// <summary>
        /// Revalidates the volatile evidences a slice relies on: one ReadLogAsync, then ValidateAsync of each with that listing.
        /// </summary>
        public static ChunkGate ForEvidences(CleanupConfiguration configuration, IFileStorage storage, Func<IReadOnlyCollection<CleanupEvidence>> relied)
        {
            return new ChunkGate(false, async context =>
            {
                var evidences = relied();
                // A durable evidence suspends where this gate stops
                if (evidences.Any(x => x.Durable))
                {
                    return new GateResult(GateVerdict.Unavailable, "A durable evidence needs a gate of its own");
                }
                var (check, log) = await ListingEvidence.ReadLogAsync(configuration, storage, context.Head, context.Stop, context.Memory);
                if (check != ListingCheck.Passed)
                {
                    return GateResult.OfListing(check, false);
                }
                var reads = new GateReads();
                GateResult? passed = null;
                GateResult? stopped = null;
                foreach (var evidence in evidences)
                {
                    var result = await evidence.ValidateAsync(configuration, storage, log!, context, reads: reads);
                    if (result.Passed)
                    {
                        passed ??= result;
                    }
                    else if (result.Action == GateAction.Stop)
                    {
                        // That evidence is stopped for good; the others are still judged
                        stopped ??= result;
                    }
                    else
                    {
                        return result;
                    }
                }
                return stopped ?? passed ?? new GateResult(GateVerdict.Unavailable, "The slice relies on no evidence");
            });
        }

        /// <summary>
        /// Log-side removal that relies on absence: ReadLogAsync above the admission's published version, then the caller's decision from that checked listing.
        /// </summary>
        public static ChunkGate ForLog(CleanupConfiguration configuration, IFileStorage storage, Func<LogEvidence, CleanupContext, Task<GateResult>> decide)
        {
            return new ChunkGate(false, async context =>
            {
                var (check, log) = await ListingEvidence.ReadLogAsync(configuration, storage, context.Head, context.Stop, context.Memory);
                if (check != ListingCheck.Passed)
                {
                    return GateResult.OfListing(check, false);
                }
                return await decide(log!, context);
            });
        }

        /// <summary>
        /// A started advance's slices: ReadLogAsync above the admission's published version, its evidence relied on from C, C rehashed whole to its header's content, its authority by name, then the caller's decision; every result durable, and spent reads promote.
        /// </summary>
        public static ChunkGate ForAdvance(CleanupConfiguration configuration, IFileStorage storage, ActiveAdvance active, Func<LogEvidence, CleanupContext, Task<GateResult>> decide)
        {
            return new ChunkGate(true, async context =>
            {
                var result = await PassAdvanceAsync(configuration, storage, active, decide, context);
                // A fresh short admission spends the same reads
                return result.Verdict == GateVerdict.ReadsSpent ? result with { Verdict = GateVerdict.NeedsLongAdmission, Durable = true } : result with { Durable = true };
            });
        }

        private static async Task<GateResult> PassAdvanceAsync(CleanupConfiguration configuration, IFileStorage storage, ActiveAdvance active, Func<LogEvidence, CleanupContext, Task<GateResult>> decide,
            CleanupContext context)
        {
            var (check, log) = await ListingEvidence.ReadLogAsync(configuration, storage, context.Head, context.Stop, context.Memory);
            if (check != ListingCheck.Passed)
            {
                return GateResult.OfListing(check, true);
            }
            // Intentional gaps below C pass
            var validated = await active.Evidence.ValidateAsync(configuration, storage, log!, context, active.C);
            // A gap after the planned head is a foreign change
            if (validated.Verdict == GateVerdict.ListingFailed && log!.CheckRange(active.Started.Head + 1, log.Head) != ListingCheck.Passed)
            {
                return new GateResult(GateVerdict.ForeignHead, $"Commits after the planned head {active.Started.Head} up to {log.Head} are not all listed") { Head = log.Head };
            }
            if (!validated.Passed)
            {
                return validated;
            }
            var checkpoint = await CheckpointAsync(storage, log!, active, context);
            if (checkpoint != null)
            {
                return checkpoint;
            }
            return Authority(log!, active) ?? await decide(log!, context);
        }

        // C listed at the size and modification time its header names, then rehashed whole to its content; null when it is
        private static async Task<GateResult?> CheckpointAsync(IFileStorage storage, LogEvidence log, ActiveAdvance active, CleanupContext context)
        {
            var destination = active.Destination;
            var file = ContentProof.Find(log.Listing, active.C, true);
            if (file == null || (file.IOEntry.Size ?? -1) != destination.ListedSize || (file.IOEntry.LastModificationTime?.UtcTicks ?? -1) != destination.ListedModifiedTicks)
            {
                return new GateResult(GateVerdict.CheckpointReplaced, $"Checkpoint {active.C} is not listed as its header names it") { Head = log.Head };
            }
            var read = await ProofReader.ReadAsync(storage, file.IOEntry.Path, active.C, file.IOEntry.Size, false, context);
            return read.Status switch
            {
                ProofStatus.Read when read.Length == destination.Length && read.Sha256.AsSpan().SequenceEqual(destination.Sha256) => null,
                ProofStatus.Preempted => new GateResult(GateVerdict.Preempted, $"Rehashing checkpoint {active.C} stopped") { Stop = read.Stop, Head = log.Head },
                ProofStatus.ReadsSpent => new GateResult(GateVerdict.ReadsSpent, $"Rehashing checkpoint {active.C} needs a read this admission has spent") { Head = log.Head },
                _ => new GateResult(GateVerdict.CheckpointReplaced, $"Checkpoint {active.C} is gone or has other content than its header names") { Head = log.Head }
            };
        }

        // The group's started record, and its logs-complete past the log phase, listed under its exact association; no other advance's reservation
        private static GateResult? Authority(LogEvidence log, ActiveAdvance active)
        {
            var association = active.Association;
            var digest = ControlName.D16(association.HeaderSha256);
            var started = false;
            var logsComplete = false;
            foreach (var entry in log.Listing.Controls)
            {
                if (entry.Path.IsFolder || !ControlName.TryParse(entry.Name, out var name) || name.Kind is not (ControlKind.Started or ControlKind.LogsComplete or ControlKind.Progress))
                {
                    continue;
                }
                if (name.Boundary != association.C || !string.Equals(name.AdvanceId, association.AdvanceId, StringComparison.Ordinal) || !string.Equals(name.Digest, digest, StringComparison.Ordinal))
                {
                    return new GateResult(GateVerdict.AuthorityChanged, $"Another advance's record reserves version {name.Boundary}") { Head = log.Head };
                }
                started |= name.Kind == ControlKind.Started;
                logsComplete |= name.Kind == ControlKind.LogsComplete;
            }
            if (!started || (active.Phase != AdvancePhase.Log && !logsComplete))
            {
                return new GateResult(GateVerdict.AuthorityChanged, $"The advance to checkpoint {active.C} has no {(started ? "logs-complete" : "started")} record listed") { Head = log.Head };
            }
            return null;
        }

        /// <summary>
        /// Positively proven stage-file removal: ReadStagesAsync above the admission's published version, then the caller's decision from that stage evidence.
        /// </summary>
        public static ChunkGate ForStages(CleanupConfiguration configuration, IFileStorage storage, Func<StageEvidence, CleanupContext, Task<GateResult>> decide)
        {
            return new ChunkGate(false, async context =>
            {
                var (check, stages) = await ListingEvidence.ReadStagesAsync(configuration, storage, context.Head, context.Stop, context.Memory);
                if (check != ListingCheck.Passed)
                {
                    return GateResult.OfListing(check, false);
                }
                return await decide(stages!, context);
            });
        }

        /// <summary>
        /// Passes the gate with a listing of its own; never throws for runtime failures, and a stop found first preempts the listing.
        /// </summary>
        public async Task<GateResult> PassAsync(CleanupContext context)
        {
            var stop = context.CheckStop();
            if (stop != IssuanceStop.None)
            {
                return new GateResult(GateVerdict.Preempted, $"Issuance stopped: {stop}") { Stop = stop, Durable = Durable };
            }
            try
            {
                return await _pass(context);
            }
            catch (OperationCanceledException) when (context.Stop.IsCancellationRequested)
            {
                return new GateResult(GateVerdict.Preempted, "The worker stopped") { Stop = IssuanceStop.Stopped, Durable = Durable };
            }
            catch (Exception e)
            {
                // Listing errors and ledger limits are transient: the run keeps everything and tries again
                return new GateResult(GateVerdict.Unavailable, $"{e.GetType().Name}: {e.Message}") { Durable = Durable };
            }
        }
    }
}
