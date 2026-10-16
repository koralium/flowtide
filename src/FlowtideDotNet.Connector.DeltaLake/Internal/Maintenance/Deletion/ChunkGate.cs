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
