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

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.Advance
{
    /// <summary>
    /// Builds every advance demand estimate's inputs: at reservation, at recovery's restore and after each slice.
    /// </summary>
    internal static class AdvanceDemand
    {
        /// <summary>
        /// An advance at C with the given work left, priced at a checked listing and the evidence the job established.
        /// </summary>
        public static AdvanceDemandInputs Of(CleanupConfiguration configuration, LogEvidence log, CleanupEvidence evidence, long c, long logTargets, long payloadPaths, int parts, long partBytes,
            TimeSpan latency)
        {
            // C as listed; an in-memory listing carries no size
            var checkpointBytes = log.Listing.Checkpoints.Where(x => x.Version == c).Sum(x => x.IOEntry.Size ?? 0);
            return Inputs(configuration, evidence, log.Entries.Count, checkpointBytes, logTargets, payloadPaths, parts, partBytes, latency);
        }

        /// <summary>
        /// What an active advance has left after its latest slice, with C as its last gate listed it.
        /// </summary>
        public static AdvanceDemandInputs Of(CleanupConfiguration configuration, ActiveAdvance active, TimeSpan latency)
        {
            var totals = active.Started.Totals;
            return Inputs(configuration, active.Evidence, active.LogEntries, Math.Max(0, active.Destination.ListedSize), active.Phase == AdvancePhase.Log ? active.LogTargetsLeft : 0,
                Math.Max(0, totals.Emitted - active.Processed), Math.Max(0, totals.Parts - active.Cursor.Part), active.PartBytesLeft, latency);
        }

        // The anchor's bytes from the evidence's own read, never a record's
        private static AdvanceDemandInputs Inputs(CleanupConfiguration configuration, CleanupEvidence evidence, long logEntries, long checkpointBytes, long logTargets, long payloadPaths, int parts,
            long partBytes, TimeSpan latency)
        {
            var store = configuration.Store.Store;
            var sources = evidence.Capability.Sources.ToList();
            var keyExtra = Math.Max(0, DeletionExecutor.KeyPrefixLength(configuration) - (configuration.Store.Table.Full.Length + 1));
            return new AdvanceDemandInputs(logTargets, payloadPaths, parts, partBytes, checkpointBytes, evidence.Anchor.Proof.Length, sources.Sum(x => x.Length), sources.Count, logEntries,
                store.Paging, DeletionRun.OutstandingLimit(store), latency, UnknownStore: store.Kind == StoreKind.Unknown, KeyExtra: keyExtra);
        }
    }
}
