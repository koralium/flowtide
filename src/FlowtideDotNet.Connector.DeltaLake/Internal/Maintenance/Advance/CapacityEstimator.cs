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
    /// What an advance's demand is estimated from: its counts and encoded bytes, its gates' rehashed objects and the store's paging and latency.
    /// </summary>
    /// <param name="PartBytes">Whole-object bytes of every part.</param>
    /// <param name="LogEntries">Entries of the log listing every gate makes.</param>
    /// <param name="Outstanding">Payload deletes in flight at once.</param>
    /// <param name="OrphanCandidates">Azure orphan candidates, each probed for a positive size.</param>
    /// <param name="KeyExtra">Characters a payload target's key holds beyond the path its part names.</param>
    internal readonly record struct AdvanceDemandInputs(long LogTargets, long PayloadPaths, int Parts, long PartBytes, long CheckpointBytes, long AnchorBytes, long CapabilityBytes, int CapabilityObjects,
        long LogEntries, ListingPaging Paging, int Outstanding, TimeSpan Latency, long OrphanCandidates = 0, bool UnknownStore = false, long KeyExtra = 0);

    /// <summary>
    /// An advance's request and byte demand; no root listing enters it.
    /// </summary>
    internal static class CapacityEstimator
    {
        public static readonly TimeSpan DefaultLatency = TimeSpan.FromMilliseconds(50);

        // Header, started, logs-complete and done
        private const int FixedRecords = 4;

        // A header descriptor: ordinal, attempt id, paths, bytes and SHA-256
        private const int DescriptorBytes = 4 + ControlTokens.Length + 4 + 4 + 32;

        // A buffered target of the longest path a part holds
        private const long CostliestTarget = DeletionRun.TargetOverheadBytes + 2L * ControlRecords.MaxPathBytes;

        // A slice's issuance time, which its deletes' latency spends
        private static readonly long s_issuanceTicks = new DeletionRunSettings().IssuanceTime.Ticks;

        public static CapacityDemand Advance(AdvanceDemandInputs inputs, long requestsPerSecond)
        {
            // Each part with paths also ends its own last slice
            Int128 payloadSlices = Slices(inputs.PayloadPaths, inputs.Outstanding, inputs.Latency) + FullBuffers(inputs) + Math.Min(inputs.Parts, inputs.PayloadPaths);
            Int128 logGates = Slices(inputs.LogTargets, DeletionRun.LogOutstanding, inputs.Latency);
            var gates = logGates + payloadSlices;
            // A log listing with its further pages, the successor probe, and C, the anchor and the capability sources rehashed
            var gateRequests = 1 + MaintenanceBuckets.ExtraPages(inputs.Paging, inputs.LogEntries) + 1 + 2 + inputs.CapabilityObjects;
            var gateBytes = (Int128)inputs.CheckpointBytes + inputs.AnchorBytes + inputs.CapabilityBytes;
            // Each part is written and read back, then read whole by every payload slice from it
            Int128 parts = inputs.Parts;
            var partRequests = 2 * parts + payloadSlices;
            var partBytes = 2 * (Int128)inputs.PartBytes + (parts == 0 ? 0 : payloadSlices * ((inputs.PartBytes + parts - 1) / parts));
            // The header, read again as the payload enters each part
            var headerBytes = parts * (ControlRecords.SmallCap + parts * DescriptorBytes);
            // Each record is written and read back; a progress record follows every payload slice, which then deletes its predecessor
            var records = FixedRecords + payloadSlices;
            var recordRequests = 2 * records + payloadSlices;
            var recordBytes = 2 * (records * ControlRecords.SmallCap + parts * DescriptorBytes);
            var logRequests = Targets(inputs.LogTargets, 1, DeletionRun.LogOutstanding, inputs.Latency, requestsPerSecond);
            var payloadRequests = Targets(inputs.PayloadPaths, inputs.UnknownStore ? 2 : 1, inputs.Outstanding, inputs.Latency, requestsPerSecond);
            // Every log gate also reads the hint
            return new CapacityDemand(Saturated(gates * gateRequests + logGates + partRequests + parts + recordRequests + logRequests + payloadRequests + inputs.OrphanCandidates),
                Saturated(gates * gateBytes + partBytes + headerBytes + recordBytes));
        }

        // Clamped to a long instead of wrapping
        private static long Saturated(Int128 value) => value > long.MaxValue ? long.MaxValue : (long)value;

        // One gate per slice of at most a slice's paths, and of no more deletes than its outstanding ones issue within its issuance time
        private static Int128 Slices(long targets, int outstanding, TimeSpan latency)
        {
            var perSlice = latency <= TimeSpan.Zero ? DeletionRunSettings.DefaultMaxPaths
                : Math.Clamp(s_issuanceTicks * Math.Max(1, outstanding) / latency.Ticks, 1, DeletionRunSettings.DefaultMaxPaths);
            return ((Int128)targets + perSlice - 1) / perSlice;
        }

        // Payload slices a full buffer ends, each holding more than the buffer less the costliest target's charge
        private static Int128 FullBuffers(AdvanceDemandInputs inputs)
        {
            if (inputs.PayloadPaths <= 0)
            {
                return 0;
            }
            // Each target's overhead and key characters, at most its path's UTF-8 bytes and the key's extra characters
            var charged = DeletionRun.TargetOverheadBytes * (Int128)inputs.PayloadPaths
                + 2 * Int128.Max(0, inputs.PartBytes - inputs.Parts * (Int128)ControlRecords.PartLength(0, 0) - 4 * (Int128)inputs.PayloadPaths)
                + 2 * (Int128)inputs.KeyExtra * inputs.PayloadPaths;
            return charged / Int128.Max(1, DeletionRunSettings.DefaultMaxBufferBytes - CostliestTarget - 2 * (Int128)inputs.KeyExtra + 1);
        }

        // A delete waiting on latency counts as the requests the bucket could have granted meanwhile; in whole ticks, rounded up once
        private static Int128 Targets(long count, int requests, int outstanding, TimeSpan latency, long requestsPerSecond)
        {
            var perSecond = (Int128)TimeSpan.TicksPerSecond * Math.Max(1, outstanding);
            var waited = ((Int128)count * latency.Ticks * requestsPerSecond + perSecond - 1) / perSecond;
            return Int128.Max((Int128)count * requests, waited);
        }
    }
}
