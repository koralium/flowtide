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

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance
{
    /// <summary>
    /// Why maintenance declined or failed; a finite set so warnings deduplicate in a 64-bit mask.
    /// </summary>
    internal enum MaintenanceReason : byte
    {
        None = 0,
        ManagedMemoryCap = 1,
        NativeMemoryCap = 2,
        DiskCap = 3,
        LineOverCap = 4,
        EntriesOverCap = 5,
        InvalidLogLine = 6,
        InvalidUtf8 = 7,
        RecordOverCap = 8,
        ValueOutOfRange = 9,
        RowGroupCap = 10,
        MetadataCap = 11,
        DecodeCap = 12,
        InvalidRetention = 13,
        UnknownProtocolVersion = 14,
        ExcludedFeature = 15,
        UnsupportedFeature = 16,
        CheckpointLayout = 17,
        DomainWithoutFeature = 18,
        DvWithoutFeature = 19,
        DvWithoutRecordCount = 20,
        ParsedOnlyRoot = 21,
        UnknownColumn = 22,
        OutOfEnvelope = 24,
        RootDecodeFailure = 25,
        SidecarRoot = 26,
        TargetNotInLog = 27,
        MissingHistory = 28,
        NoAdmissibleRoot = 29,
        CheckpointExists = 30,
        ValidationFailed = 31,
        UnknownCheckpoint = 32,
        PublishFailed = 33,
        // Worker outcomes: the job or its housekeeping, never the build's content
        TornRepairDeclined = 34,
        Transient = 35,
        Unexpected = 36,
        DispatchFailed = 37,
        ReleaseFailed = 38,
        InitFailed = 39,
        SlowBuild = 40,
        ScratchLocksIneffective = 41,
        LinkedScratchSkipped = 42,
        LegacyGapKept = 43
    }

    /// <summary>
    /// What a decline depends on, which decides whether and how it is memoized.
    /// </summary>
    internal enum DeclineDependency : byte
    {
        // Never memoized: output-dependent, transient or missing history
        None = 0,
        Root = 1,
        Tail = 2,
        Protocol = 3
    }

    /// <summary>
    /// A build that cannot produce a checkpoint within its limits; the checkpoint is optional, so this is never a stream failure.
    /// </summary>
    internal sealed class CheckpointDeclinedException : Exception
    {
        public CheckpointDeclinedException(MaintenanceReason reason, DeclineDependency dependency, string detail)
            : base($"{reason}: {detail}")
        {
            Reason = reason;
            Dependency = dependency;
        }

        public MaintenanceReason Reason { get; }

        public DeclineDependency Dependency { get; }
    }
}
