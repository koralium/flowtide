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


namespace FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.Compaction
{
    // Internal knobs for the compaction copier, overridable by tests
    internal sealed class CompactionLimits
    {
        public static readonly CompactionLimits Default = new CompactionLimits();

        // Inputs per bin
        public int MaxInputs { get; set; } = 50;

        // An input starts only while fewer groups are written (R27)
        public int SoftGroups { get; set; } = 64;

        // More groups abandon the bin (group-cap)
        public int HardGroups { get; set; } = 1024;

        // Deletion vector containers fetched per job (dv-allowance)
        public long DeletionVectorBytes { get; set; } = 64L << 20;

        // Live rows per bin (R28)
        public long MaxRows { get; set; } = 10_000_000;

        // Data commits a quiet table needs since its last settled candidate
        public int QuietCommits { get; set; } = 500;

        // The longest the quiet window grows
        public TimeSpan MaxQuietWindow { get; set; } = TimeSpan.FromDays(30);

        // Slots a cursor inspects per call in array mode
        public int ScanSlots { get; set; } = 32_768;

        // Leaf pages a cursor reads per call in tree mode
        public int ScanLeaves { get; set; } = 256;

        // Cold records read per call for revalidation
        public int ColdReads { get; set; } = 64;

        // Hits a cursor holds between calls
        public int Hits { get; set; } = 50;

        // Declined files remembered per table
        public int MemoEntries { get; set; } = 1_024;
    }
}
