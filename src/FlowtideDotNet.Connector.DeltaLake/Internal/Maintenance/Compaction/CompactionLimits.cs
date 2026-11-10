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
    }
}
