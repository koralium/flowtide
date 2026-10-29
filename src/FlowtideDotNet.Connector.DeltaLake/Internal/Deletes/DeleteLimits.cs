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

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Deletes
{
    // Internal knobs for the sink's delete and intake paths
    internal sealed class DeleteLimits
    {
        public static readonly DeleteLimits Default = new DeleteLimits();

        // Input batches go through the bulk inserter
        public bool BulkIntake { get; set; } = true;

        // Delete rows matched per pass
        public int RowCap { get; set; } = 1_000_000;

        // Stored bytes and the counted table per pass
        public long ByteCap { get; set; } = 128L * 1024 * 1024;

        // Counts the builders, up to twice the bytes, and their copy
        public int WrittenSlack { get; set; } = 3;

        // Cold file records fetched per lookup
        public int RecordGroup { get; set; } = 1024;

        // A file's entries are tested one by one up to this many
        public int SelectorThreshold { get; set; } = 8;

        // Every row hashes the same, for tests
        public bool CollideHashes { get; set; }

        // Test the first pruning column before the whole row
        public bool Prefilter { get; set; } = true;
    }
}
