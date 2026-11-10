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

using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat.ParquetWriters;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat
{
    // Allowance is Built plus W_group, and Pool is 1.5·Cap
    internal sealed record GroupBudget(JobMemory Memory, long Allowance, long GroupMetadata, long Pool, Func<BuilderSizes, long, long> WriterBytes)
    {
        // Groups whose metadata admission charged, g_i of the current input
        public int ChargedGroups { get; set; }

        // Disk for the group and the footer, reserved before the writer takes them
        public Action<BuilderSizes, long, int>? CoverDisk { get; init; }
    }
}
