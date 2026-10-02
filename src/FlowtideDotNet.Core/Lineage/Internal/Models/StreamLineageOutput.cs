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

namespace FlowtideDotNet.Core.Lineage.Internal.Models
{
    internal sealed class StreamLineageOutput : StreamLineageTable
    {
        // Merged across every write to the table.
        public ColumnLineage? ColumnLineage { get; init; }

        // Input keys reachable from the writes, first seen order.
        public required IReadOnlyList<string> UpstreamInputKeys { get; init; }
    }
}
