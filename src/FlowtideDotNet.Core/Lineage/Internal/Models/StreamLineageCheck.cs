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
    // A table that the rows of a check are written to.
    internal sealed record StreamLineageCheckTarget(string Key, string Namespace, string TableName, IReadOnlyList<string> NameParts);

    internal sealed class StreamLineageCheck
    {
        // The id the check status notifications carry.
        public required string CheckId { get; init; }

        // The message template, placeholders unrendered.
        public required string Message { get; init; }

        // Writes downstream of the check, in plan order.
        public required IReadOnlyList<StreamLineageCheckTarget> Targets { get; init; }

        // In a global relation that every referencing substream builds, so the copies see the same rows.
        public bool Replicated { get; init; }
    }
}
