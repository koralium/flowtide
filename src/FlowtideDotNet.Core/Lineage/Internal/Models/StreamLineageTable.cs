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
    internal abstract class StreamLineageTable
    {
        // Plan identity, the dot separated table name.
        public required string Key { get; init; }

        // Physical name parts, catalog prefix stripped.
        public required IReadOnlyList<string> NameParts { get; init; }

        public required string Namespace { get; init; }

        public required string TableName { get; init; }

        public IReadOnlyList<LineageColumn>? ConnectorColumns { get; init; }

        public required IReadOnlyList<LineageColumn> PlanColumns { get; init; }

        public IReadOnlyList<LineageColumn> SchemaColumns => ConnectorColumns ?? PlanColumns;
    }
}
