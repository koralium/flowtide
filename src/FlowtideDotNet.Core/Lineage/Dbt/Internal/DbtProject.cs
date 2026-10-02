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

namespace FlowtideDotNet.Core.Lineage.Dbt.Internal
{
    internal sealed class DbtNodeColumn
    {
        public required string Name { get; init; }

        public string? DataType { get; init; }

        // Flat meta string, null when the column has no inputs.
        public string? Inputs { get; init; }
    }

    internal sealed class DbtModelNode
    {
        public required DbtNodeName Name { get; init; }

        public required DbtTableIdentity Identity { get; init; }

        public required string RelationName { get; init; }

        public required IReadOnlyList<DbtNodeColumn> Columns { get; init; }

        public required string CompiledCode { get; init; }

        public required string Checksum { get; init; }

        public required string Streams { get; init; }

        // Unique ids in FROM order, cycle breaking removes edges.
        public required List<string> DependsOn { get; init; }

        public List<string> DroppedDependencies { get; } = new List<string>();

        // Other unique ids on the same relation.
        public string? RelationCollision { get; init; }

        public IReadOnlyList<string> RefNames { get; set; } = [];

        public IReadOnlyList<(string SourceName, string Name)> Sources { get; set; } = [];
    }

    internal sealed class DbtSourceNode
    {
        public required DbtNodeName Name { get; init; }

        public required DbtTableIdentity Identity { get; init; }

        public required string RelationName { get; init; }

        public required IReadOnlyList<DbtNodeColumn> Columns { get; init; }

        public required string Streams { get; init; }

        public string? RelationCollision { get; init; }
    }

    internal sealed class DbtProject
    {
        public required string ProjectName { get; init; }

        public required string AdapterType { get; init; }

        public required DateTimeOffset GeneratedAt { get; init; }

        // Sorted by unique id for stable bytes.
        public required IReadOnlyList<DbtModelNode> Models { get; init; }

        public required IReadOnlyList<DbtSourceNode> Sources { get; init; }

        public required SortedDictionary<string, List<string>> ParentMap { get; init; }

        public required SortedDictionary<string, List<string>> ChildMap { get; init; }
    }
}
