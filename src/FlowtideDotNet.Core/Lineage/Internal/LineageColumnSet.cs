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

using FlowtideDotNet.Substrait.Type;

namespace FlowtideDotNet.Core.Lineage.Internal
{
    internal sealed class LineageMergedColumn
    {
        public LineageMergedColumn(string name, SubstraitBaseType type, bool fromConnector)
        {
            Name = name;
            Type = type;
            FromConnector = fromConnector;
        }

        public string Name { get; set; }

        public SubstraitBaseType Type { get; set; }

        public bool FromConnector { get; set; }
    }

    // Case-insensitive column list, connector casing wins.
    internal sealed class LineageColumnSet
    {
        private readonly List<LineageMergedColumn> _columns = new List<LineageMergedColumn>();
        private readonly Dictionary<string, LineageMergedColumn> _byName = new Dictionary<string, LineageMergedColumn>(StringComparer.OrdinalIgnoreCase);

        public IReadOnlyList<LineageMergedColumn> Columns => _columns;

        public void Add(string name, SubstraitBaseType type, bool fromConnector)
        {
            if (!_byName.TryGetValue(name, out var existing))
            {
                existing = new LineageMergedColumn(name, type, fromConnector);
                _byName.Add(name, existing);
                _columns.Add(existing);
                return;
            }

            if (fromConnector && !existing.FromConnector)
            {
                existing.Name = name;
                existing.FromConnector = true;
                if (!IsUnknown(type))
                {
                    existing.Type = type;
                }
            }
            else if (IsUnknown(existing.Type) && !IsUnknown(type))
            {
                existing.Type = type;
            }
        }

        public void AddRange(IEnumerable<LineageMergedColumn> columns)
        {
            foreach (var column in columns)
            {
                Add(column.Name, column.Type, column.FromConnector);
            }
        }

        // Unknown names keep the casing they came with.
        public string Canonical(string name)
        {
            return _byName.TryGetValue(name, out var column) ? column.Name : name;
        }

        private static bool IsUnknown(SubstraitBaseType type)
        {
            return type is AnyType || type is NullType;
        }
    }
}
