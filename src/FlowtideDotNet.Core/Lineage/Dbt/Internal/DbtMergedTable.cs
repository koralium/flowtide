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
    // One physical table across every snapshot in scope.
    internal sealed class DbtMergedTable
    {
        private readonly List<string> _outputColumns = new List<string>();
        private readonly HashSet<string> _outputColumnSet = new HashSet<string>(StringComparer.Ordinal);
        private readonly Dictionary<string, DbtRefList> _fields = new Dictionary<string, DbtRefList>(StringComparer.Ordinal);
        private readonly List<DbtTableIdentity> _reachable = new List<DbtTableIdentity>();
        private readonly HashSet<DbtTableIdentity> _reachableSet = new HashSet<DbtTableIdentity>();

        public DbtMergedTable(DbtTableIdentity identity)
        {
            Identity = identity;
        }

        public DbtTableIdentity Identity { get; }

        // Written by any stream in scope, so a model.
        public bool IsWritten { get; set; }

        public SortedSet<string> Writers { get; } = new SortedSet<string>(StringComparer.Ordinal);

        public SortedSet<string> Readers { get; } = new SortedSet<string>(StringComparer.Ordinal);

        public List<DbtColumn> WrittenColumns { get; } = new List<DbtColumn>();

        public List<DbtColumn> ReadColumns { get; } = new List<DbtColumn>();

        public List<DbtColumn> ReferencedColumns { get; } = new List<DbtColumn>();

        public DbtColumnSet Columns { get; private set; } = new DbtColumnSet();

        // Projection order, keyed by canonical column names.
        public IReadOnlyList<string> OutputColumns => _outputColumns;

        public IReadOnlyDictionary<string, DbtRefList> Fields => _fields;

        public DbtRefList Dataset { get; } = new DbtRefList();

        public IReadOnlyList<DbtTableIdentity> Reachable => _reachable;

        // Written columns first, then reads, then referenced fields.
        public void FinishColumns()
        {
            var columns = new DbtColumnSet();
            columns.AddRange(WrittenColumns);
            columns.AddRange(ReadColumns);
            columns.AddRange(ReferencedColumns);
            Columns = columns;
        }

        public void AddOutputColumn(string name)
        {
            if (_outputColumnSet.Add(name))
            {
                _outputColumns.Add(name);
            }
        }

        public DbtRefList GetFieldRefs(string column)
        {
            if (!_fields.TryGetValue(column, out var refs))
            {
                refs = new DbtRefList();
                _fields.Add(column, refs);
            }
            return refs;
        }

        public void AddReachable(DbtTableIdentity identity)
        {
            if (_reachableSet.Add(identity))
            {
                _reachable.Add(identity);
            }
        }
    }
}
