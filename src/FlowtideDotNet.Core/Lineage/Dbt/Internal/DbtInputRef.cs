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

using FlowtideDotNet.Core.Lineage.Internal;

namespace FlowtideDotNet.Core.Lineage.Dbt.Internal
{
    // Lineage input field re-keyed to a resolved table.
    internal sealed class DbtInputRef
    {
        public DbtInputRef(DbtTableIdentity? identity, string @namespace, string tableName, string field, IReadOnlyList<LineageTransformation> transformations)
        {
            Identity = identity;
            Namespace = @namespace;
            TableName = tableName;
            Field = field;
            Transformations = transformations;
        }

        // Null when the namespace is excluded.
        public DbtTableIdentity? Identity { get; }

        public string Namespace { get; }

        public string TableName { get; }

        public string Field { get; }

        public IReadOnlyList<LineageTransformation> Transformations { get; }

        public bool IsDirect => Transformations.Any(x => x.Type == LineageTransformationType.Direct);

        public bool Has(LineageTransformationSubtype subtype)
        {
            return Transformations.Any(x => x.SubType == subtype);
        }

        public DbtInputRef WithTransformations(IReadOnlyList<LineageTransformation> transformations)
        {
            return new DbtInputRef(Identity, Namespace, TableName, Field, transformations);
        }
    }

    // Ordered union, collisions merge their transformations.
    internal sealed class DbtRefList
    {
        private readonly List<DbtInputRef> _refs = new List<DbtInputRef>();
        private readonly Dictionary<(DbtTableIdentity?, string?, string?, string), int> _index = new Dictionary<(DbtTableIdentity?, string?, string?, string), int>();

        public IReadOnlyList<DbtInputRef> Refs => _refs;

        public void Add(DbtInputRef inputRef)
        {
            (DbtTableIdentity?, string?, string?, string) key = inputRef.Identity != null
                ? (inputRef.Identity, null, null, inputRef.Field)
                : (null, inputRef.Namespace, inputRef.TableName, inputRef.Field);
            if (_index.TryGetValue(key, out var position))
            {
                var existing = _refs[position];
                _refs[position] = existing.WithTransformations(LineageMerge.MergeTransformations(existing.Transformations, inputRef.Transformations));
                return;
            }
            _index.Add(key, _refs.Count);
            _refs.Add(inputRef);
        }
    }
}
