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

using PermifyProto = Base.V1;

namespace FlowtideDotNet.Connector.Permify.Internal
{
    internal class DeleteRequest
    {
        private readonly List<string> _entityIds;
        private readonly List<string> _subjectIds;
        private readonly string _entityType;
        private readonly string _relation;
        private readonly string _subjectType;
        private readonly string? _subjectRelation;

        public DeleteRequest(string entityType, string relation, string subjectType, string? subjectRelation)
        {
            _entityIds = new List<string>();
            _subjectIds = new List<string>();
            _entityType = entityType;
            _relation = relation;
            _subjectType = subjectType;
            _subjectRelation = subjectRelation;
        }

        public IReadOnlyList<string> EntityIds => _entityIds;

        public IReadOnlyList<string> SubjectIds => _subjectIds;

        public void Add(PermifyProto.Tuple tuple)
        {
            _entityIds.Add(tuple.Entity.Id);
            _subjectIds.Add(tuple.Subject.Id);
        }

        public PermifyProto.TupleFilter GetFilter()
        {
            var filter = new PermifyProto.TupleFilter()
            {
                Entity = new PermifyProto.EntityFilter()
                {
                    Type = _entityType
                },
                Relation = _relation,
                Subject = new PermifyProto.SubjectFilter()
                {
                    Type = _subjectType
                }
            };

            if (!string.IsNullOrEmpty(_subjectRelation))
            {
                filter.Subject.Relation = _subjectRelation;
            }

            foreach(var entityId in _entityIds)
            {
                filter.Entity.Ids.Add(entityId);
            }

            foreach(var subjectId in _subjectIds)
            {
                filter.Subject.Ids.Add(subjectId);
            }

            return filter;
        }
    }
}
