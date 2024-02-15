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

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Zanzibar.QueryPlanner.Models
{
    public class ZanzibarVisitor<T, TState>
    {

        public virtual T Visit(ZanzibarRelation relation, TState state)
        {
            return relation.Accept(this, state);
        }

        public virtual T VisitZanzibarChangeRelationName(ZanzibarChangeRelationName changeRelationName, TState state)
        {
            throw new NotImplementedException();
        }

        public virtual T VisitZanzibarJoinIntersectWildcard(ZanzibarJoinIntersectWildcard joinIntersectWildcard, TState state)
        {
            throw new NotImplementedException();
        }

        public virtual T VisitZanzibarJoinOnUserTypeId(ZanzibarJoinOnUserTypeId joinOnUserTypeId, TState state)
        {
            throw new NotImplementedException();
        }

        public virtual T VisitZanzibarJoinUserRelation(ZanzibarJoinUserRelation joinUserRelation, TState state)
        {
            throw new NotImplementedException();
        }

        public virtual T VisitZanzibarJoinUserToObject(ZanzibarJoinUserToObject joinUserToObject, TState state)
        {
            throw new NotImplementedException();
        }

        public virtual T VisitZanzibarLoop(ZanzibarLoop loop, TState state)
        {
            throw new NotImplementedException();
        }

        public virtual T VisitZanzibarReadLoop(ZanzibarReadLoop readLoop, TState state)
        {
            throw new NotImplementedException();
        }

        public virtual T VisitZanzibarReadUserAndObjectType(ZanzibarReadUserAndObjectType readUserAndObjectType, TState state)
        {
            throw new NotImplementedException();
        }

        public virtual T VisitZanzibarReadUserRelation(ZanzibarReadUserRelation readUserRelation, TState state)
        {
            throw new NotImplementedException();
        }

        public virtual T VisitZanzibarUnion(ZanzibarUnion union, TState state)
        {
            throw new NotImplementedException();
        }

        public virtual T VisitZanzibarRelationReference(ZanzibarRelationReference relationReference, TState state)
        {
            throw new NotImplementedException();
        }
    }
}
