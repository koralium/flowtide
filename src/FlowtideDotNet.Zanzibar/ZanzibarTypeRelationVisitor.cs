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

namespace FlowtideDotNet.Zanzibar
{
    public class ZanzibarTypeRelationVisitor<T, TState>
    {
        public virtual T Visit(ZanzibarTypeRelation relation, TState state)
        {
            return relation.Accept(this, state);
        }

        public virtual T VisitComputedUserset(ZanzibarComputedUsersetRelation relation, TState state)
        {
            throw new NotImplementedException();
        }

        public virtual T VisitIntersect(ZanzibarIntersectRelation relation, TState state)
        {
            throw new NotImplementedException();
        }

        public virtual T VisitThis(ZanzibarThisRelation relation, TState state)
        {
            throw new NotImplementedException();
        }

        public virtual T VisitTupleToUserset(ZanzibarTupleToUsersetRelation relation, TState state)
        {
            throw new NotImplementedException();
        }

        public virtual T VisitUnion(ZanzibarUnionRelation relation, TState state)
        {
            throw new NotImplementedException();
        }
    }
}
