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
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Zanzibar
{
    [DebuggerDisplay("{DebuggerDisplay,nq}")]
    public class ZanzibarTupleToUsersetRelation : ZanzibarTypeRelation
    {
        public ZanzibarTupleToUsersetRelation(string referenceRelation, string pointerRelation)
        {
            ReferenceRelation = referenceRelation;
            PointerRelation = pointerRelation;
        }

        public string ReferenceRelation { get; set; }
        public string PointerRelation { get; }

        internal override string DebuggerDisplay => $"{ReferenceRelation}->{PointerRelation}";

        public override T Accept<T, TState>(ZanzibarTypeRelationVisitor<T, TState> visitor, TState state)
        {
            return visitor.VisitTupleToUserset(this, state);
        }
    }
}
