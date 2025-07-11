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

using System.Diagnostics;
using System.Text;

namespace FlowtideDotNet.Zanzibar
{
    [DebuggerDisplay("{DebuggerDisplay,nq}")]
    public class ZanzibarIntersectRelation : ZanzibarTypeRelation
    {
        public ZanzibarIntersectRelation(List<ZanzibarTypeRelation> children)
        {
            Children = children;
        }

        public List<ZanzibarTypeRelation> Children { get; }

        internal override string DebuggerDisplay
        {
            get
            {
                StringBuilder sb = new StringBuilder();
                foreach (var type in Children)
                {
                    sb.Append(type.DebuggerDisplay);
                    sb.Append(" & ");
                }
                sb.Length -= 3;
                return sb.ToString();
            }
        }

        public override T Accept<T, TState>(ZanzibarTypeRelationVisitor<T, TState> visitor, TState state)
        {
            return visitor.VisitIntersect(this, state);
        }
    }
}
