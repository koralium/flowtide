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
    public class ZanzibarTypeReference
    {
        public string Name { get; }

        public string? Relation { get; }

        public bool Wildcard { get; }

        public string? Caveat { get; }

        public ZanzibarTypeReference(string name, string? relation, bool wildcard, string? caveat)
        {
            Name = name;
            Relation = relation;
            Wildcard = wildcard;
            Caveat = caveat;
        }

        internal string DebuggerDisplay
        {
            get
            {
                StringBuilder sb = new StringBuilder();
                sb.Append(Name);
                if (Wildcard)
                {
                    sb.Append(":*");
                }
                if (Relation != null)
                {
                    sb.Append('#');
                    sb.Append(Relation);
                }
                if (Caveat != null)
                {
                    sb.Append(" with ");
                    sb.Append(Caveat);
                }
                return sb.ToString();
            }
        }
    }
}
