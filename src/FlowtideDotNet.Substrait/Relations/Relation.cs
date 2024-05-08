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

using System.Diagnostics.CodeAnalysis;
using System.Linq;
using static SqlParser.Ast.FetchDirection;

namespace FlowtideDotNet.Substrait.Relations
{
    public abstract class Relation
    {
        
        public List<int>? Emit { get; set; }

        [MemberNotNullWhen(true, nameof(Emit))]
        public bool EmitSet => Emit != null;

        public abstract int OutputLength { get; }

        public abstract TReturn Accept<TReturn, TState>(RelationVisitor<TReturn, TState> visitor, TState state);

        public override bool Equals(object? obj)
        {
            if (obj is Relation other)
            {
                if (Emit == null && other.Emit != null)
                {
                    return false;
                }
                if (Emit != null && other.Emit == null)
                {
                    return false;
                }
                if (Emit != null && other.Emit != null &&
                    !Emit.SequenceEqual(other.Emit))
                {
                    return false;
                }
                return true;
            }
            return false;
        }

        public override int GetHashCode()
        {
            var code = new HashCode();
            if (Emit != null)
            {
                foreach (var emit in Emit)
                {
                    code.Add(emit);
                }
            }
            return code.ToHashCode();
        }
    }
}
