﻿// Licensed under the Apache License, Version 2.0 (the "License")
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

using FlowtideDotNet.Substrait.Expressions;

namespace FlowtideDotNet.Substrait.Relations
{
    /// <summary>
    /// A relation that normalizes the input based on a key column.
    /// Any update on that key will replace the current value with the new value.
    /// A delete will delete the key even though there have been multiple inserts before with different values but the same key.
    /// </summary>
    public class NormalizationRelation : Relation
    {
        public required List<int> KeyIndex { get; set; }

        public Expression? Filter { get; set; }

        public required Relation Input { get; set; }

        public override int OutputLength
        {
            get
            {
                if (EmitSet)
                {
                    return Emit!.Count;
                }
                return Input.OutputLength;
            }
        }

        public override TReturn Accept<TReturn, TState>(RelationVisitor<TReturn, TState> visitor, TState state)
        {
            return visitor.VisitNormalizationRelation(this, state);
        }

        public override bool Equals(object? obj)
        {
            if (obj is NormalizationRelation relation)
            {
                return EmitEquals(relation.Emit) &&
                   KeyIndex.SequenceEqual(relation.KeyIndex) &&
                   Equals(Filter, relation.Filter) &&
                   Equals(Input, relation.Input);
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
            foreach(var index in KeyIndex)
            {
                code.Add(index);
            }
            code.Add(Filter);
            code.Add(Input);
            return code.ToHashCode();
        }
    }
}
