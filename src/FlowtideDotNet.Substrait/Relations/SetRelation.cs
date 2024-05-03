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


namespace FlowtideDotNet.Substrait.Relations
{
    public class SetRelation : Relation
    {
        public required List<Relation> Inputs { get; set; }

        public SetOperation Operation { get; set; }

        public override int OutputLength
        {
            get
            {
                if (EmitSet)
                {
                    return Emit!.Count;
                }
                return Inputs[0].OutputLength;
            }
        }

        public override TReturn Accept<TReturn, TState>(RelationVisitor<TReturn, TState> visitor, TState state)
        {
            return visitor.VisitSetRelation(this, state);
        }

        public override bool Equals(object? obj)
        {
            if (obj is SetRelation relation)
            {
                return EmitEquals(relation.Emit) &&
                   Inputs.SequenceEqual(relation.Inputs) &&
                   Equals(Operation, relation.Operation);
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
            foreach (var input in Inputs)
            {
                code.Add(input);
            }
            code.Add(Operation);
            return code.ToHashCode();
        }
    }
}
