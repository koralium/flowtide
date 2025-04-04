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


namespace FlowtideDotNet.Substrait.Relations
{
    public sealed class SetRelation : Relation, IEquatable<SetRelation>
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
            return obj is SetRelation relation &&
                Equals(relation);
        }

        public bool Equals(SetRelation? other)
        {
            return other != null &&
                base.Equals(other) &&
                Inputs.SequenceEqual(other.Inputs) &&
                Equals(Operation, other.Operation);
        }

        public override int GetHashCode()
        {
            var code = new HashCode();
            code.Add(base.GetHashCode());
            foreach (var input in Inputs)
            {
                code.Add(input);
            }
            code.Add(Operation);
            return code.ToHashCode();
        }

        public static bool operator ==(SetRelation? left, SetRelation? right)
        {
            return EqualityComparer<SetRelation>.Default.Equals(left, right);
        }

        public static bool operator !=(SetRelation? left, SetRelation? right)
        {
            return !(left == right);
        }
    }
}
