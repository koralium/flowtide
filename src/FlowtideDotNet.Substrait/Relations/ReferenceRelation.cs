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
    public sealed class ReferenceRelation : Relation, IEquatable<ReferenceRelation>
    {
        public ReferenceRelation()
        {
        }

        public override int OutputLength => ReferenceOutputLength;

        public int RelationId { get; set; }

        public int ReferenceOutputLength { get; set; }

        public override TReturn Accept<TReturn, TState>(RelationVisitor<TReturn, TState> visitor, TState state)
        {
            return visitor.VisitReferenceRelation(this, state);
        }

        public override bool Equals(object? obj)
        {
            return obj is ReferenceRelation relation &&
                Equals(relation);
        }

        public bool Equals(ReferenceRelation? other)
        {
            return other != null &&
                base.Equals(other) &&
                Equals(RelationId, other.RelationId) &&
                Equals(ReferenceOutputLength, other.ReferenceOutputLength);
        }

        public override int GetHashCode()
        {
            var code = new HashCode();
            code.Add(base.GetHashCode());
            code.Add(RelationId);
            code.Add(ReferenceOutputLength);
            return code.ToHashCode();
        }

        public static bool operator ==(ReferenceRelation? left, ReferenceRelation? right)
        {
            return EqualityComparer<ReferenceRelation>.Default.Equals(left, right);
        }

        public static bool operator !=(ReferenceRelation? left, ReferenceRelation? right)
        {
            return !(left == right);
        }
    }
}
