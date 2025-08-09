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
    /// <summary>
    /// Relation that marks that it should get its data from an exchange relations standard output.
    /// </summary>
    public sealed class StandardOutputExchangeReferenceRelation : Relation, IEquatable<StandardOutputExchangeReferenceRelation>
    {
        /// <summary>
        /// Relation id where the exchange relation is located
        /// </summary>
        public int RelationId { get; set; }

        /// <summary>
        /// Id of the target
        /// </summary>
        public int TargetId { get; set; }

        public int ReferenceOutputLength { get; set; }

        public override int OutputLength => ReferenceOutputLength;

        public override TReturn Accept<TReturn, TState>(RelationVisitor<TReturn, TState> visitor, TState state)
        {
            return visitor.VisitStandardOutputExchangeReferenceRelation(this, state);
        }

        public override bool Equals(object? obj)
        {
            return obj is StandardOutputExchangeReferenceRelation relation &&
                Equals(relation);
        }

        public bool Equals(StandardOutputExchangeReferenceRelation? other)
        {
            return other != null &&
                base.Equals(other) &&
                Equals(RelationId, other.RelationId) &&
                Equals(TargetId, other.TargetId) &&
                Equals(ReferenceOutputLength, other.ReferenceOutputLength);
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(base.GetHashCode(), RelationId, TargetId, ReferenceOutputLength);
        }

        public static bool operator ==(StandardOutputExchangeReferenceRelation? left, StandardOutputExchangeReferenceRelation? right)
        {
            return EqualityComparer<StandardOutputExchangeReferenceRelation>.Default.Equals(left, right);
        }

        public static bool operator !=(StandardOutputExchangeReferenceRelation? left, StandardOutputExchangeReferenceRelation? right)
        {
            return !(left == right);
        }
    }
}
