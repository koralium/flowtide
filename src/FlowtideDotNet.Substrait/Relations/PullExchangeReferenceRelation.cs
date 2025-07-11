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
    public sealed class PullExchangeReferenceRelation : Relation, IEquatable<PullExchangeReferenceRelation>
    {
        public override int OutputLength => ReferenceOutputLength;

        public required string SubStreamName { get; set; }

        public int ReferenceOutputLength { get; set; }

        public int ExchangeTargetId { get; set; }

        public override TReturn Accept<TReturn, TState>(RelationVisitor<TReturn, TState> visitor, TState state)
        {
            return visitor.VisitPullExchangeReferenceRelation(this, state);
        }

        public override bool Equals(object? obj)
        {
            return obj is PullExchangeReferenceRelation other &&
                Equals(other);
        }

        public bool Equals(PullExchangeReferenceRelation? other)
        {
            return other != null &&
                base.Equals(other) &&
                ReferenceOutputLength == other.ReferenceOutputLength &&
                ExchangeTargetId == other.ExchangeTargetId &&
                Equals(SubStreamName, other.SubStreamName);
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(SubStreamName, ReferenceOutputLength, ExchangeTargetId);
        }

        public static bool operator ==(PullExchangeReferenceRelation? left, PullExchangeReferenceRelation? right)
        {
            return EqualityComparer<PullExchangeReferenceRelation>.Default.Equals(left, right);
        }

        public static bool operator !=(PullExchangeReferenceRelation? left, PullExchangeReferenceRelation? right)
        {
            return !(left == right);
        }
    }
}
