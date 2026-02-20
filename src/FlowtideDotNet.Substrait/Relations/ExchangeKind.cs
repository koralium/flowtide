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

using FlowtideDotNet.Substrait.Expressions;

namespace FlowtideDotNet.Substrait.Relations
{
    public enum ExchangeKindType
    {
        /// <summary>
        /// Distribute data using a system defined hashing function that considers one or more fields. 
        /// For the same type of fields and same ordering of values, the same partition target should be identified for different ExchangeRels
        /// </summary>
        Scatter = 1,
        /// <summary>
        /// Returns a single bucket number for each row, this will be taken modulo the number of partitions
        /// </summary>
        SingleBucket = 2,
        // Id 3 is reserved for Multi bucket, but at this point the substrait relation only contains a single expression
        // Id 4 is round robin, it will be difficult to implement in a differental dataflow setting since one needs to send new weights to the same target all the time.
        /// <summary>
        /// Broadcasts the event to all targets
        /// </summary>
        Broadcast = 5
    }

    public abstract class ExchangeKind
    {
        public abstract ExchangeKindType Type { get; }

        public override bool Equals(object? obj)
        {
            return obj is ExchangeKind other &&
                Equals(Type, other.Type);
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(Type);
        }
    }

    public sealed class ScatterExchangeKind : ExchangeKind, IEquatable<ScatterExchangeKind>
    {
        public override ExchangeKindType Type => ExchangeKindType.Scatter;

        public required List<FieldReference> Fields { get; set; }

        public bool Equals(ScatterExchangeKind? other)
        {
            return other != null &&
                base.Equals(other) &&
                Fields.SequenceEqual(other.Fields);
        }

        public override bool Equals(object? obj)
        {
            return obj is ScatterExchangeKind scatterExchangeKind &&
                Equals(scatterExchangeKind);
        }

        public override int GetHashCode()
        {
            var code = new HashCode();
            code.Add(base.GetHashCode());

            foreach (var field in Fields)
            {
                code.Add(field.GetHashCode());
            }

            return code.ToHashCode();
        }

        public static bool operator ==(ScatterExchangeKind? left, ScatterExchangeKind? right)
        {
            return EqualityComparer<ScatterExchangeKind>.Default.Equals(left, right);
        }

        public static bool operator !=(ScatterExchangeKind? left, ScatterExchangeKind? right)
        {
            return !(left == right);
        }
    }

    public sealed class SingleBucketExchangeKind : ExchangeKind, IEquatable<SingleBucketExchangeKind>
    {
        public override ExchangeKindType Type => ExchangeKindType.SingleBucket;

        /// <summary>
        /// Expression that returns the bucket number, this will be taken modulo the number of partitions
        /// </summary>
        public required Expression Expression { get; set; }

        public override bool Equals(object? obj)
        {
            return obj is SingleBucketExchangeKind singleBucketExchangeKind &&
                Equals(singleBucketExchangeKind);
        }

        public bool Equals(SingleBucketExchangeKind? other)
        {
            return other != null &&
                base.Equals(other) &&
                Equals(Expression, other.Expression);
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(base.GetHashCode(), Expression.GetHashCode());
        }

        public static bool operator ==(SingleBucketExchangeKind? left, SingleBucketExchangeKind? right)
        {
            return EqualityComparer<SingleBucketExchangeKind>.Default.Equals(left, right);
        }

        public static bool operator !=(SingleBucketExchangeKind? left, SingleBucketExchangeKind? right)
        {
            return !(left == right);
        }
    }

    public sealed class BroadcastExchangeKind : ExchangeKind, IEquatable<BroadcastExchangeKind>
    {
        public override ExchangeKindType Type => ExchangeKindType.Broadcast;

        public override bool Equals(object? obj)
        {
            return obj is BroadcastExchangeKind broadcastExchangeKind &&
                Equals(broadcastExchangeKind);
        }

        public bool Equals(BroadcastExchangeKind? other)
        {
            return other != null;
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(base.GetHashCode());
        }

        public static bool operator ==(BroadcastExchangeKind? left, BroadcastExchangeKind? right)
        {
            return EqualityComparer<BroadcastExchangeKind>.Default.Equals(left, right);
        }

        public static bool operator !=(BroadcastExchangeKind? left, BroadcastExchangeKind? right)
        {
            return !(left == right);
        }
    }
}
