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
    public enum ExchangeTargetType
    {
        /// <summary>
        /// Sends the result in the same stream to the relation that has the exchange relation as input
        /// </summary>
        StandardOutput = 1,
        /// <summary>
        /// Stores the result in a bucket that can be pulled by the target.
        /// This is useful to allow the target to pull the data when it is ready.
        /// </summary>
        PullBucket = 2
    }

    public abstract class ExchangeTarget
    {
        public abstract ExchangeTargetType Type { get; }

        /// <summary>
        /// List of partition ids to send to this target.
        /// If it is an empty list all partition ids should be sent to this target.
        /// </summary>
        public required List<int> PartitionIds { get; set; }

        public override bool Equals(object? obj)
        {
            return obj is ExchangeTarget exchangeTarget &&
                Equals(Type, exchangeTarget.Type) &&
                PartitionIds.SequenceEqual(exchangeTarget.PartitionIds);
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(Type);
        }
    }

    public sealed class StandardOutputExchangeTarget : ExchangeTarget, IEquatable<StandardOutputExchangeTarget>
    {
        public override ExchangeTargetType Type => ExchangeTargetType.StandardOutput;

        public override bool Equals(object? obj)
        {
            return obj is StandardOutputExchangeTarget standardOutputExchangeTarget &&
                Equals(standardOutputExchangeTarget);
        }

        public bool Equals(StandardOutputExchangeTarget? other)
        {
            return other != null &&
                base.Equals(other);
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(base.GetHashCode());
        }

        public static bool operator ==(StandardOutputExchangeTarget? left, StandardOutputExchangeTarget? right)
        {
            return EqualityComparer<StandardOutputExchangeTarget>.Default.Equals(left, right);
        }

        public static bool operator !=(StandardOutputExchangeTarget? left, StandardOutputExchangeTarget? right)
        {
            return !(left == right);
        }
    }

    public sealed class PullBucketExchangeTarget : ExchangeTarget, IEquatable<PullBucketExchangeTarget>
    {
        public override ExchangeTargetType Type => ExchangeTargetType.PullBucket;

        /// <summary>
        /// An identifier that should be unique inside the stream/substream.
        /// This is used when the target wants to pull the data.
        /// </summary>
        public int ExchangeTargetId { get; set; }

        public bool Equals(PullBucketExchangeTarget? other)
        {
            return other != null &&
                base.Equals(other) &&
                Equals(ExchangeTargetId, other.ExchangeTargetId);
        }

        public override bool Equals(object? obj)
        {
            return obj is PullBucketExchangeTarget other &&
                Equals(other);
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(base.GetHashCode(), ExchangeTargetId);
        }

        public static bool operator ==(PullBucketExchangeTarget? left, PullBucketExchangeTarget? right)
        {
            return EqualityComparer<PullBucketExchangeTarget>.Default.Equals(left, right);
        }

        public static bool operator !=(PullBucketExchangeTarget? left, PullBucketExchangeTarget? right)
        {
            return !(left == right);
        }
    }
}
