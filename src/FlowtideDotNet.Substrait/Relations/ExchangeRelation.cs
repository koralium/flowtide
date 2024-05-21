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
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

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

            foreach(var field in Fields)
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
            return other != null && base.Equals(other);
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

    public class StandardOutputExchangeTarget : ExchangeTarget, IEquatable<StandardOutputExchangeTarget>
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

    public class PullBucketExchangeTarget : ExchangeTarget, IEquatable<PullBucketExchangeTarget>
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


    public class ExchangeRelation : Relation, IEquatable<ExchangeRelation>
    {
        public required Relation Input { get; set; }

        public int? PartitionCount { get; set; }

        public required ExchangeKind ExchangeKind { get; set; }

        public required List<ExchangeTarget> Targets { get; set; }

        public override int OutputLength
        {
            get
            {
                if (EmitSet)
                {
                    return Emit.Count;
                }
                return Input.OutputLength;
            }
        }

        public override TReturn Accept<TReturn, TState>(RelationVisitor<TReturn, TState> visitor, TState state)
        {
            return visitor.VisitExchangeRelation(this, state);
        }

        public bool Equals(ExchangeRelation? other)
        {
            return other != null &&
                base.Equals(other) &&
                Equals(Input, other.Input) &&
                Equals(PartitionCount, other.PartitionCount) &&
                Equals(ExchangeKind, other.ExchangeKind) &&
                Targets.SequenceEqual(other.Targets);
        }

        public override bool Equals(object? obj)
        {
            return obj is ExchangeRelation other &&
                Equals(other);
        }

        public override int GetHashCode()
        {
            HashCode code = new HashCode();
            code.Add(base.GetHashCode());
            code.Add(Input);
            code.Add(PartitionCount);
            code.Add(ExchangeKind);

            foreach(var target in Targets)
            {
                code.Add(target);
            }

            return code.ToHashCode();
        }

        public static bool operator ==(ExchangeRelation? left, ExchangeRelation? right)
        {
            return EqualityComparer<ExchangeRelation>.Default.Equals(left, right);
        }

        public static bool operator !=(ExchangeRelation? left, ExchangeRelation? right)
        {
            return !(left == right);
        }
    }
}
