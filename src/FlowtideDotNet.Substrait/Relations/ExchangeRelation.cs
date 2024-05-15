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
    }

    public class ScatterExchangeKind : ExchangeKind
    {
        public override ExchangeKindType Type => ExchangeKindType.Scatter;

        public required List<FieldReference> Fields { get; set; }
    }

    public class SingleBucketExchangeKind : ExchangeKind
    {
        public override ExchangeKindType Type => ExchangeKindType.SingleBucket;

        /// <summary>
        /// Expression that returns the bucket number, this will be taken modulo the number of partitions
        /// </summary>
        public required Expression Expression { get; set; }
    }

    public class BroadcastExchangeKind : ExchangeKind
    {
        public override ExchangeKindType Type => ExchangeKindType.Broadcast;
    }

    public enum ExchangeTargetType
    {
        /// <summary>
        /// Sends the result in the same stream to the relation that has the exchange relation as input
        /// </summary>
        StandardOutput = 1,
        /// <summary>
        /// Sends the result to another stream based on a stream reference name
        /// </summary>
        StreamReference = 2
    }

    public abstract class ExchangeTarget
    {
        public abstract ExchangeTargetType Type { get; }
    }

    public class StandardOutputExchangeTarget : ExchangeTarget
    {
        public override ExchangeTargetType Type => ExchangeTargetType.StandardOutput;
    }

    public class StreamReferenceExchangeTarget : ExchangeTarget
    {
        public override ExchangeTargetType Type => ExchangeTargetType.StreamReference;

        /// <summary>
        /// Reference id of the stream to send the result to.
        /// It is up to the ececutor on how to find the stream with the given id.
        /// The id should be the relation id of the relation that has the exchange relation as input.
        /// </summary>
        public required int StreamReferenceId { get; set; }

        /// <summary>
        /// An identifier that should be unique inside the stream/substream.
        /// This is used so it knows where to send the result to.
        /// </summary>
        public int ExchangeTargetId { get; set; }
    }


    public class ExchangeRelation : Relation
    {
        public required Relation Input { get; set; }

        public int PartitionCount { get; set; }

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
    }
}
