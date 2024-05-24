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
{public sealed class ExchangeRelation : Relation, IEquatable<ExchangeRelation>
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
