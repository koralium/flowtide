using FlowtideDotNet.Substrait.Expressions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using static Substrait.Protobuf.AggregateRel.Types;

namespace FlowtideDotNet.Substrait.Relations
{
    public class ConsistentPartitionWindowRelation : Relation, IEquatable<ConsistentPartitionWindowRelation>
    {
        public required Relation Input { get; set; }

        public required List<WindowFunction> WindowFunctions { get; set; }

        public required List<Expression> PartitionBy { get; set; }

        public required List<SortField> OrderBy { get; set; }

        public override int OutputLength => CalculateOutputLength();

        private int CalculateOutputLength()
        {
            if (EmitSet)
            {
                return Emit.Count;
            }
            var inputLength = Input.OutputLength;

            inputLength += WindowFunctions.Count;
            return inputLength;
        }

        public override TReturn Accept<TReturn, TState>(RelationVisitor<TReturn, TState> visitor, TState state)
        {
            return visitor.VisitConsistentPartitionWindowRelation(this, state);
        }

        public override bool Equals(object? obj)
        {
            return obj is ConsistentPartitionWindowRelation relation &&
                Equals(relation);
        }


        public override int GetHashCode()
        {
            var code = new HashCode();
            code.Add(base.GetHashCode());
            code.Add(Input);
            foreach (var windowFunc in WindowFunctions)
            {
                code.Add(windowFunc);
            }
            foreach (var partition in PartitionBy)
            {
                code.Add(partition);
            }
            foreach (var order in OrderBy)
            {
                code.Add(order);
            }
            return code.ToHashCode();
        }

        public bool Equals(ConsistentPartitionWindowRelation? other)
        {
            return other != null &&
                base.Equals(other) &&
                Equals(Input, other.Input) &&
                WindowFunctions.SequenceEqual(other.WindowFunctions) &&
                PartitionBy.SequenceEqual(other.PartitionBy) &&
                OrderBy.SequenceEqual(other.OrderBy);
        }

        public static bool operator ==(ConsistentPartitionWindowRelation? left, ConsistentPartitionWindowRelation? right)
        {
            return EqualityComparer<ConsistentPartitionWindowRelation>.Default.Equals(left, right);
        }

        public static bool operator !=(ConsistentPartitionWindowRelation? left, ConsistentPartitionWindowRelation? right)
        {
            return !(left == right);
        }
    }
}
