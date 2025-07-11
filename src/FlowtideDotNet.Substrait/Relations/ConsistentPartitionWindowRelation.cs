using FlowtideDotNet.Substrait.Expressions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using static Substrait.Protobuf.AggregateRel.Types;

namespace FlowtideDotNet.Substrait.Relations
{
    public class ConsistentPartitionWindowRelation : Relation
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
    }
}
