using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema.Types
{
    internal class IntegerType : PrimitiveType, IEquatable<IntegerType>
    {
        public override SchemaType Type => SchemaType.Integer;

        public override T Accept<T>(DeltaTypeVisitor<T> visitor)
        {
            return visitor.VisitIntegerType(this);
        }

        public bool Equals(IntegerType? other)
        {
            return other != null;
        }

        public override bool Equals(object? obj)
        {
            return Equals(obj as IntegerType);
        }

        public override int GetHashCode()
        {
            return Type.GetHashCode();
        }
    }
}
