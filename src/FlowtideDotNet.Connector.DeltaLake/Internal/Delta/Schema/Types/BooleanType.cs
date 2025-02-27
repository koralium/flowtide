using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema.Types
{
    internal class BooleanType : PrimitiveType, IEquatable<BooleanType>
    {
        public override SchemaType Type => SchemaType.Boolean;

        public override T Accept<T>(DeltaTypeVisitor<T> visitor)
        {
            return visitor.VisitBooleanType(this);
        }

        public bool Equals(BooleanType? other)
        {
            return other != null;
        }

        public override bool Equals(object? obj)
        {
            return Equals(obj as BooleanType);
        }

        public override int GetHashCode()
        {
            return Type.GetHashCode();
        }
    }
}
