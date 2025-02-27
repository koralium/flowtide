using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema.Types
{
    internal class BinaryType : PrimitiveType, IEquatable<BinaryType>
    {
        public override SchemaType Type => SchemaType.Binary;

        public override T Accept<T>(DeltaTypeVisitor<T> visitor)
        {
            return visitor.VisitBinaryType(this);
        }

        public bool Equals(BinaryType? other)
        {
            return other != null;
        }

        public override bool Equals(object? obj)
        {
            return Equals(obj as BinaryType);
        }

        public override int GetHashCode()
        {
            return Type.GetHashCode();
        }
    }
}
