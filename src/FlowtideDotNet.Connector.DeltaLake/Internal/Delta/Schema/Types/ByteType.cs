using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema.Types
{
    internal class ByteType : PrimitiveType, IEquatable<ByteType>
    {
        public override SchemaType Type => SchemaType.Byte;

        public override T Accept<T>(DeltaTypeVisitor<T> visitor)
        {
            return visitor.VisitByteType(this);
        }

        public bool Equals(ByteType? other)
        {
            return other != null;
        }

        public override bool Equals(object? obj)
        {
            return Equals(obj as ByteType);
        }

        public override int GetHashCode()
        {
            return Type.GetHashCode();
        }
    }
}
