using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema.Types
{
    internal class StringType : PrimitiveType, IEquatable<StringType>
    {
        public override SchemaType Type => SchemaType.String;

        public override T Accept<T>(DeltaTypeVisitor<T> visitor)
        {
            return visitor.VisitStringType(this);
        }

        public bool Equals(StringType? other)
        {
            return other != null;
        }

        public override bool Equals(object? obj)
        {
            return Equals(obj as StringType);
        }

        public override int GetHashCode()
        {
            return Type.GetHashCode();
        }
    }
}
