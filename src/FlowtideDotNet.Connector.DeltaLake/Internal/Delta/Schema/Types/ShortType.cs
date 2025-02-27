using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema.Types
{
    internal class ShortType : PrimitiveType, IEquatable<ShortType>
    {
        public override SchemaType Type => SchemaType.Short;

        public override T Accept<T>(DeltaTypeVisitor<T> visitor)
        {
            return visitor.VisitShortType(this);
        }

        public bool Equals(ShortType? other)
        {
            return other != null;
        }

        public override bool Equals(object? obj)
        {
            return Equals(obj as ShortType);
        }

        public override int GetHashCode()
        {
            return Type.GetHashCode();
        }
    }
}
