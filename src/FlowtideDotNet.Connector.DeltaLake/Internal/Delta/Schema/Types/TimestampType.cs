using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema.Types
{
    internal class TimestampType : PrimitiveType, IEquatable<TimestampType>
    {
        public override SchemaType Type => SchemaType.Timestamp;

        public override T Accept<T>(DeltaTypeVisitor<T> visitor)
        {
            return visitor.VisitTimestampType(this);
        }

        public bool Equals(TimestampType? other)
        {
            return other != null;
        }

        public override bool Equals(object? obj)
        {
            return Equals(obj as TimestampType);
        }

        public override int GetHashCode()
        {
            return Type.GetHashCode();
        }
    }
}
