using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema.Types
{
    internal class DateType : PrimitiveType, IEquatable<DateType>
    {
        public override SchemaType Type => SchemaType.Date;

        public override T Accept<T>(DeltaTypeVisitor<T> visitor)
        {
            return visitor.VisitDateType(this);
        }

        public bool Equals(DateType? other)
        {
            return other != null;
        }

        public override bool Equals(object? obj)
        {
            return Equals(obj as DateType);
        }

        public override int GetHashCode()
        {
            return Type.GetHashCode();
        }
    }
}
