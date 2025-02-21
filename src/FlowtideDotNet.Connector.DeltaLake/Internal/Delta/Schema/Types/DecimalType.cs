using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema.Types
{
    internal class DecimalType : PrimitiveType, IEquatable<DecimalType>
    {
        public DecimalType(int precision, int scale)
        {
            Precision = precision;
            Scale = scale;
        }

        public override SchemaType Type => SchemaType.Decimal;

        public int Precision { get; }

        public int Scale { get; }

        public override T Accept<T>(DeltaTypeVisitor<T> visitor)
        {
            return visitor.VisitDecimalType(this);
        }

        public bool Equals(DecimalType? other)
        {
            return other != null && Precision == other.Precision && Scale == other.Scale;
        }

        public override bool Equals(object? obj)
        {
            return Equals(obj as DecimalType);
        }

        public override int GetHashCode()
        {
            return Type.GetHashCode();
        }
    }
}
