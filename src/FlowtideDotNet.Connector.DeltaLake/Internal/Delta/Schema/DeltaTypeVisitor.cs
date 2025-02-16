using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema.Types;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema
{
    internal class DeltaTypeVisitor<T>
    {
        public T Visit(SchemaBaseType type)
        {
            return type.Accept(this);
        }

        public virtual T VisitStringType(StringType type)
        {
            throw new NotImplementedException("String type is not implemented");
        }

        public virtual T VisitArrayType(ArrayType type)
        {
            throw new NotImplementedException("Array type is not implemented");
        }

        public virtual T VisitBinaryType(BinaryType type)
        {
            throw new NotImplementedException("Binary type is not implemented");
        }

        public virtual T VisitBooleanType(BooleanType type)
        {
            throw new NotImplementedException("Boolean type is not implemented");
        }

        public virtual T VisitByteType(ByteType type)
        {
            throw new NotImplementedException("Byte type is not implemented");
        }

        public virtual T VisitDateType(DateType type)
        {
            throw new NotImplementedException("Date type is not implemented");
        }

        public virtual T VisitDecimalType(DecimalType type)
        {
            throw new NotImplementedException("Decimal type is not implemented");
        }

        public virtual T VisitDoubleType(DoubleType type)
        {
            throw new NotImplementedException("Double type is not implemented");
        }

        public virtual T VisitFloatType(FloatType type)
        {
            throw new NotImplementedException("Float type is not implemented");
        }

        public virtual T VisitIntegerType(IntegerType type)
        {
            throw new NotImplementedException("Integer type is not implemented");
        }

        public virtual T VisitLongType(LongType type)
        {
            throw new NotImplementedException("Long type is not implemented");
        }

        public virtual T VisitShortType(ShortType type)
        {
            throw new NotImplementedException("Short type is not implemented");
        }

        public virtual T VisitStructType(StructType type)
        {
            throw new NotImplementedException("Struct type is not implemented");
        }

        public virtual T VisitTimestampType(TimestampType type)
        {
            throw new NotImplementedException("Timestamp type is not implemented");
        }

        public virtual T VisitMapType(MapType type)
        {
            throw new NotImplementedException("Map type is not implemented");
        }

    }
}
