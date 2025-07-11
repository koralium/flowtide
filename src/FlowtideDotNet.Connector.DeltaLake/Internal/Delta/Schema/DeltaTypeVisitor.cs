// Licensed under the Apache License, Version 2.0 (the "License")
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//  
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema.Types;

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
