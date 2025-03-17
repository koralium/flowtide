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

using Apache.Arrow;
using Apache.Arrow.Types;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat.ParquetWriters
{
    class DeltaTypeArrowTypeVisitor : DeltaTypeVisitor<Apache.Arrow.Types.ArrowType>
    {
        public override ArrowType VisitStringType(Schema.Types.StringType type)
        {
            return new StringType();
        }

        public override ArrowType VisitLongType(Schema.Types.LongType type)
        {
            return new Int64Type();
        }

        public override ArrowType VisitIntegerType(Schema.Types.IntegerType type)
        {
            return new Int32Type();
        }

        public override ArrowType VisitByteType(Schema.Types.ByteType type)
        {
            return new Int8Type();
        }

        public override ArrowType VisitShortType(Schema.Types.ShortType type)
        {
            return new Int16Type();
        }

        public override ArrowType VisitFloatType(Schema.Types.FloatType type)
        {
            return new FloatType();
        }

        public override ArrowType VisitDoubleType(Schema.Types.DoubleType type)
        {
            return new DoubleType();
        }

        public override ArrowType VisitBooleanType(Schema.Types.BooleanType type)
        {
            return new BooleanType();
        }

        public override ArrowType VisitDateType(Schema.Types.DateType type)
        {
            return new Date32Type();
        }

        public override ArrowType VisitDecimalType(Schema.Types.DecimalType type)
        {
            return new Decimal128Type(type.Precision, type.Scale);
        }

        public override ArrowType VisitTimestampType(Schema.Types.TimestampType type)
        {
            return new TimestampType(unit: TimeUnit.Millisecond, timezone: default(string));
        }

        public override ArrowType VisitStructType(Schema.Types.StructType type)
        {
            List<Field> fields = new List<Field>();
            foreach(var field in type.Fields)
            {
                var arrowType = Visit(field.Type);
                fields.Add(new Field(field.Name, arrowType, field.Nullable));
            }
            return new StructType(fields);
        }

        public override ArrowType VisitArrayType(Schema.Types.ArrayType type)
        {
            if (type.ElementType == null)
            {
                throw new InvalidOperationException("ArrayType must have an ElementType");
            }
            var inner = Visit(type.ElementType);
            return new ListType(inner);
        }

        public override ArrowType VisitBinaryType(Schema.Types.BinaryType type)
        {
            return new BinaryType();
        }

        public override ArrowType VisitMapType(Schema.Types.MapType type)
        {
            var keyType = Visit(type.KeyType);
            var valueType = Visit(type.ValueType);

            return new MapType(keyType, valueType);
        }
    }
}
