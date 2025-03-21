﻿// Licensed under the Apache License, Version 2.0 (the "License")
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

using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat.ParquetWriters;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema.Types;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat
{
    internal class ParquetSharpWriteVisitor : DeltaTypeVisitor<IParquetWriter>
    {
        public override IParquetWriter VisitStringType(StringType type)
        {
            return new ParquetStringWriter();
        }

        public override IParquetWriter VisitLongType(LongType type)
        {
            return new ParquetInt64Writer();
        }

        public override IParquetWriter VisitBooleanType(BooleanType type)
        {
            return new ParquetBoolWriter();
        }

        public override IParquetWriter VisitDateType(DateType type)
        {
            return new ParquetDateWriter();
        }

        public override IParquetWriter VisitIntegerType(IntegerType type)
        {
            return new ParquetIntegerWriter();
        }

        public override IParquetWriter VisitFloatType(FloatType type)
        {
            return new ParquetFloat32Writer();
        }

        public override IParquetWriter VisitDecimalType(DecimalType type)
        {
            return new ParquetDecimalWriter(type.Precision, type.Scale);
        }

        public override IParquetWriter VisitDoubleType(DoubleType type)
        {
            return new ParquetFloat64Writer();
        }

        public override IParquetWriter VisitByteType(ByteType type)
        {
            return new ParquetInt8Writer();
        }

        public override IParquetWriter VisitShortType(ShortType type)
        {
            return new ParquetInt16Writer();
        }

        public override IParquetWriter VisitTimestampType(TimestampType type)
        {
            return new ParquetTimestampWriter();
        }

        public override IParquetWriter VisitBinaryType(BinaryType type)
        {
            return new ParquetBinaryWriter();
        }

        public override IParquetWriter VisitArrayType(ArrayType type)
        {
            if (type.ElementType == null)
            {
                throw new InvalidOperationException("ArrayType must have an ElementType");
            }
            return new ParquetArrayWriter(Visit(type.ElementType));
        }

        public override IParquetWriter VisitStructType(StructType type)
        {
            List<KeyValuePair<string, IParquetWriter>> fields = new List<KeyValuePair<string, IParquetWriter>>();
            foreach (var field in type.Fields)
            {
                var writer = Visit(field.Type);
                fields.Add(new KeyValuePair<string, IParquetWriter>(field.Name, writer));
            }
            return new ParquetStructWriter(fields);
        }

        public override IParquetWriter VisitMapType(MapType type)
        {
            var keyWriter = Visit(type.KeyType);
            var valueWriter = Visit(type.ValueType);

            return new ParquetMapWriter(keyWriter, valueWriter);
        }
    }
}
