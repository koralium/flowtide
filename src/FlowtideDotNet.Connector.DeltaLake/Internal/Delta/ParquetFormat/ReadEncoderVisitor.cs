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

using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat.Encoders;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema.Types;
using FlowtideDotNet.Core.ColumnStore;
using SqlParser.Ast;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat
{
    internal class ReadEncoderVisitor : DeltaTypeVisitor<IParquetEncoder>
    {
        private List<string> _path = new List<string>();
        private int _repititionLevel = 0;
        private int _definitionLevel = 0;
        public ReadEncoderVisitor()
        {

        }

        public void SetRootPath(string rootPath)
        {
            _path.Add(rootPath);
        }

        public void ClearRootPath()
        {
            _path.Clear();
        }

        public override IParquetEncoder VisitIntegerType(IntegerType type)
        {
            var encoder = new Int32Encoder(new Parquet.Schema.FieldPath(_path));
            return encoder;
        }

        public override IParquetEncoder VisitStringType(StringType type)
        {
            var encoder = new ParquetStringEncoder(new Parquet.Schema.FieldPath(_path));
            return encoder;
        }

        public override IParquetEncoder VisitDoubleType(DoubleType type)
        {
            var encoder = new DoubleEncoder(new Parquet.Schema.FieldPath(_path));
            return encoder;
        }

        public override IParquetEncoder VisitLongType(LongType type)
        {
            var encoder = new Int64Encoder(new Parquet.Schema.FieldPath(_path));
            return encoder;
        }

        public override IParquetEncoder VisitStructType(StructType type)
        {
            List<IDataValue> propertyNames = new List<IDataValue>();
            List<IParquetEncoder> inner = new List<IParquetEncoder>();
            for (int i = 0; i < type.Fields.Count; i++)
            {
                propertyNames.Add(new StringValue(type.Fields[i].Name));
                _path.Add(type.Fields[i].Name);
                inner.Add(type.Fields[i].Type.Accept(this));
                _path.RemoveAt(_path.Count - 1);
            }

            return new StructEncoder(inner, propertyNames);
        }

        public override IParquetEncoder VisitShortType(ShortType type)
        {
            var encoder = new Int16Encoder(new Parquet.Schema.FieldPath(_path));
            return encoder;
        }

        public override IParquetEncoder VisitByteType(ByteType type)
        {
            var encoder = new Int8Encoder(new Parquet.Schema.FieldPath(_path));
            return encoder;
        }

        public override IParquetEncoder VisitFloatType(FloatType type)
        {
            var encoder = new FloatEncoder(new Parquet.Schema.FieldPath(_path));
            return encoder;
        }

        public override IParquetEncoder VisitBooleanType(BooleanType type)
        {
            return new BoolEncoder(new Parquet.Schema.FieldPath(_path));
        }

        public override IParquetEncoder VisitDecimalType(DecimalType type)
        {
            return new DecimalEncoder(new Parquet.Schema.FieldPath(_path));
        }

        public override IParquetEncoder VisitDateType(DateType type)
        {
            return new DateEncoder(new Parquet.Schema.FieldPath(_path));
        }

        public override IParquetEncoder VisitTimestampType(TimestampType type)
        {
            return new TimestampEncoder(new Parquet.Schema.FieldPath(_path));
        }

        public override IParquetEncoder VisitBinaryType(BinaryType type)
        {
            return new BinaryEncoder(new Parquet.Schema.FieldPath(_path));
        }

        public override IParquetEncoder VisitArrayType(ArrayType type)
        {
            if (type.ElementType == null)
            {
                throw new System.Exception("ArrayType must have an element type");
            }
            _repititionLevel++;
            _path.Add("list");
            _path.Add("element");
            var inner = type.ElementType.Accept(this);
            _path.RemoveAt(_path.Count - 1);
            _path.RemoveAt(_path.Count - 1);
            var arrEncoder = new ArrayEncoder(inner, _repititionLevel);
            _repititionLevel--;
            return arrEncoder;
        }

        public override IParquetEncoder VisitMapType(MapType type)
        {
            if (type.KeyType == null || type.ValueType == null)
            {
                throw new System.Exception("MapType must have a key and value type");
            }

            _repititionLevel++;
            _definitionLevel++;
            _path.Add("key_value");
            _path.Add("key");
            var keyEncoder = type.KeyType.Accept(this);
            _path.RemoveAt(_path.Count - 1);
            _path.RemoveAt(_path.Count - 1);

            _path.Add("key_value");
            _path.Add("value");
            var valueEncoder = type.ValueType.Accept(this);
            _path.RemoveAt(_path.Count - 1);
            _path.RemoveAt(_path.Count - 1);
            _definitionLevel--;

            var mapEncoder = new MapEncoder(keyEncoder, valueEncoder, _repititionLevel, _definitionLevel);
            _repititionLevel--;
            return mapEncoder;
        }
    }
}
