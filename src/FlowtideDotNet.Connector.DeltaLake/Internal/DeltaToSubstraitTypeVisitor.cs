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

using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema;
using FlowtideDotNet.Substrait.Type;

namespace FlowtideDotNet.Connector.DeltaLake.Internal
{
    internal class DeltaToSubstraitTypeVisitor : DeltaTypeVisitor<SubstraitBaseType>
    {
        public static readonly DeltaToSubstraitTypeVisitor Instance = new DeltaToSubstraitTypeVisitor();
        public override SubstraitBaseType VisitStringType(Delta.Schema.Types.StringType type)
        {
            return new StringType();
        }

        public override SubstraitBaseType VisitIntegerType(Delta.Schema.Types.IntegerType type)
        {
            return new Int64Type();
        }

        public override SubstraitBaseType VisitLongType(Delta.Schema.Types.LongType type)
        {
            return new Int64Type();
        }

        public override SubstraitBaseType VisitByteType(Delta.Schema.Types.ByteType type)
        {
            return new Int64Type();
        }

        public override SubstraitBaseType VisitShortType(Delta.Schema.Types.ShortType type)
        {
            return new Int64Type();
        }

        public override SubstraitBaseType VisitBinaryType(Delta.Schema.Types.BinaryType type)
        {
            return new BinaryType();
        }

        public override SubstraitBaseType VisitBooleanType(Delta.Schema.Types.BooleanType type)
        {
            return new BoolType();
        }

        public override SubstraitBaseType VisitDateType(Delta.Schema.Types.DateType type)
        {
            return new TimestampType();
        }

        public override SubstraitBaseType VisitDoubleType(Delta.Schema.Types.DoubleType type)
        {
            return new Fp64Type();
        }

        public override SubstraitBaseType VisitFloatType(Delta.Schema.Types.FloatType type)
        {
            return new Fp64Type();
        }

        public override SubstraitBaseType VisitDecimalType(Delta.Schema.Types.DecimalType type)
        {
            return new DecimalType();
        }

        public override SubstraitBaseType VisitArrayType(Delta.Schema.Types.ArrayType type)
        {
            if (type.ElementType == null)
            {
                throw new Exception("ArrayType must have an element type");
            }
            var innerType = Visit(type.ElementType);
            return new ListType(innerType);
        }

        public override SubstraitBaseType VisitTimestampType(Delta.Schema.Types.TimestampType type)
        {
            return new TimestampType();
        }

        public override SubstraitBaseType VisitStructType(Delta.Schema.Types.StructType type)
        {
            return new MapType(new StringType(), new AnyType());
        }

        public override SubstraitBaseType VisitMapType(Delta.Schema.Types.MapType type)
        {
            var keyType = Visit(type.KeyType);
            var valueType = Visit(type.ValueType);
            return new MapType(keyType, valueType);
        }
    }
}
