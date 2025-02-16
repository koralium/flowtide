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

using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat.ArrowEncoders;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema.Types;
using FlowtideDotNet.Core.ColumnStore;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat
{
    internal class ParquetArrowTypeVisitor : DeltaTypeVisitor<IArrowEncoder>
    {
        public override IArrowEncoder VisitBooleanType(BooleanType type)
        {
            return new BoolEncoder();
        }

        public override IArrowEncoder VisitStringType(StringType type)
        {
            return new StringEncoder();
        }

        public override IArrowEncoder VisitShortType(ShortType type)
        {
            return new ShortEncoder();
        }

        public override IArrowEncoder VisitByteType(ByteType type)
        {
            return new Int8Encoder();
        }

        public override IArrowEncoder VisitLongType(LongType type)
        {
            return new Int64Encoder();
        }

        public override IArrowEncoder VisitIntegerType(IntegerType type)
        {
            return new Int32Encoder();
        }

        public override IArrowEncoder VisitDoubleType(DoubleType type)
        {
            return new Float64Encoder();
        }

        public override IArrowEncoder VisitFloatType(FloatType type)
        {
            return new Float32Encoder();
        }

        public override IArrowEncoder VisitTimestampType(TimestampType type)
        {
            return new TimestampEncoder();
        }

        public override IArrowEncoder VisitDecimalType(DecimalType type)
        {
            return new DecimalEncoder();
        }

        public override IArrowEncoder VisitDateType(DateType type)
        {
            return new DateEncoder();
        }

        public override IArrowEncoder VisitArrayType(ArrayType type)
        {
            if (type.ElementType == null)
            {
                throw new ArgumentException("ArrayType must have an element type");
            }
            var innerEncoder = Visit(type.ElementType);
            return new ListEncoder(innerEncoder);
        }

        public override IArrowEncoder VisitStructType(StructType type)
        {
            var encoders = new List<IArrowEncoder>();
            List<IDataValue> propertyNames = new List<IDataValue>();
            foreach (var field in type.Fields)
            {
                propertyNames.Add(new StringValue(field.Name));
                encoders.Add(Visit(field.Type));
            }
            return new StructEncoder(encoders, propertyNames);
        }
    }
}
