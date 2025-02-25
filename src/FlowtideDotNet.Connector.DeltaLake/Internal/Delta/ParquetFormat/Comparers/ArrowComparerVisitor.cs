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
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema.Types;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat.Comparers
{
    internal class ArrowComparerVisitor : DeltaTypeVisitor<IArrowComparer>
    {
        public override IArrowComparer VisitArrayType(ArrayType type)
        {
            if (type.ElementType == null)
            {
                throw new ArgumentNullException(nameof(type.ElementType));
            }
            return new ArrowArrayComparer(Visit(type.ElementType));
        }

        public override IArrowComparer VisitBooleanType(BooleanType type)
        {
            return new ArrowBoolComparer();
        }

        public override IArrowComparer VisitByteType(ByteType type)
        {
            return new ArrowInt8Comparer();
        }

        public override IArrowComparer VisitDateType(DateType type)
        {
            return new ArrowDateComparer();
        }

        public override IArrowComparer VisitDecimalType(DecimalType type)
        {
            return new ArrowDecimalComparer();
        }

        public override IArrowComparer VisitDoubleType(DoubleType type)
        {
            return new ArrowFloat64Comparer();
        }

        public override IArrowComparer VisitFloatType(FloatType type)
        {
            return new ArrowFloat32Comparer();
        }

        public override IArrowComparer VisitIntegerType(IntegerType type)
        {
            return new ArrowInt32Comparer();
        }

        public override IArrowComparer VisitLongType(LongType type)
        {
            return new ArrowInt64Comparer();
        }

        public override IArrowComparer VisitShortType(ShortType type)
        {
            return new ArrowInt16Comparer();
        }

        public override IArrowComparer VisitStringType(StringType type)
        {
            return new ArrowStringComparer();
        }

        public override IArrowComparer VisitStructType(StructType type)
        {
            List<IArrowComparer> propertyComparers = new List<IArrowComparer>();

            foreach(var field in type.Fields)
            {
                propertyComparers.Add(Visit(field.Type));
            }

            return new ArrowStructComparer(propertyComparers);
        }

        public override IArrowComparer VisitTimestampType(TimestampType type)
        {
            return new ArrowTimestampComparer();
        }

        public override IArrowComparer VisitBinaryType(BinaryType type)
        {
            return new ArrowBinaryComparer();
        }

        public override IArrowComparer VisitMapType(MapType type)
        {
            var keyComparer = Visit(type.KeyType);
            var valueComparer = Visit(type.ValueType);
            return new ArrowMapComparer(keyComparer, valueComparer);
        }
    }
}
