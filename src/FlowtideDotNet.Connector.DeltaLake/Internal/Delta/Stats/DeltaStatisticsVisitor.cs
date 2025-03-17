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
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Stats.Parsers;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Stats
{
    internal class DeltaStatisticsVisitor : DeltaTypeVisitor<IStatisticsParser?>
    {
        public override IStatisticsParser? VisitBinaryType(BinaryType type)
        {
            return new BinaryStatisticsParser();
        }

        public override IStatisticsParser? VisitBooleanType(BooleanType type)
        {
            return new BoolStatisticsParser();
        }

        public override IStatisticsParser? VisitDateType(DateType type)
        {
            return new DateStatisticsParser();
        }

        public override IStatisticsParser? VisitDecimalType(DecimalType type)
        {
            return new DecimalStatisticsParser();
        }

        public override IStatisticsParser? VisitFloatType(FloatType type)
        {
            return new FloatStatisticsParser();
        }

        public override IStatisticsParser? VisitDoubleType(DoubleType type)
        {
            return new FloatStatisticsParser();
        }

        public override IStatisticsParser? VisitByteType(ByteType type)
        {
            return new IntegerStatisticsParser();
        }

        public override IStatisticsParser? VisitIntegerType(IntegerType type)
        {
            return new IntegerStatisticsParser();
        }

        public override IStatisticsParser? VisitLongType(LongType type)
        {
            return new IntegerStatisticsParser();
        }

        public override IStatisticsParser? VisitShortType(ShortType type)
        {
            return new IntegerStatisticsParser();
        }

        public override IStatisticsParser? VisitStructType(StructType type)
        {
            Dictionary<string, IStatisticsParser> propertyComparers = new Dictionary<string, IStatisticsParser>(StringComparer.OrdinalIgnoreCase);
            foreach(var field in type.Fields)
            {
                var comparer = Visit(field.Type);
                if (comparer != null)
                {
                    propertyComparers.Add(field.Name, comparer);
                }
            }
            return new StructStatisticsParser(propertyComparers);
        }

        public override IStatisticsParser? VisitStringType(StringType type)
        {
            return new StringStatisticsParser();
        }

        public override IStatisticsParser? VisitTimestampType(TimestampType type)
        {
            return new TimestampStatisticsParser();
        }

        public override IStatisticsParser? VisitArrayType(ArrayType type)
        {
            return null;
        }

        public override IStatisticsParser? VisitMapType(MapType type)
        {
            return null;
        }
    }
}
