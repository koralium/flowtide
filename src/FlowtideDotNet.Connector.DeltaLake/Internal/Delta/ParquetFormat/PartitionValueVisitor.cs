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
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat.PartitionValueEncoders;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema.Types;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat
{
    internal class PartitionValueVisitor : DeltaTypeVisitor<IParquetEncoder>
    {
        private readonly string physicalName;

        public PartitionValueVisitor(string physicalName)
        {
            this.physicalName = physicalName;
        }

        public override IParquetEncoder VisitStringType(StringType type)
        {
            return new StringPartitionEncoder(physicalName);
        }

        public override IParquetEncoder VisitIntegerType(IntegerType type)
        {
            return new IntegerPartitionEncoder(physicalName);
        }

        public override IParquetEncoder VisitBooleanType(BooleanType type)
        {
            return new BoolPartitionEncoder(physicalName);
        }

        public override IParquetEncoder VisitTimestampType(TimestampType type)
        {
            return new TimestampPartitionEncoder(physicalName);
        }

        public override IParquetEncoder VisitDateType(DateType type)
        {
            return new DatePartitionEncoder(physicalName);
        }

        public override IParquetEncoder VisitFloatType(FloatType type)
        {
            return new FloatingPointPartitionEncoder(physicalName);
        }

        public override IParquetEncoder VisitDoubleType(DoubleType type)
        {
            return new FloatingPointPartitionEncoder(physicalName);
        }

        public override IParquetEncoder VisitBinaryType(BinaryType type)
        {
            return new BinaryPartitionEncoder(physicalName);
        }

        public override IParquetEncoder VisitByteType(ByteType type)
        {
            return new IntegerPartitionEncoder(physicalName);
        }

        public override IParquetEncoder VisitLongType(LongType type)
        {
            return new IntegerPartitionEncoder(physicalName);
        }

        public override IParquetEncoder VisitShortType(ShortType type)
        {
            return new IntegerPartitionEncoder(physicalName);
        }

        public override IParquetEncoder VisitDecimalType(DecimalType type)
        {
            return new DecimalPartitionEncoder(physicalName);
        }
    }
}
