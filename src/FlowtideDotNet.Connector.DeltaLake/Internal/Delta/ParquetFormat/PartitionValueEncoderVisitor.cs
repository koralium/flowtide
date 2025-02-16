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
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat.ArrowPartitionEncoders;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat.Encoders;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema.Types;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat
{
    internal class PartitionValueEncoderVisitor : DeltaTypeVisitor<IArrowEncoder>
    {
        private readonly string physicalName;

        public PartitionValueEncoderVisitor(string physicalName)
        {
            this.physicalName = physicalName;
        }

        public override IArrowEncoder VisitStringType(StringType type)
        {
            return new StringPartitionEncoder(physicalName);
        }

        public override IArrowEncoder VisitIntegerType(IntegerType type)
        {
            return new IntegerPartitionEncoder(physicalName);
        }

        public override IArrowEncoder VisitBooleanType(BooleanType type)
        {
            return new BoolPartitionEncoder(physicalName);
        }

        public override IArrowEncoder VisitTimestampType(TimestampType type)
        {
            return new TimestampPartitionEncoder(physicalName);
        }

        public override IArrowEncoder VisitDateType(DateType type)
        {
            return new DatePartitionEncoder(physicalName);
        }

        public override IArrowEncoder VisitFloatType(FloatType type)
        {
            return new FloatingPointPartitionEncoder(physicalName);
        }

        public override IArrowEncoder VisitDoubleType(DoubleType type)
        {
            return new FloatingPointPartitionEncoder(physicalName);
        }

        public override IArrowEncoder VisitBinaryType(BinaryType type)
        {
            return new BinaryPartitionEncoder(physicalName);
        }

        public override IArrowEncoder VisitByteType(ByteType type)
        {
            return new IntegerPartitionEncoder(physicalName);
        }

        public override IArrowEncoder VisitLongType(LongType type)
        {
            return new IntegerPartitionEncoder(physicalName);
        }

        public override IArrowEncoder VisitShortType(ShortType type)
        {
            return new IntegerPartitionEncoder(physicalName);
        }

        public override IArrowEncoder VisitDecimalType(DecimalType type)
        {
            return new DecimalPartitionEncoder(physicalName);
        }
    }
}
