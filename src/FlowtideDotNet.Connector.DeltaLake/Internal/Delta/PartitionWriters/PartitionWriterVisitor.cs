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

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.PartitionWriters
{
    internal class PartitionWriterVisitor : DeltaTypeVisitor<IColumnPartitionWriter>
    {
        public static readonly PartitionWriterVisitor Instance = new PartitionWriterVisitor();

        public override IColumnPartitionWriter VisitBinaryType(BinaryType type)
        {
            return new BinaryColumnPartitionWriter();
        }

        public override IColumnPartitionWriter VisitBooleanType(BooleanType type)
        {
            return new BoolColumnPartitionWriter();
        }

        public override IColumnPartitionWriter VisitIntegerType(IntegerType type)
        {
            return new IntegerColumnPartitionWriter();
        }

        public override IColumnPartitionWriter VisitLongType(LongType type)
        {
            return new IntegerColumnPartitionWriter();
        }

        public override IColumnPartitionWriter VisitByteType(ByteType type)
        {
            return new IntegerColumnPartitionWriter();
        }

        public override IColumnPartitionWriter VisitDateType(DateType type)
        {
            return new DateColumnPartitionWriter();
        }

        public override IColumnPartitionWriter VisitDecimalType(DecimalType type)
        {
            return new DecimalColumnPartitionWriter(type);
        }

        public override IColumnPartitionWriter VisitDoubleType(DoubleType type)
        {
            return new FloatingPointColumnPartitionWriter();
        }

        public override IColumnPartitionWriter VisitFloatType(FloatType type)
        {
            return new FloatingPointColumnPartitionWriter();
        }

        public override IColumnPartitionWriter VisitShortType(ShortType type)
        {
            return new IntegerColumnPartitionWriter();
        }

        public override IColumnPartitionWriter VisitStringType(StringType type)
        {
            return new StringColumnPartitionWriter();
        }

        public override IColumnPartitionWriter VisitTimestampType(TimestampType type)
        {
            return new TimestampColumnPartitionWriter();
        }
    }
}
