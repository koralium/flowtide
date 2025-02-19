using Apache.Arrow.Types;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

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
    }
}
