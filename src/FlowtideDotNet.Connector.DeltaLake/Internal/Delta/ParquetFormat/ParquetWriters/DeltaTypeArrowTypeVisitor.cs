using Apache.Arrow;
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

        public override ArrowType VisitIntegerType(Schema.Types.IntegerType type)
        {
            return new Int32Type();
        }

        public override ArrowType VisitStructType(Schema.Types.StructType type)
        {
            List<Field> fields = new List<Field>();
            foreach(var field in type.Fields)
            {
                var arrowType = Visit(field.Type);
                fields.Add(new Field(field.Name, arrowType, field.Nullable));
            }
            return new StructType(fields);
        }
    }
}
