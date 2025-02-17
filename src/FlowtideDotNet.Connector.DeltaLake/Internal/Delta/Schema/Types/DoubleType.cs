using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema.Types
{
    internal class DoubleType : PrimitiveType
    {
        public override SchemaType Type => SchemaType.Double;

        public override T Accept<T>(DeltaTypeVisitor<T> visitor)
        {
            return visitor.VisitDoubleType(this);
        }
    }
}
