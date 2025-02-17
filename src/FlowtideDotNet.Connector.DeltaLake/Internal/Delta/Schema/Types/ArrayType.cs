using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema.Types
{
    internal class ArrayType : SchemaBaseType
    {
        public override SchemaType Type => SchemaType.Array;

        public SchemaBaseType? ElementType { get; set; }

        public bool ContainsNull { get; set; }

        public override T Accept<T>(DeltaTypeVisitor<T> visitor)
        {
            return visitor.VisitArrayType(this);
        }
    }
}
