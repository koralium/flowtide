using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json.Serialization;
using System.Threading.Tasks;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema.Types
{
    internal class StructType : SchemaBaseType
    {
        public override SchemaType Type => SchemaType.Struct;

        [JsonPropertyName("fields")]
        public IReadOnlyList<StructField> Fields { get; set; }

        public StructType(IReadOnlyList<StructField> fields)
        {
            Fields = fields;
        }

        public override T Accept<T>(DeltaTypeVisitor<T> visitor)
        {
            return visitor.VisitStructType(this);
        }
    }
}
