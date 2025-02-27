using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json.Serialization;
using System.Threading.Tasks;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema.Types
{
    internal class StructType : SchemaBaseType, IEquatable<StructType>
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

        public bool Equals(StructType? other)
        {
            if (other == null)
            {
                return false;
            }

            if (Fields.Count != other.Fields.Count)
            {
                return false;
            }

            for (int i = 0; i < Fields.Count; i++)
            {
                if (!Fields[i].Equals(other.Fields[i]))
                {
                    return false;
                }
            }

            return true;
        }

        public override bool Equals(object? obj)
        {
            return Equals(obj as StructType);
        }

        public override int GetHashCode()
        {
            return Type.GetHashCode();
        }
    }
}
