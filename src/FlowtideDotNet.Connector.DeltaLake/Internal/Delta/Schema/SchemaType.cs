using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema
{
    internal enum SchemaType
    {
        String,
        Long,
        Integer,
        Short,
        Byte,
        Float,
        Double,
        Decimal,
        Boolean,
        Binary,
        Date,
        Timestamp,

        Struct,
        Array,
        Map
    }
}
