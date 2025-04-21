using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Substrait.Type
{
    public class NullType : SubstraitBaseType
    {
        public static readonly NullType Instance = new NullType();

        public override SubstraitType Type => SubstraitType.Null;
    }
}
