using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.ColumnStore.Serialization
{
#pragma warning disable CS0282 // There is no defined ordering between fields in multiple declarations of partial struct
    internal ref partial struct ArrowSerializer
#pragma warning restore CS0282 // There is no defined ordering between fields in multiple declarations of partial struct
    {
        public int CreateBuffer(long offset, long length)
        {
            Prep(8, 16);
            PutLong(length);
            PutLong(offset);
            return Offset;
        }
    }
}
