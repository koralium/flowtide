using Apache.Arrow;
using SqlParser.Ast;
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
        private int m_bufferBodyLength = 0;

        public int AddBuffer(long bufferLength)
        {
            Prep(8, 16);
            PutLong(bufferLength);
            PutLong(m_bufferBodyLength);

            int paddedLength = checked((int)BitUtility.RoundUpToMultipleOf8(bufferLength));
            m_bufferBodyLength += paddedLength;
            return Offset;
        }

        public int CreateBuffer(long offset, long length)
        {
            Prep(8, 16);
            PutLong(length);
            PutLong(offset);
            return Offset;
        }
    }
}
