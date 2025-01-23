using FlowtideDotNet.Core.ColumnStore.Serialization.Serializer;
using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.ColumnStore.Serialization
{
    internal ref struct BufferStruct
    {
        private readonly ReadOnlySpan<byte> span;
        private readonly int position;

        public BufferStruct(ReadOnlySpan<byte> span, int position)
        {
            this.span = span;
            this.position = position;
        }

        public long Length
        {
            get
            {
                return ReadUtils.GetLong(in span, position + 8);
            }
        }

        public long Offset
        {
            get
            {
                return ReadUtils.GetLong(in span, position + 0);
            }
        }
    }
}
