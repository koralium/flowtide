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
        private readonly Span<byte> span;
        private readonly int position;

        public BufferStruct(Span<byte> span, int position)
        {
            this.span = span;
            this.position = position;
        }

        public long Length
        {
            get
            {
                return GetLong(position + 8);
            }
        }

        public long Offset
        {
            get
            {
                return GetLong(position + 0);
            }
        }

        public void SetLength(long value)
        {
            BinaryPrimitives.WriteInt64LittleEndian(span.Slice(position + 8), value);
        }

        public void SetOffset(long value)
        {
            BinaryPrimitives.WriteInt64LittleEndian(span.Slice(position + 0), value);
        }

        private long GetLong(int offset)
        {
            ReadOnlySpan<byte> span = this.span.Slice(offset);
            return BitConverter.ToInt64(span);
        }
    }
}
