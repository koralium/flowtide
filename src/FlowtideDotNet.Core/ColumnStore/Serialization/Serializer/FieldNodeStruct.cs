using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.ColumnStore.Serialization
{
    internal ref struct FieldNodeStruct
    {
        private readonly Span<byte> span;
        private readonly int position;

        public FieldNodeStruct(Span<byte> span, int position)
        {
            this.span = span;
            this.position = position;
        }

        public long Length 
        { 
            get 
            { 
                return GetLong(position + 0); 
            } 
        }

        public long NullCount 
        { 
            get 
            { 
                return GetLong(position + 8); 
            } 
        }

        private long GetLong(int offset)
        {
            ReadOnlySpan<byte> span = this.span.Slice(offset);
            return BinaryPrimitives.ReadInt64LittleEndian(span);
        }
    }
}
