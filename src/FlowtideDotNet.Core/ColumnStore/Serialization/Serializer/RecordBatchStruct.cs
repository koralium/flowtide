using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.ColumnStore.Serialization
{
    internal ref struct RecordBatchStruct
    {
        private readonly Span<byte> span;
        private readonly int position;

        public RecordBatchStruct(Span<byte> span, int position)
        {
            this.span = span;
            this.position = position;
        }

        public long Length 
        { 
            get 
            { 
                int o = __offset(4); 
                return o != 0 ? GetLong(o + position) : (long)0; 
            } 
        }

        public int NodesLength 
        { 
            get 
            { 
                int o = __offset(6); 
                return o != 0 ? __vector_len(in span, in position, o) : 0; 
            } 
        }

        public FieldNodeStruct Nodes(int j) 
        { 
            int o = __offset(6);
            var location = __vector(o) + j * 16;
            return new FieldNodeStruct(span, location); 
        }

        public int BuffersLength 
        { 
            get 
            { 
                int o = __offset(8); 
                return o != 0 ? __vector_len(in span, in position, o) : 0; 
            } 
        }

        public BufferStruct Buffers(int j) 
        { 
            int o = __offset(8);
            var location = __vector(o) + j * 16;
            return new BufferStruct(span, location);
        }

        private int __offset(int vtableOffset)
        {
            int vtable = position - GetInt(in span, in position);
            return vtableOffset < GetShort(in span, vtable) ? (int)GetShort(in span, vtable + vtableOffset) : 0;
        }

        private long GetLong(int offset)
        {
            ReadOnlySpan<byte> span = this.span.Slice(offset);
            return BinaryPrimitives.ReadInt64LittleEndian(span);
        }

        private static short GetShort(ref readonly Span<byte> data, int position)
        {
            ReadOnlySpan<byte> span = data.Slice(position);
            return BinaryPrimitives.ReadInt16LittleEndian(span);
        }

        private static int GetInt(ref readonly Span<byte> span, ref readonly int offset)
        {
            ReadOnlySpan<byte> readSpan = span.Slice(offset);
            return BinaryPrimitives.ReadInt32LittleEndian(readSpan);
        }

        private static int __vector_len(ref readonly Span<byte> span, ref readonly int position, int offset)
        {
            offset += position;
            offset += GetInt(in span, in offset);
            return GetInt(in span, in offset);
        }

        private int __vector(int offset)
        {
            offset += position;
            return offset + GetInt(in span, in offset) + sizeof(int);  // data starts after the length
        }
    }
}
