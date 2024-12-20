using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.ColumnStore.Serialization
{
    internal ref struct SchemaStruct
    {
        private readonly Span<byte> span;
        private readonly int position;

        public SchemaStruct(Span<byte> span, int position)
        {
            this.span = span;
            this.position = position;
        }

        public short Endianness
        {
            get
            {
                int o = __offset(4);
                return o != 0 ? GetShort(in span, o + position) : (short)0;
            }
        }

        public int FieldsLength 
        { 
            get 
            { 
                int o = __offset(6);
                return o != 0 ? __vector_len(in span, in position, o) : 0; 
            } 
        }

        public FieldStruct Fields(int j) 
        { 
            int o = __offset(6);
            var fieldLocation = __indirect(__vector(o) + j * 4);
            return new FieldStruct(span, fieldLocation);
        }

        public static int GetInt(ref readonly Span<byte> span, ref readonly int offset)
        {
            ReadOnlySpan<byte> readSpan = span.Slice(offset);
            return BinaryPrimitives.ReadInt32LittleEndian(readSpan);
        }

        private int __offset(int vtableOffset)
        {
            int vtable = position - GetInt(in span, in position);
            return vtableOffset < GetShort(in span, vtable) ? (int)GetShort(in span, vtable + vtableOffset) : 0;
        }

        private static short GetShort(ref readonly Span<byte> data, int position)
        {
            ReadOnlySpan<byte> span = data.Slice(position);
            return BinaryPrimitives.ReadInt16LittleEndian(span);
        }

        private static int __vector_len(ref readonly Span<byte> span, ref readonly int position, int offset)
        {
            offset += position;
            offset += GetInt(in span, in offset);
            return GetInt(in span, in offset);
        }

        private int __indirect(int offset)
        {
            return offset + GetInt(in span, in offset);
        }

        private int __vector(int offset)
        {
            offset += position;
            return offset + GetInt(in span, in offset) + sizeof(int);  // data starts after the length
        }
    }
}
