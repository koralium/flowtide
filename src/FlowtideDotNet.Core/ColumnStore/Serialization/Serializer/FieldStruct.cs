using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.ColumnStore.Serialization
{
    internal ref struct FieldStruct
    {
        private readonly Span<byte> span;
        private readonly int position;

        public FieldStruct(Span<byte> span, int position)
        {
            this.span = span;
            this.position = position;
        }

        public bool Nullable 
        { 
            get 
            {
                int o = __offset(6); 
                return o != 0 ? 0 != Get(o + position) : (bool)false; 
            } 
        }

        public byte TypeType 
        { 
            get 
            { 
                int o = __offset(8); 
                return o != 0 ? Get(o + position) : (byte)0; 
            } 
        }

        public int CustomMetadataLength 
        { 
            get 
            { 
                int o = __offset(16); 
                return o != 0 ? __vector_len(in span, in position, o) : 0; 
            } 
        }

        public Span<byte> GetNameBytes() 
        { 
            return __vector_as_span<byte>(4, 1);
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

        public Span<T> __vector_as_span<T>(int offset, int elementSize) where T : struct
        {
            if (!BitConverter.IsLittleEndian)
            {
                throw new NotSupportedException("Getting typed span on a Big Endian " +
                                                "system is not support");
            }

            var o = __offset(offset);
            if (0 == o)
            {
                return new Span<T>();
            }

            var pos = __vector(o);
            var len = __vector_len(in span, in position, o);
            return MemoryMarshal.Cast<byte, T>(span.Slice(pos, len * elementSize));
        }

        private int __vector(int offset)
        {
            offset += position;
            return offset + GetInt(in span, in offset) + sizeof(int);  // data starts after the length
        }

        private static int __vector_len(ref readonly Span<byte> span, ref readonly int position, int offset)
        {
            offset += position;
            offset += GetInt(in span, in offset);
            return GetInt(in span, in offset);
        }

        public byte Get(int index)
        {
            return span[index];
        }
    }
}
