using FlowtideDotNet.Core.ColumnStore.Serialization.Serializer;
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
        private readonly ReadOnlySpan<byte> span;
        private readonly int position;

        public SchemaStruct(ReadOnlySpan<byte> span, int position)
        {
            this.span = span;
            this.position = position;
        }

        public short Endianness
        {
            get
            {
                int o = ReadUtils.__offset(in span, in position, 4);
                return o != 0 ? ReadUtils.GetShort(in span, o + position) : (short)0;
            }
        }

        public int FieldsLength 
        { 
            get 
            { 
                int o = ReadUtils.__offset(in span, in position, 6);
                return o != 0 ? ReadUtils.__vector_len(in span, in position, o) : 0; 
            } 
        }

        public FieldStruct Fields(int j) 
        { 
            int o = ReadUtils.__offset(in span, in position, 6);
            var fieldLocation = ReadUtils.__indirect(in span, ReadUtils.__vector(in span, in position, o) + j * 4);
            return new FieldStruct(span, fieldLocation);
        }

        public int FieldPosition(int j)
        {
            int o = ReadUtils.__offset(in span, in position, 6);
            var fieldLocation = ReadUtils.__indirect(in span, ReadUtils.__vector(in span, in position, o) + j * 4);
            return fieldLocation;
        }
    }
}
