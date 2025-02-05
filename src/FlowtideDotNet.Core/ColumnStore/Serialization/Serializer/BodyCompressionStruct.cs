using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.ColumnStore.Serialization.Serializer
{
    internal ref struct BodyCompressionStruct
    {
        private readonly ReadOnlySpan<byte> span;
        private readonly int position;

        public BodyCompressionStruct(ReadOnlySpan<byte> span, int position)
        {
            this.span = span;
            this.position = position;
        }

        public CompressionType Codec 
        { 
            get 
            { 
                int o = ReadUtils.__offset(in span, in position, 4); 
                return o != 0 ? (CompressionType)ReadUtils.GetSbyte(in span, o + position) : CompressionType.LZ4_FRAME; 
            } 
        }

        public BodyCompressionMethod Method 
        { 
            get 
            { 
                int o = ReadUtils.__offset(in span, in position, 6); 
                return o != 0 ? (BodyCompressionMethod)ReadUtils.GetSbyte(in span, o + position) : BodyCompressionMethod.BUFFER; 
            } 
        }

    }
}
