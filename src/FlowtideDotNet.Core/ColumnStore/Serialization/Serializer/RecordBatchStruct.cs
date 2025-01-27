using FlowtideDotNet.Core.ColumnStore.Serialization.Serializer;
using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.ColumnStore.Serialization
{
    internal readonly ref struct RecordBatchStruct
    {
        private readonly ReadOnlySpan<byte> span;
        private readonly int position;

        public RecordBatchStruct(ReadOnlySpan<byte> span, int position)
        {
            this.span = span;
            this.position = position;
        }

        public long Length 
        { 
            get 
            { 
                int o = ReadUtils.__offset(in span, in position, 4); 
                return o != 0 ? ReadUtils.GetLong(in span, o + position) : (long)0; 
            } 
        }

        public int NodesLength 
        { 
            get 
            { 
                int o = ReadUtils.__offset(in span, in position, 6); 
                return o != 0 ? ReadUtils.__vector_len(in span, in position, o) : 0; 
            } 
        }

        public FieldNodeStruct Nodes(int j) 
        { 
            int o = ReadUtils.__offset(in span, in position, 6);
            var location = ReadUtils.__vector(in span, in position, o) + j * 16;
            return new FieldNodeStruct(span, location); 
        }

        public int BuffersLength 
        { 
            get 
            { 
                int o = ReadUtils.__offset(in span, in position, 8); 
                return o != 0 ? ReadUtils.__vector_len(in span, in position, o) : 0; 
            } 
        }

        public int BuffersStartIndex
        {
            get
            {
                int o = ReadUtils.__offset(in span, in position, 8);
                var location = ReadUtils.__vector(in span, in position, o);
                return location;
            }
        }

        public BufferStruct Buffers(int j) 
        { 
            int o = ReadUtils.__offset(in span, in position, 8);
            var location = ReadUtils.__vector(in span, in position, o) + j * 16;
            return new BufferStruct(span, location);
        }

        public bool HasCompression
        {
            get
            {
                int o = ReadUtils.__offset(in span, in position, 10);
                return o != 0;
            }
        }

        public BodyCompressionStruct Compression 
        { 
            get 
            { 
                int o = ReadUtils.__offset(in span, in position, 10); 
                if (o != 0)
                {
                    return new BodyCompressionStruct(span, ReadUtils.__indirect(in span, position + o));
                }
                return default;
            } 
        }
    }
}
