using FlowtideDotNet.Core.ColumnStore.Serialization.Serializer;
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
        private readonly ReadOnlySpan<byte> span;
        private readonly int position;

        public FieldNodeStruct(ReadOnlySpan<byte> span, int position)
        {
            this.span = span;
            this.position = position;
        }

        public long Length 
        { 
            get 
            { 
                return ReadUtils.GetLong(in span, position + 0); 
            } 
        }

        public long NullCount 
        { 
            get 
            { 
                return ReadUtils.GetLong(in span, position + 8); 
            } 
        }
    }
}
