// Licensed under the Apache License, Version 2.0 (the "License")
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//  
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using FlowtideDotNet.Core.ColumnStore.Serialization.Serializer;

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
