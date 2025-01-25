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
