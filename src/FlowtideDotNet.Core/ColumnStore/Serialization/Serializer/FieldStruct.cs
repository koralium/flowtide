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
    internal ref struct FieldStruct
    {
        private readonly ReadOnlySpan<byte> span;
        private readonly int position;

        public FieldStruct(ReadOnlySpan<byte> span, int position)
        {
            this.span = span;
            this.position = position;
        }

        public bool Nullable
        {
            get
            {
                int o = ReadUtils.__offset(in span, in position, 6);
                return o != 0 ? 0 != ReadUtils.Get(in span, o + position) : (bool)false;
            }
        }

        public ArrowType TypeType
        {
            get
            {
                int o = ReadUtils.__offset(in span, in position, 8);
                return o != 0 ? (ArrowType)ReadUtils.Get(in span, o + position) : 0;
            }
        }

        public int CustomMetadataLength
        {
            get
            {
                int o = ReadUtils.__offset(in span, in position, 16);
                return o != 0 ? ReadUtils.__vector_len(in span, in position, o) : 0;
            }
        }

        public KeyValueStruct CustomMetadata(int j)
        {
            int o = ReadUtils.__offset(in span, in position, 16);
            var pos = ReadUtils.__indirect(in span, ReadUtils.__vector(in span, in position, o) + j * 4);
            return new KeyValueStruct(span, pos);
        }

        public int ChildrenLength
        {
            get
            {
                int o = ReadUtils.__offset(in span, in position, 14);
                return o != 0 ? ReadUtils.__vector_len(in span, in position, o) : 0;
            }
        }

        public FieldStruct Children(int j)
        {
            int o = ReadUtils.__offset(in span, in position, 14);
            return new FieldStruct(span, ReadUtils.__indirect(in span, ReadUtils.__vector(in span, in position, o) + j * 4));
        }

        public ReadOnlySpan<byte> GetNameBytes()
        {
            return ReadUtils.__vector_as_span<byte>(in span, in position, 4, 1);
        }

        public TypeUtf8Struct TypeAsUtf8()
        {
            int o = ReadUtils.__offset(in span, in position, 10);
            return new TypeUtf8Struct(span, ReadUtils.__indirect(in span, position + o));
        }

        public TypeIntStruct TypeAsInt()
        {
            int o = ReadUtils.__offset(in span, in position, 10);
            return new TypeIntStruct(span, ReadUtils.__indirect(in span, position + o));
        }
    }
}
