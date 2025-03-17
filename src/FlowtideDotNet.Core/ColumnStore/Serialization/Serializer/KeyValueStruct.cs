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

namespace FlowtideDotNet.Core.ColumnStore.Serialization.Serializer
{
    internal ref struct KeyValueStruct
    {
        private readonly ReadOnlySpan<byte> span;
        private readonly int position;

        public KeyValueStruct(ReadOnlySpan<byte> span, int position)
        {
            this.span = span;
            this.position = position;
        }

        public ReadOnlySpan<byte> KeyBytes
        {
            get
            {
                return ReadUtils.__vector_as_span<byte>(in span, in position, 4, 1);
            }
        }

        public ReadOnlySpan<byte> ValueBytes
        {
            get
            {
                return ReadUtils.__vector_as_span<byte>(in span, in position, 6, 1);
            }
        }

        public int KeyLength
        {
            get
            {
                int o = ReadUtils.__offset(in span, in position, 4);
                return o != 0 ? ReadUtils.__vector_len(in span, in position, o) : 0;
            }
        }

        public int ValueLength
        {
            get
            {
                int o = ReadUtils.__offset(in span, in position, 6);
                return o != 0 ? ReadUtils.__vector_len(in span, in position, o) : 0;
            }
        }
    }
}
