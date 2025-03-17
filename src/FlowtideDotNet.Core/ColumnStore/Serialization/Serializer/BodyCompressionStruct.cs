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
