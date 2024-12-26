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

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.ColumnStore.Serialization.Serializer
{
    internal ref struct TypeIntStruct
    {
        private readonly Span<byte> span;
        private readonly int position;

        public TypeIntStruct(Span<byte> span, int position)
        {
            this.span = span;
            this.position = position;
        }

        public int BitWidth 
        { 
            get 
            { 
                int o = __offset(4); 
                return o != 0 ? GetInt(in span, o + position) : (int)0; 
            } 
        }

        public bool IsSigned 
        { 
            get 
            { 
                int o = __offset(6); 
                return o != 0 ? 0 != Get(o + position) : (bool)false; 
            } 
        }


        public static int GetInt(ref readonly Span<byte> span, in int offset)
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

        public byte Get(int index)
        {
            return span[index];
        }

    }
}
