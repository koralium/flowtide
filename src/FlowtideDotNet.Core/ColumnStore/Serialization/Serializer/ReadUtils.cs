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

using System.Buffers.Binary;
using System.Runtime.InteropServices;

namespace FlowtideDotNet.Core.ColumnStore.Serialization.Serializer
{
    internal static class ReadUtils
    {
        public static int __offset(scoped ref readonly ReadOnlySpan<byte> span, scoped ref readonly int position, int vtableOffset)
        {
            int vtable = position - GetInt(in span, position);
            return vtableOffset < GetShort(in span, vtable) ? (int)GetShort(in span, vtable + vtableOffset) : 0;
        }

        public static sbyte GetSbyte(scoped ref readonly ReadOnlySpan<byte> span, int index)
        {
            return (sbyte)span[index];
        }

        public static int GetInt(scoped ref readonly ReadOnlySpan<byte> span, int offset)
        {
            ReadOnlySpan<byte> readSpan = span.Slice(offset);
            return BinaryPrimitives.ReadInt32LittleEndian(readSpan);
        }

        public static byte Get(scoped ref readonly ReadOnlySpan<byte> span, int index)
        {
            return span[index];
        }

        public static int __indirect(scoped ref readonly ReadOnlySpan<byte> span, int offset)
        {
            return offset + GetInt(in span, offset);
        }

        public static short GetShort(ref readonly ReadOnlySpan<byte> data, int position)
        {
            ReadOnlySpan<byte> span = data.Slice(position);
            return BinaryPrimitives.ReadInt16LittleEndian(span);
        }

        public static ReadOnlySpan<T> __vector_as_span<T>(scoped ref readonly ReadOnlySpan<byte> span, scoped ref readonly int position, int offset, int elementSize) where T : struct
        {
            if (!BitConverter.IsLittleEndian)
            {
                throw new NotSupportedException("Getting typed span on a Big Endian " +
                                                "system is not support");
            }

            var o = __offset(in span, in position, offset);
            if (0 == o)
            {
                return new Span<T>();
            }

            var pos = __vector(in span, in position, o);
            var len = __vector_len(in span, in position, o);
            var var = MemoryMarshal.Cast<byte, T>(span.Slice(pos, len * elementSize));
            return var;
        }

        public static int __vector(ref readonly ReadOnlySpan<byte> span, ref readonly int position, int offset)
        {
            offset += position;
            return offset + GetInt(in span, offset) + sizeof(int);  // data starts after the length
        }

        public static int __vector_len(ref readonly ReadOnlySpan<byte> span, ref readonly int position, int offset)
        {
            offset += position;
            offset += GetInt(in span, offset);
            return GetInt(in span, offset);
        }

        public static long GetLong(scoped ref readonly ReadOnlySpan<byte> data, int offset)
        {
            ReadOnlySpan<byte> span = data.Slice(offset);
            return BitConverter.ToInt64(span);
        }
    }
}
