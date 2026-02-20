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
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Linq;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Storage.DataStructures
{
    internal static class Utils
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ushort HighBits(int value)
        {
            return (ushort)(value >> 16);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ushort LowBits(int value)
        {
            return (ushort)(value & 0xFFFF);
        }

        public static int UnsignedBinarySearch(ushort[] array, int begin, int end, ushort k)
        {
            return Array.BinarySearch(array, begin, end - begin, k);
        }

        public static void FillArray(ulong[] bitmap, ushort[] array)
        {
            int pos = 0;
            int b = 0;
            for (int k = 0; k < bitmap.Length; ++k)
            {
                ulong bitset = bitmap[k];
                while (bitset != 0)
                {
                    array[pos++] = (ushort)(b + BitOperations.TrailingZeroCount(bitset));
                    bitset &= (bitset - 1);
                }
                b += 64;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void WriteInt(ref readonly IBufferWriter<byte> writer, in int value)
        {
            Span<byte> span = writer.GetSpan(4);
            BinaryPrimitives.WriteInt32LittleEndian(span, value);
            writer.Advance(4);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void WriteUshort(ref readonly IBufferWriter<byte> writer, in ushort value)
        {
            Span<byte> span = writer.GetSpan(2);
            BinaryPrimitives.WriteUInt16LittleEndian(span, value);
            writer.Advance(2);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void WriteLong(ref readonly IBufferWriter<byte> writer, in long value)
        {
            Span<byte> span = writer.GetSpan(8);
            BinaryPrimitives.WriteInt64LittleEndian(span, value);
            writer.Advance(8);
        }

        public static void WriteULong(ref readonly IBufferWriter<byte> writer, in ulong value)
        {
            Span<byte> span = writer.GetSpan(8);
            BinaryPrimitives.WriteUInt64LittleEndian(span, value);
            writer.Advance(8);
        }

        public static uint ReadUint32(ref SequenceReader<byte> reader)
        {
            if (reader.TryReadLittleEndian(out int value))
            {
                return (uint)value;
            }
            throw new InvalidOperationException("Failed to read uint32 from the reader.");
        }

        public static byte[] ReadBytes(ref SequenceReader<byte> reader, int count)
        {
            byte[] result = new byte[count];
            if (reader.TryCopyTo(result))
            {
                reader.Advance(count);
                return result;
            }
            throw new InvalidOperationException($"Failed to read {count} bytes from the reader.");
        }

        public static ushort ReadUshort(ref SequenceReader<byte> reader)
        {
            if (reader.TryReadLittleEndian(out short value))
            {
                return (ushort)value;
            }
            throw new InvalidOperationException("Failed to read ushort from the reader.");
        }

        public static ulong ReadUint64(ref SequenceReader<byte> reader)
        {
            if (reader.TryReadLittleEndian(out long value))
            {
                return (ulong)value;
            }
            throw new InvalidOperationException("Failed to read uint64 from the reader.");
        }
    }
}
