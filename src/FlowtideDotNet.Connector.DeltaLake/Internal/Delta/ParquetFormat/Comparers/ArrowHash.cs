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


using Apache.Arrow;
using System.IO.Hashing;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat.Comparers
{
    // A value's bits for the row hash
    internal interface IValueBits<T>
    {
        static abstract ulong Of(T value);
    }

    internal readonly struct WideBits : IValueBits<sbyte>, IValueBits<short>, IValueBits<int>, IValueBits<long>
    {
        public static ulong Of(sbyte value) => (ulong)(long)value;

        public static ulong Of(short value) => (ulong)(long)value;

        public static ulong Of(int value) => (ulong)(long)value;

        public static ulong Of(long value) => (ulong)value;
    }

    // Hashing for delete matching, bytes use a per-process seed
    internal static class ArrowHash
    {
        // Every null hashes to this, its descendants are skipped
        public const ulong Null = 0x9E3779B97F4A7C15UL;

        private static long s_seed = Random.Shared.NextInt64();

        // Tests fix the seed so a failure can be repeated
        internal static void SetSeedForTests(long seed)
        {
            s_seed = seed;
        }

        public static void Add(ref ulong state, ulong value)
        {
            state = Mix(state ^ value);
        }

        public static ulong Bytes(ReadOnlySpan<byte> bytes)
        {
            return XxHash3.HashToUInt64(bytes, s_seed);
        }

        // The 64-bit finalizer of MurmurHash3
        public static ulong Mix(ulong x)
        {
            x ^= x >> 33;
            x *= 0xFF51AFD7ED558CCDUL;
            x ^= x >> 33;
            x *= 0xC4CEB9FE1A85EC53UL;
            x ^= x >> 33;
            return x;
        }

        // A primitive column into each row's state, as TryHash does per row
        public static void AddColumn<T, TBits>(IArrowArray? array, Span<ulong> states)
            where T : struct, IEquatable<T>
            where TBits : IValueBits<T>
        {
            if (array == null)
            {
                AddNulls(states);
                return;
            }
            var primitive = (PrimitiveArray<T>)array;
            var values = primitive.Values;
            var bitmap = primitive.NullBitmapBuffer.Span;
            var nulls = primitive.NullCount != 0 && !bitmap.IsEmpty;
            var offset = primitive.Offset;
            for (int i = 0; i < states.Length; i++)
            {
                var value = nulls && !BitUtility.GetBit(bitmap, offset + i) ? Null : TBits.Of(values[i]);
                states[i] = Mix(states[i] ^ value);
            }
        }

        // A string or binary column into each row's state
        public static void AddBytesColumn(IArrowArray? array, Span<ulong> states)
        {
            if (array == null)
            {
                AddNulls(states);
                return;
            }
            var binary = (BinaryArray)array;
            var offsets = binary.ValueOffsets;
            var data = binary.Values;
            var bitmap = binary.NullBitmapBuffer.Span;
            var nulls = binary.NullCount != 0 && !bitmap.IsEmpty;
            var offset = binary.Offset;
            for (int i = 0; i < states.Length; i++)
            {
                var value = nulls && !BitUtility.GetBit(bitmap, offset + i) ? Null : Bytes(data.Slice(offsets[i], offsets[i + 1] - offsets[i]));
                states[i] = Mix(states[i] ^ value);
            }
        }

        // A missing column is null in every row
        public static void AddNulls(Span<ulong> states)
        {
            for (int i = 0; i < states.Length; i++)
            {
                states[i] = Mix(states[i] ^ Null);
            }
        }

        public static bool IsNull(IArrowArray? array, int index)
        {
            return array == null || array.IsNull(index);
        }

        // Nulls first, null when both values are present
        public static int? CompareNulls(IArrowArray? left, int leftIndex, IArrowArray? right, int rightIndex)
        {
            var leftNull = IsNull(left, leftIndex);
            var rightNull = IsNull(right, rightIndex);
            if (leftNull || rightNull)
            {
                return leftNull == rightNull ? 0 : leftNull ? -1 : 1;
            }
            return null;
        }
    }
}
