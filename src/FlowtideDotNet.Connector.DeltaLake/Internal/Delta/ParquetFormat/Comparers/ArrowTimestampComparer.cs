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
using Apache.Arrow.Types;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat.Comparers
{
    internal class ArrowTimestampComparer : IArrowComparer
    {
        // The same instant, whatever the two units
        public bool IsEqual(int leftIndex, IArrowArray array, int rightIndex, IArrowArray otherArray)
        {
            var left = (TimestampArray)array;
            var right = (TimestampArray)otherArray;

            if (left.IsNull(leftIndex) || right.IsNull(rightIndex))
            {
                return left.IsNull(leftIndex) && right.IsNull(rightIndex);
            }
            return Nanoseconds(left, leftIndex) == Nanoseconds(right, rightIndex);
        }

        // Whole milliseconds, a value between them equals no stored row
        public bool TryHash(IArrowArray? array, int index, ref ulong state)
        {
            if (ArrowHash.IsNull(array, index))
            {
                ArrowHash.Add(ref state, ArrowHash.Null);
                return true;
            }
            var timestamps = (TimestampArray)array!;
            if (!TryMilliseconds(Unit(timestamps), timestamps.Values[index], out var milliseconds))
            {
                return false;
            }
            ArrowHash.Add(ref state, (ulong)milliseconds);
            return true;
        }

        public void HashColumn(IArrowArray? array, Span<ulong> states, Span<bool> rejected)
        {
            if (array == null)
            {
                ArrowHash.AddNulls(states);
                return;
            }
            var timestamps = (TimestampArray)array;
            var unit = Unit(timestamps);
            var values = timestamps.Values;
            var bitmap = timestamps.NullBitmapBuffer.Span;
            var nulls = timestamps.NullCount != 0 && !bitmap.IsEmpty;
            var offset = timestamps.Offset;
            for (int i = 0; i < states.Length; i++)
            {
                if (nulls && !BitUtility.GetBit(bitmap, offset + i))
                {
                    states[i] = ArrowHash.Mix(states[i] ^ ArrowHash.Null);
                }
                else if (TryMilliseconds(unit, values[i], out var milliseconds))
                {
                    states[i] = ArrowHash.Mix(states[i] ^ (ulong)milliseconds);
                }
                else
                {
                    rejected[i] = true;
                }
            }
        }

        private static bool TryMilliseconds(TimeUnit unit, long value, out long milliseconds)
        {
            switch (unit)
            {
                case TimeUnit.Second:
                    if (value > long.MaxValue / 1_000 || value < long.MinValue / 1_000)
                    {
                        milliseconds = 0;
                        return false;
                    }
                    milliseconds = value * 1_000;
                    return true;
                case TimeUnit.Millisecond:
                    milliseconds = value;
                    return true;
                case TimeUnit.Microsecond:
                    milliseconds = value / 1_000;
                    return value % 1_000 == 0;
                default:
                    milliseconds = value / 1_000_000;
                    return value % 1_000_000 == 0;
            }
        }

        // By instant, across units
        public int CompareCanonical(IArrowArray? left, int leftIndex, IArrowArray? right, int rightIndex)
        {
            return ArrowHash.CompareNulls(left, leftIndex, right, rightIndex) ?? Nanoseconds((TimestampArray)left!, leftIndex).CompareTo(Nanoseconds((TimestampArray)right!, rightIndex));
        }

        private static Int128 Nanoseconds(TimestampArray array, int index)
        {
            var scale = Unit(array) switch
            {
                TimeUnit.Second => 1_000_000_000,
                TimeUnit.Millisecond => 1_000_000,
                TimeUnit.Microsecond => 1_000,
                _ => 1
            };
            return (Int128)array.Values[index] * scale;
        }

        private static TimeUnit Unit(TimestampArray array)
        {
            return ((TimestampType)array.Data.DataType).Unit;
        }
    }
}
