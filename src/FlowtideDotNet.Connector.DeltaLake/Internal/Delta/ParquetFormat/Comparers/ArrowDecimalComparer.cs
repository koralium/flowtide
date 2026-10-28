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
using System.Buffers.Binary;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat.Comparers
{
    internal class ArrowDecimalComparer : IArrowComparer
    {
        // Raw 128-bit values at the bound scale
        public bool IsEqual(int leftIndex, IArrowArray array, int rightIndex, IArrowArray otherArray)
        {
            var left = (Decimal128Array)array;
            var right = (Decimal128Array)otherArray;

            if (left.IsNull(leftIndex) || right.IsNull(rightIndex))
            {
                return left.IsNull(leftIndex) && right.IsNull(rightIndex);
            }
            return left.GetBytes(leftIndex).SequenceEqual(right.GetBytes(rightIndex));
        }

        public bool TryHash(IArrowArray? array, int index, ref ulong state)
        {
            if (ArrowHash.IsNull(array, index))
            {
                ArrowHash.Add(ref state, ArrowHash.Null);
                return true;
            }
            var bytes = ((Decimal128Array)array!).GetBytes(index);
            ArrowHash.Add(ref state, BinaryPrimitives.ReadUInt64LittleEndian(bytes));
            ArrowHash.Add(ref state, BinaryPrimitives.ReadUInt64LittleEndian(bytes.Slice(8)));
            return true;
        }

        public void HashColumn(IArrowArray? array, Span<ulong> states, Span<bool> rejected)
        {
            if (array == null)
            {
                ArrowHash.AddNulls(states);
                return;
            }
            var decimals = (Decimal128Array)array;
            var data = decimals.ValueBuffer.Span;
            var bitmap = decimals.NullBitmapBuffer.Span;
            var nulls = decimals.NullCount != 0 && !bitmap.IsEmpty;
            var offset = decimals.Offset;
            for (int i = 0; i < states.Length; i++)
            {
                if (nulls && !BitUtility.GetBit(bitmap, offset + i))
                {
                    states[i] = ArrowHash.Mix(states[i] ^ ArrowHash.Null);
                    continue;
                }
                var bytes = data.Slice((offset + i) * 16, 16);
                states[i] = ArrowHash.Mix(ArrowHash.Mix(states[i] ^ BinaryPrimitives.ReadUInt64LittleEndian(bytes)) ^ BinaryPrimitives.ReadUInt64LittleEndian(bytes.Slice(8)));
            }
        }

        public int CompareCanonical(IArrowArray? left, int leftIndex, IArrowArray? right, int rightIndex)
        {
            return ArrowHash.CompareNulls(left, leftIndex, right, rightIndex)
                ?? BinaryPrimitives.ReadInt128LittleEndian(((Decimal128Array)left!).GetBytes(leftIndex)).CompareTo(BinaryPrimitives.ReadInt128LittleEndian(((Decimal128Array)right!).GetBytes(rightIndex)));
        }
    }
}
