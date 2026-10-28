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

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat.Comparers
{
    internal class ArrowFloat64Comparer : IArrowComparer
    {
        public bool IsEqual(int leftIndex, IArrowArray array, int rightIndex, IArrowArray otherArray)
        {
            var left = (DoubleArray)array;
            var right = (DoubleArray)otherArray;

            return IsSame(left.GetValue(leftIndex), right.GetValue(rightIndex));
        }

        // Engine equality: NaN equals NaN and -0 equals +0, == gives false for NaN
        private static bool IsSame(double? left, double? right)
        {
            if (!left.HasValue)
            {
                return !right.HasValue;
            }
            return right.HasValue && left.Value.Equals(right.Value);
        }

        // -0 hashes as +0, every NaN as one NaN
        public bool TryHash(IArrowArray? array, int index, ref ulong state)
        {
            if (ArrowHash.IsNull(array, index))
            {
                ArrowHash.Add(ref state, ArrowHash.Null);
                return true;
            }
            ArrowHash.Add(ref state, Bits.Of(((DoubleArray)array!).Values[index]));
            return true;
        }

        public void HashColumn(IArrowArray? array, Span<ulong> states, Span<bool> rejected)
        {
            ArrowHash.AddColumn<double, Bits>(array, states);
        }

        // One NaN, and -0 as +0
        private readonly struct Bits : IValueBits<double>
        {
            public static ulong Of(double value) => double.IsNaN(value) ? 0x7FF8000000000000UL : value == 0 ? 0UL : BitConverter.DoubleToUInt64Bits(value);
        }

        // -0 equals +0, every NaN is equal and last
        public int CompareCanonical(IArrowArray? left, int leftIndex, IArrowArray? right, int rightIndex)
        {
            return ArrowHash.CompareNulls(left, leftIndex, right, rightIndex) ?? Compare(((DoubleArray)left!).Values[leftIndex], ((DoubleArray)right!).Values[rightIndex]);
        }

        internal static int Compare(double left, double right)
        {
            if (double.IsNaN(left) || double.IsNaN(right))
            {
                return double.IsNaN(left) == double.IsNaN(right) ? 0 : double.IsNaN(left) ? 1 : -1;
            }
            return left < right ? -1 : left > right ? 1 : 0;
        }
    }
}
