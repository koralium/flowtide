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
    internal class ArrowArrayComparer : IArrowComparer
    {
        private readonly IArrowComparer _innerComparer;

        public ArrowArrayComparer(IArrowComparer innerComparer)
        {
            _innerComparer = innerComparer;
        }

        public bool IsEqual(int leftIndex, IArrowArray array, int rightIndex, IArrowArray otherArray)
        {
            var left = (ListArray)array;
            var right = (ListArray)otherArray;

            if (left.IsNull(leftIndex) && right.IsNull(rightIndex))
            {
                return true;
            }

            if (left.IsNull(leftIndex) || right.IsNull(rightIndex))
            {
                return false;
            }

            var leftOffset = left.ValueOffsets[leftIndex];
            var rightOffset = right.ValueOffsets[rightIndex];
            var leftLength = left.GetValueLength(leftIndex);
            var rightLength = right.GetValueLength(rightIndex);

            if (leftLength != rightLength)
            {
                return false;
            }

            for (int i = 0; i < leftLength; i++)
            {
                if (!_innerComparer.IsEqual(leftOffset + i, left.Values, rightOffset + i, right.Values))
                {
                    return false;
                }
            }
            return true;
        }

        // The length, then the elements in order
        public bool TryHash(IArrowArray? array, int index, ref ulong state)
        {
            if (ArrowHash.IsNull(array, index))
            {
                ArrowHash.Add(ref state, ArrowHash.Null);
                return true;
            }
            var list = (ListArray)array!;
            var offset = list.ValueOffsets[index];
            var length = list.GetValueLength(index);
            ArrowHash.Add(ref state, (ulong)length);
            for (int i = 0; i < length; i++)
            {
                if (!_innerComparer.TryHash(list.Values, offset + i, ref state))
                {
                    return false;
                }
            }
            return true;
        }

        // Element by element, then by length
        public int CompareCanonical(IArrowArray? left, int leftIndex, IArrowArray? right, int rightIndex)
        {
            var nulls = ArrowHash.CompareNulls(left, leftIndex, right, rightIndex);
            if (nulls != null)
            {
                return nulls.Value;
            }
            var leftList = (ListArray)left!;
            var rightList = (ListArray)right!;
            var leftOffset = leftList.ValueOffsets[leftIndex];
            var rightOffset = rightList.ValueOffsets[rightIndex];
            var leftLength = leftList.GetValueLength(leftIndex);
            var rightLength = rightList.GetValueLength(rightIndex);
            for (int i = 0; i < Math.Min(leftLength, rightLength); i++)
            {
                var compared = _innerComparer.CompareCanonical(leftList.Values, leftOffset + i, rightList.Values, rightOffset + i);
                if (compared != 0)
                {
                    return compared;
                }
            }
            return leftLength.CompareTo(rightLength);
        }
    }
}
