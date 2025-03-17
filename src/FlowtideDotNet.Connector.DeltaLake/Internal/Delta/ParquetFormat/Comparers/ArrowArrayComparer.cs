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
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.DeletionVectors;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat.Comparers
{
    internal class ArrowArrayComparer : IArrowComparer
    {
        private readonly IArrowComparer _innerComparer;

        public ArrowArrayComparer(IArrowComparer innerComparer)
        {
            _innerComparer = innerComparer;
        }

        public int FindOccurance(int toFindIndex, IArrowArray toFindFrom, int searchIndex, int searchLength, IArrowArray toFindIn, int globalOffset, IDeleteVector deleteVector)
        {
            for (int i = searchIndex; i < searchLength; i++)
            {
                if (deleteVector.Contains(globalOffset + i))
                {
                    continue;
                }
                if (IsEqual(toFindIndex, toFindFrom, i, toFindIn))
                {
                    return i;
                }
            }
            return -1;
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
    }
}
