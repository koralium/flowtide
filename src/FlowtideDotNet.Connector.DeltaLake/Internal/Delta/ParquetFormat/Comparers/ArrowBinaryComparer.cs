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
    internal class ArrowBinaryComparer : IArrowComparer
    {
        public int FindOccurance(int toFindIndex, IArrowArray toFindFrom, int searchIndex, int searchLength, IArrowArray toFindIn, int globalOffset, IDeleteVector deleteVector)
        {
            var left = (BinaryArray)toFindFrom;
            var right = (BinaryArray)toFindIn;
            var toSearchValue = left.GetBytes(toFindIndex);
            for (int i = searchIndex; i < searchLength; i++)
            {
                if (deleteVector.Contains(globalOffset + i))
                {
                    continue;
                }
                if (right.GetBytes(i).SequenceEqual(toSearchValue))
                {
                    return i;
                }
            }
            return -1;
        }

        public bool IsEqual(int leftIndex, IArrowArray array, int rightIndex, IArrowArray otherArray)
        {
            var left = (BinaryArray)array;
            var right = (BinaryArray)otherArray;

            return left.GetBytes(leftIndex).SequenceEqual(right.GetBytes(rightIndex));
        }
    }
}
