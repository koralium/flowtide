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
    internal class ArrowBoolComparer : IArrowComparer
    {
        public bool IsEqual(int leftIndex, IArrowArray array, int rightIndex, IArrowArray otherArray)
        {
            var left = (BooleanArray)array;
            var right = (BooleanArray)otherArray;

            return left.GetValue(leftIndex) == right.GetValue(rightIndex);
        }

        public bool TryHash(IArrowArray? array, int index, ref ulong state)
        {
            ArrowHash.Add(ref state, ArrowHash.IsNull(array, index) ? ArrowHash.Null : ((BooleanArray)array!).GetValue(index)!.Value ? 1UL : 0UL);
            return true;
        }

        // False before true
        public int CompareCanonical(IArrowArray? left, int leftIndex, IArrowArray? right, int rightIndex)
        {
            return ArrowHash.CompareNulls(left, leftIndex, right, rightIndex) ?? ((BooleanArray)left!).GetValue(leftIndex)!.Value.CompareTo(((BooleanArray)right!).GetValue(rightIndex)!.Value);
        }
    }
}
