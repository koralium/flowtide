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

using FlowtideDotNet.Core.ColumnStore.Utils;
using System.Runtime.CompilerServices;

namespace FlowtideDotNet.Core.ColumnStore.BoundarySearching
{
    internal unsafe struct BoundaryStringComparer : IBoundaryComparer
    {
        public int* TreeOffsets;
        public byte* TreeData;

        public int* InputOffsets;
        public byte* InputData;

        [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
        public int CompareTreeToInput(int treeIndex, int inputIndex)
        {
            return CompareViews(
                TreeOffsets, treeIndex, TreeData,
                InputOffsets, inputIndex, InputData);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
        private static int CompareViews(
            int* offsetsX, int indexX, byte* dataX,
            int* offsetsY, int indexY, byte* dataY)
        {
            var xStart = offsetsX[indexX];
            var xEnd = offsetsX[indexX + 1];
            var xData = new ReadOnlySpan<byte>(dataX + xStart, xEnd - xStart);
            var yStart = offsetsY[indexY];
            var yEnd = offsetsY[indexY + 1];
            var yData = new ReadOnlySpan<byte>(dataY + yStart, yEnd - yStart);

            return xData.SequenceCompareTo(yData);
        }

        public int CompareInputToInput(int inputIndexA, int inputIndexB)
        {
            return CompareViews(
            InputOffsets, inputIndexA, InputData,
            InputOffsets, inputIndexB, InputData);
        }
    }
}
