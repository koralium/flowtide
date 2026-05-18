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

using FlowtideDotNet.Core.ColumnStore.Sort;
using FlowtideDotNet.Core.ColumnStore.Utils;
using System.Numerics;

namespace FlowtideDotNet.Core.ColumnStore.BoundarySearching
{
    internal static class BoundarySearchString
    {
        public unsafe static void RunStringBoundarySearchNoNull(
        IColumn treeColumn,
        IColumn inputColumn,
        ReadOnlySpan<int> inputSortedLookup,
        Span<int> lowerBounds,
        Span<int> upperBounds,

        // Not used parameters
        DataValueContainer xContainer,
        DataValueContainer yContainer,
        bool doNotMatchNull,
        Span<int> buffer)
        {
            // 1. Fetch Pointers
            SelfComparePointers treePointers = default;
            treeColumn.SetSelfComparePointers(ref treePointers);

            SelfComparePointers inputPointers = default;
            inputColumn.SetSelfComparePointers(ref inputPointers);

            // 2. Initialize the struct comparer with native pointers
            BoundaryStringViewComparer comparer = new BoundaryStringViewComparer
            {
                TreeViews = (BinaryViewList.ArrowBinaryView*)treePointers.secondaryPointer,
                TreeData = (byte*)treePointers.dataPointer,
                InputViews = (BinaryViewList.ArrowBinaryView*)inputPointers.secondaryPointer,
                InputData = (byte*)inputPointers.dataPointer
            };

            // 3. Execute the strictly typed generic algorithm
            BoundarySearchHybrid<BoundaryStringViewComparer>.SearchBoundaries(
                ref comparer, inputSortedLookup, lowerBounds, upperBounds);
        }

        internal unsafe static void BoundarySearchStringNoNullWithInputOffsets(
            IColumn treeColumn,
            IColumn inputColumn,
            ReadOnlySpan<int> inputSortedLookup,
            Span<int> lowerBounds,
            Span<int> upperBounds,
            DataValueContainer xContainer,
            DataValueContainer yContainer,
            bool doNotMatchNull,
            Span<int> buffer
            )
        {
            SelfComparePointers treePointers = default;
            treeColumn.SetSelfComparePointers(ref treePointers);

            SelfComparePointers inputPointers = default;
            inputColumn.SetSelfComparePointers(ref inputPointers);

            int* inputOffsets = (int*)inputPointers.columnOffsetsPointer;

            for (int i = 0; i < inputSortedLookup.Length; i++)
            {
                int lookupIndex = inputSortedLookup[i];
                int idx = inputOffsets[lookupIndex];

                if (idx < 0 && lowerBounds[i] >= 0)
                {
                    lowerBounds[i] = ~lowerBounds[i];
                    upperBounds[i] = lowerBounds[i];
                }

                buffer[i] = idx;
            }

            BoundaryStringViewComparer comparer = new BoundaryStringViewComparer
            {
                TreeViews = (BinaryViewList.ArrowBinaryView*)treePointers.secondaryPointer,
                TreeData = (byte*)treePointers.dataPointer,
                InputViews = (BinaryViewList.ArrowBinaryView*)inputPointers.secondaryPointer,
                InputData = (byte*)inputPointers.dataPointer
            };

            BoundarySearchHybrid<BoundaryStringViewComparer>.SearchBoundaries(
                ref comparer, buffer, lowerBounds, upperBounds);
        }
    }
}
