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
using Microsoft.Win32;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;

namespace FlowtideDotNet.Core.ColumnStore.BoundarySearching
{
    internal delegate void SearchBoundriesBulkDelegate(
            IColumn column,
            IColumn inputCol,
            ReadOnlySpan<int> inputSortedLookup,
            Span<int> lowerBounds,
            Span<int> upperBounds,
            DataValueContainer xContainer,
            DataValueContainer yContainer);

    internal unsafe static class ColumnBoundarySearchDelegates
    {


        private static readonly Dictionary<int, SearchBoundriesBulkDelegate> _delegateCache = new Dictionary<int, SearchBoundriesBulkDelegate>();

        static ColumnBoundarySearchDelegates()
        {
            // Here we would populate the _delegateCache with optimized delegates for specific CompareColumnState configurations.
            // For example:
            // _delegateCache[new CompareColumnState(typeof(int), SortOrder.Ascending)] = OptimizedIntAscendingSearch;
            // _delegateCache[new CompareColumnState(typeof(string), SortOrder.Descending)] = OptimizedStringDescendingSearch;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int GetKey(CompareColumnState columnState, CompareColumnState inputState)
        {
            return ((int)columnState << 16) | (int)inputState;
        }

        public static SearchBoundriesBulkDelegate GetDelegate(CompareColumnState columnState, CompareColumnState inputState)
        {
            var key = GetKey(columnState, inputState);
            if (_delegateCache.TryGetValue(key, out var del))
            {
                return del;
            }
            return FallbackMethod;
        }



        internal static void FallbackMethod(
            IColumn column,
            IColumn inputCol,
            ReadOnlySpan<int> inputSortedLookup,
            Span<int> lowerBounds,
            Span<int> upperBounds,
            DataValueContainer xContainer,
            DataValueContainer yContainer)
        {
            int currentFastForward = 0;

            for (int i = 0; i < inputSortedLookup.Length; i++)
            {
                int searchStart = lowerBounds[i];
                int searchEnd = upperBounds[i];

                searchStart = Math.Max(searchStart, currentFastForward);

                if (searchStart > searchEnd)
                {
                    continue;
                }

                var inputIndex = inputSortedLookup[i];
                inputCol.GetValueAt(inputIndex, xContainer, null);

                var (lower, upper) = column.SearchBoundries(xContainer, searchStart, searchEnd, null);

                lowerBounds[i] = lower;
                upperBounds[i] = upper;

                currentFastForward = lower < 0 ? ~lower : lower;
            }
        }

        struct SearchTask
        {
            public int InputStart, InputEnd;
            public int LeafStart, LeafEnd;
        }

        internal static void SearchBoundries_Hybrid_Int32(
        IColumn treeColumn,
        IColumn inputColumn,
        ReadOnlySpan<int> inputSortedLookup,
        Span<int> lowerBounds,
        Span<int> upperBounds,
        DataValueContainer xContainer, // Not used
        DataValueContainer yContainer) // Not used
        {
            SelfComparePointers treePointers = default;
            treeColumn.SetSelfComparePointers(ref treePointers);
            int* treeData = (int*)treePointers.dataPointer;

            SelfComparePointers inputPointers = default;
            inputColumn.SetSelfComparePointers(ref inputPointers);
            int* inputData = (int*)inputPointers.dataPointer;

            int treeCount = treeColumn.Count;
            int inputCount = inputSortedLookup.Length;

            if (inputCount == 0 || treeCount == 0) return;

            fixed (int* lookupPtr = &MemoryMarshal.GetReference(inputSortedLookup))
            fixed (int* lowerPtr = &MemoryMarshal.GetReference(lowerBounds))
            fixed (int* upperPtr = &MemoryMarshal.GetReference(upperBounds))
            {
                // Zero-allocation L1-cached stack
                SearchTask* taskStack = stackalloc SearchTask[64];
                int stackPointer = 0;

                taskStack[stackPointer++] = new SearchTask
                {
                    InputStart = 0,
                    InputEnd = inputCount - 1,
                    LeafStart = 0,
                    LeafEnd = treeCount - 1
                };

                while (stackPointer > 0)
                {
                    var task = taskStack[--stackPointer];

                    int leafSpace = task.LeafEnd - task.LeafStart + 1;

                    // --- THE CROSSOVER THRESHOLD ---
                    // Only drop to SIMD if the physical memory space of the leaf 
                    // fits perfectly inside the CPU's L1 cache (< 128 elements).
                    if (leafSpace < 128)
                    {
                        RunMicroSimdLoop(
                            treeData, inputData, lookupPtr,
                            lowerPtr, upperPtr,
                            task.InputStart, task.InputEnd,
                            task.LeafStart, task.LeafEnd);
                        continue;
                    }

                    // --- MACRO SPLIT ---
                    int inputSpace = task.InputEnd - task.InputStart + 1;
                    int midInput = task.InputStart + (inputSpace >> 1);
                    int targetValue = inputData[lookupPtr[midInput]];

                    // 1. INPUT BLOCK EXPANSION (Handle Incoming Duplicates)
                    int inputBlockStart = midInput;
                    while (inputBlockStart > task.InputStart && inputData[lookupPtr[inputBlockStart - 1]] == targetValue)
                    {
                        inputBlockStart--;
                    }

                    int inputBlockEnd = midInput;
                    while (inputBlockEnd < task.InputEnd && inputData[lookupPtr[inputBlockEnd + 1]] == targetValue)
                    {
                        inputBlockEnd++;
                    }

                    // 2. MACRO SEARCH: Binary Find + Galloping Bounds (Skew Immunity)
                    FindBoundsInt32_Pivot(treeData, targetValue, task.LeafStart, task.LeafEnd, out int lower, out int upper);

                    // 3. APPLY BOUNDS TO ENTIRE DUPLICATE BLOCK
                    for (int i = inputBlockStart; i <= inputBlockEnd; i++)
                    {
                        lowerPtr[i] = lower;
                        upperPtr[i] = upper;
                    }

                    int leftLeafSplit = lower < 0 ? ~lower : lower;
                    int rightLeafSplit = upper < 0 ? ~upper : upper;

                    // 4. PUSH RIGHT HALF (Everything strictly after the duplicate block)
                    if (inputBlockEnd + 1 <= task.InputEnd)
                    {
                        taskStack[stackPointer++] = new SearchTask
                        {
                            InputStart = inputBlockEnd + 1,
                            InputEnd = task.InputEnd,
                            LeafStart = rightLeafSplit,
                            LeafEnd = task.LeafEnd
                        };
                    }

                    // 5. PUSH LEFT HALF (Everything strictly before the duplicate block)
                    if (inputBlockStart - 1 >= task.InputStart)
                    {
                        taskStack[stackPointer++] = new SearchTask
                        {
                            InputStart = task.InputStart,
                            InputEnd = inputBlockStart - 1,
                            LeafStart = task.LeafStart,
                            LeafEnd = leftLeafSplit
                        };
                    }
                }
            }
        }

        // =========================================================
        // 3. THE MICRO AVX2 LOOP (< 128 Elements)
        // =========================================================
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static void RunMicroSimdLoop(
            int* treeData, int* inputData, int* lookupPtr,
            int* lowerPtr, int* upperPtr,
            int inStart, int inEnd,
            int leafStart, int leafEnd)
        {
            int currentFastForward = leafStart;

            for (int i = inStart; i <= inEnd; i++)
            {
                int searchStart = lowerPtr[i];
                if (searchStart < 0) continue; // Cascaded dead row

                searchStart = searchStart > currentFastForward ? searchStart : currentFastForward;
                int searchEnd = upperPtr[i] > leafEnd ? leafEnd : upperPtr[i];

                if (searchStart > searchEnd) continue;

                int targetValue = inputData[lookupPtr[i]];

                FindBoundsInt32_Avx2(treeData, targetValue, searchStart, searchEnd, out int lower, out int upper);

                lowerPtr[i] = lower;
                upperPtr[i] = upper;

                currentFastForward = lower < 0 ? ~lower : lower;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static void FindBoundsInt32_Avx2(int* data, int target, int start, int end, out int lower, out int upper)
        {
            int i = start;
            int limit = end - 7;

            if (Avx2.IsSupported)
            {
                Vector256<int> targetVec = Vector256.Create(target);

                for (; i <= limit; i += 8)
                {
                    Vector256<int> dataVec = Avx2.LoadVector256(data + i);
                    Vector256<int> maskVec = Avx2.CompareEqual(dataVec, targetVec);
                    int mask = Avx2.MoveMask(maskVec.AsByte());

                    if (mask != 0)
                    {
                        // Match found! Decode bitmask to find array offset
                        int localStart = BitOperations.TrailingZeroCount((uint)mask) >> 2;
                        lower = i + localStart;
                        upper = lower;
                        while (upper < end && data[upper + 1] == target) upper++;
                        return;
                    }

                    if (data[i + 7] > target) break; // Sorted overshoot
                }
            }

            // Scalar Tail / Fallback
            for (; i <= end; i++)
            {
                int val = data[i];
                if (val == target)
                {
                    lower = i;
                    upper = i;
                    while (upper < end && data[upper + 1] == target) upper++;
                    return;
                }
                if (val > target)
                {
                    lower = ~i; upper = ~i;
                    return;
                }
            }

            lower = ~(end + 1); upper = ~(end + 1);
        }

        // =========================================================
        // 4. THE MACRO SKEW ANNIHILATOR (Binary + Galloping)
        // =========================================================
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static void FindBoundsInt32_Pivot(int* data, int target, int start, int end, out int lower, out int upper)
        {
            int low = start;
            int high = end;
            int matchIndex = -1;

            // 1. Binary Search to find any occurrence
            while (low <= high)
            {
                int mid = low + ((high - low) >> 1);
                int midVal = data[mid];

                if (midVal == target)
                {
                    matchIndex = mid;
                    break;
                }
                if (midVal < target) low = mid + 1;
                else high = mid - 1;
            }

            if (matchIndex == -1)
            {
                lower = ~low; upper = ~low;
                return;
            }

            // Galloping Search for Lower Bound (O(log K) immunity to duplicates)
            lower = FindLowerBound_Galloping(data, target, matchIndex, start);

            // 2. Galloping Search for Upper Bound (O(log K) immunity to duplicates)
            upper = FindUpperBound_Galloping(data, target, lower, end);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int FindLowerBound_Galloping(int* data, int target, int matchIndex, int start)
        {
            int step = 1;
            int maxSearchSpace = matchIndex - start;

            // Exponential stride backwards
            while (step <= maxSearchSpace && data[matchIndex - step] == target)
            {
                step <<= 1;
            }

            // No duplicate below — matchIndex is already the lower bound (single comparison)
            if (step == 1) return matchIndex;

            int low = matchIndex - step;
            if (low < start) low = start;
            int high = matchIndex - (step >> 1);

            int lower = high;

            // Micro-binary search within the overshot window
            while (low <= high)
            {
                int mid = low + ((high - low) >> 1);

                if (data[mid] == target)
                {
                    lower = mid;
                    high = mid - 1;
                }
                else
                {
                    low = mid + 1;
                }
            }
            return lower;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int FindUpperBound_Galloping(int* data, int target, int lowerBound, int end)
        {
            int step = 1;
            int maxSearchSpace = end - lowerBound;

            // Exponential stride
            while (step <= maxSearchSpace && data[lowerBound + step] == target)
            {
                step <<= 1;
            }

            // No duplicate above — lowerBound is already the upper bound (single comparison)
            if (step == 1) return lowerBound;

            int low = lowerBound + (step >> 1);
            int high = lowerBound + step;
            if (high > end) high = end;

            int upper = low;

            // Micro-binary search within the overshot window
            while (low <= high)
            {
                int mid = low + ((high - low) >> 1);

                if (data[mid] == target)
                {
                    upper = mid;
                    low = mid + 1;
                }
                else
                {
                    high = mid - 1;
                }
            }
            return upper;
        }

    }
}
