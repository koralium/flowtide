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
using System;
using System.Collections.Generic;
using System.Linq;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.ColumnStore.BoundarySearching
{
    /// <summary>
    /// This class implements a hybrid boundary search algorithm for primitive types without nulls.
    /// It combines binary search with SIMD linear scans for small ranges from the binary search.
    /// The region order is a monomorphized type parameter: only the three ordering comparisons differ
    /// between ascending and descending, the SIMD equality scans, duplicate grouping and the divide and
    /// conquer over the probes are order agnostic since the probes arrive sorted in region order.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <typeparam name="TOrder">The region's sort order.</typeparam>
    internal unsafe static class BoundarySearchHybridPrimitiveNoNull<T, TOrder>
         where T : unmanaged, IComparisonOperators<T, T, bool>
         where TOrder : IBoundaryOrder<T>
    {
        struct SearchTask
        {
            public int InputStart, InputEnd;
            public int LeafStart, LeafEnd;
        }

        internal static void SearchBoundries_Hybrid(
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
            SelfComparePointers treePointers = default;
            treeColumn.SetSelfComparePointers(ref treePointers);

            SelfComparePointers inputPointers = default;
            inputColumn.SetSelfComparePointers(ref inputPointers);

            SearchBoundries_Hybrid_StructInput(
                treePointers, inputPointers, inputSortedLookup, lowerBounds, upperBounds);
        }

        internal static void SearchBoundries_Hybrid_StructInput(
        SelfComparePointers treePointers,
        SelfComparePointers inputPointers,
        ReadOnlySpan<int> inputSortedLookup,
        Span<int> lowerBounds,
        Span<int> upperBounds)
        {
            T* treeData = (T*)treePointers.dataPointer;
            T* inputData = (T*)inputPointers.dataPointer;

            int inputCount = inputSortedLookup.Length;

            if (inputCount == 0) return;

            fixed (int* lookupPtr = &MemoryMarshal.GetReference(inputSortedLookup))
            fixed (int* lowerPtr = &MemoryMarshal.GetReference(lowerBounds))
            fixed (int* upperPtr = &MemoryMarshal.GetReference(upperBounds))
            {
                SearchTask* taskStack = stackalloc SearchTask[64];

                int groupStart = 0;
                int groupLower = lowerPtr[0];
                int groupUpper = upperPtr[0];

                // Iterate through the input to isolate chunks that share the exact same bounds
                for (int groupIdx = 1; groupIdx <= inputCount; groupIdx++)
                {
                    bool isEnd = groupIdx == inputCount;

                    // If we reach the end, or the boundaries shift, we process the isolated chunk
                    if (isEnd || lowerPtr[groupIdx] != groupLower || upperPtr[groupIdx] != groupUpper)
                    {
                        // Only process groups that actually have valid starting bounds
                        if (groupLower >= 0)
                        {
                            int chunkInputEnd = groupIdx - 1;
                            int leafSpace = groupUpper - groupLower + 1;

                            if (leafSpace < 128)
                            {
                                RunMicroSimdLoop(
                                    treeData, inputData, lookupPtr,
                                    lowerPtr, upperPtr,
                                    groupStart, chunkInputEnd,
                                    groupLower, groupUpper);
                            }
                            else
                            {
                                int stackPointer = 0;

                                taskStack[stackPointer++] = new SearchTask
                                {
                                    InputStart = groupStart,
                                    InputEnd = chunkInputEnd,
                                    LeafStart = groupLower,
                                    LeafEnd = groupUpper
                                };

                                while (stackPointer > 0)
                                {
                                    var task = taskStack[--stackPointer];

                                    int taskLeafSpace = task.LeafEnd - task.LeafStart + 1;

                                    if (taskLeafSpace < 128)
                                    {
                                        RunMicroSimdLoop(
                                            treeData, inputData, lookupPtr,
                                            lowerPtr, upperPtr,
                                            task.InputStart, task.InputEnd,
                                            task.LeafStart, task.LeafEnd);
                                        continue;
                                    }

                                    int inputSpace = task.InputEnd - task.InputStart + 1;
                                    int midInput = task.InputStart + (inputSpace >> 1);
                                    T targetValue = inputData[lookupPtr[midInput]];

                                    
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

                                    int blockStartBound = lowerPtr[inputBlockStart];
                                    int blockEndBound = upperPtr[inputBlockEnd];

                                    FindBounds_Pivot(treeData, targetValue, blockStartBound, blockEndBound, out int lower, out int upper);

                                    // Apply bounds for all duplicates
                                    for (int i = inputBlockStart; i <= inputBlockEnd; i++)
                                    {
                                        lowerPtr[i] = lower;
                                        upperPtr[i] = upper;
                                    }

                                    int leftLeafSplit = lower < 0 ? ~lower : lower;
                                    int rightLeafSplit = upper < 0 ? ~upper : upper;

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

                        // Shift tracking to the new bounds group
                        if (!isEnd)
                        {
                            groupStart = groupIdx;
                            groupLower = lowerPtr[groupIdx];
                            groupUpper = upperPtr[groupIdx];
                        }
                    }
                }
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static void RunMicroSimdLoop(
            T* treeData, T* inputData, int* lookupPtr,
            int* lowerPtr, int* upperPtr,
            int inStart, int inEnd,
            int leafStart, int leafEnd)
        {
            int currentFastForward = leafStart;

            for (int i = inStart; i <= inEnd; i++)
            {
                int searchStart = lowerPtr[i];
                if (searchStart < 0) continue;

                searchStart = searchStart > currentFastForward ? searchStart : currentFastForward;
                int searchEnd = upperPtr[i] > leafEnd ? leafEnd : upperPtr[i];

                if (searchStart > searchEnd)
                {
                    lowerPtr[i] = ~searchStart;
                    upperPtr[i] = ~searchStart;
                    continue;
                }

                T targetValue = inputData[lookupPtr[i]];

                FindBounds_SIMD(treeData, targetValue, searchStart, searchEnd, out int lower, out int upper);

                lowerPtr[i] = lower;
                upperPtr[i] = upper;

                currentFastForward = lower < 0 ? ~lower : lower;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static void FindBounds_SIMD(T* data, T target, int start, int end, out int lower, out int upper)
        {
            int i = start;

            int step = Vector256<T>.Count;
            int limit = end - (step - 1);

            if (Vector256.IsHardwareAccelerated)
            {
                Vector256<T> targetVec = Vector256.Create(target);

                for (; i <= limit; i += step)
                {
                    Vector256<T> dataVec = Vector256.Load(data + i);

                    Vector256<T> maskVec = Vector256.Equals(dataVec, targetVec);

                    uint mask = maskVec.ExtractMostSignificantBits();

                    if (mask != 0)
                    {
                        int localStart = BitOperations.TrailingZeroCount(mask);
                        lower = i + localStart;
                        upper = lower;
                        while (upper < end && data[upper + 1] == target) upper++;
                        return;
                    }

                    if (TOrder.SortsAfter(data[i + (step - 1)], target)) break;
                }
            }

            // Scalar fallback
            for (; i <= end; i++)
            {
                T val = data[i];
                if (val == target)
                {
                    lower = i;
                    upper = i;
                    while (upper < end && data[upper + 1] == target) upper++;
                    return;
                }
                if (TOrder.SortsAfter(val, target))
                {
                    lower = ~i; upper = ~i;
                    return;
                }
            }

            lower = ~(end + 1); upper = ~(end + 1);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static void FindBounds_Pivot(T* data, T target, int start, int end, out int lower, out int upper)
        {
            int low = start;
            int high = end;
            int matchIndex = -1;

            // Normal binary search to find any occurrence of the target
            while (low <= high)
            {
                int mid = low + ((high - low) >> 1);
                T midVal = data[mid];

                if (midVal == target)
                {
                    matchIndex = mid;
                    break;
                }
                if (TOrder.SortsAfter(target, midVal)) low = mid + 1;
                else high = mid - 1;
            }

            if (matchIndex == -1)
            {
                lower = ~low; upper = ~low;
                return;
            }

            lower = FindLowerBound_Galloping(data, target, matchIndex, start);

            upper = FindUpperBound_Galloping(data, target, lower, end);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int FindLowerBound_Galloping(T* data, T target, int matchIndex, int start)
        {
            int step = 1;
            int maxSearchSpace = matchIndex - start;

            while (step <= maxSearchSpace && data[matchIndex - step] == target)
            {
                step <<= 1;
            }

            // Quick stop if it did not equal
            if (step == 1) return matchIndex;

            int low = matchIndex - step;
            if (low < start) low = start;
            int high = matchIndex - (step >> 1);

            int lower = high;

            // Binary search to find the first occurrence within the overshot window
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
        private static int FindUpperBound_Galloping(T* data, T target, int lowerBound, int end)
        {
            int step = 1;
            int maxSearchSpace = end - lowerBound;

            // Exponential stride
            while (step <= maxSearchSpace && data[lowerBound + step] == target)
            {
                step <<= 1;
            }

            // Quick stop if it did not equal
            if (step == 1) return lowerBound;

            int low = lowerBound + (step >> 1);
            int high = lowerBound + step;
            if (high > end) high = end;

            int upper = low;

            // Binary search to find the last occurrence within the overshot window
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
