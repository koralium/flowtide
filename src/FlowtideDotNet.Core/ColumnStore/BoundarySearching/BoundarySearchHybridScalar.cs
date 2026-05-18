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

using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace FlowtideDotNet.Core.ColumnStore.BoundarySearching
{
    internal unsafe static class BoundarySearchHybrid<TComparer>
    where TComparer : struct, IBoundaryComparer
    {
        struct SearchTask
        {
            public int InputStart, InputEnd;
            public int LeafStart, LeafEnd;
        }

        internal static void SearchBoundaries(
            ref TComparer comparer,
            ReadOnlySpan<int> inputSortedLookup,
            Span<int> lowerBounds,
            Span<int> upperBounds)
        {
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

                for (int groupIdx = 1; groupIdx <= inputCount; groupIdx++)
                {
                    bool isEnd = groupIdx == inputCount;

                    if (isEnd || lowerPtr[groupIdx] != groupLower || upperPtr[groupIdx] != groupUpper)
                    {
                        if (groupLower >= 0)
                        {
                            int chunkInputEnd = groupIdx - 1;
                            int leafSpace = groupUpper - groupLower + 1;

                            if (leafSpace < 128)
                            {
                                RunMicroLinearLoop(
                                    ref comparer, lookupPtr,
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
                                        RunMicroLinearLoop(
                                            ref comparer, lookupPtr,
                                            lowerPtr, upperPtr,
                                            task.InputStart, task.InputEnd,
                                            task.LeafStart, task.LeafEnd);
                                        continue;
                                    }

                                    int inputSpace = task.InputEnd - task.InputStart + 1;
                                    int midInput = task.InputStart + (inputSpace >> 1);
                                    int actualInputTarget = lookupPtr[midInput];

                                    int inputBlockStart = midInput;
                                    while (inputBlockStart > task.InputStart &&
                                           comparer.CompareInputToInput(lookupPtr[inputBlockStart - 1], actualInputTarget) == 0)
                                    {
                                        inputBlockStart--;
                                    }

                                    int inputBlockEnd = midInput;
                                    while (inputBlockEnd < task.InputEnd &&
                                           comparer.CompareInputToInput(lookupPtr[inputBlockEnd + 1], actualInputTarget) == 0)
                                    {
                                        inputBlockEnd++;
                                    }

                                    int blockStartBound = lowerPtr[inputBlockStart];
                                    int blockEndBound = upperPtr[inputBlockEnd];

                                    FindBounds_Pivot(ref comparer, actualInputTarget, blockStartBound, blockEndBound, out int lower, out int upper);

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
        private static void RunMicroLinearLoop(
            ref TComparer comparer, int* lookupPtr,
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

                int actualInputIndex = lookupPtr[i];

                FindBounds_Linear(ref comparer, actualInputIndex, searchStart, searchEnd, out int lower, out int upper);

                lowerPtr[i] = lower;
                upperPtr[i] = upper;

                currentFastForward = lower < 0 ? ~lower : lower;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static void FindBounds_Linear(ref TComparer comparer, int inputIdx, int start, int end, out int lower, out int upper)
        {
            int i = start;

            for (; i <= end; i++)
            {
                int c = comparer.CompareTreeToInput(i, inputIdx);

                if (c == 0) // Exact match
                {
                    lower = i;
                    upper = i;
                    while (upper < end && comparer.CompareTreeToInput(upper + 1, inputIdx) == 0)
                    {
                        upper++;
                    }
                    return;
                }
                if (c > 0) // Tree value > target
                {
                    lower = ~i;
                    upper = ~i;
                    return;
                }
            }

            lower = ~(end + 1); upper = ~(end + 1);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static void FindBounds_Pivot(ref TComparer comparer, int inputIdx, int start, int end, out int lower, out int upper)
        {
            int low = start;
            int high = end;
            int matchIndex = -1;

            while (low <= high)
            {
                int mid = low + ((high - low) >> 1);
                int c = comparer.CompareTreeToInput(mid, inputIdx);

                if (c == 0)
                {
                    matchIndex = mid;
                    break;
                }
                if (c < 0) low = mid + 1;
                else high = mid - 1;
            }

            if (matchIndex == -1)
            {
                lower = ~low; upper = ~low;
                return;
            }

            lower = FindLowerBound_Galloping(ref comparer, inputIdx, matchIndex, start);
            upper = FindUpperBound_Galloping(ref comparer, inputIdx, lower, end);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int FindLowerBound_Galloping(ref TComparer comparer, int inputIdx, int matchIndex, int start)
        {
            int step = 1;
            int maxSearchSpace = matchIndex - start;

            while (step <= maxSearchSpace && comparer.CompareTreeToInput(matchIndex - step, inputIdx) == 0)
            {
                step <<= 1;
            }

            if (step == 1) return matchIndex;

            int low = matchIndex - step;
            if (low < start) low = start;
            int high = matchIndex - (step >> 1);

            int lower = high;

            while (low <= high)
            {
                int mid = low + ((high - low) >> 1);

                if (comparer.CompareTreeToInput(mid, inputIdx) == 0)
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
        private static int FindUpperBound_Galloping(ref TComparer comparer, int inputIdx, int lowerBound, int end)
        {
            int step = 1;
            int maxSearchSpace = end - lowerBound;

            while (step <= maxSearchSpace && comparer.CompareTreeToInput(lowerBound + step, inputIdx) == 0)
            {
                step <<= 1;
            }

            if (step == 1) return lowerBound;

            int low = lowerBound + (step >> 1);
            int high = lowerBound + step;
            if (high > end) high = end;

            int upper = low;

            while (low <= high)
            {
                int mid = low + ((high - low) >> 1);

                if (comparer.CompareTreeToInput(mid, inputIdx) == 0)
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
