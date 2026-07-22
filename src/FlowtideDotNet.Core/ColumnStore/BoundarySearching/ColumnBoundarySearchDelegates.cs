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
using FlowtideDotNet.Substrait.Relations;
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
            DataValueContainer yContainer,
            bool doNotMatchNull,
            Span<int> buffer);

    internal unsafe static class ColumnBoundarySearchDelegates
    {
        private static readonly Dictionary<int, SearchBoundriesBulkDelegate> _delegateCache = new Dictionary<int, SearchBoundriesBulkDelegate>();

        static ColumnBoundarySearchDelegates()
        {
            _delegateCache[GetKeyFromTypeNoNull(ArrowTypeId.Int8, ArrowTypeId.Int8)]    = BoundarySearchHybridPrimitiveNoNull<sbyte, AscendingBoundaryOrder<sbyte>>.SearchBoundries_Hybrid;
            _delegateCache[GetKeyFromTypeNoNull(ArrowTypeId.Int16, ArrowTypeId.Int16)]  = BoundarySearchHybridPrimitiveNoNull<short, AscendingBoundaryOrder<short>>.SearchBoundries_Hybrid;
            _delegateCache[GetKeyFromTypeNoNull(ArrowTypeId.Int32, ArrowTypeId.Int32)]  = BoundarySearchHybridPrimitiveNoNull<int, AscendingBoundaryOrder<int>>.SearchBoundries_Hybrid;
            _delegateCache[GetKeyFromTypeNoNull(ArrowTypeId.Int64, ArrowTypeId.Int64)]  = BoundarySearchHybridPrimitiveNoNull<long, AscendingBoundaryOrder<long>>.SearchBoundries_Hybrid;
            _delegateCache[GetKeyFromTypeTreeNoNullInputWithOffset(ArrowTypeId.Int8, ArrowTypeId.Int8)] = BoundarySearchPrimitiveNoNullWithInputOffsets<sbyte, AscendingBoundaryOrder<sbyte>>;
            _delegateCache[GetKeyFromTypeTreeNoNullInputWithOffset(ArrowTypeId.Int16, ArrowTypeId.Int16)] = BoundarySearchPrimitiveNoNullWithInputOffsets<short, AscendingBoundaryOrder<short>>;
            _delegateCache[GetKeyFromTypeTreeNoNullInputWithOffset(ArrowTypeId.Int32, ArrowTypeId.Int32)] = BoundarySearchPrimitiveNoNullWithInputOffsets<int, AscendingBoundaryOrder<int>>;
            _delegateCache[GetKeyFromTypeTreeNoNullInputWithOffset(ArrowTypeId.Int64, ArrowTypeId.Int64)] = BoundarySearchPrimitiveNoNullWithInputOffsets<long, AscendingBoundaryOrder<long>>;

            // Descending regions, keyed by the sort bit. No nulls here so both null placements collapse.
            _delegateCache[GetKeyFromTypeNoNullDescending(ArrowTypeId.Int8, ArrowTypeId.Int8)]   = BoundarySearchHybridPrimitiveNoNull<sbyte, DescendingBoundaryOrder<sbyte>>.SearchBoundries_Hybrid;
            _delegateCache[GetKeyFromTypeNoNullDescending(ArrowTypeId.Int16, ArrowTypeId.Int16)] = BoundarySearchHybridPrimitiveNoNull<short, DescendingBoundaryOrder<short>>.SearchBoundries_Hybrid;
            _delegateCache[GetKeyFromTypeNoNullDescending(ArrowTypeId.Int32, ArrowTypeId.Int32)] = BoundarySearchHybridPrimitiveNoNull<int, DescendingBoundaryOrder<int>>.SearchBoundries_Hybrid;
            _delegateCache[GetKeyFromTypeNoNullDescending(ArrowTypeId.Int64, ArrowTypeId.Int64)] = BoundarySearchHybridPrimitiveNoNull<long, DescendingBoundaryOrder<long>>.SearchBoundries_Hybrid;
            _delegateCache[GetKeyFromTypeTreeNoNullInputWithOffsetDescending(ArrowTypeId.Int8, ArrowTypeId.Int8)] = BoundarySearchPrimitiveNoNullWithInputOffsets<sbyte, DescendingBoundaryOrder<sbyte>>;
            _delegateCache[GetKeyFromTypeTreeNoNullInputWithOffsetDescending(ArrowTypeId.Int16, ArrowTypeId.Int16)] = BoundarySearchPrimitiveNoNullWithInputOffsets<short, DescendingBoundaryOrder<short>>;
            _delegateCache[GetKeyFromTypeTreeNoNullInputWithOffsetDescending(ArrowTypeId.Int32, ArrowTypeId.Int32)] = BoundarySearchPrimitiveNoNullWithInputOffsets<int, DescendingBoundaryOrder<int>>;
            _delegateCache[GetKeyFromTypeTreeNoNullInputWithOffsetDescending(ArrowTypeId.Int64, ArrowTypeId.Int64)] = BoundarySearchPrimitiveNoNullWithInputOffsets<long, DescendingBoundaryOrder<long>>;
        }

        private static int GetKeyFromTypeNoNull(ArrowTypeId key1, ArrowTypeId key2)
        {
            return GetKey(CompareColumnStateBuilder.Create(key1), CompareColumnStateBuilder.Create(key2));
        }

        private static int GetKeyFromTypeNoNullDescending(ArrowTypeId key1, ArrowTypeId key2)
        {
            return GetKey(CompareColumnStateBuilder.Create(key1) | CompareColumnState.SortDescending, CompareColumnStateBuilder.Create(key2));
        }

        private static int GetKeyFromTypeTreeNoNullInputWithOffset(ArrowTypeId key1, ArrowTypeId key2)
        {
            var inputCol = CompareColumnStateBuilder.Create(key2);
            inputCol |= CompareColumnState.IsIndirectView;
            var key = GetKey(CompareColumnStateBuilder.Create(key1), inputCol);
            return key;
        }

        private static int GetKeyFromTypeTreeNoNullInputWithOffsetDescending(ArrowTypeId key1, ArrowTypeId key2)
        {
            var inputCol = CompareColumnStateBuilder.Create(key2);
            inputCol |= CompareColumnState.IsIndirectView;
            var key = GetKey(CompareColumnStateBuilder.Create(key1) | CompareColumnState.SortDescending, inputCol);
            return key;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int GetKey(CompareColumnState columnState, CompareColumnState inputState)
        {
            return ((int)columnState << 16) | (int)inputState;
        }

        public static SearchBoundriesBulkDelegate GetDelegate(CompareColumnState columnState, CompareColumnState inputState)
        {
            var key = GetKey(columnState, inputState);
            return GetDelegate(key);
        }

        public static SearchBoundriesBulkDelegate GetDelegate(int key)
        {
            if (_delegateCache.TryGetValue(key, out var del))
            {
                return del;
            }
            return FallbackMethod;
        }

        public static bool TryGetDelegate(int key, [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out SearchBoundriesBulkDelegate? del)
        {
            return _delegateCache.TryGetValue(key, out del);
        }

        internal static void BoundarySearchPrimitiveNoNullWithInputOffsets<T, TOrder>(
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
            where T : unmanaged, IComparisonOperators<T, T, bool>
            where TOrder : IBoundaryOrder<T>
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

            BoundarySearchHybridPrimitiveNoNull<T, TOrder>.SearchBoundries_Hybrid_StructInput(treePointers, inputPointers, buffer, lowerBounds, upperBounds);
        }

        internal static void FallbackMethod(
            IColumn column,
            IColumn inputCol,
            ReadOnlySpan<int> inputSortedLookup,
            Span<int> lowerBounds,
            Span<int> upperBounds,
            DataValueContainer xContainer,
            DataValueContainer yContainer,
            bool doNotMatchNull,
            Span<int> buffer)
        {
            int currentFastForward = 0;

            for (int i = 0; i < inputSortedLookup.Length; i++)
            {
                int lowerBound = lowerBounds[i];
                int searchEnd = upperBounds[i];

                if (lowerBound < 0) continue;

                int searchStart = lowerBound > currentFastForward ? lowerBound : currentFastForward;

                if (searchStart > searchEnd)
                {
                    lowerBounds[i] = ~searchStart;
                    upperBounds[i] = ~searchStart;
                    continue;
                }

                var inputIndex = inputSortedLookup[i];
                inputCol.GetValueAt(inputIndex, xContainer, null);

                if (doNotMatchNull && xContainer.Type == ArrowTypeId.Null)
                {
                    lowerBounds[i] = ~searchStart;
                    upperBounds[i] = ~searchStart;
                    continue;
                }

                var (lower, upper) = column.SearchBoundries(xContainer, searchStart, searchEnd, null);

                lowerBounds[i] = lower;
                upperBounds[i] = upper;

                currentFastForward = lower < 0 ? ~lower : lower;
            }
        }

        /// <summary>
        /// Value based fallback for regions with no specialized delegate, two binary searches per probe.
        /// </summary>
        internal static void FallbackMethodDirected<TCompare>(
            IColumn column,
            IColumn inputCol,
            ReadOnlySpan<int> inputSortedLookup,
            Span<int> lowerBounds,
            Span<int> upperBounds,
            DataValueContainer xContainer,
            DataValueContainer yContainer,
            bool doNotMatchNull,
            Span<int> buffer)
            where TCompare : IDirectedValueCompare
        {
            int currentFastForward = 0;

            for (int i = 0; i < inputSortedLookup.Length; i++)
            {
                int lowerBound = lowerBounds[i];
                int searchEnd = upperBounds[i];

                if (lowerBound < 0) continue;

                int searchStart = lowerBound > currentFastForward ? lowerBound : currentFastForward;

                if (searchStart > searchEnd)
                {
                    lowerBounds[i] = ~searchStart;
                    upperBounds[i] = ~searchStart;
                    continue;
                }

                var inputIndex = inputSortedLookup[i];
                inputCol.GetValueAt(inputIndex, xContainer, null);

                if (doNotMatchNull && xContainer.Type == ArrowTypeId.Null)
                {
                    lowerBounds[i] = ~searchStart;
                    upperBounds[i] = ~searchStart;
                    continue;
                }

                // First position at or after the target in region order.
                int low = searchStart;
                int high = searchEnd;
                while (low <= high)
                {
                    int mid = low + ((high - low) >> 1);
                    column.GetValueAt(mid, yContainer, null);
                    if (TCompare.Compare(yContainer, xContainer) < 0)
                    {
                        low = mid + 1;
                    }
                    else
                    {
                        high = mid - 1;
                    }
                }

                if (low > searchEnd)
                {
                    lowerBounds[i] = ~low;
                    upperBounds[i] = ~low;
                    currentFastForward = low;
                    continue;
                }

                column.GetValueAt(low, yContainer, null);
                if (TCompare.Compare(yContainer, xContainer) != 0)
                {
                    lowerBounds[i] = ~low;
                    upperBounds[i] = ~low;
                    currentFastForward = low;
                    continue;
                }

                // First position strictly after the target gives the upper bound.
                int matchStart = low;
                low = matchStart + 1;
                high = searchEnd;
                while (low <= high)
                {
                    int mid = low + ((high - low) >> 1);
                    column.GetValueAt(mid, yContainer, null);
                    if (TCompare.Compare(yContainer, xContainer) <= 0)
                    {
                        low = mid + 1;
                    }
                    else
                    {
                        high = mid - 1;
                    }
                }

                lowerBounds[i] = matchStart;
                upperBounds[i] = low - 1;
                currentFastForward = matchStart;
            }
        }

        internal static void FallbackMethodWithOperators(
            IColumn column,
            IColumn inputCol,
            ReadOnlySpan<int> inputSortedLookup,
            Span<int> lowerBounds,
            Span<int> upperBounds,
            DataValueContainer xContainer,
            DataValueContainer yContainer,
            bool doNotMatchNull,
            Span<int> buffer,
            JoinComparisonType op)
        {
            for (int i = 0; i < inputSortedLookup.Length; i++)
            {
                int lowerBound = lowerBounds[i];
                int searchEnd = upperBounds[i];

                if (lowerBound < 0) continue;

                int searchStart = lowerBound;

                if (searchStart > searchEnd)
                {
                    lowerBounds[i] = ~searchStart;
                    upperBounds[i] = ~searchStart;
                    continue;
                }

                var inputIndex = inputSortedLookup[i];
                inputCol.GetValueAt(inputIndex, xContainer, null);

                if (doNotMatchNull && xContainer.Type == ArrowTypeId.Null)
                {
                    lowerBounds[i] = ~searchStart;
                    upperBounds[i] = ~searchStart;
                    continue;
                }

                var (lower, upper) = column.SearchBoundries(xContainer, searchStart, searchEnd, null);

                int matchStart;
                int matchEnd;

                if (op == JoinComparisonType.Equal)
                {
                    matchStart = lower;
                    matchEnd = upper;
                }
                else if (op == JoinComparisonType.LessThan)
                {
                    int firstGte = lower >= 0 ? lower : ~lower;
                    matchStart = searchStart;
                    matchEnd = firstGte - 1;
                    if (firstGte <= searchStart)
                    {
                        matchStart = ~searchStart;
                        matchEnd = ~searchStart;
                    }
                }
                else if (op == JoinComparisonType.LessThanOrEqual)
                {
                    int firstGt = lower >= 0 ? upper + 1 : ~lower;
                    matchStart = searchStart;
                    matchEnd = firstGt - 1;
                    if (firstGt <= searchStart)
                    {
                        matchStart = ~searchStart;
                        matchEnd = ~searchStart;
                    }
                }
                else if (op == JoinComparisonType.GreaterThan)
                {
                    int firstGt = lower >= 0 ? upper + 1 : ~lower;
                    matchStart = firstGt;
                    matchEnd = searchEnd;
                    if (firstGt > searchEnd)
                    {
                        matchStart = ~firstGt;
                        matchEnd = ~firstGt;
                    }
                }
                else // GreaterThanOrEqual
                {
                    int firstGte = lower >= 0 ? lower : ~lower;
                    matchStart = firstGte;
                    matchEnd = searchEnd;
                    if (firstGte > searchEnd)
                    {
                        matchStart = ~firstGte;
                        matchEnd = ~firstGte;
                    }
                }

                lowerBounds[i] = matchStart;
                upperBounds[i] = matchEnd;
            }
        }
    }
}
