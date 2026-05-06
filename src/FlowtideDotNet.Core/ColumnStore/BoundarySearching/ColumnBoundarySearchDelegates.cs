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
            _delegateCache[GetKeyFromTypeNoNull(ArrowTypeId.Int8, ArrowTypeId.Int8)]    = BoundarySearchHybridPrimitiveNoNull<sbyte>.SearchBoundries_Hybrid;
            _delegateCache[GetKeyFromTypeNoNull(ArrowTypeId.Int16, ArrowTypeId.Int16)]  = BoundarySearchHybridPrimitiveNoNull<short>.SearchBoundries_Hybrid;
            _delegateCache[GetKeyFromTypeNoNull(ArrowTypeId.Int32, ArrowTypeId.Int32)]  = BoundarySearchHybridPrimitiveNoNull<int>.SearchBoundries_Hybrid;
            _delegateCache[GetKeyFromTypeNoNull(ArrowTypeId.Int64, ArrowTypeId.Int64)]  = BoundarySearchHybridPrimitiveNoNull<long>.SearchBoundries_Hybrid;
        }

        private static int GetKeyFromTypeNoNull(ArrowTypeId key1, ArrowTypeId key2)
        {
            return GetKey(CompareColumnStateBuilder.Create(key1), CompareColumnStateBuilder.Create(key2));
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
                int lowerBound = lowerBounds[i];
                int searchEnd = upperBounds[i];

                int searchStart = Math.Max(lowerBound, currentFastForward);

                if (searchStart > searchEnd)
                {
                    if (lowerBound >= 0)
                    {
                        lowerBounds[i] = ~searchStart;
                        upperBounds[i] = ~searchStart;
                    }
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
    }
}
