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

using FlowtideDotNet.Core.ColumnStore.Comparers;
using FlowtideDotNet.Core.ColumnStore.DataColumns;
using FlowtideDotNet.Core.ColumnStore.Utils;
using FlowtideDotNet.Substrait.Expressions;

namespace FlowtideDotNet.Core.ColumnStore.TreeStorage
{
    internal static class BoundarySearch
    {
        public static (int, int) SearchBoundriesForColumn<T>(in UnionColumn column, in T value, in int index, in int end)
            where T: IDataValue
        {
            int lo = index;
            int hi = end;
            int maxNotFound = hi;
            
            bool found = false;
            while (lo <= hi)
            {
                int i = lo + ((hi - lo) >> 1);

                
                int c = column.CompareTo(i, value, default, default);
                if (c == 0)
                {
                    found = true;
                    hi = i - 1;
                }
                else if (c < 0)
                {
                    lo = i + 1;
                }
                else
                {
                    hi = i - 1;
                    maxNotFound = hi;
                }
            }
            int lowerbound = lo;
            if (!found)
            {
                lowerbound = ~lo;
                // We did not find the value so this is the the bounds.
                return (lowerbound, lowerbound);
            }

            if (lo < end)
            {
                // Check that the next value is the same, if not we are at the of the bounds.
                int c = column.CompareTo(lo + 1, value, default, default);
                if (c != 0)
                {
                    return (lowerbound, lowerbound);
                }
            }
            else
            {
                // At the top of the array
                return (lowerbound, lowerbound);
            }

            // There are duplicate values, binary search for the end.
            hi = maxNotFound;

            while (lo <= hi)
            {
                int i = lo + ((hi - lo) >> 1);

                int c = column.CompareTo(i, value, default, default);
                if (c <= 0)
                {
                    lo = i + 1;
                }
                else
                {
                    hi = i - 1;
                }
            }
            int upperbound = lo - 1;
            if (!found)
            {
                upperbound = ~lo;
            }

            return (lowerbound, upperbound);
        }

        public static (int, int) SearchBoundriesForMapColumn<T>(
            in MapColumn column, 
            in T value, 
            in int index, 
            in int end, 
            in ReferenceSegment? child,
            in BitmapList? validityList)
            where T : IDataValue
        {
            int lo = index;
            int hi = end;
            int maxNotFound = hi;

            bool found = false;
            while (lo <= hi)
            {
                int i = lo + ((hi - lo) >> 1);


                int c = column.CompareTo(i, value, child, validityList);
                if (c == 0)
                {
                    found = true;
                    hi = i - 1;
                }
                else if (c < 0)
                {
                    lo = i + 1;
                }
                else
                {
                    hi = i - 1;
                    maxNotFound = hi;
                }
            }
            int lowerbound = lo;
            if (!found)
            {
                lowerbound = ~lo;
                // We did not find the value so this is the the bounds.
                return (lowerbound, lowerbound);
            }

            if (lo < end)
            {
                // Check that the next value is the same, if not we are at the of the bounds.
                int c = column.CompareTo(lo + 1, value, child, validityList);
                if (c != 0)
                {
                    return (lowerbound, lowerbound);
                }
            }
            else
            {
                // At the top of the array
                return (lowerbound, lowerbound);
            }

            // There are duplicate values, binary search for the end.
            hi = maxNotFound;

            while (lo <= hi)
            {
                int i = lo + ((hi - lo) >> 1);

                int c = column.CompareTo(i, value, child, validityList);
                if (c <= 0)
                {
                    lo = i + 1;
                }
                else
                {
                    hi = i - 1;
                }
            }
            int upperbound = lo - 1;
            if (!found)
            {
                upperbound = ~lo;
            }

            return (lowerbound, upperbound);
        }

        public static (int, int) SearchBoundriesForDataColumnDesc<T>(
            in IDataColumn column,
            in T value,
            in int index,
            in int end,
            in ReferenceSegment? child,
            in BitmapList? validityList)
            where T : IDataValue
        {
            int lo = index;
            int hi = end;
            int maxNotFound = hi;

            bool found = false;
            while (lo <= hi)
            {
                int i = lo + ((hi - lo) >> 1);


                int c = column.CompareTo(i, value, child, validityList);
                if (c == 0)
                {
                    found = true;
                    hi = i - 1;
                }
                else if (c > 0)
                {
                    lo = i + 1;
                }
                else
                {
                    hi = i - 1;
                    maxNotFound = hi;
                }
            }
            int lowerbound = lo;
            if (!found)
            {
                lowerbound = ~lo;
                // We did not find the value so this is the the bounds.
                return (lowerbound, lowerbound);
            }

            if (lo < (end))
            {
                // Check that the next value is the same, if not we are at the of the bounds.
                int c = column.CompareTo(lo + 1, value, child, validityList);
                if (c != 0)
                {
                    return (lowerbound, lowerbound);
                }
            }
            else
            {
                // At the top of the array
                return (lowerbound, lowerbound);
            }

            // There are duplicate values, binary search for the end.
            hi = maxNotFound;

            while (lo <= hi)
            {
                int i = lo + ((hi - lo) >> 1);

                int c = column.CompareTo(i, value, child, validityList);
                if (c >= 0)
                {
                    lo = i + 1;
                }
                else
                {
                    hi = i - 1;
                }
            }
            int upperbound = lo - 1;
            if (!found)
            {
                upperbound = ~lo;
            }

            return (lowerbound, upperbound);
        }

        public static (int, int) SearchBoundriesForDataColumn<T>(
            in IDataColumn column, 
            in T value, 
            in int index, 
            in int end, 
            in ReferenceSegment? child, 
            in BitmapList? validityList)
            where T : IDataValue
        {
            int lo = index;
            int hi = end;
            int maxNotFound = hi;

            bool found = false;
            while (lo <= hi)
            {
                int i = lo + ((hi - lo) >> 1);


                int c = column.CompareTo(i, value, child, validityList);
                if (c == 0)
                {
                    found = true;
                    hi = i - 1;
                }
                else if (c < 0)
                {
                    lo = i + 1;
                }
                else
                {
                    hi = i - 1;
                    maxNotFound = hi;
                }
            }
            int lowerbound = lo;
            if (!found)
            {
                lowerbound = ~lo;
                // We did not find the value so this is the the bounds.
                return (lowerbound, lowerbound);
            }

            if (lo < (end))
            {
                // Check that the next value is the same, if not we are at the of the bounds.
                int c = column.CompareTo(lo + 1, value, child, validityList);
                if (c != 0)
                {
                    return (lowerbound, lowerbound);
                }
            }
            else
            {
                // At the top of the array
                return (lowerbound, lowerbound);
            }

            // There are duplicate values, binary search for the end.
            hi = maxNotFound;

            while (lo <= hi)
            {
                int i = lo + ((hi - lo) >> 1);

                int c = column.CompareTo(i, value, child, validityList);
                if (c <= 0)
                {
                    lo = i + 1;
                }
                else
                {
                    hi = i - 1;
                }
            }
            int upperbound = lo - 1;
            if (!found)
            {
                upperbound = ~lo;
            }

            return (lowerbound, upperbound);
        }

        public static (int, int) SearchBoundries(in BinaryList list, in ReadOnlySpan<byte> value, in int index, in int end, ISpanByteComparer comparer)
        {
            int lo = index;
            int hi = end;
            int maxNotFound = hi;

            bool found = false;
            while (lo <= hi)
            {
                int i = lo + ((hi - lo) >> 1);

                int c = comparer.Compare(list.Get(i), in value);
                if (c == 0)
                {
                    found = true;
                    hi = i - 1;
                }
                else if (c < 0)
                {
                    lo = i + 1;
                }
                else
                {
                    hi = i - 1;
                    maxNotFound = hi;
                }
            }
            int lowerbound = lo;
            if (!found)
            {
                lowerbound = ~lo;
                // We did not find the value so this is the the bounds.
                return (lowerbound, lowerbound);
            }

            if (lo < end)
            {
                // Check that the next value is the same, if not we are at the of the bounds.
                int c = comparer.Compare(list.Get(lo + 1), in value);
                if (c != 0)
                {
                    return (lowerbound, lowerbound);
                }
            }
            else
            {
                // At the top of the array
                return (lowerbound, lowerbound);
            }

            // There are duplicate values, binary search for the end.
            hi = maxNotFound;

            while (lo <= hi)
            {
                int i = lo + ((hi - lo) >> 1);

                int c = comparer.Compare(list.Get(i), in value);
                if (c <= 0)
                {
                    lo = i + 1;
                }
                else
                {
                    hi = i - 1;
                }
            }
            int upperbound = lo - 1;
            if (!found)
            {
                upperbound = ~upperbound;
            }

            return (lowerbound, upperbound);
        }


        public static unsafe (int, int) SearchBoundriesInt64Asc(in NativeLongList list, int start, int end, long value)
        {
            if (end < start)
            {
                return (~start, ~start);
            }
            var arr = list.Pointer + start;
            var size = end - start + 1;
            while (size > 2)
            {
                var middle = size >> 1;
                size = (size + 1) >> 1;
                // Hack since cmov is not available so far from what has been benchmarked.
                // Clt returns 0 or 1, so we can multiply by the result to get the correct index.
                arr += (middle * UnsafeEx.Clt(arr[middle], value));
            }

            arr += UnsafeEx.Cgt(size, 1) & UnsafeEx.Clt(*arr, value);
            arr += UnsafeEx.Cgt(size, 0) & UnsafeEx.Clt(*arr, value);

            
            start = (int)(arr - list.Pointer);
            int lowIndex = start;
            if (start > end || *arr != value)
            {
                return (~start, ~start);
            }

            if (start == end || arr[1] != value)
            {
                return (start, start);
            }

            size = end - start + 1;
            while (size > 2)
            {
                var middle = size >> 1;
                size = (size + 1) >> 1;
                arr += middle * UnsafeEx.Cle(arr[middle], value);
            }
            arr += UnsafeEx.Cgt(size, 1) & UnsafeEx.Cle(*arr, value);
            arr += UnsafeEx.Cgt(size, 0) & UnsafeEx.Cle(*arr, value);

            return (lowIndex, (int)(arr - list.Pointer) - 1);
        }

        public static unsafe (int, int) SearchBoundriesInt64Desc(in NativeLongList list, int start, int end, long value)
        {
            if (end < start)
            {
                return (~start, ~start);
            }
            var arr = list.Pointer + start;
            var size = end - start + 1;
            while (size > 2)
            {
                var middle = size >> 1;
                size = (size + 1) >> 1;
                // Hack since cmov is not available so far from what has been benchmarked.
                // Clt returns 0 or 1, so we can multiply by the result to get the correct index.
                arr += (middle * UnsafeEx.Cgt(arr[middle], value));
            }

            arr += UnsafeEx.Cgt(size, 1) & UnsafeEx.Cgt(*arr, value);
            arr += UnsafeEx.Cgt(size, 0) & UnsafeEx.Cgt(*arr, value);

            start = (int)(arr - list.Pointer);
            int lowIndex = start;
            if (start > end || *arr != value)
            {
                return (~start, ~start);
            }

            if (start == end || arr[1] != value)
            {
                return (start, start);
            }

            size = end - start + 1;
            while (size > 2)
            {
                var middle = size >> 1;
                size = (size + 1) >> 1;
                arr += middle * UnsafeEx.Cge(arr[middle], value);
            }
            arr += UnsafeEx.Cgt(size, 1) & UnsafeEx.Cge(*arr, value);
            arr += UnsafeEx.Cgt(size, 0) & UnsafeEx.Cge(*arr, value);

            return (lowIndex, (int)(arr - list.Pointer) - 1);
        }

        public static (int, int) SearchBoundries(in NativeLongList list, in long value, in int index, in int end, IColumnComparer<long> comparer)
        {
            int lo = index;
            int hi = end;
            int maxNotFound = hi;

            bool found = false;
            while (lo <= hi)
            {
                int i = lo + ((hi - lo) >> 1);

                int c = comparer.Compare(in list.GetRef(i), in value);
                if (c == 0)
                {
                    found = true;
                    hi = i - 1;
                }
                else if (c < 0)
                {
                    lo = i + 1;
                }
                else
                {
                    hi = i - 1;
                    maxNotFound = hi;
                }
            }
            int lowerbound = lo;
            if (!found)
            {
                lowerbound = ~lo;
                // We did not find the value so this is the the bounds.
                return (lowerbound, lowerbound);
            }

            if (lo < end)
            {
                // Check that the next value is the same, if not we are at the of the bounds.
                int c = comparer.Compare(in list.GetRef(lo + 1), in value);
                if (c != 0)
                {
                    return (lowerbound, lowerbound);
                }
            }
            else
            {
                // At the top of the array
                return (lowerbound, lowerbound);
            }

            // There are duplicate values, binary search for the end.
            hi = maxNotFound;

            while (lo <= hi)
            {
                int i = lo + ((hi - lo) >> 1);

                int c = comparer.Compare(in list.GetRef(i), in value);
                if (c <= 0)
                {
                    lo = i + 1;
                }
                else
                {
                    hi = i - 1;
                }
            }
            int upperbound = lo - 1;
            if (!found)
            {
                upperbound = ~upperbound;
            }

            return (lowerbound, upperbound);
        }

        public static (int, int) SearchBoundries<T>(in IReadOnlyList<T> list, in T value, in int index, in int end, IColumnComparer<T> comparer)
        {
            int lo = index;
            int hi = end;
            int maxNotFound = hi;

            bool found = false;
            while (lo <= hi)
            {
                int i = lo + ((hi - lo) >> 1);

                int c = comparer.Compare(list[i], in value);
                if (c == 0)
                {
                    found = true;
                    hi = i - 1;
                }
                else if (c < 0)
                {
                    lo = i + 1;
                }
                else
                {
                    hi = i - 1;
                    maxNotFound = hi;
                }
            }
            int lowerbound = lo;
            if (!found)
            {
                lowerbound = ~lo;
                // We did not find the value so this is the the bounds.
                return (lowerbound, lowerbound);
            }

            if (lo < end)
            {
                // Check that the next value is the same, if not we are at the of the bounds.
                int c = comparer.Compare(list[lo + 1], in value);
                if (c != 0)
                {
                    return (lowerbound, lowerbound);
                }
            }
            else
            {
                // At the top of the array
                return (lowerbound, lowerbound);
            }

            // There are duplicate values, binary search for the end.
            hi = maxNotFound;

            while (lo <= hi)
            {
                int i = lo + ((hi - lo) >> 1);

                int c = comparer.Compare(list[i], in value);
                if (c <= 0)
                {
                    lo = i + 1;
                }
                else
                {
                    hi = i - 1;
                }
            }
            int upperbound = lo - 1;
            if (!found)
            {
                upperbound = ~upperbound;
            }

            return (lowerbound, upperbound);
        }
    }
}
