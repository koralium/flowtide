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

using FASTER.core;
using FlowtideDotNet.Core.ColumnStore.Comparers;
using FlowtideDotNet.Core.ColumnStore.DataColumns;
using FlowtideDotNet.Core.ColumnStore.Utils;
using FlowtideDotNet.Core.Utils;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using static SqlParser.Ast.TableConstraint;

namespace FlowtideDotNet.Core.ColumnStore.TreeStorage
{
    internal static class BoundarySearch
    {
        public static (int, int) SearchBoundriesForColumn<T>(in UnionColumn column, in T value, in int index, in int length)
            where T: IDataValue
        {
            int lo = index;
            int hi = index + length - 1;
            int maxNotFound = hi;
            
            bool found = false;
            while (lo <= hi)
            {
                int i = lo + ((hi - lo) >> 1);

                
                int c = column.CompareTo(i, value);
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

            if (lo < (index + length - 1))
            {
                // Check that the next value is the same, if not we are at the of the bounds.
                int c = column.CompareTo(lo + 1, value);
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

                int c = column.CompareTo(i, value);
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

        public static (int, int) SearchBoundries(in BinaryList list, in ReadOnlySpan<byte> value, in int index, in int length, ISpanByteComparer comparer)
        {
            int lo = index;
            int hi = index + length - 1;
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

            if (lo < (index + length - 1))
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

        public static (int, int) SearchBoundries(in NativeLongList list, in long value, in int index, in int length, IColumnComparer<long> comparer)
        {
            int lo = index;
            int hi = index + length - 1;
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

            if (lo < (index + length - 1))
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

        public static (int, int) SearchBoundries<T>(in List<T> list, in T value, in int index, in int length, IColumnComparer<T> comparer)
        {
            int lo = index;
            int hi = index + length - 1;
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

            if (lo < (index + length - 1))
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
