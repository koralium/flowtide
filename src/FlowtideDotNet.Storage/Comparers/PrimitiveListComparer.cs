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

using FlowtideDotNet.Storage.Tree;
using System.Collections;

namespace FlowtideDotNet.Storage.Comparers
{
    public class PrimitiveListComparer<T> : IBplusTreeComparer<T, PrimitiveListKeyContainer<T>>
        where T : unmanaged, IComparable<T>
    {
        private class Comparer : IComparer<T>
        {
            public int Compare(T x, T y)
            {
                return x.CompareTo(y);
            }
        }

        private Comparer m_comparer;

        public PrimitiveListComparer()
        {
            m_comparer = new Comparer();
        }
        public bool SeekNextPageForValue => true;

        public int CompareTo(in T x, in T y)
        {
            return x.CompareTo(y);
        }

        public int CompareTo(in T key, in PrimitiveListKeyContainer<T> keyContainer, in int index)
        {
            throw new NotImplementedException();
        }

        public int FindIndex(in T key, in PrimitiveListKeyContainer<T> keyContainer)
        {
            return keyContainer.BinarySearch(key, m_comparer);
        }

        public FindBoundriesResult FindBoundries(in T key, in PrimitiveListKeyContainer<T> keyContainer, int startIndex, int endIndex)
        {
            int lo = startIndex;
            int hi = endIndex;
            int maxNotFound = hi;

            var span = keyContainer._values.Span;

            bool found = false;
            while (lo <= hi)
            {
                int i = lo + ((hi - lo) >> 1);

                int c = span[i].CompareTo(key);
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
                return new FindBoundriesResult(lowerbound, lowerbound);
            }

            if (lo < endIndex)
            {
                // Check that the next value is the same, if not we are at the of the bounds.
                int c = span[lo + 1].CompareTo(key);
                if (c != 0)
                {
                    return new FindBoundriesResult(lowerbound, lowerbound);
                }
            }
            else
            {
                // At the top of the array
                return new FindBoundriesResult(lowerbound, lowerbound);
            }

            // There are duplicate values, binary search for the end.
            hi = maxNotFound;

            while (lo <= hi)
            {
                int i = lo + ((hi - lo) >> 1);

                int c = span[i].CompareTo(key);
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

            return new FindBoundriesResult(lowerbound, upperbound);
        }
    }
}
