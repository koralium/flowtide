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

namespace FlowtideDotNet.Storage.Comparers
{
    /// <summary>
    /// Compares <see cref="long"/> values in ascending order. Used as the element comparer for keys stored in the
    /// trees and key containers, for example wrapped inside a list comparer or passed to a key container binary search.
    /// </summary>
    public class LongComparer : IComparer<long>
    {
        /// <summary>
        /// Compares two values and returns their relative order.
        /// </summary>
        /// <param name="x">The first value.</param>
        /// <param name="y">The second value.</param>
        /// <returns>
        /// A negative number when <paramref name="x"/> is less than <paramref name="y"/>, zero when they are equal,
        /// and a positive number when <paramref name="x"/> is greater.
        /// </returns>
        public int Compare(long x, long y)
        {
            return x.CompareTo(y);
        }
    }
}
