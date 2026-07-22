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

namespace FlowtideDotNet.Core.ColumnStore.Sort
{
    /// <summary>
    /// Per column sort order for <see cref="BatchSorter"/>. The default matches the plain column
    /// comparison: ascending with nulls first.
    /// </summary>
    public enum SortColumnDirection : byte
    {
        AscendingNullsFirst = 0,
        AscendingNullsLast = 1,
        DescendingNullsFirst = 2,
        DescendingNullsLast = 3
    }

    internal static class SortColumnDirectionExtensions
    {
        public static bool IsDescending(this SortColumnDirection direction)
        {
            return direction == SortColumnDirection.DescendingNullsFirst || direction == SortColumnDirection.DescendingNullsLast;
        }

        /// <summary>
        /// True when the null placement is on the opposite end from where the value order would put it:
        /// ascending with nulls last, or descending with nulls first. These cannot be expressed by
        /// complementing radix prefix bytes, since the null marker byte is less significant than the value
        /// bytes within a column's prefix range.
        /// </summary>
        public static bool HasSwappedNulls(this SortColumnDirection direction)
        {
            return direction == SortColumnDirection.AscendingNullsLast || direction == SortColumnDirection.DescendingNullsFirst;
        }

        /// <summary>
        /// Normalizes the direction for a column that contains no nulls in the current batch, where null
        /// placement is meaningless and only the value order remains.
        /// </summary>
        public static SortColumnDirection NormalizeForNoNulls(this SortColumnDirection direction)
        {
            return direction.IsDescending() ? SortColumnDirection.DescendingNullsLast : SortColumnDirection.AscendingNullsFirst;
        }
    }
}
