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
    /// Per column sort order, default ascending nulls first.
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
        /// True for asc nulls last and desc nulls first, which radix masking can't express.
        /// </summary>
        public static bool HasSwappedNulls(this SortColumnDirection direction)
        {
            return direction == SortColumnDirection.AscendingNullsLast || direction == SortColumnDirection.DescendingNullsFirst;
        }

        /// <summary>
        /// Drops null placement for a null-free column, only value order remains.
        /// </summary>
        public static SortColumnDirection NormalizeForNoNulls(this SortColumnDirection direction)
        {
            return direction.IsDescending() ? SortColumnDirection.DescendingNullsLast : SortColumnDirection.AscendingNullsFirst;
        }
    }
}
