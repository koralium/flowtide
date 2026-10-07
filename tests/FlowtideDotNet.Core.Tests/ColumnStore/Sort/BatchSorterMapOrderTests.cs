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

using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.Comparers;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.ColumnStore.Sort;
using FlowtideDotNet.Storage.Memory;

namespace FlowtideDotNet.Core.Tests.ColumnStore.Sort
{
    /// <summary>
    /// Every direction must order a column the same way the tree comparer does.
    /// </summary>
    public class BatchSorterMapOrderTests
    {
        private static MapValue Map(string firstKey, long firstValue, string secondKey, long secondValue)
        {
            return new MapValue(
                new KeyValuePair<IDataValue, IDataValue>(new StringValue(firstKey), new Int64Value(firstValue)),
                new KeyValuePair<IDataValue, IDataValue>(new StringValue(secondKey), new Int64Value(secondValue)));
        }

        // Interleaved key/value order and all-keys-then-values order disagree on these
        private static Column BuildColumn()
        {
            var column = Column.Create(GlobalMemoryManager.Instance);
            column.Add(Map("a", 1, "b", 1));
            column.Add(Map("a", 5, "b", 1));
            column.Add(Map("a", 1, "c", 1));
            column.Add(NullValue.Instance);
            return column;
        }

        private static List<int> ExpectedOrder(Column column, bool nullsLast, bool descending)
        {
            var indices = new List<int>() { 0, 1, 2, 3 };
            indices.Sort((x, y) =>
            {
                var xNull = column.GetTypeAt(x, default) == ArrowTypeId.Null;
                var yNull = column.GetTypeAt(y, default) == ArrowTypeId.Null;
                if (xNull || yNull)
                {
                    if (xNull && yNull) return 0;
                    return xNull == nullsLast ? 1 : -1;
                }
                var result = DataValueComparer.CompareTo(column.GetValueAt(x, default), column.GetValueAt(y, default));
                return descending ? -result : result;
            });
            return indices;
        }

        [Theory]
        [InlineData(SortColumnDirection.AscendingNullsFirst, false, false)]
        [InlineData(SortColumnDirection.AscendingNullsLast, true, false)]
        [InlineData(SortColumnDirection.DescendingNullsFirst, false, true)]
        [InlineData(SortColumnDirection.DescendingNullsLast, true, true)]
        public void MapColumnOrderMatchesTheValueComparer(SortColumnDirection direction, bool nullsLast, bool descending)
        {
            using var column = BuildColumn();
            var sorter = new BatchSorter(1, new SortColumnDirection[] { direction });

            Span<int> indirect = new int[] { 0, 1, 2, 3 };
            sorter.SortData(new IColumn[] { column }, ref indirect);

            Assert.Equal(ExpectedOrder(column, nullsLast, descending), indirect.ToArray());
        }
    }
}
