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
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.ColumnStore.Sort;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Memory;

namespace FlowtideDotNet.Core.Tests.ColumnStore.Sort
{
    public class BatchSorterOffsetNullDirectionTests
    {
        /// <summary>
        /// The left join padding shape, a null free inner column where the offsets carry the nulls.
        /// </summary>
        private static ColumnWithOffset CreateOffsetColumn()
        {
            var inner = new Column(GlobalMemoryManager.Instance);
            inner.Add(new Int64Value(10));
            inner.Add(new Int64Value(20));
            inner.Add(new Int64Value(30));

            var offsets = new PrimitiveList<int>(GlobalMemoryManager.Instance);
            offsets.Add(1);
            offsets.Add(ColumnWithOffset.NullValueIndex);
            offsets.Add(0);
            offsets.Add(ColumnWithOffset.NullValueIndex);
            offsets.Add(2);

            return new ColumnWithOffset(inner, offsets);
        }

        private static long?[] ReadLongOrder(IColumn column, int[] indices)
        {
            var values = new long?[indices.Length];
            for (int i = 0; i < indices.Length; i++)
            {
                var value = column.GetValueAt(indices[i], default);
                values[i] = value.IsNull ? null : value.AsLong;
            }
            return values;
        }

        private static bool[] ReadNullOrder(IColumn column, int[] indices)
        {
            var nulls = new bool[indices.Length];
            for (int i = 0; i < indices.Length; i++)
            {
                nulls[i] = column.GetValueAt(indices[i], default).IsNull;
            }
            return nulls;
        }

        // RED: offset nulls are invisible to GetEffectiveDirection, so asc nulls last is normalized to nulls first and the nulls land at the start.
        [Fact]
        public void ColumnWithOffsetAscendingNullsLastPlacesNullsLast()
        {
            var column = CreateOffsetColumn();

            var sorter = new BatchSorter(1, new[] { SortColumnDirection.AscendingNullsLast });
            int[] indices = new int[] { 0, 1, 2, 3, 4 };
            var span = indices.AsSpan();
            sorter.SortData(new IColumn[] { column }, ref span);

            Assert.Equal(new long?[] { 10, 20, 30, null, null }, ReadLongOrder(column, indices));
        }

        // RED: offset nulls are invisible to GetEffectiveDirection, so desc nulls first is normalized to nulls last and the nulls land at the end.
        [Fact]
        public void ColumnWithOffsetDescendingNullsFirstPlacesNullsFirst()
        {
            var column = CreateOffsetColumn();

            var sorter = new BatchSorter(1, new[] { SortColumnDirection.DescendingNullsFirst });
            int[] indices = new int[] { 0, 1, 2, 3, 4 };
            var span = indices.AsSpan();
            sorter.SortData(new IColumn[] { column }, ref span);

            Assert.Equal(new long?[] { null, null, 30, 20, 10 }, ReadLongOrder(column, indices));
        }

        // RED: union conversion clears the validity bitmap, so asc nulls last is normalized to nulls first and the nulls land at the start.
        [Fact]
        public void UnionColumnAscendingNullsLastPlacesNullsLast()
        {
            var column = new Column(GlobalMemoryManager.Instance);
            column.Add(new Int64Value(20));
            column.Add(NullValue.Instance);
            column.Add(new Int64Value(10));
            column.Add(NullValue.Instance);
            column.Add(new StringValue("b"));

            var sorter = new BatchSorter(1, new[] { SortColumnDirection.AscendingNullsLast });
            int[] indices = new int[] { 0, 1, 2, 3, 4 };
            var span = indices.AsSpan();
            sorter.SortData(new IColumn[] { column }, ref span);

            Assert.Equal(new[] { false, false, false, true, true }, ReadNullOrder(column, indices));
        }

        // Control, asc nulls first is never normalized so it is green today and proves the comparer itself is sound.
        [Fact]
        public void ColumnWithOffsetAscendingNullsFirstIsUnaffected()
        {
            var column = CreateOffsetColumn();

            var sorter = new BatchSorter(1, new[] { SortColumnDirection.AscendingNullsFirst });
            int[] indices = new int[] { 0, 1, 2, 3, 4 };
            var span = indices.AsSpan();
            sorter.SortData(new IColumn[] { column }, ref span);

            Assert.Equal(new long?[] { null, null, 10, 20, 30 }, ReadLongOrder(column, indices));
        }
    }
}
