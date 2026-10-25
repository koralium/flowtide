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
using FlowtideDotNet.Core.ColumnStore.DataColumns;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.ColumnStore.Sort;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Memory;

namespace FlowtideDotNet.Core.Tests.ColumnStore.Sort
{
    public class BatchSorterEmptyIntegerTests
    {
        private static Column CreateEmptyIntegerColumn()
        {
            return new Column(0, new IntegerColumn(GlobalMemoryManager.Instance), default(BitmapList), ArrowTypeId.Int64, GlobalMemoryManager.Instance);
        }

        private static Column CreateColumn(params long[] values)
        {
            var column = new Column(GlobalMemoryManager.Instance);
            for (int i = 0; i < values.Length; i++)
            {
                column.Add(new Int64Value(values[i]));
            }
            return column;
        }

        private static ColumnWithOffset CreateOffsetColumn(Column inner, params int[] offsets)
        {
            var list = new PrimitiveList<int>(GlobalMemoryManager.Instance);
            for (int i = 0; i < offsets.Length; i++)
            {
                list.Add(offsets[i]);
            }
            return new ColumnWithOffset(inner, list);
        }

        [Fact]
        public void EmptyIntegerColumnHasOwnSortKey()
        {
            var empty = CreateEmptyIntegerColumn();
            var column = CreateColumn(3, 1, 2);

            Assert.NotEqual(SortCompiler.CreateKey(new IColumn[] { column }), SortCompiler.CreateKey(new IColumn[] { empty }));
            Assert.NotEqual(
                SortCompiler.CreateKey(new IColumn[] { CreateOffsetColumn(column, 0, -1) }),
                SortCompiler.CreateKey(new IColumn[] { CreateOffsetColumn(empty, -1, -1) }));
        }

        [Fact]
        public void SortWithTagsAfterEmptyIntegerColumnKeepsOrder()
        {
            var directions = new[] { SortColumnDirection.DescendingNullsLast, SortColumnDirection.AscendingNullsLast };
            var empty = CreateEmptyIntegerColumn();
            var emptyIndices = Span<int>.Empty;
            var emptyTags = Span<int>.Empty;
            new BatchSorter(2, directions).SortDataWithTags(new IColumn[] { empty, empty }, ref emptyIndices, ref emptyTags);

            var first = CreateColumn(1, 3, 1, 2);
            var second = CreateColumn(0, 0, 0, 0);
            var indices = new int[] { 0, 1, 2, 3 };
            var tags = new int[4];
            var indexSpan = indices.AsSpan();
            var tagSpan = tags.AsSpan();
            new BatchSorter(2, directions).SortDataWithTags(new IColumn[] { first, second }, ref indexSpan, ref tagSpan);

            Assert.Equal(new int[] { 1, 3 }, indices[0..2]);
            Assert.Equal(new int[] { 0, 1, 2, 2 }, tags);
        }

        [Fact]
        public void SortAfterEmptyWrappedIntegerColumnKeepsOrder()
        {
            var directions = new[] { SortColumnDirection.AscendingNullsFirst, SortColumnDirection.DescendingNullsLast };
            var empty = CreateOffsetColumn(CreateEmptyIntegerColumn(), -1, -1);
            var emptyIndices = new int[] { 0, 1 }.AsSpan();
            new BatchSorter(2, directions).SortData(new IColumn[] { empty, CreateColumn(5, 5) }, ref emptyIndices);

            var first = CreateOffsetColumn(CreateColumn(30, 10, 20), 0, -1, 1, 2);
            var second = CreateColumn(5, 5, 5, 5);
            var indices = new int[] { 0, 1, 2, 3 };
            var indexSpan = indices.AsSpan();
            new BatchSorter(2, directions).SortData(new IColumn[] { first, second }, ref indexSpan);

            Assert.Equal(new int[] { 1, 2, 3, 0 }, indices);
        }
    }
}
