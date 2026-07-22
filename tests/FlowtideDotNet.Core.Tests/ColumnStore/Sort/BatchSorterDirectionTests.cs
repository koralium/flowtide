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
    /// BatchSorter directions must match the row comparison, a divergence would corrupt trees.
    /// </summary>
    public class BatchSorterDirectionTests
    {
        /// <summary>
        /// Mirrors SortFieldCompareCompiler's four comparison implementations.
        /// </summary>
        private static int ReferenceCompareColumn(IColumn column, SortColumnDirection direction, int x, int y)
        {
            var xv = column.GetValueAt(x, default);
            var yv = column.GetValueAt(y, default);
            bool nullsFirst = direction == SortColumnDirection.AscendingNullsFirst || direction == SortColumnDirection.DescendingNullsFirst;
            if (xv.IsNull)
            {
                if (yv.IsNull)
                {
                    return 0;
                }
                return nullsFirst ? -1 : 1;
            }
            if (yv.IsNull)
            {
                return nullsFirst ? 1 : -1;
            }
            return direction.IsDescending() ? DataValueComparer.CompareTo(yv, xv) : DataValueComparer.CompareTo(xv, yv);
        }

        private static int ReferenceCompare(IColumn[] columns, SortColumnDirection[] directions, int x, int y)
        {
            for (int i = 0; i < columns.Length; i++)
            {
                var result = ReferenceCompareColumn(columns[i], directions[i], x, y);
                if (result != 0)
                {
                    return result;
                }
            }
            return 0;
        }

        private static void AssertSortedAndTagged(IColumn[] columns, SortColumnDirection[] directions, int count)
        {
            var sorter = new BatchSorter(columns.Length, directions);
            int[] indices = new int[count];
            int[] tags = new int[count];
            for (int i = 0; i < count; i++)
            {
                indices[i] = i;
            }
            var indexSpan = indices.AsSpan();
            var tagSpan = tags.AsSpan();
            sorter.SortDataWithTags(columns, ref indexSpan, ref tagSpan);

            var seen = new bool[count];
            for (int i = 0; i < count; i++)
            {
                Assert.False(seen[indices[i]], "Sorted indices must be a permutation");
                seen[indices[i]] = true;
            }

            if (count == 0)
            {
                return;
            }
            Assert.Equal(0, tags[0]);
            for (int i = 1; i < count; i++)
            {
                var compare = ReferenceCompare(columns, directions, indices[i - 1], indices[i]);
                Assert.True(compare <= 0,
                    $"Row {indices[i - 1]} must not sort after row {indices[i]} (position {i}, compare {compare})");
                if (compare == 0)
                {
                    Assert.Equal(tags[i - 1], tags[i]);
                }
                else
                {
                    Assert.Equal(tags[i - 1] + 1, tags[i]);
                }
            }
        }

        private static Column CreateInt64Column(Random random, int count, bool withNulls, long valueRange)
        {
            var column = new Column(GlobalMemoryManager.Instance);
            for (int i = 0; i < count; i++)
            {
                if (withNulls && random.Next(4) == 0)
                {
                    column.Add(NullValue.Instance);
                }
                else if (random.Next(20) == 0)
                {
                    column.Add(new Int64Value(random.Next(2) == 0 ? long.MinValue : long.MaxValue));
                }
                else
                {
                    column.Add(new Int64Value(random.NextInt64(-valueRange, valueRange + 1)));
                }
            }
            return column;
        }

        private static Column CreateStringColumn(Random random, int count, bool withNulls)
        {
            var column = new Column(GlobalMemoryManager.Instance);
            for (int i = 0; i < count; i++)
            {
                if (withNulls && random.Next(4) == 0)
                {
                    column.Add(NullValue.Instance);
                }
                else
                {
                    var length = random.Next(0, 6);
                    var chars = new char[length];
                    for (int c = 0; c < length; c++)
                    {
                        chars[c] = (char)('a' + random.Next(3));
                    }
                    column.Add(new StringValue(new string(chars)));
                }
            }
            return column;
        }

        private static Column CreateDoubleColumn(Random random, int count, bool withNulls)
        {
            var column = new Column(GlobalMemoryManager.Instance);
            for (int i = 0; i < count; i++)
            {
                if (withNulls && random.Next(4) == 0)
                {
                    column.Add(NullValue.Instance);
                }
                else
                {
                    column.Add(new DoubleValue(random.Next(-5, 6) * 1.5));
                }
            }
            return column;
        }

        [Fact]
        public void DescendingSingleInt64Column()
        {
            var column = new Column(GlobalMemoryManager.Instance);
            column.Add(new Int64Value(3));
            column.Add(new Int64Value(1));
            column.Add(new Int64Value(2));

            var sorter = new BatchSorter(1, new[] { SortColumnDirection.DescendingNullsLast });
            int[] indices = new int[] { 0, 1, 2 };
            var span = indices.AsSpan();
            sorter.SortData(new IColumn[] { column }, ref span);

            Assert.Equal(new[] { 0, 2, 1 }, indices);
        }

        [Fact]
        public void DescendingNullsLastPlacesNullsAtTheEnd()
        {
            var column = new Column(GlobalMemoryManager.Instance);
            column.Add(new Int64Value(3));
            column.Add(NullValue.Instance);
            column.Add(new Int64Value(long.MinValue));
            column.Add(new Int64Value(long.MaxValue));

            var sorter = new BatchSorter(1, new[] { SortColumnDirection.DescendingNullsLast });
            int[] indices = new int[] { 0, 1, 2, 3 };
            var span = indices.AsSpan();
            sorter.SortData(new IColumn[] { column }, ref span);

            Assert.Equal(new[] { 3, 0, 2, 1 }, indices);
        }

        [Theory]
        [InlineData(SortColumnDirection.AscendingNullsFirst)]
        [InlineData(SortColumnDirection.AscendingNullsLast)]
        [InlineData(SortColumnDirection.DescendingNullsFirst)]
        [InlineData(SortColumnDirection.DescendingNullsLast)]
        public void SingleNullableInt64AllDirections(SortColumnDirection direction)
        {
            var random = new Random(42 + (int)direction);
            const int count = 500;
            var column = CreateInt64Column(random, count, withNulls: true, valueRange: 10);
            AssertSortedAndTagged(new IColumn[] { column }, new[] { direction }, count);
        }

        [Fact]
        public void RandomizedMixedLayouts()
        {
            // Mixed radix and comparer types, like the window layout.
            var directionsPool = new[]
            {
                SortColumnDirection.AscendingNullsFirst,
                SortColumnDirection.AscendingNullsLast,
                SortColumnDirection.DescendingNullsFirst,
                SortColumnDirection.DescendingNullsLast
            };

            for (int seed = 0; seed < 30; seed++)
            {
                var random = new Random(1000 + seed);
                int count = seed % 5 == 0 ? random.Next(0, 3) : random.Next(50, 400);
                int columnCount = random.Next(1, 5);

                var columns = new IColumn[columnCount];
                var directions = new SortColumnDirection[columnCount];
                for (int c = 0; c < columnCount; c++)
                {
                    var typePick = random.Next(3);
                    bool withNulls = random.Next(2) == 0;
                    columns[c] = typePick switch
                    {
                        0 => CreateInt64Column(random, count, withNulls, valueRange: 5),
                        1 => CreateStringColumn(random, count, withNulls),
                        _ => CreateDoubleColumn(random, count, withNulls),
                    };
                    directions[c] = directionsPool[random.Next(directionsPool.Length)];
                }

                AssertSortedAndTagged(columns, directions, count);

                for (int c = 0; c < columnCount; c++)
                {
                    ((Column)columns[c]).Dispose();
                }
            }
        }

        [Fact]
        public void TailColumnsBeyondFastPathHonorDirections()
        {
            var random = new Random(7);
            const int count = 200;
            const int columnCount = 9;

            var columns = new IColumn[columnCount];
            var directions = new SortColumnDirection[columnCount];
            for (int c = 0; c < columnCount; c++)
            {
                // A tiny value range forces ties through to the tail columns.
                columns[c] = CreateInt64Column(random, count, withNulls: c % 2 == 0, valueRange: 1);
                directions[c] = c % 3 == 0 ? SortColumnDirection.DescendingNullsLast
                    : c % 3 == 1 ? SortColumnDirection.AscendingNullsFirst
                    : SortColumnDirection.DescendingNullsFirst;
            }

            AssertSortedAndTagged(columns, directions, count);
        }

        [Fact]
        public void SameColumnTwiceWithDifferentDirections()
        {
            // A physical column can appear as both a directed and an ascending column.
            var random = new Random(11);
            const int count = 300;
            var column = CreateInt64Column(random, count, withNulls: true, valueRange: 3);
            var other = CreateInt64Column(random, count, withNulls: false, valueRange: 3);

            var columns = new IColumn[] { other, column, other, column };
            var directions = new[]
            {
                SortColumnDirection.AscendingNullsFirst,
                SortColumnDirection.DescendingNullsLast,
                SortColumnDirection.AscendingNullsFirst,
                SortColumnDirection.AscendingNullsFirst
            };

            AssertSortedAndTagged(columns, directions, count);
        }
    }
}
