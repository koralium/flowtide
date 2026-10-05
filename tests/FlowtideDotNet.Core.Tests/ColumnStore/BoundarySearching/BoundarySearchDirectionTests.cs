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
using FlowtideDotNet.Core.ColumnStore.BoundarySearching;
using FlowtideDotNet.Core.ColumnStore.Comparers;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.ColumnStore.Sort;
using FlowtideDotNet.Storage.Memory;

namespace FlowtideDotNet.Core.Tests.ColumnStore.BoundarySearching
{
    /// <summary>
    /// Direction aware bulk boundary search vs a linear scan reference, ranges and insertion points
    /// must match since the inserter positions writes with both.
    /// </summary>
    public class BoundarySearchDirectionTests
    {
        private static int ReferenceCompareColumn(IColumn treeColumn, int treeIndex, IColumn probeColumn, int probeIndex, SortColumnDirection direction)
        {
            var treeValue = treeColumn.GetValueAt(treeIndex, default);
            var probeValue = probeColumn.GetValueAt(probeIndex, default);
            bool nullsFirst = direction == SortColumnDirection.AscendingNullsFirst || direction == SortColumnDirection.DescendingNullsFirst;
            if (treeValue.IsNull)
            {
                if (probeValue.IsNull)
                {
                    return 0;
                }
                return nullsFirst ? -1 : 1;
            }
            if (probeValue.IsNull)
            {
                return nullsFirst ? 1 : -1;
            }
            return direction.IsDescending()
                ? DataValueComparer.CompareTo(probeValue, treeValue)
                : DataValueComparer.CompareTo(treeValue, probeValue);
        }

        private static int ReferenceCompareRow(IColumn[] treeColumns, int treeIndex, IColumn[] probeColumns, int probeIndex, SortColumnDirection[] directions)
        {
            for (int c = 0; c < treeColumns.Length; c++)
            {
                var result = ReferenceCompareColumn(treeColumns[c], treeIndex, probeColumns[c], probeIndex, directions[c]);
                if (result != 0)
                {
                    return result;
                }
            }
            return 0;
        }

        private static (int lower, int upper) ReferenceBounds(IColumn[] treeColumns, int treeCount, IColumn[] probeColumns, int probeIndex, SortColumnDirection[] directions)
        {
            int first = treeCount;
            for (int i = 0; i < treeCount; i++)
            {
                if (ReferenceCompareRow(treeColumns, i, probeColumns, probeIndex, directions) >= 0)
                {
                    first = i;
                    break;
                }
            }
            if (first == treeCount || ReferenceCompareRow(treeColumns, first, probeColumns, probeIndex, directions) != 0)
            {
                return (~first, ~first);
            }
            int last = first;
            while (last + 1 < treeCount && ReferenceCompareRow(treeColumns, last + 1, probeColumns, probeIndex, directions) == 0)
            {
                last++;
            }
            return (first, last);
        }

        private static Column BuildInt64Column(Random random, int count, bool withNulls, long valueRange)
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
                    column.Add(new Int64Value(random.NextInt64(-valueRange, valueRange + 1)));
                }
            }
            return column;
        }

        private static Column BuildStringColumn(Random random, int count, bool withNulls)
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
                    var length = random.Next(0, 5);
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

        /// <summary>
        /// Sorts columns into a region like a tree page, the search's input.
        /// </summary>
        private static IColumn[] SortIntoRegion(IColumn[] columns, int count, SortColumnDirection[] directions)
        {
            var sorter = new BatchSorter(columns.Length, directions);
            int[] indices = new int[count];
            for (int i = 0; i < count; i++)
            {
                indices[i] = i;
            }
            var span = indices.AsSpan();
            sorter.SortData(columns, ref span);

            var result = new IColumn[columns.Length];
            for (int c = 0; c < columns.Length; c++)
            {
                var sortedColumn = new Column(GlobalMemoryManager.Instance);
                for (int i = 0; i < count; i++)
                {
                    sortedColumn.Add(columns[c].GetValueAt(indices[i], default));
                }
                result[c] = sortedColumn;
            }
            return result;
        }

        private static int[] SortProbes(IColumn[] probeColumns, int count, SortColumnDirection[] directions)
        {
            var sorter = new BatchSorter(probeColumns.Length, directions);
            int[] indices = new int[count];
            for (int i = 0; i < count; i++)
            {
                indices[i] = i;
            }
            var span = indices.AsSpan();
            sorter.SortData(probeColumns, ref span);
            return indices;
        }

        private static void AssertBulkSearchMatchesReference(
            IColumn[] treeColumns,
            int treeCount,
            IColumn[] probeColumns,
            int probeCount,
            SortColumnDirection[] directions)
        {
            var columnOrder = new int[treeColumns.Length];
            for (int i = 0; i < columnOrder.Length; i++)
            {
                columnOrder[i] = i;
            }

            var search = new ColumnBoundarySearch(columnOrder, columnOrder, directions);
            var sortedLookup = SortProbes(probeColumns, probeCount, directions);
            int[] lowerBounds = new int[probeCount];
            int[] upperBounds = new int[probeCount];
            int[] buffer = new int[probeCount];

            search.SearchBoundries(treeColumns, probeColumns, sortedLookup, lowerBounds, upperBounds, 0, treeCount - 1, false, buffer);

            for (int i = 0; i < probeCount; i++)
            {
                var probeIndex = sortedLookup[i];
                var (expectedLower, expectedUpper) = ReferenceBounds(treeColumns, treeCount, probeColumns, probeIndex, directions);
                Assert.True(expectedLower == lowerBounds[i] && expectedUpper == upperBounds[i],
                    $"Probe {probeIndex} (position {i}): expected ({expectedLower}, {expectedUpper}) got ({lowerBounds[i]}, {upperBounds[i]})");
            }
        }

        [Theory]
        [InlineData(SortColumnDirection.AscendingNullsFirst, false)]
        [InlineData(SortColumnDirection.DescendingNullsLast, false)]
        [InlineData(SortColumnDirection.AscendingNullsFirst, true)]
        [InlineData(SortColumnDirection.AscendingNullsLast, true)]
        [InlineData(SortColumnDirection.DescendingNullsFirst, true)]
        [InlineData(SortColumnDirection.DescendingNullsLast, true)]
        public void SingleInt64Column(SortColumnDirection direction, bool withNulls)
        {
            var random = new Random(100 + (int)direction + (withNulls ? 10 : 0));
            const int treeCount = 600;
            const int probeCount = 300;
            var directions = new[] { direction };

            // A small value range forces duplicates and both hits and misses.
            var treeSource = BuildInt64Column(random, treeCount, withNulls, valueRange: 100);
            var treeColumns = SortIntoRegion(new IColumn[] { treeSource }, treeCount, directions);
            var probeColumns = new IColumn[] { BuildInt64Column(random, probeCount, withNulls, valueRange: 120) };

            AssertBulkSearchMatchesReference(treeColumns, treeCount, probeColumns, probeCount, directions);
        }

        [Theory]
        [InlineData(SortColumnDirection.DescendingNullsLast)]
        [InlineData(SortColumnDirection.DescendingNullsFirst)]
        public void SingleStringColumnDescending(SortColumnDirection direction)
        {
            var random = new Random(55 + (int)direction);
            const int treeCount = 400;
            const int probeCount = 200;
            var directions = new[] { direction };

            var treeSource = BuildStringColumn(random, treeCount, withNulls: true);
            var treeColumns = SortIntoRegion(new IColumn[] { treeSource }, treeCount, directions);
            var probeColumns = new IColumn[] { BuildStringColumn(random, probeCount, withNulls: true) };

            AssertBulkSearchMatchesReference(treeColumns, treeCount, probeColumns, probeCount, directions);
        }

        [Fact]
        public void WindowStyleLayoutMixedDirections()
        {
            // Mixed directions like the window layout for ORDER BY x DESC.
            for (int seed = 0; seed < 10; seed++)
            {
                var random = new Random(2000 + seed);
                int treeCount = random.Next(100, 600);
                int probeCount = random.Next(50, 300);
                var directions = new[]
                {
                    SortColumnDirection.AscendingNullsFirst,
                    SortColumnDirection.DescendingNullsLast,
                    SortColumnDirection.AscendingNullsFirst
                };

                var treeSource = new IColumn[]
                {
                    BuildInt64Column(random, treeCount, withNulls: false, valueRange: 5),
                    BuildInt64Column(random, treeCount, withNulls: seed % 2 == 0, valueRange: 8),
                    BuildInt64Column(random, treeCount, withNulls: false, valueRange: 8)
                };
                var treeColumns = SortIntoRegion(treeSource, treeCount, directions);
                var probeColumns = new IColumn[]
                {
                    BuildInt64Column(random, probeCount, withNulls: false, valueRange: 5),
                    BuildInt64Column(random, probeCount, withNulls: seed % 2 == 0, valueRange: 8),
                    BuildInt64Column(random, probeCount, withNulls: false, valueRange: 8)
                };

                AssertBulkSearchMatchesReference(treeColumns, treeCount, probeColumns, probeCount, directions);
            }
        }

        [Fact]
        public void SmallTreesAndEdgeProbes()
        {
            var directions = new[] { SortColumnDirection.DescendingNullsLast };

            // Empty tree: every probe is an insertion at position 0.
            var emptyTree = new IColumn[] { new Column(GlobalMemoryManager.Instance) };
            var probes = new Column(GlobalMemoryManager.Instance);
            probes.Add(new Int64Value(5));
            probes.Add(new Int64Value(1));
            AssertBulkSearchMatchesReference(emptyTree, 0, new IColumn[] { probes }, 2, directions);

            // Single row tree with probes before, at and after it in descending order.
            var singleTree = new Column(GlobalMemoryManager.Instance);
            singleTree.Add(new Int64Value(10));
            var edgeProbes = new Column(GlobalMemoryManager.Instance);
            edgeProbes.Add(new Int64Value(20));
            edgeProbes.Add(new Int64Value(10));
            edgeProbes.Add(new Int64Value(1));
            AssertBulkSearchMatchesReference(new IColumn[] { singleTree }, 1, new IColumn[] { edgeProbes }, 3, directions);
        }

        [Fact]
        public void LargeDescendingTreeExercisesHybridSplit()
        {
            // Enough rows and probes that the hybrid search uses its divide and conquer path rather than
            // only the small range SIMD loop.
            var random = new Random(77);
            const int treeCount = 5000;
            const int probeCount = 2000;
            var directions = new[] { SortColumnDirection.DescendingNullsLast };

            var treeSource = BuildInt64Column(random, treeCount, withNulls: false, valueRange: 1500);
            var treeColumns = SortIntoRegion(new IColumn[] { treeSource }, treeCount, directions);
            var probeColumns = new IColumn[] { BuildInt64Column(random, probeCount, withNulls: false, valueRange: 1800) };

            AssertBulkSearchMatchesReference(treeColumns, treeCount, probeColumns, probeCount, directions);
        }
    }
}
