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
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.ColumnStore.Sort;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Memory;

namespace FlowtideDotNet.Core.Tests.ColumnStore.BoundarySearching
{
    public class BoundarySearchOffsetNullTests
    {
        private static Column BuildInt64Column(params long[] values)
        {
            var column = new Column(GlobalMemoryManager.Instance);
            for (int i = 0; i < values.Length; i++)
            {
                column.Add(new Int64Value(values[i]));
            }
            return column;
        }

        /// <summary>
        /// A null padded probe column like a left join produces, null rows are an offset of -1 over a null free inner column.
        /// </summary>
        private static ColumnWithOffset BuildNullPaddedProbes(long[] innerValues, int[] offsets)
        {
            var offsetList = new PrimitiveList<int>(GlobalMemoryManager.Instance);
            for (int i = 0; i < offsets.Length; i++)
            {
                offsetList.Add(offsets[i]);
            }
            return new ColumnWithOffset(BuildInt64Column(innerValues), offsetList);
        }

        /// <summary>
        /// Probes are laid out already in region order so the sorted lookup is the identity.
        /// </summary>
        private static (int[] lower, int[] upper) Search(IColumn treeColumn, int treeCount, IColumn probeColumn, int probeCount, SortColumnDirection direction)
        {
            var columnOrder = new int[] { 0 };
            var directions = new[] { direction };
            var search = new ColumnBoundarySearch(columnOrder, columnOrder, directions);

            var sortedLookup = new int[probeCount];
            for (int i = 0; i < probeCount; i++)
            {
                sortedLookup[i] = i;
            }
            int[] lowerBounds = new int[probeCount];
            int[] upperBounds = new int[probeCount];
            int[] buffer = new int[probeCount];

            search.SearchBoundries(
                new IColumn[] { treeColumn },
                new IColumn[] { probeColumn },
                sortedLookup,
                lowerBounds,
                upperBounds,
                0,
                treeCount - 1,
                false,
                buffer);

            return (lowerBounds, upperBounds);
        }

        // RED: a null padded ColumnWithOffset is misread as null free so asc nulls last collapses to nulls first and the null probe lands at the front.
        [Fact]
        public void AscendingNullsLastWithOffsetNullProbeInsertsAfterValues()
        {
            var tree = BuildInt64Column(10, 20, 30);
            var probes = BuildNullPaddedProbes(new long[] { 15, 25 }, new int[] { 0, 1, -1 });

            var (lower, upper) = Search(tree, 3, probes, 3, SortColumnDirection.AscendingNullsLast);

            Assert.Equal(~1, lower[0]);
            Assert.Equal(~1, upper[0]);
            Assert.Equal(~2, lower[1]);
            Assert.Equal(~2, upper[1]);

            // Nulls last, the null belongs after all three values.
            Assert.Equal(~3, lower[2]);
            Assert.Equal(~3, upper[2]);
        }

        // RED: the descending offset delegate reuses the ascending nulls first insertion point so a desc nulls last null probe lands at the front.
        [Fact]
        public void DescendingNullsLastWithOffsetNullProbeInsertsAfterValues()
        {
            var tree = BuildInt64Column(30, 20, 10);
            var probes = BuildNullPaddedProbes(new long[] { 25, 15 }, new int[] { 0, 1, -1 });

            var (lower, upper) = Search(tree, 3, probes, 3, SortColumnDirection.DescendingNullsLast);

            Assert.Equal(~1, lower[0]);
            Assert.Equal(~1, upper[0]);
            Assert.Equal(~2, lower[1]);
            Assert.Equal(~2, upper[1]);

            // Nulls last, the null belongs after all three values.
            Assert.Equal(~3, lower[2]);
            Assert.Equal(~3, upper[2]);
        }

        // Control, nulls first is the placement the offset delegate hard codes so this one is already correct.
        [Fact]
        public void AscendingNullsFirstWithOffsetNullProbeInsertsBeforeValues()
        {
            var tree = BuildInt64Column(10, 20, 30);
            var probes = BuildNullPaddedProbes(new long[] { 15, 25 }, new int[] { -1, 0, 1 });

            var (lower, upper) = Search(tree, 3, probes, 3, SortColumnDirection.AscendingNullsFirst);

            // Nulls first, the null belongs before all three values.
            Assert.Equal(~0, lower[0]);
            Assert.Equal(~0, upper[0]);

            Assert.Equal(~1, lower[1]);
            Assert.Equal(~1, upper[1]);
            Assert.Equal(~2, lower[2]);
            Assert.Equal(~2, upper[2]);
        }
    }
}
