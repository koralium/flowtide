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
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Tree;

namespace FlowtideDotNet.Core.Tests.ColumnStore
{
    /// <summary>
    /// An empty key container is the normal state of the root leaf before the first insert,
    /// the boundary search must report not found instead of a bound inside the container.
    /// </summary>
    public class ColumnComparerBoundaryTests
    {
        private static ColumnRowReference CreateKey(long value)
        {
            var column = Column.Create(GlobalMemoryManager.Instance);
            column.Add(new Int64Value(value));
            return new ColumnRowReference()
            {
                referenceBatch = new EventBatchData(new IColumn[] { column }),
                RowIndex = 0
            };
        }

        [Fact]
        public void FindBoundriesOnEmptyContainerReportsNotFound()
        {
            var comparer = new ColumnComparer(1);
            var container = new ColumnKeyStorageContainer(1, GlobalMemoryManager.Instance);
            var key = CreateKey(5);

            var result = comparer.FindBoundries(key, container, 0, container.Count - 1);

            // Not found, the insert position is the bitwise complement of index 0
            Assert.True(result.lowerBounds < 0);
            Assert.Equal(0, ~result.lowerBounds);
            Assert.True(result.upperBounds < 0);
        }

        [Fact]
        public void FindBoundriesBulkOnEmptyContainerReportsNotFound()
        {
            IBplusTreeComparer<ColumnRowReference, ColumnKeyStorageContainer> comparer = new ColumnComparer(1);
            var container = new ColumnKeyStorageContainer(1, GlobalMemoryManager.Instance);
            var keys = new ColumnRowReference[] { CreateKey(5) };

            var lowerBounds = new int[1];
            var upperBounds = new int[1];
            var lookupBuffer = new int[1];

            comparer.FindBoundriesBulk(keys, new int[] { 0 }, container, lowerBounds, upperBounds, lookupBuffer);

            Assert.True(lowerBounds[0] < 0);
            Assert.Equal(0, ~lowerBounds[0]);
        }

        [Fact]
        public void FindBoundriesBulkWithNoLookupsDoesNothing()
        {
            IBplusTreeComparer<ColumnRowReference, ColumnKeyStorageContainer> comparer = new ColumnComparer(1);
            var container = new ColumnKeyStorageContainer(1, GlobalMemoryManager.Instance);
            var keys = new ColumnRowReference[] { CreateKey(5) };

            comparer.FindBoundriesBulk(keys, Array.Empty<int>(), container, Array.Empty<int>(), Array.Empty<int>(), Array.Empty<int>());
        }

        [Fact]
        public void FindBoundriesFindsTheRowInANonEmptyContainer()
        {
            var comparer = new ColumnComparer(1);
            var container = new ColumnKeyStorageContainer(1, GlobalMemoryManager.Instance);
            container.Add(CreateKey(1));
            container.Add(CreateKey(3));
            container.Add(CreateKey(5));

            var found = comparer.FindBoundries(CreateKey(3), container, 0, container.Count - 1);
            Assert.Equal(1, found.lowerBounds);
            Assert.Equal(1, found.upperBounds);

            var missing = comparer.FindBoundries(CreateKey(4), container, 0, container.Count - 1);
            Assert.True(missing.lowerBounds < 0);
            Assert.Equal(2, ~missing.lowerBounds);
        }
    }
}
