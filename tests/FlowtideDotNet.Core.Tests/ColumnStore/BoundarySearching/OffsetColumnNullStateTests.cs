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
    /// <summary>
    /// An offset column carries nulls as a -1 offset, so its state must say whether it holds one.
    /// Reporting a null free column as null capable costs the specialized search and radix sort.
    /// </summary>
    public class OffsetColumnNullStateTests
    {
        private static Column CreateValueColumn()
        {
            var column = new Column(GlobalMemoryManager.Instance);
            column.Add(new Int64Value(10));
            column.Add(new Int64Value(20));
            column.Add(new Int64Value(30));
            return column;
        }

        private static ColumnWithOffset CreateOffsetColumn(bool withNull)
        {
            var offsets = new PrimitiveList<int>(GlobalMemoryManager.Instance);
            offsets.Add(0);
            offsets.Add(1);
            offsets.Add(withNull ? ColumnWithOffset.NullValueIndex : 2);

            return new ColumnWithOffset(CreateValueColumn(), offsets);
        }

        // The registered keys pair a tree and an input column of the same layout
        private static int KeyAgainstMatchingTree(ColumnWithOffset column)
        {
            using var tree = CreateValueColumn();
            return ColumnBoundarySearchDelegates.GetKey(tree.GetColumnState(), column.GetColumnState());
        }

        [Fact]
        public void NullFreeOffsetColumnIsNotNullCapable()
        {
            using var column = CreateOffsetColumn(withNull: false);

            Assert.False(CompareColumnStateBuilder.CanContainNull(column.GetColumnState()));
        }

        [Fact]
        public void OffsetColumnHoldingANullIsNullCapable()
        {
            using var column = CreateOffsetColumn(withNull: true);

            Assert.True(CompareColumnStateBuilder.CanContainNull(column.GetColumnState()));
        }

        /// <summary>
        /// The left join case. A null bearing offset column must keep hitting the offsets aware
        /// delegate, a new state bit that misses the table would silently drop it to the fallback.
        /// </summary>
        [Fact]
        public void OffsetColumnHoldingANullKeepsItsSpecializedDelegate()
        {
            using var column = CreateOffsetColumn(withNull: true);

            var key = KeyAgainstMatchingTree(column);

            Assert.True(ColumnBoundarySearchDelegates.TryGetDelegate(key, out _), "The offset delegate was not registered for this state");
        }

        [Fact]
        public void NullFreeOffsetColumnKeepsItsSpecializedDelegate()
        {
            using var column = CreateOffsetColumn(withNull: false);

            var key = KeyAgainstMatchingTree(column);

            Assert.True(ColumnBoundarySearchDelegates.TryGetDelegate(key, out _), "The offset delegate was not registered for this state");
        }
    }
}
