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
using FlowtideDotNet.Storage.Memory;

namespace FlowtideDotNet.Core.Tests.ColumnStore
{
    /// <summary>
    /// Tests for <see cref="BoundarySearchHybridPrimitiveNoNull{T}.SearchBoundries_Hybrid"/> with T=int.
    /// All columns contain at least one value > short.MaxValue to ensure Int32 internal storage.
    /// No null values are used; this method only operates on primitive data without nulls.
    /// </summary>
    public class HybridInt32BoundarySearchTests
    {
        /// <summary>
        /// Helper: creates a Column populated with the given Int64 values.
        /// At least one value must be > short.MaxValue to trigger Int32 storage.
        /// </summary>
        private static Column CreateInt32Column(params long[] values)
        {
            var column = Column.Create(GlobalMemoryManager.Instance);
            foreach (var v in values)
            {
                column.Add(new Int64Value(v));
            }
            return column;
        }

        /// <summary>
        /// Runs BoundarySearchHybridPrimitiveNoNull&lt;int&gt;.SearchBoundries_Hybrid and returns the lower/upper bound arrays.
        /// inputSortedLookup maps sorted input positions to actual column indices.
        /// </summary>
        private static (int[] lowerBounds, int[] upperBounds) RunHybridSearch(
            Column treeColumn,
            Column inputColumn,
            int[] inputSortedLookup)
        {
            var lower = new int[inputSortedLookup.Length];
            var upper = new int[inputSortedLookup.Length];

            // Match ColumnBoundarySearch.SearchBoundries initialization:
            // lowerBounds.Fill(start) -> 0, upperBounds.Fill(end) -> treeColumn.Count - 1
            Array.Fill(upper, treeColumn.Count - 1);

            BoundarySearchHybridPrimitiveNoNull<int>.SearchBoundries_Hybrid(
                treeColumn,
                inputColumn,
                inputSortedLookup,
                lower,
                upper,
                new DataValueContainer(),
                new DataValueContainer(),
                false,
                Array.Empty<int>());

            return (lower, upper);
        }

        // -----------------------------------------------------------------
        // Basic matching: single unique values, values found and not found
        // -----------------------------------------------------------------

        /// <summary>
        /// Search for each value that exists in the tree. All are unique so lower == upper.
        /// </summary>
        [Fact]
        public void TestHybridInt32SingleMatchPerValue()
        {
            // Tree: [100000, 100001, 100002, 100003]
            using var tree = CreateInt32Column(100000, 100001, 100002, 100003);

            // Input: search for each value in sorted order
            using var input = CreateInt32Column(100000, 100001, 100002, 100003);
            var lookup = new int[] { 0, 1, 2, 3 };

            var (lower, upper) = RunHybridSearch(tree, input, lookup);

            Assert.Equal(0, lower[0]); Assert.Equal(0, upper[0]);
            Assert.Equal(1, lower[1]); Assert.Equal(1, upper[1]);
            Assert.Equal(2, lower[2]); Assert.Equal(2, upper[2]);
            Assert.Equal(3, lower[3]); Assert.Equal(3, upper[3]);
        }

        /// <summary>
        /// Search for a value below all tree values. Lower and upper should be complement of insertion point.
        /// </summary>
        [Fact]
        public void TestHybridInt32ValueBelowAllTreeValues()
        {
            // Tree: [100000, 100001, 100002]
            using var tree = CreateInt32Column(100000, 100001, 100002);

            // Input: search for 99999, which is below all tree values
            using var input = CreateInt32Column(99999);
            var lookup = new int[] { 0 };

            var (lower, upper) = RunHybridSearch(tree, input, lookup);

            Assert.Equal(~0, lower[0]);
            Assert.Equal(~0, upper[0]);
        }

        /// <summary>
        /// Search for a value above all tree values. Lower and upper should be complement of end+1.
        /// </summary>
        [Fact]
        public void TestHybridInt32ValueAboveAllTreeValues()
        {
            // Tree: [100000, 100001, 100002]
            using var tree = CreateInt32Column(100000, 100001, 100002);

            // Input: search for 100003, which is above all tree values
            using var input = CreateInt32Column(100003);
            var lookup = new int[] { 0 };

            var (lower, upper) = RunHybridSearch(tree, input, lookup);

            Assert.Equal(~3, lower[0]);
            Assert.Equal(~3, upper[0]);
        }

        /// <summary>
        /// Search for a value that falls in a gap between tree values.
        /// </summary>
        [Fact]
        public void TestHybridInt32ValueInGapBetweenTreeValues()
        {
            // Tree: [100000, 100002, 100004]  (gaps at 100001, 100003)
            using var tree = CreateInt32Column(100000, 100002, 100004);

            // Input: search for 100001 and 100003
            using var input = CreateInt32Column(100001, 100003);
            var lookup = new int[] { 0, 1 };

            var (lower, upper) = RunHybridSearch(tree, input, lookup);

            // 100001 would insert at index 1
            Assert.Equal(~1, lower[0]);
            Assert.Equal(~1, upper[0]);

            // 100003 would insert at index 2
            Assert.Equal(~2, lower[1]);
            Assert.Equal(~2, upper[1]);
        }

        // -----------------------------------------------------------------
        // Duplicates in tree column
        // -----------------------------------------------------------------

        /// <summary>
        /// Tree has duplicate values. Searching for the duplicate value should return the full range.
        /// </summary>
        [Fact]
        public void TestHybridInt32DuplicatesInTree()
        {
            // Tree: [100000, 100001, 100001, 100001, 100002]
            using var tree = CreateInt32Column(100000, 100001, 100001, 100001, 100002);

            // Input: search for 100001
            using var input = CreateInt32Column(100001);
            var lookup = new int[] { 0 };

            var (lower, upper) = RunHybridSearch(tree, input, lookup);

            // 100001 spans indices 1..3
            Assert.Equal(1, lower[0]);
            Assert.Equal(3, upper[0]);
        }

        /// <summary>
        /// Tree has duplicates at the start. Searching for the start value should return the correct range.
        /// </summary>
        [Fact]
        public void TestHybridInt32DuplicatesAtTreeStart()
        {
            // Tree: [100000, 100000, 100000, 100001, 100002]
            using var tree = CreateInt32Column(100000, 100000, 100000, 100001, 100002);

            // Input: search for 100000
            using var input = CreateInt32Column(100000);
            var lookup = new int[] { 0 };

            var (lower, upper) = RunHybridSearch(tree, input, lookup);

            Assert.Equal(0, lower[0]);
            Assert.Equal(2, upper[0]);
        }

        /// <summary>
        /// Tree has duplicates at the end. Searching for the end value should return the correct range.
        /// </summary>
        [Fact]
        public void TestHybridInt32DuplicatesAtTreeEnd()
        {
            // Tree: [100000, 100001, 100002, 100002, 100002]
            using var tree = CreateInt32Column(100000, 100001, 100002, 100002, 100002);

            // Input: search for 100002
            using var input = CreateInt32Column(100002);
            var lookup = new int[] { 0 };

            var (lower, upper) = RunHybridSearch(tree, input, lookup);

            Assert.Equal(2, lower[0]);
            Assert.Equal(4, upper[0]);
        }

        // -----------------------------------------------------------------
        // Duplicates in input
        // -----------------------------------------------------------------

        /// <summary>
        /// Input contains the same value multiple times. Each lookup entry should get the same result.
        /// </summary>
        [Fact]
        public void TestHybridInt32DuplicatesInInput()
        {
            // Tree: [100000, 100001, 100002, 100003]
            using var tree = CreateInt32Column(100000, 100001, 100002, 100003);

            // Input column has 100001 three times; all lookups point to the same value
            using var input = CreateInt32Column(100001, 100001, 100001);
            var lookup = new int[] { 0, 1, 2 };

            var (lower, upper) = RunHybridSearch(tree, input, lookup);

            // All three should find index 1
            for (int i = 0; i < 3; i++)
            {
                Assert.Equal(1, lower[i]);
                Assert.Equal(1, upper[i]);
            }
        }

        /// <summary>
        /// Input contains the same missing value multiple times. Each lookup entry should get the same complement.
        /// </summary>
        [Fact]
        public void TestHybridInt32DuplicateMissingValueInInput()
        {
            // Tree: [100000, 100002, 100004]
            using var tree = CreateInt32Column(100000, 100002, 100004);

            // Input: 100001 appears three times, none exist in tree
            using var input = CreateInt32Column(100001, 100001, 100001);
            var lookup = new int[] { 0, 1, 2 };

            var (lower, upper) = RunHybridSearch(tree, input, lookup);

            for (int i = 0; i < 3; i++)
            {
                Assert.Equal(~1, lower[i]);
                Assert.Equal(~1, upper[i]);
            }
        }

        // -----------------------------------------------------------------
        // Duplicates in both tree and input
        // -----------------------------------------------------------------

        /// <summary>
        /// Both tree and input have duplicates for the same value.
        /// Every input entry should find the full duplicate range in the tree.
        /// </summary>
        [Fact]
        public void TestHybridInt32DuplicatesInBothTreeAndInput()
        {
            // Tree: [100000, 100001, 100001, 100001, 100002]
            using var tree = CreateInt32Column(100000, 100001, 100001, 100001, 100002);

            // Input: 100001 appears three times
            using var input = CreateInt32Column(100001, 100001, 100001);
            var lookup = new int[] { 0, 1, 2 };

            var (lower, upper) = RunHybridSearch(tree, input, lookup);

            for (int i = 0; i < 3; i++)
            {
                Assert.Equal(1, lower[i]);
                Assert.Equal(3, upper[i]);
            }
        }

        /// <summary>
        /// Multiple distinct duplicate groups in both tree and input.
        /// </summary>
        [Fact]
        public void TestHybridInt32MultipleDuplicateGroupsInBoth()
        {
            // Tree: [100000, 100000, 100001, 100001, 100002, 100002, 100003]
            using var tree = CreateInt32Column(100000, 100000, 100001, 100001, 100002, 100002, 100003);

            // Input: [100000, 100000, 100001, 100002, 100002]
            using var input = CreateInt32Column(100000, 100000, 100001, 100002, 100002);
            var lookup = new int[] { 0, 1, 2, 3, 4 };

            var (lower, upper) = RunHybridSearch(tree, input, lookup);

            // 100000 at tree indices 0..1
            Assert.Equal(0, lower[0]); Assert.Equal(1, upper[0]);
            Assert.Equal(0, lower[1]); Assert.Equal(1, upper[1]);

            // 100001 at tree indices 2..3
            Assert.Equal(2, lower[2]); Assert.Equal(3, upper[2]);

            // 100002 at tree indices 4..5
            Assert.Equal(4, lower[3]); Assert.Equal(5, upper[3]);
            Assert.Equal(4, lower[4]); Assert.Equal(5, upper[4]);
        }

        // -----------------------------------------------------------------
        // Edge cases: single element, empty input, all same values
        // -----------------------------------------------------------------

        /// <summary>
        /// Tree has a single element. Search for that element.
        /// </summary>
        [Fact]
        public void TestHybridInt32SingleElementTreeMatch()
        {
            using var tree = CreateInt32Column(100000);
            using var input = CreateInt32Column(100000);
            var lookup = new int[] { 0 };

            var (lower, upper) = RunHybridSearch(tree, input, lookup);

            Assert.Equal(0, lower[0]);
            Assert.Equal(0, upper[0]);
        }

        /// <summary>
        /// Tree has a single element. Search for a value that does not exist.
        /// </summary>
        [Fact]
        public void TestHybridInt32SingleElementTreeMiss()
        {
            using var tree = CreateInt32Column(100000);
            using var input = CreateInt32Column(99999);
            var lookup = new int[] { 0 };

            var (lower, upper) = RunHybridSearch(tree, input, lookup);

            Assert.Equal(~0, lower[0]);
            Assert.Equal(~0, upper[0]);
        }

        /// <summary>
        /// Empty input should be a no-op; output arrays remain at their initial zero values.
        /// </summary>
        [Fact]
        public void TestHybridInt32EmptyInput()
        {
            using var tree = CreateInt32Column(100000, 100001, 100002);
            using var input = CreateInt32Column();
            var lookup = Array.Empty<int>();

            var (lower, upper) = RunHybridSearch(tree, input, lookup);

            Assert.Empty(lower);
            Assert.Empty(upper);
        }

        /// <summary>
        /// All tree values are identical. Search for that value should span the entire tree.
        /// </summary>
        [Fact]
        public void TestHybridInt32AllTreeValuesIdentical()
        {
            // Tree: [100000, 100000, 100000, 100000, 100000]
            using var tree = CreateInt32Column(100000, 100000, 100000, 100000, 100000);

            using var input = CreateInt32Column(100000);
            var lookup = new int[] { 0 };

            var (lower, upper) = RunHybridSearch(tree, input, lookup);

            Assert.Equal(0, lower[0]);
            Assert.Equal(4, upper[0]);
        }

        /// <summary>
        /// All tree values are identical. Search for a value that is not in the tree.
        /// </summary>
        [Fact]
        public void TestHybridInt32AllTreeValuesIdenticalMiss()
        {
            using var tree = CreateInt32Column(100000, 100000, 100000, 100000, 100000);

            using var input = CreateInt32Column(100001);
            var lookup = new int[] { 0 };

            var (lower, upper) = RunHybridSearch(tree, input, lookup);

            Assert.Equal(~5, lower[0]);
            Assert.Equal(~5, upper[0]);
        }

        // -----------------------------------------------------------------
        // Non-trivial inputSortedLookup (out-of-order mapping)
        // -----------------------------------------------------------------

        /// <summary>
        /// The inputSortedLookup remaps positions. Input column has values in a different order
        /// than the sorted order used by the lookup.
        /// </summary>
        [Fact]
        public void TestHybridInt32IndirectLookupOrder()
        {
            // Tree: [100000, 100001, 100002, 100003]
            using var tree = CreateInt32Column(100000, 100001, 100002, 100003);

            // Input column values are in reverse order: [100003, 100002, 100001, 100000]
            // The sorted lookup maps to ascending order: lookup[0]->3 (100000), lookup[1]->2 (100001), etc.
            using var input = CreateInt32Column(100003, 100002, 100001, 100000);
            var lookup = new int[] { 3, 2, 1, 0 };

            var (lower, upper) = RunHybridSearch(tree, input, lookup);

            Assert.Equal(0, lower[0]); Assert.Equal(0, upper[0]); // 100000
            Assert.Equal(1, lower[1]); Assert.Equal(1, upper[1]); // 100001
            Assert.Equal(2, lower[2]); Assert.Equal(2, upper[2]); // 100002
            Assert.Equal(3, lower[3]); Assert.Equal(3, upper[3]); // 100003
        }

        // -----------------------------------------------------------------
        // Mixed hits and misses
        // -----------------------------------------------------------------

        /// <summary>
        /// Some input values exist in the tree and some do not.
        /// </summary>
        [Fact]
        public void TestHybridInt32MixedHitsAndMisses()
        {
            // Tree: [100000, 100002, 100004, 100006]
            using var tree = CreateInt32Column(100000, 100002, 100004, 100006);

            // Input: [100000, 100001, 100002, 100003, 100004, 100005, 100006]
            using var input = CreateInt32Column(100000, 100001, 100002, 100003, 100004, 100005, 100006);
            var lookup = new int[] { 0, 1, 2, 3, 4, 5, 6 };

            var (lower, upper) = RunHybridSearch(tree, input, lookup);

            Assert.Equal(0, lower[0]); Assert.Equal(0, upper[0]);   // 100000 found at 0
            Assert.Equal(~1, lower[1]); Assert.Equal(~1, upper[1]); // 100001 not found
            Assert.Equal(1, lower[2]); Assert.Equal(1, upper[2]);   // 100002 found at 1
            Assert.Equal(~2, lower[3]); Assert.Equal(~2, upper[3]); // 100003 not found
            Assert.Equal(2, lower[4]); Assert.Equal(2, upper[4]);   // 100004 found at 2
            Assert.Equal(~3, lower[5]); Assert.Equal(~3, upper[5]); // 100005 not found
            Assert.Equal(3, lower[6]); Assert.Equal(3, upper[6]);   // 100006 found at 3
        }

        // -----------------------------------------------------------------
        // Large dataset: exercises the macro split path (>128 tree elements)
        // -----------------------------------------------------------------

        /// <summary>
        /// Large tree (200 elements) to exercise the macro binary split path.
        /// Every input value exists in the tree.
        /// </summary>
        [Fact]
        public void TestHybridInt32LargeTreeAllFound()
        {
            // Tree: [100000, 100001, ..., 100199]
            var treeValues = Enumerable.Range(100000, 200).Select(v => (long)v).ToArray();
            using var tree = CreateInt32Column(treeValues);

            // Input: same values, identity lookup
            using var input = CreateInt32Column(treeValues);
            var lookup = Enumerable.Range(0, 200).ToArray();

            var (lower, upper) = RunHybridSearch(tree, input, lookup);

            for (int i = 0; i < 200; i++)
            {
                Assert.Equal(i, lower[i]);
                Assert.Equal(i, upper[i]);
            }
        }

        /// <summary>
        /// Large tree (200 elements) with duplicates. Exercises macro split duplicate expansion.
        /// </summary>
        [Fact]
        public void TestHybridInt32LargeTreeWithDuplicates()
        {
            // Tree: each value repeated twice -> [100000, 100000, 100001, 100001, ..., 100099, 100099]
            var treeValues = Enumerable.Range(100000, 100).SelectMany(v => new long[] { v, v }).ToArray();
            using var tree = CreateInt32Column(treeValues);

            // Input: search for each unique value once
            var inputValues = Enumerable.Range(100000, 100).Select(v => (long)v).ToArray();
            using var input = CreateInt32Column(inputValues);
            var lookup = Enumerable.Range(0, 100).ToArray();

            var (lower, upper) = RunHybridSearch(tree, input, lookup);

            for (int i = 0; i < 100; i++)
            {
                Assert.Equal(i * 2, lower[i]);
                Assert.Equal(i * 2 + 1, upper[i]);
            }
        }

        /// <summary>
        /// Large tree (200 elements) where none of the input values exist.
        /// </summary>
        [Fact]
        public void TestHybridInt32LargeTreeAllMissing()
        {
            // Tree: [100000, 100002, 100004, ..., 100398] (even numbers only, 200 elements)
            var treeValues = Enumerable.Range(0, 200).Select(i => (long)(100000 + i * 2)).ToArray();
            using var tree = CreateInt32Column(treeValues);

            // Input: [100001, 100003, 100005, ..., 100399] (odd numbers, between each tree value)
            var inputValues = Enumerable.Range(0, 200).Select(i => (long)(100001 + i * 2)).ToArray();
            using var input = CreateInt32Column(inputValues);
            var lookup = Enumerable.Range(0, 200).ToArray();

            var (lower, upper) = RunHybridSearch(tree, input, lookup);

            for (int i = 0; i < 200; i++)
            {
                // Each odd value falls between tree[i] and tree[i+1], insertion point is i+1
                Assert.Equal(~(i + 1), lower[i]);
                Assert.Equal(~(i + 1), upper[i]);
            }
        }

        /// <summary>
        /// Large tree with duplicates and input with duplicates exercising macro split duplicate handling.
        /// </summary>
        [Fact]
        public void TestHybridInt32LargeTreeDuplicatesInBoth()
        {
            // Tree: each value repeated 3 times -> 150 values: [100000 x3, 100001 x3, ..., 100049 x3]
            var treeValues = Enumerable.Range(100000, 50).SelectMany(v => new long[] { v, v, v }).ToArray();
            using var tree = CreateInt32Column(treeValues);

            // Input: each value repeated 2 times -> [100000 x2, 100001 x2, ..., 100049 x2]
            var inputValues = Enumerable.Range(100000, 50).SelectMany(v => new long[] { v, v }).ToArray();
            using var input = CreateInt32Column(inputValues);
            var lookup = Enumerable.Range(0, 100).ToArray();

            var (lower, upper) = RunHybridSearch(tree, input, lookup);

            for (int i = 0; i < 50; i++)
            {
                // Each pair in input maps to a triple in tree
                int inputIdx0 = i * 2;
                int inputIdx1 = i * 2 + 1;
                int expectedLower = i * 3;
                int expectedUpper = i * 3 + 2;

                Assert.Equal(expectedLower, lower[inputIdx0]);
                Assert.Equal(expectedUpper, upper[inputIdx0]);
                Assert.Equal(expectedLower, lower[inputIdx1]);
                Assert.Equal(expectedUpper, upper[inputIdx1]);
            }
        }

        // -----------------------------------------------------------------
        // Subset of input values with mixed values
        // -----------------------------------------------------------------

        /// <summary>
        /// Input has some values smaller, some matching, and some larger than the tree range,
        /// with duplicates present in both tree and input.
        /// </summary>
        [Fact]
        public void TestHybridInt32MixedBoundaryDuplicatesBoth()
        {
            // Tree: [100000, 100001, 100001, 100002, 100002, 100002, 100003]
            using var tree = CreateInt32Column(100000, 100001, 100001, 100002, 100002, 100002, 100003);

            // Input: [99999, 100001, 100001, 100002, 100002, 100004] — below, dup match, dup match, above
            using var input = CreateInt32Column(99999, 100001, 100001, 100002, 100002, 100004);
            var lookup = new int[] { 0, 1, 2, 3, 4, 5 };

            var (lower, upper) = RunHybridSearch(tree, input, lookup);

            // 99999: below all tree values
            Assert.Equal(~0, lower[0]);
            Assert.Equal(~0, upper[0]);

            // 100001: found at tree indices 1..2
            Assert.Equal(1, lower[1]); Assert.Equal(2, upper[1]);
            Assert.Equal(1, lower[2]); Assert.Equal(2, upper[2]);

            // 100002: found at tree indices 3..5
            Assert.Equal(3, lower[3]); Assert.Equal(5, upper[3]);
            Assert.Equal(3, lower[4]); Assert.Equal(5, upper[4]);

            // 100004: above all tree values
            Assert.Equal(~7, lower[5]);
            Assert.Equal(~7, upper[5]);
        }
        // -----------------------------------------------------------------
        // Macro split path (>=128 tree elements): duplicate-specific tests
        // -----------------------------------------------------------------

        /// <summary>
        /// Large tree (200 unique elements) with a large duplicate block in the center of the input.
        /// The midpoint of the input lands on the duplicate value, forcing the macro split's
        /// input block expansion loop (inputBlockStart/inputBlockEnd) to run.
        /// </summary>
        [Fact]
        public void TestHybridInt32MacroSplitInputDuplicatesAtCenter()
        {
            // Tree: 200 unique values [100000..100199]
            var treeValues = Enumerable.Range(100000, 200).Select(v => (long)v).ToArray();
            using var tree = CreateInt32Column(treeValues);

            // Input: 50 unique ascending values, then 100 copies of 100100, then 50 unique ascending values
            // This puts a large duplicate block right at the center of the input array
            var inputList = new List<long>();
            for (int i = 0; i < 50; i++) inputList.Add(100000 + i);        // [100000..100049]
            for (int i = 0; i < 100; i++) inputList.Add(100100);           // 100x 100100
            for (int i = 0; i < 50; i++) inputList.Add(100150 + i);        // [100150..100199]
            using var input = CreateInt32Column(inputList.ToArray());
            var lookup = Enumerable.Range(0, 200).ToArray();

            var (lower, upper) = RunHybridSearch(tree, input, lookup);

            // First 50: unique values 100000..100049, each at tree index i
            for (int i = 0; i < 50; i++)
            {
                Assert.Equal(i, lower[i]);
                Assert.Equal(i, upper[i]);
            }

            // Middle 100: all searching for 100100, which is at tree index 100
            for (int i = 50; i < 150; i++)
            {
                Assert.Equal(100, lower[i]);
                Assert.Equal(100, upper[i]);
            }

            // Last 50: unique values 100150..100199, each at tree index 150+j
            for (int i = 0; i < 50; i++)
            {
                Assert.Equal(150 + i, lower[150 + i]);
                Assert.Equal(150 + i, upper[150 + i]);
            }
        }

        /// <summary>
        /// Large tree (200 elements) with a massive duplicate run in the tree center.
        /// Exercises the FindBoundsInt32_Pivot galloping upper bound search with many duplicates.
        /// </summary>
        [Fact]
        public void TestHybridInt32MacroSplitTreeDuplicatesAtCenter()
        {
            // Tree: 50 unique [100000..100049], then 100x 100050, then 50 unique [100051..100100]
            var treeList = new List<long>();
            for (int i = 0; i < 50; i++) treeList.Add(100000 + i);
            for (int i = 0; i < 100; i++) treeList.Add(100050);
            for (int i = 0; i < 50; i++) treeList.Add(100051 + i);
            using var tree = CreateInt32Column(treeList.ToArray());

            // Input: search for 100050 once — should find the full run [50..149]
            using var input = CreateInt32Column(100050);
            var lookup = new int[] { 0 };

            var (lower, upper) = RunHybridSearch(tree, input, lookup);

            Assert.Equal(50, lower[0]);
            Assert.Equal(149, upper[0]);
        }

        /// <summary>
        /// Large tree (200 elements) with tree duplicates at the center,
        /// and input has multiple entries searching for that same duplicated value.
        /// Exercises both input block expansion AND tree galloping simultaneously.
        /// </summary>
        [Fact]
        public void TestHybridInt32MacroSplitDuplicatesInBothAtCenter()
        {
            // Tree: 50 unique [100000..100049], then 100x 100050, then 50 unique [100051..100100]
            var treeList = new List<long>();
            for (int i = 0; i < 50; i++) treeList.Add(100000 + i);
            for (int i = 0; i < 100; i++) treeList.Add(100050);
            for (int i = 0; i < 50; i++) treeList.Add(100051 + i);
            using var tree = CreateInt32Column(treeList.ToArray());

            // Input: 20 unique [100000..100019], then 60x 100050, then 20 unique [100051..100070]
            var inputList = new List<long>();
            for (int i = 0; i < 20; i++) inputList.Add(100000 + i);
            for (int i = 0; i < 60; i++) inputList.Add(100050);
            for (int i = 0; i < 20; i++) inputList.Add(100051 + i);
            using var input = CreateInt32Column(inputList.ToArray());
            var lookup = Enumerable.Range(0, 100).ToArray();

            var (lower, upper) = RunHybridSearch(tree, input, lookup);

            // First 20: unique 100000..100019
            for (int i = 0; i < 20; i++)
            {
                Assert.Equal(i, lower[i]);
                Assert.Equal(i, upper[i]);
            }

            // Middle 60: all searching for 100050 -> tree indices [50..149]
            for (int i = 20; i < 80; i++)
            {
                Assert.Equal(50, lower[i]);
                Assert.Equal(149, upper[i]);
            }

            // Last 20: unique 100051..100070 -> tree indices [150..169]
            for (int i = 0; i < 20; i++)
            {
                Assert.Equal(150 + i, lower[80 + i]);
                Assert.Equal(150 + i, upper[80 + i]);
            }
        }

        /// <summary>
        /// Large tree (200 elements) where the entire input is one repeated value
        /// that does NOT exist in the tree. Every entry should get the same complement result.
        /// Exercises macro split input block expansion covering the entire input range.
        /// </summary>
        [Fact]
        public void TestHybridInt32MacroSplitAllInputDuplicatesMissing()
        {
            // Tree: 200 unique values [100000..100199]
            var treeValues = Enumerable.Range(100000, 200).Select(v => (long)v).ToArray();
            using var tree = CreateInt32Column(treeValues);

            // Input: 50 copies of 99999 (below all tree values)
            var inputValues = Enumerable.Repeat(99999L, 50).ToArray();
            using var input = CreateInt32Column(inputValues);
            var lookup = Enumerable.Range(0, 50).ToArray();

            var (lower, upper) = RunHybridSearch(tree, input, lookup);

            for (int i = 0; i < 50; i++)
            {
                Assert.Equal(~0, lower[i]);
                Assert.Equal(~0, upper[i]);
            }
        }

        /// <summary>
        /// Large tree (200 elements, all identical). Input has many duplicates of that same value.
        /// Both the macro split pivot and the galloping search must handle extreme skew.
        /// </summary>
        [Fact]
        public void TestHybridInt32MacroSplitAllIdenticalTreeAndInput()
        {
            // Tree: 200x 100000
            var treeValues = Enumerable.Repeat(100000L, 200).ToArray();
            using var tree = CreateInt32Column(treeValues);

            // Input: 50x 100000
            var inputValues = Enumerable.Repeat(100000L, 50).ToArray();
            using var input = CreateInt32Column(inputValues);
            var lookup = Enumerable.Range(0, 50).ToArray();

            var (lower, upper) = RunHybridSearch(tree, input, lookup);

            for (int i = 0; i < 50; i++)
            {
                Assert.Equal(0, lower[i]);
                Assert.Equal(199, upper[i]);
            }
        }

        // -----------------------------------------------------------------
        // Pre-set micro bounds: multi-column scenario where a previous column
        // has narrowed each entry's search range. Tree is sorted within each
        // segment but NOT globally sorted.
        // -----------------------------------------------------------------

        /// <summary>
        /// Two input entries with different pre-set bounds searching
        /// different sorted segments of the tree.
        /// Tree: [50000, 60000, 70000, 40000, 45000, 50000, 55000, 60000, 65000]
        ///        segment 0: indices [0,2]   → sorted: 50000, 60000, 70000
        ///        segment 1: indices [3,8]   → sorted: 40000, 45000, 50000, 55000, 60000, 65000
        /// Input[0] = 60000, bounds [0,2]   → should find at index 1
        /// Input[1] = 55000, bounds [3,8]   → should find at index 6
        /// </summary>
        [Fact]
        public void TestHybridInt32PreSetMicroBoundsNonGlobalOrder()
        {
            // Tree is sorted within segments but NOT globally
            using var tree = CreateInt32Column(50000, 60000, 70000, 40000, 45000, 50000, 55000, 60000, 65000);

            // Input: search for 60000 in segment [0,2], search for 55000 in segment [3,8]
            using var input = CreateInt32Column(60000, 55000);
            var lookup = new int[] { 0, 1 };

            var lower = new int[] { 0, 3 };
            var upper = new int[] { 2, 8 };

            BoundarySearchHybridPrimitiveNoNull<int>.SearchBoundries_Hybrid(
                tree, input, lookup, lower, upper,
                new DataValueContainer(), new DataValueContainer(),
                false, Array.Empty<int>());

            // Input[0] = 60000 in [0,2] → found at index 1
            Assert.Equal(1, lower[0]);
            Assert.Equal(1, upper[0]);

            // Input[1] = 55000 in [3,8] → found at index 6
            Assert.Equal(6, lower[1]);
            Assert.Equal(6, upper[1]);
        }

        /// <summary>
        /// Pre-set bounds with duplicates within segments.
        /// Tree: [50000, 50000, 60000, 35000, 40000, 40000, 40000, 50000]
        ///        segment 0: indices [0,2]   → 50000, 50000, 60000
        ///        segment 1: indices [3,7]   → 35000, 40000, 40000, 40000, 50000
        /// Input[0] = 50000, bounds [0,2]   → should find at indices 0-1
        /// Input[1] = 40000, bounds [3,7]   → should find at indices 4-6
        /// </summary>
        [Fact]
        public void TestHybridInt32PreSetMicroBoundsDuplicatesInSegments()
        {
            using var tree = CreateInt32Column(50000, 50000, 60000, 35000, 40000, 40000, 40000, 50000);

            using var input = CreateInt32Column(50000, 40000);
            var lookup = new int[] { 0, 1 };

            var lower = new int[] { 0, 3 };
            var upper = new int[] { 2, 7 };

            BoundarySearchHybridPrimitiveNoNull<int>.SearchBoundries_Hybrid(
                tree, input, lookup, lower, upper,
                new DataValueContainer(), new DataValueContainer(),
                false, Array.Empty<int>());

            // Input[0] = 50000 in [0,2] → found at indices 0..1
            Assert.Equal(0, lower[0]);
            Assert.Equal(1, upper[0]);

            // Input[1] = 40000 in [3,7] → found at indices 4..6
            Assert.Equal(4, lower[1]);
            Assert.Equal(6, upper[1]);
        }

        /// <summary>
        /// Pre-set bounds where a value is missing in its segment → complement.
        /// Tree: [50000, 60000, 70000, 40000, 45000, 55000]
        ///        segment 0: indices [0,2]   → 50000, 60000, 70000
        ///        segment 1: indices [3,5]   → 40000, 45000, 55000
        /// Input[0] = 65000, bounds [0,2]   → not found, insert at 2 → ~2
        /// Input[1] = 50000, bounds [3,5]   → not found, insert at 5 → ~5
        /// </summary>
        [Fact]
        public void TestHybridInt32PreSetMicroBoundsMissInSegment()
        {
            using var tree = CreateInt32Column(50000, 60000, 70000, 40000, 45000, 55000);

            using var input = CreateInt32Column(65000, 50000);
            var lookup = new int[] { 0, 1 };

            var lower = new int[] { 0, 3 };
            var upper = new int[] { 2, 5 };

            BoundarySearchHybridPrimitiveNoNull<int>.SearchBoundries_Hybrid(
                tree, input, lookup, lower, upper,
                new DataValueContainer(), new DataValueContainer(),
                false, Array.Empty<int>());

            // Input[0] = 65000 in [0,2] → between 60000 and 70000, insert at 2 → ~2
            Assert.Equal(~2, lower[0]);
            Assert.Equal(~2, upper[0]);

            // Input[1] = 50000 in [3,5] → between 45000 and 55000, insert at 5 → ~5
            Assert.Equal(~5, lower[1]);
            Assert.Equal(~5, upper[1]);
        }

        /// <summary>
        /// Multiple entries with varying pre-set bounds across non-globally-sorted segments.
        /// Input is sorted ascending, bounds are contiguous ascending groups.
        /// Tests that each entry independently searches only its designated segment.
        /// </summary>
        [Fact]
        public void TestHybridInt32PreSetMicroBoundsMultipleSegments()
        {
            // Three segments: [0,3], [4,7], [8,11]
            // Each sorted ascending internally but NOT globally sorted
            using var tree = CreateInt32Column(
                80000, 85000, 90000, 95000,    // segment 0: [0,3]
                40000, 45000, 50000, 55000,    // segment 1: [4,7]
                60000, 65000, 70000, 75000     // segment 2: [8,11]
            );

            // Input sorted ascending, 2 entries per segment, bounds match segment
            using var input = CreateInt32Column(85000, 90000, 45000, 50000, 65000, 70000);
            var lookup = new int[] { 0, 1, 2, 3, 4, 5 };

            var lower = new int[] { 0, 0, 4, 4, 8, 8 };
            var upper = new int[] { 3, 3, 7, 7, 11, 11 };

            BoundarySearchHybridPrimitiveNoNull<int>.SearchBoundries_Hybrid(
                tree, input, lookup, lower, upper,
                new DataValueContainer(), new DataValueContainer(),
                false, Array.Empty<int>());

            // Input[0] = 85000 in [0,3] → found at index 1
            Assert.Equal(1, lower[0]);
            Assert.Equal(1, upper[0]);

            // Input[1] = 90000 in [0,3] → found at index 2
            Assert.Equal(2, lower[1]);
            Assert.Equal(2, upper[1]);

            // Input[2] = 45000 in [4,7] → found at index 5
            Assert.Equal(5, lower[2]);
            Assert.Equal(5, upper[2]);

            // Input[3] = 50000 in [4,7] → found at index 6
            Assert.Equal(6, lower[3]);
            Assert.Equal(6, upper[3]);

            // Input[4] = 65000 in [8,11] → found at index 9
            Assert.Equal(9, lower[4]);
            Assert.Equal(9, upper[4]);

            // Input[5] = 70000 in [8,11] → found at index 10
            Assert.Equal(10, lower[5]);
            Assert.Equal(10, upper[5]);
        }
    }
}
