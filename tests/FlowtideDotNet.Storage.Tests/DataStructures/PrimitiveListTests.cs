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

using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Memory;

namespace FlowtideDotNet.Storage.Tests.DataStructures
{
    public class PrimitiveListTests
    {
        [Fact]
        public void TestInsertRangeFrom()
        {
            var allocator = GlobalMemoryManager.Instance;
            using var longList = new PrimitiveList<int>(allocator);
            using var otherList = new PrimitiveList<int>(allocator);

            for (int i = 0; i < 1_000; i++)
            {
                longList.Add(i);
            }

            for (int i = 0; i < 1_000; i++)
            {
                otherList.Add(i + 1_000);
            }

            longList.InsertRangeFrom(500, otherList, 100, 500);

            Assert.Equal(1_500, longList.Count);

            for (int i = 0; i < 500; i++)
            {
                Assert.Equal(i, longList[i]);
            }

            for (int i = 0; i < 500; i++)
            {
                Assert.Equal(i + 1100, longList[i + 500]);
            }

            for (int i = 500; i < 1_000; i++)
            {
                Assert.Equal(i, longList[i + 500]);
            }
        }

        [Fact]
        public void TestInsertFromBasicInterleaved()
        {
            var allocator = GlobalMemoryManager.Instance;
            using var list = new PrimitiveList<int>(allocator);
            using var other = new PrimitiveList<int>(allocator);

            for (int i = 0; i < 5; i++)
                list.Add(i);

            other.Add(10);
            other.Add(20);
            other.Add(30);

            ReadOnlySpan<int> sortedLookup = stackalloc int[] { 0, 1, 2 };
            ReadOnlySpan<int> insertPositions = stackalloc int[] { 1, 3, 5 };

            list.InsertFrom(in other, sortedLookup, insertPositions, -1);

            Assert.Equal(8, list.Count);
            Assert.Equal(0, list[0]);
            Assert.Equal(10, list[1]);
            Assert.Equal(1, list[2]);
            Assert.Equal(2, list[3]);
            Assert.Equal(20, list[4]);
            Assert.Equal(3, list[5]);
            Assert.Equal(4, list[6]);
            Assert.Equal(30, list[7]);
        }

        [Fact]
        public void TestInsertFromAllAtStart()
        {
            var allocator = GlobalMemoryManager.Instance;
            using var list = new PrimitiveList<int>(allocator);
            using var other = new PrimitiveList<int>(allocator);

            for (int i = 0; i < 3; i++)
                list.Add(i);

            other.Add(100);
            other.Add(200);
            other.Add(300);

            ReadOnlySpan<int> sortedLookup = stackalloc int[] { 0, 1, 2 };
            ReadOnlySpan<int> insertPositions = stackalloc int[] { 0, 0, 0 };

            list.InsertFrom(in other, sortedLookup, insertPositions, -1);

            Assert.Equal(6, list.Count);
            Assert.Equal(100, list[0]);
            Assert.Equal(200, list[1]);
            Assert.Equal(300, list[2]);
            Assert.Equal(0, list[3]);
            Assert.Equal(1, list[4]);
            Assert.Equal(2, list[5]);
        }

        [Fact]
        public void TestInsertFromAllAtEnd()
        {
            var allocator = GlobalMemoryManager.Instance;
            using var list = new PrimitiveList<int>(allocator);
            using var other = new PrimitiveList<int>(allocator);

            for (int i = 0; i < 3; i++)
                list.Add(i);

            other.Add(100);
            other.Add(200);

            ReadOnlySpan<int> sortedLookup = stackalloc int[] { 0, 1 };
            ReadOnlySpan<int> insertPositions = stackalloc int[] { 3, 3 };

            list.InsertFrom(in other, sortedLookup, insertPositions, -1);

            Assert.Equal(5, list.Count);
            Assert.Equal(0, list[0]);
            Assert.Equal(1, list[1]);
            Assert.Equal(2, list[2]);
            Assert.Equal(100, list[3]);
            Assert.Equal(200, list[4]);
        }

        [Fact]
        public void TestInsertFromNonSequentialLookup()
        {
            var allocator = GlobalMemoryManager.Instance;
            using var list = new PrimitiveList<int>(allocator);
            using var other = new PrimitiveList<int>(allocator);

            for (int i = 0; i < 3; i++)
                list.Add(i);

            other.Add(10);
            other.Add(20);

            ReadOnlySpan<int> sortedLookup = stackalloc int[] { 1, 0 };
            ReadOnlySpan<int> insertPositions = stackalloc int[] { 1, 2 };

            list.InsertFrom(in other, sortedLookup, insertPositions, -1);

            Assert.Equal(5, list.Count);
            Assert.Equal(0, list[0]);
            Assert.Equal(20, list[1]);
            Assert.Equal(1, list[2]);
            Assert.Equal(10, list[3]);
            Assert.Equal(2, list[4]);
        }

        [Fact]
        public void TestInsertFromSingleElement()
        {
            var allocator = GlobalMemoryManager.Instance;
            using var list = new PrimitiveList<int>(allocator);
            using var other = new PrimitiveList<int>(allocator);

            list.Add(1);
            list.Add(3);
            other.Add(2);

            ReadOnlySpan<int> sortedLookup = stackalloc int[] { 0 };
            ReadOnlySpan<int> insertPositions = stackalloc int[] { 1 };

            list.InsertFrom(in other, sortedLookup, insertPositions, -1);

            Assert.Equal(3, list.Count);
            Assert.Equal(1, list[0]);
            Assert.Equal(2, list[1]);
            Assert.Equal(3, list[2]);
        }

        [Fact]
        public void TestInsertFromEmptyOther()
        {
            var allocator = GlobalMemoryManager.Instance;
            using var list = new PrimitiveList<int>(allocator);
            using var other = new PrimitiveList<int>(allocator);

            list.Add(1);
            list.Add(2);

            ReadOnlySpan<int> sortedLookup = ReadOnlySpan<int>.Empty;
            ReadOnlySpan<int> insertPositions = ReadOnlySpan<int>.Empty;

            list.InsertFrom(in other, sortedLookup, insertPositions, -1);

            Assert.Equal(2, list.Count);
            Assert.Equal(1, list[0]);
            Assert.Equal(2, list[1]);
        }

        [Fact]
        public void TestInsertFromLargeScale()
        {
            var allocator = GlobalMemoryManager.Instance;
            using var list = new PrimitiveList<int>(allocator);
            using var other = new PrimitiveList<int>(allocator);

            for (int i = 0; i < 1_000; i++)
                list.Add(i * 2);

            for (int i = 0; i < 1_000; i++)
                other.Add(i * 2 + 1);

            var sortedLookup = new int[1_000];
            var insertPositions = new int[1_000];
            for (int i = 0; i < 1_000; i++)
            {
                sortedLookup[i] = i;
                insertPositions[i] = i + 1; 
            }

            list.InsertFrom(in other, sortedLookup.AsSpan(), insertPositions.AsSpan(), -1);

            Assert.Equal(2_000, list.Count);
            for (int i = 0; i < 2_000; i++)
            {
                Assert.Equal(i, list[i]);
            }
        }

        [Fact]
        public void TestInsertFromSubset()
        {
            var allocator = GlobalMemoryManager.Instance;
            using var list = new PrimitiveList<int>(allocator);
            using var other = new PrimitiveList<int>(allocator);

            for (int i = 0; i < 4; i++)
                list.Add(i);

            other.Add(10);
            other.Add(20);
            other.Add(30);
            other.Add(40);
            other.Add(50);

            ReadOnlySpan<int> sortedLookup = stackalloc int[] { 1, 3 };
            ReadOnlySpan<int> insertPositions = stackalloc int[] { 2, 4 };

            list.InsertFrom(in other, sortedLookup, insertPositions, -1);

            Assert.Equal(6, list.Count);
            Assert.Equal(0, list[0]);
            Assert.Equal(1, list[1]);
            Assert.Equal(20, list[2]);
            Assert.Equal(2, list[3]);
            Assert.Equal(3, list[4]);
            Assert.Equal(40, list[5]);
        }

        [Fact]
        public void TestInsertFromIntoEmptyList()
        {
            var allocator = GlobalMemoryManager.Instance;
            using var list = new PrimitiveList<int>(allocator);
            using var other = new PrimitiveList<int>(allocator);

            other.Add(10);
            other.Add(20);
            other.Add(30);

            ReadOnlySpan<int> sortedLookup = stackalloc int[] { 0, 1, 2 };
            ReadOnlySpan<int> insertPositions = stackalloc int[] { 0, 0, 0 };

            list.InsertFrom(in other, sortedLookup, insertPositions, -1);

            Assert.Equal(3, list.Count);
            Assert.Equal(10, list[0]);
            Assert.Equal(20, list[1]);
            Assert.Equal(30, list[2]);
        }

        [Fact]
        public void TestInsertFromConsecutiveAtSameMiddlePosition()
        {
            var allocator = GlobalMemoryManager.Instance;
            using var list = new PrimitiveList<int>(allocator);
            using var other = new PrimitiveList<int>(allocator);

            for (int i = 0; i < 5; i++)
                list.Add(i);

            other.Add(100);
            other.Add(200);
            other.Add(300);

            ReadOnlySpan<int> sortedLookup = stackalloc int[] { 0, 1, 2 };
            ReadOnlySpan<int> insertPositions = stackalloc int[] { 2, 2, 2 };

            list.InsertFrom(in other, sortedLookup, insertPositions, -1);

            Assert.Equal(8, list.Count);
            Assert.Equal(0, list[0]);
            Assert.Equal(1, list[1]);
            Assert.Equal(100, list[2]);
            Assert.Equal(200, list[3]);
            Assert.Equal(300, list[4]);
            Assert.Equal(2, list[5]);
            Assert.Equal(3, list[6]);
            Assert.Equal(4, list[7]);
        }

        [Fact]
        public void TestDeleteBatchEmptyTargets()
        {
            var allocator = GlobalMemoryManager.Instance;
            using var list = new PrimitiveList<int>(allocator);

            for (int i = 0; i < 5; i++)
                list.Add(i);

            ReadOnlySpan<int> targets = stackalloc int[0];
            list.DeleteBatch(targets);

            Assert.Equal(5, list.Count);
            for (int i = 0; i < 5; i++)
                Assert.Equal(i, list[i]);
        }

        [Fact]
        public void TestDeleteBatchSingleElementAtBeginning()
        {
            var allocator = GlobalMemoryManager.Instance;
            using var list = new PrimitiveList<int>(allocator);

            for (int i = 0; i < 5; i++)
                list.Add(i);

            ReadOnlySpan<int> targets = stackalloc int[] { 0 };
            list.DeleteBatch(targets);

            Assert.Equal(4, list.Count);
            Assert.Equal(1, list[0]);
            Assert.Equal(2, list[1]);
            Assert.Equal(3, list[2]);
            Assert.Equal(4, list[3]);
        }

        [Fact]
        public void TestDeleteBatchSingleElementInMiddle()
        {
            var allocator = GlobalMemoryManager.Instance;
            using var list = new PrimitiveList<int>(allocator);

            for (int i = 0; i < 5; i++)
                list.Add(i);

            ReadOnlySpan<int> targets = stackalloc int[] { 2 };
            list.DeleteBatch(targets);

            Assert.Equal(4, list.Count);
            Assert.Equal(0, list[0]);
            Assert.Equal(1, list[1]);
            Assert.Equal(3, list[2]);
            Assert.Equal(4, list[3]);
        }

        [Fact]
        public void TestDeleteBatchSingleElementAtEnd()
        {
            var allocator = GlobalMemoryManager.Instance;
            using var list = new PrimitiveList<int>(allocator);

            for (int i = 0; i < 5; i++)
                list.Add(i);

            ReadOnlySpan<int> targets = stackalloc int[] { 4 };
            list.DeleteBatch(targets);

            Assert.Equal(4, list.Count);
            Assert.Equal(0, list[0]);
            Assert.Equal(1, list[1]);
            Assert.Equal(2, list[2]);
            Assert.Equal(3, list[3]);
        }

        [Fact]
        public void TestDeleteBatchMultiplePositions()
        {
            var allocator = GlobalMemoryManager.Instance;
            using var list = new PrimitiveList<int>(allocator);

            for (int i = 0; i < 8; i++)
                list.Add(i);

            // Delete first, middle, and last
            ReadOnlySpan<int> targets = stackalloc int[] { 0, 4, 7 };
            list.DeleteBatch(targets);

            Assert.Equal(5, list.Count);
            Assert.Equal(1, list[0]);
            Assert.Equal(2, list[1]);
            Assert.Equal(3, list[2]);
            Assert.Equal(5, list[3]);
            Assert.Equal(6, list[4]);
        }

        [Fact]
        public void TestDeleteBatchConsecutive()
        {
            var allocator = GlobalMemoryManager.Instance;
            using var list = new PrimitiveList<int>(allocator);

            for (int i = 0; i < 8; i++)
                list.Add(i);

            // Delete consecutive elements in the middle
            ReadOnlySpan<int> targets = stackalloc int[] { 2, 3, 4, 5 };
            list.DeleteBatch(targets);

            Assert.Equal(4, list.Count);
            Assert.Equal(0, list[0]);
            Assert.Equal(1, list[1]);
            Assert.Equal(6, list[2]);
            Assert.Equal(7, list[3]);
        }

        [Fact]
        public void TestDeleteBatchInterleaved()
        {
            var allocator = GlobalMemoryManager.Instance;
            using var list = new PrimitiveList<int>(allocator);

            for (int i = 0; i < 8; i++)
                list.Add(i);

            // Delete every other element
            ReadOnlySpan<int> targets = stackalloc int[] { 0, 2, 4, 6 };
            list.DeleteBatch(targets);

            Assert.Equal(4, list.Count);
            Assert.Equal(1, list[0]);
            Assert.Equal(3, list[1]);
            Assert.Equal(5, list[2]);
            Assert.Equal(7, list[3]);
        }

        [Fact]
        public void TestDeleteBatchAllElements()
        {
            var allocator = GlobalMemoryManager.Instance;
            using var list = new PrimitiveList<int>(allocator);

            for (int i = 0; i < 5; i++)
                list.Add(i);

            ReadOnlySpan<int> targets = stackalloc int[] { 0, 1, 2, 3, 4 };
            list.DeleteBatch(targets);

            Assert.Empty(list);
        }

        [Fact]
        public void TestDeleteBatchAllButOne()
        {
            var allocator = GlobalMemoryManager.Instance;
            using var list = new PrimitiveList<int>(allocator);

            for (int i = 0; i < 5; i++)
                list.Add(i);

            // Keep only the middle element
            ReadOnlySpan<int> targets = stackalloc int[] { 0, 1, 3, 4 };
            list.DeleteBatch(targets);

            Assert.Single(list);
            Assert.Equal(2, list[0]);
        }

        [Fact]
        public void TestDeleteBatchSingleElementList()
        {
            var allocator = GlobalMemoryManager.Instance;
            using var list = new PrimitiveList<int>(allocator);

            list.Add(42);

            ReadOnlySpan<int> targets = stackalloc int[] { 0 };
            list.DeleteBatch(targets);

            Assert.Empty(list);
        }

        [Fact]
        public void TestDeleteBatchThenAdd()
        {
            var allocator = GlobalMemoryManager.Instance;
            using var list = new PrimitiveList<int>(allocator);

            for (int i = 0; i < 5; i++)
                list.Add(i);

            ReadOnlySpan<int> targets = stackalloc int[] { 1, 3 };
            list.DeleteBatch(targets);

            Assert.Equal(3, list.Count);
            Assert.Equal(0, list[0]);
            Assert.Equal(2, list[1]);
            Assert.Equal(4, list[2]);

            // Verify the list is still usable
            list.Add(99);
            Assert.Equal(4, list.Count);
            Assert.Equal(99, list[3]);
        }

        [Fact]
        public void TestDeleteBatchTwoConsecutiveCalls()
        {
            var allocator = GlobalMemoryManager.Instance;
            using var list = new PrimitiveList<int>(allocator);

            for (int i = 0; i < 8; i++)
                list.Add(i);

            // First batch: delete indices 1 and 5 -> leaves [0, 2, 3, 4, 6, 7]
            ReadOnlySpan<int> targets1 = stackalloc int[] { 1, 5 };
            list.DeleteBatch(targets1);

            Assert.Equal(6, list.Count);
            Assert.Equal(0, list[0]);
            Assert.Equal(2, list[1]);
            Assert.Equal(3, list[2]);
            Assert.Equal(4, list[3]);
            Assert.Equal(6, list[4]);
            Assert.Equal(7, list[5]);

            // Second batch: delete index 2 from the remaining list -> leaves [0, 2, 4, 6, 7]
            ReadOnlySpan<int> targets2 = stackalloc int[] { 2 };
            list.DeleteBatch(targets2);

            Assert.Equal(5, list.Count);
            Assert.Equal(0, list[0]);
            Assert.Equal(2, list[1]);
            Assert.Equal(4, list[2]);
            Assert.Equal(6, list[3]);
            Assert.Equal(7, list[4]);
        }

        [Fact]
        public void TestDeleteBatchLargeScale()
        {
            var allocator = GlobalMemoryManager.Instance;
            using var list = new PrimitiveList<int>(allocator);

            var expected = new List<int>();
            for (int i = 0; i < 1_000; i++)
            {
                list.Add(i);
                expected.Add(i);
            }

            // Delete every third element (indices 0, 3, 6, 9, ...)
            var targetList = new List<int>();
            for (int i = 0; i < 1_000; i += 3)
            {
                targetList.Add(i);
            }

            list.DeleteBatch(targetList.ToArray().AsSpan());

            // Build expected by removing in reverse
            for (int i = targetList.Count - 1; i >= 0; i--)
            {
                expected.RemoveAt(targetList[i]);
            }

            Assert.Equal(expected.Count, list.Count);
            for (int i = 0; i < expected.Count; i++)
            {
                Assert.Equal(expected[i], list[i]);
            }

            // Verify list is still usable after batch delete
            list.Add(9999);
            Assert.Equal(9999, list[list.Count - 1]);
        }

        [Fact]
        public void TestDeleteBatchLeadingConsecutive()
        {
            var allocator = GlobalMemoryManager.Instance;
            using var list = new PrimitiveList<int>(allocator);

            for (int i = 0; i < 8; i++)
                list.Add(i);

            // Delete the first four elements
            ReadOnlySpan<int> targets = stackalloc int[] { 0, 1, 2, 3 };
            list.DeleteBatch(targets);

            Assert.Equal(4, list.Count);
            Assert.Equal(4, list[0]);
            Assert.Equal(5, list[1]);
            Assert.Equal(6, list[2]);
            Assert.Equal(7, list[3]);
        }

        [Fact]
        public void TestDeleteBatchTrailingConsecutive()
        {
            var allocator = GlobalMemoryManager.Instance;
            using var list = new PrimitiveList<int>(allocator);

            for (int i = 0; i < 8; i++)
                list.Add(i);

            // Delete the last four elements
            ReadOnlySpan<int> targets = stackalloc int[] { 4, 5, 6, 7 };
            list.DeleteBatch(targets);

            Assert.Equal(4, list.Count);
            Assert.Equal(0, list[0]);
            Assert.Equal(1, list[1]);
            Assert.Equal(2, list[2]);
            Assert.Equal(3, list[3]);
        }

        [Fact]
        public void TestInsertFromWithNullIndex()
        {
            var allocator = GlobalMemoryManager.Instance;
            using var list = new PrimitiveList<int>(allocator);
            using var other = new PrimitiveList<int>(allocator);

            for (int i = 0; i < 3; i++)
                list.Add(i + 1); // 1, 2, 3

            other.Add(100);
            other.Add(200);

            ReadOnlySpan<int> sortedLookup = stackalloc int[] { -2, 0, -2, 1, -2 };
            ReadOnlySpan<int> insertPositions = stackalloc int[] { 0, 1, 1, 3, 3 };

            list.InsertFrom(in other, in sortedLookup, in insertPositions, -2);

            Assert.Equal(8, list.Count);

            Assert.Equal(0, list[0]);
            Assert.Equal(1, list[1]);
            Assert.Equal(100, list[2]);
            Assert.Equal(0, list[3]);
            Assert.Equal(2, list[4]);
            Assert.Equal(3, list[5]);
            Assert.Equal(200, list[6]);
            Assert.Equal(0, list[7]);
        }
    }
}
