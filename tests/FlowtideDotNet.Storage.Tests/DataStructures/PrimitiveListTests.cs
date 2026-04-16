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

            Span<int> sortedLookup = stackalloc int[] { 0, 1, 2 };
            Span<int> insertPositions = stackalloc int[] { 1, 3, 5 };

            list.InsertFrom(other, sortedLookup, insertPositions);

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

            Span<int> sortedLookup = stackalloc int[] { 0, 1, 2 };
            Span<int> insertPositions = stackalloc int[] { 0, 0, 0 };

            list.InsertFrom(other, sortedLookup, insertPositions);

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

            Span<int> sortedLookup = stackalloc int[] { 0, 1 };
            Span<int> insertPositions = stackalloc int[] { 3, 3 };

            list.InsertFrom(other, sortedLookup, insertPositions);

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

            Span<int> sortedLookup = stackalloc int[] { 1, 0 };
            Span<int> insertPositions = stackalloc int[] { 1, 2 };

            list.InsertFrom(other, sortedLookup, insertPositions);

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

            Span<int> sortedLookup = stackalloc int[] { 0 };
            Span<int> insertPositions = stackalloc int[] { 1 };

            list.InsertFrom(other, sortedLookup, insertPositions);

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

            Span<int> sortedLookup = Span<int>.Empty;
            Span<int> insertPositions = Span<int>.Empty;

            list.InsertFrom(other, sortedLookup, insertPositions);

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

            list.InsertFrom(other, sortedLookup.AsSpan(), insertPositions.AsSpan());

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

            Span<int> sortedLookup = stackalloc int[] { 1, 3 };
            Span<int> insertPositions = stackalloc int[] { 2, 4 };

            list.InsertFrom(other, sortedLookup, insertPositions);

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

            Span<int> sortedLookup = stackalloc int[] { 0, 1, 2 };
            Span<int> insertPositions = stackalloc int[] { 0, 0, 0 };

            list.InsertFrom(other, sortedLookup, insertPositions);

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

            Span<int> sortedLookup = stackalloc int[] { 0, 1, 2 };
            Span<int> insertPositions = stackalloc int[] { 2, 2, 2 };

            list.InsertFrom(other, sortedLookup, insertPositions);

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
    }
}
