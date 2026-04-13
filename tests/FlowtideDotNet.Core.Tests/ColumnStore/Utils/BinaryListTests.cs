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

using FlowtideDotNet.Core.ColumnStore.Utils;
using FlowtideDotNet.Storage.Memory;

namespace FlowtideDotNet.Core.Tests.ColumnStore.Utils
{
    public class BinaryListTests
    {
        [Fact]
        public void TestAdd()
        {
            var e1 = new byte[] { 1, 2, 3 };
            var e2 = new byte[] { 1, 2, 3, 4 };
            using BinaryList binaryList = new BinaryList(GlobalMemoryManager.Instance);
            binaryList.Add(e1);
            binaryList.Add(e2);

            Assert.True(binaryList.Get(0).SequenceEqual(e1));
            Assert.True(binaryList.Get(1).SequenceEqual(e2));
        }

        [Fact]
        public void TestInsertInTheMiddle()
        {
            var e1 = new byte[] { 1, 2, 3 };
            var e2 = new byte[] { 1, 2, 3, 4 };
            var e3 = new byte[] { 1, 2, 3, 4, 5 };
            using BinaryList binaryList = new BinaryList(GlobalMemoryManager.Instance);
            binaryList.Add(e1);
            binaryList.Add(e2);
            binaryList.Insert(1, e3);

            Assert.True(binaryList.Get(0).SequenceEqual(e1));
            Assert.True(binaryList.Get(2).SequenceEqual(e2));
            Assert.True(binaryList.Get(1).SequenceEqual(e3));
        }

        [Fact]
        public void TestInsertInFirstIndex()
        {
            var e1 = new byte[] { 1, 2, 3 };
            var e2 = new byte[] { 1, 2, 3, 4 };
            var e3 = new byte[] { 1, 2, 3, 4, 5 };
            using BinaryList binaryList = new BinaryList(GlobalMemoryManager.Instance);
            binaryList.Add(e1);
            binaryList.Add(e2);
            binaryList.Insert(0, e3);

            Assert.True(binaryList.Get(1).SequenceEqual(e1));
            Assert.True(binaryList.Get(2).SequenceEqual(e2));
            Assert.True(binaryList.Get(0).SequenceEqual(e3));
        }

        [Fact]
        public void TestInsertInLastIndex()
        {
            var e1 = new byte[] { 1, 2, 3 };
            var e2 = new byte[] { 1, 2, 3, 4 };
            var e3 = new byte[] { 1, 2, 3, 4, 5 };
            using BinaryList binaryList = new BinaryList(GlobalMemoryManager.Instance);
            binaryList.Add(e1);
            binaryList.Add(e2);
            binaryList.Insert(2, e3);

            Assert.True(binaryList.Get(0).SequenceEqual(e1));
            Assert.True(binaryList.Get(1).SequenceEqual(e2));
            Assert.True(binaryList.Get(2).SequenceEqual(e3));
        }

        [Fact]
        public void TestRemoveFirst()
        {
            var e1 = new byte[] { 1, 2, 3 };
            var e2 = new byte[] { 1, 2, 3, 4 };
            var e3 = new byte[] { 1, 2, 3, 4, 5 };
            using BinaryList binaryList = new BinaryList(GlobalMemoryManager.Instance);
            binaryList.Add(e1);
            binaryList.Add(e2);
            binaryList.Add(e3);

            binaryList.RemoveAt(0);

            Assert.True(binaryList.Get(0).SequenceEqual(e2));
            Assert.True(binaryList.Get(1).SequenceEqual(e3));
        }

        [Fact]
        public void TestRemoveMiddle()
        {
            var e1 = new byte[] { 1, 2, 3 };
            var e2 = new byte[] { 1, 2, 3, 4 };
            var e3 = new byte[] { 1, 2, 3, 4, 5 };
            using BinaryList binaryList = new BinaryList(GlobalMemoryManager.Instance);
            binaryList.Add(e1);
            binaryList.Add(e2);
            binaryList.Add(e3);

            binaryList.RemoveAt(1);

            Assert.True(binaryList.Get(0).SequenceEqual(e1));
            Assert.True(binaryList.Get(1).SequenceEqual(e3));
        }

        [Fact]
        public void TestRemoveLast()
        {
            var e1 = new byte[] { 1, 2, 3 };
            var e2 = new byte[] { 1, 2, 3, 4 };
            var e3 = new byte[] { 1, 2, 3, 4, 5 };
            using BinaryList binaryList = new BinaryList(GlobalMemoryManager.Instance);
            binaryList.Add(e1);
            binaryList.Add(e2);
            binaryList.Add(e3);

            binaryList.RemoveAt(2);

            Assert.True(binaryList.Get(0).SequenceEqual(e1));
            Assert.True(binaryList.Get(1).SequenceEqual(e2));
        }

        [Fact]
        public void TestGetMemory()
        {
            var e1 = new byte[] { 1, 2, 3 };
            var e2 = new byte[] { 1, 2, 3, 4 };
            using BinaryList binaryList = new BinaryList(GlobalMemoryManager.Instance);
            binaryList.Add(e1);
            binaryList.Add(e2);

            var index1Mem = binaryList.GetMemory(0);
            var index2Mem = binaryList.GetMemory(1);
            Assert.True(binaryList.Get(0).SequenceEqual(e1));
            Assert.True(binaryList.Get(1).SequenceEqual(e2));
        }

        [Fact]
        public void TestUpdateFirst()
        {
            var e1 = new byte[] { 1, 2, 3 };
            var e2 = new byte[] { 1, 2, 3, 4 };
            using BinaryList binaryList = new BinaryList(GlobalMemoryManager.Instance);
            binaryList.Insert(0, e1);
            binaryList.UpdateAt(0, e2);

            Assert.True(binaryList.Get(0).SequenceEqual(e2));
        }

        [Fact]
        public void TestUpdateTwice()
        {
            var e1 = new byte[] { 1, 2, 3 };
            var e2 = new byte[] { 1, 2, 3, 4 };
            using BinaryList binaryList = new BinaryList(GlobalMemoryManager.Instance);
            binaryList.Insert(0, e1);
            binaryList.UpdateAt(0, e2);
            binaryList.UpdateAt(0, e1);

            Assert.True(binaryList.Get(0).SequenceEqual(e1));
        }

        [Fact]
        public void TestUpdateMultipleElements()
        {
            var e1 = new byte[] { 1, 2, 3 };
            var e2 = new byte[] { 1, 2, 3, 4 };
            var e3 = new byte[] { 1, 2, 3, 4, 5 };
            using BinaryList binaryList = new BinaryList(GlobalMemoryManager.Instance);
            binaryList.Insert(0, e1);
            binaryList.Insert(1, e3);
            binaryList.UpdateAt(0, e2);
            binaryList.UpdateAt(0, e1);

            Assert.True(binaryList.Get(0).SequenceEqual(e1));
            Assert.True(binaryList.Get(1).SequenceEqual(e3));
        }

        [Fact]
        public void TestUpdateLastElement()
        {
            var e1 = new byte[] { 1, 2, 3 };
            var e2 = new byte[] { 1, 2, 3, 4 };
            var e3 = new byte[] { 1, 2, 3, 4, 5 };
            using BinaryList binaryList = new BinaryList(GlobalMemoryManager.Instance);
            binaryList.Insert(0, e1);
            binaryList.Insert(1, e2);
            binaryList.UpdateAt(1, e3);

            Assert.True(binaryList.Get(0).SequenceEqual(e1));
            Assert.True(binaryList.Get(1).SequenceEqual(e3));
        }

        [Fact]
        public void TestInsertRangeFrom()
        {
            var e1 = new byte[] { 1, 2, 3 };
            var e2 = new byte[] { 1, 2, 3, 4 };
            var e3 = new byte[] { 1, 2, 3, 4, 5 };
            var e4 = new byte[] { 1, 2, 3, 4, 5, 6 };
            using BinaryList binaryList = new BinaryList(GlobalMemoryManager.Instance);
            binaryList.Insert(0, e1);
            binaryList.Insert(1, e2);

            using BinaryList other = new BinaryList(GlobalMemoryManager.Instance);
            other.Add(e3);
            other.Add(e4);
            binaryList.InsertRangeFrom(1, other, 0, 2);

            Assert.True(binaryList.Get(0).SequenceEqual(e1));
            Assert.True(binaryList.Get(1).SequenceEqual(e3));
            Assert.True(binaryList.Get(2).SequenceEqual(e4));
            Assert.True(binaryList.Get(3).SequenceEqual(e2));
        }

        [Fact]
        public void TestInsertFromEmptyLookup()
        {
            var e1 = new byte[] { 1, 2, 3 };
            using BinaryList binaryList = new BinaryList(GlobalMemoryManager.Instance);
            binaryList.Add(e1);

            using BinaryList other = new BinaryList(GlobalMemoryManager.Instance);
            other.Add(new byte[] { 4, 5 });

            Span<int> sortedLookup = stackalloc int[0];
            Span<int> insertPositions = stackalloc int[0];
            binaryList.InsertFrom(other, sortedLookup, insertPositions);

            Assert.Equal(1, binaryList.Count);
            Assert.True(binaryList.Get(0).SequenceEqual(e1));
        }

        [Fact]
        public void TestInsertFromSingleElementAtBeginning()
        {
            var e1 = new byte[] { 1, 2, 3 };
            var e2 = new byte[] { 4, 5, 6, 7 };
            var eNew = new byte[] { 10, 11 };

            using BinaryList binaryList = new BinaryList(GlobalMemoryManager.Instance);
            binaryList.Add(e1);
            binaryList.Add(e2);

            using BinaryList other = new BinaryList(GlobalMemoryManager.Instance);
            other.Add(eNew);

            Span<int> sortedLookup = stackalloc int[] { 0 };
            Span<int> insertPositions = stackalloc int[] { 0 };
            binaryList.InsertFrom(other, sortedLookup, insertPositions);

            Assert.Equal(3, binaryList.Count);
            Assert.True(binaryList.Get(0).SequenceEqual(eNew));
            Assert.True(binaryList.Get(1).SequenceEqual(e1));
            Assert.True(binaryList.Get(2).SequenceEqual(e2));
        }

        [Fact]
        public void TestInsertFromSingleElementAtEnd()
        {
            var e1 = new byte[] { 1, 2, 3 };
            var e2 = new byte[] { 4, 5, 6, 7 };
            var eNew = new byte[] { 10, 11 };

            using BinaryList binaryList = new BinaryList(GlobalMemoryManager.Instance);
            binaryList.Add(e1);
            binaryList.Add(e2);

            using BinaryList other = new BinaryList(GlobalMemoryManager.Instance);
            other.Add(eNew);

            Span<int> sortedLookup = stackalloc int[] { 0 };
            Span<int> insertPositions = stackalloc int[] { 2 };
            binaryList.InsertFrom(other, sortedLookup, insertPositions);

            Assert.Equal(3, binaryList.Count);
            Assert.True(binaryList.Get(0).SequenceEqual(e1));
            Assert.True(binaryList.Get(1).SequenceEqual(e2));
            Assert.True(binaryList.Get(2).SequenceEqual(eNew));
        }

        [Fact]
        public void TestInsertFromSingleElementInMiddle()
        {
            var e1 = new byte[] { 1, 2, 3 };
            var e2 = new byte[] { 4, 5, 6, 7 };
            var eNew = new byte[] { 10, 11 };

            using BinaryList binaryList = new BinaryList(GlobalMemoryManager.Instance);
            binaryList.Add(e1);
            binaryList.Add(e2);

            using BinaryList other = new BinaryList(GlobalMemoryManager.Instance);
            other.Add(eNew);

            Span<int> sortedLookup = stackalloc int[] { 0 };
            Span<int> insertPositions = stackalloc int[] { 1 };
            binaryList.InsertFrom(other, sortedLookup, insertPositions);

            Assert.Equal(3, binaryList.Count);
            Assert.True(binaryList.Get(0).SequenceEqual(e1));
            Assert.True(binaryList.Get(1).SequenceEqual(eNew));
            Assert.True(binaryList.Get(2).SequenceEqual(e2));
        }

        [Fact]
        public void TestInsertFromMultipleElementsAtDifferentPositions()
        {
            var e1 = new byte[] { 1, 2, 3 };
            var e2 = new byte[] { 4, 5, 6, 7 };
            var e3 = new byte[] { 8, 9 };
            var eA = new byte[] { 10, 11 };
            var eB = new byte[] { 20, 21, 22 };
            var eC = new byte[] { 30 };

            using BinaryList binaryList = new BinaryList(GlobalMemoryManager.Instance);
            binaryList.Add(e1);
            binaryList.Add(e2);
            binaryList.Add(e3);

            using BinaryList other = new BinaryList(GlobalMemoryManager.Instance);
            other.Add(eA);
            other.Add(eB);
            other.Add(eC);

            Span<int> sortedLookup = stackalloc int[] { 0, 1, 2 };
            Span<int> insertPositions = stackalloc int[] { 0, 1, 3 };
            binaryList.InsertFrom(other, sortedLookup, insertPositions);

            Assert.Equal(6, binaryList.Count);
            Assert.True(binaryList.Get(0).SequenceEqual(eA));
            Assert.True(binaryList.Get(1).SequenceEqual(e1));
            Assert.True(binaryList.Get(2).SequenceEqual(eB));
            Assert.True(binaryList.Get(3).SequenceEqual(e2));
            Assert.True(binaryList.Get(4).SequenceEqual(e3));
            Assert.True(binaryList.Get(5).SequenceEqual(eC));
        }

        [Fact]
        public void TestInsertFromSingleElementIntoEmptyList()
        {
            var eA = new byte[] { 10, 11 };
            var eB = new byte[] { 20, 21, 22 };

            using BinaryList binaryList = new BinaryList(GlobalMemoryManager.Instance);

            using BinaryList other = new BinaryList(GlobalMemoryManager.Instance);
            other.Add(eA);
            other.Add(eB);

            Span<int> sortedLookup = stackalloc int[] { 0, 1 };
            Span<int> insertPositions = stackalloc int[] { 0, 0 };
            binaryList.InsertFrom(other, sortedLookup, insertPositions);

            Assert.Equal(2, binaryList.Count);
            Assert.True(binaryList.Get(0).SequenceEqual(eA));
            Assert.True(binaryList.Get(1).SequenceEqual(eB));
        }

        [Fact]
        public void TestInsertFromAllAtEnd()
        {
            var e1 = new byte[] { 1, 2, 3 };
            var eA = new byte[] { 10, 11 };
            var eB = new byte[] { 20, 21, 22 };

            using BinaryList binaryList = new BinaryList(GlobalMemoryManager.Instance);
            binaryList.Add(e1);

            using BinaryList other = new BinaryList(GlobalMemoryManager.Instance);
            other.Add(eA);
            other.Add(eB);

            Span<int> sortedLookup = stackalloc int[] { 0, 1 };
            Span<int> insertPositions = stackalloc int[] { 1, 1 };
            binaryList.InsertFrom(other, sortedLookup, insertPositions);

            Assert.Equal(3, binaryList.Count);
            Assert.True(binaryList.Get(0).SequenceEqual(e1));
            Assert.True(binaryList.Get(1).SequenceEqual(eA));
            Assert.True(binaryList.Get(2).SequenceEqual(eB));
        }

        [Fact]
        public void TestInsertFromAllAtBeginning()
        {
            var e1 = new byte[] { 1, 2, 3 };
            var e2 = new byte[] { 4, 5, 6, 7 };
            var eA = new byte[] { 10, 11 };
            var eB = new byte[] { 20, 21, 22 };

            using BinaryList binaryList = new BinaryList(GlobalMemoryManager.Instance);
            binaryList.Add(e1);
            binaryList.Add(e2);

            using BinaryList other = new BinaryList(GlobalMemoryManager.Instance);
            other.Add(eA);
            other.Add(eB);

            Span<int> sortedLookup = stackalloc int[] { 0, 1 };
            Span<int> insertPositions = stackalloc int[] { 0, 0 };
            binaryList.InsertFrom(other, sortedLookup, insertPositions);

            Assert.Equal(4, binaryList.Count);
            Assert.True(binaryList.Get(0).SequenceEqual(eA));
            Assert.True(binaryList.Get(1).SequenceEqual(eB));
            Assert.True(binaryList.Get(2).SequenceEqual(e1));
            Assert.True(binaryList.Get(3).SequenceEqual(e2));
        }

        [Fact]
        public void TestInsertFromVaryingDataSizes()
        {
            var e1 = new byte[] { 1 };
            var e2 = new byte[] { 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 };
            var eA = new byte[] { };
            var eB = new byte[] { 100, 101, 102, 103, 104, 105, 106, 107 };

            using BinaryList binaryList = new BinaryList(GlobalMemoryManager.Instance);
            binaryList.Add(e1);
            binaryList.Add(e2);

            using BinaryList other = new BinaryList(GlobalMemoryManager.Instance);
            other.Add(eA);
            other.Add(eB);

            Span<int> sortedLookup = stackalloc int[] { 0, 1 };
            Span<int> insertPositions = stackalloc int[] { 0, 2 };
            binaryList.InsertFrom(other, sortedLookup, insertPositions);

            Assert.Equal(4, binaryList.Count);
            Assert.True(binaryList.Get(0).SequenceEqual(eA));
            Assert.True(binaryList.Get(1).SequenceEqual(e1));
            Assert.True(binaryList.Get(2).SequenceEqual(e2));
            Assert.True(binaryList.Get(3).SequenceEqual(eB));
        }

        [Fact]
        public void TestInsertFromInterleavedInsertions()
        {
            var e1 = new byte[] { 1, 2 };
            var e2 = new byte[] { 3, 4 };
            var e3 = new byte[] { 5, 6 };
            var eA = new byte[] { 10, 11 };
            var eB = new byte[] { 20, 21 };
            var eC = new byte[] { 30, 31 };

            using BinaryList binaryList = new BinaryList(GlobalMemoryManager.Instance);
            binaryList.Add(e1);
            binaryList.Add(e2);
            binaryList.Add(e3);

            using BinaryList other = new BinaryList(GlobalMemoryManager.Instance);
            other.Add(eA);
            other.Add(eB);
            other.Add(eC);

            Span<int> sortedLookup = stackalloc int[] { 0, 1, 2 };
            Span<int> insertPositions = stackalloc int[] { 0, 1, 2 };
            binaryList.InsertFrom(other, sortedLookup, insertPositions);

            Assert.Equal(6, binaryList.Count);
            Assert.True(binaryList.Get(0).SequenceEqual(eA));
            Assert.True(binaryList.Get(1).SequenceEqual(e1));
            Assert.True(binaryList.Get(2).SequenceEqual(eB));
            Assert.True(binaryList.Get(3).SequenceEqual(e2));
            Assert.True(binaryList.Get(4).SequenceEqual(eC));
            Assert.True(binaryList.Get(5).SequenceEqual(e3));
        }

        [Fact]
        public void TestInsertFromReverseLookupOrder()
        {
            var e1 = new byte[] { 1, 2, 3 };
            var eA = new byte[] { 10 };
            var eB = new byte[] { 20, 21 };
            var eC = new byte[] { 30, 31, 32 };

            using BinaryList binaryList = new BinaryList(GlobalMemoryManager.Instance);
            binaryList.Add(e1);

            using BinaryList other = new BinaryList(GlobalMemoryManager.Instance);
            other.Add(eA);
            other.Add(eB);
            other.Add(eC);

            Span<int> sortedLookup = stackalloc int[] { 2, 1, 0 };
            Span<int> insertPositions = stackalloc int[] { 0, 0, 1 };
            binaryList.InsertFrom(other, sortedLookup, insertPositions);

            Assert.Equal(4, binaryList.Count);
            Assert.True(binaryList.Get(0).SequenceEqual(eC));
            Assert.True(binaryList.Get(1).SequenceEqual(eB));
            Assert.True(binaryList.Get(2).SequenceEqual(e1));
            Assert.True(binaryList.Get(3).SequenceEqual(eA));
        }

        [Fact]
        public void TestInsertFromLargeBatchAvx()
        {
            using BinaryList binaryList = new BinaryList(GlobalMemoryManager.Instance);
            using BinaryList other = new BinaryList(GlobalMemoryManager.Instance);

            List<byte[]> expected = new List<byte[]>();
            for (int i = 0; i < 100; i++)
            {
                var data = new byte[] { (byte)i, (byte)(i + 1) };
                binaryList.Add(data);
                expected.Add(data);
            }

            List<byte[]> toInsert = new List<byte[]>();
            for (int i = 0; i < 35; i++)
            {
                var data = new byte[] { 255, (byte)i };
                other.Add(data);
                toInsert.Add(data);
            }

            Span<int> sortedLookup = stackalloc int[35];
            Span<int> insertPositions = stackalloc int[35];
            for (int i = 0; i < 35; i++)
            {
                sortedLookup[i] = i;
                insertPositions[i] = 50; // Multiple inserts at same position
            }

            binaryList.InsertFrom(other, sortedLookup, insertPositions);

            Assert.Equal(135, binaryList.Count);

            for (int i = 0; i < 50; i++)
                Assert.True(binaryList.Get(i).SequenceEqual(expected[i]));

            for (int i = 0; i < 35; i++)
                Assert.True(binaryList.Get(50 + i).SequenceEqual(toInsert[i]));

            for (int i = 0; i < 50; i++)
                Assert.True(binaryList.Get(85 + i).SequenceEqual(expected[50 + i]));

            binaryList.Add(new byte[] { 1, 2, 3 });

            Assert.True(binaryList.Get(135).SequenceEqual(new byte[] { 1, 2, 3 }));
        }

        [Fact]
        public void TestInsertFromExceedingCapacity()
        {
            using BinaryList binaryList = new BinaryList(GlobalMemoryManager.Instance);

            var e1 = new byte[] { 1, 1, 1 };
            var e2 = new byte[] { 2, 2, 2 };
            binaryList.Add(e1);
            binaryList.Add(e2);

            using BinaryList other = new BinaryList(GlobalMemoryManager.Instance);
            for (int i = 0; i < 100; i++)
            {
                other.Add(new byte[] { (byte)(10 + i) });
            }

            Span<int> sortedLookup = stackalloc int[100];
            Span<int> insertPositions = stackalloc int[100];
            for (int i = 0; i < 100; i++)
            {
                sortedLookup[i] = i;
                insertPositions[i] = 0;
            }

            binaryList.InsertFrom(other, sortedLookup, insertPositions);

            Assert.Equal(102, binaryList.Count);

            for (int i = 0; i < 100; i++)
            {
                Assert.Equal((byte)(10 + i), binaryList.Get(i)[0]);
            }

            // Verify the original items were shifted correctly to the end
            Assert.True(binaryList.Get(100).SequenceEqual(e1));
            Assert.True(binaryList.Get(101).SequenceEqual(e2));
        }
    }
}
