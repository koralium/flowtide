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

            ReadOnlySpan<int> sortedLookup = stackalloc int[0];
            ReadOnlySpan<int> insertPositions = stackalloc int[0];
            binaryList.InsertFrom(in other, in sortedLookup, in insertPositions, -1);

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

            ReadOnlySpan<int> sortedLookup = stackalloc int[] { 0 };
            ReadOnlySpan<int> insertPositions = stackalloc int[] { 0 };
            binaryList.InsertFrom(in other, in sortedLookup, in insertPositions, -1);

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

            ReadOnlySpan<int> sortedLookup = stackalloc int[] { 0 };
            ReadOnlySpan<int> insertPositions = stackalloc int[] { 2 };
            binaryList.InsertFrom(in other, in sortedLookup, in insertPositions, -1);

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

            ReadOnlySpan<int> sortedLookup = stackalloc int[] { 0 };
            ReadOnlySpan<int> insertPositions = stackalloc int[] { 1 };
            binaryList.InsertFrom(in other, in sortedLookup, in insertPositions, -1);

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

            ReadOnlySpan<int> sortedLookup = stackalloc int[] { 0, 1, 2 };
            ReadOnlySpan<int> insertPositions = stackalloc int[] { 0, 1, 3 };
            binaryList.InsertFrom(in other, in sortedLookup, in insertPositions, -1);

            Assert.Equal(6, binaryList.Count);
            Assert.True(binaryList.Get(0).SequenceEqual(eA));
            Assert.True(binaryList.Get(1).SequenceEqual(e1));
            Assert.True(binaryList.Get(2).SequenceEqual(eB));
            Assert.True(binaryList.Get(3).SequenceEqual(e2));
            Assert.True(binaryList.Get(4).SequenceEqual(e3));
            Assert.True(binaryList.Get(5).SequenceEqual(eC));
        }

        [Fact]
        public void TestInsertFromMultipleElementsIntoEmptyList()
        {
            var eA = new byte[] { 10, 11 };
            var eB = new byte[] { 20, 21, 22 };

            using BinaryList binaryList = new BinaryList(GlobalMemoryManager.Instance);

            using BinaryList other = new BinaryList(GlobalMemoryManager.Instance);
            other.Add(eA);
            other.Add(eB);

            ReadOnlySpan<int> sortedLookup = stackalloc int[] { 0, 1 };
            ReadOnlySpan<int> insertPositions = stackalloc int[] { 0, 0 };
            binaryList.InsertFrom(in other, in sortedLookup, in insertPositions, -1);

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

            ReadOnlySpan<int> sortedLookup = stackalloc int[] { 0, 1 };
            ReadOnlySpan<int> insertPositions = stackalloc int[] { 1, 1 };
            binaryList.InsertFrom(in other, in sortedLookup, in insertPositions, -1);

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

            ReadOnlySpan<int> sortedLookup = stackalloc int[] { 0, 1 };
            ReadOnlySpan<int> insertPositions = stackalloc int[] { 0, 0 };
            binaryList.InsertFrom(in other, in sortedLookup, in insertPositions, -1);

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

            ReadOnlySpan<int> sortedLookup = stackalloc int[] { 0, 1 };
            ReadOnlySpan<int> insertPositions = stackalloc int[] { 0, 2 };
            binaryList.InsertFrom(in other, in sortedLookup, in insertPositions, -1);

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

            ReadOnlySpan<int> sortedLookup = stackalloc int[] { 0, 1, 2 };
            ReadOnlySpan<int> insertPositions = stackalloc int[] { 0, 1, 2 };
            binaryList.InsertFrom(in other, in sortedLookup, in insertPositions, -1);

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

            ReadOnlySpan<int> sortedLookup = stackalloc int[] { 2, 1, 0 };
            ReadOnlySpan<int> insertPositions = stackalloc int[] { 0, 0, 1 };
            binaryList.InsertFrom(in other, in sortedLookup, in insertPositions, -1);

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

            ReadOnlySpan<int> sortedLookupRO = sortedLookup;
            ReadOnlySpan<int> insertPositionsRO = insertPositions;
            binaryList.InsertFrom(in other, in sortedLookupRO, in insertPositionsRO, -1);

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

            ReadOnlySpan<int> sortedLookupRO = sortedLookup;
            ReadOnlySpan<int> insertPositionsRO = insertPositions;
            binaryList.InsertFrom(in other, in sortedLookupRO, in insertPositionsRO, -1);

            Assert.Equal(102, binaryList.Count);

            for (int i = 0; i < 100; i++)
            {
                Assert.Equal((byte)(10 + i), binaryList.Get(i)[0]);
            }

            // Verify the original items were shifted correctly to the end
            Assert.True(binaryList.Get(100).SequenceEqual(e1));
            Assert.True(binaryList.Get(101).SequenceEqual(e2));
        }

        [Fact]
        public void TestInsertFromWithNullIndex()
        {
            var e1 = new byte[] { 1, 2 };
            var e2 = new byte[] { 3, 4 };
            var eNew = new byte[] { 10, 11 };

            using BinaryList binaryList = new BinaryList(GlobalMemoryManager.Instance);
            binaryList.Add(e1);
            binaryList.Add(e2);

            using BinaryList other = new BinaryList(GlobalMemoryManager.Instance);
            other.Add(eNew);

            ReadOnlySpan<int> sortedLookup = stackalloc int[] { -1, 0, -1 };
            ReadOnlySpan<int> insertPositions = stackalloc int[] { 0, 1, 3 };

            binaryList.InsertFrom(in other, in sortedLookup, in insertPositions, -1);

            Assert.Equal(5, binaryList.Count);
            Assert.True(binaryList.Get(0).SequenceEqual(Span<byte>.Empty));
            Assert.True(binaryList.Get(1).SequenceEqual(e1));
            Assert.True(binaryList.Get(2).SequenceEqual(eNew));
            Assert.True(binaryList.Get(3).SequenceEqual(e2));
            Assert.True(binaryList.Get(4).SequenceEqual(Span<byte>.Empty));
        }

        [Fact]
        public void TestDeleteBatchEmptyTargets()
        {
            var e1 = new byte[] { 1, 2, 3 };
            var e2 = new byte[] { 4, 5, 6, 7 };
            using BinaryList binaryList = new BinaryList(GlobalMemoryManager.Instance);
            binaryList.Add(e1);
            binaryList.Add(e2);

            ReadOnlySpan<int> targets = stackalloc int[0];
            binaryList.DeleteBatch(targets);

            Assert.Equal(2, binaryList.Count);
            Assert.True(binaryList.Get(0).SequenceEqual(e1));
            Assert.True(binaryList.Get(1).SequenceEqual(e2));
        }

        [Fact]
        public void TestDeleteBatchSingleElementAtBeginning()
        {
            var e1 = new byte[] { 1, 2, 3 };
            var e2 = new byte[] { 4, 5, 6, 7 };
            var e3 = new byte[] { 8, 9 };

            using BinaryList binaryList = new BinaryList(GlobalMemoryManager.Instance);
            binaryList.Add(e1);
            binaryList.Add(e2);
            binaryList.Add(e3);

            ReadOnlySpan<int> targets = stackalloc int[] { 0 };
            binaryList.DeleteBatch(targets);

            Assert.Equal(2, binaryList.Count);
            Assert.True(binaryList.Get(0).SequenceEqual(e2));
            Assert.True(binaryList.Get(1).SequenceEqual(e3));
        }

        [Fact]
        public void TestDeleteBatchSingleElementInMiddle()
        {
            var e1 = new byte[] { 1, 2, 3 };
            var e2 = new byte[] { 4, 5, 6, 7 };
            var e3 = new byte[] { 8, 9 };

            using BinaryList binaryList = new BinaryList(GlobalMemoryManager.Instance);
            binaryList.Add(e1);
            binaryList.Add(e2);
            binaryList.Add(e3);

            ReadOnlySpan<int> targets = stackalloc int[] { 1 };
            binaryList.DeleteBatch(targets);

            Assert.Equal(2, binaryList.Count);
            Assert.True(binaryList.Get(0).SequenceEqual(e1));
            Assert.True(binaryList.Get(1).SequenceEqual(e3));
        }

        [Fact]
        public void TestDeleteBatchSingleElementAtEnd()
        {
            var e1 = new byte[] { 1, 2, 3 };
            var e2 = new byte[] { 4, 5, 6, 7 };
            var e3 = new byte[] { 8, 9 };

            using BinaryList binaryList = new BinaryList(GlobalMemoryManager.Instance);
            binaryList.Add(e1);
            binaryList.Add(e2);
            binaryList.Add(e3);

            ReadOnlySpan<int> targets = stackalloc int[] { 2 };
            binaryList.DeleteBatch(targets);

            Assert.Equal(2, binaryList.Count);
            Assert.True(binaryList.Get(0).SequenceEqual(e1));
            Assert.True(binaryList.Get(1).SequenceEqual(e2));
        }

        [Fact]
        public void TestDeleteBatchMultipleElementsAtDifferentPositions()
        {
            var e1 = new byte[] { 1, 2, 3 };
            var e2 = new byte[] { 4, 5, 6, 7 };
            var e3 = new byte[] { 8, 9 };
            var e4 = new byte[] { 10, 11, 12 };
            var e5 = new byte[] { 13, 14 };

            using BinaryList binaryList = new BinaryList(GlobalMemoryManager.Instance);
            binaryList.Add(e1);
            binaryList.Add(e2);
            binaryList.Add(e3);
            binaryList.Add(e4);
            binaryList.Add(e5);

            // Delete first, middle, and last
            ReadOnlySpan<int> targets = stackalloc int[] { 0, 2, 4 };
            binaryList.DeleteBatch(targets);

            Assert.Equal(2, binaryList.Count);
            Assert.True(binaryList.Get(0).SequenceEqual(e2));
            Assert.True(binaryList.Get(1).SequenceEqual(e4));
        }

        [Fact]
        public void TestDeleteBatchConsecutiveElements()
        {
            var e1 = new byte[] { 1, 2, 3 };
            var e2 = new byte[] { 4, 5, 6, 7 };
            var e3 = new byte[] { 8, 9 };
            var e4 = new byte[] { 10, 11, 12 };
            var e5 = new byte[] { 13, 14 };

            using BinaryList binaryList = new BinaryList(GlobalMemoryManager.Instance);
            binaryList.Add(e1);
            binaryList.Add(e2);
            binaryList.Add(e3);
            binaryList.Add(e4);
            binaryList.Add(e5);

            // Delete consecutive elements in the middle
            ReadOnlySpan<int> targets = stackalloc int[] { 1, 2, 3 };
            binaryList.DeleteBatch(targets);

            Assert.Equal(2, binaryList.Count);
            Assert.True(binaryList.Get(0).SequenceEqual(e1));
            Assert.True(binaryList.Get(1).SequenceEqual(e5));
        }

        [Fact]
        public void TestDeleteBatchAllElements()
        {
            var e1 = new byte[] { 1, 2, 3 };
            var e2 = new byte[] { 4, 5, 6, 7 };
            var e3 = new byte[] { 8, 9 };

            using BinaryList binaryList = new BinaryList(GlobalMemoryManager.Instance);
            binaryList.Add(e1);
            binaryList.Add(e2);
            binaryList.Add(e3);

            ReadOnlySpan<int> targets = stackalloc int[] { 0, 1, 2 };
            binaryList.DeleteBatch(targets);

            Assert.Equal(0, binaryList.Count);
        }

        [Fact]
        public void TestDeleteBatchVaryingDataSizes()
        {
            var e1 = new byte[] { 1 };
            var e2 = new byte[] { 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 };
            var e3 = new byte[] { };
            var e4 = new byte[] { 100, 101, 102, 103, 104, 105, 106, 107 };

            using BinaryList binaryList = new BinaryList(GlobalMemoryManager.Instance);
            binaryList.Add(e1);
            binaryList.Add(e2);
            binaryList.Add(e3);
            binaryList.Add(e4);

            // Delete the empty element and the large element
            ReadOnlySpan<int> targets = stackalloc int[] { 2, 3 };
            binaryList.DeleteBatch(targets);

            Assert.Equal(2, binaryList.Count);
            Assert.True(binaryList.Get(0).SequenceEqual(e1));
            Assert.True(binaryList.Get(1).SequenceEqual(e2));
        }

        [Fact]
        public void TestDeleteBatchInterleavedDeletions()
        {
            var e1 = new byte[] { 1, 2 };
            var e2 = new byte[] { 3, 4 };
            var e3 = new byte[] { 5, 6 };
            var e4 = new byte[] { 7, 8 };
            var e5 = new byte[] { 9, 10 };
            var e6 = new byte[] { 11, 12 };

            using BinaryList binaryList = new BinaryList(GlobalMemoryManager.Instance);
            binaryList.Add(e1);
            binaryList.Add(e2);
            binaryList.Add(e3);
            binaryList.Add(e4);
            binaryList.Add(e5);
            binaryList.Add(e6);

            // Delete every other element
            ReadOnlySpan<int> targets = stackalloc int[] { 0, 2, 4 };
            binaryList.DeleteBatch(targets);

            Assert.Equal(3, binaryList.Count);
            Assert.True(binaryList.Get(0).SequenceEqual(e2));
            Assert.True(binaryList.Get(1).SequenceEqual(e4));
            Assert.True(binaryList.Get(2).SequenceEqual(e6));
        }

        [Fact]
        public void TestDeleteBatchLargeBatchAvx()
        {
            using BinaryList binaryList = new BinaryList(GlobalMemoryManager.Instance);

            List<byte[]> allData = new List<byte[]>();
            for (int i = 0; i < 100; i++)
            {
                var data = new byte[] { (byte)i, (byte)(i + 1) };
                binaryList.Add(data);
                allData.Add(data);
            }

            // Delete every third element (indices 0, 3, 6, 9, ...)
            List<int> targetList = new List<int>();
            for (int i = 0; i < 100; i += 3)
            {
                targetList.Add(i);
            }

            ReadOnlySpan<int> targets = targetList.ToArray().AsSpan();
            binaryList.DeleteBatch(targets);

            int expectedCount = 100 - targetList.Count;
            Assert.Equal(expectedCount, binaryList.Count);

            // Build expected list by skipping deleted indices
            List<byte[]> expected = new List<byte[]>();
            var deletedSet = new HashSet<int>(targetList);
            for (int i = 0; i < 100; i++)
            {
                if (!deletedSet.Contains(i))
                {
                    expected.Add(allData[i]);
                }
            }

            for (int i = 0; i < expected.Count; i++)
            {
                Assert.True(binaryList.Get(i).SequenceEqual(expected[i]));
            }

            // Verify list is still usable after batch delete
            binaryList.Add(new byte[] { 1, 2, 3 });
            Assert.True(binaryList.Get(binaryList.Count - 1).SequenceEqual(new byte[] { 1, 2, 3 }));
        }

        [Fact]
        public void TestDeleteBatchSingleElementList()
        {
            var e1 = new byte[] { 1, 2, 3 };
            using BinaryList binaryList = new BinaryList(GlobalMemoryManager.Instance);
            binaryList.Add(e1);

            ReadOnlySpan<int> targets = stackalloc int[] { 0 };
            binaryList.DeleteBatch(targets);

            Assert.Equal(0, binaryList.Count);
        }

        [Fact]
        public void TestDeleteBatchThenAdd()
        {
            var e1 = new byte[] { 1, 2, 3 };
            var e2 = new byte[] { 4, 5, 6, 7 };
            var e3 = new byte[] { 8, 9 };
            var eNew = new byte[] { 100, 101 };

            using BinaryList binaryList = new BinaryList(GlobalMemoryManager.Instance);
            binaryList.Add(e1);
            binaryList.Add(e2);
            binaryList.Add(e3);

            ReadOnlySpan<int> targets = stackalloc int[] { 1 };
            binaryList.DeleteBatch(targets);

            // Verify state after delete
            Assert.Equal(2, binaryList.Count);
            Assert.True(binaryList.Get(0).SequenceEqual(e1));
            Assert.True(binaryList.Get(1).SequenceEqual(e3));

            // Verify the list is still usable for add and insert operations
            binaryList.Add(eNew);
            Assert.Equal(3, binaryList.Count);
            Assert.True(binaryList.Get(2).SequenceEqual(eNew));
        }

        [Fact]
        public void TestDeleteBatchWithEmptyElements()
        {
            var e1 = new byte[] { 1, 2, 3 };
            var e2 = new byte[] { };
            var e3 = new byte[] { 4, 5 };
            var e4 = new byte[] { };
            var e5 = new byte[] { 6, 7, 8 };

            using BinaryList binaryList = new BinaryList(GlobalMemoryManager.Instance);
            binaryList.Add(e1);
            binaryList.Add(e2);
            binaryList.Add(e3);
            binaryList.Add(e4);
            binaryList.Add(e5);

            // Delete the empty elements
            ReadOnlySpan<int> targets = stackalloc int[] { 1, 3 };
            binaryList.DeleteBatch(targets);

            Assert.Equal(3, binaryList.Count);
            Assert.True(binaryList.Get(0).SequenceEqual(e1));
            Assert.True(binaryList.Get(1).SequenceEqual(e3));
            Assert.True(binaryList.Get(2).SequenceEqual(e5));
        }

        [Fact]
        public void TestDeleteBatchAllButOne()
        {
            var e1 = new byte[] { 1, 2, 3 };
            var e2 = new byte[] { 4, 5, 6, 7 };
            var e3 = new byte[] { 8, 9 };
            var e4 = new byte[] { 10, 11, 12 };
            var e5 = new byte[] { 13, 14 };

            using BinaryList binaryList = new BinaryList(GlobalMemoryManager.Instance);
            binaryList.Add(e1);
            binaryList.Add(e2);
            binaryList.Add(e3);
            binaryList.Add(e4);
            binaryList.Add(e5);

            // Keep only the middle element
            ReadOnlySpan<int> targets = stackalloc int[] { 0, 1, 3, 4 };
            binaryList.DeleteBatch(targets);

            Assert.Equal(1, binaryList.Count);
            Assert.True(binaryList.Get(0).SequenceEqual(e3));
        }

        [Fact]
        public void TestDeleteBatchTwoConsecutiveCalls()
        {
            var e1 = new byte[] { 1, 2, 3 };
            var e2 = new byte[] { 4, 5, 6, 7 };
            var e3 = new byte[] { 8, 9 };
            var e4 = new byte[] { 10, 11, 12 };
            var e5 = new byte[] { 13, 14 };

            using BinaryList binaryList = new BinaryList(GlobalMemoryManager.Instance);
            binaryList.Add(e1);
            binaryList.Add(e2);
            binaryList.Add(e3);
            binaryList.Add(e4);
            binaryList.Add(e5);

            // First batch: delete indices 1 and 3 -> leaves [e1, e3, e5]
            ReadOnlySpan<int> targets1 = stackalloc int[] { 1, 3 };
            binaryList.DeleteBatch(targets1);

            Assert.Equal(3, binaryList.Count);
            Assert.True(binaryList.Get(0).SequenceEqual(e1));
            Assert.True(binaryList.Get(1).SequenceEqual(e3));
            Assert.True(binaryList.Get(2).SequenceEqual(e5));

            // Second batch: delete index 1 from the remaining list -> leaves [e1, e5]
            ReadOnlySpan<int> targets2 = stackalloc int[] { 1 };
            binaryList.DeleteBatch(targets2);

            Assert.Equal(2, binaryList.Count);
            Assert.True(binaryList.Get(0).SequenceEqual(e1));
            Assert.True(binaryList.Get(1).SequenceEqual(e5));
        }

        [Fact]
        public void TestDeleteBatchLeadingConsecutive()
        {
            var e1 = new byte[] { 1, 2, 3 };
            var e2 = new byte[] { 4, 5, 6, 7 };
            var e3 = new byte[] { 8, 9 };
            var e4 = new byte[] { 10, 11, 12 };
            var e5 = new byte[] { 13, 14 };

            using BinaryList binaryList = new BinaryList(GlobalMemoryManager.Instance);
            binaryList.Add(e1);
            binaryList.Add(e2);
            binaryList.Add(e3);
            binaryList.Add(e4);
            binaryList.Add(e5);

            // Delete the first three elements
            ReadOnlySpan<int> targets = stackalloc int[] { 0, 1, 2 };
            binaryList.DeleteBatch(targets);

            Assert.Equal(2, binaryList.Count);
            Assert.True(binaryList.Get(0).SequenceEqual(e4));
            Assert.True(binaryList.Get(1).SequenceEqual(e5));
        }

        [Fact]
        public void TestDeleteBatchTrailingConsecutive()
        {
            var e1 = new byte[] { 1, 2, 3 };
            var e2 = new byte[] { 4, 5, 6, 7 };
            var e3 = new byte[] { 8, 9 };
            var e4 = new byte[] { 10, 11, 12 };
            var e5 = new byte[] { 13, 14 };

            using BinaryList binaryList = new BinaryList(GlobalMemoryManager.Instance);
            binaryList.Add(e1);
            binaryList.Add(e2);
            binaryList.Add(e3);
            binaryList.Add(e4);
            binaryList.Add(e5);

            // Delete the last three elements
            ReadOnlySpan<int> targets = stackalloc int[] { 2, 3, 4 };
            binaryList.DeleteBatch(targets);

            Assert.Equal(2, binaryList.Count);
            Assert.True(binaryList.Get(0).SequenceEqual(e1));
            Assert.True(binaryList.Get(1).SequenceEqual(e2));
        }

        [Fact]
        public void TestDeleteBatchAllEmptyElements()
        {
            using BinaryList binaryList = new BinaryList(GlobalMemoryManager.Instance);
            binaryList.AddEmpty();
            binaryList.AddEmpty();
            binaryList.AddEmpty();
            binaryList.AddEmpty();

            // Delete every other empty element
            ReadOnlySpan<int> targets = stackalloc int[] { 0, 2 };
            binaryList.DeleteBatch(targets);

            Assert.Equal(2, binaryList.Count);
            Assert.True(binaryList.Get(0).SequenceEqual(Array.Empty<byte>()));
            Assert.True(binaryList.Get(1).SequenceEqual(Array.Empty<byte>()));
        }
    }
}
