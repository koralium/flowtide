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
using System;
using System.Collections.Generic;
using System.Linq;
using Xunit;

namespace FlowtideDotNet.Core.Tests.ColumnStore.Utils
{
    public class BinaryViewListTests
    {
        private static byte[] GetInlineData(byte val) => new byte[] { val, val, val };
        private static byte[] GetOutOfLineData(byte val) => Enumerable.Repeat(val, 20).ToArray();

        [Fact]
        public void TestAdd()
        {
            var e1 = GetInlineData(1);
            var e2 = GetOutOfLineData(2);
            using BinaryViewList binaryList = new BinaryViewList(GlobalMemoryManager.Instance);
            binaryList.Add(e1);
            binaryList.Add(e2);

            Assert.True(binaryList.Get(0).SequenceEqual(e1));
            Assert.True(binaryList.Get(1).SequenceEqual(e2));
        }

        [Fact]
        public void TestInsertInTheMiddle()
        {
            var e1 = GetInlineData(1);
            var e2 = GetOutOfLineData(2);
            var e3 = GetInlineData(3);
            using BinaryViewList binaryList = new BinaryViewList(GlobalMemoryManager.Instance);
            binaryList.Add(e1);
            binaryList.Add(e2);
            binaryList.Insert(1, e3);

            Assert.True(binaryList.Get(0).SequenceEqual(e1));
            Assert.True(binaryList.Get(1).SequenceEqual(e3));
            Assert.True(binaryList.Get(2).SequenceEqual(e2));
        }

        [Fact]
        public void TestRemoveFirst()
        {
            var e1 = GetInlineData(1);
            var e2 = GetOutOfLineData(2);
            var e3 = GetInlineData(3);
            using BinaryViewList binaryList = new BinaryViewList(GlobalMemoryManager.Instance);
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
            var e1 = GetOutOfLineData(1);
            var e2 = GetOutOfLineData(2);
            var e3 = GetOutOfLineData(3);
            using BinaryViewList binaryList = new BinaryViewList(GlobalMemoryManager.Instance);
            binaryList.Add(e1);
            binaryList.Add(e2);
            binaryList.Add(e3);

            binaryList.RemoveAt(1);

            Assert.True(binaryList.Get(0).SequenceEqual(e1));
            Assert.True(binaryList.Get(1).SequenceEqual(e3));
        }

        [Fact]
        public void TestUpdateFirst()
        {
            var e1 = GetInlineData(1);
            var e2 = GetOutOfLineData(2);
            using BinaryViewList binaryList = new BinaryViewList(GlobalMemoryManager.Instance);
            binaryList.Insert(0, e1);
            binaryList.UpdateAt(0, e2);

            Assert.True(binaryList.Get(0).SequenceEqual(e2));
        }

        [Fact]
        public void TestUpdateTwice()
        {
            var e1 = GetOutOfLineData(1);
            var e2 = GetInlineData(2);
            using BinaryViewList binaryList = new BinaryViewList(GlobalMemoryManager.Instance);
            binaryList.Insert(0, e1);
            binaryList.UpdateAt(0, e2);
            binaryList.UpdateAt(0, e1);

            Assert.True(binaryList.Get(0).SequenceEqual(e1));
        }

        [Fact]
        public void TestInsertRangeFrom()
        {
            var e1 = GetInlineData(1);
            var e2 = GetOutOfLineData(2);
            var e3 = GetOutOfLineData(3);
            var e4 = GetInlineData(4);
            using BinaryViewList binaryList = new BinaryViewList(GlobalMemoryManager.Instance);
            binaryList.Insert(0, e1);
            binaryList.Insert(1, e2);

            using BinaryViewList other = new BinaryViewList(GlobalMemoryManager.Instance);
            other.Add(e3);
            other.Add(e4);
            binaryList.InsertRangeFrom(1, other, 0, 2);

            Assert.True(binaryList.Get(0).SequenceEqual(e1));
            Assert.True(binaryList.Get(1).SequenceEqual(e3));
            Assert.True(binaryList.Get(2).SequenceEqual(e4));
            Assert.True(binaryList.Get(3).SequenceEqual(e2));
        }

        [Fact]
        public void TestInsertFromMultipleElementsAtDifferentPositions()
        {
            var e1 = GetInlineData(1);
            var e2 = GetOutOfLineData(2);
            var e3 = GetInlineData(3);
            var eA = GetOutOfLineData(10);
            var eB = GetInlineData(20);
            var eC = GetOutOfLineData(30);

            using BinaryViewList binaryList = new BinaryViewList(GlobalMemoryManager.Instance);
            binaryList.Add(e1);
            binaryList.Add(e2);
            binaryList.Add(e3);

            using BinaryViewList other = new BinaryViewList(GlobalMemoryManager.Instance);
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
        public void TestDeleteBatchAllButOne()
        {
            var e1 = GetInlineData(1);
            var e2 = GetOutOfLineData(2);
            var e3 = GetInlineData(3);
            var e4 = GetOutOfLineData(4);
            var e5 = GetInlineData(5);

            using BinaryViewList binaryList = new BinaryViewList(GlobalMemoryManager.Instance);
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
        public void TestEnsureDataCapacityCompaction()
        {
            // This test forces a reallocation that triggers the CheckDataSizeReduction / Compaction
            // behavior when we add enough data, remove some, and add more.
            using BinaryViewList binaryList = new BinaryViewList(GlobalMemoryManager.Instance);
            
            // Add initial out-of-line data (so it allocates a buffer)
            for (int i = 0; i < 10; i++)
            {
                binaryList.Add(GetOutOfLineData((byte)i));
            }

            // Remove some out-of-line data to create fragmentation
            binaryList.RemoveAt(2);
            binaryList.RemoveAt(4); // the index has shifted
            binaryList.RemoveAt(6);

            // Add large out-of-line data to trigger EnsureDataCapacity reallocation
            // The capacity is initially small, and 10 * 20 = 200 bytes.
            // We'll add a much larger buffer to force realloc.
            var bigData = Enumerable.Repeat((byte)255, 1000).ToArray();
            binaryList.Add(bigData);

            // Verify integrity of remaining items
            Assert.True(binaryList.Get(0).SequenceEqual(GetOutOfLineData(0)));
            Assert.True(binaryList.Get(1).SequenceEqual(GetOutOfLineData(1)));
            Assert.True(binaryList.Get(2).SequenceEqual(GetOutOfLineData(3)));
            Assert.True(binaryList.Get(3).SequenceEqual(GetOutOfLineData(4)));
            Assert.True(binaryList.Get(4).SequenceEqual(GetOutOfLineData(6)));
            Assert.True(binaryList.Get(5).SequenceEqual(GetOutOfLineData(7)));
            Assert.True(binaryList.Get(6).SequenceEqual(GetOutOfLineData(9)));
            Assert.True(binaryList.Get(7).SequenceEqual(bigData));
        }

        [Fact]
        public void TestCopyAndCompact()
        {
            using BinaryViewList binaryList = new BinaryViewList(GlobalMemoryManager.Instance);
            var e1 = GetOutOfLineData(1);
            var e2 = GetOutOfLineData(2);
            var e3 = GetOutOfLineData(3);
            
            binaryList.Add(e1);
            binaryList.Add(e2);
            binaryList.Add(e3);

            // Delete to create fragmentation
            binaryList.RemoveAt(1);

            // Copy forces compact
            using var copy = binaryList.Copy(GlobalMemoryManager.Instance);

            Assert.Equal(2, copy.Count);
            Assert.True(copy.Get(0).SequenceEqual(e1));
            Assert.True(copy.Get(1).SequenceEqual(e3));

            // Original should be unchanged functionally
            Assert.Equal(2, binaryList.Count);
            Assert.True(binaryList.Get(0).SequenceEqual(e1));
            Assert.True(binaryList.Get(1).SequenceEqual(e3));
        }

        [Fact]
        public void TestClear()
        {
            using BinaryViewList binaryList = new BinaryViewList(GlobalMemoryManager.Instance);
            binaryList.Add(GetInlineData(1));
            binaryList.Add(GetOutOfLineData(2));

            Assert.Equal(2, binaryList.Count);

            binaryList.Clear();

            Assert.Equal(0, binaryList.Count);

            // Verify we can add after clear
            var e3 = GetOutOfLineData(3);
            binaryList.Add(e3);
            Assert.Equal(1, binaryList.Count);
            Assert.True(binaryList.Get(0).SequenceEqual(e3));
        }

        [Fact]
        public void TestInsertFromEmptyLookup()
        {
            var e1 = GetInlineData(1);
            using BinaryViewList binaryList = new BinaryViewList(GlobalMemoryManager.Instance);
            binaryList.Add(e1);

            using BinaryViewList other = new BinaryViewList(GlobalMemoryManager.Instance);
            other.Add(GetInlineData(4));

            ReadOnlySpan<int> sortedLookup = stackalloc int[0];
            ReadOnlySpan<int> insertPositions = stackalloc int[0];
            binaryList.InsertFrom(in other, in sortedLookup, in insertPositions, -1);

            Assert.Equal(1, binaryList.Count);
            Assert.True(binaryList.Get(0).SequenceEqual(e1));
        }

        [Fact]
        public void TestInsertFromWithNullIndex()
        {
            var e1 = GetInlineData(1);
            var e2 = GetOutOfLineData(2);
            var eNew = GetOutOfLineData(10);

            using BinaryViewList binaryList = new BinaryViewList(GlobalMemoryManager.Instance);
            binaryList.Add(e1);
            binaryList.Add(e2);

            using BinaryViewList other = new BinaryViewList(GlobalMemoryManager.Instance);
            other.Add(eNew);

            ReadOnlySpan<int> sortedLookup = stackalloc int[] { -1, 0, -1 };
            ReadOnlySpan<int> insertPositions = stackalloc int[] { 0, 1, 2 };

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
            var e1 = GetInlineData(1);
            var e2 = GetOutOfLineData(2);
            using BinaryViewList binaryList = new BinaryViewList(GlobalMemoryManager.Instance);
            binaryList.Add(e1);
            binaryList.Add(e2);

            ReadOnlySpan<int> targets = stackalloc int[0];
            binaryList.DeleteBatch(targets);

            Assert.Equal(2, binaryList.Count);
            Assert.True(binaryList.Get(0).SequenceEqual(e1));
            Assert.True(binaryList.Get(1).SequenceEqual(e2));
        }

        [Fact]
        public void TestDeleteBatchConsecutiveElements()
        {
            var e1 = GetInlineData(1);
            var e2 = GetOutOfLineData(2);
            var e3 = GetInlineData(3);
            var e4 = GetOutOfLineData(4);
            var e5 = GetInlineData(5);

            using BinaryViewList binaryList = new BinaryViewList(GlobalMemoryManager.Instance);
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
            using BinaryViewList binaryList = new BinaryViewList(GlobalMemoryManager.Instance);
            binaryList.Add(GetInlineData(1));
            binaryList.Add(GetOutOfLineData(2));
            binaryList.Add(GetInlineData(3));

            ReadOnlySpan<int> targets = stackalloc int[] { 0, 1, 2 };
            binaryList.DeleteBatch(targets);

            Assert.Equal(0, binaryList.Count);
        }

        [Fact]
        public void TestGetPrefixSumByteSizes()
        {
            using BinaryViewList binaryList = new BinaryViewList(GlobalMemoryManager.Instance);
            binaryList.Add(GetInlineData(1)); // Length 3
            binaryList.Add(GetOutOfLineData(2)); // Length 20
            binaryList.Add(GetInlineData(3)); // Length 3

            ReadOnlySpan<int> indices = stackalloc int[] { 0, 1, 2 };
            Span<int> sizes = stackalloc int[3] { 0, 0, 0 };

            binaryList.GetPrefixSumByteSizes(indices, sizes);

            // Each entry adds length + 4 (sizeof(int))
            // Index 0: 3 + 4 = 7
            // Index 1: 20 + 4 = 24. Sum so far = 31.
            // Index 2: 3 + 4 = 7. Sum so far = 38.

            Assert.Equal(7, sizes[0]);
            Assert.Equal(31, sizes[1]);
            Assert.Equal(38, sizes[2]);
            
            // Call again with some initial values in sizes to ensure it adds to them
            Span<int> prefilledSizes = stackalloc int[3] { 10, 20, 30 };
            binaryList.GetPrefixSumByteSizes(indices, prefilledSizes);

            Assert.Equal(10 + 7, prefilledSizes[0]);
            Assert.Equal(20 + 31, prefilledSizes[1]);
            Assert.Equal(30 + 38, prefilledSizes[2]);
        }

        [Fact]
        public void TestInsertInFirstIndex()
        {
            var e1 = GetInlineData(1);
            var e2 = GetOutOfLineData(2);
            var e3 = GetInlineData(3);
            using BinaryViewList binaryList = new BinaryViewList(GlobalMemoryManager.Instance);
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
            var e1 = GetInlineData(1);
            var e2 = GetOutOfLineData(2);
            var e3 = GetInlineData(3);
            using BinaryViewList binaryList = new BinaryViewList(GlobalMemoryManager.Instance);
            binaryList.Add(e1);
            binaryList.Add(e2);
            binaryList.Insert(2, e3);

            Assert.True(binaryList.Get(0).SequenceEqual(e1));
            Assert.True(binaryList.Get(1).SequenceEqual(e2));
            Assert.True(binaryList.Get(2).SequenceEqual(e3));
        }

        [Fact]
        public void TestRemoveLast()
        {
            var e1 = GetInlineData(1);
            var e2 = GetOutOfLineData(2);
            var e3 = GetInlineData(3);
            using BinaryViewList binaryList = new BinaryViewList(GlobalMemoryManager.Instance);
            binaryList.Add(e1);
            binaryList.Add(e2);
            binaryList.Add(e3);

            binaryList.RemoveAt(2);

            Assert.True(binaryList.Get(0).SequenceEqual(e1));
            Assert.True(binaryList.Get(1).SequenceEqual(e2));
            Assert.Equal(2, binaryList.Count);
        }

        [Fact]
        public void TestDeleteBatchVaryingDataSizes()
        {
            var e1 = GetInlineData(1);
            var e2 = GetOutOfLineData(2);
            var e3 = Array.Empty<byte>();
            var e4 = GetOutOfLineData(4);

            using BinaryViewList binaryList = new BinaryViewList(GlobalMemoryManager.Instance);
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
            var e1 = GetInlineData(1);
            var e2 = GetOutOfLineData(2);
            var e3 = GetInlineData(3);
            var e4 = GetOutOfLineData(4);
            var e5 = GetInlineData(5);
            var e6 = GetOutOfLineData(6);

            using BinaryViewList binaryList = new BinaryViewList(GlobalMemoryManager.Instance);
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
        public void TestDeleteBatchThenAdd()
        {
            var e1 = GetInlineData(1);
            var e2 = GetOutOfLineData(2);
            var e3 = GetInlineData(3);
            var eNew = GetOutOfLineData(100);

            using BinaryViewList binaryList = new BinaryViewList(GlobalMemoryManager.Instance);
            binaryList.Add(e1);
            binaryList.Add(e2);
            binaryList.Add(e3);

            ReadOnlySpan<int> targets = stackalloc int[] { 1 };
            binaryList.DeleteBatch(targets);

            Assert.Equal(2, binaryList.Count);
            Assert.True(binaryList.Get(0).SequenceEqual(e1));
            Assert.True(binaryList.Get(1).SequenceEqual(e3));

            binaryList.Add(eNew);
            Assert.Equal(3, binaryList.Count);
            Assert.True(binaryList.Get(2).SequenceEqual(eNew));
        }

        [Fact]
        public void TestInsertFromReverseLookupOrder()
        {
            var e1 = GetInlineData(1);
            var eA = GetOutOfLineData(10);
            var eB = GetInlineData(20);
            var eC = GetOutOfLineData(30);

            using BinaryViewList binaryList = new BinaryViewList(GlobalMemoryManager.Instance);
            binaryList.Add(e1);

            using BinaryViewList other = new BinaryViewList(GlobalMemoryManager.Instance);
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
        public void TestInsertFromVaryingDataSizes()
        {
            var e1 = GetInlineData(1);
            var e2 = GetOutOfLineData(2);
            var eA = Array.Empty<byte>();
            var eB = GetOutOfLineData(100);

            using BinaryViewList binaryList = new BinaryViewList(GlobalMemoryManager.Instance);
            binaryList.Add(e1);
            binaryList.Add(e2);

            using BinaryViewList other = new BinaryViewList(GlobalMemoryManager.Instance);
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
        public void TestInsertFromLargeBatchAvx()
        {
            using BinaryViewList binaryList = new BinaryViewList(GlobalMemoryManager.Instance);
            using BinaryViewList other = new BinaryViewList(GlobalMemoryManager.Instance);

            List<byte[]> expected = new List<byte[]>();
            for (int i = 0; i < 100; i++)
            {
                var data = (i % 2 == 0) ? GetInlineData((byte)i) : GetOutOfLineData((byte)i);
                binaryList.Add(data);
                expected.Add(data);
            }

            List<byte[]> toInsert = new List<byte[]>();
            for (int i = 0; i < 35; i++)
            {
                var data = (i % 3 == 0) ? GetInlineData((byte)(i + 100)) : GetOutOfLineData((byte)(i + 100));
                other.Add(data);
                toInsert.Add(data);
            }

            Span<int> sortedLookup = stackalloc int[35];
            Span<int> insertPositions = stackalloc int[35];
            for (int i = 0; i < 35; i++)
            {
                sortedLookup[i] = i;
                insertPositions[i] = 50; // Multiple inserts at same original position (index 50)
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

            binaryList.Add(GetInlineData(1));
            Assert.True(binaryList.Get(135).SequenceEqual(GetInlineData(1)));
        }

        [Fact]
        public void TestDeleteBatchLargeBatchAvx()
        {
            using BinaryViewList binaryList = new BinaryViewList(GlobalMemoryManager.Instance);

            List<byte[]> allData = new List<byte[]>();
            for (int i = 0; i < 100; i++)
            {
                var data = (i % 2 == 0) ? GetInlineData((byte)i) : GetOutOfLineData((byte)i);
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
            binaryList.Add(GetInlineData(1));
            Assert.True(binaryList.Get(binaryList.Count - 1).SequenceEqual(GetInlineData(1)));
        }

        [Fact]
        public void TestGetMemory()
        {
            var e1 = GetOutOfLineData(1);
            var e2 = GetInlineData(2);
            using BinaryViewList binaryList = new BinaryViewList(GlobalMemoryManager.Instance);
            binaryList.Add(e1);
            binaryList.Add(e2);

            var index1Mem = binaryList.GetMemory(0);
            var index2Mem = binaryList.GetMemory(1);
            Assert.True(index1Mem.Span.SequenceEqual(e1));
            Assert.True(index2Mem.Span.SequenceEqual(e2));
        }

        [Fact]
        public void TestInsertFromSingleElementAtBeginning()
        {
            var e1 = GetInlineData(1);
            var e2 = GetOutOfLineData(2);
            var eNew = GetOutOfLineData(10);

            using BinaryViewList binaryList = new BinaryViewList(GlobalMemoryManager.Instance);
            binaryList.Add(e1);
            binaryList.Add(e2);

            using BinaryViewList other = new BinaryViewList(GlobalMemoryManager.Instance);
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
            var e1 = GetInlineData(1);
            var e2 = GetOutOfLineData(2);
            var eNew = GetOutOfLineData(10);

            using BinaryViewList binaryList = new BinaryViewList(GlobalMemoryManager.Instance);
            binaryList.Add(e1);
            binaryList.Add(e2);

            using BinaryViewList other = new BinaryViewList(GlobalMemoryManager.Instance);
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
            var e1 = GetInlineData(1);
            var e2 = GetOutOfLineData(2);
            var eNew = GetOutOfLineData(10);

            using BinaryViewList binaryList = new BinaryViewList(GlobalMemoryManager.Instance);
            binaryList.Add(e1);
            binaryList.Add(e2);

            using BinaryViewList other = new BinaryViewList(GlobalMemoryManager.Instance);
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
        public void TestInsertFromMultipleElementsIntoEmptyList()
        {
            var eA = GetInlineData(10);
            var eB = GetOutOfLineData(20);

            using BinaryViewList binaryList = new BinaryViewList(GlobalMemoryManager.Instance);

            using BinaryViewList other = new BinaryViewList(GlobalMemoryManager.Instance);
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
        public void TestDeleteBatchSingleElementAtBeginning()
        {
            var e1 = GetInlineData(1);
            var e2 = GetOutOfLineData(2);
            var e3 = GetInlineData(3);

            using BinaryViewList binaryList = new BinaryViewList(GlobalMemoryManager.Instance);
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
            var e1 = GetInlineData(1);
            var e2 = GetOutOfLineData(2);
            var e3 = GetInlineData(3);

            using BinaryViewList binaryList = new BinaryViewList(GlobalMemoryManager.Instance);
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
            var e1 = GetInlineData(1);
            var e2 = GetOutOfLineData(2);
            var e3 = GetInlineData(3);

            using BinaryViewList binaryList = new BinaryViewList(GlobalMemoryManager.Instance);
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
        public void TestDeleteBatchTwoConsecutiveCalls()
        {
            var e1 = GetInlineData(1);
            var e2 = GetOutOfLineData(2);
            var e3 = GetInlineData(3);
            var e4 = GetOutOfLineData(4);
            var e5 = GetInlineData(5);

            using BinaryViewList binaryList = new BinaryViewList(GlobalMemoryManager.Instance);
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
            var e1 = GetInlineData(1);
            var e2 = GetOutOfLineData(2);
            var e3 = GetInlineData(3);
            var e4 = GetOutOfLineData(4);
            var e5 = GetInlineData(5);

            using BinaryViewList binaryList = new BinaryViewList(GlobalMemoryManager.Instance);
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
            var e1 = GetInlineData(1);
            var e2 = GetOutOfLineData(2);
            var e3 = GetInlineData(3);
            var e4 = GetOutOfLineData(4);
            var e5 = GetInlineData(5);

            using BinaryViewList binaryList = new BinaryViewList(GlobalMemoryManager.Instance);
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
            using BinaryViewList binaryList = new BinaryViewList(GlobalMemoryManager.Instance);
            binaryList.Add(Array.Empty<byte>());
            binaryList.Add(Array.Empty<byte>());
            binaryList.Add(Array.Empty<byte>());
            binaryList.Add(Array.Empty<byte>());

            // Delete every other empty element
            ReadOnlySpan<int> targets = stackalloc int[] { 0, 2 };
            binaryList.DeleteBatch(targets);

            Assert.Equal(2, binaryList.Count);
            Assert.True(binaryList.Get(0).SequenceEqual(Array.Empty<byte>()));
            Assert.True(binaryList.Get(1).SequenceEqual(Array.Empty<byte>()));
        }

        [Fact]
        public void TestDeleteBatchSingleElementList()
        {
            var e1 = GetOutOfLineData(1);
            using BinaryViewList binaryList = new BinaryViewList(GlobalMemoryManager.Instance);
            binaryList.Add(e1);

            ReadOnlySpan<int> targets = stackalloc int[] { 0 };
            binaryList.DeleteBatch(targets);

            Assert.Equal(0, binaryList.Count);
        }

        [Fact]
        public void TestDeleteBatchWithEmptyElements()
        {
            var e1 = GetInlineData(1);
            var e2 = Array.Empty<byte>();
            var e3 = GetOutOfLineData(3);
            var e4 = Array.Empty<byte>();
            var e5 = GetInlineData(5);

            using BinaryViewList binaryList = new BinaryViewList(GlobalMemoryManager.Instance);
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
        public void TestUpdateMultipleElements()
        {
            var e1 = GetInlineData(1);
            var e2 = GetOutOfLineData(2);
            var e3 = GetInlineData(3);
            using BinaryViewList binaryList = new BinaryViewList(GlobalMemoryManager.Instance);
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
            var e1 = GetInlineData(1);
            var e2 = GetOutOfLineData(2);
            var e3 = GetInlineData(3);
            using BinaryViewList binaryList = new BinaryViewList(GlobalMemoryManager.Instance);
            binaryList.Insert(0, e1);
            binaryList.Insert(1, e2);
            binaryList.UpdateAt(1, e3);

            Assert.True(binaryList.Get(0).SequenceEqual(e1));
            Assert.True(binaryList.Get(1).SequenceEqual(e3));
        }

        [Fact]
        public void TestInsertFromAllAtEnd()
        {
            var e1 = GetInlineData(1);
            var eA = GetOutOfLineData(10);
            var eB = GetInlineData(20);

            using BinaryViewList binaryList = new BinaryViewList(GlobalMemoryManager.Instance);
            binaryList.Add(e1);

            using BinaryViewList other = new BinaryViewList(GlobalMemoryManager.Instance);
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
            var e1 = GetInlineData(1);
            var e2 = GetOutOfLineData(2);
            var eA = GetOutOfLineData(10);
            var eB = GetInlineData(20);

            using BinaryViewList binaryList = new BinaryViewList(GlobalMemoryManager.Instance);
            binaryList.Add(e1);
            binaryList.Add(e2);

            using BinaryViewList other = new BinaryViewList(GlobalMemoryManager.Instance);
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
        public void TestInsertFromInterleavedInsertions()
        {
            var e1 = GetInlineData(1);
            var e2 = GetOutOfLineData(2);
            var e3 = GetInlineData(3);
            var eA = GetOutOfLineData(10);
            var eB = GetInlineData(20);

            using BinaryViewList binaryList = new BinaryViewList(GlobalMemoryManager.Instance);
            binaryList.Add(e1);
            binaryList.Add(e2);
            binaryList.Add(e3);

            using BinaryViewList other = new BinaryViewList(GlobalMemoryManager.Instance);
            other.Add(eA);
            other.Add(eB);

            // Insert eA after e1 (index 1), eB after e2 (which becomes index 3)
            ReadOnlySpan<int> sortedLookup = stackalloc int[] { 0, 1 };
            ReadOnlySpan<int> insertPositions = stackalloc int[] { 1, 2 };
            binaryList.InsertFrom(in other, in sortedLookup, in insertPositions, -1);

            Assert.Equal(5, binaryList.Count);
            Assert.True(binaryList.Get(0).SequenceEqual(e1));
            Assert.True(binaryList.Get(1).SequenceEqual(eA));
            Assert.True(binaryList.Get(2).SequenceEqual(e2));
            Assert.True(binaryList.Get(3).SequenceEqual(eB));
            Assert.True(binaryList.Get(4).SequenceEqual(e3));
        }

        [Fact]
        public void TestInsertFromExceedingCapacity()
        {
            using BinaryViewList binaryList = new BinaryViewList(GlobalMemoryManager.Instance);

            var e1 = GetInlineData(1);
            var e2 = GetOutOfLineData(2);
            binaryList.Add(e1);
            binaryList.Add(e2);

            using BinaryViewList other = new BinaryViewList(GlobalMemoryManager.Instance);
            for (int i = 0; i < 100; i++)
            {
                other.Add(GetOutOfLineData((byte)(10 + i)));
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
                Assert.True(binaryList.Get(i).SequenceEqual(GetOutOfLineData((byte)(10 + i))));
            }
            Assert.True(binaryList.Get(100).SequenceEqual(e1));
            Assert.True(binaryList.Get(101).SequenceEqual(e2));
        }

        [Fact]
        public void TestInlineExactBoundary()
        {
            var e1 = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12 }; // Exactly 12 bytes
            using BinaryViewList binaryList = new BinaryViewList(GlobalMemoryManager.Instance);
            binaryList.Add(e1);

            Assert.Equal(1, binaryList.Count);
            Assert.True(binaryList.Get(0).SequenceEqual(e1));
        }

        [Fact]
        public void TestOutOfLineExactBoundary()
        {
            var e1 = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13 }; // Exactly 13 bytes
            using BinaryViewList binaryList = new BinaryViewList(GlobalMemoryManager.Instance);
            binaryList.Add(e1);

            Assert.Equal(1, binaryList.Count);
            Assert.True(binaryList.Get(0).SequenceEqual(e1));
        }

        [Fact]
        public void TestDisposeMultipleTimes()
        {
            var binaryList = new BinaryViewList(GlobalMemoryManager.Instance);
            binaryList.Add(new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13 });
            
            binaryList.Dispose();
            binaryList.Dispose(); // Should not throw
        }

        [Fact]
        public void TestUpdateTransitions()
        {
            var inlineData = new byte[] { 1, 2, 3 };
            var outOfLineData = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 };

            using BinaryViewList binaryList = new BinaryViewList(GlobalMemoryManager.Instance);
            binaryList.Add(inlineData);
            Assert.True(binaryList.Get(0).SequenceEqual(inlineData));

            // Inline -> Out of Line
            binaryList.UpdateAt(0, outOfLineData);
            Assert.True(binaryList.Get(0).SequenceEqual(outOfLineData));

            // Out of Line -> Inline
            binaryList.UpdateAt(0, inlineData);
            Assert.True(binaryList.Get(0).SequenceEqual(inlineData));

            // Out of Line -> Out of Line (different sizes)
            var largerOutOfLine = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20 };
            binaryList.UpdateAt(0, largerOutOfLine);
            Assert.True(binaryList.Get(0).SequenceEqual(largerOutOfLine));
        }

        [Fact]
        public void TestCompactWithManySmallOutofLine()
        {
            using BinaryViewList binaryList = new BinaryViewList(GlobalMemoryManager.Instance);
            var data13 = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13 };
            var data14 = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14 };

            // Add 100 out-of-line elements
            for (int i = 0; i < 100; i++)
            {
                binaryList.Add(data13);
            }

            // Update all to slightly larger size to trigger heavy fragmentation
            for (int i = 0; i < 100; i++)
            {
                binaryList.UpdateAt(i, data14);
            }

            // Validate all elements are correct
            for (int i = 0; i < 100; i++)
            {
                Assert.True(binaryList.Get(i).SequenceEqual(data14));
            }

            // Cause a resize/compaction
            binaryList.Add(new byte[1000]); // Forces EnsureDataCapacity which will compact the dead bytes

            for (int i = 0; i < 100; i++)
            {
                Assert.True(binaryList.Get(i).SequenceEqual(data14));
            }
        }

        [Fact]
        public void TestAddEmpty()
        {
            using BinaryViewList binaryList = new BinaryViewList(GlobalMemoryManager.Instance);
            binaryList.AddEmpty();
            binaryList.AddEmpty();

            Assert.Equal(2, binaryList.Count);
            Assert.True(binaryList.Get(0).SequenceEqual(Array.Empty<byte>()));
            Assert.True(binaryList.Get(1).SequenceEqual(Array.Empty<byte>()));
        }

        [Fact]
        public void TestInsertNullRange()
        {
            var e1 = GetInlineData(1);
            var e2 = GetOutOfLineData(2);

            using BinaryViewList binaryList = new BinaryViewList(GlobalMemoryManager.Instance);
            binaryList.Add(e1);
            binaryList.Add(e2);

            binaryList.InsertNullRange(1, 3); // Insert 3 empty elements at index 1

            Assert.Equal(5, binaryList.Count);
            Assert.True(binaryList.Get(0).SequenceEqual(e1));
            Assert.True(binaryList.Get(1).SequenceEqual(Array.Empty<byte>()));
            Assert.True(binaryList.Get(2).SequenceEqual(Array.Empty<byte>()));
            Assert.True(binaryList.Get(3).SequenceEqual(Array.Empty<byte>()));
            Assert.True(binaryList.Get(4).SequenceEqual(e2));
        }

        [Fact]
        public void TestInsertNullRangeAtEnd()
        {
            var e1 = GetInlineData(1);

            using BinaryViewList binaryList = new BinaryViewList(GlobalMemoryManager.Instance);
            binaryList.Add(e1);

            binaryList.InsertNullRange(1, 2);

            Assert.Equal(3, binaryList.Count);
            Assert.True(binaryList.Get(0).SequenceEqual(e1));
            Assert.True(binaryList.Get(1).SequenceEqual(Array.Empty<byte>()));
            Assert.True(binaryList.Get(2).SequenceEqual(Array.Empty<byte>()));
        }


        [Fact]
        public void TestGetPrefixSumByteSizesNullRange()
        {
            using BinaryViewList binaryList = new BinaryViewList(GlobalMemoryManager.Instance);
            binaryList.Add(GetInlineData(1));
            binaryList.InsertNullRange(1, 2);
            binaryList.Add(GetOutOfLineData(2));

            ReadOnlySpan<int> indices = stackalloc int[] { 0, 1, 2, 3 };
            Span<int> sizes = stackalloc int[4];
            binaryList.GetPrefixSumByteSizes(indices, sizes);

            Assert.Equal(7, sizes[0]); // 3 + 4
            Assert.Equal(11, sizes[1]); // 7 + (0 + 4)
            Assert.Equal(15, sizes[2]); // 11 + (0 + 4)
            Assert.Equal(39, sizes[3]); // 15 + (20 + 4)
        }



        [Fact]
        public void TestLargeContinuousInsertAndDeletions()
        {
            using BinaryViewList binaryList = new BinaryViewList(GlobalMemoryManager.Instance);
            var e1 = GetOutOfLineData(1);
            int[] targetArr = new int[] { 0 };

            // Continually insert and delete elements to test stability of the data buffer
            for (int i = 0; i < 1000; i++)
            {
                binaryList.Add(e1);
                
                if (i > 0 && i % 10 == 0)
                {
                    // Every 10 iterations, delete the first element
                    binaryList.DeleteBatch(targetArr);
                }
            }

            // Loop goes from 0 to 999.
            // When i = 10, 20, ..., 990 (99 times), we delete 1 element.
            Assert.Equal(1000 - 99, binaryList.Count);
        }

        [Fact]
        public void TestInsertRangeFromEmptyCount()
        {
            using BinaryViewList binaryList = new BinaryViewList(GlobalMemoryManager.Instance);
            using BinaryViewList other = new BinaryViewList(GlobalMemoryManager.Instance);
            other.Add(GetInlineData(1));
            
            binaryList.InsertRangeFrom(0, other, 0, 0); // Count = 0
            Assert.Equal(0, binaryList.Count);
        }

        [Fact]
        public void TestAddAfterRemovalsWithCompactingGrowth()
        {
            using BinaryViewList binaryList = new BinaryViewList(GlobalMemoryManager.Instance);

            // Add 5 out-of-line elements (20 bytes each = 100 bytes total)
            for (int i = 0; i < 5; i++)
            {
                binaryList.Add(GetOutOfLineData((byte)i));
            }

            // Remove the first 3 elements to create fragmentation in the data buffer
            binaryList.RemoveAt(0);
            binaryList.RemoveAt(0);
            binaryList.RemoveAt(0);

            // 2 elements remain (original values 3 and 4)
            Assert.Equal(2, binaryList.Count);
            Assert.True(binaryList.Get(0).SequenceEqual(GetOutOfLineData(3)));
            Assert.True(binaryList.Get(1).SequenceEqual(GetOutOfLineData(4)));

            // Add a large element that forces the data buffer to grow while fragmented
            byte[] bigData = Enumerable.Repeat((byte)0xAA, 100).ToArray();
            binaryList.Add(bigData);

            Assert.Equal(3, binaryList.Count);
            Assert.True(binaryList.Get(2).SequenceEqual(bigData));

            // Add several more out-of-line elements after the growth
            byte[] extraData = Enumerable.Repeat((byte)0xBB, 20).ToArray();
            binaryList.Add(extraData);
            binaryList.Add(extraData);
            binaryList.Add(extraData);

            // Verify all elements are still intact
            Assert.Equal(6, binaryList.Count);
            Assert.True(binaryList.Get(0).SequenceEqual(GetOutOfLineData(3)));
            Assert.True(binaryList.Get(1).SequenceEqual(GetOutOfLineData(4)));
            Assert.True(binaryList.Get(2).SequenceEqual(bigData));
            Assert.True(binaryList.Get(3).SequenceEqual(extraData));
            Assert.True(binaryList.Get(4).SequenceEqual(extraData));
            Assert.True(binaryList.Get(5).SequenceEqual(extraData));
        }

        [Fact]
        public void TestRemoveRange()
        {
            var e1 = GetInlineData(1);
            var e2 = GetOutOfLineData(2);
            var e3 = GetInlineData(3);
            var e4 = GetOutOfLineData(4);
            var e5 = GetInlineData(5);

            using BinaryViewList binaryList = new BinaryViewList(GlobalMemoryManager.Instance);
            binaryList.Add(e1);
            binaryList.Add(e2);
            binaryList.Add(e3);
            binaryList.Add(e4);
            binaryList.Add(e5);

            // Remove middle 3 elements
            binaryList.RemoveRange(1, 3);

            Assert.Equal(2, binaryList.Count);
            Assert.True(binaryList.Get(0).SequenceEqual(e1));
            Assert.True(binaryList.Get(1).SequenceEqual(e5));
        }

        [Fact]
        public void TestRemoveRangeFromBeginning()
        {
            var e1 = GetOutOfLineData(1);
            var e2 = GetInlineData(2);
            var e3 = GetOutOfLineData(3);

            using BinaryViewList binaryList = new BinaryViewList(GlobalMemoryManager.Instance);
            binaryList.Add(e1);
            binaryList.Add(e2);
            binaryList.Add(e3);

            binaryList.RemoveRange(0, 2);

            Assert.Equal(1, binaryList.Count);
            Assert.True(binaryList.Get(0).SequenceEqual(e3));
        }

        [Fact]
        public void TestRemoveRangeFromEnd()
        {
            var e1 = GetInlineData(1);
            var e2 = GetOutOfLineData(2);
            var e3 = GetInlineData(3);

            using BinaryViewList binaryList = new BinaryViewList(GlobalMemoryManager.Instance);
            binaryList.Add(e1);
            binaryList.Add(e2);
            binaryList.Add(e3);

            binaryList.RemoveRange(1, 2);

            Assert.Equal(1, binaryList.Count);
            Assert.True(binaryList.Get(0).SequenceEqual(e1));
        }

        [Fact]
        public void TestGetByteSize()
        {
            var e1 = GetInlineData(1);      // 3 bytes
            var e2 = GetOutOfLineData(2);    // 20 bytes
            var e3 = new byte[] { };         // 0 bytes
            var e4 = GetInlineData(4);       // 3 bytes

            using BinaryViewList binaryList = new BinaryViewList(GlobalMemoryManager.Instance);
            binaryList.Add(e1);
            binaryList.Add(e2);
            binaryList.Add(e3);
            binaryList.Add(e4);

            // GetByteSize(start, end) = sum of (element.Length + sizeof(int)) for each element in [start, end]
            // Range [0, 3]: (3+4) + (20+4) + (0+4) + (3+4) = 42
            Assert.Equal(42, binaryList.GetByteSize(0, 3));

            // Range [1, 2]: (20+4) + (0+4) = 28
            Assert.Equal(28, binaryList.GetByteSize(1, 2));

            // Single element [0, 0]: (3+4) = 7
            Assert.Equal(7, binaryList.GetByteSize(0, 0));
        }

        [Fact]
        public void TestGetBinaryInfoInline()
        {
            var e1 = new byte[] { 0xAA, 0xBB, 0xCC };

            using BinaryViewList binaryList = new BinaryViewList(GlobalMemoryManager.Instance);
            binaryList.Add(e1);

            var info = binaryList.GetBinaryInfo(0);
            Assert.Equal(3, info.length);
            Assert.True(info.Span.SequenceEqual(e1));
        }

        [Fact]
        public void TestGetBinaryInfoOutOfLine()
        {
            var e1 = Enumerable.Repeat((byte)0xDD, 20).ToArray();

            using BinaryViewList binaryList = new BinaryViewList(GlobalMemoryManager.Instance);
            binaryList.Add(e1);

            var info = binaryList.GetBinaryInfo(0);
            Assert.Equal(20, info.length);
            Assert.True(info.Span.SequenceEqual(e1));
        }

        [Fact]
        public void TestInsertEmpty()
        {
            var e1 = GetInlineData(1);
            var e2 = GetOutOfLineData(2);

            using BinaryViewList binaryList = new BinaryViewList(GlobalMemoryManager.Instance);
            binaryList.Add(e1);
            binaryList.Add(e2);

            binaryList.InsertEmpty(1);

            Assert.Equal(3, binaryList.Count);
            Assert.True(binaryList.Get(0).SequenceEqual(e1));
            Assert.True(binaryList.Get(1).SequenceEqual(Array.Empty<byte>()));
            Assert.True(binaryList.Get(2).SequenceEqual(e2));
        }

        [Fact]
        public void TestInsertEmptyAtBeginning()
        {
            var e1 = GetOutOfLineData(1);

            using BinaryViewList binaryList = new BinaryViewList(GlobalMemoryManager.Instance);
            binaryList.Add(e1);

            binaryList.InsertEmpty(0);

            Assert.Equal(2, binaryList.Count);
            Assert.True(binaryList.Get(0).SequenceEqual(Array.Empty<byte>()));
            Assert.True(binaryList.Get(1).SequenceEqual(e1));
        }

        [Fact]
        public void TestCompactDirect()
        {
            using BinaryViewList binaryList = new BinaryViewList(GlobalMemoryManager.Instance);

            // Add out-of-line elements
            for (int i = 0; i < 10; i++)
            {
                binaryList.Add(GetOutOfLineData((byte)i));
            }

            // Remove half to create fragmentation
            binaryList.RemoveAt(1);
            binaryList.RemoveAt(2); // was index 3
            binaryList.RemoveAt(3); // was index 5
            binaryList.RemoveAt(4); // was index 7
            binaryList.RemoveAt(5); // was index 9

            // 5 elements remain: original 0, 2, 4, 6, 8
            Assert.Equal(5, binaryList.Count);

            binaryList.Compact();

            // Verify all data is still accessible after compaction
            Assert.Equal(5, binaryList.Count);
            Assert.True(binaryList.Get(0).SequenceEqual(GetOutOfLineData(0)));
            Assert.True(binaryList.Get(1).SequenceEqual(GetOutOfLineData(2)));
            Assert.True(binaryList.Get(2).SequenceEqual(GetOutOfLineData(4)));
            Assert.True(binaryList.Get(3).SequenceEqual(GetOutOfLineData(6)));
            Assert.True(binaryList.Get(4).SequenceEqual(GetOutOfLineData(8)));

            // Add more elements after compaction to verify the buffer is still usable
            var newData = GetOutOfLineData(99);
            binaryList.Add(newData);
            Assert.Equal(6, binaryList.Count);
            Assert.True(binaryList.Get(5).SequenceEqual(newData));
        }

        [Fact]
        public void TestCopyDoesNotMutateSource()
        {
            using BinaryViewList binaryList = new BinaryViewList(GlobalMemoryManager.Instance);

            for (int i = 0; i < 5; i++)
                binaryList.Add(GetOutOfLineData((byte)i));

            // Create fragmentation
            binaryList.RemoveAt(1);
            binaryList.RemoveAt(2); // was index 3

            // Snapshot the source data before copy
            var expected = new byte[3][];
            for (int i = 0; i < 3; i++)
                expected[i] = binaryList.Get(i).ToArray();

            // Copy should not mutate the source
            using var copy = binaryList.Copy(GlobalMemoryManager.Instance);

            // Verify source is unchanged
            Assert.Equal(3, binaryList.Count);
            for (int i = 0; i < 3; i++)
                Assert.True(binaryList.Get(i).SequenceEqual(expected[i]));

            // Verify copy has correct data
            Assert.Equal(3, copy.Count);
            for (int i = 0; i < 3; i++)
                Assert.True(copy.Get(i).SequenceEqual(expected[i]));

            // Verify independence: mutating the copy doesn't affect the source
            copy.UpdateAt(0, GetInlineData(99));
            Assert.True(binaryList.Get(0).SequenceEqual(expected[0]));
        }

        [Fact]
        public void TestInsertRangeFromPartialRange()
        {
            var e1 = GetInlineData(1);
            var e2 = GetOutOfLineData(2);

            using BinaryViewList binaryList = new BinaryViewList(GlobalMemoryManager.Instance);
            binaryList.Add(e1);
            binaryList.Add(e2);

            using BinaryViewList other = new BinaryViewList(GlobalMemoryManager.Instance);
            other.Add(GetInlineData(10));
            other.Add(GetOutOfLineData(20));
            other.Add(GetInlineData(30));
            other.Add(GetOutOfLineData(40));

            // Copy only elements at indices 1 and 2 from other (start=1, count=2)
            binaryList.InsertRangeFrom(1, other, 1, 2);

            Assert.Equal(4, binaryList.Count);
            Assert.True(binaryList.Get(0).SequenceEqual(e1));
            Assert.True(binaryList.Get(1).SequenceEqual(GetOutOfLineData(20)));
            Assert.True(binaryList.Get(2).SequenceEqual(GetInlineData(30)));
            Assert.True(binaryList.Get(3).SequenceEqual(e2));
        }

        [Fact]
        public void TestInsertFromWithFragmentation()
        {
            using BinaryViewList binaryList = new BinaryViewList(GlobalMemoryManager.Instance);

            // Fill with out-of-line data
            for (int i = 0; i < 10; i++)
                binaryList.Add(GetOutOfLineData((byte)i));

            // Remove half to create fragmentation
            for (int i = 9; i >= 0; i -= 2)
                binaryList.RemoveAt(i);

            // 5 elements remain (original 0, 2, 4, 6, 8)
            Assert.Equal(5, binaryList.Count);

            using BinaryViewList other = new BinaryViewList(GlobalMemoryManager.Instance);
            for (int i = 0; i < 50; i++)
                other.Add(GetOutOfLineData((byte)(100 + i)));

            // Insert all 50 elements at position 0 (forces data buffer growth with fragmentation)
            Span<int> sortedLookup = stackalloc int[50];
            Span<int> insertPositions = stackalloc int[50];
            for (int i = 0; i < 50; i++)
            {
                sortedLookup[i] = i;
                insertPositions[i] = 0;
            }

            ReadOnlySpan<int> sortedLookupRO = sortedLookup;
            ReadOnlySpan<int> insertPositionsRO = insertPositions;
            binaryList.InsertFrom(in other, in sortedLookupRO, in insertPositionsRO, -1);

            Assert.Equal(55, binaryList.Count);

            // Verify inserted elements
            for (int i = 0; i < 50; i++)
                Assert.True(binaryList.Get(i).SequenceEqual(GetOutOfLineData((byte)(100 + i))));

            // Verify original elements shifted after the insertions
            Assert.True(binaryList.Get(50).SequenceEqual(GetOutOfLineData(0)));
            Assert.True(binaryList.Get(51).SequenceEqual(GetOutOfLineData(2)));
            Assert.True(binaryList.Get(52).SequenceEqual(GetOutOfLineData(4)));
            Assert.True(binaryList.Get(53).SequenceEqual(GetOutOfLineData(6)));
            Assert.True(binaryList.Get(54).SequenceEqual(GetOutOfLineData(8)));
        }
    }
}
