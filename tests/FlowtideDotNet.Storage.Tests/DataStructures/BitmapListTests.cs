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
    public class BitmapListTests
    {
        [Fact]
        public void TestRemoveAtFirst()
        {
            var list = new BitmapList(GlobalMemoryManager.Instance);
            list.Set(0);
            list.Set(31);
            list.Set(33);
            list.RemoveAt(0);

            Assert.False(list.Get(0));
            Assert.True(list.Get(30));
            Assert.True(list.Get(32));
        }

        [Fact]
        public void TestRemoveAtMiddle()
        {
            var list = new BitmapList(GlobalMemoryManager.Instance);
            list.Set(2);
            list.Set(16);
            list.Set(28);
            list.Set(31);
            list.Set(34);
            list.RemoveAt(16);

            Assert.True(list.Get(2));
            Assert.False(list.Get(16));
            Assert.True(list.Get(27));
            Assert.True(list.Get(30));
            Assert.True(list.Get(33));
        }

        [Fact]
        public void TestRemoveAtLast()
        {
            var list = new BitmapList(GlobalMemoryManager.Instance);
            list.Set(2);
            list.Set(16);
            list.Set(28);
            list.Set(31);
            list.Set(34);
            list.RemoveAt(31);

            Assert.True(list.Get(2));
            Assert.True(list.Get(16));
            Assert.True(list.Get(28));
            Assert.False(list.Get(31));
            Assert.True(list.Get(33));
        }

        [Fact]
        public void TestRemoveAtLastNextBitTrue()
        {
            var list = new BitmapList(GlobalMemoryManager.Instance);
            list.Set(2);
            list.Set(16);
            list.Set(28);
            list.Set(31);
            list.Set(32);
            list.Set(34);
            list.RemoveAt(31);

            Assert.True(list.Get(2));
            Assert.True(list.Get(16));
            Assert.True(list.Get(28));
            Assert.False(list.Get(30));
            Assert.True(list.Get(31));
            Assert.False(list.Get(32));
            Assert.True(list.Get(33));
        }

        [Fact]
        public void TestInsertAt()
        {
            var list = new BitmapList(GlobalMemoryManager.Instance);
            list.Set(2);
            list.Set(16);
            list.Set(28);
            list.Set(31);
            list.Set(32);
            list.Set(34);
            list.InsertAt(3, true);

            Assert.True(list.Get(2));
            Assert.True(list.Get(17));
            Assert.True(list.Get(29));
            Assert.True(list.Get(32));
            Assert.True(list.Get(33));
            Assert.True(list.Get(35));
        }

        [Fact]
        public void TestInsertAtSecondInteger()
        {
            var list = new BitmapList(GlobalMemoryManager.Instance);
            list.Set(2);
            list.Set(16);
            list.Set(28);
            list.Set(31);
            list.Set(32);
            list.Set(34);
            list.InsertAt(33, true);

            Assert.True(list.Get(2));
            Assert.True(list.Get(16));
            Assert.True(list.Get(28));
            Assert.True(list.Get(31));
            Assert.True(list.Get(32));
            Assert.True(list.Get(33));
            Assert.True(list.Get(35));
        }

        [Fact]
        public void TestInitialization()
        {
            var list = new BitmapList(GlobalMemoryManager.Instance);
            for (int i = 0; i < 64; i++)
            {
                Assert.False(list.Get(i));
            }
        }

        [Fact]
        public void TestSetAndUnsetBits()
        {
            var list = new BitmapList(GlobalMemoryManager.Instance);
            list.Set(0);
            Assert.True(list.Get(0));
            list.Unset(0);
            Assert.False(list.Get(0));

            list.Set(63);
            Assert.True(list.Get(63));
            list.Unset(63);
            Assert.False(list.Get(63));
        }


        [Fact]
        public void TestBoundaryConditions()
        {
            var list = new BitmapList(GlobalMemoryManager.Instance);
            list.Set(0);
            list.Set(31);
            list.Set(32);
            list.Set(63);

            Assert.True(list.Get(0));
            Assert.True(list.Get(31));
            Assert.True(list.Get(32));
            Assert.True(list.Get(63));

            list.RemoveAt(31);
            Assert.True(list.Get(31));
            Assert.True(list.Get(62));

            list.InsertAt(32, true);
            Assert.True(list.Get(31));
            Assert.True(list.Get(32));
        }

        [Fact]
        public void TestInsertRemoveInEmptyList()
        {
            var list = new BitmapList(GlobalMemoryManager.Instance);
            list.InsertAt(0, true);
            Assert.True(list.Get(0));

            list.RemoveAt(0);
            Assert.False(list.Get(0));
        }

        [Fact]
        public void TestInsertRemoveSingleBitList()
        {
            var list = new BitmapList(GlobalMemoryManager.Instance);
            list.Set(10);
            list.InsertAt(5, true);
            Assert.True(list.Get(5));
            Assert.True(list.Get(11)); // Original bit should have shifted

            list.RemoveAt(5);
            Assert.False(list.Get(5));
            Assert.True(list.Get(10)); // Original bit should have shifted back
        }

        [Fact]
        public void TestLargeIndexSet()
        {
            var list = new BitmapList(GlobalMemoryManager.Instance);
            list.Set(1024);
            Assert.True(list.Get(1024));
            Assert.False(list.Get(1023));
        }

        /// <summary>
        /// This test was added since there was a bug when the first bit was false, where it did not extend the array correctly.
        /// </summary>
        [Fact]
        public void TestInsertAtEndOfIndex()
        {
            var list = new BitmapList(GlobalMemoryManager.Instance);
            for (int i = 1; i < 32; i++)
            {
                list.InsertAt(i, true);
            }
            list.InsertAt(31, true);

            Assert.False(list.Get(0));

            for (int i = 1; i < 33; i++)
            {
                Assert.True(list.Get(i));
            }
        }

        [Fact]
        public void TestSequence()
        {
            //var lines = File.ReadAllLines("./ColumnStore/Utils/alloperations.csv");
            var list = new BitmapList(GlobalMemoryManager.Instance);

            list.InsertAt(0, false);

            for (int i = 1; i < 65; i++)
            {
                list.InsertAt(i, true);
            }
            list.RemoveAt(64);

            Assert.False(list.Get(0));
            for (int i = 1; i < 64; i++)
            {
                Assert.True(list.Get(i));
            }
        }

        [Fact]
        public void TestInsertSpecialCaseShiftLeftAtBorder()
        {
            var list = new BitmapList(GlobalMemoryManager.Instance);

            for (int i = 0; i < 583; i++)
            {
                list.Unset(i);
            }
            list.Set(584);
            list.Unset(585);
            for (int i = 586; i <= 606; i++)
            {
                list.Unset(i);
            }

            list.InsertAt(607, false);

            for (int i = 0; i < 583; i++)
            {
                Assert.False(list.Get(i));
            }
            Assert.True(list.Get(584));
            for (int i = 585; i <= 607; i++)
            {
                Assert.False(list.Get(i));
            }
        }

        [Fact]
        public void TestInsertRandomInOrder()
        {
            var list = new BitmapList(GlobalMemoryManager.Instance);

            Random r = new Random(123);

            List<bool> expected = new List<bool>();

            for (int i = 0; i < 1_00_000; i++)
            {
                var v = r.Next(0, 2);
                if (v == 0)
                {
                    list.InsertAt(i, false);
                    expected.Add(false);
                }
                else
                {
                    list.InsertAt(i, true);
                    expected.Add(true);
                }
            }

            for (int i = 0; i < expected.Count; i++)
            {
                Assert.Equal(expected[i], list.Get(i));
            }

            for (int i = 0; i < 10_000; i++)
            {
                var index = r.Next(0, expected.Count);
                list.RemoveAt(index);
                expected.RemoveAt(index);
            }

            for (int i = 0; i < expected.Count; i++)
            {
                Assert.Equal(expected[i], list.Get(i));
            }
        }

        [Fact]
        public void TestInsertRandomRandomOrder()
        {
            var list = new BitmapList(GlobalMemoryManager.Instance);

            Random r = new Random(123);

            List<bool> expected = new List<bool>();

            for (int i = 0; i < 1_00_000; i++)
            {
                var v = r.Next(0, 2);
                bool val = true;
                if (v == 0)
                {
                    val = false;
                }
                var index = r.Next(0, expected.Count);
                list.InsertAt(index, val);
                expected.Insert(index, val);
            }

            for (int i = 0; i < expected.Count; i++)
            {
                Assert.Equal(expected[i], list.Get(i));
            }

            for (int i = 0; i < 10_000; i++)
            {
                var index = r.Next(0, expected.Count);
                list.RemoveAt(index);
                expected.RemoveAt(index);
            }

            for (int i = 0; i < expected.Count; i++)
            {
                Assert.Equal(expected[i], list.Get(i));
            }
        }

        [Fact]
        public void TestRemoveRange()
        {
            for (int seed = 0; seed < 20; seed++)
            {
                var list = new BitmapList(GlobalMemoryManager.Instance);

                Random r = new Random(seed);

                List<bool> expected = new List<bool>();

                for (int i = 0; i < 100_000; i++)
                {
                    var v = r.Next(0, 2);
                    bool val = true;
                    if (v == 0)
                    {
                        val = false;
                    }
                    var index = r.Next(0, expected.Count);
                    list.InsertAt(index, val);
                    expected.Insert(index, val);
                }

                for (int i = 0; i < expected.Count; i++)
                {
                    Assert.Equal(expected[i], list.Get(i));
                }

                for (int i = 0; i < 10_000; i++)
                {
                    var index = r.Next(0, expected.Count);
                    var toRemove = r.Next(0, expected.Count - index);
                    list.RemoveRange(index, toRemove);
                    expected.RemoveRange(index, toRemove);

                    for (int k = 0; k < expected.Count; k++)
                    {
                        Assert.Equal(expected[k], list.Get(k));
                    }
                }
                list.Dispose();
            }
        }

        [Fact]
        public void TestInsertRangeStartNoOffset()
        {
            var list = new BitmapList(GlobalMemoryManager.Instance);
            var other = new BitmapList(GlobalMemoryManager.Instance);

            Random r = new Random(1);

            List<bool> expected = new List<bool>();
            List<bool> otherList = new List<bool>();

            for (int i = 0; i < 10; i++)
            {
                var v = r.Next(0, 2);
                bool val = true;
                if (v == 0)
                {
                    val = false;
                }
                var index = r.Next(0, expected.Count);
                list.InsertAt(index, val);
                expected.Insert(index, val);
            }

            for (int i = 0; i < 10; i++)
            {
                var v = r.Next(0, 2);
                bool val = true;
                if (v == 0)
                {
                    val = false;
                }
                var index = r.Next(0, otherList.Count);
                other.InsertAt(index, val);
                otherList.Insert(index, val);
            }

            list.InsertRangeFrom(1, other, 0, 10);
            expected.InsertRange(1, otherList.GetRange(0, 10));

            for (int i = 0; i < expected.Count; i++)
            {
                Assert.Equal(expected[i], list.Get(i));
            }
        }

        [Fact]
        public void TestInsertRangeStartOffsetOne()
        {
            var list = new BitmapList(GlobalMemoryManager.Instance);
            var other = new BitmapList(GlobalMemoryManager.Instance);

            Random r = new Random(1);

            List<bool> expected = new List<bool>();
            List<bool> otherList = new List<bool>();

            for (int i = 0; i < 10; i++)
            {
                var v = r.Next(0, 2);
                bool val = true;
                if (v == 0)
                {
                    val = false;
                }
                var index = r.Next(0, expected.Count);
                list.InsertAt(index, val);
                expected.Insert(index, val);
            }

            for (int i = 0; i < 10; i++)
            {
                var v = r.Next(0, 2);
                bool val = true;
                if (v == 0)
                {
                    val = false;
                }
                var index = r.Next(0, otherList.Count);
                other.InsertAt(index, val);
                otherList.Insert(index, val);
            }

            list.InsertRangeFrom(1, other, 1, 9);
            expected.InsertRange(1, otherList.GetRange(1, 9));

            for (int i = 0; i < expected.Count; i++)
            {
                Assert.Equal(expected[i], list.Get(i));
            }
        }


        [Fact]
        public void TestInsertRangeSimple()
        {
            var list = new BitmapList(GlobalMemoryManager.Instance);
            var other = new BitmapList(GlobalMemoryManager.Instance);

            Random r = new Random(1);

            List<bool> expected = new List<bool>();
            List<bool> otherList = new List<bool>();

            for (int i = 0; i < 10; i++)
            {
                var v = r.Next(0, 2);
                bool val = true;
                if (v == 0)
                {
                    val = false;
                }
                var index = r.Next(0, expected.Count);
                list.InsertAt(index, val);
                expected.Insert(index, val);
            }

            for (int i = 0; i < 40; i++)
            {
                var v = r.Next(0, 2);
                bool val = true;
                if (v == 0)
                {
                    val = false;
                }
                var index = r.Next(0, otherList.Count);
                other.InsertAt(index, val);
                otherList.Insert(index, val);
            }

            list.InsertRangeFrom(1, other, 2, 32);
            expected.InsertRange(1, otherList.GetRange(2, 32));

            for (int i = 0; i < expected.Count; i++)
            {
                Assert.Equal(expected[i], list.Get(i));
            }
        }

        [Fact]
        public void TestInsertRangeFactor32InsertAtEnd()
        {
            var list = new BitmapList(GlobalMemoryManager.Instance);
            var other = new BitmapList(GlobalMemoryManager.Instance);

            Random r = new Random(1);

            List<bool> expected = new List<bool>();
            List<bool> otherList = new List<bool>();

            for (int i = 0; i < 64; i++)
            {
                var v = r.Next(0, 2);
                bool val = true;
                if (v == 0)
                {
                    val = false;
                }
                var index = r.Next(0, expected.Count);
                list.InsertAt(index, val);
                expected.Insert(index, val);
            }

            for (int i = 0; i < 64; i++)
            {
                var v = r.Next(0, 2);
                bool val = true;
                if (v == 0)
                {
                    val = false;
                }
                var index = r.Next(0, otherList.Count);
                other.InsertAt(index, val);
                otherList.Insert(index, val);
            }

            list.InsertRangeFrom(64, other, 0, 64);
            expected.InsertRange(64, otherList.GetRange(0, 64));

            for (int i = 0; i < expected.Count; i++)
            {
                Assert.Equal(expected[i], list.Get(i));
            }
        }

        [Fact]
        public void TestInsertRange()
        {
            for (int seed = 0; seed < 20; seed++)
            {
                var list = new BitmapList(GlobalMemoryManager.Instance);
                var other = new BitmapList(GlobalMemoryManager.Instance);

                Random r = new Random(seed);

                List<bool> expected = new List<bool>();
                List<bool> otherList = new List<bool>();

                for (int i = 0; i < 100_000; i++)
                {
                    var v = r.Next(0, 2);
                    bool val = true;
                    if (v == 0)
                    {
                        val = false;
                    }
                    var index = r.Next(0, expected.Count);
                    list.InsertAt(index, val);
                    expected.Insert(index, val);
                }

                for (int i = 0; i < 100_000; i++)
                {
                    var v = r.Next(0, 2);
                    bool val = true;
                    if (v == 0)
                    {
                        val = false;
                    }
                    var index = r.Next(0, otherList.Count);
                    other.InsertAt(index, val);
                    otherList.Insert(index, val);
                }


                // Error on index 2
                for (int i = 0; i < 100; i++)
                {
                    var insertLocation = r.Next(0, expected.Count);
                    var index = r.Next(0, otherList.Count);
                    var toAdd = r.Next(0, otherList.Count - index);
                    list.InsertRangeFrom(insertLocation, other, index, toAdd);

                    expected.InsertRange(insertLocation, otherList.GetRange(index, toAdd));
                }

                for (int k = 0; k < expected.Count; k++)
                {
                    Assert.Equal(expected[k], list.Get(k));
                }

                list.Dispose();
            }
        }

        [Fact]
        public void CountTrueInRangeFullRange()
        {
            var bitmapList = new BitmapList(GlobalMemoryManager.Instance);
            var list = new List<bool>();
            Random r = new Random(123);

            for (int i = 0; i < 64; i++)
            {
                var v = r.Next(0, 2);
                bool val = true;
                if (v == 0)
                {
                    val = false;
                }
                bitmapList.Add(val);
                list.Add(val);
            }

            var count = bitmapList.CountTrueInRange(0, list.Count);
            var expectedCount = list.Count(x => x);
            Assert.Equal(expectedCount, count);
        }

        [Fact]
        public void TestCountTrueInRange()
        {
            for (int seed = 0; seed < 100; seed++)
            {
                using var list = new BitmapList(GlobalMemoryManager.Instance);

                Random r = new Random(seed);

                List<bool> expected = new List<bool>();
                for (int i = 0; i < 1000; i++)
                {
                    var v = r.Next(0, 2);
                    bool val = true;
                    if (v == 0)
                    {
                        val = false;
                    }
                    var index = r.Next(0, expected.Count);
                    list.InsertAt(index, val);
                    expected.Insert(index, val);
                }

                for (int i = 0; i < 10_000; i++)
                {
                    var start = r.Next(0, expected.Count);
                    var end = r.Next(start, expected.Count);
                    var count = list.CountTrueInRange(start, end - start + 1);
                    var expectedCount = expected.GetRange(start, end - start + 1).Count(x => x);
                    Assert.Equal(expectedCount, count);
                }
            }
        }

        [Fact]
        public void TestCountFalseInRange()
        {
            var list = new BitmapList(GlobalMemoryManager.Instance);

            Random r = new Random(123);

            List<bool> expected = new List<bool>();
            for (int i = 0; i < 1000; i++)
            {
                var v = r.Next(0, 2);
                bool val = true;
                if (v == 0)
                {
                    val = false;
                }
                var index = r.Next(0, expected.Count);
                list.InsertAt(index, val);
                expected.Insert(index, val);
            }

            for (int i = 0; i < 1000; i++)
            {
                var start = r.Next(0, expected.Count);
                var end = r.Next(start, expected.Count);
                var count = list.CountFalseInRange(start, end - start + 1);
                var expectedCount = expected.GetRange(start, end - start + 1).Count(x => !x);
                Assert.Equal(expectedCount, count);
            }
        }

        [Fact]
        public void TestFindNextFalseIndex()
        {
            var list = new BitmapList(GlobalMemoryManager.Instance);

            for (int i = 0; i < 128; i++)
            {
                list.Add(i % 2 != 0);
            }

            // For loop that does the same thing
            for (int i = 0; i < 127; i++)
            {
                var actual = list.FindNextFalseIndex(i);
                Assert.Equal(i + i % 2, actual);
            }
            var last = list.FindNextFalseIndex(127);
            Assert.Equal(-1, last);
        }

        [Fact]
        public void TestFindNextFalseIndexStartWithTrue()
        {
            var list = new BitmapList(GlobalMemoryManager.Instance);

            for (int i = 0; i < 128; i++)
            {
                list.Add(i % 2 == 0);
            }

            // For loop that does the same thing
            for (int i = 0; i < 128; i++)
            {
                var actual = list.FindNextFalseIndex(i);
                Assert.Equal(i + (i + 1) % 2, actual);
            }
        }

        [Fact]
        public void TestFindNextTrueIndexStartWithFalse()
        {
            var list = new BitmapList(GlobalMemoryManager.Instance);

            for (int i = 0; i < 128; i++)
            {
                list.Add(i % 2 != 0);
            }

            // For loop that does the same thing
            for (int i = 1; i < 128; i++)
            {
                var actual = list.FindNextTrueIndex(i);
                Assert.Equal(i + (i + 1) % 2, actual);
            }
        }

        [Fact]
        public void TestFindNextTrueIndexStartWithTrue()
        {
            var list = new BitmapList(GlobalMemoryManager.Instance);

            for (int i = 0; i < 128; i++)
            {
                list.Add(i % 2 == 0);
            }

            // For loop that does the same thing
            for (int i = 1; i < 127; i++)
            {
                var actual = list.FindNextTrueIndex(i);
                Assert.Equal(i + (i % 2), actual);
            }
            Assert.Equal(-1, list.FindNextTrueIndex(127));
        }

        [Fact]
        public void TestInsertTrueInRangeStartEndInSameIndexStartMod0()
        {
            var list = new BitmapList(GlobalMemoryManager.Instance);
            List<bool> expected = new List<bool>();

            for (int i = 0; i < 128; i++)
            {
                list.Add(i % 2 == 0);
                expected.Add(i % 2 == 0);
            }

            list.InsertTrueInRange(0, 5);
            expected.InsertRange(0, Enumerable.Repeat(true, 5));

            for (int i = 0; i < expected.Count; i++)
            {
                Assert.Equal(expected[i], list.Get(i));
            }

            list.InsertTrueInRange(32, 7);
            expected.InsertRange(32, Enumerable.Repeat(true, 7));

            for (int i = 0; i < expected.Count; i++)
            {
                Assert.Equal(expected[i], list.Get(i));
            }
        }

        [Fact]
        public void TestInsertTrueInRangeStartEndInSameIndexStartNotMod0()
        {
            var list = new BitmapList(GlobalMemoryManager.Instance);
            List<bool> expected = new List<bool>();

            for (int i = 0; i < 128; i++)
            {
                list.Add(i % 2 == 0);
                expected.Add(i % 2 == 0);
            }

            list.InsertTrueInRange(3, 5);
            expected.InsertRange(3, Enumerable.Repeat(true, 5));

            for (int i = 0; i < expected.Count; i++)
            {
                Assert.Equal(expected[i], list.Get(i));
            }

            list.InsertTrueInRange(39, 7);
            expected.InsertRange(39, Enumerable.Repeat(true, 7));

            for (int i = 0; i < expected.Count; i++)
            {
                Assert.Equal(expected[i], list.Get(i));
            }
        }

        [Fact]
        public void TestInsertTrueInRangeStartEndInSameIndexBothMod0()
        {
            var list = new BitmapList(GlobalMemoryManager.Instance);
            List<bool> expected = new List<bool>();

            for (int i = 0; i < 128; i++)
            {
                list.Add(i % 2 == 0);
                expected.Add(i % 2 == 0);
            }

            list.InsertTrueInRange(0, 32);
            expected.InsertRange(0, Enumerable.Repeat(true, 32));

            for (int i = 0; i < expected.Count; i++)
            {
                Assert.Equal(expected[i], list.Get(i));
            }

            list.InsertTrueInRange(32, 32);
            expected.InsertRange(32, Enumerable.Repeat(true, 32));

            for (int i = 0; i < expected.Count; i++)
            {
                Assert.Equal(expected[i], list.Get(i));
            }
        }

        [Fact]
        public void TestInsertTrueInRangeStartEndInDifferentIntegers()
        {
            var list = new BitmapList(GlobalMemoryManager.Instance);
            List<bool> expected = new List<bool>();

            for (int i = 0; i < 128; i++)
            {
                list.Add(i % 2 == 0);
                expected.Add(i % 2 == 0);
            }

            list.InsertTrueInRange(5, 32);
            expected.InsertRange(5, Enumerable.Repeat(true, 32));

            for (int i = 0; i < expected.Count; i++)
            {
                Assert.Equal(expected[i], list.Get(i));
            }

            list.InsertTrueInRange(67, 17);
            expected.InsertRange(67, Enumerable.Repeat(true, 17));

            for (int i = 0; i < expected.Count; i++)
            {
                Assert.Equal(expected[i], list.Get(i));
            }
        }

        [Fact]
        public void TestInsertTrueInRangeStartEndInDifferentIntegersEndWithMod0()
        {
            var list = new BitmapList(GlobalMemoryManager.Instance);
            List<bool> expected = new List<bool>();

            for (int i = 0; i < 128; i++)
            {
                list.Add(i % 2 == 0);
                expected.Add(i % 2 == 0);
            }

            list.InsertTrueInRange(5, 59);
            expected.InsertRange(5, Enumerable.Repeat(true, 59));

            for (int i = 0; i < expected.Count; i++)
            {
                Assert.Equal(expected[i], list.Get(i));
            }

            list.InsertTrueInRange(67, 29);
            expected.InsertRange(67, Enumerable.Repeat(true, 29));

            for (int i = 0; i < expected.Count; i++)
            {
                Assert.Equal(expected[i], list.Get(i));
            }
        }

        [Fact]
        public void TestInsertFalseInRangeStartEndInSameIndexStartMod0()
        {
            var list = new BitmapList(GlobalMemoryManager.Instance);
            List<bool> expected = new List<bool>();

            for (int i = 0; i < 128; i++)
            {
                list.Add(i % 2 == 0);
                expected.Add(i % 2 == 0);
            }

            list.InsertFalseInRange(0, 5);
            expected.InsertRange(0, Enumerable.Repeat(false, 5));

            for (int i = 0; i < expected.Count; i++)
            {
                Assert.Equal(expected[i], list.Get(i));
            }

            list.InsertFalseInRange(32, 7);
            expected.InsertRange(32, Enumerable.Repeat(false, 7));

            for (int i = 0; i < expected.Count; i++)
            {
                Assert.Equal(expected[i], list.Get(i));
            }
        }

        [Fact]
        public void TestInsertFalseInRangeStartEndInSameIndexStartNotMod0()
        {
            var list = new BitmapList(GlobalMemoryManager.Instance);
            List<bool> expected = new List<bool>();

            for (int i = 0; i < 128; i++)
            {
                list.Add(i % 2 == 0);
                expected.Add(i % 2 == 0);
            }

            list.InsertFalseInRange(3, 5);
            expected.InsertRange(3, Enumerable.Repeat(false, 5));

            for (int i = 0; i < expected.Count; i++)
            {
                Assert.Equal(expected[i], list.Get(i));
            }

            list.InsertFalseInRange(39, 7);
            expected.InsertRange(39, Enumerable.Repeat(false, 7));

            for (int i = 0; i < expected.Count; i++)
            {
                Assert.Equal(expected[i], list.Get(i));
            }
        }

        [Fact]
        public void TestInsertFalseInRangeStartEndInSameIndexBothMod0()
        {
            var list = new BitmapList(GlobalMemoryManager.Instance);
            List<bool> expected = new List<bool>();

            for (int i = 0; i < 128; i++)
            {
                list.Add(i % 2 == 0);
                expected.Add(i % 2 == 0);
            }

            list.InsertFalseInRange(0, 32);
            expected.InsertRange(0, Enumerable.Repeat(false, 32));

            for (int i = 0; i < expected.Count; i++)
            {
                Assert.Equal(expected[i], list.Get(i));
            }

            list.InsertFalseInRange(32, 32);
            expected.InsertRange(32, Enumerable.Repeat(false, 32));

            for (int i = 0; i < expected.Count; i++)
            {
                Assert.Equal(expected[i], list.Get(i));
            }
        }

        [Fact]
        public void TestInsertFalseInRangeStartEndInDifferentIntegers()
        {
            var list = new BitmapList(GlobalMemoryManager.Instance);
            List<bool> expected = new List<bool>();

            for (int i = 0; i < 128; i++)
            {
                list.Add(i % 2 == 0);
                expected.Add(i % 2 == 0);
            }

            list.InsertFalseInRange(5, 32);
            expected.InsertRange(5, Enumerable.Repeat(false, 32));

            for (int i = 0; i < expected.Count; i++)
            {
                Assert.Equal(expected[i], list.Get(i));
            }

            list.InsertFalseInRange(67, 17);
            expected.InsertRange(67, Enumerable.Repeat(false, 17));

            for (int i = 0; i < expected.Count; i++)
            {
                Assert.Equal(expected[i], list.Get(i));
            }
        }

        [Fact]
        public void TestInsertFalseInRangeStartEndInDifferentIntegersEndWithMod0()
        {
            var list = new BitmapList(GlobalMemoryManager.Instance);
            List<bool> expected = new List<bool>();

            for (int i = 0; i < 128; i++)
            {
                list.Add(i % 2 == 0);
                expected.Add(i % 2 == 0);
            }

            list.InsertFalseInRange(5, 59);
            expected.InsertRange(5, Enumerable.Repeat(false, 59));

            for (int i = 0; i < expected.Count; i++)
            {
                Assert.Equal(expected[i], list.Get(i));
            }

            list.InsertFalseInRange(67, 29);
            expected.InsertRange(67, Enumerable.Repeat(false, 29));

            for (int i = 0; i < expected.Count; i++)
            {
                Assert.Equal(expected[i], list.Get(i));
            }
        }

        [Fact]
        public void TestInsertFromEmptyLookup()
        {
            var list = new BitmapList(GlobalMemoryManager.Instance);
            list.Add(true);
            list.Add(false);
            list.Add(true);

            var other = new BitmapList(GlobalMemoryManager.Instance);
            other.Add(false);

            list.InsertFrom(in other, Span<int>.Empty, Span<int>.Empty, -1);

            Assert.Equal(3, list.Count);
            Assert.True(list.Get(0));
            Assert.False(list.Get(1));
            Assert.True(list.Get(2));
        }

        [Fact]
        public void TestInsertFromSingleElement()
        {
            var list = new BitmapList(GlobalMemoryManager.Instance);
            list.Add(true);
            list.Add(false);
            list.Add(true);

            var other = new BitmapList(GlobalMemoryManager.Instance);
            other.Add(true);
            other.Add(false);

            int[] sortedLookup = [0];
            int[] insertPositions = [1];

            ReadOnlySpan<int> sl = sortedLookup; ReadOnlySpan<int> ip = insertPositions; list.InsertFrom(in other, in sl, in ip, -1);

            Assert.Equal(4, list.Count);
            Assert.True(list.Get(0));
            Assert.True(list.Get(1));
            Assert.False(list.Get(2));
            Assert.True(list.Get(3));
        }

        [Fact]
        public void TestInsertFromSingleElementFalse()
        {
            var list = new BitmapList(GlobalMemoryManager.Instance);
            list.Add(true);
            list.Add(true);
            list.Add(true);

            var other = new BitmapList(GlobalMemoryManager.Instance);
            other.Add(false);

            int[] sortedLookup = [0];
            int[] insertPositions = [1];

            ReadOnlySpan<int> sl = sortedLookup; ReadOnlySpan<int> ip = insertPositions; list.InsertFrom(in other, in sl, in ip, -1);

            Assert.Equal(4, list.Count);
            Assert.True(list.Get(0));
            Assert.False(list.Get(1));
            Assert.True(list.Get(2));
            Assert.True(list.Get(3));
        }

        [Fact]
        public void TestInsertFromAtBeginning()
        {
            var list = new BitmapList(GlobalMemoryManager.Instance);
            var expected = new List<bool>();

            for (int i = 0; i < 10; i++)
            {
                bool val = i % 3 == 0;
                list.Add(val);
                expected.Add(val);
            }

            var other = new BitmapList(GlobalMemoryManager.Instance);
            var otherList = new List<bool> { true, false, true };
            foreach (var v in otherList)
            {
                other.Add(v);
            }

            int[] sortedLookup = [0, 1, 2];
            int[] insertPositions = [0, 0, 0];

            ReadOnlySpan<int> sl = sortedLookup; ReadOnlySpan<int> ip = insertPositions; list.InsertFrom(in other, in sl, in ip, -1);

            for (int i = 0; i < sortedLookup.Length; i++)
            {
                expected.Insert(insertPositions[i] + i, otherList[sortedLookup[i]]);
            }

            Assert.Equal(expected.Count, list.Count);
            for (int i = 0; i < expected.Count; i++)
            {
                Assert.Equal(expected[i], list.Get(i));
            }
        }

        [Fact]
        public void TestInsertFromAtEnd()
        {
            var list = new BitmapList(GlobalMemoryManager.Instance);
            var expected = new List<bool>();

            for (int i = 0; i < 10; i++)
            {
                bool val = i % 2 == 0;
                list.Add(val);
                expected.Add(val);
            }

            var other = new BitmapList(GlobalMemoryManager.Instance);
            var otherList = new List<bool> { false, true, false };
            foreach (var v in otherList)
            {
                other.Add(v);
            }

            int[] sortedLookup = [0, 1, 2];
            int[] insertPositions = [10, 10, 10];

            ReadOnlySpan<int> sl = sortedLookup; ReadOnlySpan<int> ip = insertPositions; list.InsertFrom(in other, in sl, in ip, -1);

            for (int i = 0; i < sortedLookup.Length; i++)
            {
                expected.Insert(insertPositions[i] + i, otherList[sortedLookup[i]]);
            }

            Assert.Equal(expected.Count, list.Count);
            for (int i = 0; i < expected.Count; i++)
            {
                Assert.Equal(expected[i], list.Get(i));
            }
        }

        [Fact]
        public void TestInsertFromMultiplePositions()
        {
            var list = new BitmapList(GlobalMemoryManager.Instance);
            var expected = new List<bool>();

            for (int i = 0; i < 10; i++)
            {
                bool val = i % 2 == 0;
                list.Add(val);
                expected.Add(val);
            }

            var other = new BitmapList(GlobalMemoryManager.Instance);
            var otherList = new List<bool> { true, false, true, false, true };
            foreach (var v in otherList)
            {
                other.Add(v);
            }

            int[] sortedLookup = [0, 1, 2, 3, 4];
            int[] insertPositions = [1, 3, 5, 7, 9];

            ReadOnlySpan<int> sl = sortedLookup; ReadOnlySpan<int> ip = insertPositions; list.InsertFrom(in other, in sl, in ip, -1);

            for (int i = 0; i < sortedLookup.Length; i++)
            {
                expected.Insert(insertPositions[i] + i, otherList[sortedLookup[i]]);
            }

            Assert.Equal(expected.Count, list.Count);
            for (int i = 0; i < expected.Count; i++)
            {
                Assert.Equal(expected[i], list.Get(i));
            }
        }

        [Fact]
        public void TestInsertFromCrossingWordBoundary()
        {
            var list = new BitmapList(GlobalMemoryManager.Instance);
            var expected = new List<bool>();

            for (int i = 0; i < 64; i++)
            {
                bool val = i % 3 != 0;
                list.Add(val);
                expected.Add(val);
            }

            var other = new BitmapList(GlobalMemoryManager.Instance);
            var otherList = new List<bool>();
            for (int i = 0; i < 5; i++)
            {
                bool val = i % 2 == 0;
                other.Add(val);
                otherList.Add(val);
            }

            int[] sortedLookup = [0, 1, 2, 3, 4];
            int[] insertPositions = [15, 30, 31, 32, 50];

            ReadOnlySpan<int> sl = sortedLookup; ReadOnlySpan<int> ip = insertPositions; list.InsertFrom(in other, in sl, in ip, -1);

            for (int i = 0; i < sortedLookup.Length; i++)
            {
                expected.Insert(insertPositions[i] + i, otherList[sortedLookup[i]]);
            }

            Assert.Equal(expected.Count, list.Count);
            for (int i = 0; i < expected.Count; i++)
            {
                Assert.Equal(expected[i], list.Get(i));
            }
        }

        [Fact]
        public void TestInsertFromIntoEmptyList()
        {
            var list = new BitmapList(GlobalMemoryManager.Instance);
            var expected = new List<bool>();

            var other = new BitmapList(GlobalMemoryManager.Instance);
            var otherList = new List<bool> { true, false, true };
            foreach (var v in otherList)
            {
                other.Add(v);
            }

            int[] sortedLookup = [0, 1, 2];
            int[] insertPositions = [0, 0, 0];

            ReadOnlySpan<int> sl = sortedLookup; ReadOnlySpan<int> ip = insertPositions; list.InsertFrom(in other, in sl, in ip, -1);

            for (int i = 0; i < sortedLookup.Length; i++)
            {
                expected.Insert(insertPositions[i] + i, otherList[sortedLookup[i]]);
            }

            Assert.Equal(expected.Count, list.Count);
            for (int i = 0; i < expected.Count; i++)
            {
                Assert.Equal(expected[i], list.Get(i));
            }
        }

        [Fact]
        public void TestInsertFromAllTrue()
        {
            var list = new BitmapList(GlobalMemoryManager.Instance);
            var expected = new List<bool>();

            for (int i = 0; i < 32; i++)
            {
                list.Add(false);
                expected.Add(false);
            }

            var other = new BitmapList(GlobalMemoryManager.Instance);
            var otherList = new List<bool>();
            for (int i = 0; i < 4; i++)
            {
                other.Add(true);
                otherList.Add(true);
            }

            int[] sortedLookup = [0, 1, 2, 3];
            int[] insertPositions = [0, 10, 20, 30];

            ReadOnlySpan<int> sl = sortedLookup; ReadOnlySpan<int> ip = insertPositions; list.InsertFrom(in other, in sl, in ip, -1);

            for (int i = 0; i < sortedLookup.Length; i++)
            {
                expected.Insert(insertPositions[i] + i, otherList[sortedLookup[i]]);
            }

            Assert.Equal(expected.Count, list.Count);
            for (int i = 0; i < expected.Count; i++)
            {
                Assert.Equal(expected[i], list.Get(i));
            }
        }

        [Fact]
        public void TestInsertFromAllFalse()
        {
            var list = new BitmapList(GlobalMemoryManager.Instance);
            var expected = new List<bool>();

            for (int i = 0; i < 32; i++)
            {
                list.Add(true);
                expected.Add(true);
            }

            var other = new BitmapList(GlobalMemoryManager.Instance);
            var otherList = new List<bool>();
            for (int i = 0; i < 4; i++)
            {
                other.Add(false);
                otherList.Add(false);
            }

            int[] sortedLookup = [0, 1, 2, 3];
            int[] insertPositions = [0, 10, 20, 30];

            ReadOnlySpan<int> sl = sortedLookup; ReadOnlySpan<int> ip = insertPositions; list.InsertFrom(in other, in sl, in ip, -1);

            for (int i = 0; i < sortedLookup.Length; i++)
            {
                expected.Insert(insertPositions[i] + i, otherList[sortedLookup[i]]);
            }

            Assert.Equal(expected.Count, list.Count);
            for (int i = 0; i < expected.Count; i++)
            {
                Assert.Equal(expected[i], list.Get(i));
            }
        }

        [Fact]
        public void TestInsertFromConsecutivePositions()
        {
            var list = new BitmapList(GlobalMemoryManager.Instance);
            var expected = new List<bool>();

            for (int i = 0; i < 20; i++)
            {
                bool val = i % 2 == 0;
                list.Add(val);
                expected.Add(val);
            }

            var other = new BitmapList(GlobalMemoryManager.Instance);
            var otherList = new List<bool> { true, false, true };
            foreach (var v in otherList)
            {
                other.Add(v);
            }

            int[] sortedLookup = [0, 1, 2];
            int[] insertPositions = [5, 6, 7];

            ReadOnlySpan<int> sl = sortedLookup; ReadOnlySpan<int> ip = insertPositions; list.InsertFrom(in other, in sl, in ip, -1);

            for (int i = 0; i < sortedLookup.Length; i++)
            {
                expected.Insert(insertPositions[i] + i, otherList[sortedLookup[i]]);
            }

            Assert.Equal(expected.Count, list.Count);
            for (int i = 0; i < expected.Count; i++)
            {
                Assert.Equal(expected[i], list.Get(i));
            }
        }

        [Fact]
        public void TestInsertFromNonSequentialLookup()
        {
            var list = new BitmapList(GlobalMemoryManager.Instance);
            var expected = new List<bool>();

            for (int i = 0; i < 16; i++)
            {
                bool val = i % 2 == 0;
                list.Add(val);
                expected.Add(val);
            }

            var other = new BitmapList(GlobalMemoryManager.Instance);
            var otherList = new List<bool> { true, false, true, false, true };
            foreach (var v in otherList)
            {
                other.Add(v);
            }

            int[] sortedLookup = [0, 2, 4];
            int[] insertPositions = [3, 8, 12];

            ReadOnlySpan<int> sl = sortedLookup; ReadOnlySpan<int> ip = insertPositions; list.InsertFrom(in other, in sl, in ip, -1);

            for (int i = 0; i < sortedLookup.Length; i++)
            {
                expected.Insert(insertPositions[i] + i, otherList[sortedLookup[i]]);
            }

            Assert.Equal(expected.Count, list.Count);
            for (int i = 0; i < expected.Count; i++)
            {
                Assert.Equal(expected[i], list.Get(i));
            }
        }

        [Fact]
        public void TestInsertFromLargeRandom()
        {
            for (int seed = 0; seed < 20; seed++)
            {
                var list = new BitmapList(GlobalMemoryManager.Instance);
                var expected = new List<bool>();
                var r = new Random(seed);

                for (int i = 0; i < 1000; i++)
                {
                    bool val = r.Next(0, 2) == 1;
                    list.Add(val);
                    expected.Add(val);
                }

                var other = new BitmapList(GlobalMemoryManager.Instance);
                var otherList = new List<bool>();
                for (int i = 0; i < 500; i++)
                {
                    bool val = r.Next(0, 2) == 1;
                    other.Add(val);
                    otherList.Add(val);
                }

                int insertCount = r.Next(1, 100);
                var positions = new SortedSet<int>();
                while (positions.Count < insertCount)
                {
                    positions.Add(r.Next(0, expected.Count + 1));
                }

                int[] insertPositions = positions.ToArray();
                int[] sortedLookup = new int[insertCount];
                for (int i = 0; i < insertCount; i++)
                {
                    sortedLookup[i] = r.Next(0, otherList.Count);
                }

                ReadOnlySpan<int> sl = sortedLookup; ReadOnlySpan<int> ip = insertPositions; list.InsertFrom(in other, in sl, in ip, -1);

                for (int i = 0; i < sortedLookup.Length; i++)
                {
                    expected.Insert(insertPositions[i] + i, otherList[sortedLookup[i]]);
                }

                Assert.Equal(expected.Count, list.Count);
                for (int i = 0; i < expected.Count; i++)
                {
                    Assert.Equal(expected[i], list.Get(i));
                }

                list.Dispose();
            }
        }

        [Fact]
        public void TestInsertFromDuplicateInsertPositions()
        {
            var list = new BitmapList(GlobalMemoryManager.Instance);
            var expected = new List<bool>();

            for (int i = 0; i < 16; i++)
            {
                bool val = i % 2 == 0;
                list.Add(val);
                expected.Add(val);
            }

            var other = new BitmapList(GlobalMemoryManager.Instance);
            var otherList = new List<bool> { true, false, true };
            foreach (var v in otherList)
            {
                other.Add(v);
            }

            int[] sortedLookup = [0, 1, 2];
            int[] insertPositions = [5, 5, 5];

            ReadOnlySpan<int> sl = sortedLookup; ReadOnlySpan<int> ip = insertPositions; list.InsertFrom(in other, in sl, in ip, -1);

            for (int i = 0; i < sortedLookup.Length; i++)
            {
                expected.Insert(insertPositions[i] + i, otherList[sortedLookup[i]]);
            }

            Assert.Equal(expected.Count, list.Count);
            for (int i = 0; i < expected.Count; i++)
            {
                Assert.Equal(expected[i], list.Get(i));
            }
        }

        [Fact]
        public void TestInsertFromMultipleWordBoundaries()
        {
            var list = new BitmapList(GlobalMemoryManager.Instance);
            var expected = new List<bool>();

            for (int i = 0; i < 128; i++)
            {
                bool val = i % 3 == 0;
                list.Add(val);
                expected.Add(val);
            }

            var other = new BitmapList(GlobalMemoryManager.Instance);
            var otherList = new List<bool>();
            for (int i = 0; i < 10; i++)
            {
                bool val = i % 2 != 0;
                other.Add(val);
                otherList.Add(val);
            }

            int[] sortedLookup = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
            int[] insertPositions = [0, 14, 31, 32, 48, 63, 64, 80, 96, 112];

            ReadOnlySpan<int> sl = sortedLookup; ReadOnlySpan<int> ip = insertPositions; list.InsertFrom(in other, in sl, in ip, -1);

            for (int i = 0; i < sortedLookup.Length; i++)
            {
                expected.Insert(insertPositions[i] + i, otherList[sortedLookup[i]]);
            }

            Assert.Equal(expected.Count, list.Count);
            for (int i = 0; i < expected.Count; i++)
            {
                Assert.Equal(expected[i], list.Get(i));
            }
        }

        [Fact]
        public void TestInsertFromSingleElementListSingleInsert()
        {
            var list = new BitmapList(GlobalMemoryManager.Instance);
            list.Add(true);

            var other = new BitmapList(GlobalMemoryManager.Instance);
            other.Add(false);

            int[] sortedLookup = [0];
            int[] insertPositions = [0];

            ReadOnlySpan<int> sl = sortedLookup; ReadOnlySpan<int> ip = insertPositions; list.InsertFrom(in other, in sl, in ip, -1);

            Assert.Equal(2, list.Count);
            Assert.False(list.Get(0));
            Assert.True(list.Get(1));
        }

        [Fact]
        public void TestInsertFromShiftExactly32Bits()
        {
            // 32 inserts all at position 0 forces a shift of exactly one full word
            var list = new BitmapList(GlobalMemoryManager.Instance);
            var expected = new List<bool>();

            for (int i = 0; i < 32; i++)
            {
                bool val = i % 2 == 0;
                list.Add(val);
                expected.Add(val);
            }

            var other = new BitmapList(GlobalMemoryManager.Instance);
            var otherList = new List<bool>();
            for (int i = 0; i < 32; i++)
            {
                bool val = i % 3 == 0;
                other.Add(val);
                otherList.Add(val);
            }

            int[] sortedLookup = new int[32];
            int[] insertPositions = new int[32];
            for (int i = 0; i < 32; i++)
            {
                sortedLookup[i] = i;
                insertPositions[i] = 0;
            }

            ReadOnlySpan<int> sl = sortedLookup; ReadOnlySpan<int> ip = insertPositions; list.InsertFrom(in other, in sl, in ip, -1);

            for (int i = 0; i < sortedLookup.Length; i++)
            {
                expected.Insert(insertPositions[i] + i, otherList[sortedLookup[i]]);
            }

            Assert.Equal(expected.Count, list.Count);
            for (int i = 0; i < expected.Count; i++)
            {
                Assert.Equal(expected[i], list.Get(i));
            }
        }

        [Fact]
        public void TestInsertFromLargeShiftMoreThan32()
        {
            // 50 inserts all at position 0 forces shift > 32 bits (multi-word)
            var list = new BitmapList(GlobalMemoryManager.Instance);
            var expected = new List<bool>();

            for (int i = 0; i < 64; i++)
            {
                bool val = i % 2 == 0;
                list.Add(val);
                expected.Add(val);
            }

            var other = new BitmapList(GlobalMemoryManager.Instance);
            var otherList = new List<bool>();
            for (int i = 0; i < 50; i++)
            {
                bool val = i % 3 != 0;
                other.Add(val);
                otherList.Add(val);
            }

            int[] sortedLookup = new int[50];
            int[] insertPositions = new int[50];
            for (int i = 0; i < 50; i++)
            {
                sortedLookup[i] = i;
                insertPositions[i] = 0;
            }

            ReadOnlySpan<int> sl = sortedLookup; ReadOnlySpan<int> ip = insertPositions; list.InsertFrom(in other, in sl, in ip, -1);

            for (int i = 0; i < sortedLookup.Length; i++)
            {
                expected.Insert(insertPositions[i] + i, otherList[sortedLookup[i]]);
            }

            Assert.Equal(expected.Count, list.Count);
            for (int i = 0; i < expected.Count; i++)
            {
                Assert.Equal(expected[i], list.Get(i));
            }
        }

        [Fact]
        public void TestInsertFromManyInsertsIntoSmallList()
        {
            // Other list much larger than self; inserts outnumber original elements
            var list = new BitmapList(GlobalMemoryManager.Instance);
            var expected = new List<bool>();

            for (int i = 0; i < 5; i++)
            {
                bool val = i % 2 == 0;
                list.Add(val);
                expected.Add(val);
            }

            var other = new BitmapList(GlobalMemoryManager.Instance);
            var otherList = new List<bool>();
            for (int i = 0; i < 100; i++)
            {
                bool val = i % 4 != 0;
                other.Add(val);
                otherList.Add(val);
            }

            int[] sortedLookup = new int[20];
            int[] insertPositions = new int[20];
            for (int i = 0; i < 20; i++)
            {
                sortedLookup[i] = i * 5; // [0, 5, 10, ..., 95]
                insertPositions[i] = Math.Min(i / 4, 4); // clusters at 0,0,0,0,1,1,1,1,...,4
            }

            ReadOnlySpan<int> sl = sortedLookup; ReadOnlySpan<int> ip = insertPositions; list.InsertFrom(in other, in sl, in ip, -1);

            for (int i = 0; i < sortedLookup.Length; i++)
            {
                expected.Insert(insertPositions[i] + i, otherList[sortedLookup[i]]);
            }

            Assert.Equal(expected.Count, list.Count);
            for (int i = 0; i < expected.Count; i++)
            {
                Assert.Equal(expected[i], list.Get(i));
            }
        }

        [Fact]
        public void TestInsertFromLookupNotStartingFromZero()
        {
            var list = new BitmapList(GlobalMemoryManager.Instance);
            var expected = new List<bool>();

            for (int i = 0; i < 20; i++)
            {
                bool val = i % 2 == 0;
                list.Add(val);
                expected.Add(val);
            }

            var other = new BitmapList(GlobalMemoryManager.Instance);
            var otherList = new List<bool> { true, false, true, false, true, false, true, false, true, false };
            foreach (var v in otherList)
            {
                other.Add(v);
            }

            // Lookup skips first 3 elements, picks indices 3, 7, 9
            int[] sortedLookup = [3, 7, 9];
            int[] insertPositions = [2, 10, 18];

            ReadOnlySpan<int> sl = sortedLookup; ReadOnlySpan<int> ip = insertPositions; list.InsertFrom(in other, in sl, in ip, -1);

            for (int i = 0; i < sortedLookup.Length; i++)
            {
                expected.Insert(insertPositions[i] + i, otherList[sortedLookup[i]]);
            }

            Assert.Equal(expected.Count, list.Count);
            for (int i = 0; i < expected.Count; i++)
            {
                Assert.Equal(expected[i], list.Get(i));
            }
        }

        [Fact]
        public void TestInsertFromRepeatedCallsCumulative()
        {
            var list = new BitmapList(GlobalMemoryManager.Instance);
            var expected = new List<bool>();
            var r = new Random(42);

            for (int i = 0; i < 50; i++)
            {
                bool val = r.Next(0, 2) == 1;
                list.Add(val);
                expected.Add(val);
            }

            var other = new BitmapList(GlobalMemoryManager.Instance);
            var otherList = new List<bool>();
            for (int i = 0; i < 200; i++)
            {
                bool val = r.Next(0, 2) == 1;
                other.Add(val);
                otherList.Add(val);
            }

            // Apply InsertFrom multiple times in succession
            for (int round = 0; round < 5; round++)
            {
                int insertCount = r.Next(1, 15);
                var positions = new SortedSet<int>();
                while (positions.Count < insertCount)
                {
                    positions.Add(r.Next(0, expected.Count + 1));
                }

                int[] insertPositions = positions.ToArray();
                int[] sortedLookup = new int[insertCount];
                for (int i = 0; i < insertCount; i++)
                {
                    sortedLookup[i] = r.Next(0, otherList.Count);
                }

                ReadOnlySpan<int> sl = sortedLookup; ReadOnlySpan<int> ip = insertPositions; list.InsertFrom(in other, in sl, in ip, -1);

                for (int i = 0; i < sortedLookup.Length; i++)
                {
                    expected.Insert(insertPositions[i] + i, otherList[sortedLookup[i]]);
                }

                Assert.Equal(expected.Count, list.Count);
                for (int i = 0; i < expected.Count; i++)
                {
                    Assert.Equal(expected[i], list.Get(i));
                }
            }

            list.Dispose();
        }

        [Fact]
        public void TestInsertFromEveryPosition()
        {
            // Insert one element at every position in a 33-element list (crosses word boundary)
            var list = new BitmapList(GlobalMemoryManager.Instance);
            var expected = new List<bool>();

            for (int i = 0; i < 33; i++)
            {
                bool val = i % 2 == 0;
                list.Add(val);
                expected.Add(val);
            }

            var other = new BitmapList(GlobalMemoryManager.Instance);
            var otherList = new List<bool>();
            for (int i = 0; i < 33; i++)
            {
                bool val = i % 3 == 0;
                other.Add(val);
                otherList.Add(val);
            }

            int[] sortedLookup = new int[33];
            int[] insertPositions = new int[33];
            for (int i = 0; i < 33; i++)
            {
                sortedLookup[i] = i;
                insertPositions[i] = i;
            }

            ReadOnlySpan<int> sl = sortedLookup; ReadOnlySpan<int> ip = insertPositions; list.InsertFrom(in other, in sl, in ip, -1);

            for (int i = 0; i < sortedLookup.Length; i++)
            {
                expected.Insert(insertPositions[i] + i, otherList[sortedLookup[i]]);
            }

            Assert.Equal(expected.Count, list.Count);
            for (int i = 0; i < expected.Count; i++)
            {
                Assert.Equal(expected[i], list.Get(i));
            }
        }

        [Fact]
        public void TestInsertFromSingleInsertAtWordBoundary31()
        {
            // Insert at bit 31 (last bit of first word)
            var list = new BitmapList(GlobalMemoryManager.Instance);
            var expected = new List<bool>();

            for (int i = 0; i < 64; i++)
            {
                bool val = i % 2 == 0;
                list.Add(val);
                expected.Add(val);
            }

            var other = new BitmapList(GlobalMemoryManager.Instance);
            other.Add(true);

            int[] sortedLookup = [0];
            int[] insertPositions = [31];

            ReadOnlySpan<int> sl = sortedLookup; ReadOnlySpan<int> ip = insertPositions; list.InsertFrom(in other, in sl, in ip, -1);

            expected.Insert(31, true);

            Assert.Equal(expected.Count, list.Count);
            for (int i = 0; i < expected.Count; i++)
            {
                Assert.Equal(expected[i], list.Get(i));
            }
        }

        [Fact]
        public void TestInsertFromSingleInsertAtWordBoundary32()
        {
            // Insert at bit 32 (first bit of second word)
            var list = new BitmapList(GlobalMemoryManager.Instance);
            var expected = new List<bool>();

            for (int i = 0; i < 64; i++)
            {
                bool val = i % 2 == 0;
                list.Add(val);
                expected.Add(val);
            }

            var other = new BitmapList(GlobalMemoryManager.Instance);
            other.Add(false);

            int[] sortedLookup = [0];
            int[] insertPositions = [32];

            ReadOnlySpan<int> sl = sortedLookup; ReadOnlySpan<int> ip = insertPositions; list.InsertFrom(in other, in sl, in ip, -1);

            expected.Insert(32, false);

            Assert.Equal(expected.Count, list.Count);
            for (int i = 0; i < expected.Count; i++)
            {
                Assert.Equal(expected[i], list.Get(i));
            }
        }

        [Fact]
        public void TestInsertFromAlternatingBitsLargeSpread()
        {
            // Large list with alternating bits, inserts spread across many words
            var list = new BitmapList(GlobalMemoryManager.Instance);
            var expected = new List<bool>();

            for (int i = 0; i < 256; i++)
            {
                bool val = i % 2 == 0;
                list.Add(val);
                expected.Add(val);
            }

            var other = new BitmapList(GlobalMemoryManager.Instance);
            var otherList = new List<bool>();
            for (int i = 0; i < 16; i++)
            {
                bool val = i % 2 != 0;
                other.Add(val);
                otherList.Add(val);
            }

            // Inserts at positions spread across 8 words: 0, 17, 34, 51, 68, 85, 102, ...
            int[] sortedLookup = new int[16];
            int[] insertPositions = new int[16];
            for (int i = 0; i < 16; i++)
            {
                sortedLookup[i] = i;
                insertPositions[i] = i * 16;
            }

            ReadOnlySpan<int> sl = sortedLookup; ReadOnlySpan<int> ip = insertPositions; list.InsertFrom(in other, in sl, in ip, -1);

            for (int i = 0; i < sortedLookup.Length; i++)
            {
                expected.Insert(insertPositions[i] + i, otherList[sortedLookup[i]]);
            }

            Assert.Equal(expected.Count, list.Count);
            for (int i = 0; i < expected.Count; i++)
            {
                Assert.Equal(expected[i], list.Get(i));
            }
        }

        [Fact]
        public void TestDeleteBatchEmptyTargets()
        {
            var list = new BitmapList(GlobalMemoryManager.Instance);
            var expected = new List<bool>();

            for (int i = 0; i < 20; i++)
            {
                bool val = i % 2 == 0;
                list.Add(val);
                expected.Add(val);
            }

            ReadOnlySpan<int> targets = stackalloc int[0];
            list.DeleteBatch(targets);

            Assert.Equal(expected.Count, list.Count);
            for (int i = 0; i < expected.Count; i++)
            {
                Assert.Equal(expected[i], list.Get(i));
            }
        }

        [Fact]
        public void TestDeleteBatchSingleElementAtBeginning()
        {
            var list = new BitmapList(GlobalMemoryManager.Instance);
            var expected = new List<bool>();

            for (int i = 0; i < 20; i++)
            {
                bool val = i % 2 == 0;
                list.Add(val);
                expected.Add(val);
            }

            ReadOnlySpan<int> targets = stackalloc int[] { 0 };
            list.DeleteBatch(targets);
            expected.RemoveAt(0);

            Assert.Equal(expected.Count, list.Count);
            for (int i = 0; i < expected.Count; i++)
            {
                Assert.Equal(expected[i], list.Get(i));
            }
        }

        [Fact]
        public void TestDeleteBatchSingleElementInMiddle()
        {
            var list = new BitmapList(GlobalMemoryManager.Instance);
            var expected = new List<bool>();

            for (int i = 0; i < 20; i++)
            {
                bool val = i % 2 == 0;
                list.Add(val);
                expected.Add(val);
            }

            ReadOnlySpan<int> targets = stackalloc int[] { 10 };
            list.DeleteBatch(targets);
            expected.RemoveAt(10);

            Assert.Equal(expected.Count, list.Count);
            for (int i = 0; i < expected.Count; i++)
            {
                Assert.Equal(expected[i], list.Get(i));
            }
        }

        [Fact]
        public void TestDeleteBatchSingleElementAtEnd()
        {
            var list = new BitmapList(GlobalMemoryManager.Instance);
            var expected = new List<bool>();

            for (int i = 0; i < 20; i++)
            {
                bool val = i % 2 == 0;
                list.Add(val);
                expected.Add(val);
            }

            ReadOnlySpan<int> targets = stackalloc int[] { 19 };
            list.DeleteBatch(targets);
            expected.RemoveAt(19);

            Assert.Equal(expected.Count, list.Count);
            for (int i = 0; i < expected.Count; i++)
            {
                Assert.Equal(expected[i], list.Get(i));
            }
        }

        [Fact]
        public void TestDeleteBatchMultiplePositions()
        {
            var list = new BitmapList(GlobalMemoryManager.Instance);
            var expected = new List<bool>();

            for (int i = 0; i < 20; i++)
            {
                bool val = i % 2 == 0;
                list.Add(val);
                expected.Add(val);
            }

            // Delete first, middle, and last
            int[] targets = [0, 10, 19];
            list.DeleteBatch(targets.AsSpan());

            // Remove from expected in reverse to keep indices stable
            expected.RemoveAt(19);
            expected.RemoveAt(10);
            expected.RemoveAt(0);

            Assert.Equal(expected.Count, list.Count);
            for (int i = 0; i < expected.Count; i++)
            {
                Assert.Equal(expected[i], list.Get(i));
            }
        }

        [Fact]
        public void TestDeleteBatchAtWordBoundary31()
        {
            // Delete at bit 31 (last bit of first word)
            var list = new BitmapList(GlobalMemoryManager.Instance);
            var expected = new List<bool>();

            for (int i = 0; i < 64; i++)
            {
                bool val = i % 2 == 0;
                list.Add(val);
                expected.Add(val);
            }

            ReadOnlySpan<int> targets = stackalloc int[] { 31 };
            list.DeleteBatch(targets);
            expected.RemoveAt(31);

            Assert.Equal(expected.Count, list.Count);
            for (int i = 0; i < expected.Count; i++)
            {
                Assert.Equal(expected[i], list.Get(i));
            }
        }

        [Fact]
        public void TestDeleteBatchAtWordBoundary32()
        {
            // Delete at bit 32 (first bit of second word)
            var list = new BitmapList(GlobalMemoryManager.Instance);
            var expected = new List<bool>();

            for (int i = 0; i < 64; i++)
            {
                bool val = i % 2 == 0;
                list.Add(val);
                expected.Add(val);
            }

            ReadOnlySpan<int> targets = stackalloc int[] { 32 };
            list.DeleteBatch(targets);
            expected.RemoveAt(32);

            Assert.Equal(expected.Count, list.Count);
            for (int i = 0; i < expected.Count; i++)
            {
                Assert.Equal(expected[i], list.Get(i));
            }
        }

        [Fact]
        public void TestDeleteBatchCrossingWordBoundary()
        {
            // Delete bits on both sides of a word boundary
            var list = new BitmapList(GlobalMemoryManager.Instance);
            var expected = new List<bool>();

            for (int i = 0; i < 64; i++)
            {
                bool val = i % 2 == 0;
                list.Add(val);
                expected.Add(val);
            }

            int[] targets = [30, 31, 32, 33];
            list.DeleteBatch(targets.AsSpan());

            expected.RemoveAt(33);
            expected.RemoveAt(32);
            expected.RemoveAt(31);
            expected.RemoveAt(30);

            Assert.Equal(expected.Count, list.Count);
            for (int i = 0; i < expected.Count; i++)
            {
                Assert.Equal(expected[i], list.Get(i));
            }
        }

        [Fact]
        public void TestDeleteBatchAllTrue()
        {
            var list = new BitmapList(GlobalMemoryManager.Instance);
            var expected = new List<bool>();

            for (int i = 0; i < 40; i++)
            {
                list.Add(true);
                expected.Add(true);
            }

            int[] targets = [5, 15, 25, 35];
            list.DeleteBatch(targets.AsSpan());

            expected.RemoveAt(35);
            expected.RemoveAt(25);
            expected.RemoveAt(15);
            expected.RemoveAt(5);

            Assert.Equal(expected.Count, list.Count);
            for (int i = 0; i < expected.Count; i++)
            {
                Assert.Equal(expected[i], list.Get(i));
            }
        }

        [Fact]
        public void TestDeleteBatchAllFalse()
        {
            var list = new BitmapList(GlobalMemoryManager.Instance);
            var expected = new List<bool>();

            for (int i = 0; i < 40; i++)
            {
                list.Add(false);
                expected.Add(false);
            }

            int[] targets = [5, 15, 25, 35];
            list.DeleteBatch(targets.AsSpan());

            expected.RemoveAt(35);
            expected.RemoveAt(25);
            expected.RemoveAt(15);
            expected.RemoveAt(5);

            Assert.Equal(expected.Count, list.Count);
            for (int i = 0; i < expected.Count; i++)
            {
                Assert.Equal(expected[i], list.Get(i));
            }
        }

        [Fact]
        public void TestDeleteBatchConsecutive()
        {
            var list = new BitmapList(GlobalMemoryManager.Instance);
            var expected = new List<bool>();

            for (int i = 0; i < 40; i++)
            {
                bool val = i % 2 == 0;
                list.Add(val);
                expected.Add(val);
            }

            // Delete consecutive elements in the middle
            int[] targets = [10, 11, 12, 13, 14, 15, 16, 17, 18, 19];
            list.DeleteBatch(targets.AsSpan());

            for (int i = 9; i >= 0; i--)
            {
                expected.RemoveAt(10 + i);
            }

            Assert.Equal(expected.Count, list.Count);
            for (int i = 0; i < expected.Count; i++)
            {
                Assert.Equal(expected[i], list.Get(i));
            }
        }

        [Fact]
        public void TestDeleteBatchInterleaved()
        {
            // Delete every other element
            var list = new BitmapList(GlobalMemoryManager.Instance);
            var expected = new List<bool>();

            for (int i = 0; i < 40; i++)
            {
                bool val = i % 2 == 0;
                list.Add(val);
                expected.Add(val);
            }

            int[] targets = new int[20];
            for (int i = 0; i < 20; i++)
            {
                targets[i] = i * 2;
            }

            list.DeleteBatch(targets.AsSpan());

            for (int i = targets.Length - 1; i >= 0; i--)
            {
                expected.RemoveAt(targets[i]);
            }

            Assert.Equal(expected.Count, list.Count);
            for (int i = 0; i < expected.Count; i++)
            {
                Assert.Equal(expected[i], list.Get(i));
            }
        }

        [Fact]
        public void TestDeleteBatchAll()
        {
            var list = new BitmapList(GlobalMemoryManager.Instance);

            for (int i = 0; i < 20; i++)
            {
                list.Add(i % 2 == 0);
            }

            int[] targets = new int[20];
            for (int i = 0; i < 20; i++)
            {
                targets[i] = i;
            }

            list.DeleteBatch(targets.AsSpan());

            Assert.Empty(list);
        }

        [Fact]
        public void TestDeleteBatchLargeRandom()
        {
            for (int seed = 0; seed < 20; seed++)
            {
                var list = new BitmapList(GlobalMemoryManager.Instance);
                var expected = new List<bool>();
                var r = new Random(seed);

                for (int i = 0; i < 100_000; i++)
                {
                    bool val = r.Next(0, 2) == 1;
                    list.Add(val);
                    expected.Add(val);
                }

                // Generate sorted unique delete targets
                var positions = new SortedSet<int>();
                while (positions.Count < 10_000)
                {
                    positions.Add(r.Next(0, expected.Count));
                }

                int[] targets = positions.ToArray();
                list.DeleteBatch(targets.AsSpan());

                for (int i = targets.Length - 1; i >= 0; i--)
                {
                    expected.RemoveAt(targets[i]);
                }

                Assert.Equal(expected.Count, list.Count);
                for (int i = 0; i < expected.Count; i++)
                {
                    Assert.Equal(expected[i], list.Get(i));
                }

                list.Dispose();
            }
        }

        [Fact]
        public void TestDeleteBatchRepeatedCallsCumulative()
        {
            var list = new BitmapList(GlobalMemoryManager.Instance);
            var expected = new List<bool>();
            var r = new Random(42);

            for (int i = 0; i < 500; i++)
            {
                bool val = r.Next(0, 2) == 1;
                list.Add(val);
                expected.Add(val);
            }

            // Apply DeleteBatch multiple times in succession
            for (int round = 0; round < 5; round++)
            {
                int deleteCount = r.Next(1, 15);
                var positions = new SortedSet<int>();
                while (positions.Count < deleteCount && positions.Count < expected.Count)
                {
                    positions.Add(r.Next(0, expected.Count));
                }

                int[] targets = positions.ToArray();
                list.DeleteBatch(targets.AsSpan());

                for (int i = targets.Length - 1; i >= 0; i--)
                {
                    expected.RemoveAt(targets[i]);
                }

                Assert.Equal(expected.Count, list.Count);
                for (int i = 0; i < expected.Count; i++)
                {
                    Assert.Equal(expected[i], list.Get(i));
                }
            }

            list.Dispose();
        }

        [Fact]
        public void TestDeleteBatchAlternatingBitsLargeSpread()
        {
            // Large list with alternating bits, deletes spread across many words
            var list = new BitmapList(GlobalMemoryManager.Instance);
            var expected = new List<bool>();

            for (int i = 0; i < 256; i++)
            {
                bool val = i % 2 == 0;
                list.Add(val);
                expected.Add(val);
            }

            // Deletes at positions spread across 8 words: 0, 16, 32, 48, ...
            int[] targets = new int[16];
            for (int i = 0; i < 16; i++)
            {
                targets[i] = i * 16;
            }

            list.DeleteBatch(targets.AsSpan());

            for (int i = targets.Length - 1; i >= 0; i--)
            {
                expected.RemoveAt(targets[i]);
            }

            Assert.Equal(expected.Count, list.Count);
            for (int i = 0; i < expected.Count; i++)
            {
                Assert.Equal(expected[i], list.Get(i));
            }
        }

        [Fact]
        public void TestDeleteBatchThenAdd()
        {
            // Verify list is still usable after batch delete
            var list = new BitmapList(GlobalMemoryManager.Instance);
            var expected = new List<bool>();

            for (int i = 0; i < 64; i++)
            {
                bool val = i % 2 == 0;
                list.Add(val);
                expected.Add(val);
            }

            int[] targets = [10, 20, 30, 40, 50];
            list.DeleteBatch(targets.AsSpan());

            for (int i = targets.Length - 1; i >= 0; i--)
            {
                expected.RemoveAt(targets[i]);
            }

            // Add more elements after the delete
            list.Add(true);
            list.Add(false);
            expected.Add(true);
            expected.Add(false);

            Assert.Equal(expected.Count, list.Count);
            for (int i = 0; i < expected.Count; i++)
            {
                Assert.Equal(expected[i], list.Get(i));
            }
        }

        [Fact]
         public void TestInsertFromWithNullIndex()
         {
             var list = new BitmapList(GlobalMemoryManager.Instance);
             
             // Base list
             for (int i = 0; i < 5; i++)
             {
                 list.Add(true);
             }

             var other = new BitmapList(GlobalMemoryManager.Instance);
             other.Add(true);
             other.Add(true);

             // Test inserting null values (represented as false in BitmapList)
             int[] sortedLookup = [0, 100, 1]; // 100 is the null index
             int[] insertPositions = [1, 3, 5];

             ReadOnlySpan<int> sl = sortedLookup; ReadOnlySpan<int> ip = insertPositions; list.InsertFrom(in other, in sl, in ip, 100);

             Assert.Equal(8, list.Count);
             Assert.True(list.Get(0));
             Assert.True(list.Get(1)); // from other (0)
             Assert.True(list.Get(2));
             Assert.True(list.Get(3)); 
             Assert.False(list.Get(4)); // null index (100) -> false
             Assert.True(list.Get(5));
             Assert.True(list.Get(6));
             Assert.True(list.Get(7)); // from other (1)
         }
    }
}