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
                Assert.Equal(i + (i +1) % 2, actual);
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
    }
}
