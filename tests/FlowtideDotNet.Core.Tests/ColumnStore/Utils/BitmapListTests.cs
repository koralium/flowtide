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
using System.Text;
using System.Threading.Tasks;

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
            //expected.InsertRange(1, otherList);

            for (int i = 0; i < expected.Count; i++)
            {
                Assert.Equal(expected[i], list.Get(i));
            }

            //for (int seed = 0; seed < 20; seed++)
            //{



            //    for (int i = 0; i < 10_000; i++)
            //    {
            //        var insertLocation = r.Next(0, expected.Count);
            //        var index = r.Next(0, otherList.Count);
            //        var toAdd = r.Next(0, otherList.Count - index);
            //        list.InsertRangeFrom(insertLocation, other, index, toAdd);

            //        expected.InsertRange(insertLocation, otherList.GetRange(index, toAdd));

            //        for (int k = 0; k < expected.Count; k++)
            //        {
            //            Assert.Equal(expected[k], list.Get(k));
            //        }
            //    }
            //    list.Dispose();
            //}
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
                for (int i = 0; i < 10_000; i++)
                {
                    var insertLocation = r.Next(0, expected.Count);
                    var index = r.Next(0, otherList.Count);
                    var toAdd = r.Next(0, otherList.Count - index);
                    if (i == 2)
                    {

                    }
                    list.InsertRangeFrom(insertLocation, other, index, toAdd);

                    expected.InsertRange(insertLocation, otherList.GetRange(index, toAdd));

                    for (int k = 0; k < expected.Count; k++)
                    {
                        Assert.Equal(expected[k], list.Get(k));
                    }
                }
                list.Dispose();
            }
        }
    }
}
