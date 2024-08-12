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

using FlowtideDotNet.Core.ColumnStore.Memory;
using FlowtideDotNet.Core.ColumnStore.Utils;
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
            var list = new BitmapList(new BatchMemoryManager(1));
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
            var list = new BitmapList(new BatchMemoryManager(1));
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
            var list = new BitmapList(new BatchMemoryManager(1));
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
            var list = new BitmapList(new BatchMemoryManager(1));
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
            var list = new BitmapList(new BatchMemoryManager(1));
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
            var list = new BitmapList(new BatchMemoryManager(1));
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
            var list = new BitmapList(new BatchMemoryManager(1));
            for (int i = 0; i < 64; i++)
            {
                Assert.False(list.Get(i));
            }
        }

        [Fact]
        public void TestSetAndUnsetBits()
        {
            var list = new BitmapList(new BatchMemoryManager(1));
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
            var list = new BitmapList(new BatchMemoryManager(1));
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
            var list = new BitmapList(new BatchMemoryManager(1));
            list.InsertAt(0, true);
            Assert.True(list.Get(0));

            list.RemoveAt(0);
            Assert.False(list.Get(0));
        }

        [Fact]
        public void TestInsertRemoveSingleBitList()
        {
            var list = new BitmapList(new BatchMemoryManager(1));
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
            var list = new BitmapList(new BatchMemoryManager(1));
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
            var list = new BitmapList(new BatchMemoryManager(1));
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
            var list = new BitmapList(new BatchMemoryManager(1));

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
    }
}
