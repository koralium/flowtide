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
    public class BinaryListTests
    {
        [Fact]
        public void TestAdd()
        {
            var e1 = new byte[] { 1, 2, 3 };
            var e2 = new byte[] { 1, 2, 3, 4 };
            BinaryList binaryList = new BinaryList(new NativeMemoryAllocator());
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
            BinaryList binaryList = new BinaryList(new NativeMemoryAllocator());
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
            BinaryList binaryList = new BinaryList(new NativeMemoryAllocator());
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
            BinaryList binaryList = new BinaryList(new NativeMemoryAllocator());
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
            BinaryList binaryList = new BinaryList(new NativeMemoryAllocator());
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
            BinaryList binaryList = new BinaryList(new NativeMemoryAllocator());
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
            BinaryList binaryList = new BinaryList(new NativeMemoryAllocator());
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
            BinaryList binaryList = new BinaryList(new NativeMemoryAllocator());
            binaryList.Add(e1);
            binaryList.Add(e2);

            var index1Mem = binaryList.GetMemory(0);
            var index2Mem = binaryList.GetMemory(1);
            Assert.True(binaryList.Get(0).SequenceEqual(e1));
            Assert.True(binaryList.Get(1).SequenceEqual(e2));
        }
    }
}
