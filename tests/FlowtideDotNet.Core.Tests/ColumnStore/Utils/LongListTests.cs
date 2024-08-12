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
    public class LongListTests
    {
        [Fact]
        public void TestAdd()
        {
            var allocator = new BatchMemoryManager(1);
            using var longList = new NativeLongList(allocator);
            longList.Add(1);
            longList.Add(2);
            longList.Add(3);

            Assert.Equal(1, longList[0]);
            Assert.Equal(2, longList[1]);
            Assert.Equal(3, longList[2]);
        }

        [Fact]
        public void TestInsertMiddle()
        {
            var allocator = new BatchMemoryManager(1);
            using var longList = new NativeLongList(allocator);
            longList.Add(1);
            longList.Add(3);
            longList.InsertAt(1, 2);

            Assert.Equal(1, longList[0]);
            Assert.Equal(2, longList[1]);
            Assert.Equal(3, longList[2]);
        }

        [Fact]
        public void TestInsertFirst()
        {
            var allocator = new BatchMemoryManager(1);
            using var longList = new NativeLongList(allocator);
            longList.Add(2);
            longList.Add(3);
            longList.InsertAt(0, 1);

            Assert.Equal(1, longList[0]);
            Assert.Equal(2, longList[1]);
            Assert.Equal(3, longList[2]);
        }

        [Fact]
        public void TestInsertLast()
        {
            var allocator = new BatchMemoryManager(1);
            using var longList = new NativeLongList(allocator);
            longList.Add(1);
            longList.Add(2);
            longList.InsertAt(2, 3);

            Assert.Equal(1, longList[0]);
            Assert.Equal(2, longList[1]);
            Assert.Equal(3, longList[2]);
        }

        [Fact]
        public void TestRemoveMiddle()
        {
            var allocator = new BatchMemoryManager(1);
            using var longList = new NativeLongList(allocator);
            longList.Add(1);
            longList.Add(2);
            longList.Add(3);
            longList.RemoveAt(1);

            Assert.Equal(1, longList[0]);
            Assert.Equal(3, longList[1]);
            Assert.Equal(2, longList.Count);
        }

        [Fact]
        public void TestRemoveFirst()
        {
            var allocator = new BatchMemoryManager(1);
            using var longList = new NativeLongList(allocator);
            longList.Add(1);
            longList.Add(2);
            longList.Add(3);
            longList.RemoveAt(0);

            Assert.Equal(2, longList[0]);
            Assert.Equal(3, longList[1]);
            Assert.Equal(2, longList.Count);
        }

        [Fact]
        public void TestRemoveLast()
        {
            var allocator = new BatchMemoryManager(1);
            using var longList = new NativeLongList(allocator);
            longList.Add(1);
            longList.Add(2);
            longList.Add(3);
            longList.RemoveAt(2);

            Assert.Equal(1, longList[0]);
            Assert.Equal(2, longList[1]);
            Assert.Equal(2, longList.Count);
        }

        [Fact]
        public void TestRemoveRange()
        {
            var allocator = new BatchMemoryManager(1);
            using var longList = new NativeLongList(allocator);
            longList.Add(1);
            longList.Add(2);
            longList.Add(3);
            longList.Add(4);
            longList.Add(5);
            longList.RemoveRange(1, 2);

            Assert.Equal(1, longList[0]);
            Assert.Equal(4, longList[1]);
            Assert.Equal(5, longList[2]);
            Assert.Equal(3, longList.Count);
        }

        [Fact]
        public void TestAddOneMillion()
        {
            var allocator = new BatchMemoryManager(1);
            using var longList = new NativeLongList(allocator);
            
            for (int i = 0; i < 1_000_000; i++)
            {
                longList.Add(i);
            }

            Assert.Equal(1_000_000, longList.Count);
        }
    }
}
