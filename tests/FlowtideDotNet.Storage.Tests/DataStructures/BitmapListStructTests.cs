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
    public class BitmapListStructTests
    {
        [Fact]
        public void DefaultIsNull()
        {
            BitmapList list = default;
            Assert.True(list.IsNull);
            Assert.Equal(0, list.Count);
        }

        [Fact]
        public void NoneIsNull()
        {
            Assert.True(BitmapList.None.IsNull);
        }

        [Fact]
        public void AllocatedListIsNotNull()
        {
            BitmapList list = default;
            list.Add(true, GlobalMemoryManager.Instance);

            Assert.False(list.IsNull);
            Assert.True(list.Get(0));

            list.Dispose(GlobalMemoryManager.Instance);
        }

        [Fact]
        public void DisposedListIsNullAgain()
        {
            BitmapList list = default;
            list.Add(true, GlobalMemoryManager.Instance);
            list.Dispose(GlobalMemoryManager.Instance);

            Assert.True(list.IsNull);
            Assert.Equal(0, list.Count);
        }

        [Fact]
        public void DisposeTwiceIsSafe()
        {
            BitmapList list = default;
            list.Add(true, GlobalMemoryManager.Instance);

            list.Dispose(GlobalMemoryManager.Instance);
            list.Dispose(GlobalMemoryManager.Instance);
        }

        [Fact]
        public void DisposeOfDefaultIsSafe()
        {
            BitmapList list = default;
            list.Dispose(GlobalMemoryManager.Instance);
        }
    }
}
