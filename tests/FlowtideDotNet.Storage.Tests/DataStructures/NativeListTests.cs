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
    public class NativeListTests
    {
        [Fact]
        public void AddAndGet()
        {
            NativeList<long> list = default;
            for (int i = 0; i < 100; i++)
            {
                list.Add(i, GlobalMemoryManager.Instance);
            }

            Assert.Equal(100, list.Count);
            for (int i = 0; i < 100; i++)
            {
                Assert.Equal(i, list.Get(i));
            }

            list.Dispose(GlobalMemoryManager.Instance);
        }

        [Fact]
        public void AdoptedMemoryRoundTrips()
        {
            NativeList<int> source = default;
            source.Add(1, GlobalMemoryManager.Instance);
            source.Add(2, GlobalMemoryManager.Instance);
            source.Add(3, GlobalMemoryManager.Instance);

            var copy = source.Copy(GlobalMemoryManager.Instance);
            source.Dispose(GlobalMemoryManager.Instance);

            Assert.Equal(3, copy.Count);
            Assert.Equal(1, copy.Get(0));
            Assert.Equal(2, copy.Get(1));
            Assert.Equal(3, copy.Get(2));

            copy.Dispose(GlobalMemoryManager.Instance);
        }

        [Fact]
        public void DisposeTwiceIsSafe()
        {
            NativeList<int> list = default;
            list.Add(1, GlobalMemoryManager.Instance);

            list.Dispose(GlobalMemoryManager.Instance);
            list.Dispose(GlobalMemoryManager.Instance);

            Assert.Equal(0, list.Count);
        }

        [Fact]
        public void DisposeOfDefaultIsSafe()
        {
            NativeList<int> list = default;
            list.Dispose(GlobalMemoryManager.Instance);
        }
    }
}
