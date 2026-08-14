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
    public class IntListTests
    {

        [Fact]
        public void InsertIncrementalRangeConditionalAdditionOnExistingAvx()
        {
            IntList intList = default;

            intList.Add(1, GlobalMemoryManager.Instance);
            intList.Add(2, GlobalMemoryManager.Instance);

            var conditional = new sbyte[2];
            conditional[0] = 1;
            conditional[1] = 1;

            intList.InsertIncrementalRangeConditionalAdditionOnExisting(2, 3, 9, conditional, 1, 0, GlobalMemoryManager.Instance);

            for (int i = 0; i < 11; i++)
            {
                Assert.Equal(i + 1, intList.Get(i));
            }

            intList.Dispose(GlobalMemoryManager.Instance);
        }

        [Fact]
        public void DisposeTwiceIsSafe()
        {
            IntList intList = default;
            intList.Add(1, GlobalMemoryManager.Instance);

            intList.Dispose(GlobalMemoryManager.Instance);
            intList.Dispose(GlobalMemoryManager.Instance);

            Assert.Equal(0, intList.Count);
        }

        [Fact]
        public void DisposeOfDefaultIsSafe()
        {
            IntList intList = default;
            intList.Dispose(GlobalMemoryManager.Instance);
        }
    }
}
