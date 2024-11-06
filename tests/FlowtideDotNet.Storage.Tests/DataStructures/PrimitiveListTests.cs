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
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Storage.Tests.DataStructures
{
    public class PrimitiveListTests
    {
        [Fact]
        public void TestInsertRangeFrom()
        {
            var allocator = GlobalMemoryManager.Instance;
            using var longList = new PrimitiveList<int>(allocator);
            using var otherList = new PrimitiveList<int>(allocator);

            for (int i = 0; i < 1_000; i++)
            {
                longList.Add(i);
            }

            for (int i = 0; i < 1_000; i++)
            {
                otherList.Add(i + 1_000);
            }

            longList.InsertRangeFrom(500, otherList, 100, 500);

            Assert.Equal(1_500, longList.Count);

            for (int i = 0; i < 500; i++)
            {
                Assert.Equal(i, longList[i]);
            }

            for (int i = 0; i < 500; i++)
            {
                Assert.Equal(i + 1100, longList[i + 500]);
            }

            for (int i = 500; i < 1_000; i++)
            {
                Assert.Equal(i, longList[i + 500]);
            }
        }
    }
}
