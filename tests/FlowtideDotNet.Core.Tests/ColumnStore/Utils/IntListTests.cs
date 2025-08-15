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
            IntList intList = new IntList(GlobalMemoryManager.Instance);

            intList.Add(1);
            intList.Add(2);

            var conditional = new sbyte[2];
            conditional[0] = 1;
            conditional[1] = 1;

            intList.InsertIncrementalRangeConditionalAdditionOnExisting(2, 3, 9, conditional, 1, 0);

            for (int i = 0; i < 11; i++)
            {
                Assert.Equal(i + 1, intList.Get(i));
            }
        }
    }
}
