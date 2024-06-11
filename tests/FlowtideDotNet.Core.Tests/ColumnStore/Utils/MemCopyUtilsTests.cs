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
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.Tests.ColumnStore.Utils
{
    public class MemCopyUtilsTests
    {
        [Fact]
        public void TestCopyWithAddEntireArray()
        {
            int[] arr1 = new int[64];
            int[] arr2 = new int[64];
            for (int i = 0; i < 64; i++)
            {
                arr1[i] = i;
            }

            AvxUtils.MemCpyWithAdd(arr1, arr2, 5);
        }
    }
}
