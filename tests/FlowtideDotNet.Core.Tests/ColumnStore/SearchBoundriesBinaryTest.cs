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

using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.Tests.ColumnStore
{
    public  class SearchBoundriesBinaryTest
    {
        [Fact]
        public void Test()
        {
            List<long> sortedList = new List<long> { 1, 2, 2, 2, 2, 4, 5, 6 };

            for (int i = 7; i < 10000; i++)
            {
                sortedList.Add(i);
            }
            var (min, max) = IntListSearch.SearchBoundries(sortedList, 2, 0, sortedList.Count);
        }
    }
}
