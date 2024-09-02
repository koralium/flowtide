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

using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.DataColumns;
using FlowtideDotNet.Core.ColumnStore.Memory;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.Tests.ColumnStore
{
    public class UnionColumnTests
    {
        [Fact]
        public void TestGetTypeAt()
        {
            UnionColumn unionColumn = new UnionColumn(GlobalMemoryManager.Instance);

            unionColumn.Add(new Int64Value(1));
            unionColumn.Add(new StringValue("hello"));

            Assert.Equal(ArrowTypeId.Int64, unionColumn.GetTypeAt(0, default));
            Assert.Equal(ArrowTypeId.String, unionColumn.GetTypeAt(1, default));
        }
    }
}
