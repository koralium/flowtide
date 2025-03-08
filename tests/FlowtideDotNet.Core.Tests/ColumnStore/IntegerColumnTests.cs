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
using FlowtideDotNet.Storage.Memory;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.Tests.ColumnStore
{
    public class IntegerColumnTests
    {
        [Fact]
        public void TestInt8ToInt16()
        {
            IntegerColumn integerColumn = new IntegerColumn(GlobalMemoryManager.Instance);

            integerColumn.Add(new Int64Value(100));
            integerColumn.Add(new Int64Value(200));

            Assert.Equal(100, integerColumn.GetValueAt(0, default).AsLong);
            Assert.Equal(200, integerColumn.GetValueAt(1, default).AsLong);
        }

        [Fact]
        public void TestAddInt16ThenInt32()
        {
            IntegerColumn integerColumn = new IntegerColumn(GlobalMemoryManager.Instance);

            integerColumn.Add(new Int64Value(300));
            integerColumn.Add(new Int64Value(100_000));

            Assert.Equal(300, integerColumn.GetValueAt(0, default).AsLong);
            Assert.Equal(100_000, integerColumn.GetValueAt(1, default).AsLong);
        }

        [Fact]
        public void TestAddInt32ThenInt64()
        {
            IntegerColumn integerColumn = new IntegerColumn(GlobalMemoryManager.Instance);

            integerColumn.Add(new Int64Value(100_000));
            integerColumn.Add(new Int64Value(100_000_000));

            Assert.Equal(100_000, integerColumn.GetValueAt(0, default).AsLong);
            Assert.Equal(100_000_000, integerColumn.GetValueAt(1, default).AsLong);
        }
    }
}
