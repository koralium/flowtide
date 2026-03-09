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
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Storage.Memory;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.Tests.ColumnStore
{
    public class GuidColumnTests
    {
        [Fact]
        public void AddSingle()
        {
            var expected = Guid.NewGuid();
            Column column = new Column(GlobalMemoryManager.Instance)
            {
                new GuidValue(expected)
            };

            var actual = column.GetValueAt(0, default);
            Assert.Equal(expected, actual.AsGuid);
        }

        [Fact]
        public void RemoveSingle()
        {
            var expected = Guid.NewGuid();
            Column column = new Column(GlobalMemoryManager.Instance)
            {
                new GuidValue(expected)
            };
            column.RemoveAt(0);

            Assert.Empty(column);
        }
    }
}
