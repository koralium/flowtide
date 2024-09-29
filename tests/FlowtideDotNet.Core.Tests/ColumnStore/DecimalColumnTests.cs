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
    public class DecimalColumnTests
    {
        [Fact]
        public void RemoveRangeNonNull()
        {
            Column column = new Column(GlobalMemoryManager.Instance);

            for (int i = 0; i < 1000; i++)
            {
                column.Add(new DecimalValue(i));
            }

            column.RemoveRange(100, 100);

            Assert.Equal(900, column.Count);

            for (int i = 0; i < 100; i++)
            {
                Assert.Equal(i, column.GetValueAt(i, default).AsDecimal);
            }

            for (int i = 100; i < 900; i++)
            {
                Assert.Equal(i + 100, column.GetValueAt(i, default).AsDecimal);
            }
        }

        [Fact]
        public void RemoveRangeNull()
        {
            Column column = new Column(GlobalMemoryManager.Instance);

            List<decimal?> expected = new List<decimal?>();
            Random r = new Random(123);
            for (int i = 0; i < 1000; i++)
            {
                if (r.Next(0, 2) == 0)
                {
                    column.Add(new DecimalValue(i));
                    expected.Add(i);
                }
                else
                {
                    column.Add(NullValue.Instance);
                    expected.Add(null);
                }
            }

            column.RemoveRange(100, 100);
            expected.RemoveRange(100, 100);

            Assert.Equal(900, column.Count);
            Assert.Equal(expected.Count(x => x == null), column.GetNullCount());

            for (int i = 0; i < 900; i++)
            {
                if (expected[i] != null)
                {
                    Assert.Equal(expected[i], column.GetValueAt(i, default).AsDecimal);
                }
                else
                {
                    Assert.True(column.GetValueAt(i, default).IsNull);
                }
            }
        }
    }
}
