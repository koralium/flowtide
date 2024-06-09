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
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.Tests
{
    public class ColumnStoreTests
    {
        [Fact]
        public void TestColumnInt64()
        {
            Column column = new Column();

            for (int i = 0; i < 100; i++)
            {
                column.InsertAt(0, new Int64Value(i));
            }

            column.CompareTo(5, new Int64Value(5));
        }

        [Fact]
        public void TestColumnString()
        {
            Column column = new Column();

            for (int i = 0; i < 100; i++)
            {
                column.InsertAt(i, new StringValue(i.ToString()));
            }

            column.CompareTo(5, new StringValue("5"));
        }
    }
}
