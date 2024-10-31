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
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Storage.Memory;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.Tests.ColumnStore
{
    public class NullColumnTests
    {
        [Fact]
        public void RemoveRange()
        {
            NullColumn column = new NullColumn();

            for (int i = 0; i < 1000; i++)
            {
                column.Add(NullValue.Instance);
            }

            column.RemoveRange(100, 100);

            Assert.Equal(900, column.Count);
        }

        [Fact]
        public void TestJsonEncoding()
        {
            Column column = new Column(GlobalMemoryManager.Instance);

            column.Add(NullValue.Instance);

            using MemoryStream stream = new MemoryStream();
            Utf8JsonWriter writer = new Utf8JsonWriter(stream);

            column.WriteToJson(in writer, 0);
            writer.Flush();

            string json = Encoding.UTF8.GetString(stream.ToArray());

            Assert.Equal("null", json);
        }
    }
}
