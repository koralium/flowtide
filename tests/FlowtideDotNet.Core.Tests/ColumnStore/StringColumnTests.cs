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
using System.Text.Json;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.Tests.ColumnStore
{
    public class StringColumnTests
    {
        [Fact]
        public void RemoveRangeNonNull()
        {
            Column column = new Column(GlobalMemoryManager.Instance);

            List<string?> expected = new List<string?>();
            Random r = new Random(123);
            for (int i = 0; i < 1000; i++)
            {
                var byteSize = r.Next(20);

                string data = new string(Enumerable.Range(0, byteSize).Select(x => (char)r.Next(32, 127)).ToArray());
                expected.Add(data);
                column.Add(new StringValue(data));
            }

            column.RemoveRange(100, 100);
            expected.RemoveRange(100, 100);

            Assert.Equal(900, column.Count);
            Assert.Equal(expected.Count(x => x == null), column.GetNullCount());

            for (int i = 0; i < 900; i++)
            {
                var actual = column.GetValueAt(i, default);
                if (expected[i] == null)
                {
                    Assert.True(actual.IsNull);
                }
                else
                {
                    Assert.Equal(expected[i], actual.AsString.ToString());
                }
            }
        }

        [Fact]
        public void RemoveRangeWithNull()
        {
            Column column = new Column(GlobalMemoryManager.Instance);

            List<string?> expected = new List<string?>();
            Random r = new Random(123);
            for (int i = 0; i < 1000; i++)
            {
                var isNull = r.Next(2) == 0;

                if (isNull)
                {
                    expected.Add(null);
                    column.Add(NullValue.Instance);
                }
                else
                {
                    var byteSize = r.Next(20);

                    string data = new string(Enumerable.Range(0, byteSize).Select(x => (char)r.Next(32, 127)).ToArray());
                    expected.Add(data);
                    column.Add(new StringValue(data));
                }

            }

            column.RemoveRange(100, 100);
            expected.RemoveRange(100, 100);

            Assert.Equal(900, column.Count);
            Assert.Equal(expected.Count(x => x == null), column.GetNullCount());

            for (int i = 0; i < 900; i++)
            {
                var actual = column.GetValueAt(i, default);
                if (expected[i] == null)
                {
                    Assert.True(actual.IsNull);
                }
                else
                {
                    Assert.Equal(expected[i], actual.AsString.ToString());
                }
            }
        }

        [Fact]
        public void TestJsonEncoding()
        {
            Column column = new Column(GlobalMemoryManager.Instance);

            column.Add(new StringValue("hello"));

            using NativeBufferWriter nativeBufferWriter = new NativeBufferWriter(GlobalMemoryManager.Instance);
            Utf8JsonWriter writer = new Utf8JsonWriter(nativeBufferWriter);

            column.WriteToJson(in writer, 0);
            writer.Flush();

            string json = Encoding.UTF8.GetString(nativeBufferWriter.WrittenSpan);

            Assert.Equal("\"hello\"", json);
        }
    }
}
