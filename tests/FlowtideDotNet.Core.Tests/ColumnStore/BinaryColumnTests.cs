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
using System.Text;
using System.Text.Json;

namespace FlowtideDotNet.Core.Tests.ColumnStore
{
    public class BinaryColumnTests
    {
        [Fact]
        public void RemoveRangeNotNull()
        {
            Column column = new Column(GlobalMemoryManager.Instance);

            List<byte[]> expected = new List<byte[]>();
            Random r = new Random(123);
            for (int i = 0; i < 1000; i++)
            {
                var byteSize = r.Next(20);
                byte[] data = new byte[byteSize];
                r.NextBytes(data);
                expected.Add(data);

                column.Add(new BinaryValue(data));
            }

            column.RemoveRange(100, 100);
            expected.RemoveRange(100, 100);

            Assert.Equal(900, column.Count);

            for (int i = 0; i < 900; i++)
            {
                Assert.Equal(expected[i], column.GetValueAt(i, default).AsBinary);
            }
        }

        [Fact]
        public void RemoveRangeWithNull()
        {
            Column column = new Column(GlobalMemoryManager.Instance);

            List<byte[]?> expected = new List<byte[]?>();
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
                    byte[] data = new byte[byteSize];
                    r.NextBytes(data);
                    expected.Add(data);
                    column.Add(new BinaryValue(data));
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
                    Assert.Equal(expected[i], actual.AsBinary);
                }
            }
        }

        [Fact]
        public void TestJsonEncoding()
        {
            Column column = new Column(GlobalMemoryManager.Instance);

            column.Add(new BinaryValue(Encoding.UTF8.GetBytes("Hello World")));

            using MemoryStream stream = new MemoryStream();
            Utf8JsonWriter writer = new Utf8JsonWriter(stream);

            column.WriteToJson(in writer, 0);
            writer.Flush();

            string json = Encoding.UTF8.GetString(stream.ToArray());

            Assert.Equal("\"SGVsbG8gV29ybGQ=\"", json);
        }

        [Fact]
        public void TestCopy()
        {
            Column column = new Column(GlobalMemoryManager.Instance);

            List<byte[]> expected = new List<byte[]>();
            Random r = new Random(123);
            for (int i = 0; i < 1000; i++)
            {
                var byteSize = r.Next(20);
                byte[] data = new byte[byteSize];
                r.NextBytes(data);
                expected.Add(data);

                column.Add(new BinaryValue(data));
            }

            Column copy = column.Copy(GlobalMemoryManager.Instance);

            Assert.Equal(1000, copy.Count);

            for (int i = 0; i < 1000; i++)
            {
                Assert.Equal(expected[i], copy.GetValueAt(i, default).AsBinary);
            }
        }
    }
}
