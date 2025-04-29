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
using System.IO.Hashing;
using System.Linq;
using System.Text;
using System.Text.Json;

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

        [Fact]
        public void TestJsonEncoding()
        {
            Column column = new Column(GlobalMemoryManager.Instance);

            column.Add(new DecimalValue(1.23m));

            using MemoryStream stream = new MemoryStream();
            Utf8JsonWriter writer = new Utf8JsonWriter(stream);

            column.WriteToJson(in writer, 0);
            writer.Flush();

            string json = Encoding.UTF8.GetString(stream.ToArray());

            Assert.Equal("1.23", json);
        }

        [Fact]
        public void TestCopy()
        {
            Column column = new Column(GlobalMemoryManager.Instance);
            Random r = new Random(123);
            for (int i = 0; i < 1000; i++)
            {
                if (r.Next(0, 2) == 0)
                {
                    column.Add(new DecimalValue(i));
                }
                else
                {
                    column.Add(NullValue.Instance);
                }
            }

            Column copy = column.Copy(GlobalMemoryManager.Instance);

            Assert.Equal(column.Count, copy.Count);

            for (int i = 0; i < 1000; i++)
            {
                var actual = copy.GetValueAt(i, default);
                if (column.GetValueAt(i, default).IsNull)
                {
                    Assert.True(actual.IsNull);
                }
                else
                {
                    Assert.Equal(column.GetValueAt(i, default).AsDecimal, actual.AsDecimal);
                }
            }
        }

        [Fact]
        public void TestAddToHash()
        {
            Column column = new Column(GlobalMemoryManager.Instance)
            {
                new DecimalValue(1.23m)
            };

            var hash = new XxHash32();
            column.AddToHash(0, default, hash);
            var columnHash = hash.GetHashAndReset();

            column.GetValueAt(0, default).AddToHash(hash);
            var valueHash = hash.GetHashAndReset();

            Assert.Equal(columnHash, valueHash);
        }
    }
}
