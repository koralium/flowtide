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
    public class BoolColumnTests
    {
        [Fact]
        public void TestAddAndGetByIndex()
        {
            var column = new BoolColumn(GlobalMemoryManager.Instance);
            int i1 = column.Add(new BoolValue(true));
            int i2 = column.Add(new BoolValue(false));
            int i3 = column.Add(new BoolValue(true));

            Assert.Equal(0, i1);
            Assert.Equal(1, i2);
            Assert.Equal(2, i3);

            Assert.True(column.GetValueAt(i1, default).AsBool);
            Assert.False(column.GetValueAt(i2, default).AsBool);
            Assert.True(column.GetValueAt(i3, default).AsBool);
        }

        [Fact]
        public void TestSearchBoundries()
        {
            var column = new BoolColumn(GlobalMemoryManager.Instance);
            column.Add(new BoolValue(false));
            column.Add(new BoolValue(false));
            column.Add(new BoolValue(false));
            column.Add(new BoolValue(true));

            var (start, end) = column.SearchBoundries(new BoolValue(false), 0,3, default, false);
            Assert.Equal(0, start);
            Assert.Equal(2, end);

            (start, end) = column.SearchBoundries(new BoolValue(true), 0, 3, default, false);
            Assert.Equal(3, start);
            Assert.Equal(3, end);

            var emptyColumn = new BoolColumn(GlobalMemoryManager.Instance);
            (start, end) = emptyColumn.SearchBoundries(new BoolValue(true), 0, -1, default, false);
            Assert.Equal(~0, start);
            Assert.Equal(~0, end);
        }

        [Fact]
        public void TestCompareTo()
        {
            var column = new BoolColumn(GlobalMemoryManager.Instance);
            column.Add(new BoolValue(false));
            column.Add(new BoolValue(true));
            Assert.Equal(-1, column.CompareTo(0, new BoolValue(true), default, default));
            Assert.Equal(0, column.CompareTo(0, new BoolValue(false), default, default));
            Assert.Equal(0, column.CompareTo(1, new BoolValue(true), default, default));
            Assert.Equal(1, column.CompareTo(1, new BoolValue(false), default, default));
        }

        [Fact]
        public void TestRemoveRangeNonNull()
        {
            Column column = new Column(GlobalMemoryManager.Instance);

            List<bool> expected = new List<bool>();
            Random r = new Random(123);
            for (int i = 0; i < 1000; i++)
            {
                var boolVal = r.NextDouble() > 0.5;
                expected.Add(boolVal);
                column.Add(new BoolValue(boolVal));
            }

            column.RemoveRange(100, 100);
            expected.RemoveRange(100, 100);

            Assert.Equal(900, column.Count);

            for (int i = 0; i < 900; i++)
            {
                Assert.Equal(expected[i], column.GetValueAt(i, default).AsBool);
            }
        }

        [Fact]
        public void TestRemoveRangeWithNull()
        {
            Column column = new Column(GlobalMemoryManager.Instance);

            List<bool?> expected = new List<bool?>();
            Random r = new Random(123);
            for (int i = 0; i < 1000; i++)
            {
                bool? boolVal = r.NextDouble() > 0.5 ? r.NextDouble() > 0.5 : null;
                expected.Add(boolVal);
                if (boolVal.HasValue)
                {
                    column.Add(new BoolValue(boolVal.Value));
                }
                else
                {
                    column.Add(NullValue.Instance);
                }
                
            }

            column.RemoveRange(100, 100);
            expected.RemoveRange(100, 100);

            Assert.Equal(900, column.Count);
            Assert.Equal(expected.Count(x => x == null), column.GetNullCount());

            for (int i = 0; i < 900; i++)
            {
                var actual = column.GetValueAt(i, default);
                if (expected[i].HasValue)
                {
                    Assert.Equal(expected[i]!.Value, actual.AsBool);
                }
                else
                {
                    Assert.True(actual.IsNull);
                }
            }
        }

        [Fact]
        public void TestJsonEncoding()
        {
            Column column = new Column(GlobalMemoryManager.Instance);

            column.Add(new BoolValue(true));

            using MemoryStream stream = new MemoryStream();
            Utf8JsonWriter writer = new Utf8JsonWriter(stream);

            column.WriteToJson(in writer, 0);
            writer.Flush();

            string json = Encoding.UTF8.GetString(stream.ToArray());

            Assert.Equal("true", json);
        }
    }
}
