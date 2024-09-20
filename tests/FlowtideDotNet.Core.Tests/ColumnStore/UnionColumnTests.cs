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

        [Fact]
        public void TestUpdateToNull()
        {
            UnionColumn unionColumn = new UnionColumn(GlobalMemoryManager.Instance);
            Column column = new Column(GlobalMemoryManager.Instance);

            column.Add(new Int64Value(1));
            column.Add(new StringValue("hello"));

            Assert.Equal(1, column.GetValueAt(0, default).AsLong);

            column.UpdateAt(0, NullValue.Instance);

            Assert.True(column.GetValueAt(0, default).IsNull);
            Assert.Equal("hello", column.GetValueAt(1, default).AsString.ToString());
        }

        [Fact]
        public void TestUpdateToIntToStringColumnAreadyExists()
        {
            UnionColumn unionColumn = new UnionColumn(GlobalMemoryManager.Instance);
            Column column = new Column(GlobalMemoryManager.Instance);

            column.Add(new Int64Value(1));
            column.Add(new StringValue("hello"));

            Assert.Equal(1, column.GetValueAt(0, default).AsLong);

            column.UpdateAt(0, new StringValue("world"));

            Assert.Equal("world", column.GetValueAt(0, default).AsString.ToString());
            Assert.Equal("hello", column.GetValueAt(1, default).AsString.ToString());
        }

        [Fact]
        public void TestInsertNull()
        {
            UnionColumn unionColumn = new UnionColumn(GlobalMemoryManager.Instance);
            Column column = new Column(GlobalMemoryManager.Instance);

            column.Add(new Int64Value(1));
            column.Add(new StringValue("hello"));
            column.Add(new DecimalValue(123));

            column.InsertAt(0, NullValue.Instance);

            column.InsertAt(2, NullValue.Instance);

            Assert.True(column.GetValueAt(0, default).IsNull);
            Assert.Equal(1, column.GetValueAt(1, default).AsLong);
            Assert.True(column.GetValueAt(2, default).IsNull);
            Assert.Equal("hello", column.GetValueAt(3, default).AsString.ToString());
            Assert.Equal(123, column.GetValueAt(4, default).AsDecimal);
        }

        [Fact]
        public void TestInsertStrings()
        {
            UnionColumn unionColumn = new UnionColumn(GlobalMemoryManager.Instance);
            Column column = new Column(GlobalMemoryManager.Instance);

            column.Add(new Int64Value(1));
            column.Add(new StringValue("hello"));
            column.Add(new DecimalValue(123));

            column.InsertAt(0, new StringValue("world"));

            column.InsertAt(2, new StringValue("foo"));

            Assert.Equal("world", column.GetValueAt(0, default).AsString.ToString());
            Assert.Equal(1, column.GetValueAt(1, default).AsLong);
            Assert.Equal("foo", column.GetValueAt(2, default).AsString.ToString());
            Assert.Equal("hello", column.GetValueAt(3, default).AsString.ToString());
            Assert.Equal(123, column.GetValueAt(4, default).AsDecimal);
        }

        [Fact]
        public void TestDelete()
        {
            UnionColumn unionColumn = new UnionColumn(GlobalMemoryManager.Instance);
            Column column = new Column(GlobalMemoryManager.Instance);

            column.Add(new Int64Value(1));
            column.Add(new StringValue("hello"));
            column.Add(new DecimalValue(123));

            column.InsertAt(0, new StringValue("world"));

            column.InsertAt(2, new StringValue("foo"));

            column.RemoveAt(2);

            Assert.Equal("world", column.GetValueAt(0, default).AsString.ToString());
            Assert.Equal(1, column.GetValueAt(1, default).AsLong);
            Assert.Equal("hello", column.GetValueAt(2, default).AsString.ToString());
            Assert.Equal(123, column.GetValueAt(3, default).AsDecimal);
        }
    }
}
