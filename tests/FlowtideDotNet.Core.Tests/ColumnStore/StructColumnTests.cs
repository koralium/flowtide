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
using FlowtideDotNet.Substrait.Expressions;

namespace FlowtideDotNet.Core.Tests.ColumnStore
{
    public class StructColumnTests
    {
        [Fact]
        public void AddToEmptyColumn()
        {
            Column column = Column.Create(GlobalMemoryManager.Instance);

            var structHeader = StructHeader.Create("colum1", "column2");
            column.Add(new StructValue(structHeader, new Int64Value(123), new StringValue("hello")));
            column.Add(new StructValue(structHeader, new Int64Value(321), new StringValue("world")));

            var val1 = column.GetValueAt(0, default).AsStructValue;
            var val2 = column.GetValueAt(1, default).AsStructValue;

            Assert.Equal(123, val1.GetAt(0).AsLong);
            Assert.Equal("hello", val1.GetAt(1).AsString.ToString());
            Assert.Equal(321, val2.GetAt(0).AsLong);
            Assert.Equal("world", val2.GetAt(1).AsString.ToString());
        }

        [Fact]
        public void InsertIntoEmpty()
        {

            Column column = Column.Create(GlobalMemoryManager.Instance);

            var structHeader = StructHeader.Create("colum1", "column2");
            column.InsertAt(0, new StructValue(structHeader, new Int64Value(123), new StringValue("hello")));
            column.InsertAt(0, new StructValue(structHeader, new Int64Value(321), new StringValue("world")));

            var val1 = column.GetValueAt(0, default).AsStructValue;
            var val2 = column.GetValueAt(1, default).AsStructValue;

            Assert.Equal(321, val1.GetAt(0).AsLong);
            Assert.Equal("world", val1.GetAt(1).AsString.ToString());
            Assert.Equal(123, val2.GetAt(0).AsLong);
            Assert.Equal("hello", val2.GetAt(1).AsString.ToString());
        }

        [Fact]
        public void UpdateRow()
        {
            Column column = Column.Create(GlobalMemoryManager.Instance);

            var structHeader = StructHeader.Create("colum1", "column2");
            column.Add(new StructValue(structHeader, new Int64Value(123), new StringValue("hello")));
            column.Add(new StructValue(structHeader, new Int64Value(321), new StringValue("world")));

            column.UpdateAt(0, new StructValue(structHeader, new Int64Value(456), new StringValue("updated")));

            var val1 = column.GetValueAt(0, default).AsStructValue;

            Assert.Equal(456, val1.GetAt(0).AsLong);
            Assert.Equal("updated", val1.GetAt(1).AsString.ToString());
        }

        [Fact]
        public void AddStructToIntColumn()
        {
            Column column = Column.Create(GlobalMemoryManager.Instance);

            var structHeader = StructHeader.Create("colum1", "column2");
            column.Add(new Int64Value(123));
            var structVal = new StructValue(structHeader, new Int64Value(321), new StringValue("world"));
            column.Add(structVal);

            var val1 = column.GetValueAt(0, default);
            var val2 = column.GetValueAt(1, default).AsStructValue;

            Assert.Equal(123, val1.AsLong);
            Assert.Equal(321, val2.GetAt(0).AsLong);
            Assert.Equal("world", val2.GetAt(1).AsString.ToString());
        }

        [Fact]
        public void AddIntToStructColumn()
        {
            Column column = Column.Create(GlobalMemoryManager.Instance);

            var structHeader = StructHeader.Create("colum1", "column2");
            var structVal = new StructValue(structHeader, new Int64Value(123), new StringValue("hello"));
            column.Add(structVal);
            column.Add(new Int64Value(321));

            var val1 = column.GetValueAt(0, default).AsStructValue;
            var val2 = column.GetValueAt(1, default);

            Assert.Equal(123, val1.GetAt(0).AsLong);
            Assert.Equal("hello", val1.GetAt(1).AsString.ToString());
            Assert.Equal(321, val2.AsLong);
        }

        /// <summary>
        /// Tests the basic insert range from basic column for union column with structs
        /// </summary>
        [Fact]
        public void InsertStructRangeIntoIntColumn()
        {
            Column intColumn = Column.Create(GlobalMemoryManager.Instance);

            var structHeader = StructHeader.Create("colum1", "column2");
            intColumn.Add(new Int64Value(123));
            var structVal = new StructValue(structHeader, new Int64Value(321), new StringValue("world"));

            Column structColumn = Column.Create(GlobalMemoryManager.Instance);

            structColumn.Add(structVal);

            intColumn.InsertRangeFrom(1, structColumn, 0, 1);

            var val1 = intColumn.GetValueAt(0, default);
            var val2 = intColumn.GetValueAt(1, default).AsStructValue;

            Assert.Equal(123, val1.AsLong);
            Assert.Equal(321, val2.GetAt(0).AsLong);
            Assert.Equal("world", val2.GetAt(1).AsString.ToString());
        }

        [Fact]
        public void InsertUnionWithStructIntoUnionColumn()
        {
            Column unionWithoutStructColumn = Column.Create(GlobalMemoryManager.Instance);
            unionWithoutStructColumn.Add(new Int64Value(123));
            unionWithoutStructColumn.Add(new StringValue("hello"));

            var structHeader = StructHeader.Create("colum1", "column2");

            var structVal = new StructValue(structHeader, new Int64Value(321), new StringValue("world"));

            Column unionWithStructColumn = Column.Create(GlobalMemoryManager.Instance);

            unionWithStructColumn.Add(structVal);
            unionWithStructColumn.Add(new Int64Value(7));

            // we insert in the middle of the other union column
            unionWithoutStructColumn.InsertRangeFrom(1, unionWithStructColumn, 0, 2);

            var val1 = unionWithoutStructColumn.GetValueAt(0, default);
            var val2 = unionWithoutStructColumn.GetValueAt(1, default).AsStructValue;
            var val3 = unionWithoutStructColumn.GetValueAt(2, default);
            var val4 = unionWithoutStructColumn.GetValueAt(3, default);

            Assert.Equal(123, val1.AsLong);
            Assert.Equal(321, val2.GetAt(0).AsLong);
            Assert.Equal("world", val2.GetAt(1).AsString.ToString());
            Assert.Equal(7, val3.AsLong);
            Assert.Equal("hello", val4.AsString.ToString());
        }

        [Fact]
        public void RemoveAtInStruct()
        {
            Column column = Column.Create(GlobalMemoryManager.Instance);

            var structHeader = StructHeader.Create("colum1", "column2");
            column.Add(new StructValue(structHeader, new Int64Value(123), new StringValue("hello")));
            column.Add(new StructValue(structHeader, new Int64Value(321), new StringValue("world")));
            Assert.Equal(2, column.Count);

            column.RemoveAt(0);

            var val1 = column.GetValueAt(0, default).AsStructValue;

            Assert.Single(column);
            Assert.Equal(321, val1.GetAt(0).AsLong);
            Assert.Equal("world", val1.GetAt(1).AsString.ToString());
        }

        [Fact]
        public void RemoveRangeInStruct()
        {
            Column column = Column.Create(GlobalMemoryManager.Instance);

            var structHeader = StructHeader.Create("colum1", "column2");
            column.Add(new StructValue(structHeader, new Int64Value(123), new StringValue("hello")));
            column.Add(new StructValue(structHeader, new Int64Value(321), new StringValue("world")));
            Assert.Equal(2, column.Count);

            column.RemoveRange(0, 1);

            var val1 = column.GetValueAt(0, default).AsStructValue;

            Assert.Single(column);
            Assert.Equal(321, val1.GetAt(0).AsLong);
            Assert.Equal("world", val1.GetAt(1).AsString.ToString());
        }

        [Fact]
        public void SearchBoundaryAscending()
        {
            Column column = Column.Create(GlobalMemoryManager.Instance);

            var structHeader = StructHeader.Create("colum1", "column2");
            column.Add(new StructValue(structHeader, new Int64Value(123), new StringValue("hello")));
            column.Add(new StructValue(structHeader, new Int64Value(123), new StringValue("world")));

            var toFind1 = new StructValue(structHeader, new Int64Value(123), new StringValue("hello"));
            var toFind2 = new StructValue(structHeader, new Int64Value(123), new StringValue("world"));
            var toFind3 = new StructValue(structHeader, new Int64Value(122), new StringValue("world"));
            var toFind4 = new StructValue(structHeader, new Int64Value(124), new StringValue("world"));

            var (low1, high1) = column.SearchBoundries(toFind1, 0, column.Count - 1, default);
            var (low2, high2) = column.SearchBoundries(toFind2, 0, column.Count - 1, default);
            var (low3, high3) = column.SearchBoundries(toFind3, 0, column.Count - 1, default);
            var (low4, high4) = column.SearchBoundries(toFind4, 0, column.Count - 1, default);

            Assert.Equal(0, low1);
            Assert.Equal(0, high1);
            Assert.Equal(1, low2);
            Assert.Equal(1, high2);
            Assert.Equal(-1, low3);
            Assert.Equal(-1, high3);
            Assert.Equal(-3, low4);
            Assert.Equal(-3, high4);
        }

        [Fact]
        public void SearchBoundaryAscendingWithChild()
        {
            Column column = Column.Create(GlobalMemoryManager.Instance);

            var structHeader = StructHeader.Create("column1", "column2");
            column.Add(new StructValue(structHeader, new Int64Value(123), new StringValue("hello")));
            column.Add(new StructValue(structHeader, new Int64Value(123), new StringValue("world")));

            var toFind1 = new Int64Value(123);
            var toFind2 = new Int64Value(122);
            var toFind3 = new Int64Value(124);

            var childMap = new MapKeyReferenceSegment()
            {
                Key = "column1"
            };
            var (low1, high1) = column.SearchBoundries(toFind1, 0, column.Count - 1, childMap);
            var (low2, high2) = column.SearchBoundries(toFind2, 0, column.Count - 1, childMap);
            var (low3, high3) = column.SearchBoundries(toFind3, 0, column.Count - 1, childMap);

            Assert.Equal(0, low1);
            Assert.Equal(1, high1);
            Assert.Equal(-1, low2);
            Assert.Equal(-1, high2);
            Assert.Equal(-3, low3);
            Assert.Equal(-3, high3);
        }

        [Fact]
        public void SearchBoundaryDescending()
        {
            Column column = Column.Create(GlobalMemoryManager.Instance);

            var structHeader = StructHeader.Create("colum1", "column2");
            column.Add(new StructValue(structHeader, new Int64Value(123), new StringValue("world")));
            column.Add(new StructValue(structHeader, new Int64Value(123), new StringValue("hello")));

            var toFind1 = new StructValue(structHeader, new Int64Value(123), new StringValue("hello"));
            var toFind2 = new StructValue(structHeader, new Int64Value(123), new StringValue("world"));
            var toFind3 = new StructValue(structHeader, new Int64Value(122), new StringValue("world"));
            var toFind4 = new StructValue(structHeader, new Int64Value(124), new StringValue("world"));

            var (low1, high1) = column.SearchBoundries(toFind1, 0, column.Count - 1, default, true);
            var (low2, high2) = column.SearchBoundries(toFind2, 0, column.Count - 1, default, true);
            var (low3, high3) = column.SearchBoundries(toFind3, 0, column.Count - 1, default, true);
            var (low4, high4) = column.SearchBoundries(toFind4, 0, column.Count - 1, default, true);

            Assert.Equal(1, low1);
            Assert.Equal(1, high1);
            Assert.Equal(0, low2);
            Assert.Equal(0, high2);
            Assert.Equal(-3, low3);
            Assert.Equal(-3, high3);
            Assert.Equal(-1, low4);
            Assert.Equal(-1, high4);
        }

        [Fact]
        public void SearchBoundaryDescendingWithChild()
        {
            Column column = Column.Create(GlobalMemoryManager.Instance);

            var structHeader = StructHeader.Create("column1", "column2");
            column.Add(new StructValue(structHeader, new Int64Value(123), new StringValue("world")));
            column.Add(new StructValue(structHeader, new Int64Value(123), new StringValue("hello")));

            var toFind1 = new Int64Value(123);
            var toFind2 = new Int64Value(122);
            var toFind3 = new Int64Value(124);

            var childMap = new MapKeyReferenceSegment()
            {
                Key = "column1"
            };
            var (low1, high1) = column.SearchBoundries(toFind1, 0, column.Count - 1, childMap, true);
            var (low2, high2) = column.SearchBoundries(toFind2, 0, column.Count - 1, childMap, true);
            var (low3, high3) = column.SearchBoundries(toFind3, 0, column.Count - 1, childMap, true);

            Assert.Equal(0, low1);
            Assert.Equal(1, high1);
            Assert.Equal(-3, low2);
            Assert.Equal(-3, high2);
            Assert.Equal(-1, low3);
            Assert.Equal(-1, high3);
        }
    }
}
