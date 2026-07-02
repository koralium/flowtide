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

namespace FlowtideDotNet.Core.Tests.ColumnStore
{
    public class BoundarySearchTests
    {
        [Fact]
        public void TestBoundarySearchWithIntegersAscending()
        {
            using Column column = Column.Create(GlobalMemoryManager.Instance);

            column.Add(new Int64Value(1));
            column.Add(new Int64Value(2));
            column.Add(new Int64Value(2));
            column.Add(new Int64Value(3));


            var (start, end) = column.SearchBoundries(new Int64Value(0), 0, 3, default);
            Assert.Equal(-1, start);
            Assert.Equal(-1, end);

            (start, end) = column.SearchBoundries(new Int64Value(1), 0, 3, default);
            Assert.Equal(0, start);
            Assert.Equal(0, end);

            (start, end) = column.SearchBoundries(new Int64Value(2), 0, 3, default);
            Assert.Equal(1, start);
            Assert.Equal(2, end);

            (start, end) = column.SearchBoundries(new Int64Value(3), 0, 3, default);
            Assert.Equal(3, start);
            Assert.Equal(3, end);

            (start, end) = column.SearchBoundries(new Int64Value(4), 0, 3, default);
            Assert.Equal(~4, start);
            Assert.Equal(~4, end);
        }

        [Fact]
        public void TestBoundarySearchWithIntegersDescending()
        {
            using Column column = Column.Create(GlobalMemoryManager.Instance);

            column.Add(new Int64Value(3));
            column.Add(new Int64Value(2));
            column.Add(new Int64Value(2));
            column.Add(new Int64Value(1));

            var (start, end) = column.SearchBoundries(new Int64Value(0), 0, 3, default, true);
            Assert.Equal(~4, start);
            Assert.Equal(~4, end);

            (start, end) = column.SearchBoundries(new Int64Value(1), 0, 3, default, true);
            Assert.Equal(3, start);
            Assert.Equal(3, end);

            (start, end) = column.SearchBoundries(new Int64Value(2), 0, 3, default, true);
            Assert.Equal(1, start);
            Assert.Equal(2, end);

            (start, end) = column.SearchBoundries(new Int64Value(3), 0, 3, default, true);
            Assert.Equal(0, start);
            Assert.Equal(0, end);

            (start, end) = column.SearchBoundries(new Int64Value(4), 0, 3, default, true);
            Assert.Equal(-1, start);
            Assert.Equal(-1, end);
        }

        [Fact]
        public void TestBoundarySearchWithIntegersAndNullAscending()
        {
            using Column column = Column.Create(GlobalMemoryManager.Instance);

            column.Add(NullValue.Instance);
            column.Add(new Int64Value(1));
            column.Add(new Int64Value(2));
            column.Add(new Int64Value(2));
            column.Add(new Int64Value(3));

            var (start, end) = column.SearchBoundries(NullValue.Instance, 0, 4, default);
            Assert.Equal(0, start);
            Assert.Equal(0, end);

            (start, end) = column.SearchBoundries(new Int64Value(0), 0, 4, default);
            Assert.Equal(~1, start);
            Assert.Equal(~1, end);

            (start, end) = column.SearchBoundries(new Int64Value(1), 0, 4, default);
            Assert.Equal(1, start);
            Assert.Equal(1, end);

            (start, end) = column.SearchBoundries(new Int64Value(2), 0, 4, default);
            Assert.Equal(2, start);
            Assert.Equal(3, end);

            (start, end) = column.SearchBoundries(new Int64Value(3), 0, 4, default);
            Assert.Equal(4, start);
            Assert.Equal(4, end);

            (start, end) = column.SearchBoundries(new Int64Value(4), 0, 4, default);
            Assert.Equal(~5, start);
            Assert.Equal(~5, end);
        }

        [Fact]
        public void TestBoundarySearchWithIntegersAndNullDescending()
        {
            using Column column = Column.Create(GlobalMemoryManager.Instance);

            column.Add(new Int64Value(3));
            column.Add(new Int64Value(2));
            column.Add(new Int64Value(2));
            column.Add(new Int64Value(1));
            column.Add(NullValue.Instance);

            var (start, end) = column.SearchBoundries(new Int64Value(0), 0, 4, default, true);
            Assert.Equal(~4, start);
            Assert.Equal(~4, end);

            (start, end) = column.SearchBoundries(new Int64Value(1), 0, 4, default, true);
            Assert.Equal(3, start);
            Assert.Equal(3, end);

            (start, end) = column.SearchBoundries(new Int64Value(2), 0, 4, default, true);
            Assert.Equal(1, start);
            Assert.Equal(2, end);

            (start, end) = column.SearchBoundries(new Int64Value(3), 0, 4, default, true);
            Assert.Equal(0, start);
            Assert.Equal(0, end);

            (start, end) = column.SearchBoundries(new Int64Value(4), 0, 4, default, true);
            Assert.Equal(-1, start);
            Assert.Equal(-1, end);

            (start, end) = column.SearchBoundries(NullValue.Instance, 0, 4, default, true);
            Assert.Equal(4, start);
            Assert.Equal(4, end);
        }

        [Fact]
        public void TestBoundarySearchWithStringsAscending()
        {
            using Column column = Column.Create(GlobalMemoryManager.Instance);

            column.Add(new StringValue("b"));
            column.Add(new StringValue("c"));
            column.Add(new StringValue("c"));
            column.Add(new StringValue("d"));


            var (start, end) = column.SearchBoundries(new StringValue("a"), 0, 3, default);
            Assert.Equal(-1, start);
            Assert.Equal(-1, end);

            (start, end) = column.SearchBoundries(new StringValue("b"), 0, 3, default);
            Assert.Equal(0, start);
            Assert.Equal(0, end);

            (start, end) = column.SearchBoundries(new StringValue("c"), 0, 3, default);
            Assert.Equal(1, start);
            Assert.Equal(2, end);

            (start, end) = column.SearchBoundries(new StringValue("d"), 0, 3, default);
            Assert.Equal(3, start);
            Assert.Equal(3, end);

            (start, end) = column.SearchBoundries(new StringValue("e"), 0, 3, default);
            Assert.Equal(~4, start);
            Assert.Equal(~4, end);
        }

        [Fact]
        public void TestBoundarySearchWithStringsDescending()
        {
            using Column column = Column.Create(GlobalMemoryManager.Instance);

            column.Add(new StringValue("d"));
            column.Add(new StringValue("c"));
            column.Add(new StringValue("c"));
            column.Add(new StringValue("b"));

            var (start, end) = column.SearchBoundries(new StringValue("a"), 0, 3, default, true);
            Assert.Equal(~4, start);
            Assert.Equal(~4, end);

            (start, end) = column.SearchBoundries(new StringValue("b"), 0, 3, default, true);
            Assert.Equal(3, start);
            Assert.Equal(3, end);

            (start, end) = column.SearchBoundries(new StringValue("c"), 0, 3, default, true);
            Assert.Equal(1, start);
            Assert.Equal(2, end);

            (start, end) = column.SearchBoundries(new StringValue("d"), 0, 3, default, true);
            Assert.Equal(0, start);
            Assert.Equal(0, end);

            (start, end) = column.SearchBoundries(new StringValue("e"), 0, 3, default, true);
            Assert.Equal(-1, start);
            Assert.Equal(-1, end);
        }

        [Fact]
        public void TestBoundarySearchWithStringsAndNullAscending()
        {
            using Column column = Column.Create(GlobalMemoryManager.Instance);

            column.Add(NullValue.Instance);
            column.Add(new StringValue("b"));
            column.Add(new StringValue("c"));
            column.Add(new StringValue("c"));
            column.Add(new StringValue("d"));

            var (start, end) = column.SearchBoundries(NullValue.Instance, 0, 4, default);
            Assert.Equal(0, start);
            Assert.Equal(0, end);

            (start, end) = column.SearchBoundries(new StringValue("a"), 0, 4, default);
            Assert.Equal(~1, start);
            Assert.Equal(~1, end);

            (start, end) = column.SearchBoundries(new StringValue("b"), 0, 4, default);
            Assert.Equal(1, start);
            Assert.Equal(1, end);

            (start, end) = column.SearchBoundries(new StringValue("c"), 0, 4, default);
            Assert.Equal(2, start);
            Assert.Equal(3, end);

            (start, end) = column.SearchBoundries(new StringValue("d"), 0, 4, default);
            Assert.Equal(4, start);
            Assert.Equal(4, end);

            (start, end) = column.SearchBoundries(new StringValue("e"), 0, 4, default);
            Assert.Equal(~5, start);
            Assert.Equal(~5, end);
        }

        [Fact]
        public void TestBoundarySearchWithStringsAndNullDescending()
        {
            using Column column = Column.Create(GlobalMemoryManager.Instance);

            column.Add(new StringValue("d"));
            column.Add(new StringValue("c"));
            column.Add(new StringValue("c"));
            column.Add(new StringValue("b"));
            column.Add(NullValue.Instance);

            var (start, end) = column.SearchBoundries(new StringValue("a"), 0, 4, default, true);
            Assert.Equal(~4, start);
            Assert.Equal(~4, end);

            (start, end) = column.SearchBoundries(new StringValue("b"), 0, 4, default, true);
            Assert.Equal(3, start);
            Assert.Equal(3, end);

            (start, end) = column.SearchBoundries(new StringValue("c"), 0, 4, default, true);
            Assert.Equal(1, start);
            Assert.Equal(2, end);

            (start, end) = column.SearchBoundries(new StringValue("d"), 0, 4, default, true);
            Assert.Equal(0, start);
            Assert.Equal(0, end);

            (start, end) = column.SearchBoundries(new StringValue("e"), 0, 4, default, true);
            Assert.Equal(-1, start);
            Assert.Equal(-1, end);

            (start, end) = column.SearchBoundries(NullValue.Instance, 0, 4, default, true);
            Assert.Equal(4, start);
            Assert.Equal(4, end);
        }

        [Fact]
        public void TestBoundarySearchWithBoolAscending()
        {
            using Column column = Column.Create(GlobalMemoryManager.Instance);

            column.Add(new BoolValue(false));
            column.Add(new BoolValue(true));
            column.Add(new BoolValue(true));

            var (start, end) = column.SearchBoundries(new BoolValue(false), 0, 2, default);
            Assert.Equal(0, start);
            Assert.Equal(0, end);

            (start, end) = column.SearchBoundries(new BoolValue(true), 0, 2, default);
            Assert.Equal(1, start);
            Assert.Equal(2, end);
        }

        [Fact]
        public void TestBoundarySearchWithBoolDescending()
        {
            using Column column = Column.Create(GlobalMemoryManager.Instance);

            column.Add(new BoolValue(true));
            column.Add(new BoolValue(true));
            column.Add(new BoolValue(false));

            var (start, end) = column.SearchBoundries(new BoolValue(false), 0, 2, default, true);
            Assert.Equal(2, start);
            Assert.Equal(2, end);

            (start, end) = column.SearchBoundries(new BoolValue(true), 0, 2, default, true);
            Assert.Equal(0, start);
            Assert.Equal(1, end);
        }

        [Fact]
        public void TestBoundarySearchWithBoolAndNullAscending()
        {
            using Column column = Column.Create(GlobalMemoryManager.Instance);

            column.Add(NullValue.Instance);
            column.Add(new BoolValue(false));
            column.Add(new BoolValue(true));
            column.Add(new BoolValue(true));

            var (start, end) = column.SearchBoundries(NullValue.Instance, 0, 3, default);
            Assert.Equal(0, start);
            Assert.Equal(0, end);

            (start, end) = column.SearchBoundries(new BoolValue(false), 0, 3, default);
            Assert.Equal(1, start);
            Assert.Equal(1, end);

            (start, end) = column.SearchBoundries(new BoolValue(true), 0, 3, default);
            Assert.Equal(2, start);
            Assert.Equal(3, end);
        }

        [Fact]
        public void TestBoundarySearchWithBoolAndNullDescending()
        {
            using Column column = Column.Create(GlobalMemoryManager.Instance);

            column.Add(new BoolValue(true));
            column.Add(new BoolValue(true));
            column.Add(new BoolValue(false));
            column.Add(NullValue.Instance);

            var (start, end) = column.SearchBoundries(new BoolValue(false), 0, 3, default, true);
            Assert.Equal(2, start);
            Assert.Equal(2, end);

            (start, end) = column.SearchBoundries(new BoolValue(true), 0, 3, default, true);
            Assert.Equal(0, start);
            Assert.Equal(1, end);

            (start, end) = column.SearchBoundries(NullValue.Instance, 0, 3, default, true);
            Assert.Equal(3, start);
            Assert.Equal(3, end);
        }

        [Fact]
        public void TestBoundarySearchWithBinaryAscending()
        {
            using Column column = Column.Create(GlobalMemoryManager.Instance);

            column.Add(new BinaryValue(new byte[] { 1 }));
            column.Add(new BinaryValue(new byte[] { 2 }));
            column.Add(new BinaryValue(new byte[] { 2 }));
            column.Add(new BinaryValue(new byte[] { 3 }));


            var (start, end) = column.SearchBoundries(new BinaryValue(new byte[] { 0 }), 0, 3, default);
            Assert.Equal(-1, start);
            Assert.Equal(-1, end);

            (start, end) = column.SearchBoundries(new BinaryValue(new byte[] { 1 }), 0, 3, default);
            Assert.Equal(0, start);
            Assert.Equal(0, end);

            (start, end) = column.SearchBoundries(new BinaryValue(new byte[] { 2 }), 0, 3, default);
            Assert.Equal(1, start);
            Assert.Equal(2, end);

            (start, end) = column.SearchBoundries(new BinaryValue(new byte[] { 3 }), 0, 3, default);
            Assert.Equal(3, start);
            Assert.Equal(3, end);

            (start, end) = column.SearchBoundries(new BinaryValue(new byte[] { 4 }), 0, 3, default);
            Assert.Equal(~4, start);
            Assert.Equal(~4, end);
        }

        [Fact]
        public void TestBoundarySearchWithBinaryDescending()
        {
            using Column column = Column.Create(GlobalMemoryManager.Instance);

            column.Add(new BinaryValue(new byte[] { 3 }));
            column.Add(new BinaryValue(new byte[] { 2 }));
            column.Add(new BinaryValue(new byte[] { 2 }));
            column.Add(new BinaryValue(new byte[] { 1 }));

            var (start, end) = column.SearchBoundries(new BinaryValue(new byte[] { 0 }), 0, 3, default, true);
            Assert.Equal(~4, start);
            Assert.Equal(~4, end);

            (start, end) = column.SearchBoundries(new BinaryValue(new byte[] { 1 }), 0, 3, default, true);
            Assert.Equal(3, start);
            Assert.Equal(3, end);

            (start, end) = column.SearchBoundries(new BinaryValue(new byte[] { 2 }), 0, 3, default, true);
            Assert.Equal(1, start);
            Assert.Equal(2, end);

            (start, end) = column.SearchBoundries(new BinaryValue(new byte[] { 3 }), 0, 3, default, true);
            Assert.Equal(0, start);
            Assert.Equal(0, end);

            (start, end) = column.SearchBoundries(new BinaryValue(new byte[] { 4 }), 0, 3, default, true);
            Assert.Equal(-1, start);
            Assert.Equal(-1, end);
        }

        [Fact]
        public void TestBoundarySearchWithBinaryAndNullAscending()
        {
            using Column column = Column.Create(GlobalMemoryManager.Instance);

            column.Add(NullValue.Instance);
            column.Add(new BinaryValue(new byte[] { 1 }));
            column.Add(new BinaryValue(new byte[] { 2 }));
            column.Add(new BinaryValue(new byte[] { 2 }));
            column.Add(new BinaryValue(new byte[] { 3 }));

            var (start, end) = column.SearchBoundries(NullValue.Instance, 0, 4, default);
            Assert.Equal(0, start);
            Assert.Equal(0, end);

            (start, end) = column.SearchBoundries(new BinaryValue(new byte[] { 0 }), 0, 4, default);
            Assert.Equal(~1, start);
            Assert.Equal(~1, end);

            (start, end) = column.SearchBoundries(new BinaryValue(new byte[] { 1 }), 0, 4, default);
            Assert.Equal(1, start);
            Assert.Equal(1, end);

            (start, end) = column.SearchBoundries(new BinaryValue(new byte[] { 2 }), 0, 4, default);
            Assert.Equal(2, start);
            Assert.Equal(3, end);

            (start, end) = column.SearchBoundries(new BinaryValue(new byte[] { 3 }), 0, 4, default);
            Assert.Equal(4, start);
            Assert.Equal(4, end);

            (start, end) = column.SearchBoundries(new BinaryValue(new byte[] { 4 }), 0, 4, default);
            Assert.Equal(~5, start);
            Assert.Equal(~5, end);
        }

        [Fact]
        public void TestBoundarySearchWithBinaryAndNullDescending()
        {
            using Column column = Column.Create(GlobalMemoryManager.Instance);

            column.Add(new BinaryValue(new byte[] { 3 }));
            column.Add(new BinaryValue(new byte[] { 2 }));
            column.Add(new BinaryValue(new byte[] { 2 }));
            column.Add(new BinaryValue(new byte[] { 1 }));
            column.Add(NullValue.Instance);

            var (start, end) = column.SearchBoundries(new BinaryValue(new byte[] { 0 }), 0, 4, default, true);
            Assert.Equal(~4, start);
            Assert.Equal(~4, end);

            (start, end) = column.SearchBoundries(new BinaryValue(new byte[] { 1 }), 0, 4, default, true);
            Assert.Equal(3, start);
            Assert.Equal(3, end);

            (start, end) = column.SearchBoundries(new BinaryValue(new byte[] { 2 }), 0, 4, default, true);
            Assert.Equal(1, start);
            Assert.Equal(2, end);

            (start, end) = column.SearchBoundries(new BinaryValue(new byte[] { 3 }), 0, 4, default, true);
            Assert.Equal(0, start);
            Assert.Equal(0, end);

            (start, end) = column.SearchBoundries(new BinaryValue(new byte[] { 4 }), 0, 4, default, true);
            Assert.Equal(-1, start);
            Assert.Equal(-1, end);

            (start, end) = column.SearchBoundries(NullValue.Instance, 0, 4, default, true);
            Assert.Equal(4, start);
            Assert.Equal(4, end);
        }

        [Fact]
        public void TestBoundarySearchWithDecimalsAscending()
        {
            using Column column = Column.Create(GlobalMemoryManager.Instance);

            column.Add(new DecimalValue(1));
            column.Add(new DecimalValue(2));
            column.Add(new DecimalValue(2));
            column.Add(new DecimalValue(3));


            var (start, end) = column.SearchBoundries(new DecimalValue(0), 0, 3, default);
            Assert.Equal(-1, start);
            Assert.Equal(-1, end);

            (start, end) = column.SearchBoundries(new DecimalValue(1), 0, 3, default);
            Assert.Equal(0, start);
            Assert.Equal(0, end);

            (start, end) = column.SearchBoundries(new DecimalValue(2), 0, 3, default);
            Assert.Equal(1, start);
            Assert.Equal(2, end);

            (start, end) = column.SearchBoundries(new DecimalValue(3), 0, 3, default);
            Assert.Equal(3, start);
            Assert.Equal(3, end);

            (start, end) = column.SearchBoundries(new DecimalValue(4), 0, 3, default);
            Assert.Equal(~4, start);
            Assert.Equal(~4, end);
        }

        [Fact]
        public void TestBoundarySearchWithDecimalsDescending()
        {
            using Column column = Column.Create(GlobalMemoryManager.Instance);

            column.Add(new DecimalValue(3));
            column.Add(new DecimalValue(2));
            column.Add(new DecimalValue(2));
            column.Add(new DecimalValue(1));

            var (start, end) = column.SearchBoundries(new DecimalValue(0), 0, 3, default, true);
            Assert.Equal(~4, start);
            Assert.Equal(~4, end);

            (start, end) = column.SearchBoundries(new DecimalValue(1), 0, 3, default, true);
            Assert.Equal(3, start);
            Assert.Equal(3, end);

            (start, end) = column.SearchBoundries(new DecimalValue(2), 0, 3, default, true);
            Assert.Equal(1, start);
            Assert.Equal(2, end);

            (start, end) = column.SearchBoundries(new DecimalValue(3), 0, 3, default, true);
            Assert.Equal(0, start);
            Assert.Equal(0, end);

            (start, end) = column.SearchBoundries(new DecimalValue(4), 0, 3, default, true);
            Assert.Equal(-1, start);
            Assert.Equal(-1, end);
        }

        [Fact]
        public void TestBoundarySearchWithDecimalsAndNullAscending()
        {
            using Column column = Column.Create(GlobalMemoryManager.Instance);

            column.Add(NullValue.Instance);
            column.Add(new DecimalValue(1));
            column.Add(new DecimalValue(2));
            column.Add(new DecimalValue(2));
            column.Add(new DecimalValue(3));

            var (start, end) = column.SearchBoundries(NullValue.Instance, 0, 4, default);
            Assert.Equal(0, start);
            Assert.Equal(0, end);

            (start, end) = column.SearchBoundries(new DecimalValue(0), 0, 4, default);
            Assert.Equal(~1, start);
            Assert.Equal(~1, end);

            (start, end) = column.SearchBoundries(new DecimalValue(1), 0, 4, default);
            Assert.Equal(1, start);
            Assert.Equal(1, end);

            (start, end) = column.SearchBoundries(new DecimalValue(2), 0, 4, default);
            Assert.Equal(2, start);
            Assert.Equal(3, end);

            (start, end) = column.SearchBoundries(new DecimalValue(3), 0, 4, default);
            Assert.Equal(4, start);
            Assert.Equal(4, end);

            (start, end) = column.SearchBoundries(new DecimalValue(4), 0, 4, default);
            Assert.Equal(~5, start);
            Assert.Equal(~5, end);
        }

        [Fact]
        public void TestBoundarySearchWithDecimalsAndNullDescending()
        {
            using Column column = Column.Create(GlobalMemoryManager.Instance);

            column.Add(new DecimalValue(3));
            column.Add(new DecimalValue(2));
            column.Add(new DecimalValue(2));
            column.Add(new DecimalValue(1));
            column.Add(NullValue.Instance);

            var (start, end) = column.SearchBoundries(new DecimalValue(0), 0, 4, default, true);
            Assert.Equal(~4, start);
            Assert.Equal(~4, end);

            (start, end) = column.SearchBoundries(new DecimalValue(1), 0, 4, default, true);
            Assert.Equal(3, start);
            Assert.Equal(3, end);

            (start, end) = column.SearchBoundries(new DecimalValue(2), 0, 4, default, true);
            Assert.Equal(1, start);
            Assert.Equal(2, end);

            (start, end) = column.SearchBoundries(new DecimalValue(3), 0, 4, default, true);
            Assert.Equal(0, start);
            Assert.Equal(0, end);

            (start, end) = column.SearchBoundries(new DecimalValue(4), 0, 4, default, true);
            Assert.Equal(-1, start);
            Assert.Equal(-1, end);

            (start, end) = column.SearchBoundries(NullValue.Instance, 0, 4, default, true);
            Assert.Equal(4, start);
            Assert.Equal(4, end);
        }

        [Fact]
        public void TestBoundarySearchWithDoubleAscending()
        {
            using Column column = Column.Create(GlobalMemoryManager.Instance);

            column.Add(new DoubleValue(1));
            column.Add(new DoubleValue(2));
            column.Add(new DoubleValue(2));
            column.Add(new DoubleValue(3));

            var (start, end) = column.SearchBoundries(new DoubleValue(0), 0, 3, default);
            Assert.Equal(-1, start);
            Assert.Equal(-1, end);

            (start, end) = column.SearchBoundries(new DoubleValue(1), 0, 3, default);
            Assert.Equal(0, start);
            Assert.Equal(0, end);

            (start, end) = column.SearchBoundries(new DoubleValue(2), 0, 3, default);
            Assert.Equal(1, start);
            Assert.Equal(2, end);

            (start, end) = column.SearchBoundries(new DoubleValue(3), 0, 3, default);
            Assert.Equal(3, start);
            Assert.Equal(3, end);

            (start, end) = column.SearchBoundries(new DoubleValue(4), 0, 3, default);
            Assert.Equal(~4, start);
            Assert.Equal(~4, end);
        }

        [Fact]
        public void TestBoundarySearchWithDoubleDescending()
        {
            using Column column = Column.Create(GlobalMemoryManager.Instance);

            column.Add(new DoubleValue(3));
            column.Add(new DoubleValue(2));
            column.Add(new DoubleValue(2));
            column.Add(new DoubleValue(1));

            var (start, end) = column.SearchBoundries(new DoubleValue(0), 0, 3, default, true);
            Assert.Equal(~4, start);
            Assert.Equal(~4, end);

            (start, end) = column.SearchBoundries(new DoubleValue(1), 0, 3, default, true);
            Assert.Equal(3, start);
            Assert.Equal(3, end);

            (start, end) = column.SearchBoundries(new DoubleValue(2), 0, 3, default, true);
            Assert.Equal(1, start);
            Assert.Equal(2, end);

            (start, end) = column.SearchBoundries(new DoubleValue(3), 0, 3, default, true);
            Assert.Equal(0, start);
            Assert.Equal(0, end);

            (start, end) = column.SearchBoundries(new DoubleValue(4), 0, 3, default, true);
            Assert.Equal(-1, start);
            Assert.Equal(-1, end);
        }

        [Fact]
        public void TestBoundarySearchWithDoubleAndNullAscending()
        {
            using Column column = Column.Create(GlobalMemoryManager.Instance);

            column.Add(NullValue.Instance);
            column.Add(new DoubleValue(1));
            column.Add(new DoubleValue(2));
            column.Add(new DoubleValue(2));
            column.Add(new DoubleValue(3));

            var (start, end) = column.SearchBoundries(NullValue.Instance, 0, 4, default);
            Assert.Equal(0, start);
            Assert.Equal(0, end);

            (start, end) = column.SearchBoundries(new DoubleValue(0), 0, 4, default);
            Assert.Equal(~1, start);
            Assert.Equal(~1, end);

            (start, end) = column.SearchBoundries(new DoubleValue(1), 0, 4, default);
            Assert.Equal(1, start);
            Assert.Equal(1, end);

            (start, end) = column.SearchBoundries(new DoubleValue(2), 0, 4, default);
            Assert.Equal(2, start);
            Assert.Equal(3, end);

            (start, end) = column.SearchBoundries(new DoubleValue(3), 0, 4, default);
            Assert.Equal(4, start);
            Assert.Equal(4, end);

            (start, end) = column.SearchBoundries(new DoubleValue(4), 0, 4, default);
            Assert.Equal(~5, start);
            Assert.Equal(~5, end);
        }

        [Fact]
        public void TestBoundarySearchWithDoubleAndNullDescending()
        {
            using Column column = Column.Create(GlobalMemoryManager.Instance);

            column.Add(new DoubleValue(3));
            column.Add(new DoubleValue(2));
            column.Add(new DoubleValue(2));
            column.Add(new DoubleValue(1));
            column.Add(NullValue.Instance);

            var (start, end) = column.SearchBoundries(new DoubleValue(0), 0, 4, default, true);
            Assert.Equal(~4, start);
            Assert.Equal(~4, end);

            (start, end) = column.SearchBoundries(new DoubleValue(1), 0, 4, default, true);
            Assert.Equal(3, start);
            Assert.Equal(3, end);

            (start, end) = column.SearchBoundries(new DoubleValue(2), 0, 4, default, true);
            Assert.Equal(1, start);
            Assert.Equal(2, end);

            (start, end) = column.SearchBoundries(new DoubleValue(3), 0, 4, default, true);
            Assert.Equal(0, start);
            Assert.Equal(0, end);

            (start, end) = column.SearchBoundries(new DoubleValue(4), 0, 4, default, true);
            Assert.Equal(-1, start);
            Assert.Equal(-1, end);

            (start, end) = column.SearchBoundries(NullValue.Instance, 0, 4, default, true);
            Assert.Equal(4, start);
            Assert.Equal(4, end);
        }

        [Fact]
        public void TestBoundarySearchWithListAscending()
        {
            using Column column = Column.Create(GlobalMemoryManager.Instance);

            column.Add(new ListValue(new Int64Value(1)));
            column.Add(new ListValue(new Int64Value(2)));
            column.Add(new ListValue(new Int64Value(2)));
            column.Add(new ListValue(new Int64Value(3)));

            var (start, end) = column.SearchBoundries(new ListValue(new Int64Value(0)), 0, 3, default);
            Assert.Equal(-1, start);
            Assert.Equal(-1, end);

            (start, end) = column.SearchBoundries(new ListValue(new Int64Value(1)), 0, 3, default);
            Assert.Equal(0, start);
            Assert.Equal(0, end);

            (start, end) = column.SearchBoundries(new ListValue(new Int64Value(2)), 0, 3, default);
            Assert.Equal(1, start);
            Assert.Equal(2, end);

            (start, end) = column.SearchBoundries(new ListValue(new Int64Value(3)), 0, 3, default);
            Assert.Equal(3, start);
            Assert.Equal(3, end);

            (start, end) = column.SearchBoundries(new ListValue(new Int64Value(4)), 0, 3, default);
            Assert.Equal(~4, start);
            Assert.Equal(~4, end);
        }

        [Fact]
        public void TestBoundarySearchWithListDescending()
        {
            using Column column = Column.Create(GlobalMemoryManager.Instance);

            column.Add(new ListValue(new Int64Value(3)));
            column.Add(new ListValue(new Int64Value(2)));
            column.Add(new ListValue(new Int64Value(2)));
            column.Add(new ListValue(new Int64Value(1)));

            var (start, end) = column.SearchBoundries(new ListValue(new Int64Value(0)), 0, 3, default, true);
            Assert.Equal(~4, start);
            Assert.Equal(~4, end);

            (start, end) = column.SearchBoundries(new ListValue(new Int64Value(1)), 0, 3, default, true);
            Assert.Equal(3, start);
            Assert.Equal(3, end);

            (start, end) = column.SearchBoundries(new ListValue(new Int64Value(2)), 0, 3, default, true);
            Assert.Equal(1, start);
            Assert.Equal(2, end);

            (start, end) = column.SearchBoundries(new ListValue(new Int64Value(3)), 0, 3, default, true);
            Assert.Equal(0, start);
            Assert.Equal(0, end);

            (start, end) = column.SearchBoundries(new ListValue(new Int64Value(4)), 0, 3, default, true);
            Assert.Equal(-1, start);
            Assert.Equal(-1, end);
        }

        [Fact]
        public void TestBoundarySearchWithListAndNullAscending()
        {
            using Column column = Column.Create(GlobalMemoryManager.Instance);

            column.Add(NullValue.Instance);
            column.Add(new ListValue(new Int64Value(1)));
            column.Add(new ListValue(new Int64Value(2)));
            column.Add(new ListValue(new Int64Value(2)));
            column.Add(new ListValue(new Int64Value(3)));

            var (start, end) = column.SearchBoundries(NullValue.Instance, 0, 4, default);
            Assert.Equal(0, start);
            Assert.Equal(0, end);

            (start, end) = column.SearchBoundries(new ListValue(new Int64Value(0)), 0, 4, default);
            Assert.Equal(~1, start);
            Assert.Equal(~1, end);

            (start, end) = column.SearchBoundries(new ListValue(new Int64Value(1)), 0, 4, default);
            Assert.Equal(1, start);
            Assert.Equal(1, end);

            (start, end) = column.SearchBoundries(new ListValue(new Int64Value(2)), 0, 4, default);
            Assert.Equal(2, start);
            Assert.Equal(3, end);

            (start, end) = column.SearchBoundries(new ListValue(new Int64Value(3)), 0, 4, default);
            Assert.Equal(4, start);
            Assert.Equal(4, end);

            (start, end) = column.SearchBoundries(new ListValue(new Int64Value(4)), 0, 4, default);
            Assert.Equal(~5, start);
            Assert.Equal(~5, end);
        }

        [Fact]
        public void TestBoundarySearchWithListAndNullDescending()
        {
            using Column column = Column.Create(GlobalMemoryManager.Instance);

            column.Add(new ListValue(new Int64Value(3)));
            column.Add(new ListValue(new Int64Value(2)));
            column.Add(new ListValue(new Int64Value(2)));
            column.Add(new ListValue(new Int64Value(1)));
            column.Add(NullValue.Instance);

            var (start, end) = column.SearchBoundries(new ListValue(new Int64Value(0)), 0, 4, default, true);
            Assert.Equal(~4, start);
            Assert.Equal(~4, end);

            (start, end) = column.SearchBoundries(new ListValue(new Int64Value(1)), 0, 4, default, true);
            Assert.Equal(3, start);
            Assert.Equal(3, end);

            (start, end) = column.SearchBoundries(new ListValue(new Int64Value(2)), 0, 4, default, true);
            Assert.Equal(1, start);
            Assert.Equal(2, end);

            (start, end) = column.SearchBoundries(new ListValue(new Int64Value(3)), 0, 4, default, true);
            Assert.Equal(0, start);
            Assert.Equal(0, end);

            (start, end) = column.SearchBoundries(new ListValue(new Int64Value(4)), 0, 4, default, true);
            Assert.Equal(-1, start);
            Assert.Equal(-1, end);

            (start, end) = column.SearchBoundries(NullValue.Instance, 0, 4, default, true);
            Assert.Equal(4, start);
            Assert.Equal(4, end);
        }

        [Fact]
        public void TestBoundarySearchWithMapsAscending()
        {
            using Column column = Column.Create(GlobalMemoryManager.Instance);

            column.Add(new MapValue(new KeyValuePair<IDataValue, IDataValue>(new Int64Value(1), new Int64Value(1))));
            column.Add(new MapValue(new KeyValuePair<IDataValue, IDataValue>(new Int64Value(1), new Int64Value(2))));
            column.Add(new MapValue(new KeyValuePair<IDataValue, IDataValue>(new Int64Value(1), new Int64Value(2))));
            column.Add(new MapValue(new KeyValuePair<IDataValue, IDataValue>(new Int64Value(1), new Int64Value(3))));

            var (start, end) = column.SearchBoundries(new MapValue(new KeyValuePair<IDataValue, IDataValue>(new Int64Value(1), new Int64Value(0))), 0, 3, default);
            Assert.Equal(-1, start);
            Assert.Equal(-1, end);

            (start, end) = column.SearchBoundries(new MapValue(new KeyValuePair<IDataValue, IDataValue>(new Int64Value(1), new Int64Value(1))), 0, 3, default);
            Assert.Equal(0, start);
            Assert.Equal(0, end);

            (start, end) = column.SearchBoundries(new MapValue(new KeyValuePair<IDataValue, IDataValue>(new Int64Value(1), new Int64Value(2))), 0, 3, default);
            Assert.Equal(1, start);
            Assert.Equal(2, end);

            (start, end) = column.SearchBoundries(new MapValue(new KeyValuePair<IDataValue, IDataValue>(new Int64Value(1), new Int64Value(3))), 0, 3, default);
            Assert.Equal(3, start);
            Assert.Equal(3, end);

            (start, end) = column.SearchBoundries(new MapValue(new KeyValuePair<IDataValue, IDataValue>(new Int64Value(1), new Int64Value(4))), 0, 3, default);
            Assert.Equal(~4, start);
            Assert.Equal(~4, end);
        }

        [Fact]
        public void TestBoundarySearchWithMapsDescending()
        {
            using Column column = Column.Create(GlobalMemoryManager.Instance);

            column.Add(new MapValue(new KeyValuePair<IDataValue, IDataValue>(new Int64Value(1), new Int64Value(3))));
            column.Add(new MapValue(new KeyValuePair<IDataValue, IDataValue>(new Int64Value(1), new Int64Value(2))));
            column.Add(new MapValue(new KeyValuePair<IDataValue, IDataValue>(new Int64Value(1), new Int64Value(2))));
            column.Add(new MapValue(new KeyValuePair<IDataValue, IDataValue>(new Int64Value(1), new Int64Value(1))));

            var (start, end) = column.SearchBoundries(new MapValue(new KeyValuePair<IDataValue, IDataValue>(new Int64Value(1), new Int64Value(0))), 0, 3, default, true);
            Assert.Equal(~4, start);
            Assert.Equal(~4, end);

            (start, end) = column.SearchBoundries(new MapValue(new KeyValuePair<IDataValue, IDataValue>(new Int64Value(1), new Int64Value(1))), 0, 3, default, true);
            Assert.Equal(3, start);
            Assert.Equal(3, end);

            (start, end) = column.SearchBoundries(new MapValue(new KeyValuePair<IDataValue, IDataValue>(new Int64Value(1), new Int64Value(2))), 0, 3, default, true);
            Assert.Equal(1, start);
            Assert.Equal(2, end);

            (start, end) = column.SearchBoundries(new MapValue(new KeyValuePair<IDataValue, IDataValue>(new Int64Value(1), new Int64Value(3))), 0, 3, default, true);
            Assert.Equal(0, start);
            Assert.Equal(0, end);

            (start, end) = column.SearchBoundries(new MapValue(new KeyValuePair<IDataValue, IDataValue>(new Int64Value(1), new Int64Value(4))), 0, 3, default, true);
            Assert.Equal(-1, start);
            Assert.Equal(-1, end);
        }

        [Fact]
        public void TestBoundarySearchWithMapsAndNullAscending()
        {
            using Column column = Column.Create(GlobalMemoryManager.Instance);

            column.Add(NullValue.Instance);
            column.Add(new MapValue(new KeyValuePair<IDataValue, IDataValue>(new Int64Value(1), new Int64Value(1))));
            column.Add(new MapValue(new KeyValuePair<IDataValue, IDataValue>(new Int64Value(1), new Int64Value(2))));
            column.Add(new MapValue(new KeyValuePair<IDataValue, IDataValue>(new Int64Value(1), new Int64Value(2))));
            column.Add(new MapValue(new KeyValuePair<IDataValue, IDataValue>(new Int64Value(1), new Int64Value(3))));

            var (start, end) = column.SearchBoundries(NullValue.Instance, 0, 4, default);
            Assert.Equal(0, start);
            Assert.Equal(0, end);

            (start, end) = column.SearchBoundries(new MapValue(new KeyValuePair<IDataValue, IDataValue>(new Int64Value(1), new Int64Value(0))), 0, 4, default);
            Assert.Equal(~1, start);
            Assert.Equal(~1, end);

            (start, end) = column.SearchBoundries(new MapValue(new KeyValuePair<IDataValue, IDataValue>(new Int64Value(1), new Int64Value(1))), 0, 4, default);
            Assert.Equal(1, start);
            Assert.Equal(1, end);

            (start, end) = column.SearchBoundries(new MapValue(new KeyValuePair<IDataValue, IDataValue>(new Int64Value(1), new Int64Value(2))), 0, 4, default);
            Assert.Equal(2, start);
            Assert.Equal(3, end);

            (start, end) = column.SearchBoundries(new MapValue(new KeyValuePair<IDataValue, IDataValue>(new Int64Value(1), new Int64Value(3))), 0, 4, default);
            Assert.Equal(4, start);
            Assert.Equal(4, end);

            (start, end) = column.SearchBoundries(new MapValue(new KeyValuePair<IDataValue, IDataValue>(new Int64Value(1), new Int64Value(4))), 0, 4, default);
            Assert.Equal(~5, start);
            Assert.Equal(~5, end);
        }

        [Fact]
        public void TestBoundarySearchWithMapsAndNullDescending()
        {
            using Column column = Column.Create(GlobalMemoryManager.Instance);

            column.Add(new MapValue(new KeyValuePair<IDataValue, IDataValue>(new Int64Value(1), new Int64Value(3))));
            column.Add(new MapValue(new KeyValuePair<IDataValue, IDataValue>(new Int64Value(1), new Int64Value(2))));
            column.Add(new MapValue(new KeyValuePair<IDataValue, IDataValue>(new Int64Value(1), new Int64Value(2))));
            column.Add(new MapValue(new KeyValuePair<IDataValue, IDataValue>(new Int64Value(1), new Int64Value(1))));
            column.Add(NullValue.Instance);

            var (start, end) = column.SearchBoundries(new MapValue(new KeyValuePair<IDataValue, IDataValue>(new Int64Value(1), new Int64Value(0))), 0, 4, default, true);
            Assert.Equal(~4, start);
            Assert.Equal(~4, end);

            (start, end) = column.SearchBoundries(new MapValue(new KeyValuePair<IDataValue, IDataValue>(new Int64Value(1), new Int64Value(1))), 0, 4, default, true);
            Assert.Equal(3, start);
            Assert.Equal(3, end);

            (start, end) = column.SearchBoundries(new MapValue(new KeyValuePair<IDataValue, IDataValue>(new Int64Value(1), new Int64Value(2))), 0, 4, default, true);
            Assert.Equal(1, start);
            Assert.Equal(2, end);

            (start, end) = column.SearchBoundries(new MapValue(new KeyValuePair<IDataValue, IDataValue>(new Int64Value(1), new Int64Value(3))), 0, 4, default, true);
            Assert.Equal(0, start);
            Assert.Equal(0, end);

            (start, end) = column.SearchBoundries(new MapValue(new KeyValuePair<IDataValue, IDataValue>(new Int64Value(1), new Int64Value(4))), 0, 4, default, true);
            Assert.Equal(-1, start);
            Assert.Equal(-1, end);

            (start, end) = column.SearchBoundries(NullValue.Instance, 0, 4, default, true);
            Assert.Equal(4, start);
            Assert.Equal(4, end);
        }

        [Fact]
        public void TestBoundarySearchWithUnionAscending()
        {
            using Column column = Column.Create(GlobalMemoryManager.Instance);

            column.Add(new Int64Value(1));
            column.Add(new StringValue("a"));
            column.Add(new StringValue("a"));
            column.Add(new DecimalValue(3));

            var (start, end) = column.SearchBoundries(new Int64Value(0), 0, 3, default);
            Assert.Equal(-1, start);
            Assert.Equal(-1, end);

            (start, end) = column.SearchBoundries(new Int64Value(1), 0, 3, default);
            Assert.Equal(0, start);
            Assert.Equal(0, end);

            (start, end) = column.SearchBoundries(new StringValue("a"), 0, 3, default);
            Assert.Equal(1, start);
            Assert.Equal(2, end);

            (start, end) = column.SearchBoundries(new DecimalValue(3), 0, 3, default);
            Assert.Equal(3, start);
            Assert.Equal(3, end);

            (start, end) = column.SearchBoundries(new DecimalValue(4), 0, 3, default);
            Assert.Equal(~4, start);
            Assert.Equal(~4, end);
        }

        [Fact]
        public void TestBoundarySearchWithUnionDescending()
        {
            using Column column = Column.Create(GlobalMemoryManager.Instance);

            column.Add(new DecimalValue(3));
            column.Add(new StringValue("a"));
            column.Add(new StringValue("a"));
            column.Add(new Int64Value(1));

            var (start, end) = column.SearchBoundries(new Int64Value(0), 0, 3, default, true);
            Assert.Equal(~4, start);
            Assert.Equal(~4, end);

            (start, end) = column.SearchBoundries(new Int64Value(1), 0, 3, default, true);
            Assert.Equal(3, start);
            Assert.Equal(3, end);

            (start, end) = column.SearchBoundries(new StringValue("a"), 0, 3, default, true);
            Assert.Equal(1, start);
            Assert.Equal(2, end);

            (start, end) = column.SearchBoundries(new DecimalValue(3), 0, 3, default, true);
            Assert.Equal(0, start);
            Assert.Equal(0, end);

            (start, end) = column.SearchBoundries(new DecimalValue(4), 0, 3, default, true);
            Assert.Equal(-1, start);
            Assert.Equal(-1, end);
        }

        [Fact]
        public void TestBoundarySearchWithUnionAndNullAscending()
        {
            using Column column = Column.Create(GlobalMemoryManager.Instance);

            column.Add(NullValue.Instance);
            column.Add(new Int64Value(1));
            column.Add(new StringValue("a"));
            column.Add(new StringValue("a"));
            column.Add(new DecimalValue(3));

            var (start, end) = column.SearchBoundries(NullValue.Instance, 0, 4, default);
            Assert.Equal(0, start);
            Assert.Equal(0, end);

            (start, end) = column.SearchBoundries(new Int64Value(0), 0, 4, default);
            Assert.Equal(~1, start);
            Assert.Equal(~1, end);

            (start, end) = column.SearchBoundries(new Int64Value(1), 0, 4, default);
            Assert.Equal(1, start);
            Assert.Equal(1, end);

            (start, end) = column.SearchBoundries(new StringValue("a"), 0, 4, default);
            Assert.Equal(2, start);
            Assert.Equal(3, end);

            (start, end) = column.SearchBoundries(new DecimalValue(3), 0, 4, default);
            Assert.Equal(4, start);
            Assert.Equal(4, end);

            (start, end) = column.SearchBoundries(new DecimalValue(4), 0, 4, default);
            Assert.Equal(~5, start);
            Assert.Equal(~5, end);
        }

        [Fact]
        public void TestBoundarySearchWithUnionAndNullDescending()
        {
            using Column column = Column.Create(GlobalMemoryManager.Instance);

            column.Add(new DecimalValue(3));
            column.Add(new StringValue("a"));
            column.Add(new StringValue("a"));
            column.Add(new Int64Value(1));
            column.Add(NullValue.Instance);

            var (start, end) = column.SearchBoundries(new Int64Value(0), 0, 4, default, true);
            Assert.Equal(~4, start);
            Assert.Equal(~4, end);

            (start, end) = column.SearchBoundries(new Int64Value(1), 0, 4, default, true);
            Assert.Equal(3, start);
            Assert.Equal(3, end);

            (start, end) = column.SearchBoundries(new StringValue("a"), 0, 4, default, true);
            Assert.Equal(1, start);
            Assert.Equal(2, end);

            (start, end) = column.SearchBoundries(new DecimalValue(3), 0, 4, default, true);
            Assert.Equal(0, start);
            Assert.Equal(0, end);

            (start, end) = column.SearchBoundries(new DecimalValue(4), 0, 4, default, true);
            Assert.Equal(-1, start);
            Assert.Equal(-1, end);

            (start, end) = column.SearchBoundries(NullValue.Instance, 0, 4, default, true);
            Assert.Equal(4, start);
            Assert.Equal(4, end);
        }
    }
}
