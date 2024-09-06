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
using FlowtideDotNet.Core.ColumnStore.Memory;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.Tests.ColumnStore
{
    public class BoundarySearchTests
    {
        [Fact]
        public void TestBoundarySearchWithIntegersAscending()
        {
            Column column = Column.Create(GlobalMemoryManager.Instance);

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
            Column column = Column.Create(GlobalMemoryManager.Instance);

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
            Column column = Column.Create(GlobalMemoryManager.Instance);

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
            Column column = Column.Create(GlobalMemoryManager.Instance);

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
            Column column = Column.Create(GlobalMemoryManager.Instance);

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
            Column column = Column.Create(GlobalMemoryManager.Instance);

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
            Column column = Column.Create(GlobalMemoryManager.Instance);

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
            Column column = Column.Create(GlobalMemoryManager.Instance);

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
            Column column = Column.Create(GlobalMemoryManager.Instance);

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
            Column column = Column.Create(GlobalMemoryManager.Instance);

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
            Column column = Column.Create(GlobalMemoryManager.Instance);

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
            Column column = Column.Create(GlobalMemoryManager.Instance);

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
            Column column = Column.Create(GlobalMemoryManager.Instance);

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
            Column column = Column.Create(GlobalMemoryManager.Instance);

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
            Column column = Column.Create(GlobalMemoryManager.Instance);

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
            Column column = Column.Create(GlobalMemoryManager.Instance);

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
            Column column = Column.Create(GlobalMemoryManager.Instance);

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
            Column column = Column.Create(GlobalMemoryManager.Instance);

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
            Column column = Column.Create(GlobalMemoryManager.Instance);

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
            Column column = Column.Create(GlobalMemoryManager.Instance);

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
            Column column = Column.Create(GlobalMemoryManager.Instance);

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
            Column column = Column.Create(GlobalMemoryManager.Instance);

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
            Column column = Column.Create(GlobalMemoryManager.Instance);

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
            Column column = Column.Create(GlobalMemoryManager.Instance);

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
            Column column = Column.Create(GlobalMemoryManager.Instance);

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
            Column column = Column.Create(GlobalMemoryManager.Instance);

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
            Column column = Column.Create(GlobalMemoryManager.Instance);

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
            Column column = Column.Create(GlobalMemoryManager.Instance);

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
            Column column = Column.Create(GlobalMemoryManager.Instance);

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
            Column column = Column.Create(GlobalMemoryManager.Instance);

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
            Column column = Column.Create(GlobalMemoryManager.Instance);

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
            Column column = Column.Create(GlobalMemoryManager.Instance);

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
            Column column = Column.Create(GlobalMemoryManager.Instance);

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
            Column column = Column.Create(GlobalMemoryManager.Instance);

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
            Column column = Column.Create(GlobalMemoryManager.Instance);

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
            Column column = Column.Create(GlobalMemoryManager.Instance);

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
