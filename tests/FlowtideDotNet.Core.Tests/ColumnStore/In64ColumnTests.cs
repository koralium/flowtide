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

namespace FlowtideDotNet.Core.Tests.ColumnStore
{
    public class In64ColumnTests
    {

        [Fact]
        public void TestAddAndGetByIndex()
        {
            var column = new Int64Column();
            int i1 = column.Add(new Int64Value(1));
            int i2 = column.Add(new Int64Value(3));
            int i3 = column.Add(new Int64Value(2));

            Assert.Equal(0, i1);
            Assert.Equal(1, i2);
            Assert.Equal(2, i3);

            Assert.Equal(1, column.GetValueAt(i1).AsLong);
            Assert.Equal(3, column.GetValueAt(i2).AsLong);
            Assert.Equal(2, column.GetValueAt(i3).AsLong);
        }

        [Fact]
        public void TestSearchBoundries()
        {
            var c1 = new Int64Column();
            c1.Add(new Int64Value(1));
            c1.Add(new Int64Value(1));
            c1.Add(new Int64Value(1));
            c1.Add(new Int64Value(2));
            var (start, end) = c1.SearchBoundries(new Int64Value(1), 0, 4);
            Assert.Equal(0, start);
            Assert.Equal(2, end);

            var column = new Int64Column();
            column.Add(new Int64Value(1));
            column.Add(new Int64Value(2));
            column.Add(new Int64Value(2));
            column.Add(new Int64Value(2));
            column.Add(new Int64Value(2));
            column.Add(new Int64Value(3));
            column.Add(new Int64Value(4));
            column.Add(new Int64Value(4));
            column.Add(new Int64Value(5));
            column.Add(new Int64Value(6));
            column.Add(new Int64Value(7));

            (start, end) = column.SearchBoundries(new Int64Value(2), 0, 9);
            Assert.Equal(1, start);
            Assert.Equal(4, end);

            (start, end) = column.SearchBoundries(new Int64Value(3), 0, 9);
            Assert.Equal(5, start);
            Assert.Equal(5, end);

            (start, end) = column.SearchBoundries(new Int64Value(4), 0, 9);
            Assert.Equal(6, start);
            Assert.Equal(7, end);

            (start, end) = column.SearchBoundries(new Int64Value(9), 0, 9);
            Assert.Equal(~9, start);
            Assert.Equal(~9, end);

            var emptyColumn = new Int64Column();
            (start, end) = emptyColumn.SearchBoundries(new Int64Value(4), 0, 0);
            Assert.Equal(~0, start);
            Assert.Equal(~0, end);
        }

        [Fact]
        public void TestCompareTo()
        {
            var column = new Int64Column();
            column.Add(new Int64Value(0));
            column.Add(new Int64Value(1));
            column.Add(new Int64Value(2));
            Assert.Equal(-1, column.CompareTo(0, new Int64Value(1)));
            Assert.Equal(0, column.CompareTo(1, new Int64Value(1)));
            Assert.Equal(1, column.CompareTo(2, new Int64Value(1)));
        }
    }
}
