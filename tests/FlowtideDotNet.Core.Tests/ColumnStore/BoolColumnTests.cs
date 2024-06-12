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
    public class BoolColumnTests
    {
        [Fact]
        public void TestAddAndGetByIndex()
        {
            var column = new BoolColumn();
            int i1 = column.Add(new BoolValue(true));
            int i2 = column.Add(new BoolValue(false));
            int i3 = column.Add(new BoolValue(true));

            Assert.Equal(0, i1);
            Assert.Equal(1, i2);
            Assert.Equal(2, i3);

            Assert.True(column.GetValueAt(i1).AsBool);
            Assert.False(column.GetValueAt(i2).AsBool);
            Assert.True(column.GetValueAt(i3).AsBool);
        }

        [Fact]
        public void TestSearchBoundries()
        {
            var column = new BoolColumn();
            column.Add(new BoolValue(false));
            column.Add(new BoolValue(false));
            column.Add(new BoolValue(false));
            column.Add(new BoolValue(true));

            var (start, end) = column.SearchBoundries(new BoolValue(false), 0, 4);
            Assert.Equal(0, start);
            Assert.Equal(2, end);

            (start, end) = column.SearchBoundries(new BoolValue(true), 0, 4);
            Assert.Equal(3, start);
            Assert.Equal(3, end);

            var emptyColumn = new BoolColumn();
            (start, end) = emptyColumn.SearchBoundries(new BoolValue(true), 0, 0);
            Assert.Equal(~0, start);
            Assert.Equal(~0, end);
        }

        [Fact]
        public void TestCompareTo()
        {
            var column = new BoolColumn();
            column.Add(new BoolValue(false));
            column.Add(new BoolValue(true));
            Assert.Equal(-1, column.CompareTo(0, new BoolValue(true)));
            Assert.Equal(0, column.CompareTo(0, new BoolValue(false)));
            Assert.Equal(0, column.CompareTo(1, new BoolValue(true)));
            Assert.Equal(1, column.CompareTo(1, new BoolValue(false)));
        }
    }
}
