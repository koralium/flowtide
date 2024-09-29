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
using System.Threading.Tasks;
using Xunit;

namespace FlowtideDotNet.Core.Tests.ColumnStore
{
    public class ColumnTests
    {
        [Fact]
        public void TestColumnAddInt64()
        {
            Column column = new Column(GlobalMemoryManager.Instance);
            column.Add(new Int64Value(1));
            column.Add(new Int64Value(2));

            Assert.Equal(1, column.GetValueAt(0, default).AsLong);
            Assert.Equal(2, column.GetValueAt(1, default).AsLong);
        }

        [Fact]
        public void TestColumnAddNullThenInt64()
        {
            Column column = new Column(GlobalMemoryManager.Instance);
            column.Add(new NullValue());
            column.Add(new Int64Value(1));
            column.Add(new Int64Value(2));

            Assert.True(column.GetValueAt(0, default).Type == ArrowTypeId.Null);
            Assert.Equal(1, column.GetValueAt(1, default).AsLong);
            Assert.Equal(2, column.GetValueAt(2, default).AsLong);
        }

        [Fact]
        public void InsertInt64()
        {
            Column column = new Column(GlobalMemoryManager.Instance);
            column.InsertAt(0, new Int64Value(1));

            Assert.Equal(1, column.GetValueAt(0, default).AsLong);
        }

        [Fact]
        public void UpdateValueToNull()
        {
            Column column = new Column(GlobalMemoryManager.Instance);
            column.InsertAt(0, new Int64Value(1));
            column.UpdateAt(0, new NullValue());
            
            Assert.True(column.GetValueAt(0, default).Type == ArrowTypeId.Null);
        }

        [Fact]
        public void TestColumnAddNullInsertLocation0()
        {
            Column column = new Column(GlobalMemoryManager.Instance);
            column.Add(new NullValue());
            column.InsertAt(0, new Int64Value(1));

            Assert.Equal(1, column.GetValueAt(0, default).AsLong);
            Assert.True(column.GetValueAt(1, default).Type == ArrowTypeId.Null);
        }

        [Fact]
        public void Int64ToArrow()
        {
            Column column = new Column(GlobalMemoryManager.Instance);
            column.Add(new Int64Value(1));
            column.Add(NullValue.Instance);
            column.Add(new Int64Value(2));

            var result = column.ToArrowArray();
            var arrowArray = (Apache.Arrow.Int64Array)result.Item1;
            Assert.Equal(1, arrowArray.GetValue(0));
            Assert.Null(arrowArray.GetValue(1));
            Assert.Equal(2, arrowArray.GetValue(2));
        }

        [Fact]
        public void Int64NullFirstToArrow()
        {
            Column column = new Column(GlobalMemoryManager.Instance);
            column.Add(NullValue.Instance);
            column.Add(new Int64Value(1));
            column.Add(new Int64Value(2));

            var result = column.ToArrowArray();
            var arrowArray = (Apache.Arrow.Int64Array)result.Item1;
            Assert.Null(arrowArray.GetValue(0));
            Assert.Equal(1, arrowArray.GetValue(1));
            Assert.Equal(2, arrowArray.GetValue(2));
        }

        [Fact]
        public void StringToArrow()
        {
            Column column = new Column(GlobalMemoryManager.Instance);
            column.Add(new StringValue("1"));
            column.Add(NullValue.Instance);
            column.Add(new StringValue("2"));

            var result = column.ToArrowArray();
            var arrowArray = (Apache.Arrow.StringArray)result.Item1;
            Assert.Equal("1", arrowArray.GetString(0));
            Assert.Null(arrowArray.GetString(1));
            Assert.Equal("2", arrowArray.GetString(2));
        }

        [Fact]
        public void StringToArrowNullFirst()
        {
            Column column = new Column(GlobalMemoryManager.Instance);
            column.Add(NullValue.Instance);
            column.Add(new StringValue("1"));
            column.Add(new StringValue("2"));

            var result = column.ToArrowArray();
            var arrowArray = (Apache.Arrow.StringArray)result.Item1;
            Assert.Null(arrowArray.GetString(0));
            Assert.Equal("1", arrowArray.GetString(1));
            Assert.Equal("2", arrowArray.GetString(2));
        }
    }
}
