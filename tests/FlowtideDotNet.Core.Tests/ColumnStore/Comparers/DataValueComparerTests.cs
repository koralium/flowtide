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
using FlowtideDotNet.Core.ColumnStore.Comparers;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.Tests.ColumnStore.Comparers
{
    public class DataValueComparerTests
    {
        [Fact]
        public void NullComparedToNullEqualsZero()
        {
            DataValueComparer dataValueComparer = new DataValueComparer();

            var result = dataValueComparer.Compare(NullValue.Instance, NullValue.Instance);
            Assert.Equal(0, result);
        }

        [Fact]
        public void BoolTrueComparedToTrueEqualsZero()
        {
            DataValueComparer dataValueComparer = new DataValueComparer();

            var result = dataValueComparer.Compare(BoolValue.True, BoolValue.True);
            Assert.Equal(0, result);
        }

        [Fact]
        public void BoolTrueComparedToFalseEqualsOne()
        {
            DataValueComparer dataValueComparer = new DataValueComparer();

            var result = dataValueComparer.Compare(BoolValue.True, BoolValue.False);
            Assert.Equal(1, result);
        }

        [Fact]
        public void BoolFalseComparedToTrueEqualsNegativeOne()
        {
            DataValueComparer dataValueComparer = new DataValueComparer();

            var result = dataValueComparer.Compare(BoolValue.False, BoolValue.True);
            Assert.Equal(-1, result);
        }

        [Fact]
        public void Int64ComparedToInt64()
        {
            DataValueComparer dataValueComparer = new DataValueComparer();

            var result = dataValueComparer.Compare(new Int64Value(1), new Int64Value(2));
            Assert.Equal(-1, result);
        }

        [Fact]
        public void DoubleComparedToDouble()
        {
            DataValueComparer dataValueComparer = new DataValueComparer();

            var result = dataValueComparer.Compare(new DoubleValue(1.0), new DoubleValue(2.0));
            Assert.Equal(-1, result);
        }

        [Fact]
        public void StringComparedToString()
        {
            DataValueComparer dataValueComparer = new DataValueComparer();

            var result = dataValueComparer.Compare(new StringValue("a"), new StringValue("b"));
            Assert.Equal(-1, result);
        }

        [Fact]
        public void BinaryComparedToBinary()
        {
            DataValueComparer dataValueComparer = new DataValueComparer();

            var result = dataValueComparer.Compare(new BinaryValue(new byte[] { 1 }), new BinaryValue(new byte[] { 2 }));
            Assert.Equal(-1, result);
        }

        [Fact]
        public void DecimalComparedToDecimal()
        {
            DataValueComparer dataValueComparer = new DataValueComparer();

            var result = dataValueComparer.Compare(new DecimalValue(1.0m), new DecimalValue(2.0m));
            Assert.Equal(-1, result);
        }
    }
}
