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
using FlowtideDotNet.Storage.Memory;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.Tests.ColumnStore
{
    public class ColumnCompareToTests
    {

        private int Compare<T1, T2>(T1 t1, T2 t2, IDataValue? col1Extra = default, IDataValue? col2Extra = default)
            where T1 : IDataValue
            where T2 : IDataValue
        {
            Column c1 = new Column(GlobalMemoryManager.Instance);
            Column c2 = new Column(GlobalMemoryManager.Instance);

            c1.Add(t1);
            c2.Add(t2);

            if (col1Extra != null)
            {
                c1.Add(col1Extra);
            }
            if (col2Extra != null)
            {
                c2.Add(col2Extra);
            }

            var comp = c1.CompareTo(c2, 0, 0);
            var compReverse = c2.CompareTo(c1, 0, 0);

            if (comp == 0 && compReverse != 0)
            {
                Assert.Fail();
            }
            if (compReverse == 0 && comp != 0)
            {
                Assert.Fail();
            }
            if (comp < 0 && compReverse < 0)
            {
                Assert.Fail();
            }
            if (comp > 0 && compReverse > 0)
            {
                Assert.Fail();
            }

            var dataValComp = DataValueComparer.Instance.Compare(t1, t2);

            if (comp != dataValComp)
            {
                Assert.Fail();
            }


            return comp;
        }

        [Fact]
        public void CompareIntString()
        {
            var c = Compare(new Int64Value(1), new StringValue("hello"));
            Assert.True(c < 0);
        }

        [Fact]
        public void CompareIntInt()
        {
            var c = Compare(new Int64Value(1), new Int64Value(2));
            Assert.True(c < 0);
        }

        [Fact]
        public void CompareBoolInt()
        {
            var c = Compare(new BoolValue(true), new Int64Value(1));
            Assert.True(c < 0);
        }

        [Fact]
        public void CompareNullIntColToString()
        {
            var c = Compare(NullValue.Instance, new StringValue("hello"), new Int64Value(1));
            Assert.True(c < 0);
        }

        [Fact]
        public void CompareIntUnionColToString()
        {
            var c = Compare(new Int64Value(1), new StringValue("hello"), new StringValue("world"));
            Assert.True(c < 0);
        }
    }
}
