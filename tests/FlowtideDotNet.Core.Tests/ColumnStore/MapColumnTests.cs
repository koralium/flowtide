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
using FlowtideDotNet.Substrait.Expressions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.Tests.ColumnStore
{
    public class MapColumnTests
    {
        [Fact]
        public void FetchSubProperty()
        {
            Column column = new Column(new BatchMemoryManager(1));
            column.Add(new MapValue(new Dictionary<IDataValue, IDataValue>()
            {
                { new StringValue("key"), new Int64Value(1) },
                { new StringValue("value"), new StringValue("hello1") }
            }));
            column.Add(NullValue.Instance);

            var valueData = column.GetValueAt(0, new MapKeyReferenceSegment() { Key = "value" });
            var keyData = column.GetValueAt(0, new MapKeyReferenceSegment() { Key = "key" });

            Assert.Equal("hello1", valueData.ToString());
            Assert.Equal(1, keyData.AsLong);

            var nullData = column.GetValueAt(1, new MapKeyReferenceSegment() { Key = "value" });
            Assert.Equal(ArrowTypeId.Null, nullData.Type);

            var notFoundPropertyData = column.GetValueAt(0, new MapKeyReferenceSegment() { Key = "notFound" });
            Assert.Equal(ArrowTypeId.Null, notFoundPropertyData.Type);
        }

        [Fact]
        public void UpdateFirstElement()
        {
            Column column = new Column(new BatchMemoryManager(1));
            column.Add(new MapValue(new Dictionary<IDataValue, IDataValue>()
            {
                { new StringValue("key"), new Int64Value(1) },
                { new StringValue("value"), new StringValue("hello1") }
            }));
            column.Add(new MapValue(new Dictionary<IDataValue, IDataValue>()
            {
                { new StringValue("key"), new Int64Value(2) },
                { new StringValue("value"), new StringValue("hello2") }
            }));

            column.UpdateAt(0, new MapValue(new Dictionary<IDataValue, IDataValue>()
            {
                { new StringValue("key"), new Int64Value(3) },
                { new StringValue("value"), new StringValue("hello3") }
            }));

            var valueData = column.GetValueAt(0, new MapKeyReferenceSegment() { Key = "value" });
            Assert.Equal("hello3", valueData.ToString());
        }
    }
}
