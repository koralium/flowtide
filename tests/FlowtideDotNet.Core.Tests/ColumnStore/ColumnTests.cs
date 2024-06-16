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
            Column column = new Column();
            column.Add(new Int64Value(1));
            column.Add(new Int64Value(2));

            Assert.Equal(1, column.GetValueAt(0).AsLong);
            Assert.Equal(2, column.GetValueAt(1).AsLong);
        }

        [Fact]
        public void TestColumnAddNullThenInt64()
        {
            Column column = new Column();
            column.Add(new NullValue());
            column.Add(new Int64Value(1));
            column.Add(new Int64Value(2));

            Assert.True(column.GetValueAt(0).Type == ArrowTypeId.Null);
            Assert.Equal(1, column.GetValueAt(1).AsLong);
            Assert.Equal(2, column.GetValueAt(2).AsLong);
        }

        [Fact]
        public void InsertInt64()
        {
            Column column = new Column();
            column.InsertAt(0, new Int64Value(1));

            Assert.Equal(1, column.GetValueAt(0).AsLong);
        }

        [Fact]
        public void UpdateValueToNull()
        {
            Column column = new Column();
            column.InsertAt(0, new Int64Value(1));
            column.UpdateAt(0, new NullValue());
            
            Assert.True(column.GetValueAt(0).Type == ArrowTypeId.Null);
        }

        [Fact]
        public void TestColumnAddNullInsertLocation0()
        {
            Column column = new Column();
            column.Add(new NullValue());
            column.InsertAt(0, new Int64Value(1));

            Assert.Equal(1, column.GetValueAt(0).AsLong);
            Assert.True(column.GetValueAt(1).Type == ArrowTypeId.Null);
        }

        [Fact]
        public void Int64ToArrow()
        {
            Column column = new Column();
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
            Column column = new Column();
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
            Column column = new Column();
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
            Column column = new Column();
            column.Add(NullValue.Instance);
            column.Add(new StringValue("1"));
            column.Add(new StringValue("2"));

            var result = column.ToArrowArray();
            var arrowArray = (Apache.Arrow.StringArray)result.Item1;
            Assert.Null(arrowArray.GetString(0));
            Assert.Equal("1", arrowArray.GetString(1));
            Assert.Equal("2", arrowArray.GetString(2));
        }

        [Fact]
        public void ListToArrow()
        {
            Column column = new Column();
            column.Add(new ListValue(new List<IDataValue>()
            {
                new StringValue("1"),
                new StringValue("2")
            }));
            column.Add(NullValue.Instance);
            column.Add(new ListValue(new List<IDataValue>()
            {
                new StringValue("3"),
                new StringValue("4")
            }));

            var result = column.ToArrowArray();
            var arrowArray = (Apache.Arrow.ListArray)result.Item1;

            var slice1 = (Apache.Arrow.StringArray)arrowArray.GetSlicedValues(0);
            Assert.Equal(2, slice1.Length);
            Assert.Equal("1", slice1.GetString(0));
            Assert.Equal("2", slice1.GetString(1));

            var nullSlice = arrowArray.GetSlicedValues(1);
            Assert.Null(nullSlice);

            var slice2 = (Apache.Arrow.StringArray)arrowArray.GetSlicedValues(2);
            Assert.Equal(2, slice2.Length);
            Assert.Equal("3", slice2.GetString(0));
            Assert.Equal("4", slice2.GetString(1));
        }

        [Fact]
        public void UnionToArrow()
        {
            Column column = new Column();
            column.Add(new StringValue("1"));
            column.Add(NullValue.Instance);
            column.Add(new Int64Value(1));
            column.Add(new StringValue("2"));
            column.Add(new Int64Value(2));

            var result = column.ToArrowArray();
            var arr = (Apache.Arrow.DenseUnionArray)result.Item1;
            
            Assert.Equal(5, arr.Length);
            Assert.Equal(1, arr.TypeIds[0]);
            Assert.Equal(0, arr.TypeIds[1]);
            Assert.Equal(2, arr.TypeIds[2]);
            Assert.Equal(1, arr.TypeIds[3]);
            Assert.Equal(2, arr.TypeIds[4]);

            var stringColumn = (Apache.Arrow.StringArray)arr.Fields[1];
            var nullColumn = (Apache.Arrow.NullArray)arr.Fields[0];
            var intColumn = (Apache.Arrow.Int64Array)arr.Fields[2];

            Assert.Equal("1", stringColumn.GetString(arr.ValueOffsets[0]));
            Assert.True(nullColumn.IsNull(arr.ValueOffsets[1]));
            Assert.Equal(1, intColumn.GetValue(arr.ValueOffsets[2]));
            Assert.Equal("2", stringColumn.GetString(arr.ValueOffsets[3]));
            Assert.Equal(2, intColumn.GetValue(arr.ValueOffsets[4]));
        }

        [Fact]
        public void MapToArrow()
        {
            Column column = new Column();
            column.Add(new MapValue(new Dictionary<IDataValue, IDataValue>()
            {
                { new StringValue("key"), new Int64Value(1) },
                { new StringValue("value"), new StringValue("hello1") }
            }));
            column.Add(NullValue.Instance);
            // Add a list of key value pairs to make sure sorting works on keys
            column.Add(new MapValue(new List<KeyValuePair<IDataValue, IDataValue>>()
            {
                new KeyValuePair<IDataValue, IDataValue>(new StringValue("value"), new StringValue("hello2")),
                new KeyValuePair<IDataValue, IDataValue>(new StringValue("key"), new Int64Value(2))
            }));

            var result = column.ToArrowArray();
            var mapArray = (Apache.Arrow.MapArray)result.Item1;
            var firstMap = (Apache.Arrow.StructArray)mapArray.GetSlicedValues(0);
            var nullVal = mapArray.GetSlicedValues(1);
            var secondMap = (Apache.Arrow.StructArray)mapArray.GetSlicedValues(2);

            Assert.Equal(3, mapArray.Length);
            Assert.Equal(2, firstMap.Length);
            Assert.Null(nullVal);
            Assert.Equal(2, secondMap.Length);

            var firstMapKeys = (Apache.Arrow.StringArray)firstMap.Fields[0];
            var firstMapValues = (Apache.Arrow.DenseUnionArray)firstMap.Fields[1];
            Assert.Equal("key", firstMapKeys.GetString(0));
            Assert.Equal("value", firstMapKeys.GetString(1));

            var firstMapStringColumn = (Apache.Arrow.StringArray)firstMapValues.Fields[2];
            var firstMapIntColumn = (Apache.Arrow.Int64Array)firstMapValues.Fields[1];

            Assert.Equal(1, firstMapIntColumn.GetValue(firstMapValues.ValueOffsets[0]));
            Assert.Equal("hello1", firstMapStringColumn.GetString(firstMapValues.ValueOffsets[1]));

            var secondMapKeys = (Apache.Arrow.StringArray)secondMap.Fields[0];
            var secondMapValues = (Apache.Arrow.DenseUnionArray)secondMap.Fields[1];
            Assert.Equal("key", secondMapKeys.GetString(0));
            Assert.Equal("value", secondMapKeys.GetString(1));

            var secondMapStringColumn = (Apache.Arrow.StringArray)secondMapValues.Fields[2];
            var secondMapIntColumn = (Apache.Arrow.Int64Array)secondMapValues.Fields[1];

            Assert.Equal(2, secondMapIntColumn.GetValue(secondMapValues.ValueOffsets[0]));
            Assert.Equal("hello2", secondMapStringColumn.GetString(secondMapValues.ValueOffsets[1]));
        }
    }
}
