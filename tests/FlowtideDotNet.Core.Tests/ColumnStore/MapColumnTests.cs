﻿// Licensed under the Apache License, Version 2.0 (the "License")
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
            Column column = new Column(GlobalMemoryManager.Instance);
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
            Column column = new Column(GlobalMemoryManager.Instance);
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

        [Fact]
        public void UpdateSecondElement()
        {
            Column column = new Column(GlobalMemoryManager.Instance);
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
            column.Add(new MapValue(new Dictionary<IDataValue, IDataValue>()
            {
                { new StringValue("key"), new Int64Value(2) },
                { new StringValue("value"), new StringValue("hello3") }
            }));

            column.UpdateAt(1, new MapValue(new Dictionary<IDataValue, IDataValue>()
            {
                { new StringValue("key"), new Int64Value(3) },
                { new StringValue("value"), new StringValue("hello4") }
            }));

            
            var valueData = column.GetValueAt(1, new MapKeyReferenceSegment() { Key = "value" });
            Assert.Equal("hello4", valueData.ToString());
        }

        [Fact]
        public void RemoveRangeNonNull()
        {
            Column column = new Column(GlobalMemoryManager.Instance);

            List<SortedDictionary<string, string>?> expected = new List<SortedDictionary<string, string>?>();
            Random r = new Random(123);
            for (int i = 0; i < 1000; i++)
            {
                var dictSize = r.Next(20);

                SortedDictionary<string, string> rowexpected = new SortedDictionary<string, string>();
                Dictionary<IDataValue, IDataValue> actual = new Dictionary<IDataValue, IDataValue>();
                for (int k = 0; k < dictSize; k++)
                {
                    rowexpected.Add(k.ToString(), k.ToString());
                    actual.Add(new StringValue(k.ToString()), new StringValue(k.ToString()));
                }
                expected.Add(rowexpected);
                column.Add(new MapValue(actual));
            }

            column.RemoveRange(100, 100);
            expected.RemoveRange(100, 100);

            Assert.Equal(expected.Count(x => x == null), column.GetNullCount());
            Assert.Equal(900, column.Count);

            for (int i = 0; i < 900; i++)
            {
                var actual = column.GetValueAt(i, default);
                if (expected[i] == null)
                {
                    Assert.True(actual.IsNull);
                }
                else
                {
                    var dict = actual.AsMap;
                    var dictLength = dict.GetLength();
                    var expectedVal = expected[i]!;
                    Assert.Equal(expectedVal.Count, dictLength);

                    var expectedKeys = expectedVal.Keys.ToList();
                    DataValueContainer dataValueContainer = new DataValueContainer();
                    for (int k = 0; k < dictLength; k++)
                    {
                        dict.GetKeyAt(k, dataValueContainer);
                        Assert.Equal(expectedKeys[k], dataValueContainer.AsString.ToString());

                        dict.GetValueAt(k, dataValueContainer);
                        Assert.Equal(expectedVal[expectedKeys[k]], dataValueContainer.AsString.ToString());
                    }
                }
            }
        }

        [Fact]
        public void RemoveRangeWithNull()
        {
            Column column = new Column(GlobalMemoryManager.Instance);

            List<SortedDictionary<string, string>?> expected = new List<SortedDictionary<string, string>?>();
            Random r = new Random(123);
            for (int i = 0; i < 1000; i++)
            {
                var isNull = r.Next(2) == 0;

                if (isNull)
                {
                    expected.Add(null);
                    column.Add(NullValue.Instance);
                }
                else
                {
                    var dictSize = r.Next(20);

                    SortedDictionary<string, string> rowexpected = new SortedDictionary<string, string>();
                    Dictionary<IDataValue, IDataValue> actual = new Dictionary<IDataValue, IDataValue>();
                    for (int k = 0; k < dictSize; k++)
                    {
                        rowexpected.Add(k.ToString(), k.ToString());
                        actual.Add(new StringValue(k.ToString()), new StringValue(k.ToString()));
                    }
                    expected.Add(rowexpected);
                    column.Add(new MapValue(actual));
                }

            }

            column.RemoveRange(100, 100);
            expected.RemoveRange(100, 100);

            Assert.Equal(expected.Count(x => x == null), column.GetNullCount());
            Assert.Equal(900, column.Count);

            for (int i = 0; i < 900; i++)
            {
                var actual = column.GetValueAt(i, default);
                if (expected[i] == null)
                {
                    Assert.True(actual.IsNull);
                }
                else
                {
                    var dict = actual.AsMap;
                    var dictLength = dict.GetLength();
                    var expectedVal = expected[i]!;
                    Assert.Equal(expectedVal.Count, dictLength);

                    var expectedKeys = expectedVal.Keys.ToList();
                    DataValueContainer dataValueContainer = new DataValueContainer();
                    for (int k = 0; k < dictLength; k++)
                    {
                        dict.GetKeyAt(k, dataValueContainer);
                        Assert.Equal(expectedKeys[k], dataValueContainer.AsString.ToString());

                        dict.GetValueAt(k, dataValueContainer);
                        Assert.Equal(expectedVal[expectedKeys[k]], dataValueContainer.AsString.ToString());
                    }                    
                }
            }
        }
    }
}
