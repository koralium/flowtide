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
using FlowtideDotNet.Substrait.Expressions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
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

        [Fact]
        public void InsertRangeNonNull()
        {
            Column column = new Column(GlobalMemoryManager.Instance);
            Column other = new Column(GlobalMemoryManager.Instance);

            List<SortedDictionary<string, string>?> otherList = new List<SortedDictionary<string, string>?>();
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
                otherList.Add(rowexpected);
                other.Add(new MapValue(actual));
            }

            column.InsertRangeFrom(100, other, 100, 100);
            expected.InsertRange(100, otherList.Skip(100).Take(100));

            Assert.Equal(expected.Count(x => x == null), column.GetNullCount());
            Assert.Equal(expected.Count, column.Count);

            for (int i = 0; i < expected.Count; i++)
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


        /// <summary>
        /// This test validates a bug where an out of bounds exception was thrown since it tried to read the bitmap list and the index was on the boundary.
        /// </summary>
        [Fact]
        public void InsertRangeWithNullValues()
        {
            Column column = new Column(GlobalMemoryManager.Instance);
            column.Add(new MapValue(new Dictionary<IDataValue, IDataValue>()
            {
                { new StringValue("key"), new StringValue("hello1") },
                { new StringValue("value"), new StringValue("hello1") }
            }));
            column.Add(new MapValue(new Dictionary<IDataValue, IDataValue>()
            {
                { new StringValue("key"), new StringValue("hello2") },
                { new StringValue("value"), new StringValue("hello2") }
            }));
            column.Add(new MapValue(new Dictionary<IDataValue, IDataValue>()
            {
                { new StringValue("key"), NullValue.Instance },
                { new StringValue("value"), new StringValue("hello3") }
            }));

            Column other = new Column(GlobalMemoryManager.Instance);

            // Add so the null happens on the 32 element so it will be a new segment in the bitmap list
            for (int i = 0; i < 32; i++)
            {
                other.Add(new MapValue(new Dictionary<IDataValue, IDataValue>()
                {
                    { new StringValue("key"), NullValue.Instance },
                    { new StringValue("value"), new StringValue("hello1") }
                }));
            }


            other.Add(NullValue.Instance);

            column.InsertRangeFrom(3, other, 32, 1);

            Assert.Equal(4, column.Count);

            Assert.True(column.GetValueAt(3, default).IsNull);
        }

        [Fact]
        public void RemoveRangeWithNullInLastBitmapSegment()
        {
            Column column = new Column(GlobalMemoryManager.Instance);

            // Add so the null happens on the 32 element so it will be a new segment in the bitmap list
            for (int i = 0; i < 32; i++)
            {
                column.Add(new MapValue(new Dictionary<IDataValue, IDataValue>()
                {
                    { new StringValue("key"), NullValue.Instance },
                    { new StringValue("value"), new StringValue("hello1") }
                }));
            }

            column.Add(NullValue.Instance);

            column.RemoveRange(32, 1);

            Assert.Equal(32, column.Count);
        }

        [Fact]
        public void TestJsonEncoding()
        {
            Column column = new Column(GlobalMemoryManager.Instance);

            column.Add(new MapValue(new KeyValuePair<IDataValue, IDataValue>(new StringValue("a"), new Int64Value(2)), new KeyValuePair<IDataValue, IDataValue>(new Int64Value(5), new StringValue("hello"))));

            using MemoryStream stream = new MemoryStream();
            Utf8JsonWriter writer = new Utf8JsonWriter(stream);

            column.WriteToJson(in writer, 0);
            writer.Flush();

            string json = Encoding.UTF8.GetString(stream.ToArray());

            Assert.Equal("{\"5\":\"hello\",\"a\":2}", json);
        }

        [Fact]
        public void TestCopy()
        {
            Column column = new Column(GlobalMemoryManager.Instance);

            Random r = new Random(123);
            for (int i = 0; i < 1000; i++)
            {
                var dictSize = r.Next(20);

                Dictionary<IDataValue, IDataValue> actual = new Dictionary<IDataValue, IDataValue>();
                for (int k = 0; k < dictSize; k++)
                {
                    actual.Add(new StringValue(k.ToString()), new StringValue(k.ToString()));
                }
                column.Add(new MapValue(actual));
            }

            Column copy = column.Copy(GlobalMemoryManager.Instance);

            Assert.Equal(1000, copy.Count);

            for (int i = 0; i < 1000; i++)
            {
                var actual = copy.GetValueAt(i, default);
                var dict = actual.AsMap;
                var dictLength = dict.GetLength();
                Assert.Equal(column.GetValueAt(i, default).AsMap.GetLength(), dictLength);

                DataValueContainer dataValueContainer = new DataValueContainer();
                DataValueContainer dataValueContainer2 = new DataValueContainer();
                for (int k = 0; k < dictLength; k++)
                {
                    dict.GetKeyAt(k, dataValueContainer);
                    column.GetValueAt(i, default).AsMap.GetKeyAt(k, dataValueContainer2);
                    Assert.Equal(dataValueContainer2.AsString.ToString(), dataValueContainer.AsString.ToString());

                    dict.GetValueAt(k, dataValueContainer);
                    column.GetValueAt(i, default).AsMap.GetValueAt(k, dataValueContainer2);
                    Assert.Equal(dataValueContainer2.AsString.ToString(), dataValueContainer.AsString.ToString());
                }
            }
        }

        [Fact]
        public void TestInsertNullAtStart()
        {
            MapColumn column = new MapColumn(GlobalMemoryManager.Instance);

            column.InsertAt(0, new MapValue(new KeyValuePair<IDataValue, IDataValue>(new StringValue("a"), new StringValue("b"))));
            column.InsertAt(0, NullValue.Instance);
            var val = column.GetValueAt(0, default);
            Assert.Equal(0, val.AsMap.GetLength());
        }
    }
}
