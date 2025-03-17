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
using FlowtideDotNet.Core.ColumnStore.ObjectConverter;
using FlowtideDotNet.Core.ColumnStore.ObjectConverter.Resolvers;
using FlowtideDotNet.Storage.Memory;

namespace FlowtideDotNet.Core.Tests
{
    public class ObjectConverterTests
    {

        public class InnerObject
        {
            public int NotUsedValue { get; set; }
            public string? Name { get; set; }
        }

        public enum EnumTest
        {
            Value1,
            Value2
        }

        public class TestClass
        {
            public InnerObject? InnerObject { get; set; }

            public sbyte Int8Value { get; set; }

            public short Int16Value { get; set; }

            public int Int32Value { get; set; }

            public long Int64Value { get; set; }

            public List<int>? IntList { get; set; }

            public int[]? IntArray { get; set; }

            public DateTime NotNullDateTime { get; set; }

            public DateTime? NullableDateTime { get; set; }

            public Dictionary<string, int>? Dictionary { get; set; }

            public int? NullableInteger { get; set; }

            public DateTimeOffset DateTimeOffset { get; set; }

            public bool BoolValue { get; set; }

            public object? BaseObject { get; set; }

            public double DoubleValue { get; set; }

            public float FloatValue { get; set; }

            public byte[]? ByteArray { get; set; }

            public ReadOnlyMemory<byte>? ReadOnlyMemory { get; set; }

            public Memory<byte> Memory { get; set; }

            public decimal DecimalValue { get; set; }

            public EnumTest EnumValue { get; set; }

            public List<InnerObject>? ListOfObjects { get; set; }

            public Guid GuidValue { get; set; }

            public char CharValue { get; set; }

            public HashSet<int>? IntHashSet { get; set; }

            public Func<object>? Func { get; set; }
        }

        [Fact]
        public void TestConvertSubObject()
        {
            var testObject = new TestClass()
            {
                InnerObject = new InnerObject()
                {
                    Name = "test"
                }
            };

            var converter = BatchConverter.GetBatchConverter(typeof(TestClass), new List<string>() { "innerObject" });
            IColumn[] arr = [new Column(GlobalMemoryManager.Instance)];

            converter.AppendToColumns(testObject, arr);

            var deserialized = (TestClass)converter.ConvertToDotNetObject(arr, 0);

            Assert.Equal(testObject.InnerObject.Name, deserialized.InnerObject!.Name);
        }

        [Fact]
        public void TestConvertInt32()
        {
            var testObject = new TestClass()
            {
                Int32Value = 42
            };

            var converter = BatchConverter.GetBatchConverter(typeof(TestClass), new List<string>() { "int32Value" });
            IColumn[] arr = [new Column(GlobalMemoryManager.Instance)];

            converter.AppendToColumns(testObject, arr);

            var deserialized = (TestClass)converter.ConvertToDotNetObject(arr, 0);

            Assert.Equal(testObject.Int32Value, deserialized.Int32Value);
        }

        [Fact]
        public void TestConvertInt64()
        {
            var testObject = new TestClass()
            {
                Int64Value = 42
            };

            var converter = BatchConverter.GetBatchConverter(typeof(TestClass), new List<string>() { "int64Value" });
            IColumn[] arr = [new Column(GlobalMemoryManager.Instance)];

            converter.AppendToColumns(testObject, arr);

            var deserialized = (TestClass)converter.ConvertToDotNetObject(arr, 0);

            Assert.Equal(testObject.Int64Value, deserialized.Int64Value);
        }

        [Fact]
        public void TestConvertInt16()
        {
            var testObject = new TestClass()
            {
                Int16Value = 42
            };

            var converter = BatchConverter.GetBatchConverter(typeof(TestClass), new List<string>() { "int16Value" });
            IColumn[] arr = [new Column(GlobalMemoryManager.Instance)];

            converter.AppendToColumns(testObject, arr);

            var deserialized = (TestClass)converter.ConvertToDotNetObject(arr, 0);

            Assert.Equal(testObject.Int16Value, deserialized.Int16Value);
        }

        [Fact]
        public void TestConvertInt8()
        {
            var testObject = new TestClass()
            {
                Int8Value = 42
            };

            var converter = BatchConverter.GetBatchConverter(typeof(TestClass), new List<string>() { "int8Value" });
            IColumn[] arr = [new Column(GlobalMemoryManager.Instance)];

            converter.AppendToColumns(testObject, arr);

            var deserialized = (TestClass)converter.ConvertToDotNetObject(arr, 0);

            Assert.Equal(testObject.Int8Value, deserialized.Int8Value);
        }

        [Fact]
        public void TestConvertIntList()
        {
            var testObject = new TestClass()
            {
                IntList = new List<int>() { 1, 2, 3 }
            };

            var converter = BatchConverter.GetBatchConverter(typeof(TestClass), new List<string>() { "intList" });
            IColumn[] arr = [new Column(GlobalMemoryManager.Instance)];

            converter.AppendToColumns(testObject, arr);

            var deserialized = (TestClass)converter.ConvertToDotNetObject(arr, 0);

            Assert.Equal(testObject.IntList, deserialized.IntList);
        }

        [Fact]
        public void TestConvertIntArray()
        {
            var testObject = new TestClass()
            {
                IntArray = new int[] { 1, 2, 3 }
            };

            var converter = BatchConverter.GetBatchConverter(typeof(TestClass), new List<string>() { "intArray" });
            IColumn[] arr = [new Column(GlobalMemoryManager.Instance)];

            converter.AppendToColumns(testObject, arr);

            var deserialized = (TestClass)converter.ConvertToDotNetObject(arr, 0);

            Assert.Equal(testObject.IntArray, deserialized.IntArray);
        }

        [Fact]
        public void TestConvertNotNullDateTime()
        {
            var testObject = new TestClass()
            {
                NotNullDateTime = DateTime.Now
            };

            var converter = BatchConverter.GetBatchConverter(typeof(TestClass), new List<string>() { "notNullDateTime" });
            IColumn[] arr = [new Column(GlobalMemoryManager.Instance)];

            converter.AppendToColumns(testObject, arr);

            var deserialized = (TestClass)converter.ConvertToDotNetObject(arr, 0);

            Assert.Equal(testObject.NotNullDateTime, deserialized.NotNullDateTime);
        }

        [Fact]
        public void TestConvertNullableDateTime()
        {
            var testObject = new TestClass()
            {
                NullableDateTime = DateTime.Now
            };

            var converter = BatchConverter.GetBatchConverter(typeof(TestClass), new List<string>() { "nullableDateTime" });
            IColumn[] arr = [new Column(GlobalMemoryManager.Instance)];

            converter.AppendToColumns(testObject, arr);

            var deserialized = (TestClass)converter.ConvertToDotNetObject(arr, 0);

            Assert.Equal(testObject.NullableDateTime, deserialized.NullableDateTime);
        }

        [Fact]
        public void TestConvertNullableDateTimeNull()
        {
            var testObject = new TestClass()
            {
                NullableDateTime = null
            };

            var converter = BatchConverter.GetBatchConverter(typeof(TestClass), new List<string>() { "nullableDateTime" });
            IColumn[] arr = [new Column(GlobalMemoryManager.Instance)];

            converter.AppendToColumns(testObject, arr);

            var deserialized = (TestClass)converter.ConvertToDotNetObject(arr, 0);

            Assert.Equal(testObject.NullableDateTime, deserialized.NullableDateTime);
        }

        [Fact]
        public void TestConvertDictionary()
        {
            var testObject = new TestClass()
            {
                Dictionary = new Dictionary<string, int>() { { "test", 42 } }
            };

            var converter = BatchConverter.GetBatchConverter(typeof(TestClass), new List<string>() { "dictionary" });
            IColumn[] arr = [new Column(GlobalMemoryManager.Instance)];

            converter.AppendToColumns(testObject, arr);

            var deserialized = (TestClass)converter.ConvertToDotNetObject(arr, 0);

            Assert.Equal(testObject.Dictionary, deserialized.Dictionary);
        }

        [Fact]
        public void TestConvertNullableInteger()
        {
            var testObject = new TestClass()
            {
                NullableInteger = 42
            };

            var converter = BatchConverter.GetBatchConverter(typeof(TestClass), new List<string>() { "nullableInteger" });
            IColumn[] arr = [new Column(GlobalMemoryManager.Instance)];

            converter.AppendToColumns(testObject, arr);

            var deserialized = (TestClass)converter.ConvertToDotNetObject(arr, 0);

            Assert.Equal(testObject.NullableInteger, deserialized.NullableInteger);
        }

        [Fact]
        public void TestConvertNullableIntegerNull()
        {
            var testObject = new TestClass()
            {
                NullableInteger = null
            };

            var converter = BatchConverter.GetBatchConverter(typeof(TestClass), new List<string>() { "nullableInteger" });
            IColumn[] arr = [new Column(GlobalMemoryManager.Instance)];

            converter.AppendToColumns(testObject, arr);

            var deserialized = (TestClass)converter.ConvertToDotNetObject(arr, 0);

            Assert.Equal(testObject.NullableInteger, deserialized.NullableInteger);
        }

        [Fact]
        public void TestConvertDateTimeOffset()
        {
            var testObject = new TestClass()
            {
                DateTimeOffset = DateTimeOffset.Now
            };

            var converter = BatchConverter.GetBatchConverter(typeof(TestClass), new List<string>() { "dateTimeOffset" });
            IColumn[] arr = [new Column(GlobalMemoryManager.Instance)];

            converter.AppendToColumns(testObject, arr);

            var deserialized = (TestClass)converter.ConvertToDotNetObject(arr, 0);

            Assert.Equal(testObject.DateTimeOffset, deserialized.DateTimeOffset);
        }

        [Fact]
        public void TestConvertBoolTrue()
        {
            var testObject = new TestClass()
            {
                BoolValue = true
            };

            var converter = BatchConverter.GetBatchConverter(typeof(TestClass), new List<string>() { "boolValue" });
            IColumn[] arr = [new Column(GlobalMemoryManager.Instance)];

            converter.AppendToColumns(testObject, arr);

            var deserialized = (TestClass)converter.ConvertToDotNetObject(arr, 0);

            Assert.Equal(testObject.BoolValue, deserialized.BoolValue);
        }

        [Fact]
        public void TestConvertBoolFalse()
        {
            var testObject = new TestClass()
            {
                BoolValue = false
            };

            var converter = BatchConverter.GetBatchConverter(typeof(TestClass), new List<string>() { "boolValue" });
            IColumn[] arr = [new Column(GlobalMemoryManager.Instance)];

            converter.AppendToColumns(testObject, arr);

            var deserialized = (TestClass)converter.ConvertToDotNetObject(arr, 0);

            Assert.Equal(testObject.BoolValue, deserialized.BoolValue);
        }

        [Fact]
        public void TestConvertBaseObject()
        {
            var testObject = new TestClass()
            {
                BaseObject = new InnerObject()
                {
                    Name = "test"
                }
            };

            var converter = BatchConverter.GetBatchConverter(typeof(TestClass), new List<string>() { "baseObject" });
            IColumn[] arr = [new Column(GlobalMemoryManager.Instance)];

            converter.AppendToColumns(testObject, arr);

            var deserialized = (TestClass)converter.ConvertToDotNetObject(arr, 0);

            if (deserialized.BaseObject is Dictionary<object, object> dict)
            {
                Assert.Equal(((InnerObject)testObject.BaseObject!).Name, dict["Name"]);
            }
            else
            {
                Assert.Fail("Expected dictionary");
            }

            testObject = new TestClass()
            {
                BaseObject = "hello"
            };

            converter.AppendToColumns(testObject, arr);

            deserialized = (TestClass)converter.ConvertToDotNetObject(arr, 1);

            Assert.Equal(testObject.BaseObject, deserialized.BaseObject);
        }

        [Fact]
        public void TestConvertDouble()
        {
            var testObject = new TestClass()
            {
                DoubleValue = 42.0
            };

            var converter = BatchConverter.GetBatchConverter(typeof(TestClass), new List<string>() { "doubleValue" });
            IColumn[] arr = [new Column(GlobalMemoryManager.Instance)];

            converter.AppendToColumns(testObject, arr);

            var deserialized = (TestClass)converter.ConvertToDotNetObject(arr, 0);

            Assert.Equal(testObject.DoubleValue, deserialized.DoubleValue);
        }

        [Fact]
        public void TestConvertFloat()
        {
            var testObject = new TestClass()
            {
                FloatValue = 42.0f
            };

            var converter = BatchConverter.GetBatchConverter(typeof(TestClass), new List<string>() { "floatValue" });
            IColumn[] arr = [new Column(GlobalMemoryManager.Instance)];

            converter.AppendToColumns(testObject, arr);

            var deserialized = (TestClass)converter.ConvertToDotNetObject(arr, 0);

            Assert.Equal(testObject.FloatValue, deserialized.FloatValue);
        }

        [Fact]
        public void TestConvertByteArray()
        {
            var testObject = new TestClass()
            {
                ByteArray = new byte[] { 1, 2, 3 }
            };

            var converter = BatchConverter.GetBatchConverter(typeof(TestClass), new List<string>() { "byteArray" });
            IColumn[] arr = [new Column(GlobalMemoryManager.Instance)];

            converter.AppendToColumns(testObject, arr);

            var deserialized = (TestClass)converter.ConvertToDotNetObject(arr, 0);

            Assert.Equal(testObject.ByteArray, deserialized.ByteArray);
        }

        [Fact]
        public void TestConvertReadOnlyMemory()
        {
            var testObject = new TestClass()
            {
                ReadOnlyMemory = new ReadOnlyMemory<byte>(new byte[] { 1, 2, 3 })
            };

            var converter = BatchConverter.GetBatchConverter(typeof(TestClass), new List<string>() { "readOnlyMemory" });
            IColumn[] arr = [new Column(GlobalMemoryManager.Instance)];

            converter.AppendToColumns(testObject, arr);

            var deserialized = (TestClass)converter.ConvertToDotNetObject(arr, 0);

            Assert.Equal(testObject.ReadOnlyMemory.Value, deserialized.ReadOnlyMemory!.Value);
        }

        [Fact]
        public void TestConvertMemory()
        {
            var testObject = new TestClass()
            {
                Memory = new Memory<byte>(new byte[] { 1, 2, 3 })
            };

            var converter = BatchConverter.GetBatchConverter(typeof(TestClass), new List<string>() { "memory" });
            IColumn[] arr = [new Column(GlobalMemoryManager.Instance)];

            converter.AppendToColumns(testObject, arr);

            var deserialized = (TestClass)converter.ConvertToDotNetObject(arr, 0);

            Assert.Equal(testObject.Memory, deserialized.Memory);
        }

        [Fact]
        public void TestConvertDecimal()
        {
            var testObject = new TestClass()
            {
                DecimalValue = 42.0m
            };

            var converter = BatchConverter.GetBatchConverter(typeof(TestClass), new List<string>() { "decimalValue" });
            IColumn[] arr = [new Column(GlobalMemoryManager.Instance)];

            converter.AppendToColumns(testObject, arr);

            var deserialized = (TestClass)converter.ConvertToDotNetObject(arr, 0);

            Assert.Equal(testObject.DecimalValue, deserialized.DecimalValue);
        }

        [Fact]
        public void TestConvertEnum()
        {
            var testObject = new TestClass()
            {
                EnumValue = EnumTest.Value1
            };

            var converter = BatchConverter.GetBatchConverter(typeof(TestClass), new List<string>() { "enumValue" });
            IColumn[] arr = [new Column(GlobalMemoryManager.Instance)];

            converter.AppendToColumns(testObject, arr);

            var deserialized = (TestClass)converter.ConvertToDotNetObject(arr, 0);

            Assert.Equal(testObject.EnumValue, deserialized.EnumValue);
        }

        [Fact]
        public void TestConvertEnumAsString()
        {
            var testObject = new TestClass()
            {
                EnumValue = EnumTest.Value2
            };

            var resolver = new ObjectConverterResolver();
            resolver.PrependResolver(new EnumResolver(true));

            var converter = BatchConverter.GetBatchConverter(typeof(TestClass), new List<string>() { "enumValue" }, resolver);
            IColumn[] arr = [new Column(GlobalMemoryManager.Instance)];

            converter.AppendToColumns(testObject, arr);

            var addedData = arr[0].GetValueAt(0, default).AsString.ToString();

            Assert.Equal("Value2", addedData);

            var deserialized = (TestClass)converter.ConvertToDotNetObject(arr, 0);

            Assert.Equal(testObject.EnumValue, deserialized.EnumValue);
        }

        [Fact]
        public void TestConvertListOfObjects()
        {
            var testObject = new TestClass()
            {
                ListOfObjects = new List<InnerObject>() { new InnerObject() { Name = "test" } }
            };

            var converter = BatchConverter.GetBatchConverter(typeof(TestClass), new List<string>() { "listOfObjects" });
            IColumn[] arr = [new Column(GlobalMemoryManager.Instance)];

            converter.AppendToColumns(testObject, arr);

            var deserialized = (TestClass)converter.ConvertToDotNetObject(arr, 0);

            Assert.Equal(testObject.ListOfObjects.Count, deserialized.ListOfObjects!.Count);
            Assert.Equal(testObject.ListOfObjects[0].Name, deserialized.ListOfObjects[0].Name);
        }

        [Fact]
        public void TestConvertGuid()
        {
            var testObject = new TestClass()
            {
                GuidValue = Guid.NewGuid()
            };

            var converter = BatchConverter.GetBatchConverter(typeof(TestClass), new List<string>() { "guidValue" });
            IColumn[] arr = [new Column(GlobalMemoryManager.Instance)];

            converter.AppendToColumns(testObject, arr);

            var deserialized = (TestClass)converter.ConvertToDotNetObject(arr, 0);

            Assert.Equal(testObject.GuidValue, deserialized.GuidValue);
        }

        [Fact]
        public void TestConvertChar()
        {
            var testObject = new TestClass()
            {
                CharValue = 'a'
            };

            var converter = BatchConverter.GetBatchConverter(typeof(TestClass), new List<string>() { "charValue" });
            IColumn[] arr = [new Column(GlobalMemoryManager.Instance)];

            converter.AppendToColumns(testObject, arr);

            var deserialized = (TestClass)converter.ConvertToDotNetObject(arr, 0);

            Assert.Equal(testObject.CharValue, deserialized.CharValue);
        }

        [Fact]
        public void TestConvertIntHashSet()
        {
            var testObject = new TestClass()
            {
                IntHashSet = new HashSet<int>() { 1, 2, 3 }
            };

            var converter = BatchConverter.GetBatchConverter(typeof(TestClass), new List<string>() { "intHashSet" });
            IColumn[] arr = [new Column(GlobalMemoryManager.Instance)];

            converter.AppendToColumns(testObject, arr);

            var deserialized = (TestClass)converter.ConvertToDotNetObject(arr, 0);

            Assert.Equal(testObject.IntHashSet, deserialized.IntHashSet);
        }

        [Fact]
        public void TestConvertTwoColumns()
        {
            var testObject = new TestClass()
            {
                Int32Value = 42,
                Int64Value = 43
            };

            var converter = BatchConverter.GetBatchConverter(typeof(TestClass), new List<string>() { "int32Value", "int64Value" });
            IColumn[] arr = [new Column(GlobalMemoryManager.Instance), new Column(GlobalMemoryManager.Instance)];

            converter.AppendToColumns(testObject, arr);

            var deserialized = (TestClass)converter.ConvertToDotNetObject(arr, 0);

            Assert.Equal(testObject.Int32Value, deserialized.Int32Value);
            Assert.Equal(testObject.Int64Value, deserialized.Int64Value);
        }

        [Fact]
        public void TestConvertWithNoColumnNames()
        {
            var obj = new InnerObject()
            {
                Name = "hello"
            };

            var converter = BatchConverter.GetBatchConverter(typeof(InnerObject));

            var batch = converter.ConvertToEventBatch(GlobalMemoryManager.Instance, obj);

            var deserialized = converter.ConvertToDotNetObjects(batch);

            Assert.Equal(obj.Name, (deserialized.First() as InnerObject)!.Name);
        }

        [Fact]
        public void TestConvertFuncThrowsException()
        {
            var ex = Assert.Throws<InvalidOperationException>(() =>
            {
                var converter = BatchConverter.GetBatchConverter(typeof(TestClass), new List<string>() { "func" });
            });
        }
    }
}
