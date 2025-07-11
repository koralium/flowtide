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

using Apache.Arrow.Ipc;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.ColumnStore.Serialization;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Substrait.Expressions;

namespace FlowtideDotNet.Core.Tests.ColumnStore
{
    public class ArrowTests
    {
        [Fact]
        public void Int64SerializeDeserialize()
        {
            Column column = new Column(GlobalMemoryManager.Instance);
            column.Add(new Int64Value(1));
            column.Add(NullValue.Instance);
            column.Add(new Int64Value(2));

            var recordBatch = EventArrowSerializer.BatchToArrow(new EventBatchData(
            [
                column
            ]), column.Count);

            var memoryStream = new System.IO.MemoryStream();
            var writer = new ArrowStreamWriter(memoryStream, recordBatch.Schema, true);
            writer.WriteRecordBatch(recordBatch);
            writer.Dispose();
            memoryStream.Position = 0;
            var reader = new ArrowStreamReader(memoryStream, new Apache.Arrow.Memory.NativeMemoryAllocator(), true);
            var deserializedRecordBatch = reader.ReadNextRecordBatch();
            var deserializedBatch = EventArrowSerializer.ArrowToBatch(deserializedRecordBatch, GlobalMemoryManager.Instance);

            Assert.Equal(1, deserializedBatch.Columns[0].GetValueAt(0, default).AsLong);
            Assert.True(deserializedBatch.Columns[0].GetValueAt(1, default).Type == ArrowTypeId.Null);
            Assert.Equal(2, deserializedBatch.Columns[0].GetValueAt(2, default).AsLong);
        }

        [Fact]
        public void StringSerializeDeserialize()
        {
            Column column = new Column(GlobalMemoryManager.Instance);
            column.Add(new StringValue("hello"));
            column.Add(NullValue.Instance);
            column.Add(new StringValue("world"));

            var recordBatch = EventArrowSerializer.BatchToArrow(new EventBatchData(
            [
                column
            ]), column.Count);

            var memoryStream = new System.IO.MemoryStream();
            var writer = new ArrowStreamWriter(memoryStream, recordBatch.Schema, true);
            writer.WriteRecordBatch(recordBatch);
            writer.Dispose();
            memoryStream.Position = 0;
            var reader = new ArrowStreamReader(memoryStream, new Apache.Arrow.Memory.NativeMemoryAllocator(), true);
            var deserializedRecordBatch = reader.ReadNextRecordBatch();
            var deserializedBatch = EventArrowSerializer.ArrowToBatch(deserializedRecordBatch, GlobalMemoryManager.Instance);

            Assert.Equal("hello", deserializedBatch.Columns[0].GetValueAt(0, default).ToString());
            Assert.True(deserializedBatch.Columns[0].GetValueAt(1, default).Type == ArrowTypeId.Null);
            Assert.Equal("world", deserializedBatch.Columns[0].GetValueAt(2, default).ToString());
        }

        [Fact]
        public void DoubleToArrow()
        {
            Column column = new Column(GlobalMemoryManager.Instance);
            column.Add(new DoubleValue(1.0));
            column.Add(NullValue.Instance);
            column.Add(new DoubleValue(2.0));

            var result = column.ToArrowArray();
            var arr = (Apache.Arrow.DoubleArray)result.Item1;

            Assert.Equal(1.0, arr.GetValue(0));
            Assert.True(arr.IsNull(1));
            Assert.Equal(2.0, arr.GetValue(2));
        }

        [Fact]
        public void DoubleSerializeDeserialize()
        {
            Column column = new Column(GlobalMemoryManager.Instance);
            column.Add(new DoubleValue(1.0));
            column.Add(NullValue.Instance);
            column.Add(new DoubleValue(2.0));

            var recordBatch = EventArrowSerializer.BatchToArrow(new EventBatchData(
            [
                column
            ]), column.Count);

            var memoryStream = new MemoryStream();
            var writer = new ArrowStreamWriter(memoryStream, recordBatch.Schema, true);
            writer.WriteRecordBatch(recordBatch);
            writer.Dispose();
            memoryStream.Position = 0;
            var reader = new ArrowStreamReader(memoryStream, new Apache.Arrow.Memory.NativeMemoryAllocator(), true);
            var deserializedRecordBatch = reader.ReadNextRecordBatch();
            var deserializedBatch = EventArrowSerializer.ArrowToBatch(deserializedRecordBatch, GlobalMemoryManager.Instance);

            Assert.Equal(1.0, deserializedBatch.Columns[0].GetValueAt(0, default).AsDouble);
            Assert.True(deserializedBatch.Columns[0].GetValueAt(1, default).Type == ArrowTypeId.Null);
            Assert.Equal(2.0, deserializedBatch.Columns[0].GetValueAt(2, default).AsDouble);
        }

        [Fact]
        public void ListToArrow()
        {
            Column column = new Column(GlobalMemoryManager.Instance);
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
        public void ListSerializeDeserialize()
        {
            Column column = new Column(GlobalMemoryManager.Instance);
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

            var recordBatch = EventArrowSerializer.BatchToArrow(new EventBatchData(
            [
                column
            ]), column.Count);

            MemoryStream memoryStream = new MemoryStream();
            var writer = new ArrowStreamWriter(memoryStream, recordBatch.Schema, true);
            writer.WriteRecordBatch(recordBatch);
            writer.Dispose();
            memoryStream.Position = 0;
            var reader = new ArrowStreamReader(memoryStream, new Apache.Arrow.Memory.NativeMemoryAllocator(), true);
            var deserializedRecordBatch = reader.ReadNextRecordBatch();
            var deserializedBatch = EventArrowSerializer.ArrowToBatch(deserializedRecordBatch, GlobalMemoryManager.Instance);

            var firstElement = deserializedBatch.Columns[0].GetValueAt(0, default);
            Assert.Equal("1", firstElement.AsList.GetAt(0).ToString());
            Assert.Equal("2", firstElement.AsList.GetAt(1).ToString());

            var secondElement = deserializedBatch.Columns[0].GetValueAt(2, default);
            Assert.Equal("3", secondElement.AsList.GetAt(0).ToString());
            Assert.Equal("4", secondElement.AsList.GetAt(1).ToString());

            Assert.True(deserializedBatch.Columns[0].GetValueAt(1, default).Type == ArrowTypeId.Null);
        }

        /// <summary>
        /// This test checks that memory does not overlap between offsets in list and in the binary list.
        /// There was a bug where avx operations would update the inner list offset.
        /// </summary>
        [Fact]
        public void ListSerializeDeserializeUpdateElementWithSmallerList()
        {
            Column column = new Column(GlobalMemoryManager.Instance);
            column.Add(new ListValue(new List<IDataValue>()
            {
                new StringValue("1"),
                new StringValue("2")
            }));
            column.Add(new ListValue(new List<IDataValue>()
            {
                new StringValue("3"),
                new StringValue("4")
            }));
            column.Add(new ListValue(new List<IDataValue>()
            {
                new StringValue("5"),
                new StringValue("6")
            }));
            column.Add(new ListValue(new List<IDataValue>()
            {
                new StringValue("7"),
                new StringValue("8")
            }));
            column.Add(new ListValue(new List<IDataValue>()
            {
                new StringValue("9"),
                new StringValue("10")
            }));
            column.Add(new ListValue(new List<IDataValue>()
            {
                new StringValue("11"),
                new StringValue("12")
            }));
            column.Add(new ListValue(new List<IDataValue>()
            {
                new StringValue("13"),
                new StringValue("14")
            }));
            column.Add(new ListValue(new List<IDataValue>()
            {
                new StringValue("15"),
                new StringValue("16")
            }));
            column.Add(new ListValue(new List<IDataValue>()
            {
                new StringValue("17"),
                new StringValue("18")
            }));
            column.Add(new ListValue(new List<IDataValue>()
            {
                new StringValue("19"),
                new StringValue("20")
            }));
            column.Add(new ListValue(new List<IDataValue>()
            {
                new StringValue("21"),
                new StringValue("22")
            }));
            column.Add(new ListValue(new List<IDataValue>()
            {
                new StringValue("23"),
                new StringValue("24")
            }));
            column.Add(new ListValue(new List<IDataValue>()
            {
                new StringValue("25"),
                new StringValue("26")
            }));
            column.Add(new ListValue(new List<IDataValue>()
            {
                new StringValue("27"),
                new StringValue("28")
            }));
            column.Add(new ListValue(new List<IDataValue>()
            {
                new StringValue("29"),
                new StringValue("30")
            }));

            var recordBatch = EventArrowSerializer.BatchToArrow(new EventBatchData(
            [
                column
            ]), column.Count);

            MemoryStream memoryStream = new MemoryStream();
            var writer = new ArrowStreamWriter(memoryStream, recordBatch.Schema, true);
            writer.WriteRecordBatch(recordBatch);
            writer.Dispose();
            memoryStream.Position = 0;
            var reader = new ArrowStreamReader(memoryStream, new Apache.Arrow.Memory.NativeMemoryAllocator(), true);
            var deserializedRecordBatch = reader.ReadNextRecordBatch();
            var deserializedBatch = EventArrowSerializer.ArrowToBatch(deserializedRecordBatch, GlobalMemoryManager.Instance);

            deserializedBatch.Columns[0].UpdateAt(1, new ListValue(new List<IDataValue>()
            {
                new StringValue("3")
            }));

            var firstElement = deserializedBatch.Columns[0].GetValueAt(0, default);
            Assert.Equal("1", firstElement.AsList.GetAt(0).ToString());
            Assert.Equal("2", firstElement.AsList.GetAt(1).ToString());

            var secondElement = deserializedBatch.Columns[0].GetValueAt(1, default);
            Assert.Equal(1, secondElement.AsList.Count);
            Assert.Equal("3", secondElement.AsList.GetAt(0).ToString());


            for (int i = 2; i < 15; i++)
            {
                var element = deserializedBatch.Columns[0].GetValueAt(i, default);
                Assert.Equal(((i * 2) + 1).ToString(), element.AsList.GetAt(0).ToString());
                Assert.Equal(((i * 2) + 2).ToString(), element.AsList.GetAt(1).ToString());
            }
        }

        [Fact]
        public void UnionToArrow()
        {
            Column column = new Column(GlobalMemoryManager.Instance);
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
            var intColumn = (Apache.Arrow.Int8Array)arr.Fields[2];

            Assert.Equal("1", stringColumn.GetString(arr.ValueOffsets[0]));
            Assert.True(nullColumn.IsNull(arr.ValueOffsets[1]));
            Assert.Equal(1, (long)intColumn.GetValue(arr.ValueOffsets[2])!);
            Assert.Equal("2", stringColumn.GetString(arr.ValueOffsets[3]));
            Assert.Equal(2, (long)intColumn.GetValue(arr.ValueOffsets[4])!);
        }

        [Fact]
        public void UnionSerializeDeserialize()
        {
            Column column = new Column(GlobalMemoryManager.Instance);
            column.Add(new StringValue("1"));
            column.Add(NullValue.Instance);
            column.Add(new Int64Value(1));
            column.Add(new StringValue("2"));
            column.Add(new Int64Value(2));
            column.Add(new MapValue(new KeyValuePair<IDataValue, IDataValue>(new StringValue("hello"), new StringValue("world"))));

            var recordBatch = EventArrowSerializer.BatchToArrow(new EventBatchData(
            [
                column
            ]), column.Count);

            MemoryStream memoryStream = new MemoryStream();
            var writer = new ArrowStreamWriter(memoryStream, recordBatch.Schema, true);
            writer.WriteRecordBatch(recordBatch);
            writer.Dispose();
            memoryStream.Position = 0;
            var reader = new ArrowStreamReader(memoryStream, new Apache.Arrow.Memory.NativeMemoryAllocator(), true);
            var deserializedRecordBatch = reader.ReadNextRecordBatch();
            var deserializedBatch = EventArrowSerializer.ArrowToBatch(deserializedRecordBatch, GlobalMemoryManager.Instance);

            Assert.Equal("1", deserializedBatch.Columns[0].GetValueAt(0, default).ToString());
            Assert.True(deserializedBatch.Columns[0].GetValueAt(1, default).Type == ArrowTypeId.Null);
            Assert.Equal(1, deserializedBatch.Columns[0].GetValueAt(2, default).AsLong);
            Assert.Equal("2", deserializedBatch.Columns[0].GetValueAt(3, default).ToString());
            Assert.Equal(2, deserializedBatch.Columns[0].GetValueAt(4, default).AsLong);


        }

        [Fact]
        public void UnionSerializeDeserializeWithOnlyNull()
        {
            Column column = new Column(GlobalMemoryManager.Instance);
            column.Add(new StringValue("1"));
            column.Add(NullValue.Instance);
            column.Add(new Int64Value(1));

            Column toInsertInto = new Column(GlobalMemoryManager.Instance);

            toInsertInto.InsertRangeFrom(0, column, 1, 1);

            var recordBatch = EventArrowSerializer.BatchToArrow(new EventBatchData(
            [
                toInsertInto
            ]), toInsertInto.Count);

            MemoryStream memoryStream = new MemoryStream();
            var writer = new ArrowStreamWriter(memoryStream, recordBatch.Schema, true);
            writer.WriteRecordBatch(recordBatch);
            writer.Dispose();
            memoryStream.Position = 0;
            var reader = new ArrowStreamReader(memoryStream, new Apache.Arrow.Memory.NativeMemoryAllocator(), true);
            var deserializedRecordBatch = reader.ReadNextRecordBatch();
            var deserializedBatch = EventArrowSerializer.ArrowToBatch(deserializedRecordBatch, GlobalMemoryManager.Instance);

            // This threw an error since null count was incorrectly set on the union column from the null column inside
            deserializedBatch.Columns[0].RemoveAt(0);
            Assert.Empty(deserializedBatch);
        }

        [Fact]
        public void MapToArrow()
        {
            Column column = new Column(GlobalMemoryManager.Instance);
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
            var firstMapIntColumn = (Apache.Arrow.Int8Array)firstMapValues.Fields[1];

            Assert.Equal(1, (long)firstMapIntColumn.GetValue(firstMapValues.ValueOffsets[0])!);
            Assert.Equal("hello1", firstMapStringColumn.GetString(firstMapValues.ValueOffsets[1]));

            var secondMapKeys = (Apache.Arrow.StringArray)secondMap.Fields[0];
            var secondMapValues = (Apache.Arrow.DenseUnionArray)secondMap.Fields[1];
            Assert.Equal("key", secondMapKeys.GetString(0));
            Assert.Equal("value", secondMapKeys.GetString(1));

            var secondMapStringColumn = (Apache.Arrow.StringArray)secondMapValues.Fields[2];
            var secondMapIntColumn = (Apache.Arrow.Int8Array)secondMapValues.Fields[1];

            Assert.Equal(2, (long)secondMapIntColumn.GetValue(secondMapValues.ValueOffsets[0])!);
            Assert.Equal("hello2", secondMapStringColumn.GetString(secondMapValues.ValueOffsets[1]));
        }

        [Fact]
        public void MapSerializeDeserialize()
        {
            Column column = new Column(GlobalMemoryManager.Instance);
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

            var recordBatch = EventArrowSerializer.BatchToArrow(new EventBatchData(
            [
                column
            ]), column.Count);

            MemoryStream memoryStream = new MemoryStream();
            var writer = new ArrowStreamWriter(memoryStream, recordBatch.Schema, true);
            writer.WriteRecordBatch(recordBatch);
            writer.Dispose();
            memoryStream.Position = 0;
            var reader = new ArrowStreamReader(memoryStream, new Apache.Arrow.Memory.NativeMemoryAllocator(), true);
            var deserializedRecordBatch = reader.ReadNextRecordBatch();
            var deserializedBatch = EventArrowSerializer.ArrowToBatch(deserializedRecordBatch, GlobalMemoryManager.Instance);


            Assert.Equal(1, deserializedBatch.Columns[0].GetValueAt(0, new MapKeyReferenceSegment() { Key = "key" }).AsLong);
            Assert.Equal("hello1", deserializedBatch.Columns[0].GetValueAt(0, new MapKeyReferenceSegment() { Key = "value" }).ToString());

            Assert.True(deserializedBatch.Columns[0].GetValueAt(1, default).Type == ArrowTypeId.Null);

            Assert.Equal(2, deserializedBatch.Columns[0].GetValueAt(2, new MapKeyReferenceSegment() { Key = "key" }).AsLong);
            Assert.Equal("hello2", deserializedBatch.Columns[0].GetValueAt(2, new MapKeyReferenceSegment() { Key = "value" }).ToString());
        }

        [Fact]
        public void BoolToArrow()
        {
            Column column = new Column(GlobalMemoryManager.Instance);
            column.Add(new BoolValue(true));
            column.Add(NullValue.Instance);
            column.Add(new BoolValue(false));

            var result = column.ToArrowArray();
            var arr = (Apache.Arrow.BooleanArray)result.Item1;

            Assert.Equal(3, arr.Length);
            Assert.True(arr.GetValue(0));
            Assert.True(arr.IsNull(1));
            Assert.False(arr.GetValue(2));
        }

        [Fact]
        public void NullColumnToArrow()
        {
            Column column = new Column(GlobalMemoryManager.Instance);
            column.Add(NullValue.Instance);
            column.Add(NullValue.Instance);
            column.Add(NullValue.Instance);

            var result = column.ToArrowArray();
            var arrowArray = (Apache.Arrow.NullArray)result.Item1;
            Assert.Equal(3, arrowArray.NullCount);
            Assert.Equal(3, arrowArray.Length);
        }

        [Fact]
        public void NullSerializeDeserialize()
        {
            Column column = new Column(GlobalMemoryManager.Instance);
            column.Add(NullValue.Instance);
            column.Add(NullValue.Instance);
            column.Add(NullValue.Instance);

            var recordBatch = EventArrowSerializer.BatchToArrow(new EventBatchData(
            [
                column
            ]), column.Count);

            MemoryStream memoryStream = new MemoryStream();
            var writer = new ArrowStreamWriter(memoryStream, recordBatch.Schema, true);
            writer.WriteRecordBatch(recordBatch);
            writer.Dispose();
            memoryStream.Position = 0;
            var reader = new ArrowStreamReader(memoryStream, new Apache.Arrow.Memory.NativeMemoryAllocator(), true);
            var deserializedRecordBatch = reader.ReadNextRecordBatch();
            var deserializedBatch = EventArrowSerializer.ArrowToBatch(deserializedRecordBatch, GlobalMemoryManager.Instance);

            Assert.True(deserializedBatch.Columns[0].GetValueAt(0, default).Type == ArrowTypeId.Null);
            Assert.True(deserializedBatch.Columns[0].GetValueAt(1, default).Type == ArrowTypeId.Null);
            Assert.True(deserializedBatch.Columns[0].GetValueAt(2, default).Type == ArrowTypeId.Null);
        }

        [Fact]
        public void BoolSerializeDeserialize()
        {
            Column column = new Column(GlobalMemoryManager.Instance);
            column.Add(new BoolValue(true));
            column.Add(NullValue.Instance);
            column.Add(new BoolValue(false));

            var recordBatch = EventArrowSerializer.BatchToArrow(new EventBatchData(
            [
                column
            ]), column.Count);

            MemoryStream memoryStream = new MemoryStream();
            var writer = new ArrowStreamWriter(memoryStream, recordBatch.Schema, true);
            writer.WriteRecordBatch(recordBatch);
            writer.Dispose();
            memoryStream.Position = 0;
            var reader = new ArrowStreamReader(memoryStream, new Apache.Arrow.Memory.NativeMemoryAllocator(), true);
            var deserializedRecordBatch = reader.ReadNextRecordBatch();
            var deserializedBatch = EventArrowSerializer.ArrowToBatch(deserializedRecordBatch, GlobalMemoryManager.Instance);

            Assert.True(deserializedBatch.Columns[0].GetValueAt(0, default).AsBool);
            Assert.True(deserializedBatch.Columns[0].GetValueAt(1, default).Type == ArrowTypeId.Null);
            Assert.False(deserializedBatch.Columns[0].GetValueAt(2, default).AsBool);
        }

        [Fact]
        public void BinaryToArrow()
        {
            Column column = new Column(GlobalMemoryManager.Instance);
            column.Add(new BinaryValue(new byte[] { 1, 2, 3 }));
            column.Add(NullValue.Instance);
            column.Add(new BinaryValue(new byte[] { 4, 5, 6 }));

            var result = column.ToArrowArray();
            var arr = (Apache.Arrow.BinaryArray)result.Item1;

            Assert.Equal(3, arr.Length);
            Assert.Equal(new byte[] { 1, 2, 3 }, arr.GetBytes(0));
            Assert.True(arr.IsNull(1));
            Assert.Equal(new byte[] { 4, 5, 6 }, arr.GetBytes(2));
        }

        [Fact]
        public void BinarySerializeDeserialize()
        {
            Column column = new Column(GlobalMemoryManager.Instance);
            column.Add(new BinaryValue(new byte[] { 1, 2, 3 }));
            column.Add(NullValue.Instance);
            column.Add(new BinaryValue(new byte[] { 4, 5, 6 }));

            var recordBatch = EventArrowSerializer.BatchToArrow(new EventBatchData(
            [
                column
            ]), column.Count);

            MemoryStream memoryStream = new MemoryStream();
            var writer = new ArrowStreamWriter(memoryStream, recordBatch.Schema, true);
            writer.WriteRecordBatch(recordBatch);
            writer.Dispose();
            memoryStream.Position = 0;
            var reader = new ArrowStreamReader(memoryStream, new Apache.Arrow.Memory.NativeMemoryAllocator(), true);
            var deserializedRecordBatch = reader.ReadNextRecordBatch();
            var deserializedBatch = EventArrowSerializer.ArrowToBatch(deserializedRecordBatch, GlobalMemoryManager.Instance);

            Assert.Equal(new byte[] { 1, 2, 3 }, deserializedBatch.Columns[0].GetValueAt(0, default).AsBinary);
            Assert.True(deserializedBatch.Columns[0].GetValueAt(1, default).Type == ArrowTypeId.Null);
            Assert.Equal(new byte[] { 4, 5, 6 }, deserializedBatch.Columns[0].GetValueAt(2, default).AsBinary);
        }

        [Fact]
        public void DecimalSerializeDeserialize()
        {
            Column column = new Column(GlobalMemoryManager.Instance);
            column.Add(new DecimalValue(1.0m));
            column.Add(NullValue.Instance);
            column.Add(new DecimalValue(2.0m));

            var recordBatch = EventArrowSerializer.BatchToArrow(new EventBatchData(
            [
                column
            ]), column.Count);

            MemoryStream memoryStream = new MemoryStream();
            var writer = new ArrowStreamWriter(memoryStream, recordBatch.Schema, true);
            writer.WriteRecordBatch(recordBatch);
            writer.Dispose();
            memoryStream.Position = 0;
            var reader = new ArrowStreamReader(memoryStream, new Apache.Arrow.Memory.NativeMemoryAllocator(), true);
            var deserializedRecordBatch = reader.ReadNextRecordBatch();
            var deserializedBatch = EventArrowSerializer.ArrowToBatch(deserializedRecordBatch, GlobalMemoryManager.Instance);

            Assert.Equal(1.0m, deserializedBatch.Columns[0].GetValueAt(0, default).AsDecimal);
            Assert.True(deserializedBatch.Columns[0].GetValueAt(1, default).Type == ArrowTypeId.Null);
            Assert.Equal(2.0m, deserializedBatch.Columns[0].GetValueAt(2, default).AsDecimal);
        }

        [Fact]
        public void DecimalToArrow()
        {
            Column column = new Column(GlobalMemoryManager.Instance);
            column.Add(new DecimalValue(1.0m));
            column.Add(NullValue.Instance);
            column.Add(new DecimalValue(2.0m));

            var result = column.ToArrowArray();
            var arr = (Apache.Arrow.Arrays.FixedSizeBinaryArray)result.Item1;

            Assert.Equal(1.0m, Apache.Arrow.SpanExtensions.CastTo<decimal>(arr.GetBytes(0))[0]);
            Assert.True(arr.IsNull(1));
            Assert.Equal(2.0m, Apache.Arrow.SpanExtensions.CastTo<decimal>(arr.GetBytes(2))[0]);
        }

        /// <summary>
        /// This test checks that list functions with custom types function correctly
        /// </summary>
        [Fact]
        public void TestDecimalInListSerializeDeserialize()
        {
            Column column = new Column(GlobalMemoryManager.Instance);
            column.Add(new ListValue(new List<IDataValue>()
            {
                new DecimalValue(1.0m),
                new DecimalValue(2.0m)
            }));

            var recordBatch = EventArrowSerializer.BatchToArrow(new EventBatchData(
            [
                column
            ]), column.Count);

            MemoryStream memoryStream = new MemoryStream();

            var writer = new ArrowStreamWriter(memoryStream, recordBatch.Schema, true);
            writer.WriteRecordBatch(recordBatch);
            writer.Dispose();
            memoryStream.Position = 0;
            var reader = new ArrowStreamReader(memoryStream, new Apache.Arrow.Memory.NativeMemoryAllocator(), true);
            var deserializedRecordBatch = reader.ReadNextRecordBatch();
            var deserializedBatch = EventArrowSerializer.ArrowToBatch(deserializedRecordBatch, GlobalMemoryManager.Instance);

            var list = deserializedBatch.Columns[0].GetValueAt(0, default).AsList;
            Assert.Equal(1.0m, list.GetAt(0).AsDecimal);
            Assert.Equal(2.0m, list.GetAt(1).AsDecimal);
        }

        /// <summary>
        /// This test checks that map functions with custom types function correctly
        /// </summary>
        [Fact]
        public void TestDecimalInMapSerializeDeserialize()
        {
            Column column = new Column(GlobalMemoryManager.Instance);
            column.Add(new MapValue(new Dictionary<IDataValue, IDataValue>()
            {
                { new StringValue("key"), new DecimalValue(1.0m) },
                { new StringValue("value"), new DecimalValue(2.0m) }
            }));

            var recordBatch = EventArrowSerializer.BatchToArrow(new EventBatchData(
            [
                column
            ]), column.Count);

            MemoryStream memoryStream = new MemoryStream();

            var writer = new ArrowStreamWriter(memoryStream, recordBatch.Schema, true);
            writer.WriteRecordBatch(recordBatch);
            writer.Dispose();
            memoryStream.Position = 0;
            var reader = new ArrowStreamReader(memoryStream, new Apache.Arrow.Memory.NativeMemoryAllocator(), true);
            var deserializedRecordBatch = reader.ReadNextRecordBatch();
            var deserializedBatch = EventArrowSerializer.ArrowToBatch(deserializedRecordBatch, GlobalMemoryManager.Instance);

            var map = deserializedBatch.Columns[0].GetValueAt(0, new MapKeyReferenceSegment() { Key = "key" }).AsDecimal;
            Assert.Equal(1.0m, map);

            map = deserializedBatch.Columns[0].GetValueAt(0, new MapKeyReferenceSegment() { Key = "value" }).AsDecimal;
            Assert.Equal(2.0m, map);

        }

        [Fact]
        public void TimestampInUnionSerializeDeserialize()
        {
            Column column = new Column(GlobalMemoryManager.Instance);
            column.Add(new TimestampTzValue(1, 0));
            column.Add(NullValue.Instance);
            column.Add(new Int64Value(2));

            var recordBatch = EventArrowSerializer.BatchToArrow(new EventBatchData(
            [
                column
            ]), column.Count);

            MemoryStream memoryStream = new MemoryStream();

            var writer = new ArrowStreamWriter(memoryStream, recordBatch.Schema, true);
            writer.WriteRecordBatch(recordBatch);
            writer.Dispose();
            memoryStream.Position = 0;
            var reader = new ArrowStreamReader(memoryStream, new Apache.Arrow.Memory.NativeMemoryAllocator(), true);
            var deserializedRecordBatch = reader.ReadNextRecordBatch();
            var deserializedBatch = EventArrowSerializer.ArrowToBatch(deserializedRecordBatch, GlobalMemoryManager.Instance);

            Assert.Equal(1, column.GetValueAt(0, default).AsTimestamp.ticks);
        }

        [Fact]
        public void StructSerializeDeserialize()
        {
            var structHeader = StructHeader.Create("column1", "column2");

            Column column = new Column(GlobalMemoryManager.Instance)
            {
                new StructValue(structHeader, new Int64Value(123), new StringValue("hello"))
            };

            var recordBatch = EventArrowSerializer.BatchToArrow(new EventBatchData(
            [
                column
            ]), column.Count);

            MemoryStream memoryStream = new MemoryStream();

            var writer = new ArrowStreamWriter(memoryStream, recordBatch.Schema, true);
            writer.WriteRecordBatch(recordBatch);
            writer.Dispose();
            memoryStream.Position = 0;
            var reader = new ArrowStreamReader(memoryStream, new Apache.Arrow.Memory.NativeMemoryAllocator(), true);
            var deserializedRecordBatch = reader.ReadNextRecordBatch();
            var deserializedBatch = EventArrowSerializer.ArrowToBatch(deserializedRecordBatch, GlobalMemoryManager.Instance);

            var row1 = deserializedBatch.Columns[0].GetValueAt(0, default).AsStruct;
            Assert.Equal(123, row1.GetAt(0).AsLong);
            Assert.Equal("hello", row1.GetAt(1).AsString.ToString());
        }

        [Fact]
        public void StructInUnionSerializeDeserialize()
        {
            var structHeader = StructHeader.Create("column1", "column2");
            var otherStructHeader = StructHeader.Create("col1", "col2");

            Column column = new Column(GlobalMemoryManager.Instance)
            {
                new StructValue(structHeader, new Int64Value(123), new StringValue("hello")),
                new StructValue(otherStructHeader, new Int64Value(321), new StringValue("world"))
            };

            var recordBatch = EventArrowSerializer.BatchToArrow(new EventBatchData(
            [
                column
            ]), column.Count);

            MemoryStream memoryStream = new MemoryStream();

            var writer = new ArrowStreamWriter(memoryStream, recordBatch.Schema, true);
            writer.WriteRecordBatch(recordBatch);
            writer.Dispose();
            memoryStream.Position = 0;
            var reader = new ArrowStreamReader(memoryStream, new Apache.Arrow.Memory.NativeMemoryAllocator(), true);
            var deserializedRecordBatch = reader.ReadNextRecordBatch();
            var deserializedBatch = EventArrowSerializer.ArrowToBatch(deserializedRecordBatch, GlobalMemoryManager.Instance);

            var row1 = deserializedBatch.Columns[0].GetValueAt(0, default).AsStruct;
            Assert.Equal(123, row1.GetAt(0).AsLong);
            Assert.Equal("hello", row1.GetAt(1).AsString.ToString());

            var row2 = deserializedBatch.Columns[0].GetValueAt(1, default).AsStruct;
            Assert.Equal(321, row2.GetAt(0).AsLong);
            Assert.Equal("world", row2.GetAt(1).AsString.ToString());
        }   
    }
}
