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
using Apache.Arrow.Types;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.ColumnStore.Serialization;
using FlowtideDotNet.Storage.Memory;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.Tests.ColumnStore
{
    public class FlowtideArrowSerializerTests
    {
        [Fact]
        public void TestSerializeIntegerColumn()
        {
            Column column = Column.Create(GlobalMemoryManager.Instance);
            column.Add(new Int64Value(1));
            column.Add(new Int64Value(2));

            var batch = new EventBatchData([column]);

            var serializer = new EventBatchSerializer();
            var bufferWriter = new ArrayBufferWriter<byte>();
            serializer.SerializeEventBatch(bufferWriter, batch, 2);

            var serializedBytes = bufferWriter.WrittenSpan.ToArray();

            MemoryStream memoryStream = new MemoryStream(serializedBytes);
            ArrowStreamReader reader = new ArrowStreamReader(memoryStream);
            var recordBatch = reader.ReadNextRecordBatch();

            Assert.True(recordBatch.Schema.FieldsList[0].DataType is Int64Type);

            var deserializedColumn = (Apache.Arrow.Int64Array)recordBatch.Column(0);
            Assert.Equal(1, deserializedColumn.GetValue(0));
            Assert.Equal(2, deserializedColumn.GetValue(1));
        }

        [Fact]
        public void TestSerializeBoolColumn()
        {
            Column column = Column.Create(GlobalMemoryManager.Instance);
            column.Add(new BoolValue(true));
            column.Add(new BoolValue(false));
            column.Add(new BoolValue(true));

            var batch = new EventBatchData([column]);

            var serializer = new EventBatchSerializer();
            var bufferWriter = new ArrayBufferWriter<byte>();
            serializer.SerializeEventBatch(bufferWriter, batch, 2);

            var serializedBytes = bufferWriter.WrittenSpan.ToArray();

            MemoryStream memoryStream = new MemoryStream(serializedBytes);
            ArrowStreamReader reader = new ArrowStreamReader(memoryStream);
            var recordBatch = reader.ReadNextRecordBatch();

            Assert.True(recordBatch.Schema.FieldsList[0].DataType is BooleanType);

            var deserializedColumn = (Apache.Arrow.BooleanArray)recordBatch.Column(0);
            Assert.Equal(true, deserializedColumn.GetValue(0));
            Assert.Equal(false, deserializedColumn.GetValue(1));
            Assert.Equal(true, deserializedColumn.GetValue(2));
        }

        [Fact]
        public void TestSerializeDecimalColumn()
        {
            Column column = Column.Create(GlobalMemoryManager.Instance);
            column.Add(new DecimalValue(1.23m));
            column.Add(new DecimalValue(3.17m));

            var batch = new EventBatchData([column]);

            var serializer = new EventBatchSerializer();
            var bufferWriter = new ArrayBufferWriter<byte>();
            serializer.SerializeEventBatch(bufferWriter, batch, 2);

            var serializedBytes = bufferWriter.WrittenSpan.ToArray();

            MemoryStream memoryStream = new MemoryStream(serializedBytes);
            ArrowStreamReader reader = new ArrowStreamReader(memoryStream);
            var recordBatch = reader.ReadNextRecordBatch();

            Assert.True(recordBatch.Schema.FieldsList[0].Metadata.TryGetValue("ARROW:extension:name", out var customExtensionName));
            Assert.Equal("flowtide.floatingdecimaltype", customExtensionName);

            Assert.True(recordBatch.Schema.FieldsList[0].DataType is FixedSizeBinaryType);
            var fixedSizeType = (FixedSizeBinaryType)recordBatch.Schema.FieldsList[0].DataType;
            Assert.Equal(16, fixedSizeType.ByteWidth);

            var deserializedColumn = (Apache.Arrow.Arrays.FixedSizeBinaryArray)recordBatch.Column(0);
            Assert.Equal(1.23m, MemoryMarshal.Cast<byte, decimal>(deserializedColumn.GetBytes(0))[0]);
            Assert.Equal(3.17m, MemoryMarshal.Cast<byte, decimal>(deserializedColumn.GetBytes(1))[0]);
        }

        [Fact]
        public void TestSerializeDoubleColumn()
        {
            Column column = Column.Create(GlobalMemoryManager.Instance);
            column.Add(new DoubleValue(1.23));
            column.Add(new DoubleValue(3.17));

            var batch = new EventBatchData([column]);

            var serializer = new EventBatchSerializer();
            var bufferWriter = new ArrayBufferWriter<byte>();
            serializer.SerializeEventBatch(bufferWriter, batch, 2);

            var serializedBytes = bufferWriter.WrittenSpan.ToArray();

            MemoryStream memoryStream = new MemoryStream(serializedBytes);
            ArrowStreamReader reader = new ArrowStreamReader(memoryStream);
            var recordBatch = reader.ReadNextRecordBatch();

            Assert.True(recordBatch.Schema.FieldsList[0].DataType is DoubleType);

            var deserializedColumn = (Apache.Arrow.DoubleArray)recordBatch.Column(0);
            Assert.Equal(1.23, deserializedColumn.GetValue(0));
            Assert.Equal(3.17, deserializedColumn.GetValue(1));
        }

        [Fact]
        public void TestSerializeListColumn()
        {
            Column column = Column.Create(GlobalMemoryManager.Instance);
            column.Add(new ListValue(new Int64Value(1), new Int64Value(2)));
            column.Add(new ListValue(new Int64Value(3), new Int64Value(4), new Int64Value(5)));

            var batch = new EventBatchData([column]);

            var serializer = new EventBatchSerializer();
            var bufferWriter = new ArrayBufferWriter<byte>();
            serializer.SerializeEventBatch(bufferWriter, batch, 2);

            var serializedBytes = bufferWriter.WrittenSpan.ToArray();

            MemoryStream memoryStream = new MemoryStream(serializedBytes);
            ArrowStreamReader reader = new ArrowStreamReader(memoryStream);
            var recordBatch = reader.ReadNextRecordBatch();

            Assert.True(recordBatch.Schema.FieldsList[0].DataType is ListType);

            var deserializedColumn = (Apache.Arrow.ListArray)recordBatch.Column(0);
            Assert.Equal(2, deserializedColumn.GetValueLength(0));
            Assert.Equal(3, deserializedColumn.GetValueLength(1));

            var valueOffset = deserializedColumn.ValueOffsets[0];

            var valuesArray = (Apache.Arrow.Int64Array)deserializedColumn.Values;
            Assert.Equal(1, valuesArray.GetValue(valueOffset));
            Assert.Equal(2, valuesArray.GetValue(valueOffset + 1));

            valueOffset = deserializedColumn.ValueOffsets[1];

            Assert.Equal(3, valuesArray.GetValue(valueOffset));
            Assert.Equal(4, valuesArray.GetValue(valueOffset + 1));
            Assert.Equal(5, valuesArray.GetValue(valueOffset + 2));
        }

        [Fact]
        public void SerializeMapColumn()
        {
            Column column = Column.Create(GlobalMemoryManager.Instance);
            column.Add(new MapValue(new KeyValuePair<IDataValue, IDataValue>(new StringValue("a"), new Int64Value(2)), new KeyValuePair<IDataValue, IDataValue>(new StringValue("b"), new Int64Value(4))));
            column.Add(new MapValue(new KeyValuePair<IDataValue, IDataValue>(new StringValue("a"), new Int64Value(5))));

            var batch = new EventBatchData([column]);
            var serializer = new EventBatchSerializer();
            var bufferWriter = new ArrayBufferWriter<byte>();

            serializer.SerializeEventBatch(bufferWriter, batch, 2);

            var serializedBytes = bufferWriter.WrittenSpan.ToArray();

            MemoryStream memoryStream = new MemoryStream(serializedBytes);
            ArrowStreamReader reader = new ArrowStreamReader(memoryStream);

            var recordBatch = reader.ReadNextRecordBatch();

            Assert.True(recordBatch.Schema.FieldsList[0].DataType is MapType);

            var deserializedColumn = (Apache.Arrow.MapArray)recordBatch.Column(0);
            var offset = deserializedColumn.ValueOffsets[0];
            var keys = (Apache.Arrow.StringArray)deserializedColumn.Keys;
            var values = (Apache.Arrow.Int64Array)deserializedColumn.Values;

            Assert.Equal(2, deserializedColumn.GetValueLength(0));
            Assert.Equal(1, deserializedColumn.GetValueLength(1));

            Assert.Equal("a", keys.GetString(offset));
            Assert.Equal(2, values.GetValue(offset));
            Assert.Equal("b", keys.GetString(offset + 1));
            Assert.Equal(4, values.GetValue(offset + 1));

            offset = deserializedColumn.ValueOffsets[1];

            Assert.Equal("a", keys.GetString(offset));
            Assert.Equal(5, values.GetValue(offset));
        }

        [Fact]
        public void TestSerializeStringColumn()
        {
            Column column = Column.Create(GlobalMemoryManager.Instance);
            column.Add(new StringValue("a"));
            column.Add(new StringValue("b"));

            var batch = new EventBatchData([column]);
            var serializer = new EventBatchSerializer();
            var bufferWriter = new ArrayBufferWriter<byte>();

            serializer.SerializeEventBatch(bufferWriter, batch, 2);

            var serializedBytes = bufferWriter.WrittenSpan.ToArray();

            MemoryStream memoryStream = new MemoryStream(serializedBytes);
            ArrowStreamReader reader = new ArrowStreamReader(memoryStream);

            var recordBatch = reader.ReadNextRecordBatch();

            Assert.True(recordBatch.Schema.FieldsList[0].DataType is StringType);

            var deserializedColumn = (Apache.Arrow.StringArray)recordBatch.Column(0);

            Assert.Equal("a", deserializedColumn.GetString(0));
            Assert.Equal("b", deserializedColumn.GetString(1));
        }

        [Fact]
        public void TestSerializeUnionColumn()
        {
            Column column = Column.Create(GlobalMemoryManager.Instance);
            column.Add(new StringValue("a"));
            column.Add(new Int64Value(17));
            column.Add(new StringValue("b"));
            column.Add(NullValue.Instance);

            var batch = new EventBatchData([column]);
            var serializer = new EventBatchSerializer();
            var bufferWriter = new ArrayBufferWriter<byte>();

            serializer.SerializeEventBatch(bufferWriter, batch, 2);

            var serializedBytes = bufferWriter.WrittenSpan.ToArray();

            MemoryStream memoryStream = new MemoryStream(serializedBytes);
            ArrowStreamReader reader = new ArrowStreamReader(memoryStream);

            var recordBatch = reader.ReadNextRecordBatch();

            Assert.True(recordBatch.Schema.FieldsList[0].DataType is UnionType);

            var deserializedColumn = (Apache.Arrow.DenseUnionArray)recordBatch.Column(0);

            Assert.Equal(1, deserializedColumn.TypeIds[0]);
            Assert.Equal(2, deserializedColumn.TypeIds[1]);
            Assert.Equal(1, deserializedColumn.TypeIds[2]);
            Assert.Equal(0, deserializedColumn.TypeIds[3]);

            Assert.Equal(0, deserializedColumn.ValueOffsets[0]);
            Assert.Equal(0, deserializedColumn.ValueOffsets[1]);
            Assert.Equal(1, deserializedColumn.ValueOffsets[2]);
            Assert.Equal(0, deserializedColumn.ValueOffsets[3]);

            var nullArray = (Apache.Arrow.NullArray)deserializedColumn.Fields[0];
            var stringArray = (Apache.Arrow.StringArray)deserializedColumn.Fields[1];
            var intArray = (Apache.Arrow.Int64Array)deserializedColumn.Fields[2];

            Assert.Equal(1, nullArray.NullCount);
            Assert.Equal(1, nullArray.Length);
            Assert.Equal("a", stringArray.GetString(0));
            Assert.Equal(17, intArray.GetValue(0));
            Assert.Equal("b", stringArray.GetString(1));
        }
    }
}
