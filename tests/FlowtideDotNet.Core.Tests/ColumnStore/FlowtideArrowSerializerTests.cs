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

using Apache.Arrow.Compression;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.Comparers;
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
using ZstdSharp;

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
        public void TestSerializeMapColumn()
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

        [Fact]
        public void TestSerializeDeserializeEmptyUnionColumn()
        {
            Column column = Column.Create(GlobalMemoryManager.Instance);
            column.Add(new StringValue("a"));
            column.Add(new Int64Value(17));

            column.RemoveAt(1);
            column.RemoveAt(0);

            var batch = new EventBatchData([column]);
            var serializer = new EventBatchSerializer();
            var bufferWriter = new ArrayBufferWriter<byte>();

            serializer.SerializeEventBatch(bufferWriter, batch, 2);

            var serializedBytes = bufferWriter.WrittenSpan.ToArray();

            var reader = new SequenceReader<byte>(new ReadOnlySequence<byte>(serializedBytes));

            EventBatchDeserializer batchDeserializer = new EventBatchDeserializer(GlobalMemoryManager.Instance, reader);
            var deserializedBatch = batchDeserializer.DeserializeBatch();
            Assert.Equal(batch, deserializedBatch, new EventBatchDataComparer());

            column.Add(new Int64Value(3));
            deserializedBatch.Columns[0].Add(new Int64Value(3));

            Assert.Equal(batch, deserializedBatch, new EventBatchDataComparer());
        }

        [Fact]
        public void TestSerializeTimestampTzColumn()
        {
            var date = DateTime.UtcNow;
            var secondDate = date.AddSeconds(1);
            Column column = Column.Create(GlobalMemoryManager.Instance);
            column.Add(new TimestampTzValue(date));
            column.Add(new TimestampTzValue(secondDate));

            var batch = new EventBatchData([column]);
            var serializer = new EventBatchSerializer();
            var bufferWriter = new ArrayBufferWriter<byte>();

            serializer.SerializeEventBatch(bufferWriter, batch, 2);

            var serializedBytes = bufferWriter.WrittenSpan.ToArray();

            MemoryStream memoryStream = new MemoryStream(serializedBytes);
            ArrowStreamReader reader = new ArrowStreamReader(memoryStream);

            var recordBatch = reader.ReadNextRecordBatch();

            Assert.True(recordBatch.Schema.FieldsList[0].Metadata.TryGetValue("ARROW:extension:name", out var customExtensionName));
            Assert.Equal("flowtide.timestamptz", customExtensionName);

            Assert.True(recordBatch.Schema.FieldsList[0].DataType is FixedSizeBinaryType);
            var fixedSizeType = (FixedSizeBinaryType)recordBatch.Schema.FieldsList[0].DataType;
            Assert.Equal(16, fixedSizeType.ByteWidth);

            var deserializedColumn = (Apache.Arrow.Arrays.FixedSizeBinaryArray)recordBatch.Column(0);
            Assert.Equal(date, MemoryMarshal.Cast<byte, TimestampTzValue>(deserializedColumn.GetBytes(0))[0].ToDateTimeOffset());
            Assert.Equal(secondDate, MemoryMarshal.Cast<byte, TimestampTzValue>(deserializedColumn.GetBytes(1))[0].ToDateTimeOffset());
        }

        [Fact]
        public void TestSerializeColumnWithNull()
        {
            Column column = Column.Create(GlobalMemoryManager.Instance);
            column.Add(new StringValue("a"));
            column.Add(NullValue.Instance);
            column.Add(new StringValue("b"));

            var batch = new EventBatchData([column]);
            var serializer = new EventBatchSerializer();
            var bufferWriter = new ArrayBufferWriter<byte>();

            serializer.SerializeEventBatch(bufferWriter, batch, 2);

            var serializedBytes = bufferWriter.WrittenSpan.ToArray();

            MemoryStream memoryStream = new MemoryStream(serializedBytes);
            ArrowStreamReader reader = new ArrowStreamReader(memoryStream);

            var recordBatch = reader.ReadNextRecordBatch();
            Assert.NotNull(recordBatch);

            Assert.True(recordBatch.Schema.FieldsList[0].DataType is StringType);

            var deserializedColumn = (Apache.Arrow.StringArray)recordBatch.Column(0);

            Assert.Equal("a", deserializedColumn.GetString(0));
            Assert.True(deserializedColumn.IsNull(1));
            Assert.Equal("b", deserializedColumn.GetString(2));
        }

        [Fact]
        public void TestSerializeTwoColumnsIntAndString()
        {
            Column intColumn = Column.Create(GlobalMemoryManager.Instance);
            intColumn.Add(new Int64Value(1));
            intColumn.Add(new Int64Value(2));

            Column stringColumn = Column.Create(GlobalMemoryManager.Instance);
            stringColumn.Add(new StringValue("a"));
            stringColumn.Add(new StringValue("b"));

            var batch = new EventBatchData([intColumn, stringColumn]);
            var serializer = new EventBatchSerializer();
            var bufferWriter = new ArrayBufferWriter<byte>();

            serializer.SerializeEventBatch(bufferWriter, batch, 2);

            var serializedBytes = bufferWriter.WrittenSpan.ToArray();

            MemoryStream memoryStream = new MemoryStream(serializedBytes);
            ArrowStreamReader reader = new ArrowStreamReader(memoryStream);

            var recordBatch = reader.ReadNextRecordBatch();
            Assert.NotNull(recordBatch);

            Assert.True(recordBatch.Schema.FieldsList[0].DataType is Int64Type);
            Assert.True(recordBatch.Schema.FieldsList[1].DataType is StringType);

            var deserializedIntColumn = (Apache.Arrow.Int64Array)recordBatch.Column(0);
            var deserializedStringColumn = (Apache.Arrow.StringArray)recordBatch.Column(1);

            Assert.Equal(1, deserializedIntColumn.GetValue(0));
            Assert.Equal(2, deserializedIntColumn.GetValue(1));

            Assert.Equal("a", deserializedStringColumn.GetString(0));
            Assert.Equal("b", deserializedStringColumn.GetString(1));
        }

        private void SerializeDeserializeTest(params IDataValue[] values)
        {
            Column column = Column.Create(GlobalMemoryManager.Instance);

            for (int i = 0; i < values.Length; i++)
            {
                column.Add(values[i]);
            }

            var batch = new EventBatchData([column]);

            var serializer = new EventBatchSerializer();
            var bufferWriter = new ArrayBufferWriter<byte>();
            serializer.SerializeEventBatch(bufferWriter, batch, values.Length);

            var serializedBytes = bufferWriter.WrittenSpan.ToArray();

            var reader = new SequenceReader<byte>(new ReadOnlySequence<byte>(serializedBytes));

            EventBatchDeserializer batchDeserializer = new EventBatchDeserializer(GlobalMemoryManager.Instance, reader);
            var deserializedBatch = batchDeserializer.DeserializeBatch();

            Assert.Equal(batch, deserializedBatch, new EventBatchDataComparer());
        }

        [Fact]
        public void TestSerializeDeserializeIntegerColumn()
        {
            SerializeDeserializeTest(
                new Int64Value(1), 
                new Int64Value(2));
        }

        [Fact]
        public void TestSerializeDeserializeBoolColumn()
        {
            SerializeDeserializeTest(
                new BoolValue(true), 
                new BoolValue(false), 
                new BoolValue(true));
        }

        [Fact]
        public void TestSerializeDeserializeStringColumn()
        {
            SerializeDeserializeTest(
                new StringValue("a"),
                new StringValue("b"));
        }

        [Fact]
        public void TestSerializeDeserializeStringColumnWithNull()
        {
            SerializeDeserializeTest(
                new StringValue("a"),
                new StringValue("b"),
                NullValue.Instance);
        }

        [Fact]
        public void TestSerializeDeserializeBinaryColumn()
        {
            SerializeDeserializeTest(
                new BinaryValue(Encoding.UTF8.GetBytes("a")),
                new BinaryValue(Encoding.UTF8.GetBytes("abc")));
        }

        [Fact]
        public void TestSerializeDeserializeDecimalColumn()
        {
            SerializeDeserializeTest(
                new DecimalValue(1.23m),
                new DecimalValue(3.17m));
        }

        [Fact]
        public void TestSerializeDeserializeTimestampTzColumn()
        {
            var date = DateTime.UtcNow;
            var secondDate = date.AddSeconds(1);

            SerializeDeserializeTest(
                new TimestampTzValue(date),
                new TimestampTzValue(secondDate));
        }

        [Fact]
        public void TestSerializeDeserializeDoubleColumn()
        {
            SerializeDeserializeTest(
                new DoubleValue(1.23),
                new DoubleValue(3.17));
        }

        [Fact]
        public void TestSerializeDeserializeListColumn()
        {
            SerializeDeserializeTest(
                new ListValue(new Int64Value(1), new Int64Value(2)),
                new ListValue(new Int64Value(3), new Int64Value(4), new Int64Value(5)));
        }

        [Fact]
        public void TestSerializeDeserializeEmptyListColumn()
        {
            Column column = Column.Create(GlobalMemoryManager.Instance);

            column.Add(new ListValue(new Int64Value(1), new Int64Value(2)));

            column.RemoveAt(0);

            var batch = new EventBatchData([column]);

            var serializer = new EventBatchSerializer();
            var bufferWriter = new ArrayBufferWriter<byte>();
            serializer.SerializeEventBatch(bufferWriter, batch, 0);

            var serializedBytes = bufferWriter.WrittenSpan.ToArray();

            var reader = new SequenceReader<byte>(new ReadOnlySequence<byte>(serializedBytes));

            EventBatchDeserializer batchDeserializer = new EventBatchDeserializer(GlobalMemoryManager.Instance, reader);
            var deserializedBatch = batchDeserializer.DeserializeBatch();

            Assert.Equal(batch, deserializedBatch, new EventBatchDataComparer());
        }

        [Fact]
        public void TestSerializeDeserializeColumnWithNullOnly()
        {
            Column column = Column.Create(GlobalMemoryManager.Instance);

            column.Add(NullValue.Instance);


            var batch = new EventBatchData([column]);

            var serializer = new EventBatchSerializer();
            var bufferWriter = new ArrayBufferWriter<byte>();
            serializer.SerializeEventBatch(bufferWriter, batch, 0);

            var serializedBytes = bufferWriter.WrittenSpan.ToArray();

            var reader = new SequenceReader<byte>(new ReadOnlySequence<byte>(serializedBytes));

            EventBatchDeserializer batchDeserializer = new EventBatchDeserializer(GlobalMemoryManager.Instance, reader);
            var deserializedBatch = batchDeserializer.DeserializeBatch();

            Assert.Equal(batch, deserializedBatch, new EventBatchDataComparer());

            deserializedBatch.Columns[0].Add(NullValue.Instance);
            deserializedBatch.Columns[0].Add(new StringValue("a"));

            column.Add(NullValue.Instance);
            column.Add(new StringValue("a"));

            Assert.Equal(batch, deserializedBatch, new EventBatchDataComparer());

        }

        [Fact]
        public void TestSerializeDeserializeStringColumnWithNullOnly()
        {
            Column column = Column.Create(GlobalMemoryManager.Instance);

            column.Add(new StringValue("a"));
            column.Add(NullValue.Instance);

            column.RemoveAt(0);

            var batch = new EventBatchData([column]);

            var serializer = new EventBatchSerializer();
            var bufferWriter = new ArrayBufferWriter<byte>();
            serializer.SerializeEventBatch(bufferWriter, batch, 0);

            var serializedBytes = bufferWriter.WrittenSpan.ToArray();

            var reader = new SequenceReader<byte>(new ReadOnlySequence<byte>(serializedBytes));

            EventBatchDeserializer batchDeserializer = new EventBatchDeserializer(GlobalMemoryManager.Instance, reader);
            var deserializedBatch = batchDeserializer.DeserializeBatch();

            Assert.Equal(batch, deserializedBatch, new EventBatchDataComparer());

            deserializedBatch.Columns[0].Add(NullValue.Instance);
            deserializedBatch.Columns[0].Add(new StringValue("a"));

            column.Add(NullValue.Instance);
            column.Add(new StringValue("a"));

            Assert.Equal(batch, deserializedBatch, new EventBatchDataComparer());

        }

        [Fact]
        public void TestSerializeDeserializeMapColumn()
        {
            SerializeDeserializeTest(
                new MapValue(new KeyValuePair<IDataValue, IDataValue>(new StringValue("a"), new Int64Value(2)), new KeyValuePair<IDataValue, IDataValue>(new StringValue("b"), new Int64Value(4))),
                new MapValue(new KeyValuePair<IDataValue, IDataValue>(new StringValue("a"), new Int64Value(5))));
        }

        [Fact]
        public void TestSerializeDeserializeUnionColumn()
        {
            SerializeDeserializeTest(
                new StringValue("a"),
                new Int64Value(17),
                new StringValue("b"),
                NullValue.Instance);
        }

        [Fact]
        public void TestSerializeDeserializeUnionColumnWithDecimalAndTimestamp()
        {
            SerializeDeserializeTest(
                new StringValue("a"),
                new Int64Value(17),
                new StringValue("b"),
                NullValue.Instance,
                new DecimalValue(3.17m),
                new TimestampTzValue(DateTime.Now));
        }

        class BatchCompressor : IBatchCompressor
        {
            Compressor compressor;
            public BatchCompressor(Compressor compressor)
            {
                this.compressor = compressor;
            }
            public void ColumnChange(int columnIndex)
            {
            }

            public int Wrap(ReadOnlySpan<byte> input, Span<byte> output)
            {
                return compressor.Wrap(input, output);
            }
        }

        class BatchDecompressor : IBatchDecompressor
        {
            Decompressor decompressor;
            public BatchDecompressor(Decompressor decompressor)
            {
                this.decompressor = decompressor;
            }
            public void ColumnChange(int columnIndex)
            {
            }

            public int Unwrap(ReadOnlySpan<byte> input, Span<byte> output)
            {
                return decompressor.Unwrap(input, output);
            }
        }

        class TrainingBatchCompressor : IBatchCompressor
        {
            
            private List<byte[]> dictList = new List<byte[]>();
            public TrainingBatchCompressor()
            {
            }
            public void ColumnChange(int columnIndex)
            {
            }

            public int Wrap(ReadOnlySpan<byte> input, Span<byte> output)
            {
                if (input.Length == 0)
                {
                    return 0;
                }
                dictList.Add(input.ToArray());
                return 0;
            }

            public byte[] GetDictionary()
            {
                return DictBuilder.TrainFromBuffer(dictList);
            }
        }

        [Fact]
        public void TestSerializeStringColumnCompressed()
        {
            Column column = Column.Create(GlobalMemoryManager.Instance);

            for (int i = 0; i < 1000; i++)
            {
                column.Add(new StringValue("a"));
                column.Add(new StringValue("b"));
            }

            var batch = new EventBatchData([column]);
            var serializer = new EventBatchSerializer();
            var bufferWriter = new ArrayBufferWriter<byte>();

            serializer.SerializeEventBatch(bufferWriter, batch, column.Count, new BatchCompressor(new Compressor()));

            var serializedBytes = bufferWriter.WrittenSpan.ToArray();

            File.WriteAllBytes("compressed.arrow", serializedBytes);

            MemoryStream memoryStream = new MemoryStream(serializedBytes);
            ArrowStreamReader reader = new ArrowStreamReader(memoryStream, new CompressionCodecFactory());
            var recordBatch = reader.ReadNextRecordBatch();

            Assert.Equal(2000, recordBatch.Length);
        }

        [Fact]
        public void TestSerializeDeserializeStringColumnCompressed()
        {
            Column column = Column.Create(GlobalMemoryManager.Instance);

            for (int i = 0; i < 1000; i++)
            {
                column.Add(new StringValue("a"));
                column.Add(new StringValue("b"));
            }

            var batch = new EventBatchData([column]);
            var serializer = new EventBatchSerializer();
            var bufferWriter = new ArrayBufferWriter<byte>();

            serializer.SerializeEventBatch(bufferWriter, batch, column.Count, new BatchCompressor(new Compressor()));

            var serializedBytes = bufferWriter.WrittenSpan.ToArray();

            var reader = new SequenceReader<byte>(new ReadOnlySequence<byte>(serializedBytes));

            EventBatchDeserializer batchDeserializer = new EventBatchDeserializer(GlobalMemoryManager.Instance, reader, new BatchDecompressor(new Decompressor()));
            var deserializedBatch = batchDeserializer.DeserializeBatch();

            Assert.Equal(batch, deserializedBatch, new EventBatchDataComparer());
        }

        [Fact]
        public void TestSerializeDeserializeWithoutSchemaWritten()
        {
            // Test that tests the scenario where the schema is not saved on disk but stored elsewhere

            Column column = Column.Create(GlobalMemoryManager.Instance);

            for (int i = 0; i < 10; i++)
            {
                column.Add(new StringValue("a"));
                column.Add(new StringValue("b"));
            }

            var batch = new EventBatchData([column]);
            var serializer = new EventBatchSerializer();
            var bufferWriter = new ArrayBufferWriter<byte>();

            var estimation = serializer.GetSerializationEstimation(batch);
            var bufferSize = serializer.GetEstimatedBufferSize(estimation);

            var span = bufferWriter.GetSpan(bufferSize);

            // Save the schema to an byte array
            var savedSchema = serializer.SerializeSchemaHeader(span, batch, estimation).ToArray();

            // Overwrite the schema with the recordbatch
            var recordBatchHeader = serializer.SerializeRecordBatchHeader(span, batch, batch.Count, estimation, false);

            var writtenDataLength = serializer.SerializeRecordBatchData(span.Slice(recordBatchHeader.Length), recordBatchHeader, batch);

            bufferWriter.Advance(recordBatchHeader.Length + writtenDataLength);

            var serializedBytes = bufferWriter.WrittenSpan.ToArray();

            var reader = new SequenceReader<byte>(new ReadOnlySequence<byte>(serializedBytes));

            EventBatchDeserializer batchDeserializer = new EventBatchDeserializer(GlobalMemoryManager.Instance, reader);
            // Set the schema for the deserializer
            batchDeserializer.SetSchema(savedSchema);
            var deserializedBatch = batchDeserializer.DeserializeBatch();

            Assert.Equal(batch, deserializedBatch, new EventBatchDataComparer());
        }
    }
}
