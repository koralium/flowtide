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

using FlowtideDotNet.Core.ColumnStore.DataColumns;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.ColumnStore.Utils;
using FlowtideDotNet.Storage.Memory;
using System.Buffers;
using System.Buffers.Binary;
using System.Diagnostics.CodeAnalysis;
using System.Text;

namespace FlowtideDotNet.Core.ColumnStore.Serialization
{
    /// <summary>
    /// Deserializer that aims to not allocate any extra memory except for the buffers that are needed to store the data.
    /// </summary>
    public ref struct EventBatchDeserializer
    {
        private ReadOnlySpan<byte> _schemaBytes;
        private ReadOnlySpan<byte> _recordBatchHeaderBytes;

        private readonly IMemoryAllocator memoryAllocator;
        private readonly IBatchDecompressor? decompressor;
        private int bufferIndex;
        private int fieldNodeIndex;
        private int readDataIndex;
        private int bufferStart;
        private bool isCompressed;
        private bool _schemaRead;

        public EventBatchDeserializer(IMemoryAllocator memoryAllocator, IBatchDecompressor? decompressor = default)
        {
            this.memoryAllocator = memoryAllocator;
            this.decompressor = decompressor;
        }

        private void ReadSchemaFromSequence(ref SequenceReader<byte> data)
        {
            if (data.TryReadLittleEndian(out int magicNumber))
            {
                if (magicNumber != -1)
                {
                    throw new Exception("Invalid magic number");
                }
            }
            else
            {
                throw new Exception("Failed to read magic number");
            }

            if (!data.TryReadLittleEndian(out int messageLength))
            {
                throw new Exception("Failed to read message length");
            }
            if (data.UnreadSpan.Length < messageLength)
            {
                throw new Exception("Not enough data to read schema message");
            }

            _schemaBytes = data.UnreadSpan.Slice(0, messageLength);
            data.Advance(messageLength);
        }

        private void ReadRecordBatchHeaderFromSequence(ref SequenceReader<byte> data)
        {
            if (!data.TryReadLittleEndian(out int magicNumber))
            {
                throw new Exception("Failed to read magic number");
            }
            if (magicNumber != -1)
            {
                throw new Exception("Invalid magic number");
            }
            if (!data.TryReadLittleEndian(out int messageLength))
            {
                throw new Exception("Failed to read message length");
            }

            if (data.UnreadSpan.Length < messageLength)
            {
                throw new Exception("Not enough data to read record batch message");
            }

            _recordBatchHeaderBytes = data.UnreadSpan.Slice(0, messageLength);
            data.Advance(messageLength);
        }

        public void SetSchema(ReadOnlySpan<byte> schemaBytes)
        {
            if (BinaryPrimitives.ReadInt32LittleEndian(schemaBytes) == -1)
            {
                // Skip the first 8 bytes to remove the IPC header
                schemaBytes = schemaBytes.Slice(8);
            }
            _schemaBytes = schemaBytes;
            _schemaRead = true;
        }

        public EventBatchDeserializeResult DeserializeBatch(ref SequenceReader<byte> data)
        {
            if (!_schemaRead)
            {
                ReadSchemaFromSequence(ref data);
                _schemaRead = true;
            }

            ReadRecordBatchHeaderFromSequence(ref data);

            var message = MessageStruct.GetRootAsMessage(ref _schemaBytes, 0);

            var headerType = message.HeaderType;

            if (headerType != MessageHeader.Schema)
            {
                throw new Exception("Expecting schema message type as the first message");
            }

            var schema = new SchemaStruct(_schemaBytes, message.HeaderPosition());
            var fieldsLength = schema.FieldsLength;

            var recordBatchMessage = MessageStruct.GetRootAsMessage(ref _recordBatchHeaderBytes, 0);
            var recordBatchHeader = new RecordBatchStruct(_recordBatchHeaderBytes, recordBatchMessage.HeaderPosition());

            if (recordBatchHeader.HasCompression)
            {
                var compressionInfo = recordBatchHeader.Compression;
                if (compressionInfo.Codec != CompressionType.ZSTD)
                {
                    throw new NotSupportedException("Only zstd compression is supported at this time");
                }
                if (compressionInfo.Method != BodyCompressionMethod.BUFFER)
                {
                    throw new NotSupportedException("Only buffer compression method is supported at this time");
                }
                isCompressed = true;
            }

            bufferStart = recordBatchHeader.BuffersStartIndex;
            IColumn[] columns = new IColumn[fieldsLength];
            for (int i = 0; i < fieldsLength; i++)
            {
                var field = new FieldStruct(_schemaBytes, schema.FieldPosition(i));
                if (decompressor != null)
                {
                    decompressor.ColumnChange(i);
                }
                columns[i] = DeserializeColumn(ref data, in field, in recordBatchHeader);
            }

            if (readDataIndex < recordBatchMessage.BodyLength)
            {
                // Padding at the end of the record batch, advance past the padding
                var padding = recordBatchMessage.BodyLength - readDataIndex;
                data.Advance(padding);
                readDataIndex += (int)padding;
            }
            if (readDataIndex > recordBatchMessage.BodyLength)
            {
                throw new Exception("Read past the end of the record batch");
            }
            if (fieldNodeIndex < recordBatchHeader.NodesLength)
            {
                throw new Exception("Not all field nodes were read");
            }

            return new EventBatchDeserializeResult(new EventBatchData(columns), (int)recordBatchHeader.Length);
        }

        public DataColumnsDeserializeResult DeserializeDataColumns(ref SequenceReader<byte> data)
        {
            if (!_schemaRead)
            {
                ReadSchemaFromSequence(ref data);
                _schemaRead = true;
            }

            ReadRecordBatchHeaderFromSequence(ref data);

            var message = MessageStruct.GetRootAsMessage(ref _schemaBytes, 0);

            var headerType = message.HeaderType;

            if (headerType != MessageHeader.Schema)
            {
                throw new Exception("Expecting schema message type as the first message");
            }

            var schema = new SchemaStruct(_schemaBytes, message.HeaderPosition());
            var fieldsLength = schema.FieldsLength;

            var recordBatchMessage = MessageStruct.GetRootAsMessage(ref _recordBatchHeaderBytes, 0);
            var recordBatchHeader = new RecordBatchStruct(_recordBatchHeaderBytes, recordBatchMessage.HeaderPosition());

            if (recordBatchHeader.HasCompression)
            {
                var compressionInfo = recordBatchHeader.Compression;
                if (compressionInfo.Codec != CompressionType.ZSTD)
                {
                    throw new NotSupportedException("Only zstd compression is supported at this time");
                }
                if (compressionInfo.Method != BodyCompressionMethod.BUFFER)
                {
                    throw new NotSupportedException("Only buffer compression method is supported at this time");
                }
                isCompressed = true;
            }

            bufferStart = recordBatchHeader.BuffersStartIndex;
            IDataColumn[] columns = new IDataColumn[fieldsLength];
            for (int i = 0; i < fieldsLength; i++)
            {
                var field = new FieldStruct(_schemaBytes, schema.FieldPosition(i));
                if (decompressor != null)
                {
                    decompressor.ColumnChange(i);
                }
                var fieldNode = ReadNextFieldNode(in recordBatchHeader);
                var dataColumnResult = DeserializeDataColumn(ref data, in field, in recordBatchHeader, (int)fieldNode.Length);
                columns[i] = dataColumnResult.dataColumn;
            }

            if (readDataIndex < recordBatchMessage.BodyLength)
            {
                // Padding at the end of the record batch, advance past the padding
                var padding = recordBatchMessage.BodyLength - readDataIndex;
                data.Advance(padding);
                readDataIndex += (int)padding;
            }
            if (readDataIndex > recordBatchMessage.BodyLength)
            {
                throw new Exception("Read past the end of the record batch");
            }
            if (fieldNodeIndex < recordBatchHeader.NodesLength)
            {
                throw new Exception("Not all field nodes were read");
            }

            return new DataColumnsDeserializeResult(columns, (int)recordBatchHeader.Length);
        }

        private FieldNodeStruct ReadNextFieldNode(ref readonly RecordBatchStruct recordBatchStruct)
        {
            var fieldNode = recordBatchStruct.Nodes(fieldNodeIndex);
            fieldNodeIndex++;
            return fieldNode;
        }

        private Column DeserializeColumn(
            ref SequenceReader<byte> data,
            ref readonly FieldStruct fieldStruct,
            ref readonly RecordBatchStruct recordBatchStruct)
        {
            var fieldNode = ReadNextFieldNode(in recordBatchStruct);

            BitmapList? validityList;

            if (fieldStruct.TypeType == ArrowType.Null)
            {
                return new Column((int)fieldNode.NullCount, default, new BitmapList(memoryAllocator), ArrowTypeId.Null, memoryAllocator);
            }

            if (fieldStruct.TypeType != ArrowType.Union)
            {
                if (TryReadNextBuffer(ref data, out var validityMemory))
                {
                    validityList = new BitmapList(validityMemory, (int)fieldNode.Length, memoryAllocator);
                }
                else
                {
                    validityList = new BitmapList(memoryAllocator);
                }
            }
            else
            {
                validityList = new BitmapList(memoryAllocator);
            }

            var dataColumnResult = DeserializeDataColumn(ref data, in fieldStruct, in recordBatchStruct, (int)fieldNode.Length);
            var finalColumn = new Column((int)fieldNode.NullCount, dataColumnResult.dataColumn, validityList, dataColumnResult.arrowTypeId, memoryAllocator);

            return finalColumn;
        }

        private struct DataColumnResult
        {
            public IDataColumn dataColumn;
            public ArrowTypeId arrowTypeId;

            public DataColumnResult(IDataColumn dataColumn, ArrowTypeId arrowTypeId)
            {
                this.dataColumn = dataColumn;
                this.arrowTypeId = arrowTypeId;
            }
        }

        private DataColumnResult DeserializeDataColumn(
            ref SequenceReader<byte> data,
            ref readonly FieldStruct fieldStruct,
            ref readonly RecordBatchStruct recordBatchStruct,
            int length)
        {
            switch (fieldStruct.TypeType)
            {
                case ArrowType.Null:
                    return new DataColumnResult(DeserializeNullColumn(in fieldStruct, in recordBatchStruct, length), ArrowTypeId.Null);
                case ArrowType.Int:
                    return new DataColumnResult(DeserializeIntegerColumn(ref data, in fieldStruct, in recordBatchStruct, length), ArrowTypeId.Int64);
                case ArrowType.Bool:
                    return new DataColumnResult(DeserializeBoolColumn(ref data, in fieldStruct, in recordBatchStruct, length), ArrowTypeId.Boolean);
                case ArrowType.Utf8:
                    return new DataColumnResult(DeserializeStringColumn(ref data, in fieldStruct, in recordBatchStruct, length), ArrowTypeId.String);
                case ArrowType.Binary:
                    return new DataColumnResult(DeserializeBinaryColumn(ref data, in fieldStruct, in recordBatchStruct, length), ArrowTypeId.Binary);
                case ArrowType.FixedSizeBinary:
                    return DeserializeFixedSizeBinaryColumn(ref data, in fieldStruct, in recordBatchStruct, length);
                case ArrowType.FloatingPoint:
                    return new DataColumnResult(DeserializeDoubleColumn(ref data, in fieldStruct, in recordBatchStruct, length), ArrowTypeId.Double);
                case ArrowType.List:
                    return new DataColumnResult(DeserializeListColumn(ref data, in fieldStruct, in recordBatchStruct, length), ArrowTypeId.List);
                case ArrowType.Map:
                    return new DataColumnResult(DeserializeMapColumn(ref data, in fieldStruct, in recordBatchStruct, length), ArrowTypeId.Map);
                case ArrowType.Union:
                    return new DataColumnResult(DeserializeUnionColumn(ref data, in fieldStruct, in recordBatchStruct, length), ArrowTypeId.Union);
                case ArrowType.Struct_:
                    return new DataColumnResult(DeserializeStructColumn(ref data, in fieldStruct, in recordBatchStruct, length), ArrowTypeId.Struct);
                default:
                    throw new NotImplementedException(fieldStruct.TypeType.ToString());
            }
        }

        private NullColumn DeserializeNullColumn(ref readonly FieldStruct fieldStruct, ref readonly RecordBatchStruct recordBatchStruct, int length)
        {
            return new NullColumn(length);
        }

        private UnionColumn DeserializeUnionColumn(
            ref SequenceReader<byte> data,
            ref readonly FieldStruct fieldStruct,
            ref readonly RecordBatchStruct recordBatchStruct,
            int length)
        {
            bool hasTypeMemory = TryReadNextBuffer(ref data, out var typeMemory);
            bool hasOffsetMemory = TryReadNextBuffer(ref data, out var offsetMemory);

            var childrenCount = fieldStruct.ChildrenLength;

            List<IDataColumn> children = new List<IDataColumn>();

            var nullFieldNode = ReadNextFieldNode(in recordBatchStruct);
            children.Add(new NullColumn((int)nullFieldNode.Length));

            for (int i = 1; i < childrenCount; i++)
            {
                var child = fieldStruct.Children(i);
                var fieldNode = ReadNextFieldNode(in recordBatchStruct);
                ExceptEmptyBuffer(ref data);
                children.Add(DeserializeDataColumn(ref data, in child, in recordBatchStruct, (int)fieldNode.Length).dataColumn);
            }

            if (hasTypeMemory && hasOffsetMemory)
            {
                return new UnionColumn(children, typeMemory!, offsetMemory!, length, memoryAllocator);
            }
            else
            {
                if (hasTypeMemory || hasOffsetMemory)
                {
                    throw new InvalidOperationException("Union column must have both type and offset memory");
                }
                // Dispose the children
                for (int i = 0; i < children.Count; i++)
                {
                    children[i].Dispose();
                }
                return new UnionColumn(memoryAllocator);
            }
        }

        private MapColumn DeserializeMapColumn(
            ref SequenceReader<byte> data,
            ref readonly FieldStruct fieldStruct,
            ref readonly RecordBatchStruct recordBatchStruct,
            int length)
        {
            if (fieldStruct.ChildrenLength != 1)
            {
                throw new InvalidOperationException("Map column must have exactly one child of type struct with two columns");
            }

            var structField = fieldStruct.Children(0);

            if (structField.ChildrenLength != 2)
            {
                throw new InvalidOperationException("Map column struct must have exactly two children");
            }

            var listFieldNode = ReadNextFieldNode(in recordBatchStruct);

            var keyField = structField.Children(0);
            var valueField = structField.Children(1);

            bool readOffsets = TryReadNextBuffer(ref data, out var offsetMemory);

            // Read validity buffer, skipped here
            ExceptEmptyBuffer(ref data);

            var keyColumn = DeserializeColumn(ref data, in keyField, in recordBatchStruct);
            var valueColumn = DeserializeColumn(ref data, in valueField, in recordBatchStruct);

            if (readOffsets)
            {
                return new MapColumn(keyColumn, valueColumn, offsetMemory!, length + 1, memoryAllocator);
            }

            // This should not happen normally
            keyColumn.Dispose();
            valueColumn.Dispose();
            return new MapColumn(memoryAllocator);

        }

        private ListColumn DeserializeListColumn(
            ref SequenceReader<byte> data,
            scoped ref readonly FieldStruct fieldStruct,
            scoped ref readonly RecordBatchStruct recordBatchStruct,
            int length)
        {
            if (fieldStruct.ChildrenLength != 1)
            {
                throw new InvalidOperationException("List column must have exactly one child");
            }

            var child = fieldStruct.Children(0);

            bool readOffsets = TryReadNextBuffer(ref data, out var offsetMemory);

            var internalColumn = DeserializeColumn(ref data, in child, in recordBatchStruct);

            if (readOffsets)
            {
                return new ListColumn(internalColumn, offsetMemory!, length + 1, memoryAllocator);
            }
            // If there was no offsets, ignore the read internal column since the column is empty
            // This should not happen normally
            internalColumn.Dispose();
            return new ListColumn(memoryAllocator);
        }

        private DoubleColumn DeserializeDoubleColumn(
            ref SequenceReader<byte> data,
            ref readonly FieldStruct fieldStruct,
            ref readonly RecordBatchStruct recordBatchStruct,
            int length)
        {
            if (TryReadNextBuffer(ref data, out var memory))
            {
                return new DoubleColumn(memory, length, memoryAllocator);
            }
            return new DoubleColumn(memoryAllocator);
        }

        private DataColumnResult DeserializeFixedSizeBinaryColumn(
            ref SequenceReader<byte> data,
            ref readonly FieldStruct fieldStruct,
            ref readonly RecordBatchStruct recordBatchStruct,
            int length)
        {
            if (fieldStruct.CustomMetadataLength < 1)
            {
                throw new InvalidOperationException("Fixed size binary column must have custom metadata to mark which data type it is");
            }

            bool foundExtension = false;
            ReadOnlySpan<byte> valueBytes = default;
            for (int i = 0; i < fieldStruct.CustomMetadataLength; i++)
            {
                var customMetadata = fieldStruct.CustomMetadata(i);

                if (customMetadata.KeyBytes.SequenceEqual("ARROW:extension:name"u8))
                {
                    valueBytes = customMetadata.ValueBytes;
                    foundExtension = true;
                }
            }

            if (!foundExtension)
            {
                throw new InvalidOperationException("Fixed size binary column must have custom metadata to mark which data type it is");
            }

            if (valueBytes.SequenceEqual("flowtide.floatingdecimaltype"u8))
            {
                return new DataColumnResult(DeserializeDecimalColumn(ref data, in fieldStruct, in recordBatchStruct, length), ArrowTypeId.Decimal128);
            }
            if (valueBytes.SequenceEqual("flowtide.timestamptz"u8))
            {
                return new DataColumnResult(DeserializeTimestampTzColumn(ref data, in fieldStruct, in recordBatchStruct, length), ArrowTypeId.Timestamp);
            }

            throw new NotImplementedException(Encoding.UTF8.GetString(valueBytes));
        }

        private TimestampTzColumn DeserializeTimestampTzColumn(
            ref SequenceReader<byte> data,
            ref readonly FieldStruct fieldStruct,
            ref readonly RecordBatchStruct recordBatchStruct,
            int length)
        {
            if (TryReadNextBuffer(ref data, out var memory))
            {
                return new TimestampTzColumn(memory, length, memoryAllocator);
            }
            return new TimestampTzColumn(memoryAllocator);
        }

        private DecimalColumn DeserializeDecimalColumn(
            ref SequenceReader<byte> data,
            ref readonly FieldStruct fieldStruct,
            ref readonly RecordBatchStruct recordBatchStruct,
            int length)
        {
            if (TryReadNextBuffer(ref data, out var memory))
            {
                return new DecimalColumn(memory, length, memoryAllocator);
            }
            return new DecimalColumn(memoryAllocator);
        }

        private BinaryColumn DeserializeBinaryColumn(
            ref SequenceReader<byte> data,
            ref readonly FieldStruct fieldStruct,
            ref readonly RecordBatchStruct recordBatchStruct,
            int length)
        {
            bool haveOffsetBuffer = TryReadNextBuffer(ref data, out var offsetMemory);
            bool haveDataBuffer = TryReadNextBuffer(ref data, out var dataMemory);
            if (haveDataBuffer && !haveOffsetBuffer)
            {
                throw new InvalidOperationException("Data buffer found without offset buffer");
            }
            if (!haveOffsetBuffer)
            {
                return new BinaryColumn(memoryAllocator);
            }
            return new BinaryColumn(offsetMemory!, length + 1, dataMemory, memoryAllocator);
        }

        private IntegerColumn DeserializeIntegerColumn(
            ref SequenceReader<byte> data,
            ref readonly FieldStruct fieldStruct,
            ref readonly RecordBatchStruct recordBatchStruct,
            int length)
        {
            var intType = fieldStruct.TypeAsInt();
            if (TryReadNextBuffer(ref data, out var memory))
            {
                return new IntegerColumn(memoryAllocator, memory, length, intType.BitWidth);
            }
            return new IntegerColumn(memoryAllocator);
        }

        private BoolColumn DeserializeBoolColumn(
            ref SequenceReader<byte> data,
            ref readonly FieldStruct fieldStruct,
            ref readonly RecordBatchStruct recordBatchStruct,
            int length)
        {
            if (TryReadNextBuffer(ref data, out var memory))
            {
                return new BoolColumn(memory, length, memoryAllocator);
            }
            return new BoolColumn(memoryAllocator);
        }

        private StringColumn DeserializeStringColumn(
            ref SequenceReader<byte> data,
            ref readonly FieldStruct fieldStruct,
            ref readonly RecordBatchStruct recordBatchStruct,
            int length)
        {
            bool haveOffsetBuffer = TryReadNextBuffer(ref data, out var offsetMemory);
            bool haveDataBuffer = TryReadNextBuffer(ref data, out var dataMemory);
            if (haveDataBuffer && !haveOffsetBuffer)
            {
                throw new InvalidOperationException("Data buffer found without offset buffer");
            }
            if (!haveOffsetBuffer)
            {
                return new StringColumn(memoryAllocator);
            }
            return new StringColumn(offsetMemory!, length + 1, dataMemory, memoryAllocator);
        }

        private StructColumn DeserializeStructColumn(
            ref SequenceReader<byte> data,
            ref readonly FieldStruct fieldStruct,
            ref readonly RecordBatchStruct recordBatchStruct,
            int length)
        {
            var childLength = fieldStruct.ChildrenLength;

            Column[] children = new Column[childLength];

            // This array might be able to be improved
            string[] fieldNames = new string[childLength];
            for (int i = 0; i < childLength; i++)
            {
                var field = fieldStruct.Children(i);
                fieldNames[i] = Encoding.UTF8.GetString(field.GetNameBytes());
                children[i] = DeserializeColumn(ref data, ref field, in recordBatchStruct);
            }

            StructHeader structHeader = StructHeader.Create(fieldNames);
            return new StructColumn(structHeader, children, length);
        }

        private void ExceptEmptyBuffer(ref SequenceReader<byte> data)
        {
            var bufferInfoSpan = _recordBatchHeaderBytes.Slice(bufferStart + (bufferIndex * 16));
            var bufferOffset = (int)BinaryPrimitives.ReadInt64LittleEndian(bufferInfoSpan);
            var bufferLength = (int)BinaryPrimitives.ReadInt64LittleEndian(bufferInfoSpan.Slice(8));
            bufferIndex++;

            var difference = bufferOffset - readDataIndex;
            if (difference != 0)
            {
                readDataIndex += difference;
                data.Advance(difference);
            }

            if (bufferLength != 0)
            {
                throw new InvalidOperationException("Expected empty buffer");
            }
        }

        private bool TryReadNextBuffer(ref SequenceReader<byte> data, [NotNullWhen(true)] out IMemoryOwner<byte>? memory)
        {
            var bufferInfoSpan = _recordBatchHeaderBytes.Slice(bufferStart + (bufferIndex * 16));
            var bufferOffset = (int)BinaryPrimitives.ReadInt64LittleEndian(bufferInfoSpan);
            var bufferLength = (int)BinaryPrimitives.ReadInt64LittleEndian(bufferInfoSpan.Slice(8));
            bufferIndex++;

            var difference = bufferOffset - readDataIndex;
            if (difference != 0)
            {
                readDataIndex += difference;
                data.Advance(difference);
            }

            if (bufferLength == 0)
            {
                memory = null;
                return false;
            }

            if (isCompressed)
            {
                if (!data.TryReadLittleEndian(out long uncompressedLength))
                {
                    throw new InvalidOperationException("Failed to read uncompressed length");
                }
                if (uncompressedLength == -1)
                {
                    memory = memoryAllocator.Allocate(bufferLength - 8, 64);
                    data.TryCopyTo(memory.Memory.Span.Slice(0, bufferLength - 8));
                    readDataIndex += bufferLength;
                    data.Advance(bufferLength - 8);
                }
                else
                {
                    memory = memoryAllocator.Allocate((int)uncompressedLength, 64);
                    if (data.UnreadSpan.Length < (bufferLength - 8))
                    {
                        throw new InvalidOperationException("Not enough data to read compressed buffer");
                    }
                    var compressedData = data.UnreadSpan.Slice(0, bufferLength - 8);
                    decompressor!.Unwrap(compressedData, memory.Memory.Span);
                    readDataIndex += bufferLength;
                    data.Advance(bufferLength - 8);
                }
            }
            else
            {
                memory = memoryAllocator.Allocate(bufferLength, 64);
                data.TryCopyTo(memory.Memory.Span.Slice(0, bufferLength));
                readDataIndex += bufferLength;
                data.Advance(bufferLength);
            }


            return true;
        }
    }
}
