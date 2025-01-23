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
using FlowtideDotNet.Core.ColumnStore.Utils;
using FlowtideDotNet.Core.Sources.Generic.Internal;
using FlowtideDotNet.Storage.Memory;
using Google.Protobuf;
using Microsoft.VisualBasic;
using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

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
        private SequenceReader<byte> data;
        private int bufferIndex;
        private int fieldNodeIndex;
        private int readDataIndex;
        private int bufferStart;

        public EventBatchDeserializer(IMemoryAllocator memoryAllocator, SequenceReader<byte> sequenceReader)
        {
            this.memoryAllocator = memoryAllocator;
            this.data = sequenceReader;
        }

        private void ReadSchemaFromSequence()
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

        private void ReadRecordBatchHeaderFromSequence()
        {
            //var schema = message.Schema;

            //var fieldsLength = schema.FieldsLength;

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

        public EventBatchData DeserializeBatch()
        {
            ReadSchemaFromSequence();
            ReadRecordBatchHeaderFromSequence();

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

            bufferStart = recordBatchHeader.BuffersStartIndex;
            IColumn[] columns = new IColumn[fieldsLength];
            for (int i = 0; i < fieldsLength; i++)
            {
                var field = new FieldStruct(_schemaBytes, schema.FieldPosition(i));
                columns[i] = DeserializeColumn(in field, in recordBatchHeader);
            }

            return new EventBatchData(columns);
        }

        private FieldNodeStruct ReadNextFieldNode(ref readonly RecordBatchStruct recordBatchStruct)
        {
            var fieldNode = recordBatchStruct.Nodes(fieldNodeIndex);
            fieldNodeIndex++;
            return fieldNode;
        }

        private Column DeserializeColumn(
            ref readonly FieldStruct fieldStruct, 
            ref readonly RecordBatchStruct recordBatchStruct)
        {
            var fieldNode = ReadNextFieldNode(in recordBatchStruct);

            BitmapList? validityList;
            if (fieldStruct.TypeType != ArrowType.Union)
            {
                if (TryReadNextBuffer(out var validityMemory))
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

            var dataColumnResult = DeserializeDataColumn(in fieldStruct, in recordBatchStruct, (int)fieldNode.Length);
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

        private DataColumnResult DeserializeDataColumn(ref readonly FieldStruct fieldStruct, ref readonly RecordBatchStruct recordBatchStruct, int length)
        {
            switch (fieldStruct.TypeType)
            {
                case ArrowType.Null:
                    return new DataColumnResult(DeserializeNullColumn(in fieldStruct, in recordBatchStruct, length), ArrowTypeId.Null);
                case ArrowType.Int:
                    return new DataColumnResult(DeserializeInt64Column(in fieldStruct, in recordBatchStruct, length), ArrowTypeId.Int64);
                case ArrowType.Bool:
                    return new DataColumnResult(DeserializeBoolColumn(in fieldStruct, in recordBatchStruct, length), ArrowTypeId.Boolean);
                case ArrowType.Utf8:
                    return new DataColumnResult(DeserializeStringColumn(in fieldStruct, in recordBatchStruct, length), ArrowTypeId.String);
                case ArrowType.Binary:
                    return new DataColumnResult(DeserializeBinaryColumn(in fieldStruct, in recordBatchStruct, length), ArrowTypeId.Binary);
                case ArrowType.FixedSizeBinary:
                    return DeserializeFixedSizeBinaryColumn(in fieldStruct, in recordBatchStruct, length);
                case ArrowType.FloatingPoint:
                    return new DataColumnResult(DeserializeDoubleColumn(in fieldStruct, in recordBatchStruct, length), ArrowTypeId.Double);
                case ArrowType.List:
                    return new DataColumnResult(DeserializeListColumn(in fieldStruct, in recordBatchStruct, length), ArrowTypeId.List);
                case ArrowType.Map:
                    return new DataColumnResult(DeserializeMapColumn(in fieldStruct, in recordBatchStruct, length), ArrowTypeId.Map);
                case ArrowType.Union:
                    return new DataColumnResult(DeserializeUnionColumn(in fieldStruct, in recordBatchStruct, length), ArrowTypeId.Union);
                default:
                    throw new NotImplementedException(fieldStruct.TypeType.ToString());
            }
        }

        private NullColumn DeserializeNullColumn(ref readonly FieldStruct fieldStruct, ref readonly RecordBatchStruct recordBatchStruct, int length)
        {
            return new NullColumn(length);
        }

        private UnionColumn DeserializeUnionColumn(
            ref readonly FieldStruct fieldStruct,
            ref readonly RecordBatchStruct recordBatchStruct,
            int length)
        {
            TryReadNextBuffer(out var typeMemory);
            TryReadNextBuffer(out var offsetMemory);

            var childrenCount = fieldStruct.ChildrenLength;

            List<IDataColumn> children = new List<IDataColumn>();

            var nullFieldNode = ReadNextFieldNode(in recordBatchStruct);
            children.Add(new NullColumn((int)nullFieldNode.Length));

            for (int i = 1; i < childrenCount; i++)
            {
                var child = fieldStruct.Children(i);
                var fieldNode = ReadNextFieldNode(in recordBatchStruct);
                ExceptEmptyBuffer();
                children.Add(DeserializeDataColumn(in child, in recordBatchStruct, (int)fieldNode.Length).dataColumn);
            }

            return new UnionColumn(children, typeMemory, offsetMemory, length, memoryAllocator);
        }

        private MapColumn DeserializeMapColumn(
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

            var keyField = structField.Children(0);
            var valueField = structField.Children(1);

            bool readOffsets = TryReadNextBuffer(out var offsetMemory);

            // Read validity buffer, skipped here
            ExceptEmptyBuffer();

            var keyColumn = DeserializeColumn(in keyField, in recordBatchStruct);
            var valueColumn = DeserializeColumn(in valueField, in recordBatchStruct);

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
            scoped ref readonly FieldStruct fieldStruct,
            scoped ref readonly RecordBatchStruct recordBatchStruct, 
            int length)
        {
            if (fieldStruct.ChildrenLength != 1)
            {
                throw new InvalidOperationException("List column must have exactly one child");
            }

            var child = fieldStruct.Children(0);

            bool readOffsets = TryReadNextBuffer(out var offsetMemory);

            var internalColumn = DeserializeColumn(in child, in recordBatchStruct);

            if (readOffsets)
            {
                return new ListColumn(internalColumn, offsetMemory!, length + 1, memoryAllocator);
            }
            // If there was no offsets, ignore the read internal column since the column is empty
            // This should not happen normally
            internalColumn.Dispose();
            return new ListColumn(memoryAllocator);
        }

        private DoubleColumn DeserializeDoubleColumn(ref readonly FieldStruct fieldStruct,
            ref readonly RecordBatchStruct recordBatchStruct,
            int length)
        {
            if (TryReadNextBuffer(out var memory))
            {
                return new DoubleColumn(memory, length, memoryAllocator);
            }
            return new DoubleColumn(memoryAllocator);
        }

        private DataColumnResult DeserializeFixedSizeBinaryColumn(ref readonly FieldStruct fieldStruct,
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
                return new DataColumnResult(DeserializeDecimalColumn(in fieldStruct, in recordBatchStruct, length), ArrowTypeId.Decimal128);
            }
            if (valueBytes.SequenceEqual("flowtide.timestamptz"u8))
            {
                return new DataColumnResult(DeserializeTimestampTzColumn(in fieldStruct, in recordBatchStruct, length), ArrowTypeId.Timestamp);
            }

            throw new NotImplementedException(Encoding.UTF8.GetString(valueBytes));
        }

        private TimestampTzColumn DeserializeTimestampTzColumn(ref readonly FieldStruct fieldStruct,
            ref readonly RecordBatchStruct recordBatchStruct,
            int length)
        {
            if (TryReadNextBuffer(out var memory))
            {
                return new TimestampTzColumn(memory, length, memoryAllocator);
            }
            return new TimestampTzColumn(memoryAllocator);
        }

        private DecimalColumn DeserializeDecimalColumn(ref readonly FieldStruct fieldStruct,
            ref readonly RecordBatchStruct recordBatchStruct,
            int length)
        {
            if (TryReadNextBuffer(out var memory))
            {
                return new DecimalColumn(memory, length, memoryAllocator);
            }
            return new DecimalColumn(memoryAllocator);
        }

        private BinaryColumn DeserializeBinaryColumn(ref readonly FieldStruct fieldStruct,
            ref readonly RecordBatchStruct recordBatchStruct,
            int length)
        {
            bool haveOffsetBuffer = TryReadNextBuffer(out var offsetMemory);
            bool haveDataBuffer = TryReadNextBuffer(out var dataMemory);
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

        private Int64Column DeserializeInt64Column(ref readonly FieldStruct fieldStruct,
            ref readonly RecordBatchStruct recordBatchStruct,
            int length)
        {
            if (TryReadNextBuffer(out var memory))
            {
                return new Int64Column(memory, length, memoryAllocator);
            }
            return new Int64Column(memoryAllocator);
        }

        private BoolColumn DeserializeBoolColumn(ref readonly FieldStruct fieldStruct,
            ref readonly RecordBatchStruct recordBatchStruct,
            int length)
        {
            if (TryReadNextBuffer(out var memory))
            {
                return new BoolColumn(memory, length, memoryAllocator);
            }
            return new BoolColumn(memoryAllocator);
        }

        private StringColumn DeserializeStringColumn(ref readonly FieldStruct fieldStruct,
            ref readonly RecordBatchStruct recordBatchStruct,
            int length)
        {
            bool haveOffsetBuffer = TryReadNextBuffer(out var offsetMemory);
            bool haveDataBuffer = TryReadNextBuffer(out var dataMemory);
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

        private void ExceptEmptyBuffer()
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

        private bool TryReadNextBuffer([NotNullWhen(true)] out IMemoryOwner<byte>? memory)
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

            memory = memoryAllocator.Allocate(bufferLength, 64);
            data.TryCopyTo(memory.Memory.Span.Slice(0, bufferLength));
            readDataIndex += bufferLength;
            data.Advance(bufferLength);
            return true;
        }
    }
}
