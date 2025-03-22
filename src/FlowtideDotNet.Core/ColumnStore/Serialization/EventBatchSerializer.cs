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

using FlowtideDotNet.Core.ColumnStore.Serialization.Serializer;
using System.Buffers;
using System.Buffers.Binary;

namespace FlowtideDotNet.Core.ColumnStore.Serialization
{
    internal class EventBatchSerializer
    {
        private int[] vtable;
        private int[] vtables;
        private int[] stackPointers;

        public EventBatchSerializer()
        {
            vtable = new int[256];
            vtables = new int[256];
            stackPointers = new int[256];
        }

        public SerializationEstimation GetSerializationEstimation(EventBatchData eventBatchData)
        {
            SerializationEstimation serializationEstimation = new SerializationEstimation();

            for (int i = 0; i < eventBatchData.Columns.Count; i++)
            {
                var estimate = eventBatchData.Columns[i].GetSerializationEstimate();
                serializationEstimation.fieldNodeCount += estimate.fieldNodeCount;
                serializationEstimation.bodyLength += estimate.bodyLength;
                serializationEstimation.bufferCount += estimate.bufferCount;
            }
            return serializationEstimation;
        }

        public int GetEstimatedBufferSize(SerializationEstimation serializationEstimation)
        {
            var overhead = (200 + (serializationEstimation.fieldNodeCount * 100)) * 2;
            return serializationEstimation.bodyLength + overhead + (serializationEstimation.bufferCount * 16);
        }

        /// <summary>
        /// Serializes an event batch data to an IBufferWriter
        /// </summary>
        /// <param name="bufferWriter">The writer to serialize to</param>
        /// <param name="eventBatchData">The data to serialize</param>
        public void SerializeEventBatch(IBufferWriter<byte> bufferWriter, EventBatchData eventBatchData, int count, IBatchCompressor? compressor = default)
        {
            var estimation = GetSerializationEstimation(eventBatchData);
            var size = GetEstimatedBufferSize(estimation);
            var buffer = bufferWriter.GetSpan(size);
            var writtenData = SerializeEventBatch(buffer, estimation, eventBatchData, count, compressor);
            bufferWriter.Advance(writtenData);
        }

        /// <summary>
        /// Serializes a batch directly to a span.
        /// This method enables writing directly to a span without the need for a buffer writer.
        /// This is required for flowtide to write compressed buffers since the recordbatch header must be updated when the data is written.
        /// </summary>
        /// <param name="destination"></param>
        /// <param name="serializationEstimation"></param>
        /// <param name="eventBatchData"></param>
        /// <param name="count"></param>
        /// <returns>The amount of bytes written to the destination</returns>
        public int SerializeEventBatch(
            Span<byte> destination,
            SerializationEstimation serializationEstimation,
            EventBatchData eventBatchData,
            int count,
            IBatchCompressor? compressor = default)
        {
            var schemaSpan = SerializeSchemaHeader(destination, eventBatchData, serializationEstimation);
            var recordBatchHeader = SerializeRecordBatchHeader(destination.Slice(schemaSpan.Length), eventBatchData, count, serializationEstimation, compressor != null);

            // Get the buffer span from the record batch to update lengths and offsets
            var writtenDataLength = SerializeRecordBatchData(destination.Slice(schemaSpan.Length + recordBatchHeader.Length), recordBatchHeader, eventBatchData, compressor);

            return schemaSpan.Length + recordBatchHeader.Length + writtenDataLength;
        }

        public int SerializeRecordBatchData(Span<byte> destination, Span<byte> recordBatchHeader, EventBatchData eventBatchData, IBatchCompressor? compressor = default)
        {
            ReadOnlySpan<byte> readOnlyHeaderSpan = recordBatchHeader;
            var message = MessageStruct.ReadMessage(in readOnlyHeaderSpan);
            var recordBatch = message.RecordBatch();
            var bufferSpan = recordBatchHeader.Slice(recordBatch.BuffersStartIndex, recordBatch.BuffersLength * 16);

            var writtenDataLength = SerializeBufferData(destination, bufferSpan, eventBatchData, compressor);

            // Write new body length since compression can have reduced it, but only if there is data, otherwise this value will not be written in the flatbuffer table
            if (writtenDataLength != 0)
            {
                BinaryPrimitives.WriteInt64LittleEndian(recordBatchHeader.Slice((int)message.BodyLengthIndex), writtenDataLength);
            }

            return writtenDataLength;
        }

        private int SerializeBufferData(Span<byte> destinationSpan, Span<byte> bufferSpan, EventBatchData eventBatchData, IBatchCompressor? compressor = default)
        {
            var arrowDataWriter = new ArrowDataWriter(destinationSpan, bufferSpan, compressor);
            for (int i = 0; i < eventBatchData.Columns.Count; i++)
            {
                if (compressor != null)
                {
                    // Notify compressor that we changed column, this allows it to change dictionaries if needed/in use
                    compressor.ColumnChange(i);
                }
                eventBatchData.Columns[i].WriteDataToBuffer(ref arrowDataWriter);
            }
            return arrowDataWriter.BodyLength;
        }

        public void SerializeRecordBatchHeader(IBufferWriter<byte> bufferWriter, EventBatchData eventBatchData, int count, SerializationEstimation serializationEstimation)
        {
            var overhead = 200 + (serializationEstimation.fieldNodeCount * 100) + (serializationEstimation.bufferCount * 16);
            var span = bufferWriter.GetSpan(overhead);
            var recordBatchSpan = SerializeRecordBatchHeader(span, eventBatchData, count, serializationEstimation, false);
            bufferWriter.Advance(recordBatchSpan.Length);
        }

        public Span<byte> SerializeRecordBatchHeader(
            Span<byte> destination,
            EventBatchData eventBatchData,
            int count,
            SerializationEstimation serializationEstimation,
            bool compressed)
        {
            var overhead = 200 + (serializationEstimation.fieldNodeCount * 100) + (serializationEstimation.bufferCount * 16);

            if (destination.Length < overhead)
            {
                throw new ArgumentException("Destination span is too small");
            }

            if ((serializationEstimation.fieldNodeCount * 2) > vtable.Length)
            {
                vtable = new int[serializationEstimation.fieldNodeCount * 2];
                vtables = new int[serializationEstimation.fieldNodeCount * 2];
                stackPointers = new int[serializationEstimation.fieldNodeCount * 2];
            }

            ArrowSerializer arrowSerializer = new ArrowSerializer(destination, vtable, vtables);

            arrowSerializer.RecordBatchStartNodesVector(serializationEstimation.fieldNodeCount);
            for (int i = eventBatchData.Columns.Count - 1; i >= 0; i--)
            {
                eventBatchData.Columns[i].AddFieldNodes(ref arrowSerializer);
            }
            var nodesPointer = arrowSerializer.EndVector();

            // Allocates the memory for the entire vector, then forward iteration is used to add the buffers since their offset must be added
            // Which is much easier to calculate in forward mode
            arrowSerializer.RecordBatchStartBuffersVectorForward(serializationEstimation.bufferCount);

            for (int i = 0; i < eventBatchData.Columns.Count; i++)
            {
                eventBatchData.Columns[i].AddBuffers(ref arrowSerializer);
            }

            var buffersPointer = arrowSerializer.EndVector();

            int compressionOffset = 0;
            if (compressed)
            {
                compressionOffset = arrowSerializer.CreateBodyCompression(CompressionType.ZSTD, BodyCompressionMethod.BUFFER);
            }

            var recordBatchPointer = arrowSerializer.CreateRecordBatch(count, nodesPointer, buffersPointer, compressionOffset);

            var messagePointer = arrowSerializer.CreateMessage(4, MessageHeader.RecordBatch, recordBatchPointer, arrowSerializer.BufferBodyLength);

            arrowSerializer.Finish(messagePointer);
            var padding = arrowSerializer.WriteMessageLengthAndPadding();
            var recordBatchSpan = arrowSerializer.CopyToStart(padding);

            return recordBatchSpan;
        }

        /// <summary>
        /// Serialize the schema to a buffer writer
        /// </summary>
        /// <param name="bufferWriter"></param>
        /// <param name="eventBatchData"></param>
        /// <param name="serializationEstimation"></param>
        public void SerializeSchemaHeader(IBufferWriter<byte> bufferWriter, EventBatchData eventBatchData, SerializationEstimation serializationEstimation)
        {
            var overhead = 200 + (serializationEstimation.fieldNodeCount * 100);
            var span = bufferWriter.GetSpan(overhead);
            var resultSpan = SerializeSchemaHeader(span, eventBatchData, serializationEstimation);
            bufferWriter.Advance(resultSpan.Length);
        }

        public Span<byte> SerializeSchemaHeader(Span<byte> destination, EventBatchData eventBatchData, SerializationEstimation serializationEstimation)
        {
            var overhead = 200 + (serializationEstimation.fieldNodeCount * 100);

            if (destination.Length < overhead)
            {
                throw new ArgumentException("Destination span is too small");
            }

            if ((serializationEstimation.fieldNodeCount * 2) > vtable.Length)
            {
                vtable = new int[serializationEstimation.fieldNodeCount * 2];
                vtables = new int[serializationEstimation.fieldNodeCount * 2];
                stackPointers = new int[serializationEstimation.fieldNodeCount * 2];
            }

            ArrowSerializer arrowSerializer = new ArrowSerializer(destination, vtable, vtables);

            var stack = stackPointers.AsSpan();
            var childrenStack = stack.Slice(eventBatchData.Columns.Count);

            var emptyStringPos = arrowSerializer.CreateEmptyString();
            for (int i = 0; i < eventBatchData.Columns.Count; i++)
            {
                stack[i] = eventBatchData.Columns[i].CreateSchemaField(ref arrowSerializer, emptyStringPos, childrenStack);
            }

            var fieldsPointer = arrowSerializer.SchemaCreateFieldsVector(stack.Slice(0, eventBatchData.Columns.Count));

            var schemaPointer = arrowSerializer.CreateSchema(0, fieldsPointer);

            var messagePointer = arrowSerializer.CreateMessage(4, MessageHeader.Schema, schemaPointer);

            arrowSerializer.Finish(messagePointer);
            var schemaPadding = arrowSerializer.WriteMessageLengthAndPadding();
            var resultSpan = arrowSerializer.CopyToStart(schemaPadding);
            return resultSpan;
        }
    }
}
