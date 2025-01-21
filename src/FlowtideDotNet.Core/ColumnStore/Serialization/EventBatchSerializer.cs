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
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using static SqlParser.Ast.FetchDirection;

namespace FlowtideDotNet.Core.ColumnStore.Serialization
{
    internal class EventBatchSerializer
    {
        private int[] vtable;
        private int[] vtables;
        private int[] stackPointers;
        private byte[] memory;
        private bool _schemaWritten;
        private int _space;

        public EventBatchSerializer()
        {
            memory = new byte[4096];
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
            var overhead = 200 + (serializationEstimation.fieldNodeCount * 100);
            return serializationEstimation.bodyLength + overhead + (serializationEstimation.bufferCount * 8);
        }

        public Span<byte> SerializeRecordBatch(EventBatchData eventBatchData, int count, SerializationEstimation serializationEstimation)
        {
            ArrowSerializer arrowSerializer = new ArrowSerializer(memory, vtable, vtables);
            if (_schemaWritten)
            {
                arrowSerializer.SetSpacePosition(_space);
                var schemaPadding = arrowSerializer.WriteMessageLengthAndPadding();
                arrowSerializer.CopyToStart(schemaPadding);
            }

            var estimateBufferSize = GetEstimatedBufferSize(serializationEstimation);

            if (memory.Length < estimateBufferSize)
            {
                if (_schemaWritten)
                {
                    throw new InvalidOperationException("Schema has been written, but buffer is too small to write record batch");
                }
                memory = new byte[estimateBufferSize];
            }

            if ((serializationEstimation.fieldNodeCount * 2) > vtable.Length)
            {
                vtable = new int[serializationEstimation.fieldNodeCount * 2];
                vtables = new int[serializationEstimation.fieldNodeCount * 2];
                stackPointers = new int[serializationEstimation.fieldNodeCount * 2];
            }

            arrowSerializer.RecordBatchStartNodesVector(serializationEstimation.fieldNodeCount);
            for (int i = eventBatchData.Columns.Count - 1; i >= 0; i--)
            {
                eventBatchData.Columns[i].AddFieldNodes(ref arrowSerializer);
            }
            var nodesPointer = arrowSerializer.EndVector();

            arrowSerializer.RecordBatchStartBuffersVector(serializationEstimation.bufferCount);

            for (int i = 0; i < serializationEstimation.bufferCount; i++)
            {
                eventBatchData.Columns[i].AddBuffers(ref arrowSerializer);
                //arrowSerializer.CreateBuffer(0, 0);
            }
            var buffersPointer = arrowSerializer.EndVector();

            var recordBatchPointer = arrowSerializer.CreateRecordBatch(count, nodesPointer, buffersPointer);

            var messagePointer = arrowSerializer.CreateMessage(4, MessageHeader.RecordBatch, recordBatchPointer, 1);

            arrowSerializer.Finish(messagePointer);
            var padding = arrowSerializer.WriteMessageLengthAndPadding();
            var recordBatchSpan = arrowSerializer.CopyToStart(padding);
            var message = MessageStruct.ReadMessage(ref recordBatchSpan);
            var recordBatchPosition = message.HeaderPosition();

            var recordBatchStruct = new RecordBatchStruct(recordBatchSpan, recordBatchPosition);
            int bufferIndex = 0;
            for (int i = 0; i < eventBatchData.Columns.Count; i++)
            {
                //eventBatchData.Columns[i].WriteDataToBuffer(ref arrowSerializer, ref recordBatchStruct, ref bufferIndex);
            }

            var totalBodyLength = arrowSerializer.FinishWritingBufferData();
            message.SetBodyLength(totalBodyLength);

            return memory.AsSpan().Slice(0, arrowSerializer.Position);
            //return new byte[0];
        }

        /// <summary>
        /// Serializes an event batch data to an IBufferWriter
        /// </summary>
        /// <param name="bufferWriter">The writer to serialize to</param>
        /// <param name="eventBatchData">The data to serialize</param>
        public void SerializeEventBatch(IBufferWriter<byte> bufferWriter, EventBatchData eventBatchData, int count)
        {
            var estimation = GetSerializationEstimation(eventBatchData);
            SerializeSchemaHeader(bufferWriter, eventBatchData, estimation);
            SerializeRecordBatchHeader(bufferWriter, eventBatchData, count, estimation);
            SerializeBufferData(bufferWriter, eventBatchData);
        }

        public void SerializeBufferData(IBufferWriter<byte> bufferWriter, EventBatchData eventBatchData)
        {
            var arrowDataWriter = new ArrowDataWriter(bufferWriter);
            for (int i = 0; i < eventBatchData.Columns.Count; i++)
            {
                eventBatchData.Columns[i].WriteDataToBuffer(ref arrowDataWriter);
            }
        }

        public void SerializeRecordBatchHeader(IBufferWriter<byte> bufferWriter, EventBatchData eventBatchData, int count, SerializationEstimation serializationEstimation)
        {
            var overhead = 200 + (serializationEstimation.fieldNodeCount * 100) + (serializationEstimation.bufferCount * 8);

            if ((serializationEstimation.fieldNodeCount * 2) > vtable.Length)
            {
                vtable = new int[serializationEstimation.fieldNodeCount * 2];
                vtables = new int[serializationEstimation.fieldNodeCount * 2];
                stackPointers = new int[serializationEstimation.fieldNodeCount * 2];
            }

            var span = bufferWriter.GetSpan(overhead);

            ArrowSerializer arrowSerializer = new ArrowSerializer(span, vtable, vtables);

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

            var recordBatchPointer = arrowSerializer.CreateRecordBatch(count, nodesPointer, buffersPointer);

            var messagePointer = arrowSerializer.CreateMessage(4, MessageHeader.RecordBatch, recordBatchPointer, arrowSerializer.BufferBodyLength);

            arrowSerializer.Finish(messagePointer);
            var padding = arrowSerializer.WriteMessageLengthAndPadding();
            var recordBatchSpan = arrowSerializer.CopyToStart(padding);

            bufferWriter.Advance(recordBatchSpan.Length);
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

            if ((serializationEstimation.fieldNodeCount * 2) > vtable.Length)
            {
                vtable = new int[serializationEstimation.fieldNodeCount * 2];
                vtables = new int[serializationEstimation.fieldNodeCount * 2];
                stackPointers = new int[serializationEstimation.fieldNodeCount * 2];
            }

            var span = bufferWriter.GetSpan(overhead);

            ArrowSerializer arrowSerializer = new ArrowSerializer(span, vtable, vtables);

            var stack = stackPointers.AsSpan();
            var childrenStack = stack.Slice(eventBatchData.Columns.Count);
            for (int i = 0; i < eventBatchData.Columns.Count; i++)
            {
                var strPos = arrowSerializer.CreateString(i.ToString());
                stack[i] = eventBatchData.Columns[i].CreateSchemaField(ref arrowSerializer, strPos, childrenStack);
            }

            var fieldsPointer = arrowSerializer.SchemaCreateFieldsVector(stack.Slice(0, eventBatchData.Columns.Count));

            var schemaPointer = arrowSerializer.CreateSchema(0, fieldsPointer);

            var messagePointer = arrowSerializer.CreateMessage(4, MessageHeader.Schema, schemaPointer);

            arrowSerializer.Finish(messagePointer);
            var schemaPadding = arrowSerializer.WriteMessageLengthAndPadding();
            var resultSpan = arrowSerializer.CopyToStart(schemaPadding);
            bufferWriter.Advance(resultSpan.Length);
        }

        public Span<byte> SerializeSchema(EventBatchData eventBatchData, SerializationEstimation serializationEstimation)
        {
            var estimateBufferSize = GetEstimatedBufferSize(serializationEstimation);

            if (estimateBufferSize > memory.Length)
                memory = new byte[estimateBufferSize];
            if ((serializationEstimation.fieldNodeCount * 2) > vtable.Length)
            {
                vtable = new int[serializationEstimation.fieldNodeCount * 2];
                vtables = new int[serializationEstimation.fieldNodeCount * 2];
                stackPointers = new int[serializationEstimation.fieldNodeCount * 2];
            }
            ArrowSerializer arrowSerializer = new ArrowSerializer(memory, vtable, vtables);

            var stack = stackPointers.AsSpan();
            var childrenStack = stack.Slice(eventBatchData.Columns.Count);
            for (int i = 0; i < eventBatchData.Columns.Count; i++)
            {
                var strPos = arrowSerializer.CreateString(i.ToString());
                stack[i] = eventBatchData.Columns[i].CreateSchemaField(ref arrowSerializer, strPos, childrenStack);
            }

            var fieldsPointer = arrowSerializer.SchemaCreateFieldsVector(stack.Slice(0, eventBatchData.Columns.Count));

            var schemaPointer = arrowSerializer.CreateSchema(0, fieldsPointer);

            var messagePointer = arrowSerializer.CreateMessage(4, MessageHeader.Schema, schemaPointer);

            _space = arrowSerializer.Finish(messagePointer);

            _schemaWritten = true;
            return memory.AsSpan().Slice(_space);
        }
    }
}
