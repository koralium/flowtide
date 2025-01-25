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
using FlowtideDotNet.Storage.Memory;
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
            var overhead = 200 + (serializationEstimation.fieldNodeCount * 100);
            return serializationEstimation.bodyLength + overhead + (serializationEstimation.bufferCount * 8);
        }

        /// <summary>
        /// Serializes an event batch data to an IBufferWriter
        /// </summary>
        /// <param name="bufferWriter">The writer to serialize to</param>
        /// <param name="eventBatchData">The data to serialize</param>
        public void SerializeEventBatch(IBufferWriter<byte> bufferWriter, EventBatchData eventBatchData, int count)
        {
            var flowtideBufferWriter = new InternalBufferWriter(bufferWriter);
            var estimation = GetSerializationEstimation(eventBatchData);
            SerializeSchemaHeader(bufferWriter, eventBatchData, estimation);
            SerializeRecordBatchHeader(bufferWriter, eventBatchData, count, estimation);
            SerializeBufferData(flowtideBufferWriter, eventBatchData);
        }

        public void SerializeEventBatch<TBufferWriter>(TBufferWriter bufferWriter, EventBatchData eventBatchData, int count)
            where TBufferWriter : IFlowtideBufferWriter
        {
            var estimation = GetSerializationEstimation(eventBatchData);
            SerializeSchemaHeader(bufferWriter, eventBatchData, estimation);
            SerializeRecordBatchHeader(bufferWriter, eventBatchData, count, estimation);
            SerializeBufferData(bufferWriter, eventBatchData);
        }

        public void SerializeBufferData<TBufferWriter>(TBufferWriter bufferWriter, EventBatchData eventBatchData)
            where TBufferWriter : IFlowtideBufferWriter
        {
            var arrowDataWriter = new ArrowDataWriter<TBufferWriter>(bufferWriter);
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
            bufferWriter.Advance(resultSpan.Length);
        }
    }
}
