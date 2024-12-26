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

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.ColumnStore.Serialization
{
    internal class EventBatchSerializer
    {
        //private byte[] schemaMemory;
        private int[] vtable;
        private int[] vtables;
        private int[] stackPointers;
        //private byte[] recordBatchMemory;
        private byte[] memory;
        private bool _schemaWritten;
        private int _space;

        public EventBatchSerializer()
        {
            memory = new byte[4096];
            //schemaMemory = new byte[4096];
            vtable = new int[256];
            vtables = new int[256];
            stackPointers = new int[256];
            //recordBatchMemory = new byte[4096];
        }

        public Span<byte> SerializeRecordBatch(EventBatchData eventBatchData)
        {
            if (_schemaWritten)
            {
                
            }
            SerializationEstimation serializationEstimation = new SerializationEstimation();

            for (int i = 0; i < eventBatchData.Columns.Count; i++)
            {
                var estimate = eventBatchData.Columns[i].GetSerializationEstimate();
                serializationEstimation.fieldNodeCount += estimate.fieldNodeCount;
                serializationEstimation.bodyLength += estimate.bodyLength;
                serializationEstimation.bufferCount += estimate.bufferCount;
            }

            var overhead = 200 + (serializationEstimation.fieldNodeCount * 100); //100 bytes per field as a big overestimate
            
            if (memory.Length < (serializationEstimation.bodyLength + overhead) * 2)
                memory = new byte[(serializationEstimation.bodyLength + overhead) * 2];

            if ((serializationEstimation.fieldNodeCount * 2) > vtable.Length)
            {
                vtable = new int[serializationEstimation.fieldNodeCount * 2];
                vtables = new int[serializationEstimation.fieldNodeCount * 2];
                stackPointers = new int[serializationEstimation.fieldNodeCount * 2];
            }

            ArrowSerializer arrowSerializer = new ArrowSerializer(memory, vtable, vtables);

            arrowSerializer.RecordBatchStartNodesVector(serializationEstimation.fieldNodeCount);
            for (int i = eventBatchData.Columns.Count - 1; i >= 0; i--)
            {
                eventBatchData.Columns[i].AddFieldNodes(ref arrowSerializer);
            }
            var nodesPointer = arrowSerializer.EndVector();

            arrowSerializer.RecordBatchStartBuffersVector(serializationEstimation.bufferCount);

            for (int i = 0; i < serializationEstimation.bufferCount; i++)
            {
                arrowSerializer.CreateBuffer(0, 0);
            }
            var buffersPointer = arrowSerializer.EndVector();

            var recordBatchPointer = arrowSerializer.CreateRecordBatch(eventBatchData.Count, nodesPointer, buffersPointer);

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
                eventBatchData.Columns[i].WriteDataToBuffer(ref arrowSerializer, ref recordBatchStruct, ref bufferIndex);
            }

            for (int i = 0; i < recordBatchStruct.BuffersLength; i++)
            {
                var buffer = recordBatchStruct.Buffers(i);
            }

            var totalBodyLength = arrowSerializer.FinishWritingBufferData();
            message.SetBodyLength(totalBodyLength);
            
            return new byte[0];
        }

        public Span<byte> SerializeSchema(EventBatchData eventBatchData)
        {
            int schemaFieldCount = 0;
            for (int i = 0; i <  eventBatchData.Columns.Count; i++)
            {
                schemaFieldCount = eventBatchData.Columns[i].GetSchemaFieldCountEstimate();
            }
            var overhead = 200 + (schemaFieldCount * 100); //100 bytes per field as a big overestimate

            if (overhead > memory.Length)
                memory = new byte[overhead];
            if ((schemaFieldCount * 2) > vtable.Length)
            {
                vtable = new int[schemaFieldCount * 2];
                vtables = new int[schemaFieldCount * 2];
                stackPointers = new int[schemaFieldCount * 2];
            }
            ArrowSerializer arrowSerializer = new ArrowSerializer(memory, vtable, vtables);

            var emptyStringOffset = arrowSerializer.CreateEmptyString();

            var stack = stackPointers.AsSpan();
            var childrenStack = stack.Slice(eventBatchData.Columns.Count);
            for (int i = 0; i < eventBatchData.Columns.Count; i++)
            {
                stack[i] = eventBatchData.Columns[i].CreateSchemaField(ref arrowSerializer, emptyStringOffset, childrenStack);
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
