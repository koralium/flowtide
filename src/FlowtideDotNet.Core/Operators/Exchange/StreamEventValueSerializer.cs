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
using FlowtideDotNet.Base;
using FlowtideDotNet.Core.ColumnStore.Serialization;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Tree;
using Google.Protobuf.WellKnownTypes;
using SqlParser.Ast;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.Operators.Exchange
{
    internal class StreamEventValueSerializer : IBplusTreeValueSerializer<IStreamEvent, StreamEventValueContainer>
    {
        private const byte StreamEventBatchType = 0;
        private const byte WatermarkType = 1;
        private const byte LockingEventPrepareType = 2;
        private const byte InitWatermarksEventType = 3;
        private const byte CheckpointType = 4;

        private readonly IMemoryAllocator memoryAllocator;

        public StreamEventValueSerializer(IMemoryAllocator memoryAllocator)
        {
            this.memoryAllocator = memoryAllocator;
        }

        public Task CheckpointAsync(IBPlusTreeSerializerCheckpointContext context)
        {
            return Task.CompletedTask;
        }

        public StreamEventValueContainer CreateEmpty()
        {
            return new StreamEventValueContainer(memoryAllocator);
        }

        public StreamEventValueContainer Deserialize(in BinaryReader reader)
        {
            throw new NotImplementedException();
        }

        public Task InitializeAsync(IBPlusTreeSerializerInitializeContext context)
        {
            return Task.CompletedTask;
        }

        private void SerializeBatch(in BinaryWriter writer, in StreamMessage<StreamEventBatch> batch)
        {
            writer.Write(StreamEventBatchType);
            writer.Write(batch.Time);

            var weightsSpan = batch.Data.Data.Weights.SlicedMemory.Span;
            writer.Write(weightsSpan.Length);
            writer.Write(batch.Data.Data.Weights.SlicedMemory.Span);

            var iterationsSpan = batch.Data.Data.Iterations.SlicedMemory.Span;
            writer.Write(iterationsSpan.Length);
            writer.Write(batch.Data.Data.Iterations.SlicedMemory.Span);

            var recordBatch = EventArrowSerializer.BatchToArrow(batch.Data.Data.EventBatchData, batch.Data.Data.Count);
            var batchWriter = new ArrowStreamWriter(writer.BaseStream, recordBatch.Schema, true);
            batchWriter.WriteRecordBatch(recordBatch);
        }

        private void SerializeWatermark(in BinaryWriter writer, Watermark watermark)
        {
            writer.Write(WatermarkType);
            writer.Write(watermark.SourceOperatorId ?? "");
            writer.Write(watermark.StartTime.ToUnixTimeMilliseconds());

            writer.Write(watermark.Watermarks.Count);

            foreach (var wm in watermark.Watermarks)
            {
                writer.Write(wm.Key);
                writer.Write(wm.Value);
            }
        }

        private void SerializeLockingEventPrepare(in BinaryWriter writer, LockingEventPrepare lockingEventPrepare)
        {
            writer.Write(LockingEventPrepareType);
            writer.Write(lockingEventPrepare.OtherInputsNotInCheckpoint);
            SerializeLockingEvent(writer, lockingEventPrepare.LockingEvent);
        }

        private void SerializeCheckpoint(in BinaryWriter writer, Checkpoint checkpoint)
        {
            writer.Write(CheckpointType);
            writer.Write(checkpoint.CheckpointTime);
            writer.Write(checkpoint.NewTime);
        }

        private void SerializeInitWatermarksEvent(in BinaryWriter writer, InitWatermarksEvent initWatermarksEvent)
        {
            writer.Write(InitWatermarksEventType);
            writer.Write(initWatermarksEvent.WatermarkNames.Count);

            foreach (var wm in initWatermarksEvent.WatermarkNames)
            {
                writer.Write(wm);
            }
        }

        private void SerializeLockingEvent(in BinaryWriter writer, ILockingEvent lockingEvent)
        {
            if (lockingEvent is InitWatermarksEvent initWatermarksEvent)
            {
                SerializeInitWatermarksEvent(writer, initWatermarksEvent);
                return;
            }
            if (lockingEvent is Checkpoint checkpointEvent)
            {
                SerializeCheckpoint(writer, checkpointEvent);
                return;
            }
            throw new NotImplementedException();
        }

        public void Serialize(in BinaryWriter writer, in StreamEventValueContainer values)
        {
            for (int i = 0; i < values._streamEvents.Count; i++)
            {
                var val = values._streamEvents[i];

                if (val is StreamMessage<StreamEventBatch> batch)
                {
                    SerializeBatch(writer, batch);
                }
                else if (val is Watermark watermark)
                {
                    SerializeWatermark(writer, watermark);
                }
                else if (val is LockingEventPrepare lockingEventPrepare)
                {
                    SerializeLockingEventPrepare(writer, lockingEventPrepare);
                }
                else if (val is ILockingEvent lockingEvent)
                {
                    SerializeLockingEvent(writer, lockingEvent);
                }
                else
                {
                    throw new NotImplementedException();
                }
            }
            throw new NotImplementedException();
        }
    }
}
