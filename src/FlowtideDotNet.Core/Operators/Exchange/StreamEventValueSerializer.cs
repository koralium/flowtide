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
using FlowtideDotNet.Base.Utils;
using FlowtideDotNet.Core.ColumnStore.Serialization;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Tree;
using Google.Protobuf.WellKnownTypes;
using SqlParser.Ast;
using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Collections.Immutable;
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
        private readonly EventBatchBPlusTreeSerializer _eventBatchBPlusTreeSerializer;

        public StreamEventValueSerializer(IMemoryAllocator memoryAllocator)
        {
            this.memoryAllocator = memoryAllocator;
            _eventBatchBPlusTreeSerializer = new EventBatchBPlusTreeSerializer();
        }

        public Task CheckpointAsync(IBPlusTreeSerializerCheckpointContext context)
        {
            return Task.CompletedTask;
        }

        public StreamEventValueContainer CreateEmpty()
        {
            return new StreamEventValueContainer(memoryAllocator);
        }

        private StreamMessage<StreamEventBatch> DeserializeBatch(ref SequenceReader<byte> reader)
        {
            if (!reader.TryReadLittleEndian(out long time))
            {
                throw new InvalidOperationException("Failed to read time");
            }

            if (!reader.TryReadLittleEndian(out int weightsLength))
            {
                throw new InvalidOperationException("Failed to read weights length");
            }

            var weightsMemory = memoryAllocator.Allocate(weightsLength, 64);
            if (!reader.TryCopyTo(weightsMemory.Memory.Span.Slice(0, weightsLength)))
            {
                throw new InvalidOperationException("Failed to read weights");
            }
            reader.Advance(weightsLength);

            if (!reader.TryReadLittleEndian(out int iterationsLength))
            {
                throw new InvalidOperationException("Failed to read iterations length");
            }
            var iterationsMemory = memoryAllocator.Allocate(iterationsLength, 64);
            if (!reader.TryCopyTo(iterationsMemory.Memory.Span.Slice(0, iterationsLength)))
            {
                throw new InvalidOperationException("Failed to read iterations");
            }
            reader.Advance(iterationsLength);

            var eventBatchData = _eventBatchBPlusTreeSerializer.Deserialize(ref reader, memoryAllocator);

            var weights = new PrimitiveList<int>(weightsMemory, eventBatchData.Count, memoryAllocator);
            var iterations = new PrimitiveList<uint>(weightsMemory, eventBatchData.Count, memoryAllocator);

            return new StreamMessage<StreamEventBatch>(new StreamEventBatch(new ColumnStore.EventBatchWeighted(weights, iterations, eventBatchData.EventBatch)), time);
        }

        private InitWatermarksEvent DeserializeInitWatermark(ref SequenceReader<byte> reader)
        {
            if (!reader.TryReadLittleEndian(out int watermarkCount))
            {
                throw new InvalidOperationException("Failed to read watermark count");
            }

            var watermarkNames = new HashSet<string>(watermarkCount);
            for (int i = 0; i < watermarkCount; i++)
            {
                if (!reader.TryReadLittleEndian(out int keyLength))
                {
                    throw new InvalidOperationException("Failed to read key length");
                }

                var keyBytes = reader.Sequence.Slice(reader.Position, keyLength);
                reader.Advance(keyLength);
                watermarkNames.Add(Encoding.UTF8.GetString(keyBytes));
            }

            return new InitWatermarksEvent(watermarkNames);
        }

        private Checkpoint DeserializeCheckpoint(ref SequenceReader<byte> reader)
        {
            if (!reader.TryReadLittleEndian(out long checkpointTime))
            {
                throw new InvalidOperationException("Failed to read checkpoint time");
            }

            if (!reader.TryReadLittleEndian(out long newTime))
            {
                throw new InvalidOperationException("Failed to read new time");
            }

            return new Checkpoint(checkpointTime, newTime);
        }

        private Watermark DeserializeWatermark(ref SequenceReader<byte> reader)
        {
            if (!reader.TryReadLittleEndian(out long startTimeUnix))
            {
                throw new InvalidOperationException("Failed to read start time");
            }
            var startTime = DateTimeOffset.FromUnixTimeMilliseconds(startTimeUnix);

            if (!reader.TryReadLittleEndian(out int sourceLength))
            {
                throw new InvalidOperationException("Failed to read source length");
            }

            var sourceBytes = reader.Sequence.Slice(reader.Position, sourceLength);
            reader.Advance(sourceLength);
            var sourceOperatorId = Encoding.UTF8.GetString(sourceBytes);

            if (!reader.TryReadLittleEndian(out int watermarkCount))
            {
                throw new InvalidOperationException("Failed to read watermark count");
            }

            var watermarksBuilder = ImmutableDictionary.CreateBuilder<string, AbstractWatermarkValue>();
            for (int i = 0; i < watermarkCount; i++)
            {
                if (!reader.TryReadLittleEndian(out int keyLength))
                {
                    throw new InvalidOperationException("Failed to read key length");
                }

                var keyBytes = reader.Sequence.Slice(reader.Position, keyLength);
                reader.Advance(keyLength);
                var key = Encoding.UTF8.GetString(keyBytes);

                if (!reader.TryReadLittleEndian(out int watermarkValueTypeId))
                {
                    throw new InvalidOperationException("Failed to read value");
                }

                var watermarkSerializer = WatermarkSerializeFactory.GetWatermarkSerializer(watermarkValueTypeId);
                watermarksBuilder.Add(new KeyValuePair<string, AbstractWatermarkValue>(key, watermarkSerializer.Deserialize(ref reader)));
            }

            return new Watermark(watermarksBuilder.ToImmutableDictionary(), startTime, sourceOperatorId);
        }

        private unsafe LockingEventPrepare DeserializeLockingEventPrepare(ref SequenceReader<byte> reader)
        {
            if (!reader.TryRead(out byte otherInputsNotInCheckpoint))
            {
                throw new InvalidOperationException("Failed to read other inputs not in checkpoint");
            }
            if (!reader.TryRead(out byte isInitEvent))
            {
                throw new InvalidOperationException("Failed to read is init event");
            }

            var guidStack = stackalloc byte[16];
            var idSpan = new Span<byte>(guidStack, 16);
            if (!reader.TryCopyTo(idSpan))
            {
                throw new InvalidOperationException("Failed to read id");
            }

            var id = new Guid(idSpan);

            reader.TryRead(out byte type);

            ILockingEvent? lockingEvent;
            switch (type)
            {
                case InitWatermarksEventType:
                    lockingEvent = DeserializeInitWatermark(ref reader);
                    break;
                case CheckpointType:
                    lockingEvent =  DeserializeCheckpoint(ref reader);
                    break;
                default:
                    throw new NotImplementedException();
            }

            return new LockingEventPrepare(lockingEvent, isInitEvent != 0, otherInputsNotInCheckpoint != 0, id);
        }

        public StreamEventValueContainer Deserialize(ref SequenceReader<byte> reader)
        {
            if (!reader.TryReadLittleEndian(out int count))
            {
                throw new InvalidOperationException("Failed to read count");
            }

            var container = new StreamEventValueContainer(memoryAllocator);

            for (int i = 0; i < count; i++)
            {
                if (!reader.TryRead(out byte type))
                {
                    throw new InvalidOperationException("Failed to read type");
                }

                switch (type)
                {
                    case StreamEventBatchType:
                        container._streamEvents.Add(DeserializeBatch(ref reader));
                        break;
                    case WatermarkType:
                        container._streamEvents.Add(DeserializeWatermark(ref reader));
                        break;
                    case LockingEventPrepareType:
                        container._streamEvents.Add(DeserializeLockingEventPrepare(ref reader));
                        break;
                    case CheckpointType:
                        container._streamEvents.Add(DeserializeCheckpoint(ref reader));
                        break;
                    case InitWatermarksEventType:
                        container._streamEvents.Add(DeserializeInitWatermark(ref reader));
                        break;
                    default:
                        throw new NotImplementedException();
                }
            }
            return container;
        }

        public Task InitializeAsync(IBPlusTreeSerializerInitializeContext context)
        {
            return Task.CompletedTask;
        }

        private void SerializeBatch(in IBufferWriter<byte> writer, in StreamMessage<StreamEventBatch> batch)
        {
            var destinationSpan = writer.GetSpan(13);
            destinationSpan[0] = StreamEventBatchType;
            BinaryPrimitives.WriteInt64LittleEndian(destinationSpan.Slice(1), batch.Time);

            var weightsSpan = batch.Data.Data.Weights.SlicedMemory.Span;

            BinaryPrimitives.WriteInt32LittleEndian(destinationSpan.Slice(9), weightsSpan.Length);
            writer.Advance(13);
            writer.Write(batch.Data.Data.Weights.SlicedMemory.Span);

            destinationSpan = writer.GetSpan(4);

            var iterationsSpan = batch.Data.Data.Iterations.SlicedMemory.Span;
            BinaryPrimitives.WriteInt32LittleEndian(destinationSpan, iterationsSpan.Length);
            writer.Advance(4);

            writer.Write(batch.Data.Data.Iterations.SlicedMemory.Span);

            _eventBatchBPlusTreeSerializer.Serialize(writer, batch.Data.Data.EventBatchData, batch.Data.Data.Count);
        }

        private void SerializeWatermark(in IBufferWriter<byte> writer, Watermark watermark)
        {
            var sourceOperatorSpan = (watermark.SourceOperatorId ?? "").AsSpan();
            var sourceLength = Encoding.UTF8.GetByteCount(sourceOperatorSpan);
            var destinationSpan = writer.GetSpan(17 + sourceLength);

            destinationSpan[0] = WatermarkType;
            BinaryPrimitives.WriteInt64LittleEndian(destinationSpan.Slice(1), watermark.StartTime.ToUnixTimeMilliseconds());
            BinaryPrimitives.WriteInt32LittleEndian(destinationSpan.Slice(9), sourceLength);
            Encoding.UTF8.GetBytes(sourceOperatorSpan, destinationSpan.Slice(13));

            BinaryPrimitives.WriteInt32LittleEndian(destinationSpan.Slice(13 + sourceLength), watermark.Watermarks.Count);
            writer.Advance(17 + sourceLength);

            foreach (var wm in watermark.Watermarks)
            {
                var keyLength = Encoding.UTF8.GetByteCount(wm.Key);
                var spanLength = keyLength + 8;
                var span = writer.GetSpan(spanLength);
                BinaryPrimitives.WriteInt32LittleEndian(span, keyLength);
                Encoding.UTF8.GetBytes(wm.Key, span.Slice(4));
                // Write the typeId of the watermark
                BinaryPrimitives.WriteInt32LittleEndian(span.Slice(4 + keyLength), wm.Value.TypeId);
                writer.Advance(spanLength);
                WatermarkSerializeFactory.GetWatermarkSerializer(wm.Value.TypeId).Serialize(wm.Value, writer);
            }
        }

        private void SerializeLockingEventPrepare(in IBufferWriter<byte> writer, LockingEventPrepare lockingEventPrepare)
        {
            var destinationSpan = writer.GetSpan(19);
            destinationSpan[0] = LockingEventPrepareType;
            destinationSpan[1] = (byte)(lockingEventPrepare.OtherInputsNotInCheckpoint ?  1 : 0);
            destinationSpan[3] = (byte)(lockingEventPrepare.IsInitEvent ? 1 : 0);
            lockingEventPrepare.Id.TryWriteBytes(destinationSpan.Slice(3));
            writer.Advance(19);
            
            SerializeLockingEvent(writer, lockingEventPrepare.LockingEvent);
        }

        private void SerializeCheckpoint(in IBufferWriter<byte> writer, Checkpoint checkpoint)
        {
            var destinationSpan = writer.GetSpan(17);
            destinationSpan[0] = CheckpointType;
            BinaryPrimitives.WriteInt64LittleEndian(destinationSpan.Slice(1), checkpoint.CheckpointTime);
            BinaryPrimitives.WriteInt64LittleEndian(destinationSpan.Slice(9), checkpoint.NewTime);
            writer.Advance(17);
        }

        private void SerializeInitWatermarksEvent(in IBufferWriter<byte> writer, InitWatermarksEvent initWatermarksEvent)
        {
            var destinationSpan = writer.GetSpan(5);
            destinationSpan[0] = InitWatermarksEventType;
            BinaryPrimitives.WriteInt32LittleEndian(destinationSpan.Slice(1), initWatermarksEvent.WatermarkNames.Count);
            writer.Advance(5);

            foreach (var wm in initWatermarksEvent.WatermarkNames)
            {
                var keyLength = Encoding.UTF8.GetByteCount(wm);
                var spanLength = keyLength + 4;
                var span = writer.GetSpan(spanLength);
                BinaryPrimitives.WriteInt32LittleEndian(span, keyLength);
                Encoding.UTF8.GetBytes(wm, span.Slice(4));
                writer.Advance(spanLength);
            }
        }

        private void SerializeLockingEvent(in IBufferWriter<byte> writer, ILockingEvent lockingEvent)
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

        public void Serialize(in IBufferWriter<byte> writer, in StreamEventValueContainer values)
        {
            var destinationSpan = writer.GetSpan(4);
            BinaryPrimitives.WriteInt32LittleEndian(destinationSpan, values._streamEvents.Count);
            writer.Advance(4);
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
        }
    }
}
