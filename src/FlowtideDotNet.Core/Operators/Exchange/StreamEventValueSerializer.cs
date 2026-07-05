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

using FlowtideDotNet.Base;
using FlowtideDotNet.Base.Utils;
using FlowtideDotNet.Core.ColumnStore.Serialization;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Tree;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Immutable;
using System.Text;

namespace FlowtideDotNet.Core.Operators.Exchange
{
    internal class StreamEventValueSerializer : IBplusTreeValueSerializer<IStreamEvent, StreamEventValueContainer>
    {
        private const byte StreamEventBatchType = 0;
        private const byte WatermarkType = 1;
        private const byte LockingEventPrepareType = 2;
        private const byte InitWatermarksEventType = 3;
        private const byte CheckpointType = 4;
        // Stop checkpoints keep their own type so the receiving substream can recognize the
        // other substreams stop barrier after serialization.
        private const byte StopCheckpointType = 5;

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

        private static StreamMessage<StreamEventBatch> DeserializeBatch(ref SequenceReader<byte> reader, IMemoryAllocator memoryAllocator, EventBatchBPlusTreeSerializer batchSerializer)
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

            var eventBatchData = batchSerializer.Deserialize(ref reader, memoryAllocator);

            var weights = new PrimitiveList<int>(weightsMemory, eventBatchData.Count, memoryAllocator);
            var iterations = new PrimitiveList<uint>(iterationsMemory, eventBatchData.Count, memoryAllocator);

            return new StreamMessage<StreamEventBatch>(new StreamEventBatch(new ColumnStore.EventBatchWeighted(weights, iterations, eventBatchData.EventBatch)), time);
        }

        private static InitWatermarksEvent DeserializeInitWatermark(ref SequenceReader<byte> reader)
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

        private static Checkpoint DeserializeCheckpoint(ref SequenceReader<byte> reader, bool isStopCheckpoint)
        {
            if (!reader.TryReadLittleEndian(out long checkpointTime))
            {
                throw new InvalidOperationException("Failed to read checkpoint time");
            }

            if (!reader.TryReadLittleEndian(out long newTime))
            {
                throw new InvalidOperationException("Failed to read new time");
            }

            if (isStopCheckpoint)
            {
                return new StopStreamCheckpoint(checkpointTime, newTime);
            }
            return new Checkpoint(checkpointTime, newTime);
        }

        private static Watermark DeserializeWatermark(ref SequenceReader<byte> reader)
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

                if (watermarkValueTypeId == -1)
                {
                    // Null watermark value
                    watermarksBuilder.Add(new KeyValuePair<string, AbstractWatermarkValue>(key, null!));
                    continue;
                }

                if (!reader.TryReadLittleEndian(out long batchId))
                {
                    throw new InvalidOperationException("Failed to read watermark batch id");
                }

                var watermarkSerializer = WatermarkSerializeFactory.GetWatermarkSerializer(watermarkValueTypeId);
                var watermarkValue = watermarkSerializer.Deserialize(ref reader);
                watermarkValue.BatchID = batchId;
                watermarksBuilder.Add(new KeyValuePair<string, AbstractWatermarkValue>(key, watermarkValue));
            }

            return new Watermark(watermarksBuilder.ToImmutableDictionary(), startTime, sourceOperatorId);
        }

        private static unsafe LockingEventPrepare DeserializeLockingEventPrepare(ref SequenceReader<byte> reader)
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
            reader.Advance(16);

            var id = new Guid(idSpan);

            if (!reader.TryRead(out byte type))
            {
                throw new InvalidOperationException("Failed to read type");
            }

            ILockingEvent? lockingEvent;
            switch (type)
            {
                case InitWatermarksEventType:
                    lockingEvent = DeserializeInitWatermark(ref reader);
                    break;
                case CheckpointType:
                    lockingEvent = DeserializeCheckpoint(ref reader, isStopCheckpoint: false);
                    break;
                case StopCheckpointType:
                    lockingEvent = DeserializeCheckpoint(ref reader, isStopCheckpoint: true);
                    break;
                default:
                    throw new NotSupportedException($"Unknown locking event type id '{type}' inside a locking event prepare.");
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
                container.Add(DeserializeEvent(ref reader, memoryAllocator, _eventBatchBPlusTreeSerializer));
            }
            return container;
        }

        /// <summary>
        /// Deserializes a single event, the format is self delimiting so events can be read
        /// in sequence. Also used for the wire format when events are sent between substreams
        /// on different nodes, batch memory is allocated from the given allocator so received
        /// data is accounted on the operator that consumes it.
        /// </summary>
        internal static IStreamEvent DeserializeEvent(ref SequenceReader<byte> reader, IMemoryAllocator memoryAllocator, EventBatchBPlusTreeSerializer batchSerializer)
        {
            if (!reader.TryRead(out byte type))
            {
                throw new InvalidOperationException("Failed to read type");
            }

            switch (type)
            {
                case StreamEventBatchType:
                    return DeserializeBatch(ref reader, memoryAllocator, batchSerializer);
                case WatermarkType:
                    return DeserializeWatermark(ref reader);
                case LockingEventPrepareType:
                    return DeserializeLockingEventPrepare(ref reader);
                case CheckpointType:
                    return DeserializeCheckpoint(ref reader, isStopCheckpoint: false);
                case StopCheckpointType:
                    return DeserializeCheckpoint(ref reader, isStopCheckpoint: true);
                case InitWatermarksEventType:
                    return DeserializeInitWatermark(ref reader);
                default:
                    throw new NotSupportedException($"Unknown stream event type id '{type}'.");
            }
        }

        public Task InitializeAsync(IBPlusTreeSerializerInitializeContext context)
        {
            return Task.CompletedTask;
        }

        private static void SerializeBatch(in IBufferWriter<byte> writer, in StreamMessage<StreamEventBatch> batch, EventBatchBPlusTreeSerializer batchSerializer)
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

            batchSerializer.Serialize(writer, batch.Data.Data.EventBatchData, batch.Data.Data.Count);
        }

        private static void SerializeWatermark(in IBufferWriter<byte> writer, Watermark watermark)
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
                // Write the typeId of the watermark, -1 marks a null watermark value
                var typeId = wm.Value?.TypeId ?? -1;
                BinaryPrimitives.WriteInt32LittleEndian(span.Slice(4 + keyLength), typeId);
                writer.Advance(spanLength);
                if (wm.Value != null)
                {
                    // The batch id is written here since it lives on the base class, the per
                    // type serializers only write their own value. Losing it collapses
                    // batched watermarks with the same value into one, the comparison tie
                    // break on batch id then reports no progress between batches.
                    var batchIdSpan = writer.GetSpan(8);
                    BinaryPrimitives.WriteInt64LittleEndian(batchIdSpan, wm.Value.BatchID);
                    writer.Advance(8);
                    WatermarkSerializeFactory.GetWatermarkSerializer(typeId).Serialize(wm.Value, writer);
                }
            }
        }

        private static void SerializeLockingEventPrepare(in IBufferWriter<byte> writer, LockingEventPrepare lockingEventPrepare)
        {
            var destinationSpan = writer.GetSpan(19);
            destinationSpan[0] = LockingEventPrepareType;
            destinationSpan[1] = (byte)(lockingEventPrepare.OtherInputsNotInCheckpoint ?  1 : 0);
            destinationSpan[2] = (byte)(lockingEventPrepare.IsInitEvent ? 1 : 0);
            lockingEventPrepare.Id.TryWriteBytes(destinationSpan.Slice(3));
            writer.Advance(19);
            
            SerializeLockingEvent(writer, lockingEventPrepare.LockingEvent);
        }

        private static void SerializeCheckpoint(in IBufferWriter<byte> writer, Checkpoint checkpoint)
        {
            var destinationSpan = writer.GetSpan(17);
            destinationSpan[0] = checkpoint is StopStreamCheckpoint ? StopCheckpointType : CheckpointType;
            BinaryPrimitives.WriteInt64LittleEndian(destinationSpan.Slice(1), checkpoint.CheckpointTime);
            BinaryPrimitives.WriteInt64LittleEndian(destinationSpan.Slice(9), checkpoint.NewTime);
            writer.Advance(17);
        }

        private static void SerializeInitWatermarksEvent(in IBufferWriter<byte> writer, InitWatermarksEvent initWatermarksEvent)
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

        private static void SerializeLockingEvent(in IBufferWriter<byte> writer, ILockingEvent lockingEvent)
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
            throw new NotSupportedException($"Locking event type '{lockingEvent.GetType().Name}' cannot be serialized for the exchange queue.");
        }

        public void Serialize(in IBufferWriter<byte> writer, in StreamEventValueContainer values)
        {
            var destinationSpan = writer.GetSpan(4);
            BinaryPrimitives.WriteInt32LittleEndian(destinationSpan, values._streamEvents.Count);
            writer.Advance(4);
            for (int i = 0; i < values._streamEvents.Count; i++)
            {
                SerializeEvent(writer, values._streamEvents[i], _eventBatchBPlusTreeSerializer);
            }
        }

        /// <summary>
        /// Serializes a single event with a leading type byte. Also used for the wire format
        /// when events are sent between substreams on different nodes. Serialization does not
        /// allocate from a memory allocator, it only writes into the buffer writer.
        /// </summary>
        internal static void SerializeEvent(in IBufferWriter<byte> writer, IStreamEvent val, EventBatchBPlusTreeSerializer batchSerializer)
        {
            if (val is StreamMessage<StreamEventBatch> batch)
            {
                SerializeBatch(writer, batch, batchSerializer);
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
                throw new NotSupportedException($"Stream event type '{val.GetType().Name}' cannot be serialized for the exchange queue.");
            }
        }
    }
}
