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
using FlowtideDotNet.Core.ColumnStore.Serialization;
using FlowtideDotNet.Storage.Memory;
using System.Buffers;
using System.Buffers.Binary;

namespace FlowtideDotNet.Core.Operators.Exchange
{
    /// <summary>
    /// Wire format for events fetched between substreams on different nodes.
    ///
    /// The payload is a single opaque byte stream, so any transport, for example Orleans
    /// grain calls or an actor framework, can move it without knowing anything about the
    /// events. Data batches use the same columnar encoding as the exchange queues use when
    /// pages spill to disk, no per row work is done when serializing.
    ///
    /// Serialization writes into an <see cref="IBufferWriter{T}"/> and deserialization reads
    /// from a <see cref="ReadOnlySequence{T}"/>, so a transport with pooled segmented
    /// buffers, such as Orleans PooledBuffer, moves events without allocating or copying any
    /// intermediate byte arrays.
    ///
    /// Memory ownership over the wire: serializing only writes into the buffer, it never
    /// allocates batch memory. Deserializing allocates batch memory from the allocator
    /// resolved for the events exchange target, so received data is accounted on the read
    /// operator that consumes it. Nothing deserialized references the payload sequence, so
    /// the transport may release it as soon as <see cref="Deserialize"/> returns.
    ///
    /// Rent ownership over the wire: the receiver claim taken when the events were dequeued
    /// is released by <see cref="ReturnEvents"/> on the sending side after serializing, the
    /// events created by <see cref="Deserialize"/> carry a fresh receiver claim, so from the
    /// fetching sides point of view the contract is the same as a local fetch.
    /// </summary>
    public sealed class SubstreamEventWireSerializer
    {
        // Holds the compression contexts which are reused between calls, the serializer
        // synchronizes access to them internally.
        private readonly EventBatchBPlusTreeSerializer _batchSerializer = new EventBatchBPlusTreeSerializer();

        /// <summary>
        /// Serializes fetched events into the buffer writer.
        /// Format: [int count] then per event [int exchangeTargetId][event].
        /// </summary>
        public void Serialize(IReadOnlyList<SubstreamEventData> events, IBufferWriter<byte> writer)
        {
            var countSpan = writer.GetSpan(4);
            BinaryPrimitives.WriteInt32LittleEndian(countSpan, events.Count);
            writer.Advance(4);

            for (int i = 0; i < events.Count; i++)
            {
                var targetSpan = writer.GetSpan(4);
                BinaryPrimitives.WriteInt32LittleEndian(targetSpan, events[i].ExchangeTargetId);
                writer.Advance(4);
                StreamEventValueSerializer.SerializeEvent(writer, events[i].StreamEvent, _batchSerializer);
            }
        }

        /// <summary>
        /// Releases the receiver claims on events after they have been serialized, the local
        /// copies end their journey on the sending side, the receiving side works with the
        /// deserialized copies.
        /// </summary>
        public static void ReturnEvents(IReadOnlyList<SubstreamEventData> events)
        {
            for (int i = 0; i < events.Count; i++)
            {
                SubstreamCommunicationPoint.DisposeEvent(events[i].StreamEvent);
            }
        }

        /// <summary>
        /// Deserializes a payload created by <see cref="Serialize"/>. Batch memory is
        /// allocated from the allocator the resolver returns for the events exchange target
        /// id. Each rentable event is rented once, the receiver claim, matching what a local
        /// fetch hands out, the reader returns it after the event has been sent into the
        /// stream.
        /// </summary>
        public List<SubstreamEventData> Deserialize(in ReadOnlySequence<byte> payload, Func<int, IMemoryAllocator> allocatorResolver)
        {
            var reader = new SequenceReader<byte>(payload);
            if (!reader.TryReadLittleEndian(out int count))
            {
                throw new InvalidOperationException("Failed to read event count");
            }

            var result = new List<SubstreamEventData>(count);
            for (int i = 0; i < count; i++)
            {
                if (!reader.TryReadLittleEndian(out int exchangeTargetId))
                {
                    throw new InvalidOperationException("Failed to read exchange target id");
                }
                var allocator = allocatorResolver(exchangeTargetId);
                var streamEvent = StreamEventValueSerializer.DeserializeEvent(ref reader, allocator, _batchSerializer);
                RentEvent(streamEvent);
                result.Add(new SubstreamEventData()
                {
                    ExchangeTargetId = exchangeTargetId,
                    StreamEvent = streamEvent
                });
            }
            return result;
        }

        private static void RentEvent(IStreamEvent streamEvent)
        {
            if (streamEvent is StreamMessage<StreamEventBatch> streamMessage && streamMessage.Data is IRentable rentableData)
            {
                rentableData.Rent(1);
            }
            else if (streamEvent is IRentable rentable)
            {
                rentable.Rent(1);
            }
        }
    }
}
