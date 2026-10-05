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
    /// Wire format for events fetched between substreams on different nodes. The payload is a single
    /// opaque byte stream, so any transport (Orleans grain calls, an actor framework) can move it,
    /// using the same columnar encoding the exchange queues spill with (no per-row work).
    ///
    /// Serialize writes into an <see cref="IBufferWriter{T}"/> and Deserialize reads from a
    /// <see cref="ReadOnlySequence{T}"/>, so pooled segmented buffers (e.g. Orleans PooledBuffer)
    /// move events without intermediate byte arrays. Serialize never allocates batch memory;
    /// Deserialize allocates from the target's resolved allocator and references nothing in the
    /// payload, so the transport may release it once Deserialize returns.
    ///
    /// Rent ownership: the receiver claim from the dequeue is released by <see cref="ReturnEvents"/>
    /// on the sending side after serializing, and deserialized events carry a fresh claim, so the
    /// fetching side sees the same contract as a local fetch.
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
                StreamEventRent.Dispose(events[i].StreamEvent);
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
            try
            {
                for (int i = 0; i < count; i++)
                {
                    if (!reader.TryReadLittleEndian(out int exchangeTargetId))
                    {
                        throw new InvalidOperationException("Failed to read exchange target id");
                    }
                    var allocator = allocatorResolver(exchangeTargetId);
                    var streamEvent = StreamEventValueSerializer.DeserializeEvent(ref reader, allocator, _batchSerializer);
                    StreamEventRent.Rent(streamEvent);
                    result.Add(new SubstreamEventData()
                    {
                        ExchangeTargetId = exchangeTargetId,
                        StreamEvent = streamEvent
                    });
                }
            }
            catch
            {
                // A failure mid payload must not abandon the events already materialized,
                // their claims would never be returned and the batch memory leaks.
                ReturnEvents(result);
                throw;
            }
            return result;
        }

    }
}
