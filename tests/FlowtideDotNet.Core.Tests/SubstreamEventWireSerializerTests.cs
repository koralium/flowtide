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
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.Operators.Exchange;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Memory;
using System.Buffers;

namespace FlowtideDotNet.Core.Tests
{
    /// <summary>
    /// Round trip tests for the wire format used when events are fetched between substreams
    /// on different nodes.
    /// </summary>
    public class SubstreamEventWireSerializerTests
    {
        private static SubstreamEventData CreateBatchEvent(int targetId, params long[] values)
        {
            var allocator = GlobalMemoryManager.Instance;
            var weights = new PrimitiveList<int>(allocator);
            var iterations = new PrimitiveList<uint>(allocator);
            var column = Column.Create(allocator);
            foreach (var value in values)
            {
                weights.Add(1);
                iterations.Add(0);
                column.Add(new Int64Value(value));
            }
            var message = new StreamMessage<StreamEventBatch>(new StreamEventBatch(new EventBatchWeighted(weights, iterations, new EventBatchData(new IColumn[] { column }))), 3);
            // The receiver claim a fetch hands out
            message.Data.Rent(1);
            return new SubstreamEventData()
            {
                ExchangeTargetId = targetId,
                StreamEvent = message
            };
        }

        [Fact]
        public void BatchesAndControlEventsRoundTrip()
        {
            var serializer = new SubstreamEventWireSerializer();

            var events = new List<SubstreamEventData>()
            {
                CreateBatchEvent(1, 10, 20, 30),
                new SubstreamEventData() { ExchangeTargetId = 2, StreamEvent = new Watermark("table1", LongWatermarkValue.Create(17)) },
                new SubstreamEventData() { ExchangeTargetId = 1, StreamEvent = new Checkpoint(5, 6) },
                new SubstreamEventData() { ExchangeTargetId = 2, StreamEvent = new StopStreamCheckpoint(7, 8) },
                CreateBatchEvent(2, 40)
            };

            var writer = new ArrayBufferWriter<byte>();
            serializer.Serialize(events, writer);
            SubstreamEventWireSerializer.ReturnEvents(events);

            var result = serializer.Deserialize(new ReadOnlySequence<byte>(writer.WrittenMemory), _ => GlobalMemoryManager.Instance);

            Assert.Equal(5, result.Count);

            Assert.Equal(1, result[0].ExchangeTargetId);
            var batch1 = Assert.IsType<StreamMessage<StreamEventBatch>>(result[0].StreamEvent);
            Assert.Equal(3, batch1.Time);
            Assert.Equal(3, batch1.Data.Data.Count);
            var container = new DataValueContainer();
            batch1.Data.Data.EventBatchData.Columns[0].GetValueAt(1, container, default);
            Assert.Equal(20, container.AsLong);
            Assert.Equal(1, batch1.Data.Data.Weights[2]);

            Assert.Equal(2, result[1].ExchangeTargetId);
            var watermark = Assert.IsType<Watermark>(result[1].StreamEvent);
            Assert.Equal(17, Assert.IsType<LongWatermarkValue>(watermark.Watermarks["table1"]).Value);

            var checkpoint = Assert.IsType<Checkpoint>(result[2].StreamEvent);
            Assert.Equal(5, checkpoint.CheckpointTime);

            // The stop checkpoint must keep its type so the receiving substream can
            // recognize the other substreams stop barrier.
            var stopCheckpoint = Assert.IsType<StopStreamCheckpoint>(result[3].StreamEvent);
            Assert.Equal(7, stopCheckpoint.CheckpointTime);

            var batch2 = Assert.IsType<StreamMessage<StreamEventBatch>>(result[4].StreamEvent);
            batch2.Data.Data.EventBatchData.Columns[0].GetValueAt(0, container, default);
            Assert.Equal(40, container.AsLong);

            // The deserialized events carry a receiver claim, returning it must free them
            // without any errors.
            SubstreamEventWireSerializer.ReturnEvents(result);
        }

        /// <summary>
        /// A watermark can carry a null value for a name, for example when a source has not
        /// produced a watermark yet, the round trip must preserve the null instead of
        /// failing.
        /// </summary>
        [Fact]
        public void WatermarkWithNullValueRoundTrips()
        {
            var builder = System.Collections.Immutable.ImmutableDictionary.CreateBuilder<string, AbstractWatermarkValue>();
            builder.Add("with_value", LongWatermarkValue.Create(42));
            builder.Add("without_value", null!);
            var watermark = new Watermark(builder.ToImmutable());

            var serializer = new SubstreamEventWireSerializer();
            var events = new List<SubstreamEventData>()
            {
                new SubstreamEventData() { ExchangeTargetId = 1, StreamEvent = watermark }
            };
            var writer = new ArrayBufferWriter<byte>();
            serializer.Serialize(events, writer);
            SubstreamEventWireSerializer.ReturnEvents(events);

            var result = serializer.Deserialize(new ReadOnlySequence<byte>(writer.WrittenMemory), _ => GlobalMemoryManager.Instance);

            var roundTripped = Assert.IsType<Watermark>(result[0].StreamEvent);
            Assert.Equal(42, Assert.IsType<LongWatermarkValue>(roundTripped.Watermarks["with_value"]).Value);
            Assert.True(roundTripped.Watermarks.ContainsKey("without_value"));
            Assert.Null(roundTripped.Watermarks["without_value"]);
            SubstreamEventWireSerializer.ReturnEvents(result);
        }

        /// <summary>
        /// The batch id splits a watermark with the same value into multiple batches and is
        /// the tie breaker in watermark comparisons. It lives on the base class so the per
        /// type serializers do not write it, losing it across the wire collapses batched
        /// watermarks into one and progress between batches disappears.
        /// </summary>
        [Fact]
        public void WatermarkBatchIdSurvivesRoundTrip()
        {
            var value = LongWatermarkValue.Create(42);
            value.BatchID = 7;
            var builder = System.Collections.Immutable.ImmutableDictionary.CreateBuilder<string, AbstractWatermarkValue>();
            builder.Add("users", value);
            var watermark = new Watermark(builder.ToImmutable());

            var serializer = new SubstreamEventWireSerializer();
            var events = new List<SubstreamEventData>()
            {
                new SubstreamEventData() { ExchangeTargetId = 1, StreamEvent = watermark }
            };
            var writer = new ArrayBufferWriter<byte>();
            serializer.Serialize(events, writer);
            SubstreamEventWireSerializer.ReturnEvents(events);

            var result = serializer.Deserialize(new ReadOnlySequence<byte>(writer.WrittenMemory), _ => GlobalMemoryManager.Instance);

            var roundTripped = Assert.IsType<LongWatermarkValue>(Assert.IsType<Watermark>(result[0].StreamEvent).Watermarks["users"]);
            Assert.Equal(42, roundTripped.Value);
            Assert.Equal(7, roundTripped.BatchID);
            SubstreamEventWireSerializer.ReturnEvents(result);
        }

        /// <summary>
        /// Init watermark events flow through the exchange when a substream starts, the
        /// watermark names must survive the round trip.
        /// </summary>
        [Fact]
        public void InitWatermarksEventRoundTrips()
        {
            var initEvent = new InitWatermarksEvent(new HashSet<string>() { "users", "orders" });

            var serializer = new SubstreamEventWireSerializer();
            var events = new List<SubstreamEventData>()
            {
                new SubstreamEventData() { ExchangeTargetId = 3, StreamEvent = initEvent }
            };
            var writer = new ArrayBufferWriter<byte>();
            serializer.Serialize(events, writer);
            SubstreamEventWireSerializer.ReturnEvents(events);

            var result = serializer.Deserialize(new ReadOnlySequence<byte>(writer.WrittenMemory), _ => GlobalMemoryManager.Instance);

            Assert.Equal(3, result[0].ExchangeTargetId);
            var roundTripped = Assert.IsType<InitWatermarksEvent>(result[0].StreamEvent);
            Assert.Equal(new HashSet<string>() { "users", "orders" }, roundTripped.WatermarkNames.ToHashSet());
            SubstreamEventWireSerializer.ReturnEvents(result);
        }

        /// <summary>
        /// Locking event prepares wrap another locking event with alignment flags, all parts
        /// must survive the round trip.
        /// </summary>
        [Fact]
        public void LockingEventPrepareRoundTrips()
        {
            var id = Guid.NewGuid();
            var prepare = new LockingEventPrepare(new Checkpoint(11, 12), isInitEvent: true, otherInputsNotInCheckpoint: false, id: id);

            var serializer = new SubstreamEventWireSerializer();
            var events = new List<SubstreamEventData>()
            {
                new SubstreamEventData() { ExchangeTargetId = 2, StreamEvent = prepare }
            };
            var writer = new ArrayBufferWriter<byte>();
            serializer.Serialize(events, writer);
            SubstreamEventWireSerializer.ReturnEvents(events);

            var result = serializer.Deserialize(new ReadOnlySequence<byte>(writer.WrittenMemory), _ => GlobalMemoryManager.Instance);

            var roundTripped = Assert.IsType<LockingEventPrepare>(result[0].StreamEvent);
            Assert.True(roundTripped.IsInitEvent);
            Assert.False(roundTripped.OtherInputsNotInCheckpoint);
            Assert.Equal(id, roundTripped.Id);
            var inner = Assert.IsType<Checkpoint>(roundTripped.LockingEvent);
            Assert.Equal(11, inner.CheckpointTime);
            SubstreamEventWireSerializer.ReturnEvents(result);
        }

        /// <summary>
        /// An empty fetch response serializes to a payload that deserializes back to an
        /// empty list.
        /// </summary>
        [Fact]
        public void EmptyEventListRoundTrips()
        {
            var serializer = new SubstreamEventWireSerializer();
            var writer = new ArrayBufferWriter<byte>();
            serializer.Serialize(new List<SubstreamEventData>(), writer);

            var result = serializer.Deserialize(new ReadOnlySequence<byte>(writer.WrittenMemory), _ => GlobalMemoryManager.Instance);
            Assert.Empty(result);
        }
    }
}
