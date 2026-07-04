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
using FlowtideDotNet.Core;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.Operators.Exchange;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Memory;
using Orleans.Serialization.Buffers;
using System.Buffers;

namespace FlowtideDotNet.Orleans.Tests
{
    /// <summary>
    /// Round trips large event payloads through the pooled buffer the fetch responses use
    /// between silos. Large payloads span several pooled segments, so the deserializer must
    /// handle a multi segment sequence, where a value can be split across a segment
    /// boundary. The regular wire tests use a single contiguous buffer and never hit those
    /// paths.
    /// </summary>
    public class PooledBufferWireTests
    {
        [Fact]
        public void LargePayloadSpansSegmentsAndRoundTrips()
        {
            var allocator = GlobalMemoryManager.Instance;
            var random = new Random(543212345);
            var values = new long[40_000];
            for (int i = 0; i < values.Length; i++)
            {
                values[i] = random.NextInt64();
            }

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
            message.Data.Rent(1);
            var events = new List<SubstreamEventData>()
            {
                new SubstreamEventData() { ExchangeTargetId = 1, StreamEvent = message }
            };

            var serializer = new SubstreamEventWireSerializer();
            var buffer = new PooledBuffer();
            IBufferWriter<byte> writer = buffer;
            serializer.Serialize(events, writer);
            SubstreamEventWireSerializer.ReturnEvents(events);
            var finalBuffer = (PooledBuffer)writer;

            var sequence = finalBuffer.AsReadOnlySequence();
            Assert.False(sequence.IsSingleSegment, $"The payload of {finalBuffer.Length} bytes was expected to span several pooled segments.");

            var result = serializer.Deserialize(sequence, _ => GlobalMemoryManager.Instance);
            finalBuffer.Dispose();

            Assert.Single(result);
            var batch = Assert.IsType<StreamMessage<StreamEventBatch>>(result[0].StreamEvent);
            Assert.Equal(values.Length, batch.Data.Data.Count);
            var container = new DataValueContainer();
            // Spot check values across the whole batch, including ones that sit near segment
            // boundaries somewhere in the middle.
            for (int i = 0; i < values.Length; i += 997)
            {
                batch.Data.Data.EventBatchData.Columns[0].GetValueAt(i, container, default);
                Assert.Equal(values[i], container.AsLong);
            }
            SubstreamEventWireSerializer.ReturnEvents(result);
        }
    }
}
