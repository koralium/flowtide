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
using FlowtideDotNet.Core.Operators.Exchange;
using FlowtideDotNet.Cluster.Orleans.Messages;
using FlowtideDotNet.Storage.Memory;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;
using Orleans.Serialization.Cloning;

namespace FlowtideDotNet.Cluster.Orleans.Tests
{
    /// <summary>
    /// Fetched events must only be serialized when they actually cross a silo boundary.
    /// Between grains on the same silo the payload passes by reference and the receiver
    /// consumes the live event instances with zero copies; serializing every fetch would
    /// throw away the point of colocating substreams. Orleans deep copies same-silo
    /// arguments and results through the registered copier and serializes cross-silo ones
    /// through the registered codec, so those two are what pin the behavior.
    /// </summary>
    public class FetchDataPayloadTests
    {
        private static ServiceProvider CreateSerializerServices()
        {
            var services = new ServiceCollection();
            services.AddSerializer(b => b.AddAssembly(typeof(FetchDataResponse).Assembly));
            return services.BuildServiceProvider();
        }

        private static List<SubstreamEventData> CreateEvents()
        {
            return new List<SubstreamEventData>()
            {
                new SubstreamEventData() { ExchangeTargetId = 1, StreamEvent = new Watermark("left", LongWatermarkValue.Create(3)) },
                new SubstreamEventData() { ExchangeTargetId = 2, StreamEvent = new Watermark("right", LongWatermarkValue.Create(4)) },
            };
        }

        [Fact]
        public void SameSiloPassesTheLiveEventsByReference()
        {
            using var provider = CreateSerializerServices();
            var copier = provider.GetRequiredService<DeepCopier>();

            var events = CreateEvents();
            var response = new FetchDataResponse(SubstreamEventsPayload.FromLive(events));

            // Orleans runs the copier on every same-silo call; a deep copy here would mean
            // the events are duplicated (and their receiver claims double released).
            var copied = copier.Copy(response);

            Assert.Same(response.Events, copied.Events);
            Assert.True(copied.Events!.TryTakeLive(out var liveEvents));
            Assert.Same(events[0].StreamEvent, liveEvents[0].StreamEvent);
            Assert.Same(events[1].StreamEvent, liveEvents[1].StreamEvent);
        }

        [Fact]
        public void CrossSiloSerializesTheWireFormatAndDeserializesLazily()
        {
            using var provider = CreateSerializerServices();
            var serializer = provider.GetRequiredService<Serializer>();

            var response = new FetchDataResponse(SubstreamEventsPayload.FromLive(CreateEvents()));

            // The real binary serializer is what a cross-silo grain call runs.
            var bytes = serializer.SerializeToArray(response);
            var roundTripped = serializer.Deserialize<FetchDataResponse>(bytes);

            // The sending payload was consumed by the codec, its claims were released.
            Assert.False(response.Events!.TryTakeLive(out _));

            // The receiving payload holds the raw wire format, not materialized events: the
            // handler deserializes lazily with the consuming operator's allocator.
            Assert.NotNull(roundTripped.Events);
            Assert.True(roundTripped.Events!.IsSerialized);
            Assert.False(roundTripped.Events.TryTakeLive(out _));

            var buffer = roundTripped.Events.TakeSerialized();
            try
            {
                var wireSerializer = new SubstreamEventWireSerializer();
                var events = wireSerializer.Deserialize(buffer.AsReadOnlySequence(), _ => GlobalMemoryManager.Instance);
                Assert.Equal(2, events.Count);
                Assert.Equal(1, events[0].ExchangeTargetId);
                Assert.Equal(2, events[1].ExchangeTargetId);
                var first = Assert.IsType<Watermark>(events[0].StreamEvent);
                Assert.Equal(3, Assert.IsType<LongWatermarkValue>(first.Watermarks["left"]).Value);
                var second = Assert.IsType<Watermark>(events[1].StreamEvent);
                Assert.Equal(4, Assert.IsType<LongWatermarkValue>(second.Watermarks["right"]).Value);
                SubstreamEventWireSerializer.ReturnEvents(events);
            }
            finally
            {
                buffer.Dispose();
            }
        }

        [Fact]
        public void EmptyFetchRoundTripsAcrossSilos()
        {
            using var provider = CreateSerializerServices();
            var serializer = provider.GetRequiredService<Serializer>();

            var response = new FetchDataResponse(SubstreamEventsPayload.FromLive(new List<SubstreamEventData>()));
            var roundTripped = serializer.Deserialize<FetchDataResponse>(serializer.SerializeToArray(response));

            Assert.NotNull(roundTripped.Events);
            var buffer = roundTripped.Events!.TakeSerialized();
            try
            {
                var events = new SubstreamEventWireSerializer().Deserialize(buffer.AsReadOnlySequence(), _ => GlobalMemoryManager.Instance);
                Assert.Empty(events);
            }
            finally
            {
                buffer.Dispose();
            }
        }
    }
}
