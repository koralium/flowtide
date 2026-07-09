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

using FlowtideDotNet.Orleans.Messages;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;

namespace FlowtideDotNet.Orleans.Tests
{
    /// <summary>
    /// Message fields must carry an [Id] or Orleans drops them on a cross-silo call (a
    /// same-silo [Immutable] pass keeps the reference and hides the loss). These tests round
    /// trip through the real binary serializer, which is what a cross-silo grain call does,
    /// so a dropped field is caught here even though single-silo TestingHost tests pass.
    /// </summary>
    public class MessageSerializationTests
    {
        private static Serializer CreateSerializer()
        {
            var services = new ServiceCollection();
            services.AddSerializer(b => b.AddAssembly(typeof(InitSubstreamResponse).Assembly));
            return services.BuildServiceProvider().GetRequiredService<Serializer>();
        }

        [Fact]
        public void InitSubstreamResponseRoundTripsNotStarted()
        {
            var serializer = CreateSerializer();
            var original = new InitSubstreamResponse(notStarted: true, success: false, restoreVersion: 7);

            var bytes = serializer.SerializeToArray(original);
            var roundTripped = serializer.Deserialize<InitSubstreamResponse>(bytes);

            // NotStarted drives the initialize-handshake 'peer not started, retry' backoff; if
            // it is dropped it arrives false and the handshake fail-and-recovers instead.
            Assert.True(roundTripped.NotStarted, "InitSubstreamResponse.NotStarted did not survive serialization (missing [Id]).");
            Assert.False(roundTripped.Success);
            Assert.Equal(7, roundTripped.RestoreVersion);
        }

        [Fact]
        public void InitSubstreamResponseRoundTripsRecordedFetchEpoch()
        {
            var serializer = CreateSerializer();
            var original = new InitSubstreamResponse(notStarted: false, success: true, restoreVersion: 7, checkpointEpoch: 11, recordedFetchEpoch: 42, recordedCheckpointEpoch: 43);

            var bytes = serializer.SerializeToArray(original);
            var roundTripped = serializer.Deserialize<InitSubstreamResponse>(bytes);

            // The recorded epochs are how a failed-over substream detects that a dead instance's
            // higher epoch is still recorded at the peer; if either is dropped the requestor never
            // re-seeds above it and is permanently fenced out of its fetches or its acks.
            Assert.Equal(42, roundTripped.RecordedFetchEpoch);
            Assert.Equal(43, roundTripped.RecordedCheckpointEpoch);
            Assert.Equal(11, roundTripped.CheckpointEpoch);
        }

        [Fact]
        public void StartStreamRequestRoundTripsPlanFields()
        {
            var serializer = CreateSerializer();
            var sqlBuilder = new FlowtideDotNet.Substrait.Sql.SqlPlanBuilder();
            sqlBuilder.Sql("CREATE TABLE t (val any); INSERT INTO o SELECT val FROM t;");
            var original = StartStreamRequest.FromPlan(sqlBuilder.GetPlan(), substreamCount: 2, optimizePlan: true);

            var bytes = serializer.SerializeToArray(original);
            var roundTripped = serializer.Deserialize<StartStreamRequest>(bytes);

            // The plan json is the whole start request for user created plans; a dropped
            // field would arrive as null and the grain could not start the stream.
            Assert.Equal(original.PlanJson, roundTripped.PlanJson);
            Assert.NotNull(roundTripped.PlanJson);
            Assert.Null(roundTripped.SqlText);
            Assert.Equal(2, roundTripped.SubstreamCount);
            Assert.True(roundTripped.OptimizePlan, "StartStreamRequest.OptimizePlan did not survive serialization (missing [Id]).");
        }

        [Fact]
        public void StartStreamMessageRoundTripsPlanJson()
        {
            var serializer = CreateSerializer();
            var original = new StartStreamMessage("stream1", "{\"relations\":[]}", "substream_0");

            var bytes = serializer.SerializeToArray(original);
            var roundTripped = serializer.Deserialize<StartStreamMessage>(bytes);

            Assert.Equal("stream1", roundTripped.StreamName);
            Assert.Equal("{\"relations\":[]}", roundTripped.PlanJson);
            Assert.Equal("substream_0", roundTripped.SubstreamName);
        }

        [Fact]
        public void GetEventsResponseRoundTripsNotStarted()
        {
            var serializer = CreateSerializer();
            var original = new GetEventsResponse(lastEventId: 3, events: new List<Base.IStreamEvent>(), notStarted: true);

            var bytes = serializer.SerializeToArray(original);
            var roundTripped = serializer.Deserialize<GetEventsResponse>(bytes);

            Assert.True(roundTripped.NotStarted, "GetEventsResponse.NotStarted did not survive serialization (missing [Id]).");
            Assert.Equal(3, roundTripped.LastEventId);
        }
    }
}
