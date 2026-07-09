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

using FlowtideDotNet.Core.Operators.Exchange;
using FlowtideDotNet.Orleans.Grains;
using FlowtideDotNet.Orleans.Interfaces;
using FlowtideDotNet.Orleans.Internal;
using FlowtideDotNet.Orleans.Messages;
using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Runtime;

namespace FlowtideDotNet.Orleans.Tests
{
    /// <summary>
    /// The substream grain wraps the communication point's handshake response into the wire
    /// message. Every field must survive that mapping - the local hub passes the response by
    /// reference, so a dropped field only shows up as rare cross-grain misbehavior. NotStarted
    /// mapped to a plain failure makes the requestor fail over instead of retrying, rolling
    /// back both substreams during what should be a clean handoff.
    /// </summary>
    public class SubStreamGrainHandshakeMappingTests
    {
        private sealed class StubPersistentState : IPersistentState<SubStreamGrainStorage>
        {
            public SubStreamGrainStorage State { get; set; } = new SubStreamGrainStorage();
            public string Etag => string.Empty;
            public bool RecordExists { get; set; }
            public Task ClearStateAsync() => Task.CompletedTask;
            public Task WriteStateAsync() => Task.CompletedTask;
            public Task ReadStateAsync() => Task.CompletedTask;
        }

        /// <summary>
        /// The communication handler resolves its peer grain reference at construction; the
        /// stub satisfies that, no outbound call is ever made in these tests.
        /// </summary>
        private sealed class StubSubStreamGrain : ISubStreamGrain
        {
            public Task<InitSubstreamResponse> InitializeSubstreamRequest(InitSubstreamRequest request) => throw new NotImplementedException();
            public Task<FetchDataResponse> FetchDataAsync(FetchDataRequest request) => throw new NotImplementedException();
            public Task StartStreamAsync(StartStreamMessage startStreamMessage) => throw new NotImplementedException();
            public Task<GetEventsResponse> GetEventsAsync(GetEventsRequest request) => throw new NotImplementedException();
            public Task FailAndRecoverAsync(FailAndRecoverRequest request) => throw new NotImplementedException();
            public Task CheckpointDone(CheckpointDoneRequest request) => throw new NotImplementedException();
            public Task StopStreamAsync() => throw new NotImplementedException();
            public Task DeleteStreamAsync() => throw new NotImplementedException();
            public Task MigrateAsync() => throw new NotImplementedException();
            public Task<SubstreamStatus> GetStatusAsync() => throw new NotImplementedException();
        }

        private sealed class StubGrainFactory : IGrainFactory
        {
            public TGrainInterface GetGrain<TGrainInterface>(string primaryKey, string? grainClassNamePrefix = null) where TGrainInterface : IGrainWithStringKey => (TGrainInterface)(object)new StubSubStreamGrain();
            public TGrainInterface GetGrain<TGrainInterface>(Guid primaryKey, string? grainClassNamePrefix = null) where TGrainInterface : IGrainWithGuidKey => throw new NotImplementedException();
            public TGrainInterface GetGrain<TGrainInterface>(long primaryKey, string? grainClassNamePrefix = null) where TGrainInterface : IGrainWithIntegerKey => throw new NotImplementedException();
            public TGrainInterface GetGrain<TGrainInterface>(Guid primaryKey, string keyExtension, string? grainClassNamePrefix = null) where TGrainInterface : IGrainWithGuidCompoundKey => throw new NotImplementedException();
            public TGrainInterface GetGrain<TGrainInterface>(long primaryKey, string keyExtension, string? grainClassNamePrefix = null) where TGrainInterface : IGrainWithIntegerCompoundKey => throw new NotImplementedException();
            public TGrainObserverInterface CreateObjectReference<TGrainObserverInterface>(IGrainObserver obj) where TGrainObserverInterface : IGrainObserver => throw new NotImplementedException();
            public void DeleteObjectReference<TGrainObserverInterface>(IGrainObserver obj) where TGrainObserverInterface : IGrainObserver => throw new NotImplementedException();
            public IGrain GetGrain(Type grainInterfaceType, Guid grainPrimaryKey) => throw new NotImplementedException();
            public IGrain GetGrain(Type grainInterfaceType, long grainPrimaryKey) => throw new NotImplementedException();
            public IGrain GetGrain(Type grainInterfaceType, string grainPrimaryKey) => throw new NotImplementedException();
            public IGrain GetGrain(Type grainInterfaceType, Guid grainPrimaryKey, string keyExtension) => throw new NotImplementedException();
            public IGrain GetGrain(Type grainInterfaceType, long grainPrimaryKey, string keyExtension) => throw new NotImplementedException();
            public TGrainInterface GetGrain<TGrainInterface>(GrainId grainId) where TGrainInterface : IAddressable => throw new NotImplementedException();
            public IAddressable GetGrain(GrainId grainId) => throw new NotImplementedException();
            public IAddressable GetGrain(GrainId grainId, GrainInterfaceType interfaceType) => throw new NotImplementedException();
        }

        private static SubStreamGrain CreateGrain()
        {
            return new SubStreamGrain(
                new StubPersistentState(),
                new ConnectorManagerFactory((_, _) => { }),
                NullLoggerFactory.Instance,
                new StubGrainFactory(),
                (_, _, _) => { },
                new FlowtideOrleansOptions());
        }

        /// <summary>
        /// Wires a handler whose target-initialize callback returns the given point response,
        /// recording what the callback received from the grain.
        /// </summary>
        private static SubStreamGrain CreateGrainWithPointResponse(
            SubstreamInitializeResponse pointResponse,
            Action<long, long, bool>? onTargetInitialize = null)
        {
            var grain = CreateGrain();
            var factory = new OrleansCommunicationFactory("stream", new StubGrainFactory());
            var handler = factory.GetCommunicationHandler("peer", "self");
            handler.Initialize(
                (targets, count, ct) => Task.FromResult<IReadOnlyList<SubstreamEventData>>(Array.Empty<SubstreamEventData>()),
                _ => Task.CompletedTask,
                (restoreVersion, checkpointEpoch, cleanHandoff) =>
                {
                    onTargetInitialize?.Invoke(restoreVersion, checkpointEpoch, cleanHandoff);
                    return Task.FromResult(pointResponse);
                },
                (_, _) => Task.CompletedTask);
            grain.SetCommunicationFactoryForTests(factory);
            return grain;
        }

        [Fact]
        public async Task NotStartedFromThePointSurvivesTheWireMapping()
        {
            // The point answers not started for transient states (a fail over in progress, a
            // clean handoff whose stop barrier is still being consumed); the requestor must
            // see it and retry with backoff instead of failing over.
            var grain = CreateGrainWithPointResponse(
                new SubstreamInitializeResponse(notStarted: true, success: false, restoreVersion: 4));

            var response = await grain.InitializeSubstreamRequest(
                new InitSubstreamRequest("peer", restorePoint: 4, fetchEpoch: 1, checkpointEpoch: 0, cleanHandoff: true));

            Assert.True(response.NotStarted,
                "The point answered NotStarted but the grain mapped it away; the requestor fails over instead of retrying.");
            Assert.False(response.Success);
        }

        [Fact]
        public async Task AcceptedCleanReconnectSurvivesTheWireMappingWithAllFields()
        {
            bool? receivedCleanHandoff = null;
            long receivedRestore = -1;
            var grain = CreateGrainWithPointResponse(
                new SubstreamInitializeResponse(notStarted: false, success: true, restoreVersion: 4, checkpointEpoch: 7, recordedCheckpointEpoch: 9, cleanReconnect: true),
                (restoreVersion, checkpointEpoch, cleanHandoff) =>
                {
                    receivedRestore = restoreVersion;
                    receivedCleanHandoff = cleanHandoff;
                });

            var response = await grain.InitializeSubstreamRequest(
                new InitSubstreamRequest("peer", restorePoint: 4, fetchEpoch: 17, checkpointEpoch: 3, cleanHandoff: true));

            // The clean handoff flag must reach the point and every response field must
            // reach the requestor.
            Assert.True(receivedCleanHandoff, "The clean handoff announcement did not reach the communication point.");
            Assert.Equal(4, receivedRestore);
            Assert.False(response.NotStarted);
            Assert.True(response.Success);
            Assert.Equal(4, response.RestoreVersion);
            Assert.Equal(7, response.CheckpointEpoch);
            Assert.Equal(9, response.RecordedCheckpointEpoch);
            Assert.Equal(17, response.RecordedFetchEpoch);
            Assert.True(response.CleanReconnect,
                "The accepted clean reconnect did not survive the mapping; the requestor's readers would wait for an init watermarks event that never comes.");
        }
    }
}
