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

using FlowtideDotNet.Orleans.Interfaces;
using FlowtideDotNet.Orleans.Internal;
using FlowtideDotNet.Orleans.Messages;
using Orleans.Runtime;

namespace FlowtideDotNet.Orleans.Tests
{
    /// <summary>
    /// Tests the fetch epoch contract of the Orleans communication handler. Fetches remove
    /// events from the other substreams queues, a fetch from a stale stream instance, for
    /// example one abandoned when its grain moved to another silo, or a fetch in flight
    /// across a rollback, must be recognizable by the serving side so it can refuse it. The
    /// handler therefore announces its epoch through the initialize handshake, sends it with
    /// every fetch, and changes it on every stream failure. This mechanism ended a freeze
    /// where abandoned fetch loops consumed checkpoint barriers meant for the restarted
    /// stream.
    /// </summary>
    public class FetchEpochFencingTests
    {
        private class RecordingSubStreamGrain : ISubStreamGrain
        {
            public List<long> FetchEpochs = new List<long>();
            public List<long> AnnouncedEpochs = new List<long>();

            public Task<FetchDataResponse> FetchDataAsync(FetchDataRequest request)
            {
                FetchEpochs.Add(request.FetchEpoch);
                return Task.FromResult(new FetchDataResponse(default));
            }

            public Task<InitSubstreamResponse> InitializeSubstreamRequest(InitSubstreamRequest request)
            {
                AnnouncedEpochs.Add(request.FetchEpoch);
                return Task.FromResult(new InitSubstreamResponse(false, true, request.RestorePoint));
            }

            public Task StartStreamAsync(StartStreamMessage startStreamMessage) => Task.CompletedTask;
            public Task<GetEventsResponse> GetEventsAsync(GetEventsRequest request) => throw new NotImplementedException();
            public Task FailAndRecoverAsync(FailAndRecoverRequest request) => Task.CompletedTask;
            public Task CheckpointDone(CheckpointDoneRequest request) => Task.CompletedTask;
            public Task StopStreamAsync() => Task.CompletedTask;
            public Task DeleteStreamAsync() => Task.CompletedTask;
            public Task<SubstreamStatus> GetStatusAsync() => Task.FromResult(new SubstreamStatus());
        }

        private class SingleGrainFactory : IGrainFactory
        {
            private readonly ISubStreamGrain _grain;

            public SingleGrainFactory(ISubStreamGrain grain)
            {
                _grain = grain;
            }

            public TGrainInterface GetGrain<TGrainInterface>(string primaryKey, string? grainClassNamePrefix = null) where TGrainInterface : IGrainWithStringKey
            {
                return (TGrainInterface)_grain;
            }

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

        [Fact]
        public async Task FetchCarriesTheAnnouncedEpochAndFailureChangesIt()
        {
            var grain = new RecordingSubStreamGrain();
            var handler = new OrleansCommunicationHandler("stream", "peer", "self", new SingleGrainFactory(grain));

            await handler.SendInitializeRequest(0, default);
            await handler.FetchData(new HashSet<int>() { 1 }, 10, default);

            Assert.Single(grain.AnnouncedEpochs);
            Assert.Single(grain.FetchEpochs);
            // The fetch must carry exactly the epoch that was announced.
            Assert.Equal(grain.AnnouncedEpochs[0], grain.FetchEpochs[0]);

            // A stream failure must change the epoch so fetches from before the failure can
            // be refused by the other substream.
            handler.OnStreamFailure();
            await handler.FetchData(new HashSet<int>() { 1 }, 10, default);
            Assert.Equal(2, grain.FetchEpochs.Count);
            Assert.NotEqual(grain.FetchEpochs[0], grain.FetchEpochs[1]);

            // The next handshake announces the new epoch, matching the fetches again.
            await handler.SendInitializeRequest(0, default);
            Assert.Equal(2, grain.AnnouncedEpochs.Count);
            Assert.Equal(grain.FetchEpochs[1], grain.AnnouncedEpochs[1]);
        }

        [Fact]
        public void HandlerInstancesNeverShareAnEpoch()
        {
            var grain = new RecordingSubStreamGrain();
            // Two handler instances model the abandoned stream instance of a previous grain
            // activation next to the current one, their epochs must differ so the abandoned
            // instances fetches are refused.
            var first = new OrleansCommunicationHandler("stream", "peer", "self", new SingleGrainFactory(grain));
            var second = new OrleansCommunicationHandler("stream", "peer", "self", new SingleGrainFactory(grain));

            first.FetchData(new HashSet<int>() { 1 }, 10, default).GetAwaiter().GetResult();
            second.FetchData(new HashSet<int>() { 1 }, 10, default).GetAwaiter().GetResult();

            Assert.NotEqual(grain.FetchEpochs[0], grain.FetchEpochs[1]);
        }
    }
}
