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

using FlowtideDotNet.Cluster.Orleans;
using FlowtideDotNet.Cluster.Orleans.Grains;
using FlowtideDotNet.Cluster.Orleans.Interfaces;
using FlowtideDotNet.Cluster.Orleans.Internal;
using FlowtideDotNet.Cluster.Orleans.Messages;
using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Runtime;

namespace FlowtideDotNet.Cluster.Orleans.Tests
{
    /// <summary>
    /// The serving grain records the fetch epoch each substream announces through the
    /// initialize handshake and only serves fetches carrying that epoch. Fetch epochs come
    /// from a monotonically increasing seed, so a newer stream instance always announces a
    /// higher epoch than an abandoned one. A handshake carrying an older epoch is therefore
    /// a stale instance, for example one left running on a silo the requestors grain has
    /// moved off of. It must not overwrite the live instances announcement, doing so would
    /// make every fetch from the live instance mismatch the recorded epoch and fence the
    /// healthy consumer out of its own data.
    /// </summary>
    public class InitializeEpochFencingTests
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

        private sealed class ThrowingGrainFactory : IGrainFactory
        {
            public TGrainInterface GetGrain<TGrainInterface>(string primaryKey, string? grainClassNamePrefix = null) where TGrainInterface : IGrainWithStringKey => throw new NotImplementedException();
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

        // Forwards handshakes to the real grain while recording the fetch epoch each one
        // announced, so a test can compare what a real handler announced against what the
        // grain ended up recording.
        private sealed class ForwardingRecordingGrain : ISubStreamGrain
        {
            private readonly SubStreamGrain _inner;

            public ForwardingRecordingGrain(SubStreamGrain inner)
            {
                _inner = inner;
            }

            public List<long> AnnouncedFetchEpochs { get; } = new List<long>();

            public Task<InitSubstreamResponse> InitializeSubstreamRequest(InitSubstreamRequest request)
            {
                AnnouncedFetchEpochs.Add(request.FetchEpoch);
                return _inner.InitializeSubstreamRequest(request);
            }

            public Task<FetchDataResponse> FetchDataAsync(FetchDataRequest request) => _inner.FetchDataAsync(request);
            public Task StartStreamAsync(StartStreamMessage startStreamMessage) => _inner.StartStreamAsync(startStreamMessage);
            public Task<GetEventsResponse> GetEventsAsync(GetEventsRequest request) => _inner.GetEventsAsync(request);
            public Task FailAndRecoverAsync(FailAndRecoverRequest request) => _inner.FailAndRecoverAsync(request);
            public Task CheckpointDone(CheckpointDoneRequest request) => _inner.CheckpointDone(request);
            public Task StopStreamAsync() => _inner.StopStreamAsync();
            public Task DeleteStreamAsync() => _inner.DeleteStreamAsync();
            public Task MigrateAsync() => _inner.MigrateAsync();
            public Task<SubstreamStatus> GetStatusAsync() => _inner.GetStatusAsync();
        }

        private sealed class SingleGrainFactory : IGrainFactory
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

        // A bare grain instance is enough to drive the handshake handler: it only touches the
        // epoch table and the logger, the stream itself is never started so the other
        // dependencies stay unused.
        private static SubStreamGrain CreateGrain()
        {
            return new SubStreamGrain(
                new StubPersistentState(),
                new ConnectorManagerFactory((_, _) => { }),
                NullLoggerFactory.Instance,
                new ThrowingGrainFactory(),
                (_, _, _) => { },
                new FlowtideOrleansOptions());
        }

        [Fact]
        public async Task StaleInitializeDoesNotDisplaceTheLiveEpoch()
        {
            var grain = CreateGrain();
            const string requestor = "peer";
            const long liveEpoch = 5000;
            const long staleEpoch = 4000;

            // The live instance announces first, its epoch is recorded.
            await grain.InitializeSubstreamRequest(new InitSubstreamRequest(requestor, 0, liveEpoch));
            Assert.True(grain.TryGetAnnouncedFetchEpoch(requestor, out var afterLive));
            Assert.Equal(liveEpoch, afterLive);

            // A stale instance re-runs the handshake with an older epoch. It must not
            // overwrite the live announcement, otherwise the live instances fetches would
            // start mismatching the recorded epoch and get refused forever.
            await grain.InitializeSubstreamRequest(new InitSubstreamRequest(requestor, 0, staleEpoch));
            Assert.True(grain.TryGetAnnouncedFetchEpoch(requestor, out var afterStale));
            Assert.Equal(liveEpoch, afterStale);
        }

        [Fact]
        public async Task NewerInitializeUpdatesTheEpoch()
        {
            var grain = CreateGrain();
            const string requestor = "peer";

            // A legitimate restart announces a higher epoch than before, that one takes over
            // so fetches from the restarted instance are served and the previous epoch fenced.
            await grain.InitializeSubstreamRequest(new InitSubstreamRequest(requestor, 0, 5000));
            await grain.InitializeSubstreamRequest(new InitSubstreamRequest(requestor, 0, 6000));

            Assert.True(grain.TryGetAnnouncedFetchEpoch(requestor, out var epoch));
            Assert.Equal(6000, epoch);
        }

        [Fact]
        public async Task StaleInitializeIsAcknowledgedWithoutForcingRecovery()
        {
            var grain = CreateGrain();
            const string requestor = "peer";

            await grain.InitializeSubstreamRequest(new InitSubstreamRequest(requestor, 0, 5000));

            // The refused stale handshake is answered as an already reconciled success, it
            // must not report a version mismatch or failure, either would drive the live
            // serving stream into a needless fail over to the stale instances restore point.
            var response = await grain.InitializeSubstreamRequest(new InitSubstreamRequest(requestor, 42, 4000));
            Assert.True(response.Success);
            Assert.False(response.NotStarted);
            Assert.Equal(42, response.RestoreVersion);
        }

        /// <summary>
        /// A stop must clear the recorded fetch epochs (and, in the same teardown block, the
        /// communication factory), so a peer fetch that races the stop takes FetchDataAsync's
        /// RequestorUnknown fast-path instead of being routed into the torn-down exchange
        /// point. Without the clear the announcement survives the stop and the guard's stated
        /// purpose ('a stop cleared the grain state') is defeated.
        /// </summary>
        [Fact]
        public async Task StopClearsAnnouncedFetchEpochs()
        {
            var grain = CreateGrain();
            const string requestor = "peer";
            await grain.InitializeSubstreamRequest(new InitSubstreamRequest(requestor, 0, 5000));
            Assert.True(grain.TryGetAnnouncedFetchEpoch(requestor, out _), "The epoch should be recorded after the handshake.");

            // StopStreamCore clears the epochs and factory before it reaches the reminder
            // unregister, which needs the grain runtime and throws on a bare grain; the clear
            // runs first, so the swallowed exception does not affect what is under test.
            try
            {
                await grain.StopStreamAsync();
            }
            catch
            {
            }

            Assert.False(grain.TryGetAnnouncedFetchEpoch(requestor, out _), "A stop must clear the recorded fetch epochs.");
        }

        /// <summary>
        /// The guard's assumption "a newer stream instance always announces a higher epoch"
        /// only holds within one process: the epoch seed is a per-process static initialized
        /// from the clock. After a silo failover, the requestor's grain reactivates on
        /// another, longer-running process whose seed started earlier, so the LIVE instance
        /// announces a LOWER epoch than the dead instance recorded here - and is refused as
        /// if it were stale, with a success-shaped answer it cannot distinguish from a real
        /// handshake. Its fetches are then refused as RequestorUnknown, the stall limit fails
        /// and recovers it, and the recovery draws the next epoch from the same low local
        /// seed: +1 per failure never bridges a clock-scale gap (~600M ticks per minute of
        /// process-start difference), while this serving grain keeps the recorded epoch alive
        /// under its keep-alive reminder. The result is a deterministic, permanent
        /// fail-and-recover loop for the only live instance.
        ///
        /// This pins the guard's own contract ("recovers on its own refused fetches, and
        /// re-announces a newer epoch when it comes back"): after the live instance's
        /// recovery handshake, the grain must end up serving the epoch it announced. Note the
        /// deliberate tension with StaleInitializeDoesNotDisplaceTheLiveEpoch - a zombie's
        /// announcement must still be refused - so the fix has to make the two cases
        /// distinguishable rather than pick one.
        /// </summary>
        [Fact]
        public async Task FailedOverInstanceFromAnEarlierSeededProcessIsNotPermanentlyFencedOut()
        {
            const string requestor = "peer";
            var grain = CreateGrain();
            var recorder = new ForwardingRecordingGrain(grain);
            var handler = new OrleansCommunicationHandler("stream", "target", requestor, new SingleGrainFactory(recorder));

            // The live instance handshakes once so the test learns its real, seed-drawn epoch.
            await handler.SendInitializeRequest(0, 0, false, default);
            long liveEpoch = recorder.AnnouncedFetchEpochs[0];

            // The dead instance ran on a silo whose process started a day later, so its
            // clock-seeded announcement is far above anything this process's seed produces.
            // It is recorded here and survives in grain memory.
            await grain.InitializeSubstreamRequest(new InitSubstreamRequest(requestor, 0, liveEpoch + TimeSpan.FromDays(1).Ticks));

            // Failover: the live instance's fetches are refused as RequestorUnknown until the
            // stall limit fails and recovers it. The recovery bumps its epoch off the LOCAL
            // seed and the restart handshake re-announces it.
            handler.OnStreamFailure();
            await handler.SendInitializeRequest(0, 0, false, default);
            long reAnnounced = recorder.AnnouncedFetchEpochs[^1];

            Assert.True(grain.TryGetAnnouncedFetchEpoch(requestor, out var recorded));
            Assert.True(
                recorded == reAnnounced,
                $"The grain still records the dead instance's epoch ({recorded}) instead of the live instance's re-announced {reAnnounced}. " +
                "Every fetch from the only live instance is refused as RequestorUnknown, and each recovery draws +1 from its local process " +
                "seed which never bridges a clock-seeded gap: the substream is permanently fenced out of its own data after a silo failover.");
        }
    }
}
