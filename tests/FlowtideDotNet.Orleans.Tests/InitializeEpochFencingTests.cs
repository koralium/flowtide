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

using FlowtideDotNet.Orleans;
using FlowtideDotNet.Orleans.Grains;
using FlowtideDotNet.Orleans.Interfaces;
using FlowtideDotNet.Orleans.Internal;
using FlowtideDotNet.Orleans.Messages;
using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Runtime;

namespace FlowtideDotNet.Orleans.Tests
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
    }
}
