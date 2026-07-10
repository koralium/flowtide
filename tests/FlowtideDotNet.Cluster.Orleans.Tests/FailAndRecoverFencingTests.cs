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
using FlowtideDotNet.Cluster.Orleans.Grains;
using FlowtideDotNet.Cluster.Orleans.Interfaces;
using FlowtideDotNet.Cluster.Orleans.Internal;
using FlowtideDotNet.Cluster.Orleans.Messages;
using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Runtime;

namespace FlowtideDotNet.Cluster.Orleans.Tests
{
    /// <summary>
    /// A fail and recover request must be fenced by the requestor's fetch epoch like every
    /// other cross-substream message. Unfenced, a request from an abandoned stream instance
    /// (a zombie left on a silo the grain moved off of, or a message delayed across a
    /// restart) rolls the live stream back to the requested point, and a lower point is
    /// kept - the substreams converge to the lowest common version - so already committed
    /// progress is discarded on both sides for no reason.
    ///
    /// A live requestor bumps its epoch on failure BEFORE it notifies, so a legitimate
    /// request carries an epoch at or above the announced one; only a request strictly
    /// below the announcement is provably from an abandoned instance. Without any
    /// announcement recorded the requestor cannot be told apart from a zombie, the request
    /// is refused and the requestor's restart handshake reconciles the versions instead.
    /// </summary>
    public class FailAndRecoverFencingTests
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

        private sealed class RecordingSubStreamGrain : ISubStreamGrain
        {
            public List<long> AnnouncedEpochs = new List<long>();
            public List<long> FailAndRecoverEpochs = new List<long>();

            public Task<InitSubstreamResponse> InitializeSubstreamRequest(InitSubstreamRequest request)
            {
                AnnouncedEpochs.Add(request.FetchEpoch);
                return Task.FromResult(new InitSubstreamResponse(false, true, request.RestorePoint));
            }

            public Task FailAndRecoverAsync(FailAndRecoverRequest request)
            {
                FailAndRecoverEpochs.Add(request.FetchEpoch);
                return Task.CompletedTask;
            }

            public Task<FetchDataResponse> FetchDataAsync(FetchDataRequest request) => Task.FromResult(new FetchDataResponse(default));
            public Task StartStreamAsync(StartStreamMessage startStreamMessage) => Task.CompletedTask;
            public Task<GetEventsResponse> GetEventsAsync(GetEventsRequest request) => throw new NotImplementedException();
            public Task CheckpointDone(CheckpointDoneRequest request) => Task.CompletedTask;
            public Task StopStreamAsync() => Task.CompletedTask;
            public Task DeleteStreamAsync() => Task.CompletedTask;
            public Task MigrateAsync() => Task.CompletedTask;
            public Task<SubstreamStatus> GetStatusAsync() => Task.FromResult(new SubstreamStatus());
        }

        private sealed class SingleGrainFactory : IGrainFactory
        {
            private readonly ISubStreamGrain? _grain;

            public SingleGrainFactory(ISubStreamGrain? grain = null)
            {
                _grain = grain;
            }

            public TGrainInterface GetGrain<TGrainInterface>(string primaryKey, string? grainClassNamePrefix = null) where TGrainInterface : IGrainWithStringKey
            {
                return (TGrainInterface)(object)(_grain ?? new RecordingSubStreamGrain());
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

        private static SubStreamGrain CreateGrain()
        {
            return new SubStreamGrain(
                new StubPersistentState(),
                new ConnectorManagerFactory((_, _) => { }),
                NullLoggerFactory.Instance,
                new SingleGrainFactory(),
                (_, _, _) => { },
                new FlowtideOrleansOptions());
        }

        /// <summary>
        /// Wires a grain whose communication handler records dispatched recoveries, so a
        /// test can tell whether a fail and recover request was applied or refused.
        /// </summary>
        private static (SubStreamGrain grain, List<long> recoveredPoints, TaskCompletionSource recovered) CreateGrainWithRecordingHandler()
        {
            var grain = CreateGrain();
            var factory = new OrleansCommunicationFactory("stream", new SingleGrainFactory());
            var handler = factory.GetCommunicationHandler("peer", "self");
            var recoveredPoints = new List<long>();
            var recovered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            handler.Initialize(
                (targets, count, ct) => Task.FromResult<IReadOnlyList<SubstreamEventData>>(Array.Empty<SubstreamEventData>()),
                point =>
                {
                    lock (recoveredPoints)
                    {
                        recoveredPoints.Add(point);
                    }
                    recovered.TrySetResult();
                    return Task.CompletedTask;
                },
                (restoreVersion, checkpointEpoch, cleanHandoff) => Task.FromResult(new SubstreamInitializeResponse(notStarted: false, success: true, restoreVersion: restoreVersion)),
                (_, _, _) => Task.CompletedTask);
            grain.SetCommunicationFactoryForTests(factory);
            return (grain, recoveredPoints, recovered);
        }

        private static async Task<bool> RecoveryDispatched(TaskCompletionSource recovered)
        {
            // The grain dispatches the recovery fire-and-forget; a refusal never dispatches,
            // so the wait can only end early on a (wrong) dispatch.
            var finished = await Task.WhenAny(recovered.Task, Task.Delay(250));
            return finished == recovered.Task;
        }

        [Fact]
        public async Task RollbackFromAStaleInstanceIsRefused()
        {
            var (grain, recoveredPoints, recovered) = CreateGrainWithRecordingHandler();
            await grain.InitializeSubstreamRequest(new InitSubstreamRequest("peer", 0, fetchEpoch: 5000));

            // A zombie instance of the peer, fenced out of fetching, asks for a rollback to
            // its own old restore point. Applying it would discard committed progress.
            await grain.FailAndRecoverAsync(new FailAndRecoverRequest("peer", recoveryPoint: 3, fetchEpoch: 4000));

            Assert.False(await RecoveryDispatched(recovered),
                "A fail and recover from a stale fetch epoch was applied; a zombie instance can roll the live stream back below its committed version.");
            Assert.Empty(recoveredPoints);
        }

        [Fact]
        public async Task RollbackWithoutAnAnnouncementIsRefused()
        {
            var (grain, recoveredPoints, recovered) = CreateGrainWithRecordingHandler();

            // No handshake has announced an epoch to this activation, the requestor cannot
            // be told apart from a zombie. Its restart handshake reconciles the versions.
            await grain.FailAndRecoverAsync(new FailAndRecoverRequest("peer", recoveryPoint: 3, fetchEpoch: 4000));

            Assert.False(await RecoveryDispatched(recovered),
                "A fail and recover with no announced fetch epoch was applied instead of being left to the handshake reconciliation.");
            Assert.Empty(recoveredPoints);
        }

        [Fact]
        public async Task RollbackFromTheAnnouncedInstanceIsApplied()
        {
            var (grain, recoveredPoints, recovered) = CreateGrainWithRecordingHandler();
            await grain.InitializeSubstreamRequest(new InitSubstreamRequest("peer", 0, fetchEpoch: 5000));

            await grain.FailAndRecoverAsync(new FailAndRecoverRequest("peer", recoveryPoint: 3, fetchEpoch: 5000));

            Assert.True(await RecoveryDispatched(recovered), "A fail and recover carrying the announced epoch must be applied.");
            Assert.Equal(new[] { 3L }, recoveredPoints);
        }

        [Fact]
        public async Task RollbackWithTheBumpedPostFailureEpochIsApplied()
        {
            var (grain, recoveredPoints, recovered) = CreateGrainWithRecordingHandler();
            await grain.InitializeSubstreamRequest(new InitSubstreamRequest("peer", 0, fetchEpoch: 5000));

            // The real shape of a legitimate request: the peer bumps its epoch on failure
            // before it notifies, so the request arrives above the announcement (the new
            // epoch is announced only at the restart handshake). It must not be refused.
            await grain.FailAndRecoverAsync(new FailAndRecoverRequest("peer", recoveryPoint: 3, fetchEpoch: 5001));

            Assert.True(await RecoveryDispatched(recovered),
                "A fail and recover carrying the peer's post-failure bumped epoch was refused; every legitimate recovery notification would be dropped.");
            Assert.Equal(new[] { 3L }, recoveredPoints);
        }

        [Fact]
        public async Task SendFailAndRecoverCarriesTheFetchEpochAndFailureBumpsIt()
        {
            var grain = new RecordingSubStreamGrain();
            var handler = new OrleansCommunicationHandler("stream", "peer", "self", new SingleGrainFactory(grain));

            await handler.SendInitializeRequest(0, 0, false, default);
            await handler.SendFailAndRecover(7);

            Assert.Single(grain.FailAndRecoverEpochs);
            Assert.Equal(grain.AnnouncedEpochs[0], grain.FailAndRecoverEpochs[0]);

            // The failure bump runs before the notification, so the stamped epoch is above
            // the announcement - the serving grain must accept it, only lower is stale.
            handler.OnStreamFailure();
            await handler.SendFailAndRecover(7);
            Assert.Equal(2, grain.FailAndRecoverEpochs.Count);
            Assert.True(grain.FailAndRecoverEpochs[1] > grain.AnnouncedEpochs[0]);
        }
    }
}
