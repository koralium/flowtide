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

using FlowtideDotNet.Core.Engine.Distributed;
using FlowtideDotNet.Core.Operators.Exchange;
using Microsoft.Extensions.Logging.Abstractions;

namespace FlowtideDotNet.Core.Tests.Exchange
{
    // Serialized with SubstreamDurabilitySenderTests, both set the durability resend statics.
    [Collection("SubstreamDurabilityResendStatics")]
    public class SubstreamDurabilityClaimResendTests : IDisposable
    {
        private readonly TimeSpan _resendInterval = SubstreamDurabilityCoordinator.ResendInterval;
        private readonly TimeSpan _resendAbandonAfter = SubstreamDurabilityCoordinator.ResendAbandonAfter;

        public SubstreamDurabilityClaimResendTests()
        {
            SubstreamDurabilityCoordinator.ResendInterval = TimeSpan.FromMilliseconds(200);
            SubstreamDurabilityCoordinator.ResendAbandonAfter = TimeSpan.FromSeconds(4);
        }

        public void Dispose()
        {
            SubstreamDurabilityCoordinator.ResendInterval = _resendInterval;
            SubstreamDurabilityCoordinator.ResendAbandonAfter = _resendAbandonAfter;
        }

        private static SubstreamRecoveryWaves Starting()
        {
            var waves = new SubstreamRecoveryWaves();
            waves.ForStart();
            return waves;
        }

        /// <summary>
        /// B agrees and goes quiet while everything it sent to A was lost, only A still waits.
        /// </summary>
        [Fact]
        public async Task TheWaitingSideGetsTheClaimsItsAgreedPeerNoLongerSends()
        {
            var hub = new LocalSubstreamCommunicationHub();
            var handlerA = hub.CreateFactory("subA").GetCommunicationHandler("subB", "subA");
            var handlerB = new ClaimDroppingHandler(hub.CreateFactory("subB").GetCommunicationHandler("subA", "subB"));

            var durabilityA = new SubstreamDurabilityCoordinator(NullLogger.Instance, "subA", new[] { "subB" }, 1);
            var durabilityB = new SubstreamDurabilityCoordinator(NullLogger.Instance, "subB", new[] { "subA" }, 1);
            var pointA = new SubstreamCommunicationPoint(NullLogger.Instance, "subA", "subB", handlerA, false, durabilityA, Starting());
            var pointB = new SubstreamCommunicationPoint(NullLogger.Instance, "subB", "subA", handlerB, false, durabilityB, Starting());

            await pointA.InitializeOperator(0);
            await pointB.InitializeOperator(0);

            handlerB.Drop = true;
            durabilityB.LocalDurable(5);
            durabilityA.LocalDurable(5);

            // B heard A and agreed, nothing it sent arrived.
            await durabilityB.WhenAgreed(5, default).WaitAsync(TimeSpan.FromSeconds(10));
            Assert.True(handlerB.Dropped > 0);
            Assert.False(durabilityA.IsAgreed(5));

            // The link works again, B has nothing new to say on its own.
            handlerB.Drop = false;
            await durabilityA.WhenAgreed(5, default).WaitAsync(TimeSpan.FromSeconds(30));
        }

        /// <summary>
        /// The peer is unreachable, a send to it never completes.
        /// </summary>
        [Fact]
        public async Task ResendsDoNotPileUpOnAPeerThatDoesNotAnswer()
        {
            var hub = new LocalSubstreamCommunicationHub();
            var handlerA = new ClaimDroppingHandler(hub.CreateFactory("subA").GetCommunicationHandler("subB", "subA"));
            var handlerB = hub.CreateFactory("subB").GetCommunicationHandler("subA", "subB");

            var durabilityA = new SubstreamDurabilityCoordinator(NullLogger.Instance, "subA", new[] { "subB" }, 1);
            var pointA = new SubstreamCommunicationPoint(NullLogger.Instance, "subA", "subB", handlerA, false, durabilityA, Starting());
            var pointB = new SubstreamCommunicationPoint(NullLogger.Instance, "subB", "subA", handlerB, waves: Starting());

            await pointA.InitializeOperator(0);
            await pointB.InitializeOperator(0);

            handlerA.HangReplyRequests = true;
            durabilityA.LocalDurable(5);
            using var cancel = new CancellationTokenSource();
            var wait = durabilityA.WhenAgreed(5, cancel.Token);
            try
            {
                // Several intervals pass, only the first request is outstanding.
                await WaitFor(() => handlerA.HungReplyRequests >= 1, "the first request");
                var sinceFirst = System.Diagnostics.Stopwatch.StartNew();
                await Task.Delay(TimeSpan.FromMilliseconds(600));
                var seen = handlerA.HungReplyRequests;
                if (sinceFirst.Elapsed < TimeSpan.FromSeconds(2))
                {
                    // Only judged when this thread was not held up past the point where a second one is right.
                    Assert.Equal(1, seen);
                }

                // Cancellation asks the transport to end, but cannot release a live call.
                await WaitFor(() => handlerA.FirstHungRequestCancelled, "the cancellation request");
                Assert.Equal(1, handlerA.HungReplyRequests);
                handlerA.ReleaseHungRequests();
                await WaitFor(() => handlerA.HungReplyRequests >= 2, "the retry after actual settlement");
            }
            finally
            {
                cancel.Cancel();
                handlerA.ReleaseHungRequests();
            }
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => wait);
        }

        private static async Task WaitFor(Func<bool> condition, string what)
        {
            var deadline = DateTime.UtcNow.AddSeconds(20);
            while (!condition())
            {
                Assert.True(DateTime.UtcNow < deadline, $"Timed out waiting for {what}");
                await Task.Delay(10);
            }
        }

        private sealed class ClaimDroppingHandler : ISubstreamCommunicationHandler
        {
            private readonly ISubstreamCommunicationHandler _inner;
            private readonly TaskCompletionSource _never = new TaskCompletionSource();
            private int _dropped;
            private int _hungReplyRequests;

            public ClaimDroppingHandler(ISubstreamCommunicationHandler inner)
            {
                _inner = inner;
            }

            public volatile bool Drop;

            public int Dropped => Volatile.Read(ref _dropped);

            public volatile bool HangReplyRequests;

            public int HungReplyRequests => Volatile.Read(ref _hungReplyRequests);

            private CancellationToken _firstHungRequest;

            public bool FirstHungRequestCancelled => _firstHungRequest.IsCancellationRequested;

            public void ReleaseHungRequests() => _never.TrySetResult();

            public void Initialize(
                Func<IReadOnlySet<int>, int, CancellationToken, Task<IReadOnlyList<SubstreamEventData>>> getDataFunction,
                Func<RecoveryWave, Task> callFailAndRecover,
                Func<long, long, bool, RecoveryWave, Task<SubstreamInitializeResponse>> initializeFromTarget,
                Func<long, long, bool, Task> callRecieveCheckpointDone)
            {
                _inner.Initialize(getDataFunction, callFailAndRecover, initializeFromTarget, callRecieveCheckpointDone);
            }

            public Task<IReadOnlyList<SubstreamEventData>> FetchData(IReadOnlySet<int> targetIds, int numberOfEvents, CancellationToken cancellationToken)
            {
                return _inner.FetchData(targetIds, numberOfEvents, cancellationToken);
            }

            public Task SendFailAndRecover(RecoveryWave wave)
            {
                return _inner.SendFailAndRecover(wave);
            }

            public Task<SubstreamInitializeResponse> SendInitializeRequest(long restoreVersion, long checkpointEpoch, bool cleanHandoff, RecoveryWave wave, CancellationToken cancellationToken)
            {
                return _inner.SendInitializeRequest(restoreVersion, checkpointEpoch, cleanHandoff, wave, cancellationToken);
            }

            public Task SendCheckpointDone(long checkpointVersion, long targetCheckpointEpoch, bool coversPeerStopBarrier)
            {
                return _inner.SendCheckpointDone(checkpointVersion, targetCheckpointEpoch, coversPeerStopBarrier);
            }

            public void InitializeDurabilityClaims(Func<long, int, long, RecoveryWave, long, long, bool, Task> callReceiveDurabilityClaim)
            {
                _inner.InitializeDurabilityClaims(callReceiveDurabilityClaim);
            }

            public Task SendDurabilityClaim(long version, int radius, long initVersion, RecoveryWave wave, long senderCheckpointEpoch, long targetCheckpointEpoch, bool requestReply, CancellationToken cancellationToken)
            {
                if (Drop)
                {
                    Interlocked.Increment(ref _dropped);
                    return Task.CompletedTask;
                }
                if (HangReplyRequests && requestReply)
                {
                    if (Interlocked.Increment(ref _hungReplyRequests) == 1)
                    {
                        _firstHungRequest = cancellationToken;
                    }
                    return _never.Task;
                }
                return _inner.SendDurabilityClaim(version, radius, initVersion, wave, senderCheckpointEpoch, targetCheckpointEpoch, requestReply, cancellationToken);
            }
        }
    }
}
