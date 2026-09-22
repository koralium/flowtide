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

        /// <summary>
        /// B agrees and goes quiet while what it sent to A after its first claim was lost, only A still waits.
        /// </summary>
        [Fact]
        public async Task TheWaitingSideGetsTheClaimsItsAgreedPeerNoLongerSends()
        {
            var hub = new LocalSubstreamCommunicationHub();
            var handlerA = hub.CreateFactory("subA").GetCommunicationHandler("subB", "subA");
            var handlerB = new ClaimDroppingHandler(hub.CreateFactory("subB").GetCommunicationHandler("subA", "subB"));

            var durabilityA = new SubstreamDurabilityCoordinator(NullLogger.Instance, "subA", new[] { "subB" }, 1);
            var durabilityB = new SubstreamDurabilityCoordinator(NullLogger.Instance, "subB", new[] { "subA" }, 1);
            var pointA = new SubstreamCommunicationPoint(NullLogger.Instance, "subA", "subB", handlerA, false, durabilityA);
            var pointB = new SubstreamCommunicationPoint(NullLogger.Instance, "subB", "subA", handlerB, false, durabilityB);

            await pointA.InitializeOperator(0);
            await pointB.InitializeOperator(0);

            handlerB.Drop = true;
            durabilityB.LocalDurable(5);
            durabilityA.LocalDurable(5);

            // B heard A and agreed, of what it sent only that it is durable arrived.
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
            var pointA = new SubstreamCommunicationPoint(NullLogger.Instance, "subA", "subB", handlerA, false, durabilityA);
            var pointB = new SubstreamCommunicationPoint(NullLogger.Instance, "subB", "subA", handlerB);

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

                // A send that never completes is given up on: cancelled, and the peer is asked again.
                await WaitFor(() => handlerA.HungReplyRequests >= 2, "the request after the first one was given up on");
                Assert.True(handlerA.FirstHungRequestCancelled, "The send that was given up on was left running");
            }
            finally
            {
                cancel.Cancel();
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

            public void Initialize(
                Func<IReadOnlySet<int>, int, CancellationToken, Task<IReadOnlyList<SubstreamEventData>>> getDataFunction,
                Func<long, Task> callFailAndRecover,
                Func<long, long, bool, Task<SubstreamInitializeResponse>> initializeFromTarget,
                Func<long, long, bool, Task> callRecieveCheckpointDone)
            {
                _inner.Initialize(getDataFunction, callFailAndRecover, initializeFromTarget, callRecieveCheckpointDone);
            }

            public Task<IReadOnlyList<SubstreamEventData>> FetchData(IReadOnlySet<int> targetIds, int numberOfEvents, CancellationToken cancellationToken)
            {
                return _inner.FetchData(targetIds, numberOfEvents, cancellationToken);
            }

            public Task SendFailAndRecover(long restoreVersion)
            {
                return _inner.SendFailAndRecover(restoreVersion);
            }

            public Task<SubstreamInitializeResponse> SendInitializeRequest(long restoreVersion, long checkpointEpoch, bool cleanHandoff, CancellationToken cancellationToken)
            {
                return _inner.SendInitializeRequest(restoreVersion, checkpointEpoch, cleanHandoff, cancellationToken);
            }

            public Task SendCheckpointDone(long checkpointVersion, long targetCheckpointEpoch, bool coversPeerStopBarrier)
            {
                return _inner.SendCheckpointDone(checkpointVersion, targetCheckpointEpoch, coversPeerStopBarrier);
            }

            public void InitializeDurabilityClaims(Func<long, int, long, long, bool, Task> callReceiveDurabilityClaim)
            {
                _inner.InitializeDurabilityClaims(callReceiveDurabilityClaim);
            }

            public Task SendDurabilityClaim(long version, int radius, long senderCheckpointEpoch, long targetCheckpointEpoch, bool requestReply, CancellationToken cancellationToken)
            {
                if (Drop && radius > 0)
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
                return _inner.SendDurabilityClaim(version, radius, senderCheckpointEpoch, targetCheckpointEpoch, requestReply, cancellationToken);
            }
        }
    }
}
