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
    public class SubstreamDurabilityCoordinatorTests
    {
        /// <summary>
        /// The claim passed the epoch fence, then the peer restarted and can have come back lower.
        /// </summary>
        [Fact]
        public void AClaimFencedBeforeThePeersEpochChangedIsDropped()
        {
            var coordinator = new SubstreamDurabilityCoordinator(NullLogger.Instance, "subA", new[] { "subB" }, 1);
            coordinator.LocalDurable(5);

            var fenced = coordinator.Generation;
            coordinator.PeerEpochChanged("subB");
            coordinator.PeerClaim("subB", new SubstreamDurabilityClaim(0, 5, 5), default, fenced, requestReply: false);

            Assert.False(coordinator.IsAgreed(5));

            // The same claim fenced after the change counts.
            coordinator.PeerClaim("subB", new SubstreamDurabilityClaim(0, 5, 5), default, coordinator.Generation, requestReply: false);
            Assert.True(coordinator.IsAgreed(5));
        }

        [Fact]
        public void AClaimFencedBeforeAFailureIsDropped()
        {
            var coordinator = new SubstreamDurabilityCoordinator(NullLogger.Instance, "subA", new[] { "subB" }, 1);
            coordinator.LocalDurable(5);

            var fenced = coordinator.Generation;
            coordinator.Invalidate();
            coordinator.EnterWave(default);
            coordinator.LocalDurable(4);
            coordinator.PeerClaim("subB", new SubstreamDurabilityClaim(0, 5, 5), default, fenced, requestReply: false);

            Assert.False(coordinator.IsAgreed(4));
        }

        [Fact]
        public async Task AWithdrawnAgreementIsWaitedForAgain()
        {
            var coordinator = new SubstreamDurabilityCoordinator(NullLogger.Instance, "subA", new[] { "subB" }, 1);
            var held = new HeldContext();
            var known = WithdrawTheAgreementAfterTheWake(coordinator, held, CancellationToken.None);

            // The peer's new run in the same wave, without its storage.
            coordinator.PeerClaim("subB", new SubstreamDurabilityClaim(0, 0, 0), default, coordinator.Generation, requestReply: false);
            held.RunAll();

            Assert.Equal(0, await known.WaitAsync(TimeSpan.FromSeconds(5)));
        }

        [Fact]
        public async Task AnAbortedStartStopsWaitingForAWithdrawnAgreement()
        {
            var coordinator = new SubstreamDurabilityCoordinator(NullLogger.Instance, "subA", new[] { "subB" }, 1);
            var held = new HeldContext();
            using var abort = new CancellationTokenSource();
            var known = WithdrawTheAgreementAfterTheWake(coordinator, held, abort.Token);

            abort.Cancel();
            held.RunAll();

            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => known.WaitAsync(TimeSpan.FromSeconds(5)));
        }

        /// <summary>
        /// The start read the group's version, then the peer that had to come down lost its storage and rejoined the wave at 0.
        /// </summary>
        [Fact]
        public async Task APeerRejoiningBelowTheReadVersionDoesNotStallTheStart()
        {
            var hub = new LocalSubstreamCommunicationHub();
            var wave = new RecoveryWave(3, Guid.NewGuid());
            var (wavesB, durabilityB, pointB) = Substream(hub, "subB", "subC");
            var (wavesC, durabilityC, pointC) = Substream(hub, "subC", "subB");
            Assert.True(wavesB.TryEnter(wave));
            durabilityB.EnterWave(wavesB.ForStart());
            Assert.True(wavesC.TryEnter(wave));
            durabilityC.EnterWave(wavesC.ForStart());
            await pointB.InitializeOperator(5);
            await pointC.InitializeOperator(7);
            durabilityB.LocalInit(5);
            durabilityC.LocalInit(7);
            using var cancel = new CancellationTokenSource(TimeSpan.FromSeconds(60));
            try
            {
                // B has the group's version and waits for C to come down.
                Assert.Equal(5, await SubstreamReadOperator.WhenGroupVersionKnown(durabilityB, cancel.Token).WaitAsync(TimeSpan.FromSeconds(10)));
                Task settledB = SubstreamReadOperator.WhenGroupSettled(durabilityB, cancel.Token);
                Assert.False(settledB.IsCompleted);

                // C dies before its come-down and comes back as a fresh object without its storage.
                durabilityC.Invalidate();
                var (wavesC2, durabilityC2, pointC2) = Substream(hub, "subC", "subB");
                durabilityC2.EnterWave(wavesC2.ForStart());
                await pointC2.InitializeOperator(0);
                durabilityC2.LocalInit(0);
                var knownC = await SubstreamReadOperator.WhenGroupVersionKnown(durabilityC2, cancel.Token).WaitAsync(TimeSpan.FromSeconds(10));
                Task settledC = SubstreamReadOperator.WhenGroupSettled(durabilityC2, cancel.Token);

                var ended = await Task.WhenAny(settledB, Task.Delay(TimeSpan.FromSeconds(10))) == settledB;
                Assert.True(ended, $"B: wave {durabilityB.Wave}, agreed at 0 {durabilityB.IsAgreed(0)}, settled wait {settledB.Status}; C: read {knownC}, wave {durabilityC2.Wave}, settled wait {settledC.Status}.");
                // B comes down to C's 0, C waits for B's lowering wave.
                Assert.Equal(0, await (Task<long?>)settledB);
                Assert.False(settledC.IsCompleted);
            }
            finally
            {
                cancel.Cancel();
            }
        }

        private static (SubstreamRecoveryWaves, SubstreamDurabilityCoordinator, SubstreamCommunicationPoint) Substream(LocalSubstreamCommunicationHub hub, string self, string peer)
        {
            var waves = new SubstreamRecoveryWaves();
            var durability = new SubstreamDurabilityCoordinator(NullLogger.Instance, self, new[] { peer }, 1);
            var handler = hub.CreateFactory(self).GetCommunicationHandler(peer, self);
            return (waves, durability, new SubstreamCommunicationPoint(NullLogger.Instance, self, peer, handler, false, durability, waves));
        }

        // Agreed at 5 wakes the waiter, the peer resets before the woken continuation runs.
        private static Task<long?> WithdrawTheAgreementAfterTheWake(SubstreamDurabilityCoordinator coordinator, HeldContext held, CancellationToken cancellationToken)
        {
            coordinator.LocalInit(5);
            var previous = SynchronizationContext.Current;
            SynchronizationContext.SetSynchronizationContext(held);
            Task<long?> known;
            try
            {
                known = SubstreamReadOperator.WhenGroupVersionKnown(coordinator, cancellationToken);
            }
            finally
            {
                SynchronizationContext.SetSynchronizationContext(previous);
            }

            coordinator.PeerClaim("subB", new SubstreamDurabilityClaim(0, 5, 5), default, coordinator.Generation, requestReply: false);
            Assert.True(coordinator.IsAgreed(5));
            Assert.True(held.HasPending);
            Assert.False(known.IsCompleted);

            coordinator.PeerEpochChanged("subB");
            Assert.False(coordinator.IsAgreedKnown);
            held.RunAll();
            Assert.False(known.IsCompleted, $"Returned {(known.IsCompletedSuccessfully ? known.Result : null)} for a withdrawn agreement.");
            return known;
        }

        private sealed class HeldContext : SynchronizationContext
        {
            private readonly Queue<(SendOrPostCallback Callback, object? State)> _posted = new();

            public bool HasPending
            {
                get
                {
                    lock (_posted) return _posted.Count > 0;
                }
            }

            public override void Post(SendOrPostCallback d, object? state)
            {
                lock (_posted) _posted.Enqueue((d, state));
            }

            public void RunAll()
            {
                var previous = Current;
                SetSynchronizationContext(this);
                try
                {
                    while (true)
                    {
                        (SendOrPostCallback Callback, object? State) next;
                        lock (_posted)
                        {
                            if (!_posted.TryDequeue(out next)) return;
                        }
                        next.Callback(next.State);
                    }
                }
                finally
                {
                    SetSynchronizationContext(previous);
                }
            }
        }
    }
}
