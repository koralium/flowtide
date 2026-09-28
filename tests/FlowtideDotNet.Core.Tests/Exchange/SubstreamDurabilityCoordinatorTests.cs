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
