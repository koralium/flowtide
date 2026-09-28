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

using Microsoft.Extensions.Logging;
using System.Diagnostics;

namespace FlowtideDotNet.Core.Operators.Exchange
{
    /// <summary>
    /// One per substream: owns the durability claims and sends what grew to every direct peer.
    /// </summary>
    internal sealed class SubstreamDurabilityCoordinator
    {
        /// <summary>
        /// How often a substream that waits for an agreement asks its peers again.
        /// </summary>
        internal static TimeSpan ResendInterval = TimeSpan.FromSeconds(2);

        /// <summary>
        /// When to request transport cancellation. The peer slot stays occupied until the
        /// actual transport operation settles, even if it ignores cancellation.
        /// </summary>
        internal static TimeSpan ResendAbandonAfter = TimeSpan.FromSeconds(30);

        private readonly object _lock = new object();
        private readonly ILogger _logger;
        private readonly string _selfSubstreamName;
        private readonly SubstreamDurabilityClaims _claims;
        private readonly Dictionary<string, PeerSender> _peers = new Dictionary<string, PeerSender>();
        // Bumped by every failure, every start and every peer epoch change. Version numbers are
        // reused after a rollback, so nothing read or received under an older generation may be
        // written or sent.
        private long _generation;
        // False from a failure until the next start resets the claims.
        private bool _active = true;
        private int _pendingWaits;
        private bool _resendLoopRunning;

        private sealed class PeerSender(SubstreamCommunicationPoint point, string name)
        {
            public SubstreamCommunicationPoint Point = point;
            public string Name = name;
            public bool Pending;
            public bool RequestReply;
            public bool Running;
            public long InFlightSince;
            public bool ReportedStall;
        }

        public SubstreamDurabilityCoordinator(ILogger logger, string selfSubstreamName, IReadOnlyCollection<string> peers, int distance)
        {
            _logger = logger;
            _selfSubstreamName = selfSubstreamName;
            _claims = new SubstreamDurabilityClaims(peers, distance);
        }

        public long Generation
        {
            get
            {
                lock (_lock)
                {
                    return _generation;
                }
            }
        }

        public void Register(string peer, SubstreamCommunicationPoint communicationPoint)
        {
            lock (_lock)
            {
                if (_peers.TryGetValue(peer, out var sender))
                {
                    sender.Point = communicationPoint;
                }
                else
                {
                    _peers.Add(peer, new PeerSender(communicationPoint, peer));
                }
            }
        }

        /// <summary>
        /// The stream failed, what is claimed or received from here on belongs to a run that is over.
        /// </summary>
        public void Invalidate()
        {
            lock (_lock)
            {
                _generation++;
                _active = false;
            }
        }

        /// <summary>
        /// A new start in the wave, claims from before it and of other waves do not count.
        /// </summary>
        public void EnterWave(RecoveryWave wave)
        {
            lock (_lock)
            {
                _generation++;
                _active = true;
                _claims.EnterWave(wave);
            }
        }

        public RecoveryWave Wave => _claims.Wave;

        public bool IsAgreedKnown => _claims.IsAgreedKnown;

        /// <summary>
        /// This substream started its run at the version.
        /// </summary>
        public void LocalInit(long version)
        {
            IReadOnlyList<SubstreamDurabilityClaim> grown;
            long generation;
            lock (_lock)
            {
                if (!_active)
                {
                    return;
                }
                generation = _generation;
                grown = _claims.SetLocalInit(version);
            }
            Send(grown, generation, requestReply: false);
        }

        /// <summary>
        /// This substream durably committed the version.
        /// </summary>
        public void LocalDurable(long version)
        {
            IReadOnlyList<SubstreamDurabilityClaim> grown;
            long generation;
            lock (_lock)
            {
                if (!_active)
                {
                    return;
                }
                generation = _generation;
                grown = _claims.SetLocalDurable(version);
            }
            Send(grown, generation, requestReply: false);
        }

        /// <summary>
        /// A claim that passed the epoch fence of its pair while the generation was current.
        /// </summary>
        public void PeerClaim(string peer, SubstreamDurabilityClaim claim, RecoveryWave wave, long fencedGeneration, bool requestReply)
        {
            IReadOnlyList<SubstreamDurabilityClaim> grown;
            long generation;
            lock (_lock)
            {
                if (!_active || fencedGeneration != _generation)
                {
                    return;
                }
                generation = _generation;
                grown = _claims.ApplyPeerClaim(peer, claim, wave);
                if (requestReply && _peers.TryGetValue(peer, out var sender))
                {
                    // Queue the response and return. Waiting for the outgoing slot here
                    // would deadlock two peers replying to each other.
                    Queue_NoLock(sender, requestReply: false);
                }
            }
            Send(grown, generation, requestReply: false);
        }

        /// <summary>
        /// The peer's epoch changed, it can have come back lower than it claimed.
        /// </summary>
        public void PeerEpochChanged(string peer)
        {
            lock (_lock)
            {
                // A claim of the peer that already passed the fence must not land in the emptied row.
                _generation++;
                _claims.ResetPeer(peer);
                if (_active)
                {
                    foreach (var sender in _peers.Values) Queue_NoLock(sender, requestReply: false);
                }
            }
        }

        /// <summary>
        /// Everything this substream claims, to a peer that just finished a handshake.
        /// </summary>
        public void ResendTo(SubstreamCommunicationPoint communicationPoint)
        {
            lock (_lock)
            {
                if (!_active) return;
                foreach (var sender in _peers.Values)
                {
                    if (ReferenceEquals(sender.Point, communicationPoint))
                    {
                        Queue_NoLock(sender, requestReply: false);
                        break;
                    }
                }
            }
        }

        public bool IsAgreed(long version)
        {
            return _claims.Agreed >= version;
        }



        public Task WhenAgreed(long version, CancellationToken cancellationToken)
        {
            return WithResends(_claims.WhenAgreed(version, cancellationToken));
        }

        /// <summary>
        /// The group's version, once every substream of the group claimed in this wave.
        /// </summary>
        public async Task<long> WhenAgreedKnown(CancellationToken cancellationToken)
        {
            while (true)
            {
                await WithResends(_claims.WhenAgreedKnown(cancellationToken));
                // A peer reset after the wake withdrew it, wait for the new one.
                var agreed = _claims.Agreed;
                if (agreed != SubstreamDurabilityClaims.Unknown) return agreed;
            }
        }

        /// <summary>
        /// Completes once every direct peer started its run at the group's version, see <see cref="SubstreamDurabilityClaims.IsSettled"/>.
        /// </summary>
        public Task WhenSettled(CancellationToken cancellationToken)
        {
            return WithResends(_claims.WhenSettled(cancellationToken));
        }

        // A wait on the table keeps the claims going out until it ends, a lost one is sent again.
        private async Task WithResends(Task agreed)
        {
            if (agreed.IsCompleted)
            {
                await agreed;
                return;
            }
            lock (_lock)
            {
                _pendingWaits++;
                if (!_resendLoopRunning)
                {
                    _resendLoopRunning = true;
                    _ = Task.Run(ResendLoop);
                }
            }
            try
            {
                await agreed;
            }
            finally
            {
                lock (_lock)
                {
                    _pendingWaits--;
                }
            }
        }

        private async Task ResendLoop()
        {
            while (true)
            {
                await Task.Delay(ResendInterval).ConfigureAwait(false);
                lock (_lock)
                {
                    if (_pendingWaits == 0)
                    {
                        _resendLoopRunning = false;
                        return;
                    }
                    if (!_active) continue;
                    foreach (var sender in _peers.Values) Queue_NoLock(sender, requestReply: true);
                }
            }
        }

        private void Send(IReadOnlyList<SubstreamDurabilityClaim> claims, long generation, bool requestReply)
        {
            if (claims.Count == 0) return;
            lock (_lock)
            {
                if (!_active || generation != _generation) return;
                foreach (var sender in _peers.Values) Queue_NoLock(sender, requestReply);
            }
        }

        // A single sender per neighbour covers publication, handshakes, replies and retries.
        // Pending work is a request for the current frontier, not retained historical facts.
        private void Queue_NoLock(PeerSender sender, bool requestReply)
        {
            if (sender.InFlightSince != 0 && !sender.ReportedStall &&
                Stopwatch.GetElapsedTime(sender.InFlightSince) >= ResendAbandonAfter)
            {
                sender.ReportedStall = true;
                _logger.LogWarning("A durability send from substream {self} to {peer} is still unsettled after {timeout}. Keeping its neighbour slot occupied until the transport retires the operation.", _selfSubstreamName, sender.Name, ResendAbandonAfter);
            }
            sender.Pending = true;
            sender.RequestReply |= requestReply;
            if (!sender.Running)
            {
                sender.Running = true;
                _ = Task.Run(() => Drain(sender));
            }
        }

        private async Task Drain(PeerSender sender)
        {
            while (true)
            {
                IReadOnlyList<SubstreamDurabilityClaim> current;
                RecoveryWave wave;
                long generation;
                bool requestReply;
                SubstreamCommunicationPoint point;
                lock (_lock)
                {
                    if (!_active || !sender.Pending)
                    {
                        sender.Pending = sender.RequestReply = sender.Running = false;
                        return;
                    }
                    current = _claims.CurrentClaims();
                    wave = _claims.Wave;
                    generation = _generation;
                    point = sender.Point;
                    requestReply = sender.RequestReply;
                    sender.Pending = sender.RequestReply = false;
                }

                // The entire compressed frontier matters: a newer local version cannot
                // replace an older claim covering a larger radius.
                foreach (var claim in current)
                {
                    lock (_lock)
                    {
                        if (!_active || generation != _generation) break;
                        sender.InFlightSince = Stopwatch.GetTimestamp();
                        sender.ReportedStall = false;
                    }
                    using var cancel = new CancellationTokenSource(ResendAbandonAfter);
                    _logger.LogTrace("Substream {self} claims radius {radius} for version {version}", _selfSubstreamName, claim.Radius, claim.Version);
                    // Cancellation is a request, never evidence of settlement. This task
                    // must cover the real operation, including inside transport adapters.
                    await point.SendDurabilityClaim(claim, wave, generation, requestReply, cancel.Token).ConfigureAwait(false);
                    lock (_lock) sender.InFlightSince = 0;
                }
            }
        }
    }
}
