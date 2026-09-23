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
        /// How long a re-send that does not complete keeps the next one to that peer back. It is
        /// cancelled then and sent again.
        /// </summary>
        internal static TimeSpan ResendAbandonAfter = TimeSpan.FromSeconds(30);

        private readonly object _lock = new object();
        private readonly ILogger _logger;
        private readonly string _selfSubstreamName;
        private readonly SubstreamDurabilityClaims _claims;
        private readonly Dictionary<string, SubstreamCommunicationPoint> _peers = new Dictionary<string, SubstreamCommunicationPoint>();
        // Bumped by every failure, every start and every peer epoch change. Version numbers are
        // reused after a rollback, so nothing read or received under an older generation may be
        // written or sent.
        private long _generation;
        // False from a failure until the next start resets the claims.
        private bool _active = true;
        private int _pendingWaits;
        private bool _resendLoopRunning;

        public SubstreamDurabilityCoordinator(ILogger logger, string selfSubstreamName, IReadOnlyCollection<string> peers, int distance)
        {
            _logger = logger;
            _selfSubstreamName = selfSubstreamName;
            _claims = new SubstreamDurabilityClaims(peers, distance);
        }

        public long Agreed => _claims.Agreed;

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
                _peers[peer] = communicationPoint;
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
            IReadOnlyList<SubstreamDurabilityClaim>? reply = null;
            SubstreamCommunicationPoint? replyTo = null;
            long generation;
            lock (_lock)
            {
                // Checked together with the write, a failure or a start in between drops it.
                if (!_active || fencedGeneration != _generation)
                {
                    return;
                }
                generation = _generation;
                grown = _claims.ApplyPeerClaim(peer, claim, wave);
                if (requestReply && _peers.TryGetValue(peer, out replyTo))
                {
                    // It waits and may have missed what was sent, this substream may no longer.
                    reply = _claims.CurrentClaims();
                }
            }
            Send(grown, generation, requestReply: false);
            if (reply != null && replyTo != null)
            {
                var ownWave = _claims.Wave;
                foreach (var current in reply)
                {
                    _ = replyTo.SendDurabilityClaim(current, ownWave, generation, requestReply: false);
                }
            }
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
            }
        }

        /// <summary>
        /// Everything this substream claims, to a peer that just finished a handshake.
        /// </summary>
        public void ResendTo(SubstreamCommunicationPoint communicationPoint)
        {
            IReadOnlyList<SubstreamDurabilityClaim> current;
            long generation;
            lock (_lock)
            {
                if (!_active)
                {
                    return;
                }
                generation = _generation;
                current = _claims.CurrentClaims();
            }
            var wave = _claims.Wave;
            foreach (var claim in current)
            {
                _ = communicationPoint.SendDurabilityClaim(claim, wave, generation, requestReply: false);
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
        /// Completes once every substream of the group claimed in this wave, <see cref="Agreed"/> is then the group's version.
        /// </summary>
        public Task WhenAgreedKnown(CancellationToken cancellationToken)
        {
            return WithResends(_claims.WhenAgreedKnown(cancellationToken));
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
            // A claim can be dropped in either direction. The one that still waits asks, a peer
            // that already agreed has stopped sending on its own.
            var inFlight = new Dictionary<string, (Task Batch, long StartedAt, CancellationTokenSource Cancel)>();
            while (true)
            {
                await Task.Delay(ResendInterval);
                IReadOnlyList<SubstreamDurabilityClaim> current;
                List<KeyValuePair<string, SubstreamCommunicationPoint>> peers;
                long generation;
                lock (_lock)
                {
                    if (_pendingWaits == 0)
                    {
                        _resendLoopRunning = false;
                        foreach (var batch in inFlight.Values)
                        {
                            batch.Cancel.Cancel();
                            batch.Cancel.Dispose();
                        }
                        return;
                    }
                    if (!_active)
                    {
                        continue;
                    }
                    generation = _generation;
                    current = _claims.CurrentClaims();
                    peers = new List<KeyValuePair<string, SubstreamCommunicationPoint>>(_peers);
                }
                var wave = _claims.Wave;
                foreach (var peer in peers)
                {
                    if (inFlight.TryGetValue(peer.Key, out var pending))
                    {
                        if (!pending.Batch.IsCompleted && Stopwatch.GetElapsedTime(pending.StartedAt) < ResendAbandonAfter)
                        {
                            // Unreachable, asking again now would only pile up sends.
                            continue;
                        }
                        // Done, or given up on: cancelled so it ends, never more than one per peer.
                        // A send that never completes must not silence the peer for good.
                        pending.Cancel.Cancel();
                        pending.Cancel.Dispose();
                    }
                    var cancel = new CancellationTokenSource();
                    var sends = new List<Task>(current.Count);
                    foreach (var claim in current)
                    {
                        sends.Add(peer.Value.SendDurabilityClaim(claim, wave, generation, requestReply: true, cancel.Token));
                    }
                    inFlight[peer.Key] = (Task.WhenAll(sends), Stopwatch.GetTimestamp(), cancel);
                }
            }
        }

        private void Send(IReadOnlyList<SubstreamDurabilityClaim> claims, long generation, bool requestReply)
        {
            if (claims.Count == 0)
            {
                return;
            }
            List<SubstreamCommunicationPoint> peers;
            lock (_lock)
            {
                peers = new List<SubstreamCommunicationPoint>(_peers.Values);
            }
            var wave = _claims.Wave;
            foreach (var peer in peers)
            {
                foreach (var claim in claims)
                {
                    _logger.LogTrace("Substream {self} claims radius {radius} for version {version}", _selfSubstreamName, claim.Radius, claim.Version);
                    _ = peer.SendDurabilityClaim(claim, wave, generation, requestReply);
                }
            }
        }
    }
}
