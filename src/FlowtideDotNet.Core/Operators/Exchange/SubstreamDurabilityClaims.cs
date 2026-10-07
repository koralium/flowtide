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

using System.Diagnostics;

namespace FlowtideDotNet.Core.Operators.Exchange
{
    /// <summary>
    /// Radius k for a version: every substream within k hops of the sender is durable at it. InitVersion is the version the
    /// sender started its run at, it tells a peer at the gate whether the sender is running ahead or still has to come down.
    /// </summary>
    internal readonly record struct SubstreamDurabilityClaim(int Radius, long Version, long InitVersion);

    /// <summary>
    /// Works out the highest version every substream of a connected group is durable at, from
    /// claims each substream only ever makes about itself to its direct peers. Every claim belongs
    /// to a wave, one recovery of the group, and only claims of the wave this table is in count.
    /// </summary>
    internal sealed class SubstreamDurabilityClaims
    {
        /// <summary>
        /// Never claimed, distinct from version 0 which is a real restore version.
        /// </summary>
        public const long Unknown = -1;

        private static readonly IReadOnlyList<SubstreamDurabilityClaim> NoClaims = Array.Empty<SubstreamDurabilityClaim>();

        private readonly object _lock = new object();
        private RecoveryWave _wave;
        private readonly long[] _mine;
        private long _mineInit = Unknown;
        private readonly Dictionary<string, long[]> _peers;
        private readonly Dictionary<string, long> _peersInit;
        private readonly List<(long Version, TaskCompletionSource Waiter)> _waiters = new List<(long, TaskCompletionSource)>();

        /// <param name="peers">The substreams this one exchanges data with directly.</param>
        /// <param name="distance">The most hops between two substreams of the connected group.</param>
        public SubstreamDurabilityClaims(IEnumerable<string> peers, int distance)
        {
            _peers = peers.Distinct().ToDictionary(peer => peer, _ => NewRow(distance));
            _peersInit = _peers.Keys.ToDictionary(peer => peer, _ => Unknown);
            if (_peers.Count > 0 && distance < 1)
            {
                throw new ArgumentOutOfRangeException(nameof(distance), "A direct peer is one hop away.");
            }
            _mine = NewRow(distance);
        }

        /// <summary>
        /// The wave this table is in, the only one whose claims count.
        /// </summary>
        public RecoveryWave Wave
        {
            get
            {
                lock (_lock)
                {
                    return _wave;
                }
            }
        }

        /// <summary>
        /// True once every substream of the group has claimed in this wave, then <see cref="Agreed"/> is the group's version.
        /// </summary>
        public bool IsAgreedKnown
        {
            get
            {
                lock (_lock)
                {
                    return _mine[_mine.Length - 1] != Unknown;
                }
            }
        }

        /// <summary>
        /// The version every substream of the group is known to be durable at, or later.
        /// </summary>
        public long Agreed
        {
            get
            {
                lock (_lock)
                {
                    return _mine[_mine.Length - 1];
                }
            }
        }

        /// <summary>
        /// True once the group's version is known and no direct peer started its run above it: nobody next to this
        /// substream still has to come down, the group leaves init together.
        /// </summary>
        public bool IsSettled
        {
            get
            {
                lock (_lock)
                {
                    return IsSettled_NoLock();
                }
            }
        }

        /// <summary>
        /// Records the version this substream started its run at, returns the claims that grew.
        /// </summary>
        public IReadOnlyList<SubstreamDurabilityClaim> SetLocalInit(long version)
        {
            if (version < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(version));
            }
            lock (_lock)
            {
                _mineInit = version;
                if (version <= _mine[0])
                {
                    return NoClaims;
                }
                _mine[0] = version;
                var grown = new List<SubstreamDurabilityClaim>() { new SubstreamDurabilityClaim(0, version, _mineInit) };
                Recompute(grown);
                return grown;
            }
        }

        /// <summary>
        /// Records that this substream is durable at the version, returns the claims that grew.
        /// </summary>
        public IReadOnlyList<SubstreamDurabilityClaim> SetLocalDurable(long version)
        {
            if (version < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(version));
            }
            lock (_lock)
            {
                if (_mineInit == Unknown)
                {
                    _mineInit = version;
                }
                if (version <= _mine[0])
                {
                    return NoClaims;
                }
                _mine[0] = version;
                var grown = new List<SubstreamDurabilityClaim>() { new SubstreamDurabilityClaim(0, version, _mineInit) };
                Recompute(grown);
                return grown;
            }
        }

        /// <summary>
        /// Records a claim a direct peer made about itself, returns the own claims that grew. A claim of another wave is
        /// from a recovery this table is not in, it decides nothing here.
        /// </summary>
        public IReadOnlyList<SubstreamDurabilityClaim> ApplyPeerClaim(string peer, SubstreamDurabilityClaim claim, RecoveryWave wave = default)
        {
            if (claim.Radius < 0 || claim.Version < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(claim));
            }
            lock (_lock)
            {
                if (wave != _wave)
                {
                    return NoClaims;
                }
                if (!_peers.TryGetValue(peer, out var row))
                {
                    throw new ArgumentException($"'{peer}' is not a direct peer.", nameof(peer));
                }
                // A radius holds every smaller radius, a lost smaller claim is repaired here.
                var highest = Math.Min(claim.Radius, row.Length - 1);
                bool changed = false;
                // The sender's own start version rides on every radius, a lost radius 0 must not hide it.
                if (_peersInit[peer] != claim.InitVersion)
                {
                    _peersInit[peer] = claim.InitVersion;
                    changed = true;
                }
                for (int k = 0; k <= highest; k++)
                {
                    if (claim.Version > row[k])
                    {
                        row[k] = claim.Version;
                        changed = true;
                    }
                }
                if (!changed)
                {
                    return NoClaims;
                }
                var grown = new List<SubstreamDurabilityClaim>();
                Recompute(grown);
                return grown;
            }
        }

        /// <summary>
        /// Every claim this substream currently makes, for a peer that may have missed some.
        /// </summary>
        public IReadOnlyList<SubstreamDurabilityClaim> CurrentClaims()
        {
            lock (_lock)
            {
                var claims = new List<SubstreamDurabilityClaim>();
                // The highest radius per version says it all, see ApplyPeerClaim.
                for (int k = _mine.Length - 1; k >= 0; k--)
                {
                    if (_mine[k] != Unknown && (k == _mine.Length - 1 || _mine[k] > _mine[k + 1]))
                    {
                        claims.Add(new SubstreamDurabilityClaim(k, _mine[k], _mineInit));
                    }
                }
                return claims;
            }
        }

        /// <summary>
        /// Forgets what one peer claimed and everything built on it, its pair epoch changed.
        /// </summary>
        public void ResetPeer(string peer)
        {
            lock (_lock)
            {
                if (!_peers.TryGetValue(peer, out var row))
                {
                    throw new ArgumentException($"'{peer}' is not a direct peer.", nameof(peer));
                }
                Array.Fill(row, Unknown);
                _peersInit[peer] = Unknown;
                // The peer can come back lower, every radius above 0 leaned on its old claims.
                Recompute(null);
            }
        }

        /// <summary>
        /// Forgets everything and joins the wave, this substream re-initializes and version numbers may be reused.
        /// </summary>
        public void EnterWave(RecoveryWave wave)
        {
            List<TaskCompletionSource> cancelled;
            lock (_lock)
            {
                _wave = wave;
                Array.Fill(_mine, Unknown);
                _mineInit = Unknown;
                foreach (var row in _peers.Values)
                {
                    Array.Fill(row, Unknown);
                }
                foreach (var peer in _peersInit.Keys.ToList())
                {
                    _peersInit[peer] = Unknown;
                }
                cancelled = _waiters.Select(w => w.Waiter).ToList();
                _waiters.Clear();
            }
            foreach (var waiter in cancelled)
            {
                waiter.TrySetCanceled();
            }
        }

        /// <summary>
        /// Completes when the group agreed on the version, cancelled by a reset.
        /// </summary>
        public Task WhenAgreed(long version, CancellationToken cancellationToken = default)
        {
            return WhenAgreedAtLeast(version, cancellationToken);
        }

        /// <summary>
        /// Completes as soon as the group's version is known, whatever it is, cancelled by a reset.
        /// </summary>
        public Task WhenAgreedKnown(CancellationToken cancellationToken = default)
        {
            // Every real version is at least 0.
            return WhenAgreedAtLeast(0, cancellationToken);
        }

        /// <summary>
        /// Completes once <see cref="IsSettled"/> or the group's version fell below this substream's start, cancelled by a reset.
        /// </summary>
        public Task WhenSettled(CancellationToken cancellationToken = default)
        {
            return WhenAgreedAtLeast(Settled, cancellationToken);
        }

        /// <summary>
        /// True once settled or lowered, with the group's version if it fell below this substream's start.
        /// </summary>
        public bool TryGetSettleOutcome(out long? loweredTo)
        {
            lock (_lock)
            {
                loweredTo = LoweredBelowInit_NoLock();
                return loweredTo.HasValue || IsSettled_NoLock();
            }
        }

        // A waiter version that stands for the settled condition rather than a threshold.
        private const long Settled = long.MaxValue;

        private Task WhenAgreedAtLeast(long version, CancellationToken cancellationToken)
        {
            TaskCompletionSource waiter;
            lock (_lock)
            {
                if (version == Settled ? IsSettled_NoLock() || LoweredBelowInit_NoLock().HasValue : _mine[_mine.Length - 1] >= version)
                {
                    return Task.CompletedTask;
                }
                waiter = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                _waiters.Add((version, waiter));
            }
            if (cancellationToken.CanBeCanceled)
            {
                var registration = cancellationToken.Register(() =>
                {
                    lock (_lock)
                    {
                        _waiters.RemoveAll(w => ReferenceEquals(w.Waiter, waiter));
                    }
                    waiter.TrySetCanceled(cancellationToken);
                });
                waiter.Task.ContinueWith(static (_, state) => ((CancellationTokenRegistration)state!).Dispose(), registration, TaskScheduler.Default);
            }
            return waiter.Task;
        }

        private void Recompute(List<SubstreamDurabilityClaim>? grown)
        {
            Debug.Assert(Monitor.IsEntered(_lock));
            for (int k = 0; k < _mine.Length - 1; k++)
            {
                // Unknown is the lowest value, one silent peer keeps the radius unknown.
                var candidate = _mine[k];
                foreach (var row in _peers.Values)
                {
                    candidate = Math.Min(candidate, row[k]);
                }
                if (candidate > _mine[k + 1])
                {
                    grown?.Add(new SubstreamDurabilityClaim(k + 1, candidate, _mineInit));
                }
                // Assigned, not maxed: the inputs only grow between resets, a reset must lower it.
                _mine[k + 1] = candidate;
            }
            var agreed = _mine[_mine.Length - 1];
            var settled = IsSettled_NoLock() || LoweredBelowInit_NoLock().HasValue;
            for (int i = _waiters.Count - 1; i >= 0; i--)
            {
                if (_waiters[i].Version == Settled ? settled : _waiters[i].Version <= agreed)
                {
                    _waiters[i].Waiter.TrySetResult();
                    _waiters.RemoveAt(i);
                }
            }
        }

        private bool IsSettled_NoLock()
        {
            // A peer that started at or below the group's version runs ahead of it at most, one that started above still
            // has to come down to it.
            var agreed = _mine[_mine.Length - 1];
            if (agreed == Unknown || _mineInit == Unknown || _mineInit > agreed)
            {
                return false;
            }
            foreach (var init in _peersInit.Values)
            {
                if (init == Unknown || init > agreed)
                {
                    return false;
                }
            }
            return true;
        }

        private long? LoweredBelowInit_NoLock()
        {
            // A peer's new run came back below this start, which then has to come down and never settles.
            var agreed = _mine[_mine.Length - 1];
            return agreed != Unknown && _mineInit != Unknown && agreed < _mineInit ? agreed : null;
        }

        private static long[] NewRow(int distance)
        {
            if (distance < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(distance));
            }
            // Radius 0 up to the distance across the group.
            var row = new long[distance + 1];
            Array.Fill(row, Unknown);
            return row;
        }
    }
}
