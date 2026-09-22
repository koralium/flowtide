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
    /// Radius k for a version: every substream within k hops of the sender is durable at it.
    /// </summary>
    internal readonly record struct SubstreamDurabilityClaim(int Radius, long Version);

    /// <summary>
    /// Works out the highest version every substream of a connected group is durable at, from
    /// claims each substream only ever makes about itself to its direct peers. The claims travel
    /// the group twice: after the first pass this substream knows the group is durable at a
    /// version, after the second it knows that every substream knows that.
    /// </summary>
    internal sealed class SubstreamDurabilityClaims
    {
        /// <summary>
        /// Never claimed, distinct from version 0 which is a real restore version.
        /// </summary>
        public const long Unknown = -1;

        private static readonly IReadOnlyList<SubstreamDurabilityClaim> NoClaims = Array.Empty<SubstreamDurabilityClaim>();

        private readonly object _lock = new object();
        // The radius at which this substream knows the whole group is durable.
        private readonly int _firstPass;
        private long _highestKnownDurable = Unknown;
        private readonly long[] _mine;
        private readonly Dictionary<string, long[]> _peers;
        private readonly List<(long Version, TaskCompletionSource Waiter)> _waiters = new List<(long, TaskCompletionSource)>();

        /// <param name="peers">The substreams this one exchanges data with directly.</param>
        /// <param name="distance">The most hops between two substreams of the connected group.</param>
        public SubstreamDurabilityClaims(IEnumerable<string> peers, int distance)
        {
            _peers = peers.Distinct().ToDictionary(peer => peer, _ => NewRow(distance));
            if (_peers.Count > 0 && distance < 1)
            {
                throw new ArgumentOutOfRangeException(nameof(distance), "A direct peer is one hop away.");
            }
            _firstPass = distance;
            _mine = NewRow(distance);
        }

        /// <summary>
        /// The version this substream knows every substream of the group to be durable at.
        /// </summary>
        public long KnownDurable
        {
            get
            {
                lock (_lock)
                {
                    return _mine[_firstPass];
                }
            }
        }

        /// <summary>
        /// The highest version it ever knew that of. Survives the resets: a version that anyone may
        /// have acted on stays one this substream must not go below.
        /// </summary>
        public long HighestKnownDurable
        {
            get
            {
                lock (_lock)
                {
                    return _highestKnownDurable;
                }
            }
        }

        /// <summary>
        /// The version every substream of the group knows the group to be durable at. Whoever
        /// sees this, every other substream already has it as its <see cref="HighestKnownDurable"/>.
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
                if (version <= _mine[0])
                {
                    return NoClaims;
                }
                _mine[0] = version;
                var grown = new List<SubstreamDurabilityClaim>() { new SubstreamDurabilityClaim(0, version) };
                Recompute(grown);
                return grown;
            }
        }

        /// <summary>
        /// Records a claim a direct peer made about itself, returns the own claims that grew.
        /// </summary>
        public IReadOnlyList<SubstreamDurabilityClaim> ApplyPeerClaim(string peer, SubstreamDurabilityClaim claim)
        {
            if (claim.Radius < 0 || claim.Version < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(claim));
            }
            lock (_lock)
            {
                if (!_peers.TryGetValue(peer, out var row))
                {
                    throw new ArgumentException($"'{peer}' is not a direct peer.", nameof(peer));
                }
                // A radius holds every smaller radius, a lost smaller claim is repaired here.
                var highest = Math.Min(claim.Radius, row.Length - 1);
                bool changed = false;
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
                        claims.Add(new SubstreamDurabilityClaim(k, _mine[k]));
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
                // The peer can come back lower, every radius above 0 leaned on its old claims.
                Recompute(null);
            }
        }

        /// <summary>
        /// Forgets everything, this substream re-initialized and version numbers may be reused.
        /// </summary>
        public void Reset()
        {
            List<TaskCompletionSource> cancelled;
            lock (_lock)
            {
                Array.Fill(_mine, Unknown);
                foreach (var row in _peers.Values)
                {
                    Array.Fill(row, Unknown);
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
            TaskCompletionSource waiter;
            lock (_lock)
            {
                if (_mine[_mine.Length - 1] >= version)
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
                    grown?.Add(new SubstreamDurabilityClaim(k + 1, candidate));
                }
                // Assigned, not maxed: the inputs only grow between resets, a reset must lower it.
                _mine[k + 1] = candidate;
            }
            if (_mine[_firstPass] > _highestKnownDurable)
            {
                _highestKnownDurable = _mine[_firstPass];
            }
            var agreed = _mine[_mine.Length - 1];
            for (int i = _waiters.Count - 1; i >= 0; i--)
            {
                if (_waiters[i].Version <= agreed)
                {
                    _waiters[i].Waiter.TrySetResult();
                    _waiters.RemoveAt(i);
                }
            }
        }

        private static long[] NewRow(int distance)
        {
            if (distance < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(distance));
            }
            // Twice the distance across the group, the second pass rides on the same recursion.
            var row = new long[2 * distance + 1];
            Array.Fill(row, Unknown);
            return row;
        }
    }
}
