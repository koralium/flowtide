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

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance
{
    /// <summary>
    /// Maintenance requests and bytes; a ledger drains them at its rates.
    /// </summary>
    internal readonly record struct CapacityDemand(long Requests, long Bytes)
    {
        public static CapacityDemand operator +(CapacityDemand left, CapacityDemand right) => new CapacityDemand(left.Requests + right.Requests, left.Bytes + right.Bytes);

        public static CapacityDemand operator -(CapacityDemand left, CapacityDemand right) => new CapacityDemand(left.Requests - right.Requests, left.Bytes - right.Bytes);
    }

    /// <summary>
    /// Whether a lease's demand counts toward admission; only runnable demand does.
    /// </summary>
    internal enum CapacityState : byte
    {
        Runnable,
        Paused,
        Suspended
    }

    /// <summary>
    /// The process's maintenance demand by advance at the configured rates: new expensive work is declined above twelve hours of runnable drain until it falls below six.
    /// </summary>
    internal sealed class CapacityLedger
    {
        public const double CloseSeconds = 43_200;

        public const double ReopenSeconds = 21_600;

        private readonly object _lock = new object();
        private readonly Func<CapacityDemand> _ambient;
        private readonly Dictionary<(string LeaseKey, string AdvanceId), CapacityLease> _leases = new Dictionary<(string LeaseKey, string AdvanceId), CapacityLease>();
        // By state; guarded by _lock
        private readonly CapacityDemand[] _totals = new CapacityDemand[3];
        private volatile bool _closed;

        // Ambient demand is work outside the ledger, read outside its lock
        public CapacityLedger(Func<CapacityDemand> ambient, long requestsPerSecond = MaintenanceBuckets.DefaultRequestsPerSecond, long bytesPerSecond = MaintenanceBuckets.DefaultBytesPerSecond)
        {
            _ambient = ambient;
            RequestsPerSecond = requestsPerSecond;
            BytesPerSecond = bytesPerSecond;
        }

        public long RequestsPerSecond { get; }

        public long BytesPerSecond { get; }

        // The slower of the two drains
        public double DrainSeconds(CapacityDemand demand) => Math.Max(demand.Requests / (double)RequestsPerSecond, demand.Bytes / (double)BytesPerSecond);

        /// <summary>
        /// Queued discard hints, one request each, and the buckets' unpaid debt.
        /// </summary>
        public static CapacityDemand Ambient(MaintenanceBuckets buckets, DiscardHandOff discards) => new CapacityDemand(discards.Queued + buckets.Requests.Debt, buckets.Bytes.Debt);

        // Set by a decline or by runnable demand past twelve hours, cleared by a check once the runnable drain is below six hours
        public bool Closed => _closed;

        // Runs under the lock between a reservation's check and its add
        internal Action? BetweenCheckAndAddForTests { get; set; }

        internal object Sync => _lock;

        // Runnable leases and ambient demand, as admission sees them
        public CapacityDemand Current
        {
            get
            {
                var ambient = _ambient();
                lock (_lock)
                {
                    return _totals[(int)CapacityState.Runnable] + ambient;
                }
            }
        }

        public CapacityDemand Total(CapacityState state)
        {
            lock (_lock)
            {
                return _totals[(int)state];
            }
        }

        /// <summary>
        /// Whether new expensive work may plan; holds nothing.
        /// </summary>
        public bool Admits()
        {
            var ambient = _ambient();
            lock (_lock)
            {
                return AdmitsLocked(_totals[(int)CapacityState.Runnable] + ambient, default);
            }
        }

        /// <summary>
        /// Adds the demand as runnable when admission is open and the runnable drain with it stays within twelve hours; null otherwise.
        /// </summary>
        /// <param name="owner">The stop token of the worker whose job holds the lease.</param>
        public CapacityLease? TryReserve(CancellationToken owner, string leaseKey, string advanceId, CapacityDemand demand)
        {
            var ambient = _ambient();
            lock (_lock)
            {
                if (!AdmitsLocked(_totals[(int)CapacityState.Runnable] + ambient, demand))
                {
                    return null;
                }
                BetweenCheckAndAddForTests?.Invoke();
                return InstallLocked(owner, leaseKey, advanceId, demand, CapacityState.Runnable);
            }
        }

        /// <summary>
        /// Demand found after a restart, exempt from admission; it replaces any lease of the same key, and a stopped owner's runnable demand is paused.
        /// </summary>
        /// <param name="owner">The stop token of the worker whose job holds the lease.</param>
        public CapacityLease Restore(CancellationToken owner, string leaseKey, string advanceId, CapacityDemand demand, CapacityState state)
        {
            var ambient = _ambient();
            lock (_lock)
            {
                var lease = InstallLocked(owner, leaseKey, advanceId, demand, state);
                CloseAboveLocked(_totals[(int)CapacityState.Runnable] + ambient);
                return lease;
            }
        }

        /// <summary>
        /// The most severe state among the owner's leases of the table, null without one.
        /// </summary>
        public CapacityState? StateOf(CancellationToken owner, string leaseKey)
        {
            lock (_lock)
            {
                CapacityState? state = null;
                foreach (var lease in _leases.Values)
                {
                    // Suspended over paused over runnable, in value order
                    if (lease.Owner == owner && lease.LeaseKey == leaseKey && (state == null || lease.StateLocked > state.Value))
                    {
                        state = lease.StateLocked;
                    }
                }
                return state;
            }
        }

        /// <summary>
        /// Removes the advance's lease, whichever lease holds its key.
        /// </summary>
        public void Release(string leaseKey, string advanceId)
        {
            lock (_lock)
            {
                if (_leases.Remove((leaseKey, advanceId), out var lease))
                {
                    _totals[(int)lease.StateLocked] -= lease.DemandLocked;
                }
            }
        }

        /// <summary>
        /// Removes the owner's and stopped owners' leases of the table whose advance is not among those given.
        /// </summary>
        public void Retain(CancellationToken owner, string leaseKey, IReadOnlySet<string> advanceIds)
        {
            lock (_lock)
            {
                foreach (var lease in _leases.Values.Where(x => (x.Owner == owner || x.Owner.IsCancellationRequested) && x.LeaseKey == leaseKey && !advanceIds.Contains(x.AdvanceId)).ToList())
                {
                    _totals[(int)lease.StateLocked] -= lease.DemandLocked;
                    _leases.Remove((lease.LeaseKey, lease.AdvanceId));
                }
            }
        }

        /// <summary>
        /// Moves the owner's runnable leases to paused, as when its worker stops.
        /// </summary>
        public void Pause(CancellationToken owner)
        {
            lock (_lock)
            {
                foreach (var lease in _leases.Values)
                {
                    if (lease.Owner == owner && lease.StateLocked == CapacityState.Runnable)
                    {
                        MoveLocked(lease, lease.DemandLocked, CapacityState.Paused);
                    }
                }
            }
        }

        internal void Update(CapacityLease lease, CapacityDemand remaining)
        {
            var ambient = _ambient();
            lock (_lock)
            {
                if (IsCurrentLocked(lease))
                {
                    MoveLocked(lease, remaining, lease.StateLocked);
                    CloseAboveLocked(_totals[(int)CapacityState.Runnable] + ambient);
                }
            }
        }

        internal void Remove(CapacityLease lease)
        {
            lock (_lock)
            {
                if (IsCurrentLocked(lease))
                {
                    _totals[(int)lease.StateLocked] -= lease.DemandLocked;
                    _leases.Remove((lease.LeaseKey, lease.AdvanceId));
                }
            }
        }

        // Reopens below six hours, then closes when the proposal would pass twelve
        private bool AdmitsLocked(CapacityDemand current, CapacityDemand proposed)
        {
            if (_closed && DrainSeconds(current) < ReopenSeconds)
            {
                _closed = false;
            }
            if (!_closed && DrainSeconds(current + proposed) > CloseSeconds)
            {
                _closed = true;
            }
            return !_closed;
        }

        // Closes admission once runnable demand passes twelve hours, however it got there
        private void CloseAboveLocked(CapacityDemand current)
        {
            if (DrainSeconds(current) > CloseSeconds)
            {
                _closed = true;
            }
        }

        private CapacityLease InstallLocked(CancellationToken owner, string leaseKey, string advanceId, CapacityDemand demand, CapacityState state)
        {
            if (_leases.Remove((leaseKey, advanceId), out var replaced))
            {
                _totals[(int)replaced.StateLocked] -= replaced.DemandLocked;
            }
            // A stopped owner's runnable demand installs paused
            if (state == CapacityState.Runnable && owner.IsCancellationRequested)
            {
                state = CapacityState.Paused;
            }
            var lease = new CapacityLease(this, owner, leaseKey, advanceId, demand, state);
            _leases[(leaseKey, advanceId)] = lease;
            _totals[(int)state] += demand;
            return lease;
        }

        private void MoveLocked(CapacityLease lease, CapacityDemand demand, CapacityState state)
        {
            _totals[(int)lease.StateLocked] -= lease.DemandLocked;
            lease.DemandLocked = demand;
            lease.StateLocked = state;
            _totals[(int)state] += demand;
        }

        // A replaced or removed lease changes nothing
        private bool IsCurrentLocked(CapacityLease lease) => _leases.TryGetValue((lease.LeaseKey, lease.AdvanceId), out var current) && ReferenceEquals(current, lease);
    }

    /// <summary>
    /// One advance's demand in the ledger until it is disposed or another lease of its key replaces it.
    /// </summary>
    internal sealed class CapacityLease : IDisposable
    {
        private readonly CapacityLedger _ledger;

        internal CapacityLease(CapacityLedger ledger, CancellationToken owner, string leaseKey, string advanceId, CapacityDemand demand, CapacityState state)
        {
            _ledger = ledger;
            Owner = owner;
            LeaseKey = leaseKey;
            AdvanceId = advanceId;
            DemandLocked = demand;
            StateLocked = state;
        }

        // The stop token of the worker whose job holds it
        public CancellationToken Owner { get; }

        public string LeaseKey { get; }

        public string AdvanceId { get; }

        // The rate its ledger prices requests at
        public long RequestsPerSecond => _ledger.RequestsPerSecond;

        // Changed under the ledger's lock
        internal CapacityDemand DemandLocked;

        internal CapacityState StateLocked;

        public CapacityDemand Demand
        {
            get
            {
                lock (_ledger.Sync)
                {
                    return DemandLocked;
                }
            }
        }

        public CapacityState State
        {
            get
            {
                lock (_ledger.Sync)
                {
                    return StateLocked;
                }
            }
        }

        // The demand left after validated progress
        public void Update(CapacityDemand remaining) => _ledger.Update(this, remaining);

        public void Dispose() => _ledger.Remove(this);
    }
}
