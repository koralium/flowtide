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

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Catalog
{
    /// <summary>
    /// The memory the pruning arrays of every Delta sink in the process may use.
    /// A table's first pruning column has priority: a first column that does not fit asks the tables holding more
    /// to give them up at their next checkpoint. The first registration fixes the capacity, bytes stay charged until their owner frees them.
    /// </summary>
    internal sealed class PruningReservation
    {
        public static PruningReservation Shared { get; } = new PruningReservation();

        private readonly object _lock = new object();
        private readonly List<PruningGrant> _grants = new List<PruningGrant>();
        private long? _capacity;
        private long _charged;

        public long Capacity
        {
            get
            {
                lock (_lock)
                {
                    return _capacity ?? 0;
                }
            }
        }

        public long Charged
        {
            get
            {
                lock (_lock)
                {
                    return _charged;
                }
            }
        }

        /// <summary>
        /// The first registration fixes the capacity, it never shrinks below existing grants.
        /// Returns false when the requested capacity differs from the fixed one.
        /// </summary>
        public bool Register(PruningGrant grant, long requestedCapacity)
        {
            lock (_lock)
            {
                _capacity ??= requestedCapacity;
                _grants.Add(grant);
                return _capacity == requestedCapacity;
            }
        }

        public void Unregister(PruningGrant grant)
        {
            lock (_lock)
            {
                _charged -= grant.Charged;
                grant.Charged = 0;
                _grants.Remove(grant);
            }
        }

        /// <summary>
        /// Charges the bytes if they fit. A first column that does not fit asks other tables to drop their
        /// extra columns, the caller does not get the bytes now.
        /// </summary>
        public bool TryCharge(PruningGrant grant, long bytes, bool firstColumn)
        {
            lock (_lock)
            {
                if (_charged + bytes <= _capacity)
                {
                    _charged += bytes;
                    grant.Charged += bytes;
                    return true;
                }
                if (firstColumn)
                {
                    // Largest first until the shortfall is covered, applied by each owner at its next checkpoint
                    var shortfall = _charged + bytes - (_capacity ?? 0) - _grants.Where(x => x.RevokeRequested).Sum(x => x.ExtraBytes);
                    foreach (var other in _grants.Where(x => x != grant && x.ExtraBytes > 0 && !x.RevokeRequested).OrderByDescending(x => x.ExtraBytes))
                    {
                        if (shortfall <= 0)
                        {
                            break;
                        }
                        other.RevokeRequested = true;
                        shortfall -= other.ExtraBytes;
                    }
                }
                return false;
            }
        }

        // A revocation is taken once, by the owner
        public bool TakeRevocation(PruningGrant grant)
        {
            lock (_lock)
            {
                if (!grant.RevokeRequested)
                {
                    return false;
                }
                grant.RevokeRequested = false;
                return true;
            }
        }

        public void SetExtraBytes(PruningGrant grant, long extraBytes)
        {
            lock (_lock)
            {
                grant.ExtraBytes = extraBytes;
            }
        }

        public void Release(PruningGrant grant, long bytes)
        {
            lock (_lock)
            {
                var released = Math.Min(bytes, grant.Charged);
                grant.Charged -= released;
                _charged -= released;
            }
        }
    }

    /// <summary>
    /// One table's share, only changed under the reservation's lock.
    /// </summary>
    internal sealed class PruningGrant
    {
        public long Charged { get; internal set; }

        // What the columns after the first hold, a revocation frees them
        public long ExtraBytes { get; internal set; }

        public bool RevokeRequested { get; internal set; }
    }
}
