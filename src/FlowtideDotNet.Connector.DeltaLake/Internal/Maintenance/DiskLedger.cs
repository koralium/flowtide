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
    /// The process-wide disk ledger: every local write reserves first, and a file's charge is released only after the file is deleted.
    /// </summary>
    internal sealed class DiskLedger
    {
        public const long DefaultCap = 4L << 30;

        private readonly object _lock = new object();
        private long _reserved;
        private long _peak;

        public DiskLedger(long cap = DefaultCap)
        {
            Cap = cap;
        }

        public long Cap { get; }

        public long Reserved
        {
            get
            {
                lock (_lock)
                {
                    return _reserved;
                }
            }
        }

        public long Peak
        {
            get
            {
                lock (_lock)
                {
                    return _peak;
                }
            }
        }

        // Frees unpinned cached roots for a reservation that does not fit and returns the bytes freed
        public Func<long, long>? Evict { get; set; }

        public void Reserve(long bytes)
        {
            if (bytes < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(bytes));
            }
            long reserved;
            while (!TryReserve(bytes, out reserved))
            {
                // Unpinned cache entries are evicted before a reservation declines, unless it can never fit
                if (bytes > Cap || Evict?.Invoke(reserved + bytes - Cap) is not > 0)
                {
                    throw new CheckpointDeclinedException(MaintenanceReason.DiskCap, DeclineDependency.None, $"{bytes} bytes over the {Cap} byte disk cap with {reserved} reserved");
                }
            }
        }

        private bool TryReserve(long bytes, out long reserved)
        {
            lock (_lock)
            {
                reserved = _reserved;
                if (_reserved + bytes > Cap)
                {
                    return false;
                }
                _reserved += bytes;
                _peak = Math.Max(_peak, _reserved);
                return true;
            }
        }

        public void Release(long bytes)
        {
            lock (_lock)
            {
                if (bytes < 0 || bytes > _reserved)
                {
                    throw new InvalidOperationException("Released more disk than was reserved");
                }
                _reserved -= bytes;
            }
        }
    }
}
