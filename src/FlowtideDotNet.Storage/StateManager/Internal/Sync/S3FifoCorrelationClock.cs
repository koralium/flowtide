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

namespace FlowtideDotNet.Storage.StateManager.Internal.Sync
{
    /// <summary>
    /// Logical clock implementing a 2Q-style correlated-reference window on top of the
    /// S3-FIFO small queue.
    ///
    /// Every enqueue into the small queue advances the clock, and the enqueued entry is
    /// stamped with the sequence. A cache hit only counts toward an entry's frequency once
    /// at least <see cref="SetWindowSize">WindowSize</see> newer entries have been enqueued
    /// behind it. Hits before that are correlated references — multiple touches belonging
    /// to the same logical operation as the insert (a B+ tree write reads, updates and
    /// re-reads the same page within microseconds) — and carry no evidence of genuine
    /// reuse, so counting them lets one-hit-wonder bursts earn promotion to the main queue
    /// and defeat the small queue's filtering.
    ///
    /// The window is half the small queue's target size. In steady state (enqueue rate ≈
    /// evict rate, queue length ≈ target) "WindowSize newer entries behind it" is
    /// equivalent to "the entry has traveled into the older half of the small queue". The
    /// clock is enqueue-driven rather than dequeue-driven on purpose: during warm-up the
    /// cache only grows and nothing is dequeued, so a dequeue-driven clock would never age
    /// anyone and the first eviction wave would treat every hit as correlated, dumping
    /// genuinely hot pages to the ghost queue.
    ///
    /// Thread safety: <see cref="NextSequence"/> is called under the table's queue lock but
    /// uses Interlocked so lock-free readers never see torn state. <see cref="IsCorrelated"/>
    /// runs on the read hot path without any lock and tolerates stale values — a hit
    /// miscounted or skipped right at a queue-transition boundary is noise the heuristic
    /// absorbs.
    /// </summary>
    internal sealed class S3FifoCorrelationClock
    {
        private long m_sequence;
        private int m_windowSize;
        private long m_correlatedHitsSuppressed;

        /// <summary>
        /// Advances the clock for a new small-queue enqueue and returns the stamp for the entry.
        /// </summary>
        public long NextSequence()
        {
            return Interlocked.Increment(ref m_sequence);
        }

        /// <summary>
        /// Sets the window width in small-queue enqueues. Zero disables the filter entirely
        /// (small caches), reverting to plain S3-FIFO frequency counting.
        /// </summary>
        public void SetWindowSize(int windowSize)
        {
            Volatile.Write(ref m_windowSize, windowSize);
        }

        /// <summary>
        /// True when a hit on an entry stamped with <paramref name="smallQueueStamp"/> is still
        /// inside the correlation window and must not count toward its frequency.
        /// </summary>
        public bool IsCorrelated(long smallQueueStamp)
        {
            var window = Volatile.Read(ref m_windowSize);
            if (window > 0 && Volatile.Read(ref m_sequence) - smallQueueStamp < window)
            {
                // Best-effort approximate counter: a plain non-atomic increment, so it can
                // lose a few counts under the cross-operator races the shared table permits
                // (different operators share one cache table). That is fine for an
                // observability metric and keeps the scan hot path a plain add, no lock.
                m_correlatedHitsSuppressed++;
                return true;
            }
            return false;
        }

        /// <summary>
        /// Cumulative count of hits filtered out by the correlation window: approximately how
        /// many accesses did NOT count toward promoting their page to the main queue because
        /// the page was still a correlated (young-in-small) reference.
        /// </summary>
        public long CorrelatedHitsSuppressed => Volatile.Read(ref m_correlatedHitsSuppressed);

        internal int WindowSizeForTests => Volatile.Read(ref m_windowSize);
    }
}
