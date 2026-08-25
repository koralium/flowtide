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
    // The adaptive small queue split, sized from what the ghost queue observes.
    internal partial class S3FifoTableSync
    {
        /// <summary>
        /// The default small queue share, 10% as in the paper.
        /// </summary>
        private const int DefaultSmallTargetPermille = 100;

        /// <summary>
        /// How far the adaptive split may move the small queue share.
        /// </summary>
        private const int MinSmallTargetPermille = 50;
        private const int MaxSmallTargetPermille = 950;

        /// <summary>
        /// Smallest move one ghost hit is worth.
        /// </summary>
        private const int AdaptPermillePerGhostHit = 1;

        /// <summary>
        /// Cap on how much a rare queue's hit is magnified.
        /// </summary>
        private const int AdaptMaxHitWeight = 16;

        /// <summary>
        /// Ghost entries needed before the shares are trusted.
        /// </summary>
        private const int AdaptMinimumEvidence = 64;

        /// <summary>
        /// Unforgiven expiries needed to shrink the share one step.
        /// </summary>
        private const int ExpiriesPerPermille = 64;

        /// <summary>
        /// Expiries one small queue ghost hit forgives.
        /// With ExpiriesPerPermille this sets the reuse ratio the share rests on,
        /// about one hit per 160 evictions, in events on both sides so every
        /// cache size rests at the same ratio.
        /// </summary>
        private const int ExpiriesForgivenPerGhostHit = 100;

        /// <summary>
        /// Most the split may move in one cleanup pass.
        /// </summary>
        private const int AdaptMaxPermillePerPass = 8;

        /// <summary>
        /// Movement earned since the last pass. Guarded by the queue lock.
        /// </summary>
        private long m_pendingGrowPermille;
        private long m_pendingShrinkPermille;
        private long m_expiryCredit;

        /// <summary>
        /// Expiries recent hits have paid for, capped at one ghost horizon.
        /// Guarded by the queue lock.
        /// </summary>
        private long m_expiryForgiveness;

        /// <summary>
        /// Lifetime totals, for diagnostics and tests.
        /// </summary>
        private long m_smallEvictionsSeen;
        private long m_smallGhostHits;
        private long m_mainEvictionsSeen;
        private long m_mainGhostHits;

        private long m_ghostSmallEntries;
        private long m_ghostMainEntries;

        private int SmallQueueTargetSize()
        {
            return SmallQueueTargetSize(Volatile.Read(ref maxSize));
        }

        /// <summary>
        /// Small queue share of a cache size, as the early drain caps it.
        /// Sized off what the pass leaves behind, not the capacity.
        /// </summary>
        private int SmallQueueTargetSize(int cacheSize)
        {
            var permille = tableOptions.AdaptiveSmallQueueSize
                ? Volatile.Read(ref m_smallTargetPermille)
                : DefaultSmallTargetPermille;
            return Math.Max(1, (int)((long)cacheSize * permille / 1000));
        }

        /// <summary>
        /// The small queue share to defend when the cache is full.
        /// Never above the paper's share, adaptation may still take it below.
        /// </summary>
        private int SmallQueuePressureShare(int cacheSize)
        {
            var permille = tableOptions.AdaptiveSmallQueueSize
                ? Math.Min(Volatile.Read(ref m_smallTargetPermille), DefaultSmallTargetPermille)
                : DefaultSmallTargetPermille;
            return Math.Max(1, (int)((long)cacheSize * permille / 1000));
        }

        /// <summary>
        /// Applies the movement earned since the last pass, capped on the net.
        /// Must be called under the queue lock.
        /// </summary>
        private void AdaptSmallTarget()
        {
            if (!tableOptions.AdaptiveSmallQueueSize)
            {
                return;
            }
            var net = m_pendingGrowPermille - m_pendingShrinkPermille;
            m_pendingGrowPermille = 0;
            m_pendingShrinkPermille = 0;
            if (net == 0)
            {
                return;
            }
            net = Math.Clamp(net, -AdaptMaxPermillePerPass, AdaptMaxPermillePerPass);
            var updated = Math.Clamp(m_smallTargetPermille + (int)net, MinSmallTargetPermille, MaxSmallTargetPermille);
            Volatile.Write(ref m_smallTargetPermille, updated);
        }

        /// <summary>
        /// The queue that evicted this key could not hold it.
        /// Must be called under the queue lock.
        /// </summary>
        private void RecordGhostHit(bool fromMain)
        {
            if (!tableOptions.AdaptiveSmallQueueSize)
            {
                return;
            }
            if (fromMain)
            {
                m_mainGhostHits++;
                m_pendingShrinkPermille += GhostHitWeight(m_ghostSmallEntries, m_ghostMainEntries);
            }
            else
            {
                m_smallGhostHits++;
                m_pendingGrowPermille += GhostHitWeight(m_ghostMainEntries, m_ghostSmallEntries);
                m_expiryForgiveness = Math.Min(m_expiryForgiveness + ExpiriesForgivenPerGhostHit, GhostCapacity());
            }
        }

        internal static long GhostHitWeightForTests(long otherGhostEntries, long ownGhostEntries)
            => GhostHitWeight(otherGhostEntries, ownGhostEntries);

        private static long GhostHitWeight(long otherGhostEntries, long ownGhostEntries)
        {
            if (otherGhostEntries + ownGhostEntries < AdaptMinimumEvidence)
            {
                return AdaptPermillePerGhostHit;
            }
            return Math.Clamp(otherGhostEntries / Math.Max(1, ownGhostEntries), AdaptPermillePerGhostHit, AdaptMaxHitWeight);
        }

        /// <summary>
        /// A small queue eviction aged out of ghost unused.
        /// Must be called under the queue lock.
        /// </summary>
        private void RecordGhostExpiry()
        {
            if (!tableOptions.AdaptiveSmallQueueSize)
            {
                return;
            }
            if (m_expiryForgiveness > 0)
            {
                // A recent hit paid for this one, so it is no evidence of junk.
                m_expiryForgiveness--;
                return;
            }
            if (++m_expiryCredit >= ExpiriesPerPermille)
            {
                m_expiryCredit -= ExpiriesPerPermille;
                m_pendingShrinkPermille++;
            }
        }

        internal int SmallTargetPermilleForTests => Volatile.Read(ref m_smallTargetPermille);

        internal int SmallQueuePressureShareForTests(int cacheSize) => SmallQueuePressureShare(cacheSize);

        internal (long Small, long Main, int Remembered) GhostMembershipForTests
        {
            get
            {
                lock (m_queueLock)
                {
                    return (m_ghostSmallEntries, m_ghostMainEntries, m_ghostKeys.Count);
                }
            }
        }

        /// <summary>
        /// The evidence the split is read from.
        /// </summary>
        internal (long SmallEvictions, long SmallHits, long MainEvictions, long MainHits) AdaptEvidenceForTests
        {
            get
            {
                lock (m_queueLock)
                {
                    return (m_smallEvictionsSeen, m_smallGhostHits, m_mainEvictionsSeen, m_mainGhostHits);
                }
            }
        }

    }
}
