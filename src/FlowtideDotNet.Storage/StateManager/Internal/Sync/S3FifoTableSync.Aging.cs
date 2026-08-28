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
    // The aging hand that decides which main queue pages earned their place.
    internal partial class S3FifoTableSync
    {
        /// <summary>
        /// Cache turnovers an unread main page survives.
        /// </summary>
        private const int MainQueueTurnoversToAgeOut = S3FifoCacheEntry.MaxFrequency;

        /// <summary>
        /// Max aging steps per lock hold, the rest ages on later passes.
        /// </summary>
        private const int AgingOperationBudget = 1024;

        /// <summary>
        /// Insertion count the last sweep ran at, and its carried over work.
        /// A pass earns a fraction of a step, so it carries.
        /// </summary>
        private long m_lastAgingSequence;
        private long m_agingCredit;

        /// <summary>
        /// Aging steps so far, tests pin the pacing on it.
        /// Written only on the cleanup thread.
        /// </summary>
        private long m_agingSteps;

        internal long AgingStepsForTests => Volatile.Read(ref m_agingSteps);

        internal static long UsefulAgingSteps(long stepsToRun, int liveMain)
        {
            return Math.Min(stepsToRun, (long)MainQueueTurnoversToAgeOut * Math.Max(0, liveMain));
        }

        /// <summary>
        /// Advances the aging hand over main, decrementing without evicting.
        /// Runs even when nothing needs freeing.
        /// </summary>
        private void AgeMainQueue(int currentCount)
        {
            var sequence = m_correlationClock.CurrentSequence();
            var inserted = sequence - m_lastAgingSequence;
            m_lastAgingSequence = sequence;
            if (inserted <= 0)
            {
                return;
            }

            int liveMain;
            lock (m_queueLock)
            {
                liveMain = m_mainQueue.Count - m_mainStaleCount;
            }
            if (liveMain <= 0)
            {
                return;
            }

            // One sweep of main per cache turnover.
            // Turnover is the resident pages, not the ceiling.
            var turnover = Math.Max(1, currentCount);
            m_agingCredit += inserted * liveMain;
            var stepsToRun = m_agingCredit / turnover;
            m_agingCredit -= stepsToRun * turnover;
            var steps = (int)Math.Min(int.MaxValue, UsefulAgingSteps(stepsToRun, liveMain));
            while (steps > 0)
            {
                var chunk = Math.Min(steps, AgingOperationBudget);
                steps -= chunk;
                lock (m_queueLock)
                {
                    AgeMainQueueChunk(chunk);
                }
                m_agingSteps += chunk;
                if (steps > 0)
                {
                    // Give parked writers a window, as selection does.
                    Thread.Yield();
                }
            }
        }

        /// <summary>
        /// Must be called under the queue lock.
        /// </summary>
        private void AgeMainQueueChunk(int steps)
        {
            for (int i = 0; i < steps && m_mainQueue.Count > 0; i++)
            {
                var entry = m_mainQueue.Dequeue();
                lock (entry)
                {
                    if (entry.Removed)
                    {
                        entry.Location = S3FifoQueueLocation.None;
                        if (m_mainStaleCount > 0)
                        {
                            m_mainStaleCount--;
                        }
                        continue;
                    }
                    if (Volatile.Read(ref entry.Frequency) > 0)
                    {
                        Interlocked.Decrement(ref entry.Frequency);
                    }
                    // Aged pages stay queued, eviction decides whether the space is needed.
                    m_mainQueue.Enqueue(entry);
                }
            }
        }

        /// <summary>
        /// Reaps tombstones, then true when the live head aged out.
        /// Must be called under the queue lock.
        /// </summary>
        private bool MainHeadHasAgedOut(ref int operationBudget)
        {
            while (m_mainQueue.Count > 0 && operationBudget > 0)
            {
                var head = m_mainQueue.Peek();
                lock (head)
                {
                    if (!head.Removed)
                    {
                        return Volatile.Read(ref head.Frequency) == 0;
                    }
                    m_mainQueue.Dequeue();
                    head.Location = S3FifoQueueLocation.None;
                    if (m_mainStaleCount > 0)
                    {
                        m_mainStaleCount--;
                    }
                }
                operationBudget--;
            }
            return false;
        }

    }
}
