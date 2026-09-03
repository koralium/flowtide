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
    /// Correlation window clock, ticks on every cache insertion.
    /// A hit within the window of an entrys last counted hit does not count as reuse,
    /// so a burst of accesses close together counts as one reuse event.
    /// </summary>
    internal sealed class S3FifoCorrelationClock
    {
        private long m_sequence;
        private int m_windowSize;

        /// <summary>
        /// Advances the clock and returns the new stamp.
        /// </summary>
        public long NextSequence()
        {
            return Interlocked.Increment(ref m_sequence);
        }

        /// <summary>
        /// Current clock value without advancing, used to stamp a counted hit.
        /// </summary>
        public long CurrentSequence()
        {
            return Volatile.Read(ref m_sequence);
        }

        /// <summary>
        /// Sets the window width. Zero disables the filter.
        /// </summary>
        public void SetWindowSize(int windowSize)
        {
            Volatile.Write(ref m_windowSize, windowSize);
        }

        /// <summary>
        /// True when the hit is still inside the correlation window.
        /// </summary>
        public bool IsCorrelated(long stamp)
        {
            var window = Volatile.Read(ref m_windowSize);
            return window > 0 && Volatile.Read(ref m_sequence) - stamp < window;
        }

        internal int WindowSizeForTests => Volatile.Read(ref m_windowSize);
    }
}
