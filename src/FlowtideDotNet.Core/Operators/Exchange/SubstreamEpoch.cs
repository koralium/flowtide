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

namespace FlowtideDotNet.Core.Operators.Exchange
{
    /// <summary>
    /// Clock based generation epochs, nothing shared between instances.
    /// </summary>
    internal static class SubstreamEpoch
    {
        internal const long FloorStride = 1L << 20;

        /// <summary>
        /// The clock, or above the floor when behind it.
        /// </summary>
        public static long Next(long floor)
        {
            var now = DateTime.UtcNow.Ticks;
            if (now <= floor)
            {
                // Random stride, generations raised from one floor stay apart.
                return floor + Random.Shared.NextInt64(1, FloorStride + 1);
            }
            // Wait out the tick, later draws read past it.
            var spin = new SpinWait();
            while (DateTime.UtcNow.Ticks == now)
            {
                spin.SpinOnce();
            }
            return now;
        }

        /// <summary>
        /// Raises the epoch above itself and the floor.
        /// </summary>
        public static long Advance(ref long epoch, long floor = 0)
        {
            while (true)
            {
                var current = Interlocked.Read(ref epoch);
                var next = Next(Math.Max(current, floor));
                if (Interlocked.CompareExchange(ref epoch, next, current) == current)
                {
                    return next;
                }
            }
        }
    }
}
