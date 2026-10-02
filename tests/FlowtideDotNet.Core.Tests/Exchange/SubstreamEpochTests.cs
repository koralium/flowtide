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

using FlowtideDotNet.Core.Operators.Exchange;

namespace FlowtideDotNet.Core.Tests.Exchange
{
    public class SubstreamEpochTests
    {
        /// <summary>
        /// Back to back draws never repeat, even within one tick.
        /// </summary>
        [Fact]
        public void SequentialDrawsStrictlyIncrease()
        {
            long previous = SubstreamEpoch.Next(0);
            for (int i = 0; i < 10_000; i++)
            {
                long next = SubstreamEpoch.Next(0);
                Assert.True(next > previous, $"draw {i}: {next} after {previous}");
                previous = next;
            }
        }

        /// <summary>
        /// A floor ahead of the clock is passed within stride.
        /// </summary>
        [Fact]
        public void FloorAheadOfTheClockIsPassedWithinTheStride()
        {
            long floor = DateTime.UtcNow.Ticks + TimeSpan.FromDays(1).Ticks;
            long next = SubstreamEpoch.Next(floor);
            Assert.InRange(next, floor + 1, floor + SubstreamEpoch.FloorStride);
        }

        /// <summary>
        /// Draws from one floor ahead of the clock never lockstep.
        /// </summary>
        [Fact]
        public void DrawsFromOneFloorDoNotLockstep()
        {
            long floor = DateTime.UtcNow.Ticks + TimeSpan.FromDays(1).Ticks;
            var draws = Enumerable.Range(0, 100).Select(_ => SubstreamEpoch.Next(floor)).ToList();
            Assert.True(draws.Distinct().Count() > 1, "every draw from the same floor landed on one value");
        }

        /// <summary>
        /// Advance stays above the epoch and the floor.
        /// </summary>
        [Fact]
        public void AdvanceRaisesAboveEpochAndFloor()
        {
            long epoch = SubstreamEpoch.Next(0);
            long before = epoch;
            Assert.True(SubstreamEpoch.Advance(ref epoch) > before);

            long recorded = epoch + TimeSpan.FromDays(1).Ticks;
            long raised = SubstreamEpoch.Advance(ref epoch, recorded);
            Assert.InRange(raised, recorded + 1, recorded + SubstreamEpoch.FloorStride);
            Assert.Equal(raised, epoch);

            // Ahead of the clock, failures keep counting up.
            Assert.True(SubstreamEpoch.Advance(ref epoch) > raised);
        }

        /// <summary>
        /// Racing advances on one epoch never return the same value.
        /// </summary>
        [Fact]
        public void ConcurrentAdvancesAreUnique()
        {
            long epoch = SubstreamEpoch.Next(0);
            var drawn = new System.Collections.Concurrent.ConcurrentBag<long>();
            Parallel.For(0, 8, _ =>
            {
                for (int i = 0; i < 1_000; i++)
                {
                    drawn.Add(SubstreamEpoch.Advance(ref epoch));
                }
            });
            Assert.Equal(drawn.Count, drawn.Distinct().Count());
            Assert.Equal(drawn.Max(), epoch);
        }
    }
}
