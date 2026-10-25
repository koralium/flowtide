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

using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public class DiscardRetentionTests
    {
        [Fact]
        public void SettledPrefixesMustNotRemainRetainedAfterTheirChargeIsReleased()
        {
            using var storage = new ScriptedStorage();
            // One route's share holds all ten groups
            var handOff = new DiscardHandOff(16384);
            using var producer = handOff.Acquire(new DiscardRoute(storage, "table"));
            var entry = producer.Entry;
            var now = new DateTimeOffset(2026, 10, 15, 12, 0, 0, TimeSpan.Zero);

            // Each group first fails one delete, then drains its remaining due siblings in the next turn
            for (var attempt = 0; attempt < 10; attempt++)
            {
                var paths = Enumerable.Range(0, 16)
                    .Select(i => $"g{attempt:D2}-{i:D2}-" + new string('x', 20))
                    .ToArray();
                var group = new DiscardGroup("table", 0, attempt, "stage-" + attempt, paths);
                Assert.Equal(0, entry.Enqueue(group));
                entry.Pending();
                Assert.Equal(7, entry.Conclude(group, new[]
                {
                    HintState.Failed, HintState.Settled, HintState.Settled, HintState.Settled,
                    HintState.Settled, HintState.Settled, HintState.Settled, HintState.Settled
                }, now));
                Assert.Equal(8, entry.Conclude(group, Enumerable.Repeat(HintState.Settled, 8).ToArray(), now));
                Assert.Equal(1, group.Remaining);
                Assert.Equal(0, group.Due(now, 1));
            }

            Assert.Equal(10, entry.Hints);
            Assert.InRange(handOff.Charged, 1, handOff.Capacity);
            // Every path reference a queued group retains, by the hand-off's own accounting, is still charged
            var retained = entry.Pending().Sum(group => DiscardHandOff.GroupBytes(group)
                + group.Paths.Sum(DiscardHandOff.PathBytes));
            Assert.True(retained <= handOff.Charged,
                $"The {handOff.Capacity}-byte allowance charged {handOff.Charged} bytes but retained at least {retained} bytes.");
        }

        [Fact]
        public void OneRouteHoldsAtMostHalfTheAllowance()
        {
            using var storage = new ScriptedStorage();
            var handOff = new DiscardHandOff(16384);
            using var stuck = handOff.Acquire(new DiscardRoute(storage, "stuck"));
            using var other = handOff.Acquire(new DiscardRoute(storage, "other"));
            Assert.Equal(handOff.Capacity / 2, handOff.RouteCapacity);

            // A route whose hints never drain fills its share, then drops and counts
            var dropped = 0;
            for (var version = 0; dropped == 0; version++)
            {
                dropped = stuck.Entry.Enqueue(new DiscardGroup("table", 0, version, "stage", Paths("s", version, 8)));
            }
            var probe = new DiscardGroup("table", 0, 0, "stage", Paths("s", 0, 1));
            Assert.InRange(stuck.Entry.Charged, handOff.RouteCapacity - DiscardHandOff.GroupBytes(probe) - DiscardHandOff.PathBytes(probe.Paths[0]), handOff.RouteCapacity);
            Assert.Equal(stuck.Entry.Charged, handOff.Charged);
            Assert.Equal(dropped, handOff.Dropped);

            // Another table's hints still fit
            var group = new DiscardGroup("table", 0, 0, "stage", Paths("o", 0, 8));
            Assert.Equal(0, other.Entry.Enqueue(group));
            Assert.Equal(group.Charged, other.Entry.Charged);
            Assert.Equal(stuck.Entry.Charged + other.Entry.Charged, handOff.Charged);

            // A route's release frees its share
            other.Entry.Pending();
            other.Entry.Settle(group, group.Remaining);
            Assert.Equal(0, other.Entry.Charged);
            Assert.Equal(stuck.Entry.Charged, handOff.Charged);
        }

        private static string[] Paths(string prefix, int version, int count) => Enumerable.Range(0, count).Select(i => $"_change_data/{prefix}{version:D4}-{i}.parquet").ToArray();
    }
}
