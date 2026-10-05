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

using FlowtideDotNet.Cluster.Orleans.Internal;

namespace FlowtideDotNet.Cluster.Orleans.Tests
{
    public class SubStreamGrainKeyTests
    {
        /// <summary>
        /// The key must be injective: with a plain "{stream}_{substream}" concatenation two
        /// different streams whose names contain underscores can map to the same substream
        /// grain and silently mix their state.
        /// </summary>
        [Fact]
        public void DifferentStreamsNeverShareAGrainKey()
        {
            Assert.NotEqual(
                SubStreamGrainKey.Create("orders", "eu_west"),
                SubStreamGrainKey.Create("orders_eu", "west"));
            Assert.NotEqual(
                SubStreamGrainKey.Create("a", "b_substream_0"),
                SubStreamGrainKey.Create("a_b", "substream_0"));
            // Same pair always maps to the same grain
            Assert.Equal(
                SubStreamGrainKey.Create("orders", "substream_0"),
                SubStreamGrainKey.Create("orders", "substream_0"));
        }

        /// <summary>
        /// Legacy detection cannot rely on the key FORMAT alone: an old-format key whose
        /// stream name is a small integer, for example stream "2" with substream "ab_x"
        /// giving "2_ab_x", parses as a valid current-format key for stream "ab". The real
        /// invariant is that the key matches the names persisted in the grain state, which
        /// must be checked when state exists.
        /// </summary>
        [Fact]
        public void KeyMatchingUsesPersistedStateNotFormat()
        {
            // Old format "{stream}_{substream}" for stream "2", substream "ab_x".
            var legacyKey = "2_ab_x";
            Assert.True(SubStreamGrainKey.TryParse(legacyKey, out _, out _), "The ambiguity exists: the legacy key parses as current format");

            // The state-based check must classify it as NOT matching its persisted names.
            Assert.False(SubStreamGrainKey.MatchesState(legacyKey, "2", "ab_x"));

            // And a real current-format key must match its own state.
            var currentKey = SubStreamGrainKey.Create("orders", "substream_0");
            Assert.True(SubStreamGrainKey.MatchesState(currentKey, "orders", "substream_0"));
        }
    }
}
