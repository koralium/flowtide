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

using FlowtideDotNet.Orleans.Internal;

namespace FlowtideDotNet.Orleans.Tests
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
    }
}
