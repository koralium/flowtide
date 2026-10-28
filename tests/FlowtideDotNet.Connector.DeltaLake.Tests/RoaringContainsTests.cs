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

using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.DeletionVectors.RoaringBitmap;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public class RoaringContainsTests
    {
        [Fact]
        public void AnEmptyArrayContainsNothing()
        {
            var empty = RoaringBitmapArray.Create(Array.Empty<long>());

            Assert.False(empty.Contains(0));
            Assert.False(empty.Contains(-1));
            Assert.False(empty.Contains(1L << 32));
        }

        [Fact]
        public void LookupsMatchTheEnumeratedValues()
        {
            var random = new Random(7);
            // Container edges and a second high word
            var values = new SortedSet<long>() { 0, 1, 65_535, 65_536, 131_071, (1L << 32) - 1, 1L << 32, (1L << 32) + 70_000 };
            // Over 4,096 values in one container makes a bitmap container
            for (int i = 0; i < 6_000; i++)
            {
                values.Add(262_144 + random.Next(65_536));
            }
            var array = RoaringBitmapArray.Create(values);
            var probes = values.Concat(new long[] { -1, 2, 65_534, 65_537, (1L << 32) + 1, 3L << 32, long.MaxValue })
                .Concat(Enumerable.Range(0, 2_000).Select(_ => (long)random.Next(0, 400_000)));

            foreach (var probe in probes)
            {
                Assert.True(Enumerable.Contains(array, probe) == array.Contains(probe), $"{probe}");
            }
        }
    }
}
