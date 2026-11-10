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
using System.Text;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public class RoaringRunContainerTests
    {
        [Theory]
        [InlineData(2_049, 1)]
        [InlineData(6_000, 0)]
        [InlineData(1, 4_096)]
        public void RunsOverFourThousandValuesDecodeToABitmap(int runs, int length)
        {
            var spans = Enumerable.Range(0, runs).Select(i => ((ushort)(i * (length + 2)), (ushort)length)).ToList();

            var bitmap = Decode(spans);

            var container = Assert.Single(bitmap.Containers);
            Assert.IsType<BitmapContainer>(container);
            Assert.True(container.ArraySizeInBytes <= 8_192);
            Assert.Equal(Values(spans), bitmap.ToList());
        }

        [Theory]
        [InlineData(4_095)]
        [InlineData(65_535)]
        public void AFullRunDecodesEveryValue(int length)
        {
            var spans = new List<(ushort, ushort)> { (0, (ushort)length) };

            Assert.Equal(Values(spans), Decode(spans).ToList());
        }

        [Fact]
        public void ExactlyFourThousandValuesStayAnArray()
        {
            var spans = Enumerable.Range(0, 2_048).Select(i => ((ushort)(i * 3), (ushort)1)).ToList();

            var bitmap = Decode(spans);

            Assert.IsType<ArrayContainer>(Assert.Single(bitmap.Containers));
            Assert.Equal(4_096, bitmap.Cardinality);
        }

        [Fact]
        public void DecodedRunsSurviveAWriteAndRead()
        {
            var spans = Enumerable.Range(0, 2_049).Select(i => ((ushort)(i * 3), (ushort)1)).ToList();
            var bitmap = Decode(spans);
            using var stream = new MemoryStream();

            RoaringBitmap.Serialize(bitmap, stream);
            stream.Position = 0;

            Assert.Equal(Values(spans), RoaringBitmap.Deserialize(stream).ToList());
        }

        [Fact]
        public void ARunPastTheContainersEndIsMalformed()
        {
            var error = Assert.Throws<InvalidDataException>(() => Decode(new List<(ushort, ushort)> { (65_535, 1) }));

            Assert.Contains("passes the container's end", error.Message);
            Assert.Equal(6, Decode(new List<(ushort, ushort)> { (65_530, 5) }).Cardinality);
        }

        [Fact]
        public void OverlappingOrUnsortedRunsAreMalformed()
        {
            Assert.Throws<InvalidDataException>(() => Decode(new List<(ushort, ushort)> { (10, 4_095), (0, 0) }));
            Assert.Throws<InvalidDataException>(() => Decode(new List<(ushort, ushort)> { (100, 0), (50, 0) }));
            Assert.Throws<InvalidDataException>(() => Decode(new List<(ushort, ushort)> { (0, 3_000), (1_000, 3_000) }));
            Assert.Throws<InvalidDataException>(() => Decode(new List<(ushort, ushort)> { (0, 9) }, cardinality: 5));
            Assert.Throws<InvalidDataException>(() => Decode(new List<(ushort, ushort)> { (0, 4_095) }, cardinality: 5));
            Assert.Throws<InvalidDataException>(() => Decode(new List<(ushort, ushort)> { (0, 65_535) }, cardinality: 1));
        }

        private static RoaringBitmap Decode(IReadOnlyList<(ushort Start, ushort Length)> runs, int? cardinality = null)
        {
            using var stream = new MemoryStream();
            using (var writer = new BinaryWriter(stream, Encoding.UTF8, leaveOpen: true))
            {
                writer.Write(12347u);
                writer.Write((byte)1);
                writer.Write((ushort)0);
                writer.Write((ushort)((cardinality ?? Math.Min(runs.Sum(x => x.Length + 1), 65_536)) - 1));
                writer.Write((ushort)runs.Count);
                foreach (var (start, length) in runs)
                {
                    writer.Write(start);
                    writer.Write(length);
                }
            }
            stream.Position = 0;
            return RoaringBitmap.Deserialize(stream);
        }

        private static List<int> Values(IEnumerable<(ushort Start, ushort Length)> runs)
        {
            return runs.SelectMany(x => Enumerable.Range(x.Start, x.Length + 1)).ToList();
        }
    }
}
