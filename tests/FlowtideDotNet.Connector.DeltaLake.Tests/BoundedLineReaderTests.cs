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

using Stowage;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Checkpoints;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance;
using System.Text;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public class BoundedLineReaderTests
    {
        private const int Cap = 4 << 20;

        private static async Task<List<string>> Lines(byte[] content, JobMemory? memory = null, int cap = int.MaxValue, bool skip = false)
        {
            using var reader = new BoundedLineReader(new MemoryStream(content), memory, cap, skip);
            var lines = new List<string>();
            while (await reader.ReadLineAsync())
            {
                lines.Add(Encoding.UTF8.GetString(reader.Line.Span));
            }
            return lines;
        }

        private static List<string> StreamReaderLines(byte[] content)
        {
            using var reader = new StreamReader(new MemoryStream(content));
            var lines = new List<string>();
            string? line;
            while ((line = reader.ReadLine()) != null)
            {
                lines.Add(line);
            }
            return lines;
        }

        [Fact]
        public async Task SplitsLinesAsStreamReaderDoes()
        {
            var random = new Random(7);
            var pieces = new[] { "{\"add\":{\"path\":\"a\"}}", "", " ", "\t", "åäö€😀", new string('x', 70_000), "{}" };
            var endings = new[] { "\n", "\r\n", "\r", "" };
            for (int run = 0; run < 200; run++)
            {
                var text = new StringBuilder();
                var count = random.Next(0, 8);
                for (int i = 0; i < count; i++)
                {
                    text.Append(pieces[random.Next(pieces.Length)]);
                    text.Append(endings[random.Next(endings.Length)]);
                }
                var bytes = Encoding.UTF8.GetBytes(text.ToString());
                if (random.Next(4) == 0)
                {
                    bytes = new byte[] { 0xEF, 0xBB, 0xBF }.Concat(bytes).ToArray();
                }
                Assert.Equal(StreamReaderLines(bytes), await Lines(bytes));
            }
        }

        [Fact]
        public void BlankMatchesIsNullOrWhiteSpace()
        {
            foreach (var line in new[] { "", " ", "\t \v\f", " ", "   ", "a", " {", "å" })
            {
                Assert.Equal(string.IsNullOrWhiteSpace(line), BoundedLineReader.IsBlank(Encoding.UTF8.GetBytes(line)));
            }
        }

        [Fact]
        public async Task OversizedCommitLineDeclinesBeforeBuffering()
        {
            var big = "{\"add\":{\"path\":\"a\",\"stats\":\"" + new string('x', 12 << 20) + "\"}}\n";
            var memory = new JobMemory();
            var declined = await Assert.ThrowsAsync<CheckpointDeclinedException>(() => Lines(Encoding.UTF8.GetBytes(big), memory, Cap, skip: true));
            Assert.Equal(MaintenanceReason.LineOverCap, declined.Reason);
            Assert.Equal(DeclineDependency.Tail, declined.Dependency);
            // At most a growth to the cap with its old buffer and the read buffer, well below the line itself
            Assert.True(memory.ManagedPeak <= 2L * Cap + BoundedLineReader.ReadSize && memory.ManagedPeak < big.Length, $"Peak {memory.ManagedPeak}");
        }

        [Fact]
        public async Task OversizedCommitInfoLineIsSkipped()
        {
            var commitInfo = "{\"commitInfo\":{\"timestamp\":1,\"operation\":\"" + new string('x', 12 << 20) + "\",\"nested\":{\"a\":[1,{\"b\":\"}\\\"\"}]}}}\r\n";
            var content = "{\"add\":{\"path\":\"a\"}}\n" + commitInfo + "{\"add\":{\"path\":\"b\"}}\n";
            var memory = new JobMemory();
            using (var reader = new BoundedLineReader(new MemoryStream(Encoding.UTF8.GetBytes(content)), memory, Cap, skipOversizedCommitInfo: true))
            {
                var lines = new List<string>();
                while (await reader.ReadLineAsync())
                {
                    lines.Add(Encoding.UTF8.GetString(reader.Line.Span));
                }
                Assert.Equal(new[] { "{\"add\":{\"path\":\"a\"}}", "{\"add\":{\"path\":\"b\"}}" }, lines);
                Assert.Equal(1, reader.SkippedLines);
            }
            Assert.True(memory.ManagedPeak <= 2L * Cap + BoundedLineReader.ReadSize && memory.ManagedPeak < commitInfo.Length, $"Peak {memory.ManagedPeak}");
            Assert.Equal(0, memory.Managed);
        }

        [Theory]
        [InlineData("{\"commitInfo\":{\"a\":\"FILL\"},\"add\":{\"path\":\"p\"}}")]
        [InlineData("{\"add\":{\"path\":\"p\",\"stats\":\"FILL\"}}")]
        [InlineData("{\"commit\\u0049nfo\":{\"a\":\"FILL\"}}")]
        [InlineData("{\"commitInfo\":\"FILL\"}")]
        [InlineData("{\"commitInfo\":{\"a\":\"FILL\"}} trailing")]
        [InlineData("{\"commitInfo\":{\"x\":[\"FILL\"}]}")]
        public async Task OversizedLineDeclinesUnlessItHoldsOnlyCommitInfo(string template)
        {
            var line = template.Replace("FILL", new string('x', 5 << 20));
            var declined = await Assert.ThrowsAsync<CheckpointDeclinedException>(() => Lines(Encoding.UTF8.GetBytes(line), new JobMemory(), Cap, skip: true));
            Assert.Equal(MaintenanceReason.LineOverCap, declined.Reason);
        }

        [Fact]
        public async Task OversizedCommitInfoDeclinesWhenSkippingIsOff()
        {
            var line = "{\"commitInfo\":{\"a\":\"" + new string('x', 5 << 20) + "\"}}";
            await Assert.ThrowsAsync<CheckpointDeclinedException>(() => Lines(Encoding.UTF8.GetBytes(line), new JobMemory(), Cap, skip: false));
        }

        private sealed class ShortReadStream(byte[] bytes, int chunkSize) : MemoryStream(bytes)
        {
            public override int Read(byte[] buffer, int offset, int count) => base.Read(buffer, offset, Math.Min(count, chunkSize));

            public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default) =>
                base.ReadAsync(buffer[..Math.Min(buffer.Length, chunkSize)], cancellationToken);
        }

        private sealed class ShortReadStorage(IFileStorage inner, int chunkSize) : HookFileStorage(inner), IFileStorage
        {
            public new async Task<Stream?> OpenRead(IOPath path, CancellationToken cancellationToken = default)
            {
                using var stream = await base.OpenRead(path, cancellationToken);
                if (stream == null)
                {
                    return null;
                }
                using var copy = new MemoryStream();
                await stream.CopyToAsync(copy, cancellationToken);
                return new ShortReadStream(copy.ToArray(), chunkSize);
            }
        }

        [Theory]
        [InlineData(1)]
        [InlineData(2)]
        [InlineData(3)]
        public async Task Utf8BomSplitAcrossReadsMatchesStreamReader(int chunkSize)
        {
            var bytes = new byte[] { 0xEF, 0xBB, 0xBF }.Concat("{\"add\":{\"path\":\"a\"}}\n"u8.ToArray()).ToArray();
            using var expected = new StreamReader(new ShortReadStream(bytes, chunkSize));
            using var actual = new BoundedLineReader(new ShortReadStream(bytes, chunkSize), null, int.MaxValue, false);

            Assert.True(await actual.ReadLineAsync());
            Assert.Equal(expected.ReadLine(), Encoding.UTF8.GetString(actual.Line.Span));
            Assert.False(await actual.ReadLineAsync());
        }

        [Theory]
        [InlineData(1)]
        [InlineData(2)]
        [InlineData(3)]
        public async Task ExistingReaderReplaysBomCommitOnShortStorageReads(int chunkSize)
        {
            using IFileStorage storage = new ShortReadStorage(Files.Of.InternalMemory("./bom-" + Guid.NewGuid().ToString("N")), chunkSize);
            await DeltaTestKit.WriteCommit(storage, "bom", 0, DeltaTestKit.Protocol(), DeltaTestKit.Metadata(DeltaTestKit.UserSchema));
            var path = DeltaTestKit.CommitPath("bom", 0);
            var json = await storage.ReadText(path);
            Assert.NotNull(json);
            await storage.WriteText(path, "\uFEFF" + json);

            var table = await DeltaTransactionReader.ReadTable(storage, "bom");

            Assert.NotNull(table);
            Assert.Equal(0, table.Version);
            Assert.Equal("table-id", table.Metadata.Id);
        }
    }
}
