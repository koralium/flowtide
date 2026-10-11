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

using FlowtideDotNet.Connector.DeltaLake.Internal;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Actions;
using Stowage;
using System.Text;
using static FlowtideDotNet.Connector.DeltaLake.Tests.DeltaTestKit;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public class CommitBytesTests
    {
        private static readonly byte[] Data = Encoding.UTF8.GetBytes("{\"commitInfo\":{\"a\":1}}\n{\"add\":{\"path\":\"x\"}}\n{\"remove\":{\"path\":\"y\"}}\n");

        private static CommitBytes Of(byte[] data, int chunkSize)
        {
            var bytes = new CommitBytes(chunkSize);
            // Uneven appends cross the chunk boundaries at different points
            for (int i = 0; i < data.Length; i += 5)
            {
                bytes.Append(data.AsSpan(i, Math.Min(5, data.Length - i)));
            }
            return bytes;
        }

        [Theory]
        [InlineData(1)]
        [InlineData(7)]
        [InlineData(1024)]
        public void ChunksHoldTheBytesInOrder(int chunkSize)
        {
            var bytes = Of(Data, chunkSize);

            Assert.Equal(Data.Length, bytes.Length);
            Assert.Equal(Data, bytes.ToArray());
        }

        [Theory]
        [InlineData(1)]
        [InlineData(7)]
        [InlineData(1024)]
        public void PrefixesAndContentCompareAcrossChunks(int chunkSize)
        {
            var bytes = Of(Data, chunkSize);
            var otherChunks = Of(Data, 3);
            var changed = Data.ToArray();
            changed[^3] = (byte)'z';

            Assert.True(bytes.StartsWith(Data.AsSpan(0, 30)));
            Assert.True(bytes.StartsWith(Data));
            Assert.False(bytes.StartsWith(changed));
            Assert.False(bytes.StartsWith(Data.Append((byte)'x').ToArray()));
            // The unused tail of a chunk is zero, an overlong prefix ending in zero must still fail
            Assert.False(bytes.StartsWith(Data.Append((byte)0).ToArray()));

            Assert.True(bytes.ContentEquals(otherChunks));
            Assert.False(bytes.ContentEquals(Of(changed, 3)));
            Assert.False(bytes.ContentEquals(Of(Data.AsSpan(0, 40).ToArray(), 3)));
            Assert.True(bytes.StartsWith(Of(Data.AsSpan(0, 40).ToArray(), 3)));
            Assert.False(bytes.StartsWith(Of(changed, 3)));
            Assert.False(Of(Data.AsSpan(0, 40).ToArray(), 3).StartsWith(bytes));
        }

        [Theory]
        [InlineData(1)]
        [InlineData(7)]
        [InlineData(1024)]
        public void TheFirstLineIsReadAcrossChunks(int chunkSize)
        {
            var bytes = Of(Data, chunkSize);

            Assert.Equal(Encoding.UTF8.GetBytes("{\"commitInfo\":{\"a\":1}}"), bytes.FirstLine());
            Assert.Null(Of(Encoding.UTF8.GetBytes("no newline"), chunkSize).FirstLine());
        }

        [Fact]
        public void ASmallCommitAllocatesLittle()
        {
            var small = new CommitBytes();
            small.Append(new byte[1024]);
            Assert.Equal(4096, small.Capacity);

            // The first chunk doubles until it is full, later chunks are full size
            var growing = new CommitBytes(chunkSize: 64, initialSize: 4);
            var data = Enumerable.Range(0, 150).Select(x => (byte)x).ToArray();
            growing.Append(data.AsSpan(0, 20));
            Assert.Equal(32, growing.Capacity);
            growing.Append(data.AsSpan(20));
            Assert.Equal(64 * 3, growing.Capacity);
            Assert.Equal(data, growing.ToArray());
        }

        [Theory]
        [InlineData(1, 0)]
        [InlineData(7, 13)]
        [InlineData(7, 14)]
        [InlineData(1024, 13)]
        public async Task WritingFlushesExactlyAfterThePrefix(int chunkSize, int flushAfter)
        {
            var bytes = Of(Data, chunkSize);
            var events = new List<string>();
            using var target = new RecordingStream(events);

            await bytes.WriteTo(target, flushAfter);

            Assert.Equal(Data, target.Written.ToArray());
            var flushes = events.Select((x, i) => (x, i)).Where(x => x.x == "F").ToList();
            if (flushAfter == 0)
            {
                Assert.Empty(flushes);
            }
            else
            {
                var single = Assert.Single(flushes);
                Assert.Equal(flushAfter, events.Take(single.i).Sum(x => int.Parse(x.Substring(1))));
            }
        }

        [Theory]
        [InlineData(1)]
        [InlineData(7)]
        public async Task ReadingFillsChunksFromStorage(int chunkSize)
        {
            var storage = Files.Of.InternalMemory($"./{nameof(ReadingFillsChunksFromStorage)}_{chunkSize}");
            using (var write = await storage.OpenWrite("/commit.json"))
            {
                await write.WriteAsync(Data);
            }

            var bytes = await CommitBytes.Read(storage, "/commit.json", chunkSize);

            Assert.Equal(Data, bytes!.ToArray());
            Assert.Null(await CommitBytes.Read(storage, "/missing.json", chunkSize));
        }

        [Fact]
        public async Task ACommitOfSeveralChunksIsStagedPublishedAndRepaired()
        {
            var storage = new HookFileStorage(Files.Of.InternalMemory($"./{nameof(ACommitOfSeveralChunksIsStagedPublishedAndRepaired)}"));
            var stageId = Guid.NewGuid().ToString("N");
            var actions = new List<DeltaAction>()
            {
                new DeltaAction() { CommitInfo = new DeltaCommitInfoAction() { StageId = stageId, Data = new Dictionary<string, object>() { ["operation"] = "WRITE" } } },
                Protocol(),
                Metadata(UserSchema)
            };
            // About 3 MB, so the commit spans several chunks
            for (int i = 0; i < 20_000; i++)
            {
                actions.Add(new DeltaAction() { Add = new DeltaAddAction() { Path = $"part-{i:D8}-{Guid.NewGuid()}.parquet", Size = i, DataChange = true, PartitionValues = new Dictionary<string, string>(), Statistics = "{\"numRecords\":1,\"minValues\":{\"userkey\":1},\"maxValues\":{\"userkey\":2},\"nullCount\":{\"userkey\":0}}" } });
            }
            var staged = await DeltaTransactionWriter.StageCommit(storage, "t", 0, actions);
            Assert.True(staged.Length > 2L * CommitBytes.DefaultChunkSize, $"{staged.Length} bytes");
            var pending = new DeltaLakePendingCommit() { Version = 0, StagedFile = staged.FileName, StageId = staged.StageId, Length = staged.Length, CheckpointId = 1 };
            var expected = staged.Bytes.ToArray();

            // Torn in the second chunk, repaired from the staged file a restart reads back
            using (var torn = await storage.Inner.OpenWrite("/t/_delta_log/00000000000000000000.json"))
            {
                await torn.WriteAsync(expected.AsMemory(0, CommitBytes.DefaultChunkSize + 1000));
            }
            var repaired = await DeltaTransactionWriter.PublishCommit(storage, "t", pending, null, checkSuccessor: true);
            Assert.Equal(PublishOutcome.Repaired, repaired.Outcome);
            Assert.Equal(expected, await ReadBytes(storage, "/t/_delta_log/00000000000000000000.json"));

            // The full copy is recognized as this sink's commit
            await DeltaTransactionWriter.StageCommit(storage, "t", 0, actions);
            var again = await DeltaTransactionWriter.PublishCommit(storage, "t", pending, staged.Bytes, checkSuccessor: true);
            Assert.Equal(PublishOutcome.AlreadyPublished, again.Outcome);
            Assert.Equal(actions.Count, (await DeltaTransactionReader.ReadVersionCommit(storage, "t", 0))!.AddedFiles.Count + 3);
        }

        private sealed class RecordingStream : Stream
        {
            private readonly List<string> _events;

            public RecordingStream(List<string> events)
            {
                _events = events;
            }

            public MemoryStream Written { get; } = new MemoryStream();

            public override bool CanRead => false;
            public override bool CanSeek => false;
            public override bool CanWrite => true;
            public override long Length => Written.Length;
            public override long Position { get => Written.Position; set => throw new NotSupportedException(); }

            public override void Flush()
            {
                _events.Add("F");
            }

            public override Task FlushAsync(CancellationToken cancellationToken)
            {
                _events.Add("F");
                return Task.CompletedTask;
            }

            public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();

            public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

            public override void SetLength(long value) => throw new NotSupportedException();

            public override void Write(byte[] buffer, int offset, int count)
            {
                _events.Add($"W{count}");
                Written.Write(buffer, offset, count);
            }

            public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
            {
                _events.Add($"W{buffer.Length}");
                Written.Write(buffer.Span);
                return ValueTask.CompletedTask;
            }
        }
    }
}
