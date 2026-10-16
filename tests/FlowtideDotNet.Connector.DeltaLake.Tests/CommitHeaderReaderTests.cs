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
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Checkpoints;
using Stowage;
using System.Text;
using System.Text.Json;
using static FlowtideDotNet.Connector.DeltaLake.Tests.DeltaTestKit;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public class CommitHeaderReaderTests
    {
        private static readonly string StageId = new string('a', 32);

        // An own first line as the writer serializes it
        private static byte[] OwnLine(IReadOnlyList<string> created, long adoptedAt = 3, long timestamp = 1_700_000_000_000)
        {
            var action = new DeltaAction()
            {
                CommitInfo = new DeltaCommitInfoAction()
                {
                    StageId = StageId,
                    AdoptedAt = adoptedAt,
                    Timestamp = timestamp,
                    Operation = "WRITE",
                    CreatedFiles = created.ToList()
                }
            };
            return DeltaTransactionWriter.Serialize(new List<DeltaAction>() { action }).ToArray();
        }

        private static CommitHeader Scan(byte[] line, HeaderMode mode, int chunk, List<string>? paths = null, bool terminated = true)
        {
            var scanner = new CommitHeaderScanner(mode, paths != null ? paths.Add : null);
            var end = Array.IndexOf(line, (byte)'\n');
            var body = end < 0 ? line : line.AsSpan(0, end).ToArray();
            for (int i = 0; i < body.Length; i += chunk)
            {
                scanner.Feed(body.AsSpan(i, Math.Min(chunk, body.Length - i)));
            }
            return scanner.Complete(terminated);
        }

        [Fact]
        public void OwnershipExtractionHandlesEscapesAndSplitUtf8()
        {
            var created = new[] { "part-1.parquet", "dir/a\"b.parquet", "back\\slash.parquet", "ünïcode/é.parquet", "emoji-😀.parquet", "tab\tname", "deletion_vector_x.bin" };
            var serialized = OwnLine(created);
            // The same paths written with escapes a foreign writer might use
            var escaped = Encoding.UTF8.GetBytes("{\"commitInfo\":{\"flowtide.stageId\":\"" + StageId + "\",\"flowtide.createdFiles\":[\"\\u00fcber\\/x\",\"\\ud83d\\ude00.bin\",\"a\\u0041\"]}}\n");

            for (int chunk = 1; chunk <= 17; chunk++)
            {
                foreach (var mode in new[] { HeaderMode.Lenient, HeaderMode.Strict })
                {
                    var paths = new List<string>();
                    var header = Scan(serialized, mode, chunk, paths);
                    Assert.True(header.IsValid, header.Error);
                    Assert.True(header.IsCommitInfo);
                    Assert.Equal(created, paths);
                    Assert.Equal(created.Length, header.CreatedFileCount);
                    Assert.Equal(StageId, header.StageId);
                    Assert.Equal(3, header.AdoptedAt);
                    Assert.Equal(1_700_000_000_000, header.Timestamp);
                    Assert.Equal("WRITE", header.Operation);
                    Assert.Equal(HeaderFlags.None, header.Flags);

                    var escapedPaths = new List<string>();
                    var escapedHeader = Scan(escaped, mode, chunk, escapedPaths);
                    Assert.True(escapedHeader.IsValid, escapedHeader.Error);
                    Assert.Equal(new[] { "über/x", "😀.bin", "aA" }, escapedPaths);
                }
            }
        }

        [Theory]
        // Torn before the end
        [InlineData("{\"commitInfo\":{\"flowtide.stageId\":\"abc\"", true)]
        // A trailing comma
        [InlineData("{\"commitInfo\":{\"flowtide.stageId\":\"abc\",}}", true)]
        // An invalid escape
        [InlineData("{\"commitInfo\":{\"flowtide.stageId\":\"a\\qc\"}}", true)]
        // A lone surrogate escape
        [InlineData("{\"commitInfo\":{\"flowtide.createdFiles\":[\"\\ud800x\"]}}", true)]
        // A control character inside a string
        [InlineData("{\"commitInfo\":{\"flowtide.stageId\":\"a\u0001c\"}}", true)]
        // Content after the object
        [InlineData("{\"commitInfo\":{}} x", true)]
        // Not a number
        [InlineData("{\"commitInfo\":{\"timestamp\":01}}", true)]
        // Another action beside commitInfo
        [InlineData("{\"commitInfo\":{},\"add\":{}}", false)]
        public void StrictProofRejectsTornInvalidAndDuplicateFields(string line, bool invalid)
        {
            var header = Scan(Encoding.UTF8.GetBytes(line), HeaderMode.Strict, 3);
            Assert.False(header.IsCommitInfo);
            Assert.Equal(invalid, !header.IsValid);
        }

        [Fact]
        public void StrictProofRejectsUnterminatedInvalidUtf8DepthAndDuplicates()
        {
            var valid = OwnLine(new[] { "a.parquet" });
            Assert.True(Scan(valid, HeaderMode.Strict, 5).IsCommitInfo);
            // Without its terminator a strict line is torn, a lenient one is the last line of a file
            Assert.False(Scan(valid, HeaderMode.Strict, 5, terminated: false).IsValid);
            Assert.True(Scan(valid, HeaderMode.Lenient, 5, terminated: false).IsCommitInfo);

            var badUtf8 = new List<byte>(Encoding.UTF8.GetBytes("{\"commitInfo\":{\"flowtide.createdFiles\":[\"a"));
            badUtf8.AddRange(new byte[] { 0xC3, 0x28 });
            badUtf8.AddRange(Encoding.UTF8.GetBytes("\"]}}"));
            Assert.False(Scan(badUtf8.ToArray(), HeaderMode.Strict, 1).IsValid);
            // Lenient reads decode with replacement characters, as before
            Assert.True(Scan(badUtf8.ToArray(), HeaderMode.Lenient, 1).IsCommitInfo);

            var deep = "{\"commitInfo\":{\"x\":" + new string('[', 70) + new string(']', 70) + "}}";
            Assert.False(Scan(Encoding.UTF8.GetBytes(deep), HeaderMode.Strict, 7).IsValid);

            var duplicate = Scan(Encoding.UTF8.GetBytes("{\"commitInfo\":{\"flowtide.stageId\":\"a\",\"flowtide.stageId\":\"b\"}}"), HeaderMode.Strict, 4);
            Assert.True(duplicate.IsCommitInfo);
            Assert.True(duplicate.Flags.HasFlag(HeaderFlags.DuplicateField));
            // The last value wins, as JSON deserialization does
            Assert.Equal("b", duplicate.StageId);

            var bom = new byte[] { 0xEF, 0xBB, 0xBF }.Concat(valid).ToArray();
            Assert.False(Scan(bom, HeaderMode.Strict, 2).IsValid);
            Assert.True(Scan(bom, HeaderMode.Lenient, 2).IsCommitInfo);

            var fraction = Scan(Encoding.UTF8.GetBytes("{\"commitInfo\":{\"flowtide.adoptedAt\":1.5,\"timestamp\":\"x\"}}"), HeaderMode.Strict, 3);
            Assert.True(fraction.IsCommitInfo);
            Assert.Null(fraction.AdoptedAt);
            Assert.Null(fraction.Timestamp);
            Assert.True(fraction.Flags.HasFlag(HeaderFlags.InvalidAdoptedAt) && fraction.Flags.HasFlag(HeaderFlags.InvalidTimestamp));

            var badPath = Scan(Encoding.UTF8.GetBytes("{\"commitInfo\":{\"flowtide.createdFiles\":[\"a\",1,[\"b\"]]}}"), HeaderMode.Strict, 3);
            Assert.True(badPath.Flags.HasFlag(HeaderFlags.InvalidCreatedFiles));
        }

        [Fact]
        public void RestoredPublicationValidatesWholeLineOne()
        {
            var valid = OwnLine(new[] { "a.parquet" });
            Assert.True(DeltaTransactionWriter.FirstLineHasStageId(Bytes(valid), StageId));
            Assert.False(DeltaTransactionWriter.FirstLineHasStageId(Bytes(valid), new string('b', 32)));

            // The stage id matches but the line is damaged after it
            var damaged = (byte[])valid.Clone();
            var at = Array.IndexOf(damaged, (byte)'t', 60);
            damaged[at] = (byte)'"';
            Assert.False(DeltaTransactionWriter.FirstLineHasStageId(Bytes(damaged), StageId));

            // Torn before its terminator
            Assert.False(DeltaTransactionWriter.FirstLineHasStageId(Bytes(valid.AsSpan(0, valid.Length - 1).ToArray()), StageId));

            var duplicate = Encoding.UTF8.GetBytes("{\"commitInfo\":{\"flowtide.stageId\":\"" + StageId + "\",\"flowtide.stageId\":\"" + StageId + "\"}}\n");
            Assert.False(DeltaTransactionWriter.FirstLineHasStageId(Bytes(duplicate), StageId));

            // A line spanning chunks is read in place
            var many = OwnLine(Enumerable.Range(0, 5_000).Select(i => $"part-{i:D6}.parquet").ToList());
            var chunked = new CommitBytes(chunkSize: 4096);
            chunked.Append(many);
            Assert.True(DeltaTransactionWriter.FirstLineHasStageId(chunked, StageId));
        }

        [Fact]
        public async Task RestoredPublicationOfADamagedTargetFailsClosed()
        {
            var storage = new HookFileStorage(Files.Of.InternalMemory($"./{nameof(RestoredPublicationOfADamagedTargetFailsClosed)}"));
            var actions = new List<DeltaAction>()
            {
                new DeltaAction() { CommitInfo = new DeltaCommitInfoAction() { StageId = StageId, AdoptedAt = 0, Timestamp = 1, Operation = "WRITE", CreatedFiles = new List<string>() { "a.parquet" } } },
                Protocol(),
                Metadata(UserSchema)
            };
            var staged = await DeltaTransactionWriter.StageCommit(storage, "t", 0, actions);
            var bytes = staged.Bytes.ToArray();
            // Same length and prefix, damaged later in the first line; the stage is gone after a restart
            var at = Array.IndexOf(bytes, (byte)'W');
            bytes[at] = (byte)'"';
            using (var target = await storage.Inner.OpenWrite("/t/_delta_log/00000000000000000000.json"))
            {
                await target.WriteAsync(bytes);
            }
            await storage.Rm($"/t/_delta_log/{staged.FileName}");
            var pending = new DeltaLakePendingCommit() { Version = 0, StagedFile = staged.FileName, StageId = staged.StageId, Length = staged.Length, CheckpointId = 1 };

            var result = await DeltaTransactionWriter.PublishCommit(storage, "t", pending, null, checkSuccessor: false);

            Assert.Equal(PublishOutcome.FailedClosed, result.Outcome);
        }

        [Fact]
        public async Task ForeignCommitInfoUsesExistingBehavior()
        {
            var storage = Files.Of.InternalMemory($"./{nameof(ForeignCommitInfoUsesExistingBehavior)}");
            var foreign = new DeltaAction()
            {
                CommitInfo = new DeltaCommitInfoAction()
                {
                    Timestamp = 5,
                    Operation = "MERGE",
                    Data = new Dictionary<string, object>() { ["engineInfo"] = "Apache-Spark", ["operationParameters"] = new Dictionary<string, object>() { ["predicate"] = "x" } }
                }
            };
            await WriteCommit(storage, "t", 0, foreign, Protocol(), Metadata(UserSchema), Add("a.parquet"));
            // Over the streaming threshold, without the own prefix: read whole as before
            var big = new DeltaAction() { CommitInfo = new DeltaCommitInfoAction() { Data = new Dictionary<string, object>() { ["x"] = new string('x', 200_000) } } };
            await WriteCommit(storage, "t", 1, big, Add("b.parquet"));

            var first = await DeltaTransactionReader.ReadVersionCommit(storage, "t", 0);
            Assert.Equal("MERGE", first!.CommitInfo!.Operation);
            Assert.Equal(5, first.CommitInfo.Timestamp);
            Assert.Equal("Apache-Spark", ((JsonElement)first.CommitInfo.Data!["engineInfo"]).GetString());

            var second = await DeltaTransactionReader.ReadVersionCommit(storage, "t", 1);
            Assert.Equal(200_000, ((JsonElement)second!.CommitInfo!.Data!["x"]).GetString()!.Length);

            var table = await DeltaTransactionReader.ReadTable(storage, "t");
            Assert.Equal(new[] { "a.parquet", "b.parquet" }, table!.AddFiles.Select(x => x.Path).Order());
            // A foreign head carries no provenance
            Assert.False(CommitProvenance.From(1, table.HeadHeader).IsOwn);
        }

        [Fact]
        public async Task SnapshotAndCdfStreamOwnHeaderOver4MiB()
        {
            var storage = Files.Of.InternalMemory($"./{nameof(SnapshotAndCdfStreamOwnHeaderOver4MiB)}");
            var created = Enumerable.Range(0, 70_000).Select(i => $"part-00000-{Guid.NewGuid()}.zstd.parquet").ToList();
            var header = new DeltaAction() { CommitInfo = new DeltaCommitInfoAction() { StageId = StageId, AdoptedAt = 0, Timestamp = 9, Operation = "CREATE TABLE", CreatedFiles = created } };
            await WriteCommit(storage, "t", 0, header, Protocol(), Metadata(UserSchema), Add("a.parquet"));
            var line = Encoding.UTF8.GetString(await ReadBytes(storage, CommitPath("t", 0))).Split('\n')[0];
            Assert.True(line.Length > 4 << 20, $"{line.Length} bytes");

            var table = await DeltaTransactionReader.ReadTable(storage, "t");
            Assert.Equal("a.parquet", Assert.Single(table!.AddFiles).Path);
            Assert.Equal(70_000, table.HeadHeader!.CreatedFileCount);
            Assert.True(CommitProvenance.From(0, table.HeadHeader).IsOwn);

            var commit = await DeltaTransactionReader.ReadVersionCommit(storage, "t", 0);
            Assert.Equal(StageId, commit!.CommitInfo!.StageId);
            Assert.Equal(0, commit.CommitInfo.AdoptedAt);
            Assert.Single(commit.AddedFiles);

            // The line reader never holds the header
            using var lines = new BoundedLineReader(new MemoryStream(await ReadBytes(storage, CommitPath("t", 0))), null, int.MaxValue, skipOversizedCommitInfo: false, streamOwnHeader: true);
            Assert.True(await lines.ReadLineAsync());
            Assert.NotNull(lines.StreamedHeader);
            Assert.True(lines.LineCapacity <= BoundedLineReader.OwnHeaderThreshold, $"{lines.LineCapacity}");
            Assert.True(await lines.ReadLineAsync());
            Assert.Null(lines.StreamedHeader);
            Assert.StartsWith("{\"protocol\"", Encoding.UTF8.GetString(lines.Line.Span));
        }

        [Fact]
        public async Task OwnHeaderAdditionalAllocationIsBounded()
        {
            var small = Commit(OwnLine(Paths(14_000)));
            var large = Commit(OwnLine(Paths(140_000)));
            Assert.True(large.Length > 8 << 20);

            // Warm up, so one-time costs fall outside the measurement
            await ReadAll(small);
            var smallBytes = await Measure(small);
            var largeBytes = await Measure(large);

            Assert.True(largeBytes - smallBytes <= 256 << 10, $"{smallBytes} bytes for 1 MiB, {largeBytes} bytes for 10 MiB");
        }

        [Fact]
        public async Task SnapshotAndCdfReadersAllocateNothingThatGrowsWithTheOwnHeader()
        {
            var storage = Files.Of.InternalMemory($"./{nameof(SnapshotAndCdfReadersAllocateNothingThatGrowsWithTheOwnHeader)}");
            foreach (var (table, count) in new[] { ("small", 14_000), ("large", 140_000) })
            {
                var header = new DeltaAction() { CommitInfo = new DeltaCommitInfoAction() { StageId = StageId, AdoptedAt = 0, Timestamp = 9, Operation = "CREATE TABLE", CreatedFiles = Paths(count) } };
                await WriteCommit(storage, table, 0, header, Protocol(), Metadata(UserSchema), Add("a.parquet"));
            }

            // Warm up, so one-time costs fall outside the measurement
            await DeltaTransactionReader.ReadVersionCommit(storage, "small", 0);
            await DeltaTransactionReader.ReadTable(storage, "small");
            var cdf = await AllocatedBy(() => DeltaTransactionReader.ReadVersionCommit(storage, "small", 0), () => DeltaTransactionReader.ReadVersionCommit(storage, "large", 0));
            var snapshot = await AllocatedBy(() => DeltaTransactionReader.ReadTable(storage, "small"), () => DeltaTransactionReader.ReadTable(storage, "large"));

            // A whole-line decode of the 10 MiB header alone would allocate about 20 MiB more
            Assert.True(cdf <= 1 << 20, $"CDF read allocated {cdf} more bytes for a ten times larger own header");
            Assert.True(snapshot <= 1 << 20, $"Snapshot read allocated {snapshot} more bytes for a ten times larger own header");
        }

        [Fact]
        public void RestoredPublicationCheckAllocatesNothingThatGrowsWithTheOwnHeader()
        {
            var small = new CommitBytes();
            small.Append(Commit(OwnLine(Paths(14_000))));
            var large = new CommitBytes();
            large.Append(Commit(OwnLine(Paths(140_000))));
            Assert.True(DeltaTransactionWriter.FirstLineHasStageId(small, StageId));

            var before = GC.GetAllocatedBytesForCurrentThread();
            Assert.True(DeltaTransactionWriter.FirstLineHasStageId(small, StageId));
            var middle = GC.GetAllocatedBytesForCurrentThread();
            Assert.True(DeltaTransactionWriter.FirstLineHasStageId(large, StageId));
            var after = GC.GetAllocatedBytesForCurrentThread();

            Assert.True((after - middle) - (middle - before) <= 256 << 10, $"{middle - before} bytes for 1 MiB, {after - middle} bytes for 10 MiB");
        }

        // The difference in bytes allocated on this thread; the in-memory store completes every call synchronously
        private static async Task<long> AllocatedBy<T>(Func<Task<T>> small, Func<Task<T>> large)
        {
            var before = GC.GetAllocatedBytesForCurrentThread();
            var smallTask = small();
            Assert.True(smallTask.IsCompleted);
            var middle = GC.GetAllocatedBytesForCurrentThread();
            var largeTask = large();
            Assert.True(largeTask.IsCompleted);
            var after = GC.GetAllocatedBytesForCurrentThread();
            await smallTask;
            await largeTask;
            return (after - middle) - (middle - before);
        }

        private static List<string> Paths(int count) => Enumerable.Range(0, count).Select(i => $"part-00000-{i:D36}.zstd.parquet").ToList();

        private static byte[] Commit(byte[] firstLine) => firstLine.Concat(Encoding.UTF8.GetBytes("{\"protocol\":{\"minReaderVersion\":1,\"minWriterVersion\":2}}\n")).ToArray();

        // Allocated on this thread while the commit's lines are read; a memory stream completes every read synchronously
        private static async Task<long> Measure(byte[] commit)
        {
            var before = GC.GetAllocatedBytesForCurrentThread();
            var task = ReadAll(commit);
            Assert.True(task.IsCompleted);
            var after = GC.GetAllocatedBytesForCurrentThread();
            await task;
            return after - before;
        }

        private static async Task ReadAll(byte[] commit)
        {
            using var lines = new BoundedLineReader(new MemoryStream(commit), null, int.MaxValue, skipOversizedCommitInfo: false, streamOwnHeader: true);
            while (await lines.ReadLineAsync())
            {
            }
        }

        private static CommitBytes Bytes(byte[] bytes)
        {
            var commit = new CommitBytes();
            commit.Append(bytes);
            return commit;
        }

        private static DeltaAction Add(string path) => new DeltaAction() { Add = new DeltaAddAction() { Path = path, Size = 1, DataChange = true, PartitionValues = new Dictionary<string, string>() } };
    }
}
