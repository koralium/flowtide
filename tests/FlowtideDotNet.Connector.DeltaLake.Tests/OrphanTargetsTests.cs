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
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.Deletion;
using Stowage;
using System.Text;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public class OrphanTargetsTests : IDisposable
    {
        private static readonly DateTimeOffset Old = new DateTimeOffset(2026, 9, 1, 6, 30, 15, TimeSpan.Zero);

        private readonly string _directory = Path.Combine(Path.GetTempPath(), "ot-" + ScratchOwners.ShortId());
        private readonly ScratchOwners _owners = new ScratchOwners();
        private readonly List<IDisposable> _disposables = new List<IDisposable>();

        public OrphanTargetsTests()
        {
            Directory.CreateDirectory(_directory);
        }

        public void Dispose()
        {
            foreach (var disposable in _disposables)
            {
                disposable.Dispose();
            }
            _owners.Release();
            try
            {
                Directory.Delete(_directory, true);
            }
            catch (IOException)
            {
            }
            catch (UnauthorizedAccessException)
            {
            }
        }

        private ScratchDirectory Owner() => _owners.Add(new ScratchDirectory(Path.Combine(_directory, ScratchOwners.ShortId())));

        // A frozen payload of the size, at the old time
        private static byte[] Payload(long size)
        {
            var payload = new byte[OrphanCandidatePayload.MaxLength];
            var written = OrphanCandidatePayload.Encode(payload, size, Old.AddSeconds(size), null);
            return payload[..written];
        }

        private static OrphanTargets Written(JobScratch scratch, JobMemory memory, params string[] paths)
        {
            using var writer = new OrphanTargetsWriter(scratch, memory);
            for (int i = 0; i < paths.Length; i++)
            {
                writer.Add(paths[i], Payload(i + 1));
            }
            return writer.Finish();
        }

        private static async Task<List<DeletionTarget>> ReadAsync(IDeletionSource source, long position, int? take = null)
        {
            var targets = new List<DeletionTarget>();
            await foreach (var target in source.ReadFrom(position, CancellationToken.None))
            {
                targets.Add(target);
                if (targets.Count == take)
                {
                    break;
                }
            }
            return targets;
        }

        [Fact]
        public async Task FinishedTargetsLeaveTheJobAndReadBackAsTargets()
        {
            var root = Path.Combine(_directory, ScratchOwners.ShortId());
            Directory.CreateDirectory(Path.Combine(root, "t"));
            var raw = Files.Of.LocalDisk(root);
            _disposables.Add(raw);
            var configuration = CleanupConfiguration.Validate(raw, "t", false, new CleanupOptIns(OrphanSweepTests.Retention, OrphanSweepTests.Interval));
            var prefix = configuration.Store.Table.Full + "/";
            var owner = Owner();
            var scratch = owner.CreateJob();
            _disposables.Add(scratch);
            var memory = new JobMemory();
            var paths = new[] { prefix + "a.parquet", prefix + "sub/b.parquet", prefix + "c.parquet" };

            string written;
            OrphanTargets targets;
            using (var writer = new OrphanTargetsWriter(scratch, memory))
            {
                written = writer.Path;
                for (int i = 0; i < paths.Length; i++)
                {
                    writer.Add(paths[i], Payload(i + 1));
                }
                Assert.Contains(written, scratch.Files);
                targets = writer.Finish();
                Assert.Equal(3, writer.Count);
            }
            _disposables.Add(targets);
            Assert.Equal(3, targets.Count);
            Assert.Equal(0, memory.Managed);
            Assert.Empty(scratch.Files);
            Assert.False(File.Exists(written));
            Assert.True(File.Exists(targets.Path));
            Assert.Equal(Path.Combine(owner.ProcessPath, "runs"), Path.GetDirectoryName(targets.Path));
            Assert.Equal(paths.Select((x, i) => (x, (long?)(i + 1), (DateTimeOffset?)Old.AddSeconds(i + 1))), targets.ReadAllForTests());

            // The job's folder going does not take the file
            scratch.Dispose();
            Assert.True(File.Exists(targets.Path));

            var executor = new DeletionExecutor(configuration, raw);
            var sliceMemory = new JobMemory();
            var source = targets.Source(executor, () => sliceMemory);
            var expected = paths.Select((x, i) => executor.TargetOf(x) with { Size = i + 1, Modified = Old.AddSeconds(i + 1) }).ToList();
            Assert.All(expected, x => Assert.Equal(KeepReason.None, executor.Check(x)));
            Assert.Equal(Path.GetFullPath(Path.Combine(root, "t", "sub", "b.parquet")), expected[1].Key);
            Assert.Equal(expected, await ReadAsync(source, 0));
            Assert.Equal(expected.Skip(1), await ReadAsync(source, 1));
            Assert.Empty(await ReadAsync(source, 3));
            Assert.Equal(expected.Take(1), await ReadAsync(source, 0, take: 1));
            Assert.Equal(0, sliceMemory.Managed);
        }

        [Fact]
        public async Task LaterReadsStartWhereTheLatestReadStarted()
        {
            var root = Path.Combine(_directory, ScratchOwners.ShortId());
            Directory.CreateDirectory(Path.Combine(root, "t"));
            var raw = Files.Of.LocalDisk(root);
            _disposables.Add(raw);
            var configuration = CleanupConfiguration.Validate(raw, "t", false, new CleanupOptIns(OrphanSweepTests.Retention, OrphanSweepTests.Interval));
            var scratch = Owner().CreateJob();
            _disposables.Add(scratch);
            var paths = new[] { "a.parquet", "b.parquet", "c.parquet", "d.parquet" }.Select(x => configuration.Store.Table.Full + "/" + x).ToArray();
            var targets = Written(scratch, new JobMemory(), paths);
            _disposables.Add(targets);
            var executor = new DeletionExecutor(configuration, raw);
            var memory = new JobMemory();
            var source = targets.Source(executor, () => memory);
            var keys = paths.Select(x => executor.TargetOf(x).Key).ToList();
            long BytesFrom(int position) => paths.Skip(position).Sum(x => (long)Encoding.UTF8.GetByteCount(x));
            Assert.Equal(BytesFrom(0), targets.PathBytes);
            Assert.Equal(BytesFrom(0), targets.PathBytesLeft);
            Assert.Equal(keys.Skip(2), (await ReadAsync(source, 2)).Select(x => x.Key));
            Assert.Equal(BytesFrom(2), targets.PathBytesLeft);

            // Bytes no entry can start with, before that read's start
            await using (var file = new FileStream(targets.Path, FileMode.Open, FileAccess.Write, FileShare.ReadWrite | FileShare.Delete))
            {
                await file.WriteAsync(Enumerable.Repeat((byte)0xFF, 8).ToArray());
            }

            Assert.Equal(keys.Skip(2), (await ReadAsync(source, 2)).Select(x => x.Key));
            Assert.Equal(keys.Skip(3), (await ReadAsync(source, 3)).Select(x => x.Key));
            Assert.Equal(BytesFrom(3), targets.PathBytesLeft);
            Assert.Empty(await ReadAsync(source, 4));
            Assert.Equal(0, targets.PathBytesLeft);
            Assert.Equal(0, memory.Managed);
        }

        [Fact]
        public async Task TargetsCarryTheirFrozenMd5AndCountItsBufferCharge()
        {
            var root = Path.Combine(_directory, ScratchOwners.ShortId());
            Directory.CreateDirectory(Path.Combine(root, "t"));
            var raw = Files.Of.LocalDisk(root);
            _disposables.Add(raw);
            var configuration = CleanupConfiguration.Validate(raw, "t", false, new CleanupOptIns(OrphanSweepTests.Retention, OrphanSweepTests.Interval));
            var scratch = Owner().CreateJob();
            _disposables.Add(scratch);
            // Listed base64 MD5s, none on the second
            var md5s = new[] { "AQIDBAUGBwgJCgsMDQ4PEA==", null, "ERITFBUWFxgZGhscHR4fIA==", "ISIjJCUmJygpKissLS4vMA==" };
            var paths = md5s.Select((_, i) => configuration.Store.Table.Full + $"/{i}.parquet").ToArray();
            OrphanTargets targets;
            using (var writer = new OrphanTargetsWriter(scratch, new JobMemory()))
            {
                var payload = new byte[OrphanCandidatePayload.MaxLength];
                for (int i = 0; i < paths.Length; i++)
                {
                    writer.Add(paths[i], payload.AsSpan(0, OrphanCandidatePayload.Encode(payload, i + 1, Old, md5s[i])));
                }
                targets = writer.Finish();
            }
            _disposables.Add(targets);
            var memory = new JobMemory();
            var source = targets.Source(new DeletionExecutor(configuration, raw), () => memory);
            long ChargedFrom(int position) => md5s.Skip(position).Sum(x => x == null ? 0 : DeletionRun.Md5OverheadBytes + 2L * x.Length);
            Assert.Equal(ChargedFrom(0), targets.Md5Bytes);
            Assert.Equal(ChargedFrom(0), targets.Md5BytesLeft);

            Assert.Equal(md5s.Skip(1), (await ReadAsync(source, 1)).Select(x => x.Md5));
            Assert.Equal(ChargedFrom(1), targets.Md5BytesLeft);
            Assert.Equal(md5s.Skip(3), (await ReadAsync(source, 3)).Select(x => x.Md5));
            Assert.Equal(ChargedFrom(3), targets.Md5BytesLeft);
            Assert.Equal(0, memory.Managed);
        }

        [Fact]
        public void FinishedTargetsHoldTheirChargeUntilDisposedOnce()
        {
            var owner = Owner();
            var scratch = owner.CreateJob();
            _disposables.Add(scratch);
            var memory = new JobMemory();
            var first = Written(scratch, memory, "/t/a.parquet");
            var second = Written(scratch, memory, "/t/b.parquet", "/t/c.parquet");
            var firstBytes = new FileInfo(first.Path).Length;
            var secondBytes = new FileInfo(second.Path).Length;
            // A second release of the first would leave a positive total below the second's
            Assert.True(secondBytes > firstBytes && firstBytes > 0);
            Assert.Equal(firstBytes + secondBytes, owner.Ledger.Reserved);

            scratch.Dispose();
            Assert.Equal(firstBytes + secondBytes, owner.Ledger.Reserved);

            first.Dispose();
            Assert.False(File.Exists(first.Path));
            Assert.Equal(secondBytes, owner.Ledger.Reserved);
            first.Dispose();
            Assert.Equal(secondBytes, owner.Ledger.Reserved);
            Assert.Equal(0, owner.PendingCount);

            second.Dispose();
            Assert.False(File.Exists(second.Path));
            Assert.Equal(0, owner.Ledger.Reserved);
        }

        [Fact]
        public void FailedMoveKeepsTheFileInTheJob()
        {
            var owner = Owner();
            var scratch = owner.CreateJob();
            _disposables.Add(scratch);
            var memory = new JobMemory();
            // A file where the runs folder goes
            File.WriteAllText(Path.Combine(owner.ProcessPath, "runs"), "blocked");

            var writer = new OrphanTargetsWriter(scratch, memory);
            writer.Add("/t/a.parquet", Payload(1));
            Assert.ThrowsAny<IOException>(() => writer.Finish());
            Assert.Contains(writer.Path, scratch.Files);
            Assert.True(File.Exists(writer.Path));
            Assert.Equal(new FileInfo(writer.Path).Length, owner.Ledger.Reserved);

            writer.Dispose();
            Assert.Empty(scratch.Files);
            Assert.False(File.Exists(writer.Path));
            Assert.Equal(0, owner.Ledger.Reserved);
            Assert.Equal(0, memory.Managed);
        }
    }
}
