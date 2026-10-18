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

using Apache.Arrow.Types;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Checkpoints;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance;
using Stowage;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public partial class DeltaMaintenanceWorkerTests
    {
        private static int ReadsOf(HookFileStorage hooks, long version) => hooks.Requests.Count(x => x.StartsWith("OpenRead") && x.EndsWith($"{version:D20}.checkpoint.parquet"));

        // A root every build over it declines while it streams: parsed-only statistics or an unknown non-null column
        private static Task DecliningRoot(IFileStorage storage, string kind, long version)
        {
            if (kind == "parsed")
            {
                return ParsedOnlyRoot(storage, version);
            }
            var columns = new Dictionary<string, (string Name, IArrowType Type)[]>(DeltaLogReplayTests.CheckpointColumns) { ["rowIdHighWaterMark"] = new (string, IArrowType)[] { ("value", Int64Type.Default) } };
            return DeltaLogReplayTests.WriteClassicCheckpoint(storage, "t", version, columns,
                ("protocol", new object?[] { 1, 2 }), ("metaData", new object?[] { "table-id", DeltaTestKit.UserSchema, 0L }), ("rowIdHighWaterMark", new object?[] { 5L }));
        }

        private static MaintenanceReason DeclineOf(string kind) => kind == "parsed" ? MaintenanceReason.ParsedOnlyRoot : MaintenanceReason.UnknownColumn;

        [Fact]
        public async Task SecondBuildReadsNoRootRemotely()
        {
            var rig = Create(interval: 2, cached: true);
            await Commits(rig.Storage, 0, 2);
            rig.Worker.Published(2);
            await rig.TickAsync();
            Assert.Equal(new long[] { 2 }, await Checkpoints(rig.Storage));

            await Commits(rig.Storage, 3, 4);
            rig.Worker.Published(4);
            rig.Hooks!.ClearRequests();
            await rig.TickAsync();
            Assert.Equal(new long[] { 2, 4 }, await Checkpoints(rig.Storage));
            // Our own output of 2 stayed local, so the build of 4 read its root from the cache
            Assert.Equal(2, rig.Worker.LastBuild!.RootVersion);
            Assert.Equal(0, ReadsOf(rig.Hooks, 2));
        }

        [Fact]
        public async Task BuildRootDownloadIsCached()
        {
            var rig = Create(interval: 1, stamp: true, cached: true);
            await Commits(rig.Storage, 0, 2);
            rig.Worker.Published(2);
            await rig.TickAsync();

            // A foreign root at 3, then a commit 4 with a line over the action cap: the build downloads the root and declines on the tail
            await Commits(rig.Storage, 3, 3);
            await DeltaLakeCheckpointBuildTests.LegacyRoot(rig.Storage, "t", 3);
            await rig.Storage.WriteText($"/t/_delta_log/{4:D20}.json", "{\"add\":{\"path\":\"big.parquet\",\"size\":1,\"modificationTime\":0,\"dataChange\":true,\"stats\":\"" + new string('x', 5 << 20) + "\"}}\n");
            rig.Worker.Published(4);
            rig.Hooks!.ClearRequests();
            await rig.TickAsync();
            Assert.Equal(DeclineDependency.Tail, rig.Worker.Memo!.Dependency);
            Assert.Equal(1, ReadsOf(rig.Hooks, 3));

            await DeltaTestKit.WriteCommit(rig.Storage, "t", 4, DeltaLakeCheckpointBuildTests.Add("f4.parquet"));
            rig.PastRetry();
            rig.Hooks.ClearRequests();
            await rig.TickAsync();
            Assert.Contains(4L, await Checkpoints(rig.Storage));
            Assert.Equal(3, rig.Worker.LastBuild!.RootVersion);
            Assert.Equal(0, ReadsOf(rig.Hooks, 3));
        }

        [Fact]
        public async Task CacheLossChangesNoContent()
        {
            async Task<(byte[] Digest, int RootReads)> Build(bool cached, bool lose)
            {
                var rig = Create(interval: 2, cached: cached);
                await Commits(rig.Storage, 0, 2);
                rig.Worker.Published(2);
                await rig.TickAsync();
                if (lose)
                {
                    foreach (var file in Directory.GetFiles(rig.Budget.Scratch.Cache.DirectoryPath))
                    {
                        File.Delete(file);
                    }
                }
                await Commits(rig.Storage, 3, 4);
                rig.Worker.Published(4);
                rig.Hooks!.ClearRequests();
                await rig.TickAsync();
                Assert.Equal(new long[] { 2, 4 }, await Checkpoints(rig.Storage));
                return (rig.Worker.LastBuild!.Sha256!, ReadsOf(rig.Hooks, 2));
            }

            var uncached = await Build(cached: false, lose: false);
            var hit = await Build(cached: true, lose: false);
            var lost = await Build(cached: true, lose: true);
            Assert.Equal(32, uncached.Digest.Length);
            Assert.Equal(0, hit.RootReads);
            // A cache file gone from disk is a miss, read from the table again
            Assert.True(lost.RootReads > 0);
            Assert.Equal(uncached.Digest, hit.Digest);
            Assert.Equal(uncached.Digest, lost.Digest);
        }

        [Fact]
        public async Task DecodeFailureDropsTheCacheEntry()
        {
            var (rig, _) = await UndecodableRootRig(stamp: true, cached: true);
            // The undecodable root passed its scan, so inspection kept it, and the build that failed to decode it dropped it
            Assert.Equal(0, rig.Budget.Scratch.Cache.Count);
            Assert.Empty(Directory.GetFiles(rig.Budget.Scratch.Cache.DirectoryPath));
            Assert.Equal(0, rig.Budget.Scratch.Ledger.Reserved);
        }

        [Theory]
        [InlineData("parsed")]
        [InlineData("unknown")]
        public async Task CachedRootIsReadFreshAfterIdentityChange(string kind)
        {
            // Stamped listings carry a modification time, so identities are strong
            var rig = Create(interval: 2, stamp: true, cached: true);
            await Commits(rig.Storage, 0, 5);
            await DecliningRoot(rig.Storage, kind, 3);
            rig.Worker.Published(5);
            await rig.TickAsync();
            Assert.Equal(DeclineOf(kind), rig.Worker.Memo!.Reason);
            Assert.Equal(1, rig.Budget.Scratch.Cache.Count);
            // Inspection downloaded the root once and kept it, so the build read it from the cache
            Assert.Equal(1, ReadsOf(rig.Hooks!, 3));

            rig.PastRetry();
            rig.Hooks!.ClearRequests();
            await rig.TickAsync();
            Assert.Equal(0, ReadsOf(rig.Hooks, 3));

            // Replaced at the same version; the cached bytes no longer match the listing
            await WriteBytes(rig.Storage, $"/t/_delta_log/{3:D20}.checkpoint.parquet", OwnRootBytes(3));
            rig.PastRetry();
            rig.Hooks.ClearRequests();
            await rig.TickAsync();
            Assert.True(ReadsOf(rig.Hooks, 3) > 0);
            Assert.Contains(5L, await Checkpoints(rig.Storage));
            Assert.Null(rig.Worker.Memo);
        }

        [Theory]
        [InlineData("parsed")]
        [InlineData("unknown")]
        public async Task RestartedWorkerReadsAWeakRepairedRootFresh(string kind)
        {
            // In-memory listings carry no modification time and no size, so a replacement keeps the identity
            var rig = Create(interval: 2, cached: true);
            await Commits(rig.Storage, 0, 5);
            await DecliningRoot(rig.Storage, kind, 3);
            rig.Worker.Published(5);
            await rig.TickAsync();
            Assert.True(rig.Worker.Memo!.Weak);
            Assert.Equal(1, rig.Budget.Scratch.Cache.Count);

            // Repaired at the same version, then the stream restarts; the new worker holds no memo, so it reads the root again
            await WriteBytes(rig.Storage, $"/t/_delta_log/{3:D20}.checkpoint.parquet", OwnRootBytes(3));
            await rig.Worker.StopAsync(TimeSpan.Zero);
            var successor = Create(rig.Storage, interval: 2, budget: rig.Budget, cached: true);
            successor.Worker.Published(5);
            await successor.TickAsync();
            Assert.Contains(5L, await Checkpoints(rig.Storage));
        }

        [Theory]
        [InlineData("parsed")]
        [InlineData("unknown")]
        public async Task WeakCachedRootIsDroppedWhenTheMemoExpires(string kind)
        {
            // In-memory listings carry no modification time and no size, so a replacement keeps the identity
            var rig = Create(interval: 2, cached: true);
            await Commits(rig.Storage, 0, 5);
            await DecliningRoot(rig.Storage, kind, 3);
            rig.Worker.Published(5);
            await rig.TickAsync();
            Assert.True(rig.Worker.Memo!.Weak);

            await WriteBytes(rig.Storage, $"/t/_delta_log/{3:D20}.checkpoint.parquet", OwnRootBytes(3));
            var setAt = rig.OptionsClock.GetUtcNow();
            for (var attempt = 0; !(await Checkpoints(rig.Storage)).Contains(5); attempt++)
            {
                Assert.True(attempt < 1000, "The retry time never moved");
                rig.PastRetry();
                await rig.TickAsync();
                Assert.True(rig.OptionsClock.GetUtcNow() - setAt < TimeSpan.FromHours(8), "The replaced root was not read again");
            }
            // Recovered only once the weak memo expired and raised the floor over the cached copy
            Assert.True(rig.OptionsClock.GetUtcNow() - setAt >= DeclineMemo.WeakExpiry);
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task FailedCacheTeeStillCompletesThePublishedBuild(bool timeout)
        {
            // Wrapped local disk takes the upload path, and its Stat knows the size, so the first listing after the upload is the cache tee's
            var root = Path.Combine(_directory, "tee-failure");
            Directory.CreateDirectory(root);
            var storage = new HookFileStorage(Files.Of.LocalDisk(root));
            var rig = Create(storage, interval: 2, cached: true);
            await Commits(rig.Storage, 0, 2);
            var uploaded = 0;
            var failed = 0;
            storage.Before = (verb, path) =>
            {
                if (verb == "OpenWrite" && path.Full.EndsWith($"{2:D20}.checkpoint.parquet"))
                {
                    Volatile.Write(ref uploaded, 1);
                }
                else if (verb == "Ls" && Volatile.Read(ref uploaded) == 1 && Interlocked.Exchange(ref failed, 1) == 0)
                {
                    // A storage client's own timeout is a cancellation the job did not ask for
                    throw timeout ? new TaskCanceledException("The listing timed out") : new IOException("The listing failed");
                }
                return Task.CompletedTask;
            };
            rig.Worker.Published(2);
            await rig.TickAsync();

            Assert.Equal(1, failed);
            Assert.Equal(new long[] { 2 }, await Checkpoints(rig.Storage));
            // Caching is best effort: a checkpoint already published completes, and the hint follows it at once
            Assert.Equal(2, rig.Worker.LastCompleted);
            Assert.Equal(0, rig.Worker.ConsecutiveFailures);
            Assert.Equal(2, (await Hint(rig.Storage))!.Version);
        }

        [Fact]
        public async Task AnIoErrorOnACachedRootReleasesItsPin()
        {
            var rig = Create(interval: 2, cached: true);
            await Commits(rig.Storage, 0, 2);
            rig.Worker.Published(2);
            await rig.TickAsync();
            var cached = Assert.Single(Directory.GetFiles(rig.Budget.Scratch.Cache.DirectoryPath));

            // Another handle holds the cached root exclusively, as an antivirus scan can, so reading it fails with an IOException
            await Commits(rig.Storage, 3, 4);
            rig.Worker.Published(4);
            using (new FileStream(cached, FileMode.Open, FileAccess.Read, FileShare.None))
            {
                await rig.TickAsync();
            }
            Assert.Equal(1, rig.Worker.ConsecutiveFailures);

            // The failed job settled, so nothing pins the entry: evicting the cache frees every disk charge
            rig.Budget.Scratch.Cache.EvictFor(long.MaxValue);
            Assert.Equal(0, rig.Budget.Scratch.Cache.Count);
            Assert.Equal(0, rig.Budget.Scratch.Ledger.Reserved);
        }

        [Fact]
        public async Task PublishedOutputReplacesTheTablesOlderCachedRoot()
        {
            var rig = Create(interval: 2, cached: true);
            await Commits(rig.Storage, 0, 2);
            rig.Worker.Published(2);
            await rig.TickAsync();
            Assert.Equal(1, rig.Budget.Scratch.Cache.Count);

            await Commits(rig.Storage, 3, 4);
            rig.Worker.Published(4);
            await rig.TickAsync();
            Assert.Equal(new long[] { 2, 4 }, await Checkpoints(rig.Storage));
            // The build of 4 started from the cached 2, and its published output replaced it
            Assert.Equal(2, rig.Worker.LastBuild!.RootVersion);
            Assert.Equal(1, rig.Budget.Scratch.Cache.Count);
            Assert.Single(Directory.GetFiles(rig.Budget.Scratch.Cache.DirectoryPath));
        }

        [Fact]
        public async Task UnusableNewerRootLeavesTheOlderCachedRoot()
        {
            var rig = Create(interval: 2, cached: true);
            await Commits(rig.Storage, 0, 2);
            rig.Worker.Published(2);
            await rig.TickAsync();
            Assert.Equal(new long[] { 2 }, await Checkpoints(rig.Storage));
            Assert.Equal(1, rig.Budget.Scratch.Cache.Count);

            // Another writer's checkpoint at 3 passes the admission scan, and its sidecars are found only while it is read
            await Commits(rig.Storage, 3, 6);
            await SidecarCheckpoint(rig.Storage, 3);
            rig.Worker.Published(6);
            await rig.TickAsync();
            Assert.Equal(1, rig.Worker.ConsecutiveFailures);
            Assert.Single(rig.Worker.UnsupportedRoots);

            // The next build skips 3 and starts from our own 2, which the cache still holds
            rig.Hooks!.ClearRequests();
            rig.PastRetry();
            await rig.TickAsync();
            Assert.Contains(6L, await Checkpoints(rig.Storage));
            Assert.Equal(2, rig.Worker.LastBuild!.RootVersion);
            Assert.Equal(0, ReadsOf(rig.Hooks, 2));
        }

        [Theory]
        [InlineData("parsed")]
        [InlineData("unknown")]
        public async Task RestartedWorkerRejectsAWeakRootCachedByItsDetachedPredecessor(string kind)
        {
            var storage = new RootDisposeGateStorage(Files.Of.InternalMemory($"./late-root-{Guid.NewGuid():N}"));
            var older = Create(storage, interval: 2, cached: true);
            await Commits(storage, 0, 5);
            await DecliningRoot(storage, kind, 3);
            older.Worker.Published(5);
            older.Budget.TickNow();
            var oldJob = older.Worker.CurrentJob;
            try
            {
                await storage.DisposeReached.Task.WaitAsync(TimeSpan.FromSeconds(30));
                // All the old bytes are downloaded, and the download's cleanup has not finished
                await WriteBytes(storage, $"/t/_delta_log/{3:D20}.checkpoint.parquet", OwnRootBytes(3));
                await older.Worker.StopAsync(TimeSpan.Zero);
                Assert.Equal(1, older.Worker.DetachedJobs);
                var successor = Create(storage, interval: 2, budget: older.Budget, cached: true);

                // The old root enters the cache after the successor was created
                storage.ReleaseDispose.TrySetResult();
                await oldJob.WaitAsync(TimeSpan.FromSeconds(30));
                Assert.Equal(1, older.Budget.Scratch.Cache.Count);
                successor.Worker.Published(5);
                await successor.TickAsync();
                Assert.Contains(5L, await Checkpoints(storage));
            }
            finally
            {
                storage.ReleaseDispose.TrySetResult();
                await oldJob.WaitAsync(TimeSpan.FromSeconds(30));
            }
        }

        [Theory]
        [InlineData("parsed")]
        [InlineData("unknown")]
        public async Task SuccessorQueuedOnTheLeaseRejectsAWeakRootItsPredecessorCachesLater(string kind)
        {
            var storage = new RootDisposeGateStorage(Files.Of.InternalMemory($"./queued-root-{Guid.NewGuid():N}"));
            var older = Create(storage, interval: 2, cached: true);
            await Commits(storage, 0, 5);
            await DecliningRoot(storage, kind, 3);
            older.Worker.Published(5);
            older.Budget.TickNow();
            var oldJob = older.Worker.CurrentJob;
            try
            {
                await storage.DisposeReached.Task.WaitAsync(TimeSpan.FromSeconds(30));
                await WriteBytes(storage, $"/t/_delta_log/{3:D20}.checkpoint.parquet", OwnRootBytes(3));
                await older.Worker.StopAsync(TimeSpan.Zero);
                var successor = Create(storage, interval: 2, budget: older.Budget, cached: true);

                // The successor is dispatched and waits on the table lease before the predecessor caches the old bytes
                successor.Worker.Published(5);
                successor.Budget.TickNow();
                var newJob = successor.Worker.CurrentJob;
                await Queued(() => older.Budget.Leases.Waiting == 1);
                storage.ReleaseDispose.TrySetResult();
                await oldJob.WaitAsync(TimeSpan.FromSeconds(30));
                await newJob.WaitAsync(TimeSpan.FromSeconds(30));
                Assert.Contains(5L, await Checkpoints(storage));
            }
            finally
            {
                storage.ReleaseDispose.TrySetResult();
                await oldJob.WaitAsync(TimeSpan.FromSeconds(30));
            }
        }

        // Holds the disposal of the first checkpoint download until released
        private sealed class RootDisposeGateStorage : HookFileStorage, IFileStorage
        {
            private int _held;

            public RootDisposeGateStorage(IFileStorage inner) : base(inner)
            {
            }

            public TaskCompletionSource DisposeReached { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

            public TaskCompletionSource ReleaseDispose { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

            async Task<Stream?> IFileStorage.OpenRead(IOPath path, CancellationToken cancellationToken)
            {
                var stream = await base.OpenRead(path, cancellationToken);
                if (stream is MemoryStream memory && path.Full.EndsWith(".checkpoint.parquet") && Interlocked.Exchange(ref _held, 1) == 0)
                {
                    var copy = memory.ToArray();
                    await memory.DisposeAsync();
                    return new DisposeGateMemoryStream(copy, DisposeReached, ReleaseDispose.Task);
                }
                return stream;
            }
        }

        private sealed class DisposeGateMemoryStream : MemoryStream
        {
            private readonly TaskCompletionSource _reached;
            private readonly Task _release;

            public DisposeGateMemoryStream(byte[] bytes, TaskCompletionSource reached, Task release) : base(bytes, false)
            {
                _reached = reached;
                _release = release;
            }

            public override async ValueTask DisposeAsync()
            {
                _reached.TrySetResult();
                await _release;
                await base.DisposeAsync();
            }
        }
    }
}
