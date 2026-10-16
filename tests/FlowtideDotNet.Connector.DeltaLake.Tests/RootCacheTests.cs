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

using FlowtideDotNet.Connector.DeltaLake.Internal.Delta;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Checkpoints;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance;
using Microsoft.Extensions.Time.Testing;
using Stowage;
using System.Runtime.CompilerServices;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public class RootCacheTests : IDisposable
    {
        private readonly string _directory = Path.Combine(Path.GetTempPath(), "rc-" + ScratchOwners.ShortId());

        public void Dispose()
        {
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

        private static ObjectIdentity Identity(long version, string table = "/t", long size = 100, DateTimeOffset? modified = null) => new ObjectIdentity(table, version, size, modified, null);

        // A charged file of the given length in a job's folder
        private static string WriteFile(JobScratch job, int length)
        {
            var path = job.NewPath(".download.parquet");
            using (var stream = job.Create(path))
            {
                stream.Write(new byte[length]);
            }
            return path;
        }

        private static void PutUnpinned(RootCache cache, RootCacheScope scope, ObjectIdentity identity, JobScratch job, int length)
        {
            var lease = cache.Put(scope, identity, job, WriteFile(job, length));
            Assert.NotNull(lease);
            lease!.Dispose();
        }

        [Fact]
        public void CacheMissAfterTableIdChange()
        {
            var directory = new ScratchDirectory(_directory);
            var job = directory.CreateJob();
            var storage = Files.Of.InternalMemory("./cache-table-id");
            var scope = new RootCacheScope(directory.Cache, storage, "table-a", 0);
            PutUnpinned(directory.Cache, scope, Identity(3), job, 64);

            using (var hit = directory.Cache.TryGet(scope, Identity(3)))
            {
                Assert.NotNull(hit);
            }
            // A table recreated at the same path has a new metaData.id, so its roots are never served from the old one's
            Assert.Null(directory.Cache.TryGet(scope with { TableId = "table-b" }, Identity(3)));
            // Another storage instance is another key as well
            Assert.Null(directory.Cache.TryGet(scope with { Storage = Files.Of.InternalMemory("./cache-table-id-2") }, Identity(3)));
        }

        [Fact]
        public void ChangedIdentityDropsTheEntry()
        {
            var directory = new ScratchDirectory(_directory);
            var job = directory.CreateJob();
            var scope = new RootCacheScope(directory.Cache, Files.Of.InternalMemory("./cache-identity"), "id", 0);
            PutUnpinned(directory.Cache, scope, Identity(3, modified: DateTimeOffset.UnixEpoch), job, 64);
            Assert.Equal(64, directory.Ledger.Reserved);

            Assert.Null(directory.Cache.TryGet(scope, Identity(3, modified: DateTimeOffset.UnixEpoch.AddTicks(1))));
            Assert.Equal(0, directory.Cache.Count);
            Assert.Equal(0, directory.Ledger.Reserved);
        }

        [Fact]
        public void WeakEntriesAtOrBelowTheFloorAreDropped()
        {
            long sequence = 10;
            var directory = new ScratchDirectory(_directory, nextFactSequence: () => ++sequence);
            var job = directory.CreateJob();
            var scope = new RootCacheScope(directory.Cache, Files.Of.InternalMemory("./cache-weak"), "id", 0);
            PutUnpinned(directory.Cache, scope, Identity(3, "/weak"), job, 64);
            PutUnpinned(directory.Cache, scope, Identity(4, "/strong", modified: DateTimeOffset.UnixEpoch), job, 64);
            Assert.Equal(2, directory.Cache.Count);

            // Entry 3 holds sequence 11, entry 4 sequence 12; a floor at 12 retires only the weak one
            Assert.Null(directory.Cache.TryGet(scope with { WeakFloor = 12 }, Identity(3, "/weak")));
            using var strong = directory.Cache.TryGet(scope with { WeakFloor = 12 }, Identity(4, "/strong", modified: DateTimeOffset.UnixEpoch));
            Assert.NotNull(strong);
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static void PutForAStorageThatDies(RootCache cache, JobScratch job)
        {
            // A wrapper of its own, so nothing else keeps this storage alive
            var scope = new RootCacheScope(cache, new HookFileStorage(Files.Of.InternalMemory("./cache-dead")), "id", 0);
            PutUnpinned(cache, scope, Identity(1), job, 32);
        }

        [Fact]
        public void DeadStorageEntriesAreEvicted()
        {
            var directory = new ScratchDirectory(_directory);
            var job = directory.CreateJob();
            PutForAStorageThatDies(directory.Cache, job);
            Assert.Equal(1, directory.Cache.Count);
            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();

            // Any later insert trims entries whose storage is gone, before any live one
            var live = Files.Of.InternalMemory("./cache-live");
            PutUnpinned(directory.Cache, new RootCacheScope(directory.Cache, live, "id", 0), Identity(2), job, 32);
            Assert.Equal(1, directory.Cache.Count);
            Assert.Equal(32, directory.Ledger.Reserved);
            GC.KeepAlive(live);
        }

        [Fact]
        public void DiskCapEvictsCachedRootsThenDeclines()
        {
            var directory = new ScratchDirectory(_directory, diskCap: 1000);
            var job = directory.CreateJob();
            var scope = new RootCacheScope(directory.Cache, Files.Of.InternalMemory("./cache-cap"), "id", 0);
            PutUnpinned(directory.Cache, scope, Identity(1), job, 600);
            Assert.Equal(600, directory.Ledger.Reserved);

            // The unpinned root makes room for a reservation that would not fit beside it
            directory.Ledger.Reserve(600);
            Assert.Equal(0, directory.Cache.Count);
            Assert.Equal(600, directory.Ledger.Reserved);
            directory.Ledger.Release(600);

            // A pinned root is never evicted, so the reservation declines
            using var pinned = directory.Cache.Put(scope, Identity(2), job, WriteFile(job, 600));
            Assert.NotNull(pinned);
            var declined = Assert.Throws<CheckpointDeclinedException>(() => directory.Ledger.Reserve(600));
            Assert.Equal(MaintenanceReason.DiskCap, declined.Reason);
            Assert.Equal(1, directory.Cache.Count);
        }

        [Fact]
        public async Task RootLargerThanTheDiskCapLeavesOtherTablesCachedRoots()
        {
            // A 300 KiB cap: each 256 KiB download chunk fits on its own, the 600 KiB root never does
            var directory = new ScratchDirectory(Path.Combine(_directory, "scratch"), diskCap: 300 << 10);
            var job = directory.CreateJob();
            var other = new RootCacheScope(directory.Cache, Files.Of.InternalMemory("./cache-over-cap-other"), "other-id", 0);
            PutUnpinned(directory.Cache, other, Identity(1, "/other", size: 8), job, 8);
            Assert.Equal(1, directory.Cache.Count);

            // Wrapped local disk takes the download path, and its listing reports the root's size
            var tableRoot = Path.Combine(_directory, "table");
            Directory.CreateDirectory(tableRoot);
            var storage = new HookFileStorage(Files.Of.LocalDisk(tableRoot));
            await using (var write = await storage.OpenWrite($"/t/_delta_log/{3:D20}.checkpoint.parquet"))
            {
                await write.WriteAsync(new byte[600 << 10]);
            }
            var file = Assert.Single((await DeltaLogSegment.ListAsync(storage, "t")).Listing.Checkpoints);
            Assert.Equal(600L << 10, file.IOEntry.Size);

            var scope = new RootCacheScope(directory.Cache, storage, "id", 0);
            var declined = await Assert.ThrowsAsync<CheckpointDeclinedException>(() =>
                CheckpointInspector.FetchAsync(new StowageCheckpointTransport(storage, "t"), job, new JobMemory(), null, file, CancellationToken.None, scope));
            Assert.Equal(MaintenanceReason.DiskCap, declined.Reason);
            // The listed size alone shows the root can never fit, so no other table's cached root is evicted for it
            Assert.Equal(1, directory.Cache.Count);
        }

        [Fact]
        public void PublishedRootReplacesTheTablesOlderRoots()
        {
            var directory = new ScratchDirectory(_directory);
            var job = directory.CreateJob();
            var scope = new RootCacheScope(directory.Cache, Files.Of.InternalMemory("./cache-supersede"), "id", 0);
            var other = new RootCacheScope(directory.Cache, Files.Of.InternalMemory("./cache-supersede-other"), "id", 0);
            var older = directory.Cache.Put(scope, Identity(3), job, WriteFile(job, 16))!;
            var olderPath = older.Path;
            older.Dispose();
            // A downloaded root may still fail while it is read, so it keeps the older roots
            PutUnpinned(directory.Cache, scope, Identity(4), job, 16);
            Assert.True(File.Exists(olderPath));
            Assert.Equal(2, directory.Cache.Count);
            var pinned = directory.Cache.Put(scope, Identity(5), job, WriteFile(job, 16), supersede: true)!;
            PutUnpinned(directory.Cache, other, Identity(4), job, 16);
            Assert.False(File.Exists(olderPath));
            Assert.Equal(2, directory.Cache.Count);

            // A pinned older root goes at its unpin
            directory.Cache.Put(scope, Identity(7), job, WriteFile(job, 16), supersede: true)!.Dispose();
            Assert.True(File.Exists(pinned.Path));
            pinned.Dispose();
            Assert.False(File.Exists(pinned.Path));
            // Another storage's root of the same table name stays
            Assert.Equal(2, directory.Cache.Count);
            Assert.Null(directory.Cache.TryGet(scope, Identity(5)));
            using var kept = directory.Cache.TryGet(other, Identity(4));
            Assert.NotNull(kept);
            Assert.Equal(32, directory.Ledger.Reserved);
        }

        [Fact]
        public void CacheMetadataStaysWithinItsByteCap()
        {
            var directory = new ScratchDirectory(_directory);
            var cache = new RootCache(directory, () => 1, maxEntries: 256, maxMetadataBytes: 4096);
            var job = directory.CreateJob();
            var scope = new RootCacheScope(cache, Files.Of.InternalMemory("./cache-metadata"), "id", 0);
            var table = "/" + new string('t', 300);
            for (int i = 0; i < 20; i++)
            {
                PutUnpinned(cache, scope, Identity(0, table + i), job, 8);
                Assert.InRange(cache.MetadataBytes, 1, cache.MaxMetadataBytes);
            }
            Assert.InRange(cache.Count, 2, 19);
            // The newest entries stay
            using var newest = cache.TryGet(scope, Identity(0, table + 19));
            Assert.NotNull(newest);
        }

        [Fact]
        public void CacheEntryCountStaysWithinItsCap()
        {
            var directory = new ScratchDirectory(_directory);
            var cache = new RootCache(directory, () => 1, maxEntries: 3);
            var job = directory.CreateJob();
            var scope = new RootCacheScope(cache, Files.Of.InternalMemory("./cache-count"), "id", 0);
            for (int i = 0; i < 6; i++)
            {
                PutUnpinned(cache, scope, Identity(0, "/t" + i), job, 8);
            }
            Assert.Equal(3, cache.Count);
            Assert.Null(cache.TryGet(scope, Identity(0, "/t0")));
            using var newest = cache.TryGet(scope, Identity(0, "/t5"));
            Assert.NotNull(newest);
        }

        [Fact]
        public void PinnedEntryReplacedByANewerCopyIsDeletedOnUnpin()
        {
            var directory = new ScratchDirectory(_directory);
            var job = directory.CreateJob();
            var scope = new RootCacheScope(directory.Cache, Files.Of.InternalMemory("./cache-pinned"), "id", 0);
            var first = directory.Cache.Put(scope, Identity(3), job, WriteFile(job, 64))!;
            var firstPath = first.Path;
            PutUnpinned(directory.Cache, scope, Identity(3), job, 64);
            // The replaced copy stays while a build reads it
            Assert.True(File.Exists(firstPath));
            first.Dispose();
            Assert.False(File.Exists(firstPath));
            Assert.Equal(64, directory.Ledger.Reserved);
        }

        [Fact]
        public void CapsHoldAgainOnceThePinsAreReleased()
        {
            var directory = new ScratchDirectory(_directory);
            var cache = new RootCache(directory, () => 1, maxEntries: 1);
            var job = directory.CreateJob();
            var scope = new RootCacheScope(cache, Files.Of.InternalMemory("./cache-unpin"), "id", 0);
            // Both go in pinned, so neither insertion could trim
            var first = cache.Put(scope, Identity(0, "/a"), job, WriteFile(job, 8))!;
            var second = cache.Put(scope, Identity(0, "/b"), job, WriteFile(job, 8))!;
            Assert.Equal(2, cache.Count);
            first.Dispose();
            second.Dispose();
            Assert.Equal(1, cache.Count);
            Assert.False(File.Exists(first.Path));
            Assert.True(File.Exists(second.Path));
            Assert.Equal(8, directory.Ledger.Reserved);

            // An entry larger than the whole metadata allowance goes at its last unpin
            var small = new RootCache(directory, () => 1, maxMetadataBytes: 64);
            var big = small.Put(scope, Identity(2, "/" + new string('t', 300)), job, WriteFile(job, 8))!;
            Assert.Equal(1, small.Count);
            big.Dispose();
            Assert.Equal(0, small.Count);
            Assert.InRange(small.MetadataBytes, 0, small.MaxMetadataBytes);
        }

        [Fact]
        public void EvictionGoesOnPastAFailedDeletion()
        {
            var directory = new ScratchDirectory(_directory, diskCap: 1000);
            var job = directory.CreateJob();
            var scope = new RootCacheScope(directory.Cache, Files.Of.InternalMemory("./cache-failed-delete"), "id", 0);
            var blocked = directory.Cache.Put(scope, Identity(1, "/a"), job, WriteFile(job, 600))!;
            blocked.Dispose();
            var available = directory.Cache.Put(scope, Identity(1, "/b"), job, WriteFile(job, 400))!;
            available.Dispose();
            directory.DeleteFailsForTests = path => path == blocked.Path;

            // The oldest root keeps its charge, the next one makes the room
            directory.Ledger.Reserve(300);
            Assert.Equal(900, directory.Ledger.Reserved);
            Assert.Equal(600, directory.PendingBytes);
            Assert.False(File.Exists(available.Path));
            Assert.Equal(0, directory.Cache.Count);
            directory.Ledger.Release(300);
        }

        // A foreign object replaces the published checkpoint right after the publisher's first look at it
        private sealed class ReplacedAfterFirstLook : ICheckpointTransport
        {
            private readonly StowageCheckpointTransport _inner;
            private readonly byte[] _replacement;
            private int _replaced;

            public ReplacedAfterFirstLook(IFileStorage storage, byte[] replacement)
            {
                _inner = new StowageCheckpointTransport(storage, "t");
                _replacement = replacement;
            }

            public IFileStorage Storage => _inner.Storage;
            public IOPath Table => _inner.Table;
            public Task<DeltaLogSegment> ListAsync(CancellationToken ct) => _inner.ListAsync(ct);
            public Task<LastCheckpointInfo?> ReadHintAsync(CancellationToken ct) => _inner.ReadHintAsync(ct);
            public Task WriteHintAsync(LastCheckpointInfo hint, CancellationToken ct) => _inner.WriteHintAsync(hint, ct);
            public string? NativePath(IOPath path) => null;
            public Task<long> DownloadAsync(IOPath path, Stream target, JobMemory memory, CancellationToken ct) => _inner.DownloadAsync(path, target, memory, ct);
            public Task UploadAsync(string local, IOPath destination, JobMemory memory, CancellationToken ct) => _inner.UploadAsync(local, destination, memory, ct);
            public ValueTask ChargeNativeAsync(long requests, long bytes, CancellationToken ct) => _inner.ChargeNativeAsync(requests, bytes, ct);

            public async Task<long?> SizeAsync(IOPath path, CancellationToken ct)
            {
                var size = await _inner.SizeAsync(path, ct);
                await Replace(path, ct);
                return size;
            }

            public async Task<ObjectIdentity?> IdentityAsync(IOPath path, long version, CancellationToken ct)
            {
                var identity = await _inner.IdentityAsync(path, version, ct);
                await Replace(path, ct);
                return identity;
            }

            private async Task Replace(IOPath path, CancellationToken ct)
            {
                if (Interlocked.Exchange(ref _replaced, 1) == 0)
                {
                    await using var stream = await Storage.OpenWrite(path, ct);
                    await stream.WriteAsync(_replacement, ct);
                }
            }
        }

        [Fact]
        public async Task CachedOutputKeepsTheIdentityItsSizeWasCheckedUnder()
        {
            var directory = new ScratchDirectory(_directory);
            var storage = new HookFileStorage(Files.Of.InternalMemory("./cache-identity-" + Guid.NewGuid().ToString("N"))) { StampIdentity = true };
            var clock = new FakeTimeProvider(new DateTimeOffset(2026, 10, 1, 12, 0, 0, TimeSpan.Zero));
            using var job = directory.CreateJob();
            var memory = new JobMemory();

            async Task<CheckpointDescriptor> Build(long version, bool cached = false)
            {
                var result = await CheckpointBuilder.BuildAsync(new CheckpointBuildContext()
                {
                    Storage = storage,
                    Table = "t",
                    Scratch = job,
                    Memory = memory,
                    Cache = cached ? directory.Cache : null,
                    Clock = clock,
                    Publish = false
                }, new BuildRequest() { Version = version, TableId = "table-id" });
                Assert.Equal(BuildStatus.Completed, result.Status);
                return result.Checkpoint!;
            }

            // Two roots of version 0 with the same size and different rows
            await DeltaTestKit.WriteCommit(storage, "t", 0, DeltaTestKit.Protocol(), DeltaTestKit.Metadata(DeltaTestKit.UserSchema), DeltaLakeCheckpointBuildTests.Add("old-file.parquet"));
            var original = await Build(0);
            await DeltaTestKit.WriteCommit(storage, "t", 0, DeltaTestKit.Protocol(), DeltaTestKit.Metadata(DeltaTestKit.UserSchema), DeltaLakeCheckpointBuildTests.Add("new-file.parquet"));
            var replacement = await File.ReadAllBytesAsync((await Build(0)).LocalPath);
            Assert.Equal(original.SizeInBytes, replacement.Length);

            var scope = new RootCacheScope(directory.Cache, storage, "table-id", 0);
            var published = await CheckpointPublisher.PublishAsync(new ReplacedAfterFirstLook(storage, replacement), job, memory, original, null, default, cache: scope);
            Assert.True(published.Published);
            Assert.Equal(replacement, await DeltaTestKit.ReadBytes(storage, "/t/_delta_log/00000000000000000000.checkpoint.parquet"));

            // The replacement lists under an identity of its own, so a cached build never reads the earlier bytes for it
            await DeltaTestKit.WriteCommit(storage, "t", 1, DeltaLakeCheckpointBuildTests.Add("tail.parquet"));
            var cached = await Build(1, cached: true);
            var fresh = await Build(1);
            Assert.Equal(fresh.Sha256, cached.Sha256);
        }
    }
}
