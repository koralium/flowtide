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

using FlowtideDotNet.Storage.Exceptions;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Persistence.Reservoir;
using FlowtideDotNet.Storage.Persistence.Reservoir.Internal;
using FlowtideDotNet.Storage.Persistence.Reservoir.LocalCache;
using FlowtideDotNet.Storage.Persistence.Reservoir.MemoryDisk;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.StateManager.Internal;
using Microsoft.Extensions.Logging.Abstractions;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.IO.Pipelines;
using Microsoft.Extensions.Time.Testing;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace FlowtideDotNet.Storage.Tests.Reservoir
{
    public class ConcurrentLocalCacheManagerEdgeCaseTests
    {
        private record TestStack(
            ReservoirPersistentStorage Storage,
            ConcurrentLocalCacheManager Cclm,
            TestDataProvider LocalData,
            TestDataProvider RemoteData);

        private static readonly StorageProviderContext DefaultCtx =
            new("test", "v1", GlobalMemoryManager.Instance);

        private static long _pageKeyCounter = 100;

        private static TestStack CreateStack() => CreateStack(10L * 1_000_000_000);

        private static TestStack CreateStack(long maxCacheSizeBytes, TimeProvider? timeProvider = null)
        {
            var remoteData = new TestDataProvider();
            var localData  = new TestDataProvider();

            var storage = new ReservoirPersistentStorage(
                new ReservoirStorageOptions
                {
                    FileProvider  = remoteData,
                    CacheProvider = new MemoryFileProvider()
                });
            storage.InitializeAsync(
                    new Persistence.StorageInitializationMetadata("test", NullLoggerFactory.Instance))
                .GetAwaiter().GetResult();

            var metrics = new LocalCacheMetricValues();
            var meter   = new Meter("cclm-tests");

            var cclm = new ConcurrentLocalCacheManager(
                storage, localData, remoteData, maxCacheSizeBytes, metrics, timeProvider: timeProvider);
            cclm.InitializeAsync(
                    new Persistence.StorageInitializationMetadata("test", NullLoggerFactory.Instance),
                    meter, DefaultCtx, CancellationToken.None)
                .GetAwaiter().GetResult();

            return new TestStack(storage, cclm, localData, remoteData);
        }

        private static async Task<(uint Crc32, int Offset, int FileSize)> WriteLocalAsync(
            ConcurrentLocalCacheManager cclm, ulong fileId, byte[] payload)
        {
            var blob = new BlobFileWriter((f) => { }, MemoryPool<byte>.Shared, GlobalMemoryManager.Instance);
            blob.Write((long)fileId, new SerializableObject(payload));
            blob.Finish();
            uint crc32    = blob.Crc32s[0];
            int  offset   = blob.PageOffsets[0];
            int  fileSize = blob.FileSize;
            await cclm.RegisterNewFileAsync(fileId, blob.Crc64, fileSize, blob);
            return (crc32, offset, fileSize);
        }

        private static async Task<(uint Crc32, int Offset, int FileSize)> CommitAsync(
            ReservoirPersistentStorage storage, byte[] payload)
        {
            long pageKey = Interlocked.Increment(ref _pageKeyCounter);
            var blob = new BlobFileWriter((f) => { }, MemoryPool<byte>.Shared, GlobalMemoryManager.Instance);
            blob.Write(pageKey, new SerializableObject(payload));
            blob.Finish();
            uint crc32    = blob.Crc32s[0];
            int  offset   = blob.PageOffsets[0];
            int  fileSize = blob.FileSize;
            await storage.AddCompleteBlobFile(blob);
            await storage.CheckpointAsync(new byte[] { 1 }, false);
            return (crc32, offset, fileSize);
        }

        private static async Task<int> MeasureFileSizeAsync(byte[] payload)
        {
            var probe = CreateStack();
            var (_, _, size) = await WriteLocalAsync(probe.Cclm, 1, payload);
            return size;
        }

        [Fact]
        public async Task TestCurrentSizeIsZeroOnFreshStack()
        {
            var s = CreateStack();
            Assert.Equal(0L, s.Cclm.CurrentSize);
            await Task.CompletedTask;
        }

        [Fact]
        public async Task TestCurrentSizeIncreasesAfterWrite()
        {
            var s = CreateStack();
            var (_, _, fileSize) = await WriteLocalAsync(s.Cclm, 1, new byte[] { 1, 2, 3 });
            Assert.Equal((long)fileSize, s.Cclm.CurrentSize);
        }

        [Fact]
        public async Task TestCurrentSizeDecreasesAfterEvict()
        {
            var s = CreateStack();
            await WriteLocalAsync(s.Cclm, 1, new byte[] { 1, 2, 3 });
            await s.Cclm.EvictDataFileAsync(1);
            Assert.Equal(0L, s.Cclm.CurrentSize);
        }

        [Fact]
        public async Task TestCurrentSizeCorrectForMultipleDistinctFiles()
        {
            var s = CreateStack();
            var (_, _, sz1) = await WriteLocalAsync(s.Cclm, 1, new byte[] { 1, 2, 3 });
            var (_, _, sz2) = await WriteLocalAsync(s.Cclm, 2, new byte[] { 4, 5, 6, 7 });
            Assert.Equal((long)(sz1 + sz2), s.Cclm.CurrentSize);
        }

        [Fact]
        public async Task TestCurrentSizeZeroAfterEvictingAllFiles()
        {
            var s = CreateStack();
            await WriteLocalAsync(s.Cclm, 1, new byte[] { 1, 2, 3 });
            await WriteLocalAsync(s.Cclm, 2, new byte[] { 4, 5, 6 });
            await s.Cclm.EvictDataFileAsync(1);
            await s.Cclm.EvictDataFileAsync(2);
            Assert.Equal(0L, s.Cclm.CurrentSize);
        }

        [Fact]
        public async Task TestCurrentSizeNotDoubledAfterEvictAndRedownload()
        {
            var s = CreateStack();
            var (crc32, offset, _) = await CommitAsync(s.Storage, new byte[] { 1, 2, 3 });

            _ = await s.Cclm.ReadMemoryAsync(0, offset, 3, crc32);
            long sizeAfterDownload = s.Cclm.CurrentSize;

            await s.Cclm.EvictDataFileAsync(0);
            _ = await s.Cclm.ReadMemoryAsync(0, offset, 3, crc32);

            Assert.Equal(sizeAfterDownload, s.Cclm.CurrentSize);
        }

        [Fact]
        public async Task TestCurrentSizeNeverNegativeAfterRepeatedWriteEvict()
        {
            var s = CreateStack();
            for (int round = 0; round < 5; round++)
            {
                for (ulong i = 1; i <= 4; i++)
                    await WriteLocalAsync(s.Cclm, i + (ulong)(round * 4), new byte[] { 1, 2, 3 });
                for (ulong i = 1; i <= 4; i++)
                    await s.Cclm.EvictDataFileAsync(i + (ulong)(round * 4));
            }
            Assert.True(s.Cclm.CurrentSize >= 0, $"CurrentSize went negative: {s.Cclm.CurrentSize}");
        }

        [Fact]
        public async Task TestGetMemoryAsyncCRCMismatchRedownloadsAndReturnsCorrectData()
        {
            var s = CreateStack();
            var (crc32, offset, _) = await CommitAsync(s.Storage, new byte[] { 1, 2, 3 });

            _ = await s.Cclm.ReadMemoryAsync(0, offset, 3, crc32);

            s.LocalData.TryGetFileData(0, out var bytes);
            bytes![offset + 1] ^= 0xFF;

            var result = await s.Cclm.ReadMemoryAsync(0, offset, 3, crc32);
            Assert.Equal(new byte[] { 1, 2, 3 }, result.ToArray());
        }

        [Fact]
        public async Task TestGetMemoryAsyncCRCMismatchSizeRemainsConsistent()
        {
            var s = CreateStack();
            var (crc32, offset, _) = await CommitAsync(s.Storage, new byte[] { 1, 2, 3 });

            _ = await s.Cclm.ReadMemoryAsync(0, offset, 3, crc32);
            long sizeAfterFirstDownload = s.Cclm.CurrentSize;

            s.LocalData.TryGetFileData(0, out var bytes);
            bytes![offset + 1] ^= 0xFF;

            _ = await s.Cclm.ReadMemoryAsync(0, offset, 3, crc32);

            Assert.Equal(sizeAfterFirstDownload, s.Cclm.CurrentSize);
        }

        [Fact]
        public async Task TestGetMemoryAsyncPersistentCRCMismatchPropagatesException()
        {
            var s = CreateStack();
            var (crc32, offset, _) = await CommitAsync(s.Storage, new byte[] { 1, 2, 3 });

            _ = await s.Cclm.ReadMemoryAsync(0, offset, 3, crc32);

            // Corrupt both local and remote so re-download also fails.
            s.LocalData.TryGetFileData(0, out var lb);
            lb![offset + 1] ^= 0xFF;
            s.RemoteData.TryGetFileData(0, out var rb);
            rb![offset + 1] ^= 0xFF;

            await Assert.ThrowsAsync<FlowtideChecksumMismatchException>(
                () => s.Cclm.ReadMemoryAsync(0, offset, 3, crc32).AsTask());
        }

        [Fact]
        public async Task TestEvictDataFileAsyncRemovesFromLocalStorage()
        {
            var s = CreateStack();
            await WriteLocalAsync(s.Cclm, 1, new byte[] { 1, 2, 3 });
            await s.Cclm.EvictDataFileAsync(1);
            Assert.False(s.LocalData.DataFileExists(1));
        }

        [Fact]
        public async Task TestEvictDataFileAsyncNonExistentFileDoesNotThrow()
        {
            var s = CreateStack();
            await s.Cclm.EvictDataFileAsync(999);
            Assert.Equal(0L, s.Cclm.CurrentSize);
        }

        [Fact]
        public async Task TestEvictDataFileAsyncCalledTwiceIsIdempotent()
        {
            var s = CreateStack();
            await WriteLocalAsync(s.Cclm, 1, new byte[] { 1, 2, 3 });
            await s.Cclm.EvictDataFileAsync(1);
            await s.Cclm.EvictDataFileAsync(1);
            Assert.True(s.Cclm.CurrentSize >= 0);
        }

        [Fact]
        public async Task TestEvictAndRedownloadDataIntegrityPreserved()
        {
            var s = CreateStack();
            var (crc32, offset, _) = await CommitAsync(s.Storage, new byte[] { 7, 8, 9 });

            _ = await s.Cclm.ReadMemoryAsync(0, offset, 3, crc32);
            await s.Cclm.EvictDataFileAsync(0);

            var result = await s.Cclm.ReadMemoryAsync(0, offset, 3, crc32);
            Assert.Equal(new byte[] { 7, 8, 9 }, result.ToArray());
        }

        [Fact]
        public async Task TestGetMemoryAsyncConcurrentReadsOnlyOneRemoteFetch()
        {
            var s = CreateStack();
            var (crc32, offset, _) = await CommitAsync(s.Storage, new byte[] { 1, 2, 3 });

            s.LocalData.BlockWrites();
            var tasks = Enumerable.Range(0, 8)
                .Select(_ => s.Cclm.ReadMemoryAsync(0, offset, 3, crc32).AsTask())
                .ToList();
            s.LocalData.UnblockWrites();
            var results = await Task.WhenAll(tasks);

            Assert.Equal(1, s.RemoteData.NumberOfReadDataFile);
            foreach (var r in results)
                Assert.Equal(new byte[] { 1, 2, 3 }, r.ToArray());
        }

        [Fact]
        public async Task TestConcurrentReadsAfterBlockAllReturnSameData()
        {
            var s = CreateStack();
            var (crc32, offset, _) = await CommitAsync(s.Storage, new byte[] { 9, 8, 7 });

            s.LocalData.BlockWrites();
            var tasks = Enumerable.Range(0, 6)
                .Select(_ => s.Cclm.ReadMemoryAsync(0, offset, 3, crc32).AsTask())
                .ToList();
            s.LocalData.UnblockWrites();
            var results = await Task.WhenAll(tasks);

            Assert.Equal(1, s.RemoteData.NumberOfReadDataFile);
            foreach (var r in results)
                Assert.Equal(new byte[] { 9, 8, 7 }, r.ToArray());
        }

        [Fact]
        public async Task TestConcurrentCRCMismatchAllReadersRecoverCorrectly()
        {
            var s = CreateStack();
            var (crc32, offset, _) = await CommitAsync(s.Storage, new byte[] { 1, 2, 3 });

            _ = await s.Cclm.ReadMemoryAsync(0, offset, 3, crc32);

            s.LocalData.TryGetFileData(0, out var bytes);
            bytes![offset + 1] ^= 0xFF;

            s.LocalData.BlockWrites();
            var tasks = Enumerable.Range(0, 5)
                .Select(_ => s.Cclm.ReadMemoryAsync(0, offset, 3, crc32).AsTask())
                .ToList();
            s.LocalData.UnblockWrites();
            var results = await Task.WhenAll(tasks);

            foreach (var r in results)
                Assert.Equal(new byte[] { 1, 2, 3 }, r.ToArray());
        }

        [Fact]
        public async Task TestConcurrentCRCMismatchSizeRemainsConsistentAfterRecovery()
        {
            var s = CreateStack();
            var (crc32, offset, _) = await CommitAsync(s.Storage, new byte[] { 1, 2, 3 });

            _ = await s.Cclm.ReadMemoryAsync(0, offset, 3, crc32);
            long sizeAfterFirstDownload = s.Cclm.CurrentSize;

            s.LocalData.TryGetFileData(0, out var bytes);
            bytes![offset + 1] ^= 0xFF;

            s.LocalData.BlockWrites();
            var tasks = Enumerable.Range(0, 5)
                .Select(_ => s.Cclm.ReadMemoryAsync(0, offset, 3, crc32).AsTask())
                .ToList();
            s.LocalData.UnblockWrites();
            await Task.WhenAll(tasks);

            // Exactly one re-download must have occurred
            Assert.Equal(sizeAfterFirstDownload, s.Cclm.CurrentSize);
        }

        [Fact]
        public async Task TestConcurrentEvictAndReadDoesNotDeadlockOrThrow()
        {
            var s = CreateStack();
            var (crc32, offset, _) = await CommitAsync(s.Storage, new byte[] { 5, 6, 7 });

            _ = await s.Cclm.ReadMemoryAsync(0, offset, 3, crc32);

            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(15));
            var evict = s.Cclm.EvictDataFileAsync(0);
            var read1 = s.Cclm.ReadMemoryAsync(0, offset, 3, crc32).AsTask();
            var read2 = s.Cclm.ReadMemoryAsync(0, offset, 3, crc32).AsTask();

            await Task.WhenAll(evict, read1, read2).WaitAsync(cts.Token);

            Assert.Equal(new byte[] { 5, 6, 7 }, read1.Result.ToArray());
            Assert.Equal(new byte[] { 5, 6, 7 }, read2.Result.ToArray());
        }

        [Fact]
        public async Task TestHighConcurrencyMixedReadWriteEvictNoDeadlock()
        {
            var s = CreateStack();
            var (crc32, offset, _) = await CommitAsync(s.Storage, new byte[] { 1, 2, 3 });

            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(15));
            var tasks = new List<Task>();
            for (int i = 0; i < 16; i++)
            {
                int idx = i;
                tasks.Add(Task.Run(async () =>
                {
                    switch (idx % 4)
                    {
                        case 0: await s.Cclm.EvictDataFileAsync(0); break;
                        case 1: _ = await s.Cclm.ReadMemoryAsync(0, offset, 3, crc32); break;
                        case 2: await WriteLocalAsync(s.Cclm, (ulong)(100 + idx), new byte[] { 1, 2, 3 }); break;
                        // Intentionally evicts the same fileId that case 2 for the previous
                        case 3: await s.Cclm.EvictDataFileAsync((ulong)(100 + idx - 1)); break;
                    }
                }));
            }
            await Task.WhenAll(tasks).WaitAsync(cts.Token);
        }

        [Fact]
        public async Task TestMultipleParallelDownloadsSizeAccountedExactlyOnce()
        {
            var s = CreateStack();
            var (crc32, offset, _) = await CommitAsync(s.Storage, new byte[] { 1, 2, 3 });

            s.LocalData.BlockWrites();
            var tasks = Enumerable.Range(0, 10)
                .Select(_ => s.Cclm.ReadMemoryAsync(0, offset, 3, crc32).AsTask())
                .ToList();
            s.LocalData.UnblockWrites();
            await Task.WhenAll(tasks);
            long sizeAfterFirstBatch = s.Cclm.CurrentSize;

            for (int i = 0; i < 10; i++)
                _ = await s.Cclm.ReadMemoryAsync(0, offset, 3, crc32);

            Assert.Equal(sizeAfterFirstBatch, s.Cclm.CurrentSize);
        }

        [Fact]
        public async Task TestReinitializeLocalCacheFilesRestoredNoExtraRemoteFetch()
        {
            var s = CreateStack();
            var (crc32, offset, _) = await CommitAsync(s.Storage, new byte[] { 1, 2, 3 });

            _ = await s.Cclm.ReadMemoryAsync(0, offset, 3, crc32);
            int fetchesBeforeReinit = s.RemoteData.NumberOfReadDataFile;
            await s.Cclm.InitializeAsync(
                new Persistence.StorageInitializationMetadata("test", NullLoggerFactory.Instance),
                new Meter("cclm-reinit"), DefaultCtx, CancellationToken.None);

            Assert.Equal(fetchesBeforeReinit, s.RemoteData.NumberOfReadDataFile);
        }

        [Fact]
        public async Task TestReinitializeCurrentSizeNotDoubleCountedForSameFile()
        {
            var s = CreateStack();
            var (crc32, offset, _) = await CommitAsync(s.Storage, new byte[] { 1, 2, 3 });

            _ = await s.Cclm.ReadMemoryAsync(0, offset, 3, crc32);
            long sizeAfterPopulate = s.Cclm.CurrentSize;

            await s.Cclm.InitializeAsync(
                new Persistence.StorageInitializationMetadata("test", NullLoggerFactory.Instance),
                new Meter("cclm-reinit2"), DefaultCtx, CancellationToken.None);

            Assert.Equal(sizeAfterPopulate, s.Cclm.CurrentSize);
        }

        [Fact]
        public async Task TestZombieFilesInLocalCacheRemovedOnReinit()
        {
            var s = CreateStack();

            var (crc32, offset, _) = await CommitAsync(s.Storage, new byte[] { 1, 2, 3 });
            _ = await s.Cclm.ReadMemoryAsync(0, offset, 3, crc32);
            long sizeAfterLegit = s.Cclm.CurrentSize;
            Assert.True(sizeAfterLegit > 0, "baseline must be non-zero or the size assertion is vacuous");

            s.LocalData.SetFileData(9999, new byte[] { 0xDE, 0xAD });

            await s.Cclm.InitializeAsync(
                new Persistence.StorageInitializationMetadata("test", NullLoggerFactory.Instance),
                new Meter("cclm-zombie"), DefaultCtx, CancellationToken.None);

            Assert.Equal(sizeAfterLegit, s.Cclm.CurrentSize);
            Assert.False(s.LocalData.DataFileExists(9999));
        }

        [Fact]
        public async Task TestWriteAndReadDataIntactRoundTrip()
        {
            var s = CreateStack();
            var (crc32, offset, _) = await WriteLocalAsync(s.Cclm, 1, new byte[] { 10, 20, 30 });
            var result = await s.Cclm.ReadMemoryAsync(1, offset, 3, crc32);
            Assert.Equal(new byte[] { 10, 20, 30 }, result.ToArray());
        }

        [Fact]
        public async Task TestTwoDistinctFilesReadBackCorrectData()
        {
            var s = CreateStack();
            var (crc1, off1, _) = await WriteLocalAsync(s.Cclm, 1, new byte[] { 1, 2, 3 });
            var (crc2, off2, _) = await WriteLocalAsync(s.Cclm, 2, new byte[] { 4, 5, 6 });

            var r1 = await s.Cclm.ReadMemoryAsync(1, off1, 3, crc1);
            var r2 = await s.Cclm.ReadMemoryAsync(2, off2, 3, crc2);

            Assert.Equal(new byte[] { 1, 2, 3 }, r1.ToArray());
            Assert.Equal(new byte[] { 4, 5, 6 }, r2.ToArray());
        }

        [Fact]
        public async Task TestReadAfterEvictAndRedownloadDataIntact()
        {
            var s = CreateStack();
            var (crc32, offset, _) = await CommitAsync(s.Storage, new byte[] { 9, 8, 7 });

            _ = await s.Cclm.ReadMemoryAsync(0, offset, 3, crc32);
            await s.Cclm.EvictDataFileAsync(0);

            var result = await s.Cclm.ReadMemoryAsync(0, offset, 3, crc32);
            Assert.Equal(new byte[] { 9, 8, 7 }, result.ToArray());
        }

        [Fact]
        public async Task TestCacheHitNoPersistentRead()
        {
            var s = CreateStack();
            var (crc32, offset, _) = await WriteLocalAsync(s.Cclm, 1, new byte[] { 1, 2, 3 });

            int remoteReadsBefore = s.RemoteData.NumberOfReadDataFile;
            int localReadsBefore  = s.LocalData.NumberOfReadMemory;

            _ = await s.Cclm.ReadMemoryAsync(1, offset, 3, crc32);

            Assert.Equal(remoteReadsBefore, s.RemoteData.NumberOfReadDataFile);
            Assert.Equal(localReadsBefore + 1, s.LocalData.NumberOfReadMemory);
        }

        [Fact]
        public async Task TestCacheMissOnePersistentRead()
        {
            var s = CreateStack();
            var (crc32, offset, _) = await CommitAsync(s.Storage, new byte[] { 1, 2, 3 });

            _ = await s.Cclm.ReadMemoryAsync(0, offset, 3, crc32);

            Assert.Equal(1, s.RemoteData.NumberOfReadDataFile);
        }

        [Fact]
        public async Task TestAfterDownloadSecondReadIsCacheHit()
        {
            var s = CreateStack();
            var (crc32, offset, _) = await CommitAsync(s.Storage, new byte[] { 1, 2, 3 });

            _ = await s.Cclm.ReadMemoryAsync(0, offset, 3, crc32);

            int remoteReadsAfterFirst = s.RemoteData.NumberOfReadDataFile;
            int localReadsAfterFirst  = s.LocalData.NumberOfReadMemory;

            _ = await s.Cclm.ReadMemoryAsync(0, offset, 3, crc32);

            Assert.Equal(remoteReadsAfterFirst, s.RemoteData.NumberOfReadDataFile);
            Assert.Equal(localReadsAfterFirst + 1, s.LocalData.NumberOfReadMemory);
        }

        [Fact]
        public async Task TestWaiterChainingSpaceReleasedOneByOneAllWritersComplete()
        {
            int fileSize = await MeasureFileSizeAsync(new byte[] { 1, 2, 3 });

            var s = CreateStack((long)fileSize * 2);

            await WriteLocalAsync(s.Cclm, 1, new byte[] { 1, 2, 3 });
            var (_, _, sz2) = await WriteLocalAsync(s.Cclm, 2, new byte[] { 4, 5, 6 });

            await s.Cclm.EvictDataFileAsync(1);
            var (_, _, sz3) = await WriteLocalAsync(s.Cclm, 3, new byte[] { 7, 8, 9 });

            Assert.Equal((long)(sz2 + sz3), s.Cclm.CurrentSize);
            Assert.True(s.Cclm.CurrentSize <= (long)fileSize * 2,
                $"Budget exceeded: {s.Cclm.CurrentSize} > {fileSize * 2}");
        }

        [Fact]
        public async Task TestDownloadFailureSizeNotLeaked()
        {
            var s = CreateStack();
            var (crc32, offset, _) = await CommitAsync(s.Storage, new byte[] { 1, 2, 3 });
            long baselineSize = s.Cclm.CurrentSize;

            s.RemoteData.InjectReadException(_ => new InvalidOperationException("always fail"));

            await Assert.ThrowsAsync<InvalidOperationException>(
                () => s.Cclm.ReadMemoryAsync(0, offset, 3, crc32).AsTask());

            s.RemoteData.InjectReadException(null);

            Assert.Equal(baselineSize, s.Cclm.CurrentSize);
        }

        [Fact]
        public async Task TestDownloadFailureThenRetryReturnsCorrectData()
        {
            var s = CreateStack();
            var (crc32, offset, _) = await CommitAsync(s.Storage, new byte[] { 1, 2, 3 });

            int calls = 0;
            s.RemoteData.InjectReadException(id =>
            {
                if (id == 0 && Interlocked.Increment(ref calls) == 1)
                    return new InvalidOperationException("transient");
                return null;
            });

            await Assert.ThrowsAsync<InvalidOperationException>(
                () => s.Cclm.ReadMemoryAsync(0, offset, 3, crc32).AsTask());

            s.RemoteData.InjectReadException(null);

            var result = await s.Cclm.ReadMemoryAsync(0, offset, 3, crc32);
            Assert.Equal(new byte[] { 1, 2, 3 }, result.ToArray());
            Assert.True(s.RemoteData.NumberOfReadDataFile >= 2);
        }

        [Fact]
        public async Task TestLruEvictionBudgetNotExceededAfterMultipleWrites()
        {
            int fileSize = await MeasureFileSizeAsync(new byte[] { 1, 2 });

            var s = CreateStack((long)fileSize * 2);

            for (ulong i = 1; i <= 4; i++)
                await WriteLocalAsync(s.Cclm, i, new byte[] { (byte)i, (byte)(i + 1) });

            Assert.True(s.Cclm.CurrentSize <= (long)fileSize * 2,
                $"Budget exceeded after 4 writes: {s.Cclm.CurrentSize} > {fileSize * 2}");
        }

        [Fact]
        public async Task TestConcurrentWritersAllCompleteEvenUnderBudgetPressure()
        {
            int fileSize = await MeasureFileSizeAsync(new byte[] { 1 });

            var s = CreateStack((long)fileSize * 2);
            await WriteLocalAsync(s.Cclm, 1, new byte[] { 1 });
            await WriteLocalAsync(s.Cclm, 2, new byte[] { 2 });

            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            var writers = Enumerable.Range(3, 4)
                .Select(i => Task.Run(() => WriteLocalAsync(s.Cclm, (ulong)i, new byte[] { (byte)i })))
                .ToList();

            await Task.WhenAll(writers).WaitAsync(cts.Token);

            Assert.True(s.Cclm.CurrentSize <= (long)fileSize * 2,
                $"Budget exceeded after concurrent writes: {s.Cclm.CurrentSize} > {fileSize * 2}");
        }

        [Fact]
        public async Task TestConcurrentWritersWhenAllRentedOverCommitInsteadOfDeadlock()
        {
            int fileSize = await MeasureFileSizeAsync(new byte[] { 1 });

            var s = CreateStack((long)fileSize);

            var (crc32, offset, _) = await CommitAsync(s.Storage, new byte[] { 1 });
            s.RemoteData.BlockReads();
            var download = s.Cclm.ReadMemoryAsync(0, offset, 1, crc32).AsTask();

            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            var write = WriteLocalAsync(s.Cclm, 1, new byte[] { 1 });

            s.RemoteData.UnblockReads();
            await Task.WhenAll(download, write).WaitAsync(cts.Token);
        }

        [Fact]
        public async Task TestDisposeAsyncAwaitsBackgroundTasksToComplete()
        {
            var s = CreateStack();
            var (crc32, offset, _) = await CommitAsync(s.Storage, new byte[] { 1, 2, 3 });

            s.LocalData.BlockWrites();

            _ = s.Cclm.ReadMemoryAsync(0, offset, 3, crc32).AsTask();

            var disposeTask = s.Cclm.DisposeAsync().AsTask();

            Assert.False(disposeTask.IsCompleted, "DisposeAsync finished too early and abandoned background tasks!");

            s.LocalData.UnblockWrites();

            await disposeTask.WaitAsync(TimeSpan.FromSeconds(5));
            Assert.True(disposeTask.IsCompleted);
        }

        [Fact]
        public async Task TestZeroByteFileAccountsCorrectlyAndToleratesEvictions()
        {
            var s = CreateStack();
            
            // Empty payload
            var (crc32, offset, fileSize) = await WriteLocalAsync(s.Cclm, 1, Array.Empty<byte>());

            Assert.Equal(fileSize, s.Cclm.CurrentSize);

            var result = await s.Cclm.ReadMemoryAsync(1, offset, 0, crc32);
            Assert.Equal(Array.Empty<byte>(), result.ToArray());

            await s.Cclm.EvictDataFileAsync(1);
            Assert.Equal(0, s.Cclm.CurrentSize);
        }

        [Fact]
        public async Task TestDictionaryUnlinkIsDeferredUntilPhysicalDeletionCompletes()
        {
            var s = CreateStack();
            var (crc32, offset, _) = await CommitAsync(s.Storage, new byte[] { 7, 7, 7 });

            _ = await s.Cclm.ReadMemoryAsync(0, offset, 3, crc32);

            s.LocalData.TryGetFileData(0, out var bytes);
            bytes![offset + 1] ^= 0xFF;

            var tcsDelete = new TaskCompletionSource();
            s.LocalData.InjectDeleteBlocker(0, tcsDelete);

            var badRead = s.Cclm.ReadMemoryAsync(0, offset, 3, crc32).AsTask();

            var freshRead = s.Cclm.ReadMemoryAsync(0, offset, 3, crc32).AsTask();
            
            Assert.False(badRead.IsCompleted);
            Assert.False(freshRead.IsCompleted);

            tcsDelete.SetResult();
            s.LocalData.InjectDeleteBlocker(null, null);

            var results = await Task.WhenAll(badRead, freshRead);
            Assert.Equal(new byte[] { 7, 7, 7 }, results[0].ToArray());
            Assert.Equal(new byte[] { 7, 7, 7 }, results[1].ToArray());
        }

        [Fact]
        public async Task TestRegisterNewFileAsyncFileWriteFailureExplicitlySweepsDisk()
        {
            var s = CreateStack();

            s.LocalData.InjectWriteException(_ => new InvalidOperationException("disk write faulted"));

            var blob = new BlobFileWriter((f) => { }, MemoryPool<byte>.Shared, GlobalMemoryManager.Instance);
            blob.Write(999, new SerializableObject(new byte[] { 4, 2 }));
            blob.Finish();

            await Assert.ThrowsAsync<InvalidOperationException>(
                () => s.Cclm.RegisterNewFileAsync(999, blob.Crc64, blob.FileSize, blob));

            Assert.Equal(0, s.Cclm.CurrentSize);
            Assert.False(s.LocalData.DataFileExists(999), "Physical file orphan left on disk after local IO exception!");
        }

        [Fact]
        public async Task TestProactiveEvictionLoopEvictsFilesWhenThresholdReached()
        {
            var fakeTime = new FakeTimeProvider();
            
            int fileSize = await MeasureFileSizeAsync(new byte[] { 1 });
            long maxSize = fileSize * 10;
            
            var s = CreateStack(maxSize, fakeTime);

            for (ulong i = 1; i <= 7; i++)
            {
                await WriteLocalAsync(s.Cclm, i, new byte[] { 1 });
            }
            
            Assert.Equal(fileSize * 7, s.Cclm.CurrentSize);

            fakeTime.Advance(TimeSpan.FromSeconds(6));

            Assert.Equal(fileSize * 7, s.Cclm.CurrentSize);

            for (ulong i = 8; i <= 9; i++)
            {
                await WriteLocalAsync(s.Cclm, i, new byte[] { 1 });
            }
            Assert.Equal(fileSize * 9, s.Cclm.CurrentSize);

            TaskCompletionSource evictionLoopSignal = new TaskCompletionSource();
            s.Cclm.SetEvictionLoopSignal_Test(evictionLoopSignal);
            fakeTime.Advance(TimeSpan.FromSeconds(20));

            await evictionLoopSignal.Task;

            Assert.True(s.Cclm.CurrentSize <= maxSize * 0.8, "Background loop failed to evict files.");
        }

        [Fact]
        public async Task TestReadAsyncGenericDeserializesPayload()
        {
            var s = CreateStack();
            var payload = new byte[] { 9, 8, 7 };
            var (crc32, offset, _) = await WriteLocalAsync(s.Cclm, 1, payload);

            var serializer = new LocalCacheTestSerializer();
            var result = await s.Cclm.ReadAsync(1, offset, payload.Length, crc32, serializer);

            Assert.NotNull(result);
            Assert.Equal(payload, result.Data);
        }

        [Fact]
        public async Task TestReadDataFileAsyncReturnsPipeReaderWhenCachedAndDefaultWhenMissing()
        {
            var s = CreateStack();
            var payload = new byte[] { 10, 11, 12 };
            var (_, _, fileSize) = await WriteLocalAsync(s.Cclm, 1, payload);

            var reader = await s.Cclm.ReadDataFileAsync(1, fileSize, CancellationToken.None);
            Assert.NotNull(reader);

            var readResult = await reader!.ReadAsync();
            Assert.True(readResult.Buffer.Length >= fileSize, "Returned buffer should cover the fileSize");
            reader.AdvanceTo(readResult.Buffer.End);

            var missReader = await s.Cclm.ReadDataFileAsync(999, fileSize, CancellationToken.None);
            Assert.Null(missReader);
        }
    }
}
