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

using FlowtideDotNet.Storage;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Persistence;
using FlowtideDotNet.Storage.Persistence.Reservoir;
using FlowtideDotNet.Storage.Persistence.Reservoir.Internal;
using FlowtideDotNet.Storage.Persistence.Reservoir.Internal.DiskReader;
using FlowtideDotNet.Storage.Persistence.Reservoir.LocalCache;
using FlowtideDotNet.Storage.Persistence.Reservoir.LocalDisk;
using FlowtideDotNet.Storage.Persistence.Reservoir.MemoryDisk;
using Microsoft.Extensions.Logging.Abstractions;
using System.Buffers;
using System.Buffers.Binary;
using System.Diagnostics.Metrics;
using System.IO.Pipelines;
using System.Runtime.CompilerServices;

namespace FlowtideDotNet.Core.Tests.Reservoir
{
    public sealed class LinuxFactAttribute : FactAttribute
    {
        public LinuxFactAttribute()
        {
            if (!OperatingSystem.IsLinux())
            {
                Skip = "Reads /proc, Linux only";
            }
        }
    }

    [Collection(nameof(ReservoirManyReadersCollection))]
    public class ReservoirFileReadTests
    {
        private const int FirstPage = 1000;
        private const int LastPage = 1039;
        private const int PageBytes = 8000;
        private const int SmallFile = 600;
        private static readonly TimeSpan Limit = TimeSpan.FromSeconds(30);

        private sealed class Folder : IDisposable
        {
            public string Path { get; } = System.IO.Path.GetFullPath($"./data/reservoirFileRead/{Guid.NewGuid():N}");

            public void Dispose()
            {
                if (Directory.Exists(Path))
                {
                    Directory.Delete(Path, true);
                }
            }
        }

        // A cache store that counts, fails or holds deletes
        private sealed class CacheStore : MemoryFileProvider
        {
            private int _deletes;

            public int Deletes => Volatile.Read(ref _deletes);

            public Exception? FailReadFile { get; set; }

            public bool ReadersFailToComplete { get; set; }

            public TaskCompletionSource? DeleteEntered { get; set; }

            public TaskCompletionSource? DeleteRelease { get; set; }

            public bool DeleteFinished { get; private set; }

            public override async Task DeleteDataFileAsync(ulong fileId, CancellationToken cancellationToken = default)
            {
                Interlocked.Increment(ref _deletes);
                DeleteEntered?.TrySetResult();
                if (DeleteRelease != null)
                {
                    await DeleteRelease.Task;
                }
                await base.DeleteDataFileAsync(fileId, cancellationToken);
                DeleteFinished = true;
            }

            public override async Task<PipeReader> ReadDataFileAsync(ulong fileId, int fileSize, CancellationToken cancellationToken = default)
            {
                if (FailReadFile != null)
                {
                    throw FailReadFile;
                }
                var reader = await base.ReadDataFileAsync(fileId, fileSize, cancellationToken);
                return ReadersFailToComplete ? new FailingReader(reader) : reader;
            }

            public async Task<bool> Holds(ulong fileId)
            {
                return (await GetStoredDataFileIdsAsync()).Contains(fileId);
            }

            public Task Lose(ulong fileId)
            {
                return base.DeleteDataFileAsync(fileId);
            }
        }

        // A reader whose completion throws
        private sealed class FailingReader : PipeReader
        {
            private readonly PipeReader _reader;

            public FailingReader(PipeReader reader)
            {
                _reader = reader;
            }

            public override void AdvanceTo(SequencePosition consumed)
            {
                _reader.AdvanceTo(consumed);
            }

            public override void AdvanceTo(SequencePosition consumed, SequencePosition examined)
            {
                _reader.AdvanceTo(consumed, examined);
            }

            public override void CancelPendingRead()
            {
                _reader.CancelPendingRead();
            }

            public override ValueTask<ReadResult> ReadAsync(CancellationToken cancellationToken = default)
            {
                return _reader.ReadAsync(cancellationToken);
            }

            public override bool TryRead(out ReadResult result)
            {
                return _reader.TryRead(out result);
            }

            public override void Complete(Exception? exception = null)
            {
                _reader.Complete(exception);
                throw new IOException("Completion failed in this test");
            }

            public override async ValueTask CompleteAsync(Exception? exception = null)
            {
                await _reader.CompleteAsync(exception);
                throw new IOException("Completion failed in this test");
            }
        }

        private static byte[] Bytes(int page)
        {
            var bytes = new byte[PageBytes];
            new Random(page).NextBytes(bytes);
            return bytes;
        }

        private static PipeReader Data(int size)
        {
            return PipeReader.Create(new ReadOnlySequence<byte>(new byte[size]));
        }

        private static async Task<ReservoirPersistentStorage> OpenWithOneFile(IReservoirStorageProvider provider, IReservoirStorageProvider? cache = null)
        {
            var storage = new ReservoirPersistentStorage(new ReservoirStorageOptions() { FileProvider = provider, CacheProvider = cache });
            await storage.InitializeAsync(new StorageInitializationMetadata("fileRead", NullLoggerFactory.Instance, GlobalMemoryManager.Instance));
            using var session = storage.CreateSession();
            for (int page = FirstPage; page <= LastPage; page++)
            {
                await session.Write(page, new SerializableObject(Bytes(page)));
            }
            await session.Commit();
            await storage.CheckpointAsync(new byte[] { 1 }, false);
            return storage;
        }

        // A cache that holds file 1 of 600 bytes
        private static async Task<ConcurrentLocalCacheManager> CacheWithOneFile(CacheStore store, TimeSpan? evictionInterval = null, long maxSize = 700)
        {
            var manager = new ConcurrentLocalCacheManager(null!, store, new MemoryFileProvider(), maxSize, new LocalCacheMetricValues(), evictionInterval: evictionInterval ?? TimeSpan.FromMinutes(10));
            await manager.RegisterNewFileAsync(1, 0, SmallFile, Data(SmallFile));
            return manager;
        }

        private static async Task<DataFileReader> ReadUpToThePages(PipeReader reader)
        {
            var dataFile = new DataFileReader(reader);
            await dataFile.Initialize();
            await dataFile.ReadPageIds();
            await dataFile.SkipPageOffsets();
            return dataFile;
        }

        private static Task<byte[]> ReadPage(DataFileReader dataFile, PageFileLocation location)
        {
            return Task.Run(async () => (await dataFile.ReadDataPage(location.Offset, location.Size)).ToArray()).WaitAsync(Limit);
        }

        private static int OpenDescriptors(string path)
        {
            int open = 0;
            foreach (var descriptor in Directory.GetFileSystemEntries("/proc/self/fd"))
            {
                var target = new FileInfo(descriptor).LinkTarget;
                if (target != null && target.StartsWith(path, StringComparison.Ordinal))
                {
                    open++;
                }
            }
            return open;
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task ACachedFileIsNotEvictedWhileItIsReadWhole(bool completeAsync)
        {
            using var folder = new Folder();
            var cache = Path.Join(folder.Path, "cache");
            using var storage = await OpenWithOneFile(new LocalDiskProvider(Path.Join(folder.Path, "store")), new LocalDiskProvider(cache));
            Assert.True(storage.PageFileLocationsForTests.TryGetValue(LastPage, out var last));
            Assert.True(storage.TryGetFileInformation(last.FileId, out var file));
            var cachedFile = Assert.Single(Directory.GetFiles(cache, $"dataFile_{last.FileId}.data", SearchOption.AllDirectories));
            long sizeBefore = storage.CacheProvider!.CurrentSize;

            var reader = await storage.CacheProvider.ReadDataFileAsync(last.FileId, file.FileSize);
            var dataFile = await ReadUpToThePages(reader);
            var evicted = storage.CacheProvider.EvictDataFileAsync(last.FileId);
            bool evictedUnderTheRead = evicted.IsCompleted;
            bool heldUnderTheRead = File.Exists(cachedFile);
            long sizeUnderTheRead = storage.CacheProvider.CurrentSize;
            var page = await ReadPage(dataFile, last);
            if (completeAsync)
            {
                await reader.CompleteAsync();
            }
            else
            {
                reader.Complete();
            }
            await evicted.WaitAsync(Limit);

            Assert.False(evictedUnderTheRead);
            Assert.True(heldUnderTheRead);
            Assert.Equal(sizeBefore, sizeUnderTheRead);
            Assert.Equal(Bytes(LastPage), page);
            Assert.False(File.Exists(cachedFile));
            Assert.Equal(sizeBefore - file.FileSize, storage.CacheProvider.CurrentSize);
            using var session = storage.CreateSession();
            Assert.Equal(Bytes(FirstPage), (await session.Read(FirstPage)).ToArray());
        }

        [LinuxFact]
        public async Task AFileDeletedUnderAWholeFileReadIsStillReadAndThenClosed()
        {
            using var folder = new Folder();
            var store = Path.Join(folder.Path, "store");
            var provider = new LocalDiskProvider(store);
            using var storage = await OpenWithOneFile(provider);
            Assert.True(storage.PageFileLocationsForTests.TryGetValue(LastPage, out var last));
            Assert.True(storage.TryGetFileInformation(last.FileId, out var file));
            var path = Assert.Single(Directory.GetFiles(store, $"dataFile_{last.FileId}.data", SearchOption.AllDirectories));

            var reader = await provider.ReadDataFileAsync(last.FileId, file.FileSize);
            var dataFile = await ReadUpToThePages(reader);
            await provider.DeleteDataFileAsync(last.FileId);
            bool deleted = !File.Exists(path);
            var page = await ReadPage(dataFile, last);
            int openDuringTheRead = OpenDescriptors(path);
            await reader.CompleteAsync();

            Assert.True(deleted);
            Assert.Equal(Bytes(LastPage), page);
            Assert.Equal(1, openDuringTheRead);
            Assert.Equal(0, OpenDescriptors(path));
        }

        [Theory]
        [InlineData("in the page offsets")]
        [InlineData("before the page")]
        [InlineData("inside the page")]
        public async Task ADataFileThatEndsEarlyIsAnErrorNotALoop(string end)
        {
            using var folder = new Folder();
            var store = Path.Join(folder.Path, "store");
            PageFileLocation last;
            using (var storage = await OpenWithOneFile(new LocalDiskProvider(store)))
            {
                Assert.True(storage.PageFileLocationsForTests.TryGetValue(LastPage, out last));
            }
            var bytes = File.ReadAllBytes(Directory.GetFiles(store, $"dataFile_{last.FileId}.data", SearchOption.AllDirectories)[0]);
            var whole = new DataFileReader(PipeReader.Create(new ReadOnlySequence<byte>(bytes)));
            await whole.Initialize();
            int pageIdBytes = (int)(await whole.ReadPageIds()).Length;
            int length = end switch
            {
                "in the page offsets" => 64 + pageIdBytes + 4,
                "before the page" => last.Offset - 100,
                _ => last.Offset + 100
            };

            var cut = new DataFileReader(PipeReader.Create(new ReadOnlySequence<byte>(bytes, 0, length)));
            await cut.Initialize();
            await cut.ReadPageIds();
            var read = Task.Run(async () =>
            {
                await cut.SkipPageOffsets();
                await cut.ReadDataPage(last.Offset, last.Size);
            }).WaitAsync(Limit);

            var error = await Assert.ThrowsAsync<InvalidOperationException>(() => read);
            Assert.Equal("Data file ended before all expected data was read.", error.Message);
        }

        [Theory]
        [InlineData(1000, 2000, "Reached end of file before reaching checkpoint start offset.")]
        [InlineData(100, 1000, "Reached end of file before reaching checkpoint registry start offset.")]
        public async Task ABundleThatEndsEarlyIsAnErrorNotALoop(long checkpointStart, long registryStart, string message)
        {
            var bytes = new byte[200];
            BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(0), MagicNumbers.DataFileMagicNumber);
            BinaryPrimitives.WriteInt16LittleEndian(bytes.AsSpan(4), 1);
            BinaryPrimitives.WriteInt16LittleEndian(bytes.AsSpan(6), 1);
            BinaryPrimitives.WriteInt64LittleEndian(bytes.AsSpan(24), checkpointStart);
            BinaryPrimitives.WriteInt64LittleEndian(bytes.AsSpan(32), registryStart);
            BinaryPrimitives.WriteInt64LittleEndian(bytes.AsSpan(40), registryStart + 1000);
            var reader = PipeReader.Create(new ReadOnlySequence<byte>(bytes));

            var read = Task.Run(async () => await BundleFileRegistryReader.ReadCheckpointDataAsync(reader, default)).WaitAsync(Limit);

            var error = await Assert.ThrowsAsync<InvalidOperationException>(() => read);
            Assert.Equal(message, error.Message);
        }

        [Fact]
        public async Task ARegistryReadThatFailsStillCompletesItsReader()
        {
            var stream = new MemoryStream(new byte[200]);
            var reader = PipeReader.Create(stream);

            await Assert.ThrowsAsync<InvalidOperationException>(async () => await BundleFileRegistryReader.ReadRegistryAsync(reader, GlobalMemoryManager.Instance, default));

            Assert.False(stream.CanRead);
        }

        [Fact]
        public async Task ACacheWriteWaitingForSpaceGoesOnWhenTheOnlyFileLeftIsReadWhole()
        {
            var store = new CacheStore();
            await using var manager = await CacheWithOneFile(store);
            for (int i = 0; i < 5; i++)
            {
                await (await manager.ReadDataFileAsync(1, SmallFile))!.CompleteAsync();
            }

            var write = manager.RegisterNewFileAsync(2, 0, SmallFile, Data(SmallFile));
            bool parked = !write.IsCompleted;
            var reader = await manager.ReadDataFileAsync(1, SmallFile);
            await write.WaitAsync(TimeSpan.FromSeconds(10));
            await reader!.CompleteAsync();

            Assert.True(parked);
            Assert.True(await store.Holds(2));
        }

        [Fact]
        public async Task ACacheWriteWaitingForSpaceIsServedBelowTheEvictionMark()
        {
            var store = new CacheStore();
            await using var manager = await CacheWithOneFile(store, TimeSpan.FromMilliseconds(50), 1000);
            for (int i = 0; i < 5; i++)
            {
                await (await manager.ReadDataFileAsync(1, SmallFile))!.CompleteAsync();
            }

            await manager.RegisterNewFileAsync(2, 0, SmallFile, Data(SmallFile)).WaitAsync(TimeSpan.FromSeconds(10));
            await manager.EvictDataFileAsync(1).WaitAsync(Limit);

            Assert.True(await store.Holds(2));
            Assert.False(await store.Holds(1));
        }

        [Fact]
        public async Task AReadThatCannotBeOpenedHoldsNoFile()
        {
            var store = new CacheStore();
            await using var manager = await CacheWithOneFile(store);
            store.FailReadFile = new ObjectDisposedException("file");

            await Assert.ThrowsAsync<ObjectDisposedException>(async () => await manager.ReadDataFileAsync(1, SmallFile));
            var evicted = manager.EvictDataFileAsync(1);

            Assert.True(evicted.IsCompleted);
            Assert.False(await store.Holds(1));
        }

        [Fact]
        public async Task TwoWholeFileReadsEachHoldTheFileAndItIsDeletedOnce()
        {
            var store = new CacheStore();
            await using var manager = await CacheWithOneFile(store);
            var first = await manager.ReadDataFileAsync(1, SmallFile);
            var second = await manager.ReadDataFileAsync(1, SmallFile);
            var evicted = manager.EvictDataFileAsync(1);

            first!.Complete();
            await first.CompleteAsync();
            first.Complete();
            bool heldByTheSecond = !evicted.IsCompleted && await store.Holds(1);
            await second!.CompleteAsync();
            await evicted.WaitAsync(Limit);

            Assert.True(heldByTheSecond);
            Assert.False(await store.Holds(1));
            Assert.Equal(1, store.Deletes);
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task AReaderWhoseCompletionFailsStillLetsGoOfTheFile(bool completeAsync)
        {
            var store = new CacheStore() { ReadersFailToComplete = true };
            await using var manager = await CacheWithOneFile(store);
            var reader = await manager.ReadDataFileAsync(1, SmallFile);
            var evicted = manager.EvictDataFileAsync(1);
            bool held = !evicted.IsCompleted;

            if (completeAsync)
            {
                await Assert.ThrowsAsync<IOException>(async () => await reader!.CompleteAsync());
            }
            else
            {
                Assert.Throws<IOException>(() => reader!.Complete());
            }
            await evicted.WaitAsync(Limit);

            Assert.True(held);
            Assert.False(await store.Holds(1));
        }

        [Fact]
        public async Task ACompletedReadHasDeletedItsFileWhenCompleteReturns()
        {
            var store = new CacheStore();
            await using var manager = await CacheWithOneFile(store);
            var reader = await manager.ReadDataFileAsync(1, SmallFile);
            var evicted = manager.EvictDataFileAsync(1);
            store.DeleteEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            store.DeleteRelease = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

            var deletedAtReturn = Task.Run(() =>
            {
                reader!.Complete();
                return store.DeleteFinished;
            });
            await store.DeleteEntered.Task.WaitAsync(Limit);
            store.DeleteRelease.SetResult();

            Assert.True(await deletedAtReturn.WaitAsync(Limit));
            await evicted.WaitAsync(Limit);
        }

        private sealed class OpenRead
        {
            public PipeReader? Reader { get; set; }
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static Task EvictUnderARead(ConcurrentLocalCacheManager manager, OpenRead read, out bool held)
        {
            read.Reader = manager.ReadDataFileAsync(1, SmallFile).AsTask().GetAwaiter().GetResult();
            var evicted = manager.EvictDataFileAsync(1);
            held = !evicted.IsCompleted;
            return evicted;
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static WeakReference Forget(OpenRead read)
        {
            var reader = new WeakReference(read.Reader);
            read.Reader = null;
            return reader;
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task AWholeFileReadThatIsNeverCompletedDoesNotHoldTheFileForever(bool loopStopped)
        {
            var store = new CacheStore();
            await using var manager = await CacheWithOneFile(store, TimeSpan.FromMilliseconds(50), 10_000);
            if (loopStopped)
            {
                await manager.SuspendBackgroundWorkAsync();
            }
            var read = new OpenRead();

            var evicted = EvictUnderARead(manager, read, out bool held);
            var reader = Forget(read);
            GC.Collect();
            GC.WaitForPendingFinalizers();
            await evicted.WaitAsync(Limit);

            Assert.True(held);
            Assert.False(reader.IsAlive);
            Assert.False(await store.Holds(1));
            Assert.Equal(1, store.Deletes);
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static Task<PipeReader?> FailAReadUnderAnOpenRead(ConcurrentLocalCacheManager manager, CacheStore store, OpenRead read, out bool held)
        {
            read.Reader = manager.ReadDataFileAsync(1, SmallFile).AsTask().GetAwaiter().GetResult();
            store.FailReadFile = new IOException("The cached copy cannot be opened in this test");
            var failed = manager.ReadDataFileAsync(1, SmallFile).AsTask();
            held = !failed.IsCompleted;
            return failed;
        }

        [Fact]
        public async Task AFailedCacheReadUnderAForgottenReadEndsWhileTheLoopIsStopped()
        {
            var store = new CacheStore();
            await using var manager = await CacheWithOneFile(store, TimeSpan.FromMilliseconds(50), 10_000);
            await manager.SuspendBackgroundWorkAsync();
            var read = new OpenRead();

            var failed = FailAReadUnderAnOpenRead(manager, store, read, out bool held);
            var reader = Forget(read);
            GC.Collect();
            GC.WaitForPendingFinalizers();
            var result = await failed.WaitAsync(Limit);

            Assert.True(held);
            Assert.False(reader.IsAlive);
            Assert.Null(result);
            Assert.False(await store.Holds(1));
        }

        [Fact]
        public async Task AForgottenReadFromBeforeARestartDeletesNothing()
        {
            var store = new CacheStore();
            await using var manager = await CacheWithOneFile(store, TimeSpan.FromMilliseconds(50), 10_000);
            var read = new OpenRead();

            var evicted = EvictUnderARead(manager, read, out bool held);
            await store.Lose(1);
            using var meter = new Meter("fileRead");
            await manager.InitializeAsync(new StorageInitializationMetadata("fileRead", NullLoggerFactory.Instance, GlobalMemoryManager.Instance), meter, new StorageProviderContext("fileRead", "1", GlobalMemoryManager.Instance), default);
            await manager.RegisterNewFileAsync(1, 0, SmallFile, Data(SmallFile));
            var reader = Forget(read);
            GC.Collect();
            GC.WaitForPendingFinalizers();
            await evicted.WaitAsync(Limit);
            for (int pass = 0; pass < 2; pass++)
            {
                var loop = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                manager.SetEvictionLoopSignal_Test(loop);
                await loop.Task.WaitAsync(Limit);
            }

            Assert.True(held);
            Assert.False(reader.IsAlive);
            Assert.True(await store.Holds(1));
            Assert.Equal(0, store.Deletes);
            Assert.Equal(SmallFile, manager.CurrentSize);
        }
    }
}
