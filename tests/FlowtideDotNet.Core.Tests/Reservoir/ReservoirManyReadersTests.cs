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
using FlowtideDotNet.Storage.Exceptions;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Persistence;
using FlowtideDotNet.Storage.Persistence.Reservoir;
using FlowtideDotNet.Storage.Persistence.Reservoir.Internal;
using FlowtideDotNet.Storage.Persistence.Reservoir.Internal.DiskReader;
using FlowtideDotNet.Storage.Persistence.Reservoir.LocalDisk;
using FlowtideDotNet.Storage.Persistence.Reservoir.MemoryDisk;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.StateManager.Internal;
using Microsoft.Extensions.Logging.Abstractions;
using System.Buffers;
using System.IO.Hashing;
using System.IO.Pipelines;

namespace FlowtideDotNet.Core.Tests.Reservoir
{
    [CollectionDefinition(nameof(ReservoirManyReadersCollection), DisableParallelization = true)]
    public class ReservoirManyReadersCollection
    {
    }

    [Collection(nameof(ReservoirManyReadersCollection))]
    public class ReservoirManyReadersTests
    {
        private static readonly TimeSpan Limit = TimeSpan.FromSeconds(30);

        private sealed class BytesPage : ICacheObject
        {
            private int _rentCount = 1;

            public BytesPage(byte[] bytes)
            {
                Bytes = bytes;
            }

            public byte[] Bytes { get; }

            public bool RemovedFromCache { get; set; }

            public int RentCount => Volatile.Read(ref _rentCount);

            public bool TryRent()
            {
                Interlocked.Increment(ref _rentCount);
                return true;
            }

            public void Return()
            {
                Interlocked.Decrement(ref _rentCount);
            }

            public bool TryReclaimForEviction()
            {
                return Interlocked.CompareExchange(ref _rentCount, 0, 1) == 1;
            }

            public void EnterWriteLock()
            {
                Monitor.Enter(this);
            }

            public void ExitWriteLock()
            {
                Monitor.Exit(this);
            }
        }

        private sealed class BytesSerializer : IStateSerializer<BytesPage>
        {
            public Action? DecodeHook { get; set; }

            public BytesPage Deserialize(ReadOnlySequence<byte> bytes, int length)
            {
                DecodeHook?.Invoke();
                return new BytesPage(bytes.ToArray());
            }

            public ICacheObject DeserializeCacheObject(ReadOnlySequence<byte> bytes, int length)
            {
                return Deserialize(bytes, length);
            }

            public void Serialize(in IBufferWriter<byte> bufferWriter, in BytesPage value)
            {
                bufferWriter.Write(value.Bytes);
            }

            public void Serialize(in IBufferWriter<byte> bufferWriter, in ICacheObject value)
            {
                Serialize(bufferWriter, (BytesPage)value);
            }

            public void ClearTemporaryAllocations()
            {
            }

            public Task CheckpointAsync<TMetadata>(IStateSerializerCheckpointWriter checkpointWriter, StateClientMetadata<TMetadata> metadata) where TMetadata : IStorageMetadata
            {
                return Task.CompletedTask;
            }

            public Task InitializeAsync<TMetadata>(IStateSerializerInitializeReader reader, StateClientMetadata<TMetadata> metadata) where TMetadata : IStorageMetadata
            {
                return Task.CompletedTask;
            }

            public void Dispose()
            {
            }
        }

        // Holds the first caller until released
        private sealed class Gate
        {
            private int _armed = 1;
            private int _calls;
            private int _timedOut;

            public ManualResetEventSlim Entered { get; } = new ManualResetEventSlim(false);

            public ManualResetEventSlim Release { get; } = new ManualResetEventSlim(false);

            public int Calls => Volatile.Read(ref _calls);

            // True when a held caller was never released
            public bool TimedOut => Volatile.Read(ref _timedOut) == 1;

            public void Pass()
            {
                Interlocked.Increment(ref _calls);
                if (Interlocked.Exchange(ref _armed, 0) == 1)
                {
                    Entered.Set();
                    if (!Release.Wait(TimeSpan.FromMinutes(2)))
                    {
                        Volatile.Write(ref _timedOut, 1);
                    }
                }
            }
        }

        private sealed class Folder : IDisposable
        {
            public string Path { get; } = System.IO.Path.GetFullPath($"./data/reservoirManyReaders/{Guid.NewGuid():N}");

            public void Dispose()
            {
                if (Directory.Exists(Path))
                {
                    Directory.Delete(Path, true);
                }
            }
        }

        // Runs a hook before every page read
        private sealed class HookedMemoryProvider : MemoryFileProvider
        {
            public Action<ulong>? BeforeRead { get; set; }

            public bool Asynchronous { get; set; }

            public override ValueTask<T> ReadAsync<T>(ulong fileId, int offset, int length, uint crc32, IStateSerializer<T> stateSerializer, CancellationToken cancellationToken = default)
            {
                if (Asynchronous)
                {
                    return ReadLater(fileId, offset, length, crc32, stateSerializer, cancellationToken);
                }
                BeforeRead?.Invoke(fileId);
                return base.ReadAsync(fileId, offset, length, crc32, stateSerializer, cancellationToken);
            }

            private async ValueTask<T> ReadLater<T>(ulong fileId, int offset, int length, uint crc32, IStateSerializer<T> stateSerializer, CancellationToken cancellationToken) where T : ICacheObject
            {
                await Task.Yield();
                BeforeRead?.Invoke(fileId);
                return await base.ReadAsync(fileId, offset, length, crc32, stateSerializer, cancellationToken);
            }
        }

        private sealed class TestAllocator : IMemoryAllocator
        {
            public int Allocations { get; private set; }

            public bool Fail { get; set; }

            public IMemoryOwner<byte> Allocate(int size, int alignment)
            {
                if (Fail)
                {
                    throw new OutOfMemoryException("No memory in this test");
                }
                Allocations++;
                return GlobalMemoryManager.Instance.Allocate(size, alignment);
            }

            public IMemoryOwner<byte> Realloc(IMemoryOwner<byte> memory, int size, int alignment)
            {
                throw new NotSupportedException();
            }

            public void RegisterAllocationToMetrics(int size)
            {
            }

            public void RegisterFreeToMetrics(int size)
            {
            }
        }

        private static byte[] Bytes(int page)
        {
            return Bytes(page, 200 + page * 37 % 3000);
        }

        private static byte[] Bytes(int page, int length)
        {
            var bytes = new byte[length];
            new Random(page).NextBytes(bytes);
            return bytes;
        }

        private static async Task<ReservoirPersistentStorage> Open(IReservoirStorageProvider provider, int maxFileSize = 64 * 1024 * 1024, IReservoirStorageProvider? cache = null)
        {
            var storage = new ReservoirPersistentStorage(new ReservoirStorageOptions() { FileProvider = provider, CacheProvider = cache, MaxFileSize = maxFileSize });
            await storage.InitializeAsync(new StorageInitializationMetadata("manyReaders", NullLoggerFactory.Instance, GlobalMemoryManager.Instance));
            return storage;
        }

        // On another thread, so a blocked caller times out
        private static Task<T> Run<T>(Func<Task<T>> operation)
        {
            return Task.Run(operation).WaitAsync(Limit);
        }

        private static Task Run(Func<Task> operation)
        {
            return Task.Run(operation).WaitAsync(Limit);
        }

        private static Task<BytesPage> Read(ReservoirPersistentStorage storage, long page, Action? decodeHook = null)
        {
            return Task.Run(async () => await storage.ReadConcurrentAsync(page, new BytesSerializer() { DecodeHook = decodeHook }));
        }

        private static async Task StoreInAFile(ReservoirPersistentStorage storage, IPersistentStorageSession session, params int[] pages)
        {
            foreach (var page in pages)
            {
                await session.Write(page, new SerializableObject(Bytes(page)));
            }
            await session.Commit();
            await storage.CheckpointAsync(new byte[] { 1 }, false);
            Assert.False(storage.TryGetTemporaryLocation(pages[0], out _));
        }

        // The 12 KB page keeps 101 and 102 apart
        private static async Task StoreTwoPagesApart(ReservoirPersistentStorage storage, IPersistentStorageSession session)
        {
            await session.Write(101, new SerializableObject(Bytes(101)));
            await session.Write(150, new SerializableObject(Bytes(150, 12000)));
            await session.Write(102, new SerializableObject(Bytes(102)));
            await session.Commit();
            await storage.CheckpointAsync(new byte[] { 1 }, false);
            Assert.False(storage.TryGetTemporaryLocation(101, out _));
        }

        private static int OpenDescriptors(string fileName)
        {
            int open = 0;
            foreach (var descriptor in Directory.GetFileSystemEntries("/proc/self/fd"))
            {
                var target = new FileInfo(descriptor).LinkTarget;
                if (target != null && target.Contains(fileName, StringComparison.Ordinal))
                {
                    open++;
                }
            }
            return open;
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task AReaderPausedInDecodeHoldsUpNoOtherReaderAndNoWriter(bool onDisk)
        {
            if (onDisk && !OperatingSystem.IsLinux())
            {
                return;
            }
            using var folder = new Folder();
            using var storage = await Open(onDisk ? new LocalDiskProvider(folder.Path) : new MemoryFileProvider());
            using var session = storage.CreateSession();
            await StoreTwoPagesApart(storage, session);
            var gate = new Gate();
            BytesPage other;
            BytesPage same;
            BytesPage throughSession;
            BytesPage written;

            var paused = Read(storage, 101, gate.Pass);
            try
            {
                Assert.True(gate.Entered.Wait(Limit));
                other = await Run(() => Read(storage, 102));
                same = await Run(() => Read(storage, 101));
                throughSession = await Run(async () => await session.Read(102, new BytesSerializer()));
                await Run(() => StoreInAFile(storage, session, 103));
                written = await Run(() => Read(storage, 103));
            }
            finally
            {
                gate.Release.Set();
            }
            var first = await paused.WaitAsync(Limit);

            Assert.False(gate.TimedOut);
            Assert.Equal(Bytes(102), other.Bytes);
            Assert.Equal(Bytes(101), same.Bytes);
            Assert.Equal(Bytes(102), throughSession.Bytes);
            Assert.Equal(Bytes(103), written.Bytes);
            Assert.Equal(Bytes(101), first.Bytes);
        }

        [Fact]
        public async Task AReaderPausedInTheDiskReadHoldsUpNoOtherReaderAndNoWriter()
        {
            if (!OperatingSystem.IsLinux())
            {
                return;
            }
            using var folder = new Folder();
            var provider = new LocalDiskProvider(folder.Path);
            using var storage = await Open(provider);
            using var session = storage.CreateSession();
            await StoreTwoPagesApart(storage, session);
            var gate = new Gate();
            provider.ReadHookForTests = gate.Pass;
            BytesPage other;
            BytesPage same;
            BytesPage throughSession;
            BytesPage written;

            var paused = Read(storage, 101);
            try
            {
                Assert.True(gate.Entered.Wait(Limit));
                other = await Run(() => Read(storage, 102));
                same = await Run(() => Read(storage, 101));
                throughSession = await Run(async () => await session.Read(102, new BytesSerializer()));
                await Run(() => StoreInAFile(storage, session, 103));
                written = await Run(() => Read(storage, 103));
            }
            finally
            {
                gate.Release.Set();
            }
            var first = await paused.WaitAsync(Limit);

            Assert.False(gate.TimedOut);
            Assert.Equal(Bytes(102), other.Bytes);
            Assert.Equal(Bytes(101), same.Bytes);
            Assert.Equal(Bytes(102), throughSession.Bytes);
            Assert.Equal(Bytes(103), written.Bytes);
            Assert.Equal(Bytes(101), first.Bytes);
        }

        [Fact]
        public async Task AFileDeletedUnderAReadIsStillReadAndThenClosed()
        {
            if (!OperatingSystem.IsLinux())
            {
                return;
            }
            using var folder = new Folder();
            var provider = new LocalDiskProvider(folder.Path);
            using var storage = await Open(provider);
            using var session = storage.CreateSession();
            await StoreTwoPagesApart(storage, session);
            ulong fileId = storage.PageFileLocationsForTests[101].FileId;
            var path = Directory.GetFiles(folder.Path, "*.data", SearchOption.AllDirectories).Single();
            var gate = new Gate();
            provider.ReadHookForTests = gate.Pass;
            int openWhileRead;

            var paused = Read(storage, 101);
            try
            {
                Assert.True(gate.Entered.Wait(Limit));
                await Run(() => provider.DeleteDataFileAsync(fileId));
                openWhileRead = OpenDescriptors(path);
            }
            finally
            {
                gate.Release.Set();
            }
            var page = await paused.WaitAsync(Limit);

            Assert.False(gate.TimedOut);
            Assert.False(File.Exists(path));
            Assert.Equal(1, openWhileRead);
            Assert.Equal(Bytes(101), page.Bytes);
            Assert.Equal(0, OpenDescriptors(path));
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task APageMovedBetweenTheLookupAndTheReadIsReadWhereItIsNow(bool asynchronous)
        {
            var provider = new HookedMemoryProvider() { Asynchronous = asynchronous };
            using var storage = await Open(provider);
            using var session = storage.CreateSession();
            await StoreInAFile(storage, session, 101, 102);
            var stored = storage.PageFileLocationsForTests[101];
            ulong movedTo = stored.FileId + 1000;
            int reads = 0;
            provider.BeforeRead = file =>
            {
                if (++reads == 1)
                {
                    Assert.True(provider.TryGetFileData(file, out var bytes));
                    provider.SetFileData(movedTo, bytes);
                    storage.PageFileLocationsForTests[101] = new PageFileLocation() { FileId = movedTo, Offset = stored.Offset, Size = stored.Size, Crc32 = stored.Crc32 };
                    provider.DeleteDataFileAsync(file).GetAwaiter().GetResult();
                }
            };

            var page = await storage.ReadConcurrentAsync(101, new BytesSerializer());
            var wrongCrc = storage.PageFileLocationsForTests[101];
            wrongCrc.Crc32++;
            storage.PageFileLocationsForTests[101] = wrongCrc;
            await Assert.ThrowsAsync<FlowtideChecksumMismatchException>(async () => await storage.ReadConcurrentAsync(101, new BytesSerializer()));

            Assert.Equal(Bytes(101), page.Bytes);
            Assert.Equal(3, reads);
        }

        [Fact]
        public async Task PagesOfEverySizeAreReadFromDisk()
        {
            if (!OperatingSystem.IsLinux())
            {
                return;
            }
            using var folder = new Folder();
            var path = Path.Join(folder.Path, "file.data");
            var bytes = Bytes(5, 3_000_000);
            using var manager = new LocalDiskReadManager(GlobalMemoryManager.Instance);
            await manager.Write(path, PipeReader.Create(new ReadOnlySequence<byte>(bytes)));
            (int offset, int length)[] reads = [(0, 100), (4000, 200), (5000, 70_000), (100, 300), (65_536, 1_500_000), (2_999_000, 1000), (1, 2_999_999), (4096, 4096)];

            foreach (var (offset, length) in reads)
            {
                uint crc = Crc32.HashToUInt32(bytes.AsSpan(offset, length));
                var page = await manager.Read(path, offset, length, crc, new BytesSerializer());
                var raw = await manager.Read(path, offset, length, crc);
                Assert.True(bytes.AsSpan(offset, length).SequenceEqual(page.Bytes));
                Assert.True(bytes.AsSpan(offset, length).SequenceEqual(raw.Span));
            }
            await Assert.ThrowsAsync<FlowtideChecksumMismatchException>(async () => await manager.Read(path, 0, 100, 1, new BytesSerializer()));
        }

        [Fact]
        public async Task AFailedBufferAllocationLeavesNoFileOpen()
        {
            if (!OperatingSystem.IsLinux())
            {
                return;
            }
            using var folder = new Folder();
            var path = Path.Join(folder.Path, "file.data");
            var allocator = new TestAllocator();
            using var manager = new LocalDiskReadManager(allocator);
            await manager.Write(path, PipeReader.Create(new ReadOnlySequence<byte>(Bytes(1, 4096))));
            allocator.Fail = true;

            await Assert.ThrowsAsync<OutOfMemoryException>(async () => await manager.Read(path, 0, 100, 0));
            int openBeforeTheDrop = OpenDescriptors(path);
            manager.DropFile(path);

            Assert.Equal(1, openBeforeTheDrop);
            Assert.Equal(0, OpenDescriptors(path));
        }

        [Fact]
        public void AVeryLargeReadCostsNoKeptBuffer()
        {
            var allocator = new TestAllocator();
            using var pool = new AlignedBufferPool(allocator);
            pool.Return(pool.Rent(4096));

            pool.Return(pool.Rent(2 * 1024 * 1024));
            int afterTheLargeRead = allocator.Allocations;
            pool.Return(pool.Rent(4096));

            Assert.Equal(2, afterTheLargeRead);
            Assert.Equal(2, allocator.Allocations);
        }

        [Fact]
        public async Task APageCopiedWhileItsFileRollsIsCopiedAgain()
        {
            using var storage = await Open(new MemoryFileProvider(), 4096);
            using var session = storage.CreateSession();
            await session.Write(101, new SerializableObject(Bytes(101)));
            Assert.True(storage.TryGetTemporaryLocation(101, out var location));
            var gate = new Gate();
            location.file.CopyHookForTests = gate.Pass;

            var paused = Read(storage, 101);
            try
            {
                Assert.True(gate.Entered.Wait(Limit));
                await Run(() => session.Write(102, new SerializableObject(new byte[8000])));
            }
            finally
            {
                gate.Release.Set();
            }
            var page = await paused.WaitAsync(Limit);

            Assert.False(gate.TimedOut);
            Assert.Equal(Bytes(101), page.Bytes);
            Assert.Equal(2, gate.Calls);
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task AnErrorInACopyIsOnlyForgivenWhenTheFileRolled(bool rolled)
        {
            using var storage = await Open(new MemoryFileProvider(), 4096);
            using var session = storage.CreateSession();
            await session.Write(101, new SerializableObject(Bytes(101)));
            Assert.True(storage.TryGetTemporaryLocation(101, out var location));
            int copies = 0;
            location.file.CopyHookForTests = () =>
            {
                if (++copies == 1)
                {
                    if (rolled)
                    {
                        session.Write(102, new SerializableObject(new byte[8000])).GetAwaiter().GetResult();
                    }
                    throw new InvalidDataException("A length torn by a roll");
                }
            };

            if (rolled)
            {
                var page = await storage.ReadConcurrentAsync(101, new BytesSerializer());
                Assert.Equal(Bytes(101), page.Bytes);
                Assert.Equal(2, copies);
            }
            else
            {
                await Assert.ThrowsAsync<InvalidDataException>(async () => await storage.ReadConcurrentAsync(101, new BytesSerializer()));
                Assert.Equal(1, copies);
            }
        }

        [Fact]
        public async Task ACopyWaitsWhileTheLayoutIsRewritten()
        {
            using var storage = await Open(new MemoryFileProvider(), 4096);
            using var session = storage.CreateSession();
            await session.Write(101, new SerializableObject(Bytes(101)));
            Assert.True(storage.TryGetTemporaryLocation(101, out var location));
            var gate = new Gate();
            int copies = 0;
            using var waiting = new ManualResetEventSlim(false);
            location.file.LayoutHookForTests = gate.Pass;
            location.file.CopyHookForTests = () => Interlocked.Increment(ref copies);
            location.file.CopyWaitHookForTests = waiting.Set;
            bool readWhileRewritten;
            int copiesWhileRewritten;

            var writer = Task.Run(() => session.Write(102, new SerializableObject(new byte[8000])));
            Task<BytesPage> reader;
            try
            {
                Assert.True(gate.Entered.Wait(Limit));
                reader = Read(storage, 101);
                Assert.True(waiting.Wait(Limit));
                readWhileRewritten = reader.IsCompleted;
                copiesWhileRewritten = Volatile.Read(ref copies);
            }
            finally
            {
                gate.Release.Set();
            }
            await writer.WaitAsync(Limit);
            var page = await reader.WaitAsync(Limit);

            Assert.False(gate.TimedOut);
            Assert.False(readWhileRewritten);
            Assert.Equal(0, copiesWhileRewritten);
            Assert.Equal(Bytes(101), page.Bytes);
        }

        [Fact]
        public async Task ADeletedPageIsReadUntilACheckpointRemovesIt()
        {
            using var storage = await Open(new MemoryFileProvider());
            using var session = storage.CreateSession();
            await StoreInAFile(storage, session, 101, 102);
            await session.Delete(102);
            await session.Commit();

            await Assert.ThrowsAsync<FlowtidePersistentStorageException>(async () => await session.Read(102, new BytesSerializer()));
            var beforeTheCheckpoint = await storage.ReadConcurrentAsync(102, new BytesSerializer());
            await storage.CheckpointAsync(new byte[] { 2 }, false);
            await Assert.ThrowsAsync<FlowtidePersistentStorageException>(async () => await storage.ReadConcurrentAsync(102, new BytesSerializer()));

            Assert.Equal(Bytes(102), beforeTheCheckpoint.Bytes);
        }

        [Fact]
        public async Task APageInNeitherMapIsLookedForOnceMore()
        {
            using var storage = await Open(new MemoryFileProvider());
            using var session = storage.CreateSession();
            storage.SecondReadPassHookForTests = () => session.Write(105, new SerializableObject(Bytes(105))).GetAwaiter().GetResult();

            var page = await storage.ReadConcurrentAsync(105, new BytesSerializer());
            storage.SecondReadPassHookForTests = null;
            var missing = await Assert.ThrowsAsync<FlowtidePersistentStorageException>(async () => await storage.ReadConcurrentAsync(106, new BytesSerializer()));

            Assert.Equal(Bytes(105), page.Bytes);
            Assert.Contains("Key 106 not found", missing.Message);
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task WrongBytesAreReadOnceMoreWhereThePageIsNow(bool rightTheSecondTime)
        {
            using var storage = await Open(new MemoryFileProvider());
            using var session = storage.CreateSession();
            await session.Write(101, new SerializableObject(Bytes(101)));
            Assert.True(storage.TryGetTemporaryLocation(101, out var right));
            var wrong = right;
            wrong.crc32++;
            int copies = 0;
            right.file.CopyHookForTests = () =>
            {
                if (++copies == 1 && rightTheSecondTime)
                {
                    storage.AddTemporaryLocation(101, right);
                }
            };
            storage.AddTemporaryLocation(101, wrong);

            if (rightTheSecondTime)
            {
                var page = await storage.ReadConcurrentAsync(101, new BytesSerializer());
                Assert.Equal(Bytes(101), page.Bytes);
            }
            else
            {
                await Assert.ThrowsAsync<FlowtideChecksumMismatchException>(async () => await storage.ReadConcurrentAsync(101, new BytesSerializer()));
            }
            Assert.Equal(2, copies);
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task ManyReadersGetEveryPageRightWhileFilesRoll(bool onDisk)
        {
            if (onDisk && !OperatingSystem.IsLinux())
            {
                return;
            }
            using var folder = new Folder();
            using var storage = await Open(onDisk ? new LocalDiskProvider(folder.Path) : new MemoryFileProvider(), 64 * 1024);
            using var session = storage.CreateSession();
            await StoreInAFile(storage, session, Enumerable.Range(1000, 200).ToArray());
            int stored = 1200;
            int notYetInAFile = 0;
            using var start = new ManualResetEventSlim(false);
            var readers = new List<Task<int>>();
            for (int r = 0; r < 8; r++)
            {
                int seed = r;
                readers.Add(Task.Run(async () =>
                {
                    var random = new Random(seed);
                    var serializer = new BytesSerializer();
                    int wrong = 0;
                    start.Wait();
                    for (int i = 0; i < 2000; i++)
                    {
                        int last = Volatile.Read(ref stored);
                        int page = random.Next(2) == 0 ? random.Next(1000, last) : random.Next(Math.Max(1000, last - 20), last);
                        if (storage.TryGetTemporaryLocation(page, out _))
                        {
                            Interlocked.Increment(ref notYetInAFile);
                        }
                        var read = await storage.ReadConcurrentAsync(page, serializer);
                        if (!read.Bytes.AsSpan().SequenceEqual(Bytes(page)))
                        {
                            wrong++;
                        }
                    }
                    return wrong;
                }));
            }
            start.Set();
            for (int page = 1200; page < 1700; page++)
            {
                await session.Write(page, new SerializableObject(Bytes(page)));
                Volatile.Write(ref stored, page + 1);
            }
            var results = await Task.WhenAll(readers).WaitAsync(TimeSpan.FromMinutes(2));

            Assert.All(results, wrong => Assert.Equal(0, wrong));
            Assert.True(notYetInAFile > 0);
        }

        [Fact]
        public async Task OnlyProvidersThatReadWithoutAFileLockSaySo()
        {
            using var folder = new Folder();
            using var memory = await Open(new MemoryFileProvider());
            using var disk = await Open(new LocalDiskProvider(Path.Join(folder.Path, "disk")));
            using var other = await Open(new HookedMemoryProvider());
            using var cached = await Open(new MemoryFileProvider(), cache: new LocalDiskProvider(Path.Join(folder.Path, "cache")));

            Assert.True(memory.ReadsPagesWithoutFileLock);
            Assert.Equal(OperatingSystem.IsLinux(), disk.ReadsPagesWithoutFileLock);
            Assert.False(other.ReadsPagesWithoutFileLock);
            Assert.False(cached.ReadsPagesWithoutFileLock);
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task AMissingCachedFileIsFetchedAgain(bool decodedReadFirst)
        {
            if (!OperatingSystem.IsLinux())
            {
                return;
            }
            using var folder = new Folder();
            var store = Path.Join(folder.Path, "store");
            var cache = Path.Join(folder.Path, "cache");
            using (var storage = await Open(new LocalDiskProvider(store), 256, new LocalDiskProvider(cache)))
            {
                using var session = storage.CreateSession();
                await session.Write(101, new SerializableObject(Bytes(101)));
                await session.Commit();
                await storage.CheckpointAsync(new byte[] { 1 }, false);
                Assert.Equal(Bytes(101), (await session.Read(101)).ToArray());
            }

            using (var storage = await Open(new LocalDiskProvider(store), 256, new LocalDiskProvider(cache)))
            {
                var cachedFiles = Directory.GetFiles(cache, "dataFile_*.data", SearchOption.AllDirectories);
                foreach (var file in cachedFiles)
                {
                    File.Delete(file);
                }
                using var session = storage.CreateSession();
                ReadOnlyMemory<byte> raw;
                BytesPage page;
                if (decodedReadFirst)
                {
                    page = await storage.ReadConcurrentAsync(101, new BytesSerializer());
                    raw = await session.Read(101);
                }
                else
                {
                    raw = await session.Read(101);
                    page = await storage.ReadConcurrentAsync(101, new BytesSerializer());
                }

                Assert.NotEmpty(cachedFiles);
                Assert.Equal(Bytes(101), raw.ToArray());
                Assert.Equal(Bytes(101), page.Bytes);
            }
        }
    }
}
