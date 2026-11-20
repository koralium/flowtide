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
using FlowtideDotNet.Storage.Persistence.Reservoir.MemoryDisk;
using Microsoft.Extensions.Logging.Abstractions;
using System.Buffers;
using System.Buffers.Binary;
using System.IO.Pipelines;

namespace FlowtideDotNet.Core.Tests.Reservoir
{
    [Collection(nameof(ReservoirManyReadersCollection))]
    public class ReservoirDamagedFileRestartTests
    {
        private const int Checkpoints = 4;
        private const int FirstPage = 1001;
        private static readonly TimeSpan Limit = TimeSpan.FromSeconds(30);

        // A store that hands out one file cut short
        private sealed class CuttingStore : MemoryFileProvider, IReservoirStorageProvider
        {
            public ulong CutFile { get; set; }

            public int CutLength { get; set; } = -1;

            public int CutCopies { get; private set; }

            public override async Task<PipeReader> ReadDataFileAsync(ulong fileId, int fileSize, CancellationToken cancellationToken = default)
            {
                if (fileId != CutFile || CutLength < 0)
                {
                    return await base.ReadDataFileAsync(fileId, fileSize, cancellationToken);
                }
                if (CutLength == int.MaxValue)
                {
                    CutCopies++;
                    return new UnreadableReader();
                }
                var bytes = await ReadWhole(this, fileId, true);
                CutCopies++;
                return PipeReader.Create(new ReadOnlySequence<byte>(bytes, 0, Math.Min(CutLength, bytes.Length)));
            }

            public Task<PipeReader> ReadUncut(ulong fileId)
            {
                return base.ReadDataFileAsync(fileId, 0);
            }

            public long CutCheckpoint { get; set; } = -1;

            public override async Task<PipeReader> ReadCheckpointFileAsync(CheckpointId checkpointVersion, CancellationToken cancellationToken = default)
            {
                var reader = await base.ReadCheckpointFileAsync(checkpointVersion, cancellationToken);
                if ((long)checkpointVersion.Version != CutCheckpoint || CutLength < 0)
                {
                    return reader;
                }
                var bytes = await ReadWhole(reader);
                CutCopies++;
                CutStream = new MemoryStream(bytes, 0, Math.Min(CutLength, bytes.Length), false);
                return PipeReader.Create(CutStream);
            }

            public MemoryStream? CutStream { get; private set; }

            public bool DeletedWhileItsReaderWasOpen { get; private set; }

            public new Task DeleteCheckpointFileAsync(CheckpointId checkpointVersion, CancellationToken cancellationToken = default)
            {
                if ((long)checkpointVersion.Version == CutCheckpoint && CutStream != null && CutStream.CanRead)
                {
                    DeletedWhileItsReaderWasOpen = true;
                }
                return base.DeleteCheckpointFileAsync(checkpointVersion, cancellationToken);
            }
        }

        // A reader that opens but cannot be read
        private sealed class UnreadableReader : PipeReader
        {
            public override void AdvanceTo(SequencePosition consumed)
            {
            }

            public override void AdvanceTo(SequencePosition consumed, SequencePosition examined)
            {
            }

            public override void CancelPendingRead()
            {
            }

            public override void Complete(Exception? exception = null)
            {
            }

            public override ValueTask<ReadResult> ReadAsync(CancellationToken cancellationToken = default)
            {
                throw new IOException("This file cannot be read in this test");
            }

            public override bool TryRead(out ReadResult result)
            {
                throw new IOException("This file cannot be read in this test");
            }
        }

        private static async Task<byte[]> ReadWhole(MemoryFileProvider store, ulong fileId, bool uncut = false)
        {
            var reader = uncut ? await ((CuttingStore)store).ReadUncut(fileId) : await store.ReadDataFileAsync(fileId, 0);
            return await ReadWhole(reader);
        }

        private static async Task<byte[]> ReadWhole(PipeReader reader)
        {
            var bytes = new MemoryStream();
            while (true)
            {
                var result = await reader.ReadAsync();
                foreach (var segment in result.Buffer)
                {
                    bytes.Write(segment.Span);
                }
                reader.AdvanceTo(result.Buffer.End);
                if (result.IsCompleted)
                {
                    break;
                }
            }
            await reader.CompleteAsync();
            return bytes.ToArray();
        }

        private static byte[] Bytes(int page)
        {
            var bytes = new byte[3000];
            new Random(page).NextBytes(bytes);
            return bytes;
        }

        private static ulong Bundle(int version)
        {
            return (1UL << 63) | (uint)version;
        }

        private static StorageInitializationMetadata Metadata()
        {
            return new StorageInitializationMetadata("damagedFile", NullLoggerFactory.Instance, GlobalMemoryManager.Instance);
        }

        // One page and one bundle file per version
        private static Task<ReservoirPersistentStorage> OpenWithFourCheckpoints(IReservoirStorageProvider store, IReservoirStorageProvider? cache)
        {
            return OpenWithCheckpoints(new ReservoirStorageOptions() { FileProvider = store, CacheProvider = cache }, Checkpoints);
        }

        private static async Task<ReservoirPersistentStorage> OpenWithCheckpoints(ReservoirStorageOptions options, int checkpoints)
        {
            var storage = new ReservoirPersistentStorage(options);
            await storage.InitializeAsync(Metadata());
            using var session = storage.CreateSession();
            for (int version = 1; version <= checkpoints; version++)
            {
                int page = FirstPage + version - 1;
                await session.Write(page, new SerializableObject(Bytes(page)));
                await session.Commit();
                await storage.CheckpointAsync(new byte[] { (byte)version }, false);
            }
            return storage;
        }

        private static int CutLength(string cut, byte[] file)
        {
            int registryStart = (int)BinaryPrimitives.ReadInt64LittleEndian(file.AsSpan(32));
            return cut switch
            {
                "empty" => 0,
                "inside the header" => 63,
                "after the header" => 100,
                "inside the page" => 2000,
                "inside the checkpoint" => registryStart - 10,
                "inside the registry header" => file.Length - 100,
                "unreadable" => int.MaxValue,
                _ => file.Length - 65
            };
        }

        private static Task Restart(ReservoirPersistentStorage storage)
        {
            return Task.Run(() => storage.InitializeAsync(Metadata())).WaitAsync(Limit);
        }

        [Theory]
        [InlineData("empty")]
        [InlineData("inside the header")]
        [InlineData("after the header")]
        [InlineData("inside the page")]
        [InlineData("inside the checkpoint")]
        [InlineData("inside the registry header")]
        [InlineData("inside the registry")]
        [InlineData("unreadable")]
        public async Task ADamagedCachedCopyOfTheNewestCheckpointIsReadFromTheStore(string cut)
        {
            var store = new MemoryFileProvider();
            var cache = new CuttingStore();
            using var storage = await OpenWithFourCheckpoints(store, cache);
            cache.CutFile = Bundle(Checkpoints);
            cache.CutLength = CutLength(cut, await ReadWhole(store, Bundle(Checkpoints)));

            await Restart(storage);

            Assert.True(cache.CutCopies > 0);
            Assert.Equal(Checkpoints + 1, storage.CurrentVersion);
            Assert.Contains(Bundle(Checkpoints), await store.GetStoredDataFileIdsAsync());
            using var session = storage.CreateSession();
            int lastPage = FirstPage + Checkpoints - 1;
            Assert.Equal(Bytes(lastPage), (await session.Read(lastPage)).ToArray());
        }

        [Theory]
        [InlineData("empty")]
        [InlineData("inside the header")]
        [InlineData("after the header")]
        [InlineData("inside the page")]
        [InlineData("inside the checkpoint")]
        [InlineData("unreadable")]
        public async Task ADamagedCachedCopyOfAnOlderCheckpointIsReadFromTheStore(string cut)
        {
            var store = new MemoryFileProvider();
            var cache = new CuttingStore();
            using var storage = await OpenWithFourCheckpoints(store, cache);
            cache.CutFile = Bundle(2);
            cache.CutLength = CutLength(cut, await ReadWhole(store, Bundle(2)));

            await Restart(storage);

            Assert.True(cache.CutCopies > 0);
            Assert.Equal(Checkpoints + 1, storage.CurrentVersion);
            using var session = storage.CreateSession();
            for (int page = FirstPage; page < FirstPage + Checkpoints; page++)
            {
                Assert.Equal(Bytes(page), (await session.Read(page)).ToArray());
            }
        }

        [Theory]
        [InlineData("empty")]
        [InlineData("inside the header")]
        [InlineData("after the header")]
        [InlineData("inside the page")]
        [InlineData("inside the checkpoint")]
        [InlineData("inside the registry header")]
        [InlineData("inside the registry")]
        public async Task ATornNewestCheckpointInTheStoreFallsBackToTheOneBefore(string cut)
        {
            var store = new CuttingStore();
            using var storage = await OpenWithFourCheckpoints(store, null);
            store.CutFile = Bundle(Checkpoints);
            store.CutLength = CutLength(cut, await ReadWhole(store, Bundle(Checkpoints), true));

            await Restart(storage);

            Assert.True(store.CutCopies > 0);
            Assert.Equal(Checkpoints, storage.CurrentVersion);
            Assert.DoesNotContain(Bundle(Checkpoints), await store.GetStoredDataFileIdsAsync());
            using var session = storage.CreateSession();
            int lastPage = FirstPage + Checkpoints - 1;
            Assert.Equal(Bytes(lastPage - 1), (await session.Read(lastPage - 1)).ToArray());
            await Assert.ThrowsAsync<FlowtidePersistentStorageException>(async () => await session.Read(lastPage));
        }

        [Fact]
        public async Task AnUnreadableStoreCopyOfTheNewestCheckpointFailsTheRestartAndIsKept()
        {
            var store = new CuttingStore();
            var cache = new CuttingStore();
            using var storage = await OpenWithFourCheckpoints(store, cache);
            cache.CutFile = Bundle(Checkpoints);
            cache.CutLength = 100;
            store.CutFile = Bundle(Checkpoints);
            store.CutLength = int.MaxValue;

            await Assert.ThrowsAsync<IOException>(() => Restart(storage));

            Assert.True(cache.CutCopies > 0);
            Assert.True(store.CutCopies > 0);
            Assert.Contains(Bundle(Checkpoints), await store.GetStoredDataFileIdsAsync());
        }

        [Theory]
        [InlineData(0)]
        [InlineData(30)]
        [InlineData(100)]
        [InlineData(200)]
        public async Task ATornNewestSnapshotFileFallsBackToTheCheckpointBefore(int cutLength)
        {
            var store = new CuttingStore();
            using var storage = await OpenWithCheckpoints(new ReservoirStorageOptions() { FileProvider = store, SnapshotCheckpointInterval = 2 }, 3);
            var snapshot = Assert.Single(await store.ListCheckpointFilesAsync());
            store.CutCheckpoint = (long)snapshot.Version;
            store.CutLength = cutLength;

            await Restart(storage);

            Assert.True(snapshot.IsSnapshot);
            Assert.Equal(3UL, snapshot.Version);
            Assert.True(store.CutCopies > 0);
            Assert.False(store.DeletedWhileItsReaderWasOpen);
            Assert.False(store.CutStream!.CanRead);
            Assert.Equal(3, storage.CurrentVersion);
            Assert.Empty(await store.ListCheckpointFilesAsync());
            using var session = storage.CreateSession();
            Assert.Equal(Bytes(FirstPage + 1), (await session.Read(FirstPage + 1)).ToArray());
            await Assert.ThrowsAsync<FlowtidePersistentStorageException>(async () => await session.Read(FirstPage + 2));
        }
    }
}
