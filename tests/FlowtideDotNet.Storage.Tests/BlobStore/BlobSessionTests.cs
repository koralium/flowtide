using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Persistence.ObjectStorage.Internal;
using FlowtideDotNet.Storage.Persistence.ObjectStorage.LocalDisk;
using System.Buffers;
using FlowtideDotNet.Storage.Exceptions;
using FlowtideDotNet.Storage.Persistence;

namespace FlowtideDotNet.Storage.Tests.BlobStore
{
    public class BlobSessionTests : IDisposable
    {
        private readonly string _tempPath;
        private readonly string _dataPath;
        private readonly string _checkpointPath;

        public BlobSessionTests()
        {
            _tempPath = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());
            _dataPath = Path.Combine(_tempPath, "data");
            _checkpointPath = Path.Combine(_tempPath, "checkpoints");
            Directory.CreateDirectory(_tempPath);
            Directory.CreateDirectory(_dataPath);
            Directory.CreateDirectory(_checkpointPath);
        }

        public void Dispose()
        {
            if (Directory.Exists(_tempPath))
            {
                Directory.Delete(_tempPath, true);
            }
        }

        [Fact]
        public async Task TestReadYourDeletes()
        {
            var provider = new LocalDiskProvider(_dataPath, _checkpointPath);
            var persistentStorage = new BlobPersistentStorage(new Persistence.ObjectStorage.BlobStorageOptions() { FileProvider = provider });
            await persistentStorage.InitializeAsync(new StorageInitializationMetadata("a"));

            var session = persistentStorage.CreateSession();
            // Initial write and commit
            await session.Write(100, new SerializableObject(new byte[] { 1, 2, 3, 4 }));
            await session.Commit();
            await persistentStorage.CheckpointAsync(new byte[] { 1 }, false);

            // Now delete in a new transaction/session work
            await session.Delete(100);

            await Assert.ThrowsAsync<FlowtidePersistentStorageException>(async () =>
            {
                await session.Read(100);
            });
        }

        [Fact]
        public async Task TestLargeWriteFileRolling()
        {
            var provider = new LocalDiskProvider(_dataPath, _checkpointPath);
            var persistentStorage = new BlobPersistentStorage(new Persistence.ObjectStorage.BlobStorageOptions() 
            { 
                FileProvider = provider,
                MaxFileSize = 1024 * 1024 // 1MB to force rolling after ~100 keys
            });
            await persistentStorage.InitializeAsync(new StorageInitializationMetadata("a"));

            var session = persistentStorage.CreateSession();
            
            int keyCount = 1100;
            byte[] payload = new byte[10 * 1024]; // 10KB
            new Random().NextBytes(payload);

            int startOffset = 100;
            for (int i = 0; i < keyCount; i++)
            {
                await session.Write(i + startOffset, new SerializableObject(payload));
            }

            await session.Commit();
            await persistentStorage.CheckpointAsync(new byte[] { 1 }, false);

            // Recover and verify a few keys
            {
                var provider2 = new LocalDiskProvider(_dataPath, _checkpointPath);
                var persistentStorage2 = new BlobPersistentStorage(new Persistence.ObjectStorage.BlobStorageOptions() { FileProvider = provider2 });
                await persistentStorage2.InitializeAsync(new StorageInitializationMetadata("a"));
                await persistentStorage2.RecoverAsync(persistentStorage.CurrentVersion - 1);
                var session2 = persistentStorage2.CreateSession();

                var data = await session2.Read(startOffset);
                Assert.Equal(payload, data.ToArray());

                var dataLast = await session2.Read(startOffset + keyCount - 1);
                Assert.Equal(payload, dataLast.ToArray());
            }
        }

        [Fact]
        public async Task TestConcurrentWriters()
        {
            var provider = new LocalDiskProvider(_dataPath, _checkpointPath);
            var persistentStorage = new BlobPersistentStorage(new Persistence.ObjectStorage.BlobStorageOptions() { FileProvider = provider });
            await persistentStorage.InitializeAsync(new StorageInitializationMetadata("a"));

            // Two sessions writing non-overlapping keys concurrently
            var session1 = persistentStorage.CreateSession();
            var session2 = persistentStorage.CreateSession();

            var task1 = Task.Run(async () =>
            {
                for (int i = 100; i < 200; i++)
                {
                    await session1.Write(i, new SerializableObject(new byte[] { 1 }));
                }
                await session1.Commit();
            });

            var task2 = Task.Run(async () =>
            {
                for (int i = 200; i < 300; i++)
                {
                    await session2.Write(i, new SerializableObject(new byte[] { 2 }));
                }
                await session2.Commit();
            });

            await Task.WhenAll(task1, task2);

            await persistentStorage.CheckpointAsync(new byte[] { 1 }, false);

            // Recover and verify
            {
                var provider2 = new LocalDiskProvider(_dataPath, _checkpointPath);
                var persistentStorage2 = new BlobPersistentStorage(new Persistence.ObjectStorage.BlobStorageOptions() { FileProvider = provider2 });
                await persistentStorage2.InitializeAsync(new StorageInitializationMetadata("a"));
                await persistentStorage2.RecoverAsync(persistentStorage.CurrentVersion - 1);
                var session3 = persistentStorage2.CreateSession();

                var val1 = await session3.Read(150);
                Assert.Equal(new byte[] { 1 }, val1.ToArray());

                var val2 = await session3.Read(250);
                Assert.Equal(new byte[] { 2 }, val2.ToArray());
            }
        }
    }
}