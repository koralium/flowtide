using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Persistence.ObjectStorage.Internal;
using FlowtideDotNet.Storage.Persistence.ObjectStorage.LocalDisk;
using System.Buffers;
using FlowtideDotNet.Storage.Exceptions;
using FlowtideDotNet.Storage.Persistence;

namespace FlowtideDotNet.Storage.Tests.BlobStore
{
    public class BlobSnapshotTests : IDisposable
    {
        private readonly string _tempPath;
        private readonly string _dataPath;
        private readonly string _checkpointPath;

        public BlobSnapshotTests()
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
        public async Task TestSnapshot()
        {
            var provider = new LocalDiskProvider(_dataPath, _checkpointPath);
            var persistentStorage = new BlobPersistentStorage(provider, MemoryPool<byte>.Shared, GlobalMemoryManager.Instance);
            await persistentStorage.InitializeAsync(new StorageInitializationMetadata("a"));

            var session = persistentStorage.CreateSession();
            await session.Write(100, new SerializableObject(new byte[] { 1, 2, 3, 4 }));
            await session.Commit();
            await persistentStorage.CheckpointAsync(new byte[] { 1, 2, 3 }, false);

            for (int i = 0; i < 10; i++)
            {
                await session.Write(100 + i + 1, new SerializableObject(new byte[] { 1 }));
                await session.Commit();
                await persistentStorage.CheckpointAsync(new byte[] { 1, 2, 3 }, false);
            }

            {
                var provider2 = new LocalDiskProvider(_dataPath, _checkpointPath);
                var persistentStorage2 = new BlobPersistentStorage(provider2, MemoryPool<byte>.Shared, GlobalMemoryManager.Instance);
                await persistentStorage2.InitializeAsync(new StorageInitializationMetadata("a"));
                await persistentStorage2.RecoverAsync(persistentStorage.CurrentVersion - 1);
                var session2 = persistentStorage2.CreateSession();
                var data = await session2.Read(100);
                Assert.Equal(new byte[] { 1, 2, 3, 4 }, data.ToArray());
            }
        }

        [Fact]
        public async Task TestCompaction()
        {
            var provider = new LocalDiskProvider(_dataPath, _checkpointPath);
            var persistentStorage = new BlobPersistentStorage(provider, MemoryPool<byte>.Shared, GlobalMemoryManager.Instance);
            await persistentStorage.InitializeAsync(new StorageInitializationMetadata("a"));

            var session = persistentStorage.CreateSession();
            await session.Write(100, new SerializableObject(new byte[] { 1, 2, 3, 4 }));
            await session.Commit();
            await persistentStorage.CheckpointAsync(new byte[] { 1, 2, 3 }, false);

            // Create enough checkpoints to have a snapshot and some history
            for (int i = 0; i < 10; i++)
            {
                await session.Write(100 + i + 1, new SerializableObject(new byte[] { 1 }));
                await session.Commit();
                await persistentStorage.CheckpointAsync(new byte[] { 1, 2, 3 }, false);
            }

            // Compact
            await persistentStorage.CompactAsync(0, 0);

            // Verify recovery still works for latest version
            {
                var provider2 = new LocalDiskProvider(_dataPath, _checkpointPath);
                var persistentStorage2 = new BlobPersistentStorage(provider2, MemoryPool<byte>.Shared, GlobalMemoryManager.Instance);
                await persistentStorage2.InitializeAsync(new StorageInitializationMetadata("a"));
                await persistentStorage2.RecoverAsync(persistentStorage.CurrentVersion - 1);
                var session2 = persistentStorage2.CreateSession();
                var data = await session2.Read(100);
                Assert.Equal(new byte[] { 1, 2, 3, 4 }, data.ToArray());
            }
        }

        [Fact]
        public async Task TestWriteSameDeletedPage()
        {
             var provider = new LocalDiskProvider(_dataPath, _checkpointPath);
            var persistentStorage = new BlobPersistentStorage(provider, MemoryPool<byte>.Shared, GlobalMemoryManager.Instance);
            await persistentStorage.InitializeAsync(new StorageInitializationMetadata("a"));

            var session = persistentStorage.CreateSession();
            await session.Write(100, new SerializableObject(new byte[] { 1, 2, 3, 4 }));
            await session.Delete(100);
            await session.Write(100, new SerializableObject(new byte[] { 1, 2, 3, 4 }));
            await session.Commit();
            await persistentStorage.CheckpointAsync(new byte[] { 1, 2, 3 }, false);

            {
                var provider2 = new LocalDiskProvider(_dataPath, _checkpointPath);
                var persistentStorage2 = new BlobPersistentStorage(provider2, MemoryPool<byte>.Shared, GlobalMemoryManager.Instance);
                await persistentStorage2.InitializeAsync(new StorageInitializationMetadata("a"));
                await persistentStorage2.RecoverAsync(persistentStorage.CurrentVersion - 1);
                var session2 = persistentStorage2.CreateSession();
                var data = await session2.Read(100);
                Assert.Equal(new byte[] { 1, 2, 3, 4 }, data.ToArray());
            }
        }

        [Fact]
        public async Task TestWriteAndReadUncommitted()
        {
            var provider = new LocalDiskProvider(_dataPath, _checkpointPath);
            var persistentStorage = new BlobPersistentStorage(provider, MemoryPool<byte>.Shared, GlobalMemoryManager.Instance);
            await persistentStorage.InitializeAsync(new StorageInitializationMetadata("a"));

            var session = persistentStorage.CreateSession();
            await session.Write(100, new SerializableObject(new byte[] { 1, 2, 3, 4 }));

            // Read back from same session, should see uncommitted data
            var data = await session.Read(100);
            Assert.Equal(new byte[] { 1, 2, 3, 4 }, data.ToArray());

            // Create another session, should not see uncommitted data
            var session2 = persistentStorage.CreateSession();
            // This expects exception because key does not exist in storage
            await Assert.ThrowsAsync<FlowtidePersistentStorageException>(async () =>
            {
                await session2.Read(100);
            });
        }

        [Fact]
        public async Task TestReset()
        {
            var provider = new LocalDiskProvider(_dataPath, _checkpointPath);
            var persistentStorage = new BlobPersistentStorage(provider, MemoryPool<byte>.Shared, GlobalMemoryManager.Instance);
            await persistentStorage.InitializeAsync(new StorageInitializationMetadata("a"));

            var session = persistentStorage.CreateSession();
            await session.Write(100, new SerializableObject(new byte[] { 1, 2, 3, 4 }));
            await session.Commit();
            await persistentStorage.CheckpointAsync(new byte[] { 1, 2, 3 }, false);

            await persistentStorage.ResetAsync();
            
            var session2 = persistentStorage.CreateSession();
            await session2.Write(100, new SerializableObject(new byte[] { 5, 6, 7, 8 }));
            await session2.Commit();
            await persistentStorage.CheckpointAsync(new byte[] { 1, 2, 3 }, false);

            // Recover to verify
            persistentStorage.Dispose();

            var provider2 = new LocalDiskProvider(_dataPath, _checkpointPath);
            var persistentStorage2 = new BlobPersistentStorage(provider2, MemoryPool<byte>.Shared, GlobalMemoryManager.Instance);
            await persistentStorage2.InitializeAsync(new StorageInitializationMetadata("a"));
            // Recover latest
            await persistentStorage2.RecoverAsync(1);

            var session3 = persistentStorage2.CreateSession();
            var data = await session3.Read(100);
            Assert.Equal(new byte[] { 5, 6, 7, 8 }, data.ToArray());
        }
    }
}