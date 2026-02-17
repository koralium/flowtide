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
            var persistentStorage = new BlobPersistentStorage(new Persistence.ObjectStorage.BlobStorageOptions() 
            { 
                FileProvider = provider,
                SnapshotCheckpointInterval = 5
            });
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
                var persistentStorage2 = new BlobPersistentStorage(new Persistence.ObjectStorage.BlobStorageOptions()
                {
                    FileProvider = provider2,
                    SnapshotCheckpointInterval = 5
                });
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
            var persistentStorage = new BlobPersistentStorage(new Persistence.ObjectStorage.BlobStorageOptions()
            {
                FileProvider = provider,
                SnapshotCheckpointInterval = 5
            });
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
                var persistentStorage2 = new BlobPersistentStorage(new Persistence.ObjectStorage.BlobStorageOptions()
                {
                    FileProvider = provider2,
                    SnapshotCheckpointInterval = 5
                });
                await persistentStorage2.InitializeAsync(new StorageInitializationMetadata("a"));
                await persistentStorage2.RecoverAsync(persistentStorage.CurrentVersion - 1);
                var session2 = persistentStorage2.CreateSession();
                var data = await session2.Read(100);
                Assert.Equal(new byte[] { 1, 2, 3, 4 }, data.ToArray());
            }
        }
    }
}