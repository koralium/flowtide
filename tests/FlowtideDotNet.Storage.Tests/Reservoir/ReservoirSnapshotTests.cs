using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Persistence.Reservoir.Internal;
using FlowtideDotNet.Storage.Persistence.Reservoir.LocalDisk;
using System.Buffers;
using FlowtideDotNet.Storage.Exceptions;
using FlowtideDotNet.Storage.Persistence;
using FlowtideDotNet.Storage.Persistence.Reservoir.MemoryDisk;

namespace FlowtideDotNet.Storage.Tests.Reservoir
{
    public class ReservoirSnapshotTests
    {
        [Fact]
        public async Task TestSnapshot()
        {
            var provider = new TestDataProvider();
            var persistentStorage = new ReservoirPersistentStorage(new Persistence.Reservoir.ReservoirStorageOptions() 
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
                var persistentStorage2 = new ReservoirPersistentStorage(new Persistence.Reservoir.ReservoirStorageOptions()
                {
                    FileProvider = provider,
                    SnapshotCheckpointInterval = 5
                });
                await persistentStorage2.InitializeAsync(new StorageInitializationMetadata("a"));
                await persistentStorage2.RecoverAsync(persistentStorage.CurrentVersion - 1);
                var session2 = persistentStorage2.CreateSession();
                var data = await session2.Read(100);
                Assert.Equal(new byte[] { 1, 2, 3, 4 }, data.ToArray());
                persistentStorage2.Dispose();
            }
            persistentStorage.Dispose();

        }

        [Fact]
        public async Task TestCompaction()
        {
            var provider = new MemoryFileProvider();
            var persistentStorage = new ReservoirPersistentStorage(new Persistence.Reservoir.ReservoirStorageOptions()
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
            for (int i = 0; i < 20; i += 2)
            {
                await session.Write(100 + i + 1, new SerializableObject(new byte[] { (byte)i }));
                await session.Write(100 + i + 2, new SerializableObject(new byte[] { (byte)(i + 1) }));
                await session.Commit();
                await persistentStorage.CheckpointAsync(new byte[] { 1, 2, 3 }, false);
            }

            // Compact
            await persistentStorage.CompactAsync(0, 0);

            // Verify recovery still works for latest version
            {
                var persistentStorage2 = new ReservoirPersistentStorage(new Persistence.Reservoir.ReservoirStorageOptions()
                {
                    FileProvider = provider,
                    SnapshotCheckpointInterval = 5
                });
                await persistentStorage2.InitializeAsync(new StorageInitializationMetadata("a"));
                await persistentStorage2.RecoverAsync(persistentStorage.CurrentVersion - 1);
                var session2 = persistentStorage2.CreateSession();
                var data = await session2.Read(100);
                Assert.Equal(new byte[] { 1, 2, 3, 4 }, data.ToArray());

                for (int i = 0; i < 20; i++)
                {
                    var loopData = await session2.Read(101 + i);
                    Assert.Equal(new byte[] { (byte)i }, loopData);
                }
            }
        }

        /// <summary>
        /// Verifies that CRC value of the first file was saved correctly
        /// </summary>
        /// <returns></returns>
        [Fact]
        public async Task TestCompactionAfterRestore()
        {
            var provider = new MemoryFileProvider();
            {
                var persistentStorage = new ReservoirPersistentStorage(new Persistence.Reservoir.ReservoirStorageOptions()
                {
                    FileProvider = provider,
                    SnapshotCheckpointInterval = 5
                });
                await persistentStorage.InitializeAsync(new StorageInitializationMetadata("a"));

                var session = persistentStorage.CreateSession();
                await session.Write(100, new SerializableObject(new byte[] { 1, 2, 3, 4 }));
                await session.Commit();
                await persistentStorage.CheckpointAsync(new byte[] { 1, 2, 3 }, false);
            }

            // Verify recovery still works for latest version
            {
                var persistentStorage2 = new ReservoirPersistentStorage(new Persistence.Reservoir.ReservoirStorageOptions()
                {
                    FileProvider = provider,
                    SnapshotCheckpointInterval = 5
                });
                await persistentStorage2.InitializeAsync(new StorageInitializationMetadata("a"));
                var session2 = persistentStorage2.CreateSession();

                // Create enough checkpoints to have a snapshot and some history
                for (int i = 0; i < 20; i += 2)
                {
                    await session2.Write(100 + i + 1, new SerializableObject(new byte[] { (byte)i }));
                    await session2.Write(100 + i + 2, new SerializableObject(new byte[] { (byte)(i + 1) }));
                    await session2.Commit();
                    await persistentStorage2.CheckpointAsync(new byte[] { 1, 2, 3 }, false);
                }

                await persistentStorage2.CompactAsync(0, 0);

                var data = await session2.Read(100);
                Assert.Equal(new byte[] { 1, 2, 3, 4 }, data.ToArray());

                for (int i = 0; i < 20; i++)
                {
                    var loopData = await session2.Read(101 + i);
                    Assert.Equal(new byte[] { (byte)i }, loopData);
                }
            }
        }
    }
}