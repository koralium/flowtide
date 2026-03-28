using FlowtideDotNet.Storage.Exceptions;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Persistence;
using FlowtideDotNet.Storage.Persistence.Reservoir.Internal;
using FlowtideDotNet.Storage.Persistence.Reservoir.LocalDisk;
using FlowtideDotNet.Storage.Persistence.Reservoir.MemoryDisk;
using Microsoft.Extensions.Logging.Abstractions;
using System.Buffers;

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
            await persistentStorage.InitializeAsync(new StorageInitializationMetadata("a", NullLoggerFactory.Instance));

            var session = persistentStorage.CreateSession();
            await session.Write(100, new SerializableObject(new byte[] { 1, 2, 3, 4 }));
            await session.Commit();
            await persistentStorage.CheckpointAsync(new byte[] { 1, 2, 3 }, false);

            for (int i = 0; i < 100; i++)
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
                await persistentStorage2.InitializeAsync(new StorageInitializationMetadata("a", NullLoggerFactory.Instance));
                await persistentStorage2.RecoverAsync(persistentStorage.CurrentVersion - 1);
                var session2 = persistentStorage2.CreateSession();
                var data = await session2.Read(100);
                Assert.Equal(new byte[] { 1, 2, 3, 4 }, data.ToArray());

                for (int i = 0; i < 100; i++)
                {
                    var loopData = await session2.Read(101 + i);
                    Assert.Equal(new byte[] { 1 }, loopData);
                }

                persistentStorage2.Dispose();
            }
            persistentStorage.Dispose();
        }

        /// <summary>
        /// This test checks that the stream can still start even when a snapshot checkpoint got written and ended prematurely
        /// </summary>
        /// <returns></returns>
        [Fact]
        public async Task TestFailureToReadSnapshotRegistry()
        {
            var provider = new TestDataProvider();
            var persistentStorage = new ReservoirPersistentStorage(new Persistence.Reservoir.ReservoirStorageOptions()
            {
                FileProvider = provider,
                SnapshotCheckpointInterval = 5
            });
            await persistentStorage.InitializeAsync(new StorageInitializationMetadata("a", NullLoggerFactory.Instance));

            var session = persistentStorage.CreateSession();
            for (int i = 0; i < 6; i++)
            {
                await session.Write(100 + i, new SerializableObject(new byte[] { 1 }));
                await session.Commit();
                await persistentStorage.CheckpointAsync(new byte[] { 1, 2, 3 }, false);
            }
            var getData = provider.TryGetCheckpointFileData(new Persistence.Reservoir.CheckpointId(6, true), out var checkpointData);
            Assert.True(getData);
            Assert.NotNull(checkpointData);
            var startOffset = checkpointData.Length - 30;
            for (int i = startOffset; i < checkpointData.Length; i++)
            {
                // Set 0 on the last 30 bytes to simulate that it stopped writing correctly
                checkpointData[i] = 0;
            }

            {
                var persistentStorage2 = new ReservoirPersistentStorage(new Persistence.Reservoir.ReservoirStorageOptions()
                {
                    FileProvider = provider,
                    SnapshotCheckpointInterval = 5
                });
                await persistentStorage2.InitializeAsync(new StorageInitializationMetadata("a", NullLoggerFactory.Instance));

                // Check that we restored so current version is 6, (previous storage was at 7, but restore failed to that version).
                Assert.Equal(6, persistentStorage2.CurrentVersion);
                var session2 = persistentStorage2.CreateSession();
                var data = await session2.Read(100);
                for (int i = 0; i < 5; i++)
                {
                    var loopData = await session2.Read(100 + i);
                    Assert.Equal(new byte[] { 1 }, loopData);
                }
                // check that we cant read key 105 since that was added in the last version that "failed".
                var exc = await Assert.ThrowsAsync<FlowtidePersistentStorageException>(async () =>
                {
                    await session2.Read(105);
                });
                Assert.Equal("Key 105 not found in persistent storage.", exc.Message);

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
            await persistentStorage.InitializeAsync(new StorageInitializationMetadata("a", NullLoggerFactory.Instance));

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
                await persistentStorage2.InitializeAsync(new StorageInitializationMetadata("a", NullLoggerFactory.Instance));
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
                await persistentStorage.InitializeAsync(new StorageInitializationMetadata("a", NullLoggerFactory.Instance));

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
                await persistentStorage2.InitializeAsync(new StorageInitializationMetadata("a", NullLoggerFactory.Instance));
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

        [Fact]
        public async Task TestCompactSingleFileAfterRestore()
        {
            var provider = new MemoryFileProvider();
            
            var persistentStorage = new ReservoirPersistentStorage(new Persistence.Reservoir.ReservoirStorageOptions()
            {
                FileProvider = provider,
                SnapshotCheckpointInterval = 5
            });
            await persistentStorage.InitializeAsync(new StorageInitializationMetadata("a", NullLoggerFactory.Instance));

            var session = persistentStorage.CreateSession();
            await session.Write(100, new SerializableObject(new byte[] { 1, 2, 3, 4 }));
            await session.Commit();
            await persistentStorage.CheckpointAsync(new byte[] { 1, 2, 3 }, false);

            var persistentStorage2 = new ReservoirPersistentStorage(new Persistence.Reservoir.ReservoirStorageOptions()
            {
                FileProvider = provider,
                SnapshotCheckpointInterval = 5
            });
            await persistentStorage2.InitializeAsync(new StorageInitializationMetadata("a", NullLoggerFactory.Instance));

            var lastData = (await provider.GetStoredDataFileIdsAsync()).Last();

            persistentStorage2.TryGetFileInformation(lastData, out var fileInfo);
            Assert.NotNull(fileInfo);
            await persistentStorage2.CompactFile(lastData, fileInfo.FileSize, fileInfo.Crc64);

            
        }
    }
}