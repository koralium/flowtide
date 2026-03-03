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

using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Persistence.Reservoir.Internal;
using FlowtideDotNet.Storage.Persistence.Reservoir.LocalDisk;
using System.Buffers;
using FlowtideDotNet.Storage.Exceptions;
using FlowtideDotNet.Storage.Persistence;
using FlowtideDotNet.Storage.Persistence.Reservoir.MemoryDisk;

namespace FlowtideDotNet.Storage.Tests.Reservoir
{
    public class ReservoirPersistentStorageTests
    {
        [Fact]
        public async Task TestWriteAndRead()
        {
            var provider = new MemoryFileProvider();
            var persistentStorage = new ReservoirPersistentStorage(new Persistence.Reservoir.ReservoirStorageOptions() { FileProvider = provider });
            await persistentStorage.InitializeAsync(new StorageInitializationMetadata("a"));

            var session = persistentStorage.CreateSession();
            await session.Write(100, new SerializableObject(new byte[] { 1, 2, 3, 4 }));

            var writtenData = await session.Read(100);
            Assert.Equal(new byte[] { 1, 2, 3, 4 }, writtenData.ToArray());

            await session.Commit();
            await persistentStorage.CheckpointAsync(new byte[] { 1, 2, 3 }, false);

            writtenData = await session.Read(100);
            Assert.Equal(new byte[] { 1, 2, 3, 4 }, writtenData.ToArray());
        }

        [Fact]
        public async Task TestRecovery()
        {
            var provider = new MemoryFileProvider();
            {
                var persistentStorage = new ReservoirPersistentStorage(new Persistence.Reservoir.ReservoirStorageOptions() { FileProvider = provider });
                await persistentStorage.InitializeAsync(new StorageInitializationMetadata("a"));

                var session = persistentStorage.CreateSession();
                await session.Write(100, new SerializableObject(new byte[] { 1, 2, 3, 4 }));
                await session.Commit();
                await persistentStorage.CheckpointAsync(new byte[] { 1, 2, 3 }, false);
            }

            {
                var persistentStorage = new ReservoirPersistentStorage(new Persistence.Reservoir.ReservoirStorageOptions() { FileProvider = provider });
                await persistentStorage.InitializeAsync(new StorageInitializationMetadata("a"));
                await persistentStorage.RecoverAsync(1);

                var session = persistentStorage.CreateSession();
                var data = await session.Read(100);
                Assert.Equal(new byte[] { 1, 2, 3, 4 }, data.ToArray());
            }
        }

        [Fact]
        public async Task TestRecoverSpecificCheckpoint()
        {
            var provider = new MemoryFileProvider();
            {
                var persistentStorage = new ReservoirPersistentStorage(new Persistence.Reservoir.ReservoirStorageOptions() { FileProvider = provider });
                await persistentStorage.InitializeAsync(new StorageInitializationMetadata("a"));

                var session = persistentStorage.CreateSession();
                
                // Write version 1
                await session.Write(100, new SerializableObject(new byte[] { 1 }));
                await session.Commit();
                await persistentStorage.CheckpointAsync(new byte[] { 1 }, false); // Version 1

                // Write version 2
                await session.Write(100, new SerializableObject(new byte[] { 2 }));
                await session.Commit();
                await persistentStorage.CheckpointAsync(new byte[] { 2 }, false); // Version 2
            }

            {
                var persistentStorage = new ReservoirPersistentStorage(new Persistence.Reservoir.ReservoirStorageOptions() { FileProvider = provider });
                await persistentStorage.InitializeAsync(new StorageInitializationMetadata("a"));
                // Explicitly recover checkpoint version 1
                await persistentStorage.RecoverAsync(1);

                var session = persistentStorage.CreateSession();
                var data = await session.Read(100);
                Assert.Equal(new byte[] { 1 }, data.ToArray()); // Value before second override
            }
        }

        [Fact]
        public async Task TestDelete()
        {
            var provider = new MemoryFileProvider();
            {
                var persistentStorage = new ReservoirPersistentStorage(new Persistence.Reservoir.ReservoirStorageOptions() { FileProvider = provider });
                await persistentStorage.InitializeAsync(new StorageInitializationMetadata("a"));

                var session = persistentStorage.CreateSession();
                await session.Write(100, new SerializableObject(new byte[] { 1, 2, 3, 4 }));
                await session.Commit();
                await persistentStorage.CheckpointAsync(new byte[] { 1, 2, 3 }, false);

                await session.Delete(100);
                await session.Commit();
                await persistentStorage.CheckpointAsync(new byte[] { 1, 2, 3 }, false);
            }

            {
                var persistentStorage = new ReservoirPersistentStorage(new Persistence.Reservoir.ReservoirStorageOptions() { FileProvider = provider });
                await persistentStorage.InitializeAsync(new StorageInitializationMetadata("a"));
                await persistentStorage.RecoverAsync(2);

                var session = persistentStorage.CreateSession();
                await Assert.ThrowsAsync<FlowtidePersistentStorageException>(async () =>
                {
                    await session.Read(100);
                });
            }
        }

        [Fact]
        public async Task TestDeleteUncommittedData()
        {
            var provider = new MemoryFileProvider();
            var persistentStorage = new ReservoirPersistentStorage(new Persistence.Reservoir.ReservoirStorageOptions() { FileProvider = provider });
            await persistentStorage.InitializeAsync(new StorageInitializationMetadata("a"));

            var session = persistentStorage.CreateSession();
            await session.Write(200, new SerializableObject(new byte[] { 5, 5 }));
            await session.Delete(200);

            await Assert.ThrowsAsync<FlowtidePersistentStorageException>(async () =>
            {
                await session.Read(200);
            });
        }

        [Fact]
        public async Task TestResetKeepsStableState()
        {
            var provider = new MemoryFileProvider();
            var persistentStorage = new ReservoirPersistentStorage(new Persistence.Reservoir.ReservoirStorageOptions() { FileProvider = provider });
            await persistentStorage.InitializeAsync(new StorageInitializationMetadata("a"));

            var session = persistentStorage.CreateSession();
            await session.Write(100, new SerializableObject(new byte[] { 9 }));
            await session.Commit();
            await persistentStorage.CheckpointAsync(new byte[] { 1 }, false);

            await persistentStorage.ResetAsync();

            var newSession = persistentStorage.CreateSession();
            
            // Should be empty since storage was re-initialized entirely
            await Assert.ThrowsAsync<FlowtidePersistentStorageException>(async () =>
            {
                await newSession.Read(100);
            });
        }

        /// <summary>
        /// Verifies that concurrent compaction and write operations do not result in data loss or corruption in the
        /// persistent storage implementation.
        /// </summary>
        /// <remarks>This test simulates a race condition between file compaction and write operations to
        /// ensure that the most recent data is preserved after compaction completes. It is intended to validate the
        /// thread safety and correctness of the storage system under concurrent access scenarios.
        /// 
        /// If not true, the checkpoint will throw an exception that a page exists twice in the checkpoint.
        /// </remarks>
        /// <returns></returns>
        [Fact]
        public async Task TestCompactionRaceCondition()
        {
            var provider = new TestDataProvider();
            var persistentStorage = new ReservoirPersistentStorage(new Persistence.Reservoir.ReservoirStorageOptions() 
            { 
                FileProvider = provider ,
                MaxFileSize = 10
            });
            await persistentStorage.InitializeAsync(new StorageInitializationMetadata("a"));

            var session = (ReservoirPersistentSession)persistentStorage.CreateSession();

            // Write data and checkpoint it so it gets stored
            await session.Write(3, new SerializableObject(new byte[] { 1, 2, 3 }));
            await session.Commit();
            await persistentStorage.CheckpointAsync(new byte[] { 1, 2, 3 }, false);

            provider.BlockWrites();

            await session.Write(3, new SerializableObject(new byte[] { 4, 5, 6 }));
            await session.SendBlobFile_Testing(); // Trigger a full file to storage
            await session.Commit();


            Assert.True(persistentStorage.TryGetFileInformation(0, out var fileInfo));

            // Trigger compact on the previous file
            // This tests that the previous value will not be written
            await persistentStorage.CompactFile(0, fileInfo.FileSize, fileInfo.Crc64);

            provider.UnblockWrites();
            await persistentStorage.CheckpointAsync(new byte[] { 1, 2, 3 }, false);
            var actual = await session.Read(3);
            Assert.Equal(new byte[] { 4, 5, 6 }, actual.ToArray());
        }

        /// <summary>
        /// Checks so a half written bundle file and then crash wont affect recovery process
        /// </summary>
        /// <returns></returns>
        [Fact]
        public async Task TestRecoverFromFaultyBundle()
        {
            var provider = new TestDataProvider();
            {
                var persistentStorage = new ReservoirPersistentStorage(new Persistence.Reservoir.ReservoirStorageOptions() { FileProvider = provider });
                await persistentStorage.InitializeAsync(new StorageInitializationMetadata("a"));

                var session = persistentStorage.CreateSession();

                // Write version 1
                await session.Write(100, new SerializableObject(new byte[] { 1 }));
                await session.Commit();
                await persistentStorage.CheckpointAsync(new byte[] { 1 }, false); // Version 1

                // Write version 2
                await session.Write(100, new SerializableObject(new byte[] { 2 }));
                await session.Commit();
                await persistentStorage.CheckpointAsync(new byte[] { 2 }, false); // Version 2

                var lastDataFile = (await provider.ListDataFilesAboveVersionAsync(2)).Last();

                Assert.True((lastDataFile & (1UL << 63)) > 0);
                Assert.True(provider.TryGetFileData(lastDataFile, out var fileData));

                byte[] newData = new byte[fileData.Length / 2];
                Array.Copy(fileData, newData, newData.Length);
                provider.SetFileData(lastDataFile, newData);
            }

            {
                var persistentStorage = new ReservoirPersistentStorage(new Persistence.Reservoir.ReservoirStorageOptions() { FileProvider = provider });
                await persistentStorage.InitializeAsync(new StorageInitializationMetadata("a"));

                var session = persistentStorage.CreateSession();
                var data = await session.Read(100);
                // Checkpoint 2 is corrupt so we should recover from checkpoint 1
                Assert.Equal(new byte[] { 1 }, data.ToArray()); // Value before second override
            }
        }
    }
}
