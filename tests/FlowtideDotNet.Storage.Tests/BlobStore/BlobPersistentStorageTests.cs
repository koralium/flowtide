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
using FlowtideDotNet.Storage.Persistence.ObjectStorage.Internal;
using FlowtideDotNet.Storage.Persistence.ObjectStorage.LocalDisk;
using System.Buffers;
using FlowtideDotNet.Storage.Exceptions;
using FlowtideDotNet.Storage.Persistence;

namespace FlowtideDotNet.Storage.Tests.BlobStore
{
    public class BlobPersistentStorageTests : IDisposable
    {
        private readonly string _tempPath;
        private readonly string _dataPath;
        private readonly string _checkpointPath;

        public BlobPersistentStorageTests()
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
        public async Task TestWriteAndRead()
        {
            var provider = new LocalDiskProvider(_dataPath, _checkpointPath);
            var persistentStorage = new BlobPersistentStorage(new Persistence.ObjectStorage.BlobStorageOptions() { FileProvider = provider });
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
            {
                var provider = new LocalDiskProvider(_dataPath, _checkpointPath);
                var persistentStorage = new BlobPersistentStorage(new Persistence.ObjectStorage.BlobStorageOptions() { FileProvider = provider });
                await persistentStorage.InitializeAsync(new StorageInitializationMetadata("a"));

                var session = persistentStorage.CreateSession();
                await session.Write(100, new SerializableObject(new byte[] { 1, 2, 3, 4 }));
                await session.Commit();
                await persistentStorage.CheckpointAsync(new byte[] { 1, 2, 3 }, false);
            }

            {
                var provider = new LocalDiskProvider(_dataPath, _checkpointPath);
                var persistentStorage = new BlobPersistentStorage(new Persistence.ObjectStorage.BlobStorageOptions() { FileProvider = provider });
                await persistentStorage.InitializeAsync(new StorageInitializationMetadata("a"));
                await persistentStorage.RecoverAsync(1);

                var session = persistentStorage.CreateSession();
                var data = await session.Read(100);
                Assert.Equal(new byte[] { 1, 2, 3, 4 }, data.ToArray());
            }
        }

        [Fact]
        public async Task TestDelete()
        {
            {
                var provider = new LocalDiskProvider(_dataPath, _checkpointPath);
                var persistentStorage = new BlobPersistentStorage(new Persistence.ObjectStorage.BlobStorageOptions() { FileProvider = provider });
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
                var provider = new LocalDiskProvider(_dataPath, _checkpointPath);
                var persistentStorage = new BlobPersistentStorage(new Persistence.ObjectStorage.BlobStorageOptions() { FileProvider = provider });
                await persistentStorage.InitializeAsync(new StorageInitializationMetadata("a"));
                await persistentStorage.RecoverAsync(2);

                var session = persistentStorage.CreateSession();
                await Assert.ThrowsAsync<FlowtidePersistentStorageException>(async () =>
                {
                    await session.Read(100);
                });
            }
        }
    }
}
