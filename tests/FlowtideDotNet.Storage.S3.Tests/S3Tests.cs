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
using FlowtideDotNet.Storage.Persistence.Reservoir;
using FlowtideDotNet.Storage.Persistence.Reservoir.Internal;
using FlowtideDotNet.Storage.Persistence.Reservoir.MemoryDisk;
using FlowtideDotNet.Storage.S3.Internal;
using Microsoft.Extensions.Logging.Abstractions;

namespace FlowtideDotNet.Storage.S3.Tests
{
    public class S3Tests : IClassFixture<S3ContainerFixture>
    {
        private readonly S3ContainerFixture s3Container;

        public S3Tests(S3ContainerFixture s3container)
        {
            s3Container = s3container;
        }

        [Fact]
        public async Task WriteAndRead()
        {
            {
                var storage = new ReservoirPersistentStorage(new ReservoirStorageOptions()
                {
                    FileProvider = new S3FileProvider(s3Container.GetOptions("test")),
                    CacheProvider = new MemoryFileProvider(),
                });
                await storage.InitializeAsync(new Persistence.StorageInitializationMetadata("stream", NullLoggerFactory.Instance));

                var session = storage.CreateSession();
                await session.Write(3, new SerializableObject(new byte[] { 1, 2, 3 }));
                await session.Commit();

                await storage.CheckpointAsync(new byte[] { 1 }, false);
            }
            

            {
                var storage = new ReservoirPersistentStorage(new ReservoirStorageOptions()
                {
                    FileProvider = new S3FileProvider(s3Container.GetOptions("test")),
                    CacheProvider = new MemoryFileProvider(),
                });
                await storage.InitializeAsync(new Persistence.StorageInitializationMetadata("stream", NullLoggerFactory.Instance));

                var session = storage.CreateSession();
                var memory = await session.Read(3);
                Assert.Equal(new byte[] { 1, 2, 3 }, memory);
            }
        }

        [Fact]
        public async Task WriteSnapshot()
        {
            {
                var storage = new ReservoirPersistentStorage(new ReservoirStorageOptions()
                {
                    FileProvider = new S3FileProvider(s3Container.GetOptions("test")),
                    CacheProvider = new MemoryFileProvider(),
                    SnapshotCheckpointInterval = 0
                });
                await storage.InitializeAsync(new Persistence.StorageInitializationMetadata("stream", NullLoggerFactory.Instance));

                var session = storage.CreateSession();
                await session.Write(3, new SerializableObject(new byte[] { 1, 2, 3 }));
                await session.Commit();

                await storage.CheckpointAsync(new byte[] { 1 }, false);
            }


            {
                var storage = new ReservoirPersistentStorage(new ReservoirStorageOptions()
                {
                    FileProvider = new S3FileProvider(s3Container.GetOptions("test")),
                    CacheProvider = new MemoryFileProvider(),
                });
                await storage.InitializeAsync(new Persistence.StorageInitializationMetadata("stream", NullLoggerFactory.Instance));

                var session = storage.CreateSession();
                var memory = await session.Read(3);
                Assert.Equal(new byte[] { 1, 2, 3 }, memory);
            }
        }

        [Fact]
        public async Task TestCompaction()
        {
            {
                var storage = new ReservoirPersistentStorage(new ReservoirStorageOptions()
                {
                    FileProvider = new S3FileProvider(s3Container.GetOptions("test")),
                    CacheProvider = new MemoryFileProvider(),
                    SnapshotCheckpointInterval = 0
                });
                await storage.InitializeAsync(new Persistence.StorageInitializationMetadata("stream", NullLoggerFactory.Instance));

                var session = storage.CreateSession();

                for (int i = 0; i < 10; i++)
                {
                    await session.Write(3, new SerializableObject(new byte[] { 1, 2, 3 }));
                    await session.Commit();
                    await storage.CheckpointAsync(new byte[] { 1 }, false);
                    await storage.CompactAsync(1, 1);
                }

                var memory = await session.Read(3);
                Assert.Equal(new byte[] { 1, 2, 3 }, memory);
            }
        }
    }
}
