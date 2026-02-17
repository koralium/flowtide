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

namespace FlowtideDotNet.Storage.Tests.BlobStore
{
    public class BlobPersistentStorageTests
    {
        [Fact]
        public async Task Test()
        {
            BlobPersistentStorage persistentStorage = new BlobPersistentStorage(new LocalDiskProvider("./", "./checkpoints"), MemoryPool<byte>.Shared, GlobalMemoryManager.Instance);
            var session = persistentStorage.CreateSession();
            await session.Write(1, new SerializableObject(new byte[] { 1, 2, 3, 4 }));

            var writtenData = await session.Read(1);
            Assert.Equal(new byte[] {1,2,3,4}, writtenData);

             await session.Commit();
            await persistentStorage.CheckpointAsync(new byte[] {1,2,3}, false);

            writtenData = await session.Read(1);
            Assert.Equal(new byte[] {1,2,3,4}, writtenData);

            await session.Delete(1);

            await session.Commit();
            await persistentStorage.CheckpointAsync(new byte[] { 1, 2, 3 }, false);

            //var checkpoint0 = File.ReadAllBytes("checkpoint_0.blob");
            //ReadCheckpointData(checkpoint0);

            //var checkpoint1 = File.ReadAllBytes("checkpoint_1.blob");
            //ReadCheckpointData(checkpoint1);

            await persistentStorage.InitializeAsync(new Persistence.StorageInitializationMetadata("a"));
            await persistentStorage.RecoverAsync(1);
        }

        private void ReadCheckpointData(byte[] checkpointData)
        {
            var reader = new CheckpointDataReader(new ReadOnlySequence<byte>(checkpointData));

            while (reader.TryGetNextUpsertPageInfo(out var pageInfo))
            {

            }

            while (reader.TryGetNextDeletedPageId(out var pageId))
            {

            }

            while (reader.TryGetFileInformation(out var fileInformation))
            {

            }

            while (reader.TryGetNextDeletedFileId(out var fileId))
            {

            }
        }
    }
}
