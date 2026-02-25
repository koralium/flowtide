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

using FlowtideDotNet.Storage.Exceptions;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Persistence.ObjectStorage.Internal;
using FlowtideDotNet.Storage.Persistence.ObjectStorage.LocalCache;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.StateManager.Internal;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO.Pipelines;
using System.Linq;
using System.Net.Cache;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Storage.Tests.BlobStore
{
    public class LocalCacheManagerTests
    {
        TestDataProvider persistentData;
        private LocalCacheProvider cacheProvider;
        private TestDataProvider cacheData;
        private BlobPersistentStorage blobPersistentStorage;
        public LocalCacheManagerTests()
        {
            persistentData = new TestDataProvider();
            cacheData = new TestDataProvider();
            blobPersistentStorage = new BlobPersistentStorage(new Persistence.ObjectStorage.BlobStorageOptions()
            {
                FileProvider = persistentData,
                CacheProvider = cacheData
            });
            var provider = blobPersistentStorage.CacheProvider;
            Assert.NotNull(provider);
            cacheProvider = provider;
        }

        [Fact]
        public async Task TestWriteFileToCache()
        {
            var blobFile = new BlobFileWriter((file) => { }, MemoryPool<byte>.Shared, GlobalMemoryManager.Instance);
            blobFile.Write(1, new SerializableObject(new byte[] { 1, 2, 3 }));
            blobFile.Finish();
            await cacheProvider.WriteDataFileAsync(1, blobFile.Crc64, blobFile.FileSize, blobFile);
            Assert.True(persistentData.DataFileExists(1));
            Assert.True(cacheData.DataFileExists(1));
        }

        [Fact]
        public async Task EnsureGetMemoryUsesCache()
        {
            var blobFile = new BlobFileWriter((file) => { }, MemoryPool<byte>.Shared, GlobalMemoryManager.Instance);
            blobFile.Write(1, new SerializableObject(new byte[] { 1, 2, 3 }));
            blobFile.Finish();
            var crc32 = blobFile.Crc32s[0];
            var offset = blobFile.PageOffsets[0];
            await cacheProvider.WriteDataFileAsync(1, blobFile.Crc64, blobFile.FileSize, blobFile);
            var memory = await cacheProvider.GetMemoryAsync(1, offset, 3, crc32);
            Assert.Equal(new byte[] { 1, 2, 3 }, memory.ToArray());
            Assert.Equal(0, persistentData.NumberOfReadMemory);
            Assert.Equal(1, cacheData.NumberOfReadMemory);
        }

        [Fact]
        public async Task EnsureReadAsyncUsesCache()
        {
            var blobFile = new BlobFileWriter((file) => { }, MemoryPool<byte>.Shared, GlobalMemoryManager.Instance);
            blobFile.Write(1, new SerializableObject(new byte[] { 1, 2, 3 }));
            blobFile.Finish();
            var crc32 = blobFile.Crc32s[0];
            var offset = blobFile.PageOffsets[0];
            await cacheProvider.WriteDataFileAsync(1, blobFile.Crc64, blobFile.FileSize, blobFile);
            var memory = await cacheProvider.ReadAsync(1, offset, 3, crc32, new LocalCacheTestSerializer());
            Assert.Equal(new byte[] { 1, 2, 3 }, memory.Data);
            Assert.Equal(0, persistentData.NumberOfReadAsync);
            Assert.Equal(1, cacheData.NumberOfReadAsync);
        }

        [Fact]
        public async Task GetMemoryAsyncFetchesFromPersistentStorage()
        {
            var blobFile = new BlobFileWriter((file) => { }, MemoryPool<byte>.Shared, GlobalMemoryManager.Instance);
            blobFile.Write(2, new SerializableObject(new byte[] { 1, 2, 3 }));
            blobFile.Finish();
            var crc32 = blobFile.Crc32s[0];
            var offset = blobFile.PageOffsets[0];
            await blobPersistentStorage.AddCompleteBlobFile(blobFile);
            await blobPersistentStorage.CheckpointAsync([1], false);

            await cacheProvider.EvictDataFileAsync(0);
            var memory = await cacheProvider.GetMemoryAsync(0, offset, 3, crc32);
            Assert.Equal(new byte[] { 1, 2, 3 }, memory.ToArray());
            Assert.Equal(1, persistentData.NumberOfReadDataFile);
            Assert.Equal(1, cacheData.NumberOfReadMemory);
        }

        [Fact]
        public async Task ReadAsyncFetchesFromPersistentStorage()
        {
            var blobFile = new BlobFileWriter((file) => { }, MemoryPool<byte>.Shared, GlobalMemoryManager.Instance);
            blobFile.Write(2, new SerializableObject(new byte[] { 1, 2, 3 }));
            blobFile.Finish();
            var crc32 = blobFile.Crc32s[0];
            var offset = blobFile.PageOffsets[0];
            await blobPersistentStorage.AddCompleteBlobFile(blobFile);
            await blobPersistentStorage.CheckpointAsync([1], false);
            
            await cacheProvider.EvictDataFileAsync(0);
            var memory = await cacheProvider.ReadAsync(0, offset, 3, crc32, new LocalCacheTestSerializer());
            Assert.Equal(new byte[] { 1, 2, 3 }, memory.Data);
            Assert.Equal(1, persistentData.NumberOfReadDataFile);
            Assert.Equal(1, cacheData.NumberOfReadAsync);
        }

        [Fact]
        public async Task ParallelReadsSinglePersistentRead()
        {
            var blobFile = new BlobFileWriter((file) => { }, MemoryPool<byte>.Shared, GlobalMemoryManager.Instance);
            blobFile.Write(2, new SerializableObject(new byte[] { 1, 2, 3 }));
            blobFile.Finish();
            var crc32 = blobFile.Crc32s[0];
            var offset = blobFile.PageOffsets[0];
            await blobPersistentStorage.AddCompleteBlobFile(blobFile);
            await blobPersistentStorage.CheckpointAsync([1], false);
            await cacheProvider.EvictDataFileAsync(0);

            cacheData.BlockWrites();
            var firstRead = cacheProvider.ReadAsync(0, offset, 3, crc32, new LocalCacheTestSerializer());
            var secondRead = cacheProvider.ReadAsync(0, offset, 3, crc32, new LocalCacheTestSerializer());

            // Unblock writes so the first read can complete (and also the second)
            cacheData.UnblockWrites();
            var memory1 = await firstRead;
            var memory2 = await secondRead;

            Assert.Equal(new byte[] { 1, 2, 3 }, memory1.Data);
            Assert.Equal(new byte[] { 1, 2, 3 }, memory2.Data);
            Assert.Equal(1, persistentData.NumberOfReadDataFile);
            Assert.Equal(2, cacheData.NumberOfReadAsync);
        }

        /// <summary>
        /// This test checks if a CRC32 error happens, that the data file is refetched from persistent storage
        /// </summary>
        /// <returns></returns>
        [Fact]
        public async Task ReadAsyncCRCMissmatch()
        {
            var blobFile = new BlobFileWriter((file) => { }, MemoryPool<byte>.Shared, GlobalMemoryManager.Instance);
            blobFile.Write(2, new SerializableObject(new byte[] { 1, 2, 3 }));
            blobFile.Finish();
            var crc32 = blobFile.Crc32s[0];
            var offset = blobFile.PageOffsets[0];
            await blobPersistentStorage.AddCompleteBlobFile(blobFile);
            await blobPersistentStorage.CheckpointAsync([1], false);

            var fileExists = cacheData.TryGetFileData(0, out var fileBytes);
            Assert.True(fileExists);
            Assert.NotNull(fileBytes);

            // Update a byte in the page to have a new CRC to cause a missmatch
            fileBytes[offset + 1] = 5;

            var memory = await cacheProvider.ReadAsync(0, offset, 3, crc32, new LocalCacheTestSerializer());
            Assert.Equal(new byte[] { 1, 2, 3 }, memory.Data);
            Assert.Equal(1, persistentData.NumberOfReadDataFile);
            // We read twice from cache, first time gets invalid CRC, second time is the refetched data
            Assert.Equal(2, cacheData.NumberOfReadAsync);
        }

        [Fact]
        public async Task ReadAsyncCRCMissmatchFromPersistentStorage()
        {
            var blobFile = new BlobFileWriter((file) => { }, MemoryPool<byte>.Shared, GlobalMemoryManager.Instance);
            blobFile.Write(2, new SerializableObject(new byte[] { 1, 2, 3 }));
            blobFile.Finish();
            var crc32 = blobFile.Crc32s[0];
            var offset = blobFile.PageOffsets[0];
            await blobPersistentStorage.AddCompleteBlobFile(blobFile);
            await blobPersistentStorage.CheckpointAsync([1], false);

            var cacheFileExists = cacheData.TryGetFileData(0, out var cacheFileBytes);
            Assert.True(cacheFileExists);
            Assert.NotNull(cacheFileBytes);

            var persistentFileExists = persistentData.TryGetFileData(0, out var persistentFileBytes);
            Assert.True(persistentFileExists);
            Assert.NotNull(persistentFileBytes);

            // Update both local and persistent, so refetch will also get CRC missmatch
            cacheFileBytes[offset + 1] = 5;
            persistentFileBytes[offset + 1] = 5;

            var exception = await Assert.ThrowsAsync<FlowtideChecksumMismatchException>(async () =>
            {
                await cacheProvider.ReadAsync(0, offset, 3, crc32, new LocalCacheTestSerializer());
            });
            Assert.Equal("Invalid CRC32 for page.", exception.Message);
            Assert.Equal(1, persistentData.NumberOfReadDataFile);
            Assert.Equal(2, cacheData.NumberOfReadAsync);
        }

        [Fact]
        public async Task GetMemoryAsyncCRCMissmatch()
        {
            var blobFile = new BlobFileWriter((file) => { }, MemoryPool<byte>.Shared, GlobalMemoryManager.Instance);
            blobFile.Write(2, new SerializableObject(new byte[] { 1, 2, 3 }));
            blobFile.Finish();
            var crc32 = blobFile.Crc32s[0];
            var offset = blobFile.PageOffsets[0];
            await blobPersistentStorage.AddCompleteBlobFile(blobFile);
            await blobPersistentStorage.CheckpointAsync([1], false);

            var fileExists = cacheData.TryGetFileData(0, out var fileBytes);
            Assert.True(fileExists);
            Assert.NotNull(fileBytes);

            // Update a byte in the page to have a new CRC to cause a missmatch
            fileBytes[offset + 1] = 5;

            var memory = await cacheProvider.GetMemoryAsync(0, offset, 3, crc32);
            Assert.Equal(new byte[] { 1, 2, 3 }, memory.ToArray());
            Assert.Equal(1, persistentData.NumberOfReadDataFile);
            // We read twice from cache, first time gets invalid CRC, second time is the refetched data
            Assert.Equal(2, cacheData.NumberOfReadMemory);
        }

        [Fact]
        public async Task GetMemoryAsyncCRCMissmatchFromPersistentStorage()
        {
            var blobFile = new BlobFileWriter((file) => { }, MemoryPool<byte>.Shared, GlobalMemoryManager.Instance);
            blobFile.Write(2, new SerializableObject(new byte[] { 1, 2, 3 }));
            blobFile.Finish();
            var crc32 = blobFile.Crc32s[0];
            var offset = blobFile.PageOffsets[0];
            await blobPersistentStorage.AddCompleteBlobFile(blobFile);
            await blobPersistentStorage.CheckpointAsync([1], false);

            var cacheFileExists = cacheData.TryGetFileData(0, out var cacheFileBytes);
            Assert.True(cacheFileExists);
            Assert.NotNull(cacheFileBytes);

            var persistentFileExists = persistentData.TryGetFileData(0, out var persistentFileBytes);
            Assert.True(persistentFileExists);
            Assert.NotNull(persistentFileBytes);

            // Update both local and persistent, so refetch will also get CRC missmatch
            cacheFileBytes[offset + 1] = 5;
            persistentFileBytes[offset + 1] = 5;

            var exception = await Assert.ThrowsAsync<FlowtideChecksumMismatchException>(async () =>
            {
                await cacheProvider.GetMemoryAsync(0, offset, 3, crc32);
            });
            Assert.Equal("Invalid CRC32 for page.", exception.Message);
            Assert.Equal(1, persistentData.NumberOfReadDataFile);
            Assert.Equal(2, cacheData.NumberOfReadMemory);
        }

        [Fact]
        public async Task EnsureDataInCacheIsReinitializedAfterCrash()
        {
            var blobFile = new BlobFileWriter((file) => { }, MemoryPool<byte>.Shared, GlobalMemoryManager.Instance);
            blobFile.Write(2, new SerializableObject(new byte[] { 1, 2, 3 }));
            blobFile.Finish();
            var crc32 = blobFile.Crc32s[0];
            var offset = blobFile.PageOffsets[0];
            await blobPersistentStorage.AddCompleteBlobFile(blobFile);
            await blobPersistentStorage.CheckpointAsync([1], false);

            // Read once to make sure data is in the cache
            var memory = await cacheProvider.ReadAsync(0, offset, 3, crc32, new LocalCacheTestSerializer());
            Assert.Equal(new byte[] { 1, 2, 3 }, memory.Data);

            Assert.Equal(0, persistentData.NumberOfReadDataFile);
            Assert.Equal(1, cacheData.NumberOfReadAsync);

            {
                var newStorage = new BlobPersistentStorage(new Persistence.ObjectStorage.BlobStorageOptions()
                {
                    FileProvider = persistentData,
                    CacheProvider = cacheData
                });
                var newProvider = blobPersistentStorage.CacheProvider;
                Assert.NotNull(newProvider);
                await newStorage.InitializeAsync(new Persistence.StorageInitializationMetadata("test"));

                var newMemory = await newProvider.ReadAsync(0, offset, 3, crc32, new LocalCacheTestSerializer());
                Assert.Equal(new byte[] { 1, 2, 3 }, newMemory.Data);

                // Check that persistent storage was not read and only the cache data
                Assert.Equal(0, persistentData.NumberOfReadDataFile);
                Assert.Equal(2, cacheData.NumberOfReadAsync);
            }
        }

        [Fact]
        public async Task EnsureZombieFileAreClearedFromCache()
        {
            await cacheData.WriteDataFileAsync(15, 0, 1, PipeReader.Create(new ReadOnlySequence<byte>([1])));
            await blobPersistentStorage.InitializeAsync(new Persistence.StorageInitializationMetadata("test"));

            var fileExists = cacheData.TryGetFileData(15, out _);
            Assert.False(fileExists);
        }

        /// <summary>
        /// Check that the current size updates correctly after a delete
        /// </summary>
        /// <returns></returns>
        [Fact]
        public async Task DeleteFileGivesCorrectCurrentSize()
        {
            var blobFile = new BlobFileWriter((file) => { }, MemoryPool<byte>.Shared, GlobalMemoryManager.Instance);
            blobFile.Write(1, new SerializableObject(new byte[] { 1, 2, 3 }));
            blobFile.Finish();
            await cacheProvider.WriteDataFileAsync(1, blobFile.Crc64, blobFile.FileSize, blobFile);

            await cacheProvider.DeleteDataFileAsync(1);

            Assert.Equal(0, cacheProvider.CurrentSize);
        }
    }
}
