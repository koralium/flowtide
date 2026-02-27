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
using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO.Pipelines;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Storage.Tests.BlobStore
{
    public class BundleFileTests
    {
        public BundleFileTests()
        {
        }

        [Fact]
        public async Task TestWriteAndReadRegistry()
        {
            MergedBlobFileWriter mergedFile = new MergedBlobFileWriter(MemoryPool<byte>.Shared, GlobalMemoryManager.Instance);

            BlobNewCheckpoint newCheckpoint = new BlobNewCheckpoint(MemoryPool<byte>.Shared, GlobalMemoryManager.Instance);
            newCheckpoint.AddFileInformation(new FileInformation(0, 0, 0, 0, 0, 0, 0));
            CheckpointRegistryFile registry = new CheckpointRegistryFile(GlobalMemoryManager.Instance);
            registry.AddCheckpointVersion(new Persistence.ObjectStorage.CheckpointVersion(0, false, 0, false));
            mergedFile.Finish();
            newCheckpoint.FinishForWriting();
            registry.FinishForWriting();

            DataCheckpointBundleFile dataCheckpointBundleFile = new DataCheckpointBundleFile(mergedFile, newCheckpoint, registry);

            // Get the first checkpoint version (It will have updated CRC64 from bundle)
            var firstVersion = registry.First();

            using MemoryStream memoryStream = new MemoryStream();
            await dataCheckpointBundleFile.CopyToAsync(memoryStream);

            var bytes = memoryStream.ToArray();

            var parsedRegistry = await new BundleFileRegistryReader(PipeReader.Create(new ReadOnlySequence<byte>(bytes))).ReadRegistryAsync(GlobalMemoryManager.Instance, default);
            Assert.Equal(firstVersion, parsedRegistry.First());
        }

        [Fact]
        public async Task TestWriteAndReadCheckpointInfo()
        {
            MergedBlobFileWriter mergedFile = new MergedBlobFileWriter(MemoryPool<byte>.Shared, GlobalMemoryManager.Instance);

            BlobNewCheckpoint newCheckpoint = new BlobNewCheckpoint(MemoryPool<byte>.Shared, GlobalMemoryManager.Instance);
            newCheckpoint.AddFileInformation(new FileInformation(0, 0, 0, 0, 0, 0, 0));
            CheckpointRegistryFile registry = new CheckpointRegistryFile(GlobalMemoryManager.Instance);
            registry.AddCheckpointVersion(new Persistence.ObjectStorage.CheckpointVersion(0, false, 0, false));
            mergedFile.Finish();
            newCheckpoint.FinishForWriting();
            registry.FinishForWriting();

            DataCheckpointBundleFile dataCheckpointBundleFile = new DataCheckpointBundleFile(mergedFile, newCheckpoint, registry);

            var expectedFile = new FileInformation(0, 0, 0, 0, 0, 0, newCheckpoint.ChangedFileCrc64[0]);
            using MemoryStream memoryStream = new MemoryStream();
            await dataCheckpointBundleFile.CopyToAsync(memoryStream);

            var bytes = memoryStream.ToArray();

            var checkpointData = await new BundleFileRegistryReader(PipeReader.Create(new ReadOnlySequence<byte>(bytes))).GetCheckpointData(default);
            AssertCheckpointData(expectedFile, checkpointData);
        }

        private void AssertCheckpointData(FileInformation expectedFile, ReadOnlySequence<byte> checkpointData)
        {
            var reader = new CheckpointDataReader(checkpointData);
            if (!reader.TryGetFileInformation(out var fileInfo))
            {
                Assert.Fail("Could not read file information");
            }

            Assert.Equal(expectedFile, fileInfo);
        }
    }
}
