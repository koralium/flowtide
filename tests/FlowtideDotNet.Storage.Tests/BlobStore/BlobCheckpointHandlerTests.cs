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
using FlowtideDotNet.Storage.Persistence.ObjectStorage;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Storage.Tests.BlobStore
{
    public class BlobCheckpointHandlerTests
    {
        [Fact]
        public async Task Test()
        {
            CheckpointHandler checkpointHandler = new CheckpointHandler(GlobalMemoryManager.Instance);

            BlobFileWriter blobFileWriter = new BlobFileWriter(MemoryPool<byte>.Shared, GlobalMemoryManager.Instance);
            blobFileWriter.Write(1, new SerializableObject(new byte[] { 1, 2, 3, 4 }));
            blobFileWriter.Finish();

            await checkpointHandler.EnqueueFileAsync(blobFileWriter);

            await checkpointHandler.FinishCheckpoint();

        }
    }
}
