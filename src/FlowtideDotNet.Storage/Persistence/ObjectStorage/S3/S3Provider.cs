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

using FlowtideDotNet.Storage.StateManager.Internal;
using System;
using System.Collections.Generic;
using System.IO.Pipelines;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Storage.Persistence.ObjectStorage.S3
{
    internal class S3Provider : IFileStorageProvider
    {
        private HttpClient _httpClient;
        public S3Provider()
        {
            _httpClient = new HttpClient();
        }

        public Task DeleteCheckpointFileAsync(CheckpointVersion checkpointVersion)
        {
            throw new NotImplementedException();
        }

        public Task DeleteDataFileAsync(long fileId)
        {
            throw new NotImplementedException();
        }

        public ValueTask<ReadOnlyMemory<byte>> GetMemoryAsync(long fileId, int offset, int length, uint crc32)
        {
            throw new NotImplementedException();
        }

        public Task<IEnumerable<CheckpointVersion>> ListCheckpointVersionsAsync()
        {
            throw new NotImplementedException();
        }

        public ValueTask<T> ReadAsync<T>(long fileId, int offset, int length, uint crc32, IStateSerializer<T> stateSerializer) where T : ICacheObject
        {
            throw new NotImplementedException();
        }

        public Task<PipeReader> ReadCheckpointFileAsync(CheckpointVersion checkpointVersion)
        {
            throw new NotImplementedException();
        }

        public Task<PipeReader?> ReadCheckpointRegistryFileAsync()
        {
            throw new NotImplementedException();
        }

        public Task<PipeReader> ReadDataFileAsync(long fileId)
        {
            throw new NotImplementedException();
        }

        public Task WriteCheckpointFileAsync(CheckpointVersion checkpointVersion, PipeReader data)
        {
            throw new NotImplementedException();
        }

        public Task WriteCheckpointRegistryFile(PipeReader data)
        {
            throw new NotImplementedException();
        }

        public Task WriteDataFileAsync(long fileId, ulong crc64, PipeReader data)
        {
            throw new NotImplementedException();
        }
    }
}
