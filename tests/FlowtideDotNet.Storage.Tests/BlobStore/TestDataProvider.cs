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

using FlowtideDotNet.Storage.Persistence.ObjectStorage.MemoryDisk;
using FlowtideDotNet.Storage.StateManager.Internal;
using System;
using System.Collections.Generic;
using System.IO.Pipelines;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Storage.Tests.BlobStore
{
    internal class TestDataProvider : MemoryFileProvider
    {
        private TaskCompletionSource? _writeBlock;
        private int _numberOfReadMemory;
        private int _numberOfReadAsync;
        private int _numberOfReadDataFile;

        public int NumberOfReadMemory => Volatile.Read(ref _numberOfReadMemory);

        public int NumberOfReadAsync => Volatile.Read(ref _numberOfReadAsync);

        public int NumberOfReadDataFile => Volatile.Read(ref _numberOfReadDataFile);

        public void BlockWrites()
        {
            _writeBlock = new TaskCompletionSource();
        }

        public void UnblockWrites()
        {
            _writeBlock?.SetResult();
            _writeBlock = null;
        }

        public override ValueTask<ReadOnlyMemory<byte>> GetMemoryAsync(long fileId, int offset, int length, uint crc32)
        {
            Interlocked.Increment(ref _numberOfReadMemory);
            return base.GetMemoryAsync(fileId, offset, length, crc32);
        }

        public override ValueTask<T> ReadAsync<T>(long fileId, int offset, int length, uint crc32, IStateSerializer<T> stateSerializer)
        {
            Interlocked.Increment(ref _numberOfReadAsync);
            return base.ReadAsync(fileId, offset, length, crc32, stateSerializer);
        }

        public override Task<PipeReader> ReadDataFileAsync(long fileId)
        {
            Interlocked.Increment(ref _numberOfReadDataFile);
            return base.ReadDataFileAsync(fileId);
        }

        public override async Task WriteDataFileAsync(long fileId, ulong crc64, int size, PipeReader data)
        {
            if (_writeBlock != null)
            {
                await _writeBlock.Task;
            }

            await base.WriteDataFileAsync(fileId, crc64, size, data);
        }
    }
}
