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
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Storage.Persistence.ObjectStorage.Internal.DiskReader
{
    /// <summary>
    /// Managed reader of local disk items
    /// Used to fetch pages from offsets
    /// </summary>
    internal class LocalDiskReaderManaged : ILocalDiskReader
    {
        private readonly string fileName;
        private readonly FileStream fileStream;
        private SemaphoreSlim semaphoreSlim;
        private bool disposedValue;

        public LocalDiskReaderManaged(string fileName)
        {
            this.fileName = fileName;
            fileStream = new FileStream(fileName, FileMode.Open, FileAccess.Read, FileShare.Read, 4096, FileOptions.RandomAccess);
            semaphoreSlim = new SemaphoreSlim(1, 1);
        }

        public async ValueTask<ReadOnlyMemory<byte>> Read(long position, int length)
        {
            await semaphoreSlim.WaitAsync();
            try
            {
                var bytes = new byte[length];
                fileStream.Position = position;
                await fileStream.ReadExactlyAsync(bytes);
                return bytes;
            }
            finally
            {
                semaphoreSlim.Release();
            }
        }

        public async ValueTask<T> Read<T>(long position, int length, IStateSerializer<T> serializer) where T : ICacheObject
        {
            await semaphoreSlim.WaitAsync();
            try
            {
                var bytes = new byte[length];
                fileStream.Position = position;
                await fileStream.ReadExactlyAsync(bytes);
                return serializer.Deserialize(new ReadOnlySequence<byte>(bytes), bytes.Length);
            }
            finally
            {
                semaphoreSlim.Release();
            }
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    semaphoreSlim.Wait();
                    fileStream.Dispose();
                    semaphoreSlim.Release();
                }
                disposedValue = true;
            }
        }

        public void Dispose()
        {
            // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }
    }
}
