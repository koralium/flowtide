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
using System.IO.Hashing;
using System.IO.Pipelines;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Storage.Persistence.Reservoir.Internal.DiskReader
{
    /// <summary>
    /// Managed reader of local disk items
    /// Used to fetch pages from offsets
    /// </summary>
    internal class LocalDiskReaderManaged : ILocalDiskFile
    {
        private readonly string fileName;
        private readonly FileStream fileStream;
        private SemaphoreSlim semaphoreSlim;
        private bool disposedValue;
        private byte[] _buffer;

        public LocalDiskReaderManaged(string fileName)
        {
            this.fileName = fileName;
            fileStream = new FileStream(fileName, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite, 4096, FileOptions.RandomAccess);
            semaphoreSlim = new SemaphoreSlim(1, 1);
            _buffer = new byte[4096];
        }

        public async ValueTask<ReadOnlyMemory<byte>> Read(long position, int length, uint crc32)
        {
            await semaphoreSlim.WaitAsync();
            try
            {
                var bytes = new byte[length];
                fileStream.Position = position;
                await fileStream.ReadExactlyAsync(bytes);
                CrcUtils.CheckCrc32(bytes, crc32);
                return bytes;
            }
            finally
            {
                semaphoreSlim.Release();
            }
        }

        public async ValueTask<T> Read<T>(long position, int length, uint crc32, IStateSerializer<T> serializer) where T : ICacheObject
        {
            await semaphoreSlim.WaitAsync();
            try
            {
                if (_buffer.Length < length)
                {
                    _buffer = new byte[length];
                }
                fileStream.Position = position;
                var slice = _buffer.AsMemory().Slice(0, length);
                await fileStream.ReadExactlyAsync(slice);
                CrcUtils.CheckCrc32(slice.Span, crc32);
                return serializer.Deserialize(new ReadOnlySequence<byte>(slice), length);
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

        public async Task Write(PipeReader reader)
        {
            await semaphoreSlim.WaitAsync();
            try
            {
                fileStream.Seek(0, SeekOrigin.Begin);
                await reader.CopyToAsync(fileStream);
                reader.Complete();
            }
            finally
            {
                semaphoreSlim.Release();
            }
        }

        public Task<PipeReader> ReadFile(CancellationToken cancellationToken = default)
        {
            return Task.FromResult(PipeReader.Create(File.OpenRead(fileName)));
        }
    }
}
