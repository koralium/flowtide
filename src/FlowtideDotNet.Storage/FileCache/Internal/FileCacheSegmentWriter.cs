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

using FlowtideDotNet.Storage.FileCache.Internal;

namespace FlowtideDotNet.Storage.FileCache
{
    /// <summary>
    /// Handles reading and writing to a single segment file
    /// </summary>
    internal class FileCacheSegmentWriter : IFileCacheWriter
    {
        private FileStream fileStream;
        private bool disposedValue;
        private SemaphoreSlim semaphoreSlim;

        public FileCacheSegmentWriter(string fileName, FileCacheOptions fileCacheOptions)
        {
            semaphoreSlim = new SemaphoreSlim(1, 1);
            var directoryName = Path.GetDirectoryName(fileName);
            if (directoryName != null && !Directory.Exists(directoryName))
            {
                Directory.CreateDirectory(directoryName);
            }

            // Check if the file already exists, if so delete it
            if (File.Exists(fileName))
            {
                try
                {
                    File.Delete(fileName);
                }
                catch
                {
                    File.Delete(fileName);
                }
            }

            fileStream = new FileStream(fileName, FileMode.Create, FileAccess.ReadWrite, fileCacheOptions.FileShare, 512, FileOptions.DeleteOnClose | FileOptions.RandomAccess);
        }

        public void Write(long position, byte[] data)
        {
            semaphoreSlim.Wait();
            try
            {
                fileStream.Position = position;
                fileStream.Write(data);
            }
            finally
            {
                semaphoreSlim.Release();
            }
        }

        public byte[] Read(long position, int length)
        {
            semaphoreSlim.Wait();
            try
            {
                var bytes = new byte[length];
                fileStream.Position = position;
                fileStream.Read(bytes);
                return bytes;
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

        public void Flush()
        {
            semaphoreSlim.Wait();
            // Flush data
            fileStream.Flush(true);
            // Clear cache
            PosixUnix.SetAdvice(fileStream.SafeFileHandle, 0, 0, PosixUnix.FileAdvice.POSIX_FADV_DONTNEED);
            semaphoreSlim.Release();
        }

        public void Dispose()
        {
            // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }
    }
}
