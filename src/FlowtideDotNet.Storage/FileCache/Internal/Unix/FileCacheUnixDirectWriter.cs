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
using System.Buffers;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.InteropServices;

namespace FlowtideDotNet.Storage.FileCache.Internal.Unix
{
    internal class FileCacheUnixDirectWriter : IFileCacheWriter
    {
        private const int O_RDWR = 2;
        private const int O_DIRECT = 16384;
        private const int O_CREAT = 64;
        private const int S_IRUSR = 256;
        private const int S_IWUSR = 128;

        private readonly int fileDescriptor;
        private readonly int alignment;
        private AlignedBuffer? alignedBuffer;
        private bool disposedValue;
        private readonly object _lock = new object();

        [DllImport("libc", SetLastError = true)]
        private static extern int open([MarshalAs(UnmanagedType.LPStr)] string pathname, int flags, uint mode);

        [DllImport("libc", SetLastError = true)]
        private static extern int close(int fd);

        [DllImport("libc", SetLastError = true)]
        private static extern IntPtr pread(int fd, IntPtr buffer, IntPtr count, IntPtr offset);

        [DllImport("libc", SetLastError = true)]
        private static extern IntPtr pwrite(int fd, IntPtr buffer, IntPtr count, IntPtr offset);

        [DllImport("libc")]
        private static extern IntPtr strerror(int errnum);

        private static string? GetErrorMessage(int errorCode)
        {
            return Marshal.PtrToStringAnsi(strerror(errorCode));
        }


        public FileCacheUnixDirectWriter(string fileName, int sectorSize, FileCacheOptions fileCacheOptions)
        {
            this.alignment = sectorSize;

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

            this.fileDescriptor = open(fileName, O_RDWR | O_DIRECT | O_CREAT, S_IRUSR | S_IWUSR);
            if (this.fileDescriptor == -1)
            {
                int errorCode = Marshal.GetLastWin32Error();

                throw new InvalidOperationException($"Open failed with error code {errorCode}: {strerror(errorCode)}");
            }
        }

        public unsafe void Write(long position, Memory<byte> data)
        {
            lock (_lock)
            {
                IntPtr bytesWritten = pwrite(fileDescriptor, (nint)data.Pin().Pointer, (IntPtr)data.Length, (IntPtr)position);
                if (bytesWritten.ToInt64() <= 0)
                {
                    int errorCode = Marshal.GetLastWin32Error();

                    throw new InvalidOperationException($"Failed to write data. {errorCode}: {strerror(errorCode)}");
                }
            }
        }

        public ReadOnlyMemory<byte> Read(long position, int length)
        {
            lock (_lock)
            {
                ReadIntoAlignedBuffer_NoLock(position, length);

                byte[] buffer = new byte[length];
                Marshal.Copy(alignedBuffer.Buffer, buffer, 0, length);
                return buffer;
            }
        }

        [MemberNotNull(nameof(alignedBuffer))]
        private void ReadIntoAlignedBuffer_NoLock(long position, int length)
        {
            if (position % alignment != 0)
            {
                throw new ArgumentException("Offset must be aligned to block size.");
            }
            var alignedLength = (length + alignment - 1) / alignment * alignment;

            if (alignedBuffer == null)
            {
                alignedBuffer = new AlignedBuffer(alignedLength, alignment);
            }

            if (alignedLength > alignedBuffer.Size)
            {
                (alignedBuffer as IDisposable).Dispose();
                alignedBuffer = new AlignedBuffer(alignedLength, alignment);
            }

            IntPtr bytesRead = pread(fileDescriptor, alignedBuffer.Buffer, (IntPtr)alignedLength, (IntPtr)position);
        }

        public void Flush()
        {
            // Flush is not required in direct i/o
        }

        public void ClearTemporaryAllocations()
        {
            lock (_lock)
            {
                if (alignedBuffer != null)
                {
                    (alignedBuffer as IDisposable).Dispose();
                    alignedBuffer = null;
                }
            }
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                lock (_lock)
                {
                    if (alignedBuffer != null)
                    {
                        (alignedBuffer as IDisposable).Dispose();
                        alignedBuffer = null;
                    }
                    if (fileDescriptor != -1)
                    {
                        close(fileDescriptor);
                    }
                }
                disposedValue = true;
            }
        }
        ~FileCacheUnixDirectWriter()
        {
            // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
            Dispose(disposing: false);
        }

        public void Dispose()
        {
            // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }

        public T Read<T>(long position, int length, IStateSerializer<T> serializer) where T : ICacheObject
        {
            lock (_lock)
            {
                ReadIntoAlignedBuffer_NoLock(position, length);

                return serializer.Deserialize(alignedBuffer.Memory, length);
            }
            throw new NotImplementedException();
        }
    }
}
