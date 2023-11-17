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

using System;
using System.Collections.Generic;
using System.Drawing;
using System.IO;
using System.IO.Pipes;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

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
        private AlignedBuffer alignedBuffer;
        private readonly object _lock = new object();

        [DllImport("libc", SetLastError = true)]
        private static extern int open([MarshalAs(UnmanagedType.LPStr)] string pathname, int flags, uint mode);

        [DllImport("libc", SetLastError = true)]
        private static extern int close(int fd);

        [DllImport("libc", SetLastError = true)]
        private static extern IntPtr pread(int fd, IntPtr buffer, IntPtr count, IntPtr offset);

        [DllImport("libc", SetLastError = true)]
        private static extern IntPtr pwrite(int fd, IntPtr buffer, IntPtr count, IntPtr offset);

        public FileCacheUnixDirectWriter(string fileName, int sectorSize, FileCacheOptions fileCacheOptions)
        {
            this.alignment = sectorSize;
            this.fileDescriptor = open(fileName, O_RDWR | O_DIRECT | O_CREAT, S_IRUSR | S_IWUSR);
            if (this.fileDescriptor == -1)
            {
                throw new InvalidOperationException("Unable to open file.");
            }
            alignedBuffer = new AlignedBuffer(sectorSize * 1024, sectorSize);
        }

        public void Write(long position, byte[] data)
        {
            lock (_lock)
            {
                var alignedLength = (data.Length + alignment - 1) / alignment * alignment;

                if (alignedLength > alignedBuffer.Size)
                {
                    alignedBuffer.Dispose();
                    alignedBuffer = new AlignedBuffer(alignedLength, alignment);
                }

                if (position % alignment != 0)
                {
                    throw new ArgumentException("Position must be aligned to block size.");
                }
                Marshal.Copy(data, 0, alignedBuffer.Buffer, data.Length);
                IntPtr bytesWritten = pwrite(fileDescriptor, alignedBuffer.Buffer, (IntPtr)data.Length, (IntPtr)position);
                if (bytesWritten.ToInt64() <= 0)
                {
                    throw new InvalidOperationException("Failed to write data.");
                }
            }
        }

        public byte[] Read(long position, int length)
        {
            lock (_lock)
            {
                if (position % alignment != 0)
                {
                    throw new ArgumentException("Offset must be aligned to block size.");
                }
                var alignedLength = (length + alignment - 1) / alignment * alignment;

                if (alignedLength > alignedBuffer.Size)
                {
                    alignedBuffer.Dispose();
                    alignedBuffer = new AlignedBuffer(alignedLength, alignment);
                }

                IntPtr bytesRead = pread(fileDescriptor, alignedBuffer.Buffer, (IntPtr)alignedLength, (IntPtr)position);
                if (bytesRead.ToInt64() <= 0)
                {
                    return Array.Empty<byte>(); // End of file or error
                }

                byte[] buffer = new byte[length];
                Marshal.Copy(alignedBuffer.Buffer, buffer, 0, length);
                return buffer;
            }
        }

        public void Dispose()
        {
            lock (_lock)
            {
                alignedBuffer.Dispose();
                if (fileDescriptor != -1)
                {
                    close(fileDescriptor);
                }
            }
        }

        public void Flush()
        {
            // Flush is not required in direct i/o
        }
    }
}
