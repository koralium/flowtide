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
using FlowtideDotNet.Storage.StateManager.Internal;
using System.Buffers;
using System.Diagnostics;
using System.IO.Pipelines;
using System.Runtime.InteropServices;

namespace FlowtideDotNet.Storage.Persistence.Reservoir.Internal.DiskReader
{
    internal class LocalDiskReaderUnix : ILocalDiskFile
    {
        private const int O_RDWR = 2;
        private const int O_DIRECT = 16384;
        private const int O_CREAT = 64;
        private const int S_IRUSR = 256;
        private const int S_IWUSR = 128;

        private readonly int fileDescriptor;
        private readonly int alignment;
        private readonly IMemoryAllocator memoryAllocator;
        private bool disposedValue;
        private readonly object _lock = new object();
        private IMemoryOwner<byte>? alignedBuffer;

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

        [DllImport("libc", SetLastError = true)]
        private static extern int ftruncate(int fd, long length);

        public LocalDiskReaderUnix(
            string fileName, 
            int sectorSize, 
            IMemoryAllocator memoryAllocator)
        {
            this.alignment = sectorSize;
            this.memoryAllocator = memoryAllocator;
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

        public async Task Write(PipeReader reader)
        {
            long currentFilePosition = 0;
            int bytesInBuffer = 0;

            if (alignedBuffer == null)
                alignedBuffer = memoryAllocator.Allocate(alignment * 4, alignment);

            while (true)
            {
                ReadResult result = await reader.ReadAsync();
                if (!WriteSequence(ref result, ref currentFilePosition, ref bytesInBuffer, ref reader))
                {
                    break;
                }
            }
        }

        private bool WriteSequence(ref readonly ReadResult result, ref long currentFilePosition, ref int bytesInBuffer, ref PipeReader reader)
        {
            Debug.Assert(alignedBuffer != null, "Aligned buffer should be allocated before writing.");
            ReadOnlySequence<byte> buffer = result.Buffer;
            foreach (var segment in buffer)
            {
                ReadOnlySpan<byte> span = segment.Span;
                while (span.Length > 0)
                {
                    int canCopy = Math.Min(span.Length, alignedBuffer.Memory.Length - bytesInBuffer);
                    span.Slice(0, canCopy).CopyTo(alignedBuffer.Memory.Span.Slice(bytesInBuffer));

                    bytesInBuffer += canCopy;
                    span = span.Slice(canCopy);

                    // Full sida? Skriv och uppdatera position
                    if (bytesInBuffer == alignedBuffer.Memory.Length)
                    {
                        // Här skriver vi på aktuell position
                        Write(currentFilePosition, alignedBuffer.Memory);

                        currentFilePosition += alignedBuffer.Memory.Length; // Flytta framåt för nästa write
                        bytesInBuffer = 0;
                    }
                }
            }
            reader.AdvanceTo(buffer.End);

            if (result.IsCompleted)
            {
                if (bytesInBuffer > 0)
                {
                    int alignedWriteSize = ((bytesInBuffer + alignment - 1) / alignment) * alignment;
                    alignedBuffer.Memory.Span.Slice(bytesInBuffer, alignedWriteSize - bytesInBuffer).Clear();
                    Write(currentFilePosition, alignedBuffer.Memory.Slice(0, alignedWriteSize));
                    ftruncate(fileDescriptor, currentFilePosition + bytesInBuffer);
                }
                return false;
            }
            return true;
        }

        private unsafe void Write(long position, Memory<byte> data)
        {
            var handle = data.Pin();
            try
            {
                lock (_lock)
                {
                    IntPtr bytesWritten = pwrite(fileDescriptor, (nint)handle.Pointer, (IntPtr)data.Length, (IntPtr)position);
                    if (bytesWritten.ToInt64() == -1)
                    {
                        int errorCode = Marshal.GetLastWin32Error();
                        throw new InvalidOperationException($"Write failed with error code {errorCode}: {strerror(errorCode)}");
                    }
                }
            }
            finally
            {
                handle.Dispose();
            }
        }

        private unsafe ReadOnlyMemory<byte> Read(long logicalOffset, int length)
        {
            const int AlignSize = 4096;

            long physicalOffset = (logicalOffset / AlignSize) * AlignSize;

            int relativeOffset = (int)(logicalOffset - physicalOffset);

            long endPosition = logicalOffset + length;
            long physicalEnd = ((endPosition + AlignSize - 1) / AlignSize) * AlignSize;
            int readLength = (int)(physicalEnd - physicalOffset);

            if (alignedBuffer == null || alignedBuffer.Memory.Length < readLength)
            {
                alignedBuffer?.Dispose();
                alignedBuffer = memoryAllocator.Allocate(readLength, AlignSize);
            }

            fixed (byte* ptr = alignedBuffer.Memory.Span)
            {
                int  bytesRead = (int)pread(fileDescriptor, (nint)ptr, (IntPtr)readLength, (IntPtr)physicalOffset);

                if (bytesRead == -1)
                    throw new IOException($"pread failed with errno {Marshal.GetLastPInvokeError()}");

                int availableData = Math.Max(0, bytesRead - relativeOffset);
                int bytesToCopy = Math.Min(length, availableData);

                return alignedBuffer.Memory.Slice(relativeOffset, bytesToCopy);
            }
        }

        public ValueTask<ReadOnlyMemory<byte>> Read(long position, int length, uint crc32)
        {
            lock (_lock)
            {
                var span = Read(position, length).Span;
                CrcUtils.CheckCrc32(span, crc32);
                var bytes = new byte[length];
                span.CopyTo(bytes);
                return ValueTask.FromResult<ReadOnlyMemory<byte>>(bytes);
            }
            
        }

        public ValueTask<T> Read<T>(long position, int length, uint crc32, IStateSerializer<T> serializer) where T : ICacheObject
        {
            lock (_lock)
            {
                var mem = Read(position, length);
                CrcUtils.CheckCrc32(mem.Span, crc32);
                var result = serializer.Deserialize(new ReadOnlySequence<byte>(mem), length);
                return ValueTask.FromResult<T>(result);
            }
        }

        public void Dispose()
        {
            alignedBuffer?.Dispose();
        }
    }
}
