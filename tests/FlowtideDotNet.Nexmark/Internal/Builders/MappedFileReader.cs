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

using System.Buffers;
using System.IO.MemoryMappedFiles;

namespace FlowtideDotNet.Nexmark.Internal.Builders
{
    /// <summary>
    /// Maps a file and gives out its content as one sequence.
    /// Nothing is copied, the pages are read when they are touched.
    /// </summary>
    public sealed unsafe class MappedFileReader : IDisposable
    {
        // A segment length must fit in an int
        private const long SegmentLength = 512L * 1024 * 1024;

        private readonly MemoryMappedFile? _memoryMappedFile;
        private readonly MemoryMappedViewAccessor? _accessor;
        private byte* _pointer;
        private bool _disposed;

        public MappedFileReader(string fileName)
        {
            var fileLength = new FileInfo(fileName).Length;

            if (fileLength == 0)
            {
                Sequence = ReadOnlySequence<byte>.Empty;
                return;
            }

            _memoryMappedFile = MemoryMappedFile.CreateFromFile(fileName, FileMode.Open, null, 0, MemoryMappedFileAccess.Read);
            _accessor = _memoryMappedFile.CreateViewAccessor(0, 0, MemoryMappedFileAccess.Read);
            _accessor.SafeMemoryMappedViewHandle.AcquirePointer(ref _pointer);

            var start = _pointer + _accessor.PointerOffset;

            // The view is rounded up to whole pages, the file length decides where the data ends
            MappedSegment? first = default;
            MappedSegment? last = default;
            long offset = 0;
            while (offset < fileLength)
            {
                var length = (int)Math.Min(SegmentLength, fileLength - offset);
                var segment = new MappedSegment(start + offset, length, offset);

                if (last == null)
                {
                    first = segment;
                }
                else
                {
                    last.SetNext(segment);
                }
                last = segment;
                offset += length;
            }

            Sequence = new ReadOnlySequence<byte>(first!, 0, last!, last!.Memory.Length);
        }

        /// <summary>
        /// The whole file, slice it to move forward.
        /// </summary>
        public ReadOnlySequence<byte> Sequence { get; }

        public void Dispose()
        {
            if (_disposed)
            {
                return;
            }
            _disposed = true;

            if (_accessor != null)
            {
                if (_pointer != null)
                {
                    _accessor.SafeMemoryMappedViewHandle.ReleasePointer();
                    _pointer = default;
                }
                _accessor.Dispose();
            }
            _memoryMappedFile?.Dispose();
        }

        private sealed class MappedSegment : ReadOnlySequenceSegment<byte>
        {
            public MappedSegment(byte* pointer, int length, long runningIndex)
            {
                Memory = new MappedMemoryManager(pointer, length).Memory;
                RunningIndex = runningIndex;
            }

            public void SetNext(MappedSegment next)
            {
                Next = next;
            }
        }

        /// <summary>
        /// Gives a memory over a part of the mapped file.
        /// </summary>
        private sealed class MappedMemoryManager : MemoryManager<byte>
        {
            private readonly byte* _pointer;
            private readonly int _length;

            public MappedMemoryManager(byte* pointer, int length)
            {
                _pointer = pointer;
                _length = length;
            }

            public override Span<byte> GetSpan()
            {
                return new Span<byte>(_pointer, _length);
            }

            // The mapping holds the pages in place, so pinning does nothing
            public override MemoryHandle Pin(int elementIndex = 0)
            {
                return new MemoryHandle(_pointer + elementIndex);
            }

            public override void Unpin()
            {
            }

            protected override void Dispose(bool disposing)
            {
            }
        }
    }
}
