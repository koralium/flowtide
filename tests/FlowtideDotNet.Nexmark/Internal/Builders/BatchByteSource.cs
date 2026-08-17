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

using Microsoft.Win32.SafeHandles;
using System.Buffers;

namespace FlowtideDotNet.Nexmark.Internal.Builders
{
    /// <summary>
    /// Gives out the bytes of a file to read batches from.
    /// </summary>
    public interface IBatchByteSource : IDisposable
    {
        /// <summary>
        /// The bytes that can be read right now.
        /// </summary>
        ValueTask<ReadOnlySequence<byte>> ReadAsync();

        /// <summary>
        /// True when no more bytes can arrive.
        /// </summary>
        bool IsCompleted { get; }

        /// <summary>
        /// Called when the buffer did not hold a full batch.
        /// </summary>
        void NeedMoreData(in ReadOnlySequence<byte> buffer);

        /// <summary>
        /// Moves past a batch that has been read.
        /// </summary>
        void Consume(int length);
    }

    /// <summary>
    /// Reads from a mapped file, skipped columns are never touched.
    /// </summary>
    public sealed class MappedByteSource : IBatchByteSource
    {
        private readonly MappedFileReader _reader;
        private ReadOnlySequence<byte> _buffer;

        public MappedByteSource(string fileName)
        {
            _reader = new MappedFileReader(fileName);
            _buffer = _reader.Sequence;
        }

        // The whole file is there from the start
        public bool IsCompleted => true;

        public ValueTask<ReadOnlySequence<byte>> ReadAsync()
        {
            return new ValueTask<ReadOnlySequence<byte>>(_buffer);
        }

        public void NeedMoreData(in ReadOnlySequence<byte> buffer)
        {
        }

        public void Consume(int length)
        {
            _buffer = _buffer.Slice(length);
        }

        public void Dispose()
        {
            _reader.Dispose();
        }
    }

    /// <summary>
    /// Copies the file into a buffer, every byte is read.
    /// </summary>
    public sealed class PipeByteSource : IBatchByteSource
    {
        private readonly SafeFileHandle _handle;
        private readonly FilePipeReader _reader;
        private bool _isCompleted;

        public PipeByteSource(string fileName, int bufferSize)
        {
            _handle = File.OpenHandle(fileName, FileMode.Open, FileAccess.Read, FileShare.Read, FileOptions.Asynchronous);
            _reader = new FilePipeReader(_handle, bufferSize);
        }

        public bool IsCompleted => _isCompleted;

        public async ValueTask<ReadOnlySequence<byte>> ReadAsync()
        {
            var result = await _reader.ReadAsync();
            _isCompleted = result.IsCompleted;
            return result.Buffer;
        }

        public void NeedMoreData(in ReadOnlySequence<byte> buffer)
        {
            _reader.AdvanceTo(buffer.Start, buffer.End);
        }

        public void Consume(int length)
        {
            _reader.SkipForward(length);
        }

        public void Dispose()
        {
            _reader.Complete();
            _handle.Dispose();
        }
    }
}
