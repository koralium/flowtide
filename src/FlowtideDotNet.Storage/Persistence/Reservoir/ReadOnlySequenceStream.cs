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

namespace FlowtideDotNet.Storage.Persistence.Reservoir
{
    public class ReadOnlySequenceStream : Stream
    {
        private readonly ReadOnlySequence<byte> _sequence;
        private SequencePosition _currentPosition;
        private long _position;

        public ReadOnlySequenceStream(ReadOnlySequence<byte> sequence)
        {
            _sequence = sequence;
            _currentPosition = sequence.Start;
            _position = 0;
        }

        public override bool CanRead => true;

        public override bool CanSeek => true;

        public override bool CanWrite => false;

        public override long Length => _sequence.Length;

        public override long Position
        {
            get => _position;
            set => Seek(value, SeekOrigin.Begin);
        }

        public override void Flush()
        {
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            var remaining = _sequence.Slice(_currentPosition);
            if (remaining.IsEmpty)
            {
                return 0;
            }

            var destination = buffer.AsSpan(offset, count);
            int bytesRead;

            if (remaining.Length <= destination.Length)
            {
                bytesRead = (int)remaining.Length;
                remaining.CopyTo(destination);
                _currentPosition = _sequence.End;
            }
            else
            {
                bytesRead = destination.Length;
                remaining.Slice(0, bytesRead).CopyTo(destination);
                _currentPosition = _sequence.GetPosition(bytesRead, _currentPosition);
            }

            _position += bytesRead;
            return bytesRead;
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            long newPosition = origin switch
            {
                SeekOrigin.Begin => offset,
                SeekOrigin.Current => _position + offset,
                SeekOrigin.End => _sequence.Length + offset,
                _ => throw new ArgumentOutOfRangeException(nameof(origin))
            };

            if (newPosition < 0 || newPosition > _sequence.Length)
            {
                throw new ArgumentOutOfRangeException(nameof(offset));
            }

            _position = newPosition;
            _currentPosition = _sequence.GetPosition(newPosition);
            return _position;
        }

        public override int Read(Span<byte> buffer)
        {
            var remaining = _sequence.Slice(_currentPosition);
            if (remaining.IsEmpty)
            {
                return 0;
            }

            int bytesRead;

            if (remaining.Length <= buffer.Length)
            {
                bytesRead = (int)remaining.Length;
                remaining.CopyTo(buffer);
                _currentPosition = _sequence.End;
            }
            else
            {
                bytesRead = buffer.Length;
                remaining.Slice(0, bytesRead).CopyTo(buffer);
                _currentPosition = _sequence.GetPosition(bytesRead, _currentPosition);
            }

            _position += bytesRead;
            return bytesRead;
        }

        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return Task.FromResult(Read(buffer, offset, count));
        }

        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return new ValueTask<int>(Read(buffer.Span));
        }

        public override void SetLength(long value)
        {
            throw new NotSupportedException();
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            throw new NotSupportedException();
        }
    }
}
