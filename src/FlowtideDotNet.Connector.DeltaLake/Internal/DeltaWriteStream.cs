using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Connector.DeltaLake.Internal
{
    internal class DeltaWriteStream : Stream
    {
        private readonly Stream _inner;
        private long _position;

        public DeltaWriteStream(Stream inner)
        {
            this._inner = inner;
            _position = 0;
        }

        public override bool CanRead => _inner.CanRead;

        public override bool CanSeek => _inner.CanSeek;

        public override bool CanWrite => _inner.CanWrite;

        public override long Length => _inner.Length;

        public override long Position 
        {
            get
            {
                return _position;
            }
            set
            {
                throw new NotImplementedException();
            }
        }

        public override void Flush()
        {
            _inner.Flush();
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            int bytesRead = _inner.Read(buffer, offset, count);
            _position += bytesRead;
            return bytesRead;
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            _position = _inner.Seek(offset, origin);
            return _position;
        }

        public override void SetLength(long value)
        {
            _inner.SetLength(value);
            if (_position > value)
            {
                _position = value;
            }
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            _inner.Write(buffer, offset, count);
            _position += count;
        }

        public override void WriteByte(byte value)
        {
            _inner.WriteByte(value);
            _position += 1;
        }

        public override void Write(ReadOnlySpan<byte> buffer)
        {
            _inner.Write(buffer);
            _position += buffer.Length;
        }

        public override async Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            await _inner.WriteAsync(buffer, offset, count, cancellationToken).ConfigureAwait(false);
            _position += count;
        }

        public override async ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
        {
            await _inner.WriteAsync(buffer, cancellationToken).ConfigureAwait(false);
            _position += buffer.Length;
        }

        public override Task FlushAsync(CancellationToken cancellationToken)
        {
            return _inner.FlushAsync(cancellationToken);
        }
    }
}
