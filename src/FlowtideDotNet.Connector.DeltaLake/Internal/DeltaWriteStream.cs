using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Connector.DeltaLake.Internal
{
    class DeltaWriteStream : Stream
    {
        private readonly Stream _inner;
        private int _position;

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
            return _inner.Read(buffer, offset, count);
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            return _inner.Seek(offset, origin);
        }

        public override void SetLength(long value)
        {
            _inner.SetLength(value);
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            _position += count;
            _inner.Write(buffer, offset, count);
        }
    }
}
