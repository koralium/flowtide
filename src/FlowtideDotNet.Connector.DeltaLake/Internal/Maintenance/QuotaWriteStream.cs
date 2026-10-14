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

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance
{
    /// <summary>
    /// Writes a local file, reserving every growth in the disk ledger first.
    /// </summary>
    internal sealed class QuotaWriteStream : Stream
    {
        private readonly Stream _inner;
        private readonly DiskLedger _ledger;

        public QuotaWriteStream(Stream inner, DiskLedger ledger)
        {
            _inner = inner;
            _ledger = ledger;
        }

        /// <summary>
        /// The bytes this file holds in the ledger.
        /// </summary>
        public long Charged { get; private set; }

        public override bool CanRead => false;

        public override bool CanSeek => _inner.CanSeek;

        public override bool CanWrite => true;

        public override long Length => _inner.Length;

        public override long Position
        {
            get => _inner.Position;
            set => _inner.Position = value;
        }

        private void Charge(long end)
        {
            if (end > Charged)
            {
                _ledger.Reserve(end - Charged);
                Charged = end;
            }
        }

        private long End(int count) => checked(_inner.Position + count);

        public override void Write(byte[] buffer, int offset, int count)
        {
            Charge(End(count));
            _inner.Write(buffer, offset, count);
        }

        public override void Write(ReadOnlySpan<byte> buffer)
        {
            Charge(End(buffer.Length));
            _inner.Write(buffer);
        }

        public override void WriteByte(byte value)
        {
            Charge(End(1));
            _inner.WriteByte(value);
        }

        public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            Charge(End(count));
            return _inner.WriteAsync(buffer, offset, count, cancellationToken);
        }

        public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
        {
            Charge(End(buffer.Length));
            return _inner.WriteAsync(buffer, cancellationToken);
        }

        public override void SetLength(long value)
        {
            Charge(value);
            _inner.SetLength(value);
        }

        public override void Flush() => _inner.Flush();

        public override Task FlushAsync(CancellationToken cancellationToken) => _inner.FlushAsync(cancellationToken);

        public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();

        public override long Seek(long offset, SeekOrigin origin) => _inner.Seek(offset, origin);

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                _inner.Dispose();
            }
            base.Dispose(disposing);
        }

        public override async ValueTask DisposeAsync()
        {
            await _inner.DisposeAsync();
            await base.DisposeAsync();
        }
    }
}
