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

using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Actions;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Checkpoints;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance;
using ParquetSharp;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public class QuotaWriteStreamTests
    {
        // Records the ledger's reservation at the moment each write reaches the file
        private sealed class ObservingStream : MemoryStream
        {
            private readonly DiskLedger _ledger;

            public ObservingStream(DiskLedger ledger)
            {
                _ledger = ledger;
            }

            public List<(long End, long Reserved)> Writes { get; } = new List<(long End, long Reserved)>();

            public int FailAfterBytes { get; set; } = -1;

            private void Observe(int count)
            {
                Writes.Add((Position + count, _ledger.Reserved));
            }

            public override void Write(byte[] buffer, int offset, int count)
            {
                Observe(count);
                if (FailAfterBytes >= 0 && Position + count > FailAfterBytes)
                {
                    var partial = (int)Math.Max(0, FailAfterBytes - Position);
                    base.Write(buffer, offset, partial);
                    throw new IOException("disk failure");
                }
                base.Write(buffer, offset, count);
            }

            public override void Write(ReadOnlySpan<byte> buffer)
            {
                Observe(buffer.Length);
                base.Write(buffer);
            }

            public override void WriteByte(byte value)
            {
                Observe(1);
                base.WriteByte(value);
            }

            public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
            {
                Observe(count);
                return base.WriteAsync(buffer, offset, count, cancellationToken);
            }

            public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
            {
                Observe(buffer.Length);
                return base.WriteAsync(buffer, cancellationToken);
            }

            public override void SetLength(long value)
            {
                Writes.Add((value, _ledger.Reserved));
                base.SetLength(value);
            }
        }

        [Fact]
        public async Task EveryWriteOverloadReservesFirst()
        {
            var ledger = new DiskLedger();
            var inner = new ObservingStream(ledger);
            using var stream = new QuotaWriteStream(inner, ledger);
            stream.Write(new byte[10], 0, 10);
            stream.Write(new byte[5].AsSpan());
            stream.WriteByte(1);
            await stream.WriteAsync(new byte[7], 0, 7);
            await stream.WriteAsync(new ReadOnlyMemory<byte>(new byte[3]));
            stream.SetLength(100);

            Assert.True(inner.Writes.Count >= 6);
            Assert.All(inner.Writes, x => Assert.True(x.Reserved >= x.End, $"{x.Reserved} reserved for a write ending at {x.End}"));
            Assert.Equal(100, stream.Charged);
            Assert.Equal(100, ledger.Reserved);
        }

        [Fact]
        public void PartialWriteFailureKeepsItsCharge()
        {
            var ledger = new DiskLedger();
            var inner = new ObservingStream(ledger) { FailAfterBytes = 15 };
            var stream = new QuotaWriteStream(inner, ledger);
            stream.Write(new byte[10], 0, 10);
            Assert.Throws<IOException>(() => stream.Write(new byte[10], 0, 10));
            stream.Dispose();

            // Released only after the file is deleted
            Assert.Equal(20, stream.Charged);
            Assert.Equal(20, ledger.Reserved);
        }

        [Fact]
        public void WriterCloseFailureSurfaces()
        {
            // Room for opening the file but not for the last group and the footer, which only Close writes
            var ledger = new DiskLedger(CheckpointFileWriter.OpenBytes + 1024);
            var path = Path.Combine(Path.GetTempPath(), $"close-failure-{Guid.NewGuid():N}.parquet");
            try
            {
                using var writer = new CheckpointFileWriter(path, ledger, new JobMemory(), Compression.Zstd, null);
                foreach (var record in new[] { new DeltaAction() { Protocol = new DeltaProtocolAction() { MinReaderVersion = 1, MinWriterVersion = 2 } }, new DeltaAction() { MetaData = new DeltaMetadataAction() { Id = "x" } } }.SelectMany(x => TestRecords.FromAction(x)))
                {
                    writer.Append(record);
                }
                var declined = Assert.Throws<CheckpointDeclinedException>(() => writer.Close());
                Assert.Equal(MaintenanceReason.DiskCap, declined.Reason);
                Assert.False(writer.Closed);
            }
            finally
            {
                File.Delete(path);
            }
        }
    }
}
