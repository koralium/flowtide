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

using FlowtideDotNet.Connector.DeltaLake.Internal.Delta;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Checkpoints;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Utils;
using Stowage;
using System.Security.Cryptography;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance
{
    /// <summary>
    /// The complete-content proof of one listed log object: its exact path, the identity the listing gave it, its length and its SHA-256.
    /// </summary>
    internal sealed class ContentProof
    {
        public ContentProof(IOPath path, long version, bool isCheckpoint, ObjectIdentity listed, long length, byte[] sha256)
        {
            Path = path;
            Version = version;
            IsCheckpoint = isCheckpoint;
            Listed = listed;
            Length = length;
            Sha256 = sha256;
        }

        public IOPath Path { get; }

        public long Version { get; }

        public bool IsCheckpoint { get; }

        /// <summary>
        /// The listed size, modification time and MD5: a cheap discriminator, never a proof of unchanged content.
        /// </summary>
        public ObjectIdentity Listed { get; }

        public long Length { get; }

        public byte[] Sha256 { get; }

        /// <summary>
        /// A commit's own provenance from a strict proof of its first line; unknown when the line failed the proof, null for a checkpoint.
        /// </summary>
        public CommitProvenance? Provenance { get; init; }

        public bool SameContent(long length, ReadOnlySpan<byte> sha256) => Length == length && Sha256.AsSpan().SequenceEqual(sha256);

        /// <summary>
        /// Whether a listing still shows this object under its exact name with the same discriminator.
        /// </summary>
        public bool ListedUnchanged(LogTransactionFile? file)
        {
            return file != null && file.IOEntry.Path.Full == Path.Full && file.IsCheckpoint == IsCheckpoint
                && (file.IOEntry.Size ?? -1) == Listed.Size && file.IOEntry.LastModificationTime == Listed.Modified && file.IOEntry.MD5 == Listed.Md5;
        }

        /// <summary>
        /// The object as one checked listing shows it: the commit or classic checkpoint at its version, null when it is not listed.
        /// </summary>
        public static LogTransactionFile? Find(DeltaTransactionReader.LogListing listing, long version, bool isCheckpoint)
        {
            if (!isCheckpoint)
            {
                return listing.Commits.TryGetValue(version, out var commit) ? commit : null;
            }
            foreach (var checkpoint in listing.Checkpoints)
            {
                if (checkpoint.Version == version)
                {
                    return checkpoint;
                }
            }
            return null;
        }
    }

    /// <summary>
    /// Cooperative work stopped at a deadline, a due build or the worker's stop; nothing it read is kept.
    /// </summary>
    internal sealed class CleanupPreemptedException : Exception
    {
        public CleanupPreemptedException(IssuanceStop stop)
            : base($"Cleanup validation stopped: {stop}")
        {
            Stop = stop;
        }

        public IssuanceStop Stop { get; }
    }

    internal enum ProofStatus : byte
    {
        Read,
        // The object does not exist
        Missing,
        // The context stopped issuance before the read completed
        Preempted,
        // The admission's counted reads were spent, so nothing was read
        ReadsSpent
    }

    /// <summary>
    /// One complete read of an object: its length and SHA-256, and a commit's provenance when its first line was proven.
    /// </summary>
    internal readonly record struct ProofRead(ProofStatus Status, long Length, byte[]? Sha256, CommitProvenance? Provenance, IssuanceStop Stop = IssuanceStop.None);

    /// <summary>
    /// Hashes every byte read through it and can prove a commit's first line strictly; checks the context before each read.
    /// </summary>
    internal sealed class ProofStream : Stream
    {
        private readonly Stream _inner;
        private readonly CleanupContext? _context;
        private readonly IncrementalHash _hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        private CommitHeaderScanner? _scanner;
        private CommitHeader? _header;
        private long _length;
        private bool _end;

        public ProofStream(Stream inner, CleanupContext? context, bool proveFirstLine)
        {
            _inner = inner;
            _context = context;
            _scanner = proveFirstLine ? new CommitHeaderScanner(HeaderMode.Strict) : null;
        }

        public override bool CanRead => true;

        public override bool CanSeek => false;

        public override bool CanWrite => false;

        public override long Length => throw new NotSupportedException();

        public override long Position
        {
            get => _length;
            set => throw new NotSupportedException();
        }

        public long BytesRead => _length;

        // A read returned the end of the stream
        public bool Ended => _end;

        private void CheckStop()
        {
            if (_context != null && _context.CheckStop() is var stop && stop != IssuanceStop.None)
            {
                throw new CleanupPreemptedException(stop);
            }
        }

        private void Observe(ReadOnlySpan<byte> bytes)
        {
            if (bytes.IsEmpty)
            {
                _end = true;
                return;
            }
            _hash.AppendData(bytes);
            _length += bytes.Length;
            if (_scanner != null)
            {
                var end = bytes.IndexOfAny((byte)'\r', (byte)'\n');
                if (end < 0)
                {
                    _scanner.Feed(bytes);
                }
                else
                {
                    _scanner.Feed(bytes.Slice(0, end));
                    _header = _scanner.Complete(terminated: true);
                    _scanner = null;
                }
            }
        }

        public override int Read(byte[] buffer, int offset, int count) => Read(buffer.AsSpan(offset, count));

        public override int Read(Span<byte> buffer)
        {
            CheckStop();
            var read = _inner.Read(buffer);
            Observe(buffer.Slice(0, read));
            return read;
        }

        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) => ReadAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();

        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            CheckStop();
            var read = await _inner.ReadAsync(buffer, cancellationToken);
            Observe(buffer.Span.Slice(0, read));
            return read;
        }

        /// <summary>
        /// The proof of the complete object; only valid once a read returned the end of the stream.
        /// </summary>
        public ProofRead Complete(long version)
        {
            if (!_end)
            {
                throw new InvalidOperationException("The object was not read to its end");
            }
            CommitProvenance? provenance = null;
            if (_scanner != null || _header != null)
            {
                var header = _header ?? _scanner!.Complete(terminated: false);
                _scanner = null;
                provenance = CommitProvenance.FromStrict(version, header);
            }
            return new ProofRead(ProofStatus.Read, _length, _hash.GetHashAndReset(), provenance);
        }

        public override void Flush()
        {
        }

        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

        public override void SetLength(long value) => throw new NotSupportedException();

        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                _hash.Dispose();
                _inner.Dispose();
            }
            base.Dispose(disposing);
        }

        public override async ValueTask DisposeAsync()
        {
            _hash.Dispose();
            await _inner.DisposeAsync();
            GC.SuppressFinalize(this);
        }
    }

    /// <summary>
    /// Complete-content reads for cleanup proofs: bounded charged buffers, every byte through the storage it is given, preemptible between reads.
    /// </summary>
    internal static class ProofReader
    {
        public const int BufferSize = 64 << 10;

        /// <summary>
        /// Reads an object to its end as one counted read of the admission; an error propagates, a stop returns Preempted with nothing kept.
        /// </summary>
        public static async Task<ProofRead> ReadAsync(IFileStorage storage, IOPath path, long version, long? listedSize, bool proveFirstLine, CleanupContext context)
        {
            var buffer = Array.Empty<byte>();
            try
            {
                if (context.CheckStop() is var stop && stop != IssuanceStop.None)
                {
                    return new ProofRead(ProofStatus.Preempted, 0, null, null, stop);
                }
                if (!context.TryCountRead())
                {
                    return new ProofRead(ProofStatus.ReadsSpent, 0, null, null);
                }
                buffer = context.Memory.Allocate(BufferSize);
                // A store that reads whole bodies holds the object, so the read is admitted by its listed size
                using var admitted = await context.Transfer.AdmitAsync(listedSize ?? 0, context.Stop);
                var source = await storage.OpenRead(path, context.Stop);
                if (source == null)
                {
                    return new ProofRead(ProofStatus.Missing, 0, null, null);
                }
                await using var proof = new ProofStream(source, context, proveFirstLine);
                while (await proof.ReadAsync(buffer, context.Stop) > 0)
                {
                }
                return proof.Complete(version);
            }
            catch (CleanupPreemptedException e)
            {
                return new ProofRead(ProofStatus.Preempted, 0, null, null, e.Stop);
            }
            catch (OperationCanceledException) when (context.Stop.IsCancellationRequested)
            {
                return new ProofRead(ProofStatus.Preempted, 0, null, null, IssuanceStop.Stopped);
            }
            finally
            {
                if (buffer.Length > 0)
                {
                    context.Memory.Free(buffer);
                }
            }
        }
    }
}
