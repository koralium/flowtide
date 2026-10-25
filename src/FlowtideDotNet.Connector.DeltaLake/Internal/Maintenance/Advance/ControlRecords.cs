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

using Stowage;
using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;
using System.Text.Unicode;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.Advance
{
    /// <summary>
    /// Fresh 128-bit advance and attempt tokens: 32 lowercase hex characters.
    /// </summary>
    internal static class ControlTokens
    {
        public const int Length = 32;

        public static string New() => RandomNumberGenerator.GetHexString(Length, lowercase: true);

        public static bool IsCanonical(string? token)
        {
            if (token == null || token.Length != Length)
            {
                return false;
            }
            foreach (var c in token)
            {
                if (!IsLowerHex(c))
                {
                    return false;
                }
            }
            return true;
        }

        internal static bool IsLowerHex(int c) => (uint)(c - '0') <= 9 || (uint)(c - 'a') <= 5;
    }

    /// <summary>
    /// What reading or decoding one control record found.
    /// </summary>
    internal enum RecordStatus : byte
    {
        Valid,
        Missing,
        // Longer than its kind's cap
        OverCap,
        // Truncated, tampered, malformed or of another format version
        Torn,
        // Intact, but of another kind or not the record its name says
        Mismatch
    }

    /// <summary>
    /// The anchor commit as records carry it.
    /// </summary>
    internal sealed record AnchorSummary(long Version, string StageId, long Length, byte[] Sha256, long ListedSize)
    {
        public static AnchorSummary Of(CommitAnchor anchor) => new AnchorSummary(anchor.Version, anchor.StageId, anchor.Proof.Length, anchor.Sha256, anchor.Listed.Size);
    }

    /// <summary>
    /// The destination checkpoint C as records carry it, with its listed discriminator.
    /// </summary>
    internal sealed record CheckpointSummary(long Length, byte[] Sha256, long ListedSize, long ListedModifiedTicks)
    {
        // An unlisted modification time is -1
        public static CheckpointSummary Of(ContentProof proof) => new CheckpointSummary(proof.Length, proof.Sha256, proof.Listed.Size, proof.Listed.Modified?.UtcTicks ?? -1);
    }

    internal sealed record RecordTotals(long Emitted, int Parts, long PartBytes);

    /// <summary>
    /// The exact header a group record belongs to; equal only with the same full SHA-256.
    /// </summary>
    internal sealed record Association(long C, string AdvanceId, string HeaderName, byte[] HeaderSha256)
    {
        public bool Equals(Association? other) => other != null && C == other.C && AdvanceId == other.AdvanceId && HeaderName == other.HeaderName
            && HeaderSha256.AsSpan().SequenceEqual(other.HeaderSha256);

        public override int GetHashCode() => HashCode.Combine(C, AdvanceId, HeaderName);
    }

    /// <summary>
    /// One part as its header names it; the SHA-256 is of the whole part object.
    /// </summary>
    internal sealed record PartDescriptor(int Ordinal, string AttemptId, int Paths, int Bytes, byte[] Sha256);

    /// <summary>
    /// An advance's header without its part descriptors.
    /// </summary>
    internal sealed record HeaderRecord(long C, string AdvanceId, string TableId, long O, long A, long Head, long FirstJson, long CutoffTicks,
        AnchorSummary Anchor, CheckpointSummary Destination, long Candidates, long Protected, long Kept, RecordTotals Totals);

    internal sealed record StartedRecord(Association Association, string TableId, long O, long A, long Head, long FirstJson, AnchorSummary Anchor, RecordTotals Totals);

    internal sealed record LogsCompleteRecord(Association Association, string TableId, long O);

    /// <summary>
    /// A position in an advance's parts: part ordinal, record index and byte offset.
    /// </summary>
    internal readonly record struct RecordCursor(int Part, int Record, long Byte);

    internal sealed record ProgressRecord(Association Association, long Sequence, RecordCursor Next, long Processed);

    internal sealed record DoneRecord(Association Association, string TableId, long O, long A, long Head, AnchorSummary Anchor, RecordCursor Final, RecordTotals Totals, long Processed);

    internal readonly record struct PartHead(long C, string AdvanceId, int Ordinal, int Count);

    /// <summary>
    /// The u32-framed UTF-8 paths of a validated part, in order.
    /// </summary>
    internal ref struct PartPaths
    {
        private ReadOnlySpan<byte> _rest;

        public PartPaths(ReadOnlySpan<byte> framed)
        {
            _rest = framed;
            Current = default;
        }

        public ReadOnlySpan<byte> Current { get; private set; }

        public readonly PartPaths GetEnumerator() => this;

        public bool MoveNext()
        {
            if (_rest.Length < 4 || BinaryPrimitives.ReadUInt32LittleEndian(_rest) > (uint)(_rest.Length - 4))
            {
                _rest = default;
                return false;
            }
            var length = (int)BinaryPrimitives.ReadUInt32LittleEndian(_rest);
            Current = _rest.Slice(4, length);
            _rest = _rest.Slice(4 + length);
            return true;
        }
    }

    /// <summary>
    /// One record read whole into job memory; Valid means read within its cap, not decoded.
    /// </summary>
    internal sealed class RecordBytes : IDisposable
    {
        private JobMemory? _memory;
        private byte[]? _buffer;

        public RecordBytes(RecordStatus status, long length)
        {
            Status = status;
            Length = length;
        }

        public RecordBytes(JobMemory memory, byte[] buffer, int length)
        {
            _memory = memory;
            _buffer = buffer;
            Status = RecordStatus.Valid;
            Length = length;
            Sha256 = SHA256.HashData(buffer.AsSpan(0, length));
        }

        public RecordStatus Status { get; }

        public ReadOnlyMemory<byte> Bytes => _buffer == null ? default : _buffer.AsMemory(0, (int)Length);

        public long Length { get; }

        // Of the whole object
        public byte[]? Sha256 { get; }

        public void Dispose()
        {
            if (_buffer != null)
            {
                _memory!.Free(_buffer);
                _buffer = null;
                _memory = null;
            }
        }
    }

    /// <summary>
    /// The binary control record format: a checksummed envelope around each kind's little-endian body; decoding never throws.
    /// </summary>
    internal static class ControlRecords
    {
        // "FTCR"
        public const uint Magic = 0x52435446;
        public const byte FormatVersion = 1;
        // "FTCE"
        public const uint EndMarker = 0x45435446;
        public const int EnvelopeBytes = HeadBytes + ShaBytes + 4;
        public const int SmallCap = 4 << 10;
        public const int PartCap = 2 << 20;
        public const int HeaderCap = 4 << 20;
        public const int MaxPartPaths = 10_000;
        public const int MaxDescriptors = 16_384;
        public const int MaxPathBytes = 64 << 10;
        // A part's first path length prefix, after the envelope head and the part's fixed fields
        public const int FirstPathOffset = HeadBytes + PartFixed;

        // Magic, version, kind, two zero bytes and the body length
        private const int HeadBytes = 12;
        private const int ShaBytes = 32;
        // C, advance id, ordinal and count
        private const int PartFixed = 8 + ControlTokens.Length + 4 + 4;
        // Ordinal, attempt id, paths, bytes and SHA-256
        private const int DescriptorBytes = 4 + ControlTokens.Length + 4 + 4 + ShaBytes;

        private static readonly UTF8Encoding s_utf8 = new UTF8Encoding(false, true);

        private delegate void BodyWriter<T>(ref FieldWriter writer, T record);

        public static int CapOf(ControlKind kind) => kind switch
        {
            ControlKind.Part => PartCap,
            ControlKind.Header => HeaderCap,
            _ => SmallCap
        };

        /// <summary>
        /// The whole object length of a part of count paths with pathBytes UTF-8 bytes in total.
        /// </summary>
        public static int PartLength(int pathBytes, int count) => checked(EnvelopeBytes + PartFixed + pathBytes + 4 * count);

        /// <summary>
        /// Writes a part of sorted distinct paths; returns its length and the whole object's SHA-256.
        /// </summary>
        public static int WritePart(Span<byte> target, long c, string advanceId, int ordinal, IReadOnlyList<byte[]> sortedUtf8Paths, out byte[] sha256)
        {
            if (ordinal < 0 || ordinal >= MaxDescriptors || sortedUtf8Paths.Count == 0 || sortedUtf8Paths.Count > MaxPartPaths)
            {
                throw new ArgumentOutOfRangeException(nameof(sortedUtf8Paths), $"Part {ordinal} cannot hold {sortedUtf8Paths.Count} paths");
            }
            var pathBytes = 0;
            foreach (var path in sortedUtf8Paths)
            {
                if (path.Length == 0 || path.Length > MaxPathBytes)
                {
                    throw new ArgumentOutOfRangeException(nameof(sortedUtf8Paths), $"A path of {path.Length} bytes");
                }
                pathBytes = checked(pathBytes + path.Length);
            }
            var bodyLength = PartLength(pathBytes, sortedUtf8Paths.Count) - EnvelopeBytes;
            Capped(ControlKind.Part, bodyLength);
            var writer = new FieldWriter(target.Slice(HeadBytes, bodyLength));
            writer.I64(c);
            writer.Token(advanceId);
            writer.I32(ordinal);
            writer.I32(sortedUtf8Paths.Count);
            foreach (var path in sortedUtf8Paths)
            {
                writer.U32((uint)path.Length);
                writer.Raw(path);
            }
            return Seal(target, ControlKind.Part, bodyLength, out sha256);
        }

        /// <summary>
        /// Checks a whole part object: envelope, count, path lengths, strict UTF-8 and strictly increasing byte order.
        /// </summary>
        public static RecordStatus ValidatePart(ReadOnlySpan<byte> bytes, out PartHead head)
        {
            head = default;
            var status = Open(bytes, ControlKind.Part, out var reader);
            if (status != RecordStatus.Valid)
            {
                return status;
            }
            var c = reader.I64();
            var advanceId = reader.Token();
            var ordinal = reader.I32();
            var count = reader.I32();
            reader.Require(c > 0 && ordinal >= 0 && ordinal < MaxDescriptors && count > 0 && count <= MaxPartPaths);
            var previous = ReadOnlySpan<byte>.Empty;
            for (int i = 0; i < count && !reader.Failed; i++)
            {
                var length = reader.U32();
                reader.Require(length > 0 && length <= MaxPathBytes);
                var path = reader.Take((int)length);
                reader.Require((i == 0 || previous.SequenceCompareTo(path) < 0) && Utf8.IsValid(path));
                previous = path;
            }
            if (!reader.Complete)
            {
                return RecordStatus.Torn;
            }
            head = new PartHead(c, advanceId, ordinal, count);
            return RecordStatus.Valid;
        }

        public static PartPaths Paths(ReadOnlySpan<byte> validated)
        {
            var start = HeadBytes + PartFixed;
            var end = validated.Length - ShaBytes - 4;
            return new PartPaths(end > start ? validated.Slice(start, end - start) : ReadOnlySpan<byte>.Empty);
        }

        /// <summary>
        /// The whole object length of a header with this many descriptors; over its cap declines.
        /// </summary>
        public static int HeaderLength(HeaderRecord header, int descriptors)
        {
            if (descriptors != header.Totals.Parts || descriptors < 0 || descriptors > MaxDescriptors)
            {
                throw new ArgumentOutOfRangeException(nameof(descriptors), $"{descriptors} descriptors for {header.Totals.Parts} parts");
            }
            var measure = FieldWriter.Measure();
            WriteHeaderFields(ref measure, header);
            return Capped(ControlKind.Header, measure.Position + (long)descriptors * DescriptorBytes);
        }

        /// <summary>
        /// Writes a header and its descriptors in ordinal order; returns its length and the whole object's SHA-256.
        /// </summary>
        public static int WriteHeader(Span<byte> target, HeaderRecord header, IReadOnlyList<PartDescriptor> descriptors, out byte[] sha256)
        {
            var bodyLength = HeaderLength(header, descriptors.Count) - EnvelopeBytes;
            var writer = new FieldWriter(target.Slice(HeadBytes, bodyLength));
            WriteHeaderFields(ref writer, header);
            foreach (var descriptor in descriptors)
            {
                writer.I32(descriptor.Ordinal);
                writer.Token(descriptor.AttemptId);
                writer.I32(descriptor.Paths);
                writer.I32(descriptor.Bytes);
                writer.Sha(descriptor.Sha256);
            }
            return Seal(target, ControlKind.Header, bodyLength, out sha256);
        }

        /// <summary>
        /// Decodes a header and checks its descriptors add up to its totals; descriptors are appended only when it is valid.
        /// </summary>
        public static RecordStatus DecodeHeader(ReadOnlySpan<byte> bytes, out HeaderRecord? header, List<PartDescriptor>? descriptors)
        {
            header = null;
            var status = Open(bytes, ControlKind.Header, out var reader);
            if (status != RecordStatus.Valid)
            {
                return status;
            }
            var c = reader.I64();
            var advanceId = reader.Token();
            var tableId = reader.Str();
            var o = reader.I64();
            var a = reader.I64();
            var head = reader.I64();
            var firstJson = reader.I64();
            var cutoff = reader.I64();
            var anchor = ReadAnchor(ref reader);
            var destination = ReadCheckpoint(ref reader);
            var candidates = reader.I64();
            var protectedCount = reader.I64();
            var kept = reader.I64();
            var totals = ReadTotals(ref reader);
            reader.Require(Boundary(o, c, a, head, firstJson) && candidates >= 0 && protectedCount >= 0 && kept >= 0);
            var start = descriptors?.Count ?? 0;
            long paths = 0, partBytes = 0;
            for (int i = 0; i < totals.Parts && !reader.Failed; i++)
            {
                var ordinal = reader.I32();
                var attemptId = reader.Token();
                var count = reader.I32();
                var size = reader.I32();
                var sha = reader.Take(ShaBytes);
                reader.Require(ordinal == i && count > 0 && count <= MaxPartPaths && size >= PartLength(count, count) && size <= PartCap);
                paths += count;
                partBytes += size;
                if (!reader.Failed)
                {
                    descriptors?.Add(new PartDescriptor(ordinal, attemptId, count, size, sha.ToArray()));
                }
            }
            reader.Require(paths == totals.Emitted && partBytes == totals.PartBytes);
            if (!reader.Complete)
            {
                descriptors?.RemoveRange(start, descriptors.Count - start);
                return RecordStatus.Torn;
            }
            header = new HeaderRecord(c, advanceId, tableId, o, a, head, firstJson, cutoff, anchor, destination, candidates, protectedCount, kept, totals);
            return RecordStatus.Valid;
        }

        public static byte[] Encode(StartedRecord record) => EncodeSmall(ControlKind.Started, record, WriteStarted);

        public static byte[] Encode(LogsCompleteRecord record) => EncodeSmall(ControlKind.LogsComplete, record, WriteLogsComplete);

        public static byte[] Encode(ProgressRecord record) => EncodeSmall(ControlKind.Progress, record, WriteProgress);

        public static byte[] Encode(DoneRecord record) => EncodeSmall(ControlKind.Done, record, WriteDone);

        public static RecordStatus Decode(ReadOnlySpan<byte> bytes, out StartedRecord? record)
        {
            record = null;
            var status = Open(bytes, ControlKind.Started, out var reader);
            if (status != RecordStatus.Valid)
            {
                return status;
            }
            var association = ReadAssociation(ref reader);
            var tableId = reader.Str();
            var o = reader.I64();
            var a = reader.I64();
            var head = reader.I64();
            var firstJson = reader.I64();
            var anchor = ReadAnchor(ref reader);
            var totals = ReadTotals(ref reader);
            reader.Require(Boundary(o, association.C, a, head, firstJson));
            if (!reader.Complete)
            {
                return RecordStatus.Torn;
            }
            record = new StartedRecord(association, tableId, o, a, head, firstJson, anchor, totals);
            return RecordStatus.Valid;
        }

        public static RecordStatus Decode(ReadOnlySpan<byte> bytes, out LogsCompleteRecord? record)
        {
            record = null;
            var status = Open(bytes, ControlKind.LogsComplete, out var reader);
            if (status != RecordStatus.Valid)
            {
                return status;
            }
            var association = ReadAssociation(ref reader);
            var tableId = reader.Str();
            var o = reader.I64();
            reader.Require(o >= 0 && o < association.C);
            if (!reader.Complete)
            {
                return RecordStatus.Torn;
            }
            record = new LogsCompleteRecord(association, tableId, o);
            return RecordStatus.Valid;
        }

        public static RecordStatus Decode(ReadOnlySpan<byte> bytes, out ProgressRecord? record)
        {
            record = null;
            var status = Open(bytes, ControlKind.Progress, out var reader);
            if (status != RecordStatus.Valid)
            {
                return status;
            }
            var association = ReadAssociation(ref reader);
            var sequence = reader.I64();
            var next = ReadCursor(ref reader);
            var processed = reader.I64();
            reader.Require(sequence >= 0 && sequence <= ControlName.MaxSequence && processed >= 0);
            if (!reader.Complete)
            {
                return RecordStatus.Torn;
            }
            record = new ProgressRecord(association, sequence, next, processed);
            return RecordStatus.Valid;
        }

        public static RecordStatus Decode(ReadOnlySpan<byte> bytes, out DoneRecord? record)
        {
            record = null;
            var status = Open(bytes, ControlKind.Done, out var reader);
            if (status != RecordStatus.Valid)
            {
                return status;
            }
            var association = ReadAssociation(ref reader);
            var tableId = reader.Str();
            var o = reader.I64();
            var a = reader.I64();
            var head = reader.I64();
            var anchor = ReadAnchor(ref reader);
            var final = ReadCursor(ref reader);
            var totals = ReadTotals(ref reader);
            var processed = reader.I64();
            // Terminal: the position past the last part, which a completed advance writes, with every emitted path processed
            reader.Require(Boundary(o, association.C, a, head, o) && final == new RecordCursor(totals.Parts, 0, 0) && processed == totals.Emitted);
            if (!reader.Complete)
            {
                return RecordStatus.Torn;
            }
            record = new DoneRecord(association, tableId, o, a, head, anchor, final, totals, processed);
            return RecordStatus.Valid;
        }

        /// <summary>
        /// Whether a name agrees with bytes its kind's decoder accepted: kind, C, advance id, ordinal or sequence, the exact lowercase d16 and a canonical attempt id.
        /// </summary>
        public static RecordStatus Matches(in ControlName name, ReadOnlySpan<byte> validated)
        {
            if (validated.Length < EnvelopeBytes)
            {
                return RecordStatus.Torn;
            }
            var kind = (ControlKind)validated[5];
            var reader = new FieldReader(validated.Slice(HeadBytes, validated.Length - EnvelopeBytes));
            var c = reader.I64();
            var advanceId = reader.Token();
            var matches = kind == name.Kind && c == name.Boundary && advanceId == name.AdvanceId && ControlTokens.IsCanonical(name.AttemptId);
            if (kind == ControlKind.Part)
            {
                matches &= reader.I32() == name.Ordinal;
            }
            else if (kind != ControlKind.Header)
            {
                reader.Str();
                var sha = reader.Take(ShaBytes);
                matches &= !reader.Failed && string.Equals(name.Digest, ControlName.D16(sha), StringComparison.Ordinal);
                if (kind == ControlKind.Progress)
                {
                    matches &= reader.I64() == name.Ordinal;
                }
            }
            return reader.Failed ? RecordStatus.Torn : matches ? RecordStatus.Valid : RecordStatus.Mismatch;
        }

        /// <summary>
        /// Reads at most the kind's cap plus one byte into job memory and closes the stream before returning; an error propagates.
        /// </summary>
        public static async Task<RecordBytes> ReadAsync(IFileStorage storage, IOPath path, ControlKind kind, JobMemory memory, CancellationToken cancellationToken)
        {
            var cap = CapOf(kind);
            var buffer = memory.Allocate(cap + 1);
            try
            {
                var length = 0;
                await using (var stream = await storage.OpenRead(path, cancellationToken))
                {
                    if (stream == null)
                    {
                        memory.Free(buffer);
                        return new RecordBytes(RecordStatus.Missing, 0);
                    }
                    int read;
                    while (length < buffer.Length && (read = await stream.ReadAsync(buffer.AsMemory(length), cancellationToken)) > 0)
                    {
                        length += read;
                    }
                }
                if (length > cap)
                {
                    memory.Free(buffer);
                    return new RecordBytes(RecordStatus.OverCap, length);
                }
                return new RecordBytes(memory, buffer, length);
            }
            catch
            {
                memory.Free(buffer);
                throw;
            }
        }

        private static byte[] EncodeSmall<T>(ControlKind kind, T record, BodyWriter<T> body)
        {
            var measure = FieldWriter.Measure();
            body(ref measure, record);
            var length = Capped(kind, measure.Position);
            var bytes = new byte[length];
            var writer = new FieldWriter(bytes.AsSpan(HeadBytes, length - EnvelopeBytes));
            body(ref writer, record);
            Seal(bytes, kind, length - EnvelopeBytes, out _);
            return bytes;
        }

        // The whole object length of a body, or the over-cap decline
        private static int Capped(ControlKind kind, long bodyLength)
        {
            var length = bodyLength + EnvelopeBytes;
            if (length > CapOf(kind))
            {
                throw new CheckpointDeclinedException(MaintenanceReason.RecordOverCap, DeclineDependency.None, $"A {kind} record of {length} bytes is over its {CapOf(kind)} byte cap");
            }
            return (int)length;
        }

        // Writes the head, the checksum and the end marker around a written body; returns the object's length
        private static int Seal(Span<byte> target, ControlKind kind, int bodyLength, out byte[] sha256)
        {
            BinaryPrimitives.WriteUInt32LittleEndian(target, Magic);
            target[4] = FormatVersion;
            target[5] = (byte)kind;
            BinaryPrimitives.WriteUInt16LittleEndian(target.Slice(6), 0);
            BinaryPrimitives.WriteUInt32LittleEndian(target.Slice(8), (uint)bodyLength);
            var sealedAt = HeadBytes + bodyLength;
            using var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
            hash.AppendData(target.Slice(0, sealedAt));
            hash.GetCurrentHash(target.Slice(sealedAt, ShaBytes));
            BinaryPrimitives.WriteUInt32LittleEndian(target.Slice(sealedAt + ShaBytes), EndMarker);
            hash.AppendData(target.Slice(sealedAt, ShaBytes + 4));
            sha256 = hash.GetHashAndReset();
            return sealedAt + ShaBytes + 4;
        }

        // The body of an intact envelope of the kind, or why there is none
        private static RecordStatus Open(ReadOnlySpan<byte> bytes, ControlKind kind, out FieldReader body)
        {
            body = default;
            if (bytes.Length > CapOf(kind))
            {
                return RecordStatus.OverCap;
            }
            if (bytes.Length < EnvelopeBytes || BinaryPrimitives.ReadUInt32LittleEndian(bytes) != Magic
                || BinaryPrimitives.ReadUInt32LittleEndian(bytes.Slice(8)) != (uint)(bytes.Length - EnvelopeBytes)
                || BinaryPrimitives.ReadUInt32LittleEndian(bytes.Slice(bytes.Length - 4)) != EndMarker)
            {
                return RecordStatus.Torn;
            }
            var sealedAt = bytes.Length - ShaBytes - 4;
            Span<byte> sha = stackalloc byte[ShaBytes];
            SHA256.HashData(bytes.Slice(0, sealedAt), sha);
            if (!sha.SequenceEqual(bytes.Slice(sealedAt, ShaBytes)) || bytes[4] != FormatVersion || bytes[6] != 0 || bytes[7] != 0)
            {
                return RecordStatus.Torn;
            }
            if (bytes[5] != (byte)kind)
            {
                return RecordStatus.Mismatch;
            }
            body = new FieldReader(bytes.Slice(HeadBytes, sealedAt - HeadBytes));
            return RecordStatus.Valid;
        }

        // O < C <= head, A <= C and the first JSON is O or O + 1
        private static bool Boundary(long o, long c, long a, long head, long firstJson) =>
            o >= 0 && o < c && c <= head && a >= 0 && a <= c && (firstJson == o || firstJson == o + 1);

        private static void WriteHeaderFields(ref FieldWriter writer, HeaderRecord header)
        {
            writer.I64(header.C);
            writer.Token(header.AdvanceId);
            writer.Str(header.TableId);
            writer.I64(header.O);
            writer.I64(header.A);
            writer.I64(header.Head);
            writer.I64(header.FirstJson);
            writer.I64(header.CutoffTicks);
            WriteAnchor(ref writer, header.Anchor);
            writer.I64(header.Destination.Length);
            writer.Sha(header.Destination.Sha256);
            writer.I64(header.Destination.ListedSize);
            writer.I64(header.Destination.ListedModifiedTicks);
            writer.I64(header.Candidates);
            writer.I64(header.Protected);
            writer.I64(header.Kept);
            WriteTotals(ref writer, header.Totals);
        }

        private static void WriteStarted(ref FieldWriter writer, StartedRecord record)
        {
            WriteAssociation(ref writer, record.Association);
            writer.Str(record.TableId);
            writer.I64(record.O);
            writer.I64(record.A);
            writer.I64(record.Head);
            writer.I64(record.FirstJson);
            WriteAnchor(ref writer, record.Anchor);
            WriteTotals(ref writer, record.Totals);
        }

        private static void WriteLogsComplete(ref FieldWriter writer, LogsCompleteRecord record)
        {
            WriteAssociation(ref writer, record.Association);
            writer.Str(record.TableId);
            writer.I64(record.O);
        }

        private static void WriteProgress(ref FieldWriter writer, ProgressRecord record)
        {
            WriteAssociation(ref writer, record.Association);
            writer.I64(record.Sequence);
            WriteCursor(ref writer, record.Next);
            writer.I64(record.Processed);
        }

        private static void WriteDone(ref FieldWriter writer, DoneRecord record)
        {
            WriteAssociation(ref writer, record.Association);
            writer.Str(record.TableId);
            writer.I64(record.O);
            writer.I64(record.A);
            writer.I64(record.Head);
            WriteAnchor(ref writer, record.Anchor);
            WriteCursor(ref writer, record.Final);
            WriteTotals(ref writer, record.Totals);
            writer.I64(record.Processed);
        }

        private static void WriteAssociation(ref FieldWriter writer, Association association)
        {
            writer.I64(association.C);
            writer.Token(association.AdvanceId);
            writer.Str(association.HeaderName);
            writer.Sha(association.HeaderSha256);
        }

        // The header name must be this advance's header at C with a canonical attempt id
        private static Association ReadAssociation(ref FieldReader reader)
        {
            var c = reader.I64();
            var advanceId = reader.Token();
            var headerName = reader.Str();
            var sha = reader.Sha();
            reader.Require(c > 0 && ControlName.TryParse(headerName, out var header) && header.Kind == ControlKind.Header && header.Boundary == c
                && header.AdvanceId == advanceId && ControlTokens.IsCanonical(header.AttemptId));
            return new Association(c, advanceId, headerName, sha);
        }

        private static void WriteAnchor(ref FieldWriter writer, AnchorSummary anchor)
        {
            writer.I64(anchor.Version);
            writer.Str(anchor.StageId);
            writer.I64(anchor.Length);
            writer.Sha(anchor.Sha256);
            writer.I64(anchor.ListedSize);
        }

        private static AnchorSummary ReadAnchor(ref FieldReader reader)
        {
            var version = reader.I64();
            var stageId = reader.Str();
            var length = reader.I64();
            var sha = reader.Sha();
            var listedSize = reader.I64();
            reader.Require(version >= 0 && length >= 0);
            return new AnchorSummary(version, stageId, length, sha, listedSize);
        }

        private static CheckpointSummary ReadCheckpoint(ref FieldReader reader)
        {
            var length = reader.I64();
            var sha = reader.Sha();
            var listedSize = reader.I64();
            var modified = reader.I64();
            reader.Require(length >= 0);
            return new CheckpointSummary(length, sha, listedSize, modified);
        }

        private static void WriteTotals(ref FieldWriter writer, RecordTotals totals)
        {
            writer.I64(totals.Emitted);
            writer.I32(totals.Parts);
            writer.I64(totals.PartBytes);
        }

        private static RecordTotals ReadTotals(ref FieldReader reader)
        {
            var emitted = reader.I64();
            var parts = reader.I32();
            var partBytes = reader.I64();
            // Paths only with parts, and no more paths or bytes than that many parts hold
            reader.Require(emitted >= 0 && parts >= 0 && parts <= MaxDescriptors && partBytes >= 0 && (parts == 0) == (emitted == 0)
                && emitted <= (long)parts * MaxPartPaths && partBytes <= (long)parts * PartCap);
            return new RecordTotals(emitted, parts, partBytes);
        }

        private static void WriteCursor(ref FieldWriter writer, RecordCursor cursor)
        {
            writer.I32(cursor.Part);
            writer.I32(cursor.Record);
            writer.I64(cursor.Byte);
        }

        private static RecordCursor ReadCursor(ref FieldReader reader)
        {
            var part = reader.I32();
            var record = reader.I32();
            var offset = reader.I64();
            reader.Require(part >= 0 && record >= 0 && offset >= 0);
            return new RecordCursor(part, record, offset);
        }

        // Little-endian fields into a body; a measuring writer only counts
        private ref struct FieldWriter
        {
            private readonly Span<byte> _target;
            private readonly bool _measure;

            public FieldWriter(Span<byte> target)
            {
                _target = target;
                _measure = false;
                Position = 0;
            }

            private FieldWriter(bool measure)
            {
                _target = default;
                _measure = measure;
                Position = 0;
            }

            public int Position { get; private set; }

            public static FieldWriter Measure() => new FieldWriter(true);

            public void U32(uint value)
            {
                var span = Next(4);
                if (!_measure)
                {
                    BinaryPrimitives.WriteUInt32LittleEndian(span, value);
                }
            }

            public void I32(int value)
            {
                var span = Next(4);
                if (!_measure)
                {
                    BinaryPrimitives.WriteInt32LittleEndian(span, value);
                }
            }

            public void I64(long value)
            {
                var span = Next(8);
                if (!_measure)
                {
                    BinaryPrimitives.WriteInt64LittleEndian(span, value);
                }
            }

            public void Token(string token)
            {
                if (!ControlTokens.IsCanonical(token))
                {
                    throw new ArgumentException($"'{token}' is not a canonical token", nameof(token));
                }
                var span = Next(ControlTokens.Length);
                if (!_measure)
                {
                    Encoding.ASCII.GetBytes(token, span);
                }
            }

            public void Sha(byte[] sha256)
            {
                if (sha256.Length != ShaBytes)
                {
                    throw new ArgumentException($"A SHA-256 of {sha256.Length} bytes", nameof(sha256));
                }
                Raw(sha256);
            }

            public void Str(string value)
            {
                var length = s_utf8.GetByteCount(value);
                U32((uint)length);
                var span = Next(length);
                if (!_measure)
                {
                    s_utf8.GetBytes(value, span);
                }
            }

            public void Raw(ReadOnlySpan<byte> bytes)
            {
                var span = Next(bytes.Length);
                if (!_measure)
                {
                    bytes.CopyTo(span);
                }
            }

            // The next bytes of the target, empty when measuring
            private Span<byte> Next(int length)
            {
                var start = Position;
                Position = checked(Position + length);
                return _measure ? Span<byte>.Empty : _target.Slice(start, length);
            }
        }

        // Little-endian fields from a body; a failure is sticky and later reads return defaults
        private ref struct FieldReader
        {
            private readonly ReadOnlySpan<byte> _source;
            private int _position;

            public FieldReader(ReadOnlySpan<byte> source)
            {
                _source = source;
                _position = 0;
                Failed = false;
            }

            public bool Failed { get; private set; }

            // Every byte read and nothing failed
            public readonly bool Complete => !Failed && _position == _source.Length;

            public void Require(bool condition)
            {
                if (!condition)
                {
                    Failed = true;
                }
            }

            public ReadOnlySpan<byte> Take(int length)
            {
                if (Failed || length < 0 || length > _source.Length - _position)
                {
                    Failed = true;
                    return ReadOnlySpan<byte>.Empty;
                }
                var span = _source.Slice(_position, length);
                _position += length;
                return span;
            }

            public uint U32()
            {
                var span = Take(4);
                return Failed ? 0 : BinaryPrimitives.ReadUInt32LittleEndian(span);
            }

            public int I32()
            {
                var span = Take(4);
                return Failed ? 0 : BinaryPrimitives.ReadInt32LittleEndian(span);
            }

            public long I64()
            {
                var span = Take(8);
                return Failed ? 0 : BinaryPrimitives.ReadInt64LittleEndian(span);
            }

            public string Token()
            {
                var span = Take(ControlTokens.Length);
                foreach (var b in span)
                {
                    Require(ControlTokens.IsLowerHex(b));
                }
                return Failed ? "" : Encoding.ASCII.GetString(span);
            }

            public byte[] Sha()
            {
                var span = Take(ShaBytes);
                return Failed ? Array.Empty<byte>() : span.ToArray();
            }

            // Strict UTF-8
            public string Str()
            {
                var length = U32();
                var span = Take(length > int.MaxValue ? -1 : (int)length);
                Require(Utf8.IsValid(span));
                return Failed ? "" : Encoding.UTF8.GetString(span);
            }
        }
    }
}
