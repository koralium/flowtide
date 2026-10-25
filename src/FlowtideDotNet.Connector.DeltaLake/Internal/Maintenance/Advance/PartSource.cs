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
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.Deletion;
using Stowage;
using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using System.Text;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.Advance
{
    /// <summary>
    /// One payload slice's targets: the rest of the cursor's part, read whole and validated with its header before any entry is used.
    /// </summary>
    internal sealed class PartSource : IDeletionSource, IDisposable
    {
        private readonly ActiveAdvance _active;
        private readonly DeletionExecutor _executor;
        private readonly IFileStorage _storage;
        private RecordBytes? _part;
        // The validated cursor as a position, a record and its length prefix's offset
        private long _start = -1;
        private int _record;
        private int _offset;

        public PartSource(ActiveAdvance active, DeletionExecutor executor, IFileStorage storage)
        {
            _active = active;
            _executor = executor;
            _storage = storage;
        }

        /// <summary>
        /// The gate's decision: the header when the cursor entered its part, the part against its descriptor, then the cursor walked from the part's start; anything else changed the authority.
        /// </summary>
        public async Task<GateResult> ValidateAsync(LogEvidence log, CleanupContext context)
        {
            Release();
            var active = _active;
            var totals = active.Started.Totals;
            var cursor = active.Cursor;
            if (active.Part?.Ordinal != cursor.Part)
            {
                var descriptors = await HeaderAsync(context);
                if (descriptors == null)
                {
                    return Changed(log, "The header is not the one the advance's records name");
                }
                active.PartStart = descriptors.Take(cursor.Part).Sum(x => (long)x.Paths);
                active.PartBytesLeft = descriptors.Skip(cursor.Part).Sum(x => (long)x.Bytes);
                active.Part = cursor.Part < descriptors.Count ? descriptors[cursor.Part] : null;
            }
            if (active.Part is not { } descriptor)
            {
                // Past the last part only at the terminal position with every path processed
                return cursor == new RecordCursor(totals.Parts, 0, 0) && active.Processed == totals.Emitted ? Passed(log) : Changed(log, $"Cursor {cursor} is not inside a part");
            }
            _part = await ControlRecords.ReadAsync(_storage, LogPath(ControlName.Part(active.C, active.Association.AdvanceId, descriptor.Ordinal, descriptor.AttemptId)), ControlKind.Part,
                context.Memory, context.Stop);
            if (!Matches(_part, descriptor) || cursor.Record >= descriptor.Paths)
            {
                return Changed(log, $"Part {descriptor.Ordinal} is not the part its header names, or cursor {cursor} is past it");
            }
            var offset = Walk(_part.Bytes.Span, ControlRecords.FirstPathOffset, cursor.Record);
            if (offset != cursor.Byte || active.Processed != active.PartStart + cursor.Record)
            {
                return Changed(log, $"Cursor {cursor} with {active.Processed} processed is not a record boundary of part {descriptor.Ordinal}");
            }
            (_start, _record, _offset) = (active.Processed, cursor.Record, offset);
            return Passed(log);
        }

        public IAsyncEnumerable<DeletionTarget> ReadFrom(long position, CancellationToken cancellationToken) => ReadAsync(position, cancellationToken);

        /// <summary>
        /// The canonical cursor of a position in the validated part: the next record's length prefix, the next part's first one at its end, or the terminal position after the last part.
        /// </summary>
        public RecordCursor CursorAt(long position)
        {
            var descriptor = _active.Part!;
            var record = _record + checked((int)(position - _start));
            if (record == descriptor.Paths)
            {
                var parts = _active.Started.Totals.Parts;
                return descriptor.Ordinal + 1 == parts ? new RecordCursor(parts, 0, 0) : new RecordCursor(descriptor.Ordinal + 1, 0, ControlRecords.FirstPathOffset);
            }
            return new RecordCursor(descriptor.Ordinal, record, Walk(_part!.Bytes.Span, _offset, record - _record));
        }

        public void Dispose() => Release();

        private async IAsyncEnumerable<DeletionTarget> ReadAsync(long position, [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            await Task.CompletedTask;
            if (_part == null)
            {
                // The terminal position holds nothing
                yield break;
            }
            var paths = _active.Part!.Paths;
            var offset = _offset;
            for (var record = _record; record < paths; record++)
            {
                var (path, next) = PathAt(_part.Bytes, offset);
                offset = next;
                if (_start + record - _record >= position)
                {
                    yield return _executor.TargetOf(path) with { Excluded = _active.Exclusions.Contains(path) };
                }
            }
        }

        // The header the association names, read whole and checked against its full digest and the started record; its descriptors, null when it is not that header
        private async Task<List<PartDescriptor>?> HeaderAsync(CleanupContext context)
        {
            var association = _active.Association;
            using var bytes = await ControlRecords.ReadAsync(_storage, LogPath(association.HeaderName), ControlKind.Header, context.Memory, context.Stop);
            var descriptors = new List<PartDescriptor>();
            var valid = bytes.Status == RecordStatus.Valid && bytes.Sha256.AsSpan().SequenceEqual(association.HeaderSha256)
                && ControlRecords.DecodeHeader(bytes.Bytes.Span, out var header, descriptors) == RecordStatus.Valid && Describes(header!, _active.Started);
            return valid ? descriptors : null;
        }

        // The boundary, table, anchor and totals the started records
        private static bool Describes(HeaderRecord header, StartedRecord started) =>
            header.C == started.Association.C && string.Equals(header.TableId, started.TableId, StringComparison.Ordinal) && header.O == started.O && header.A == started.A
            && header.Head == started.Head && header.FirstJson == started.FirstJson && header.Totals == started.Totals && header.Anchor.Version == started.Anchor.Version
            && string.Equals(header.Anchor.StageId, started.Anchor.StageId, StringComparison.Ordinal) && header.Anchor.Length == started.Anchor.Length
            && header.Anchor.Sha256.AsSpan().SequenceEqual(started.Anchor.Sha256) && header.Anchor.ListedSize == started.Anchor.ListedSize;

        // The whole part its descriptor names: length, digest, structure and head
        private bool Matches(RecordBytes part, PartDescriptor descriptor) =>
            part.Status == RecordStatus.Valid && part.Length == descriptor.Bytes && part.Sha256.AsSpan().SequenceEqual(descriptor.Sha256)
            && ControlRecords.ValidatePart(part.Bytes.Span, out var head) == RecordStatus.Valid
            && head == new PartHead(_active.C, _active.Association.AdvanceId, descriptor.Ordinal, descriptor.Paths);

        private IOPath LogPath(string name) => _executor.Configuration.Store.Table.Combine(DeltaTransactionReader.DeltaLogDirName).Combine(name);

        // The offset after skipping records of a validated part
        private static int Walk(ReadOnlySpan<byte> part, int offset, int records)
        {
            for (int i = 0; i < records; i++)
            {
                offset += 4 + (int)BinaryPrimitives.ReadUInt32LittleEndian(part.Slice(offset));
            }
            return offset;
        }

        private static (string Path, int Next) PathAt(ReadOnlyMemory<byte> part, int offset)
        {
            var span = part.Span;
            var length = (int)BinaryPrimitives.ReadUInt32LittleEndian(span.Slice(offset));
            return (Encoding.UTF8.GetString(span.Slice(offset + 4, length)), offset + 4 + length);
        }

        // The demand's listing debt follows the latest passed gate
        private GateResult Passed(LogEvidence log)
        {
            _active.LogEntries = log.Entries.Count;
            return new GateResult(GateVerdict.Passed) { Head = log.Head };
        }

        private static GateResult Changed(LogEvidence log, string detail) => new GateResult(GateVerdict.AuthorityChanged, detail) { Head = log.Head };

        private void Release()
        {
            _part?.Dispose();
            _part = null;
            _start = -1;
        }
    }
}
