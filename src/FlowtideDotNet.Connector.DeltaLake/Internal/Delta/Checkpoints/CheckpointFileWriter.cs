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

using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat.Bounded;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance;
using ParquetSharp;
using ParquetSharp.Arrow;
using System.Security.Cryptography;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Checkpoints
{
    /// <summary>
    /// What a closed writer produced; validation recomputes the counts and the digest from the file.
    /// </summary>
    internal sealed record CheckpointWriteResult(long Actions, long AddFiles, byte[] Sha256, IReadOnlyList<long> GroupRows, long OutputBytes);

    /// <summary>
    /// The writer's caps; tests lower them to reach each closure rule.
    /// </summary>
    internal sealed record CheckpointWriterLimits
    {
        public static readonly CheckpointWriterLimits Default = new CheckpointWriterLimits();

        // Plain-encoded value bytes fed per row group
        public long GroupValueBytes { get; init; } = 16L << 20;

        // Native memory reserved for the writer
        public long WriterReservation { get; init; } = 96L << 20;

        // A group closes 8 MiB below this decode cap
        public long DecodeCap { get; init; } = 128L << 20;

        // Fixtures with other pages, encodings or codecs replace the writer settings
        public Func<Compression, WriterProperties>? PropertiesForTests { get; init; }

        // The build declines 1 MiB below this footer metadata cap
        public long MetadataCap { get; init; } = 32L << 20;
    }

    internal enum GroupCloseReason : byte
    {
        ValueBytes,
        WriterMemory,
        DecodeBound
    }

    /// <summary>
    /// Writes records as a checkpoint file in bounded batches and row groups, charging the disk ledger ahead of the native writer.
    /// </summary>
    internal sealed class CheckpointFileWriter : IDisposable
    {
        public const long ReaderBufferSize = 64L << 10;

        // The magic the writer puts down when it opens the file, with room to spare
        public const long OpenBytes = 64L << 10;

        private const int Leaves = CheckpointSchema.LeafCount;

        private readonly string _path;
        private readonly DiskLedger _ledger;
        private readonly Compression _compression;
        private readonly CheckpointBatch _batch;
        private readonly IncrementalHash _hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        private readonly Apache.Arrow.Schema _schema;
        private readonly List<long> _groupRows = new List<long>();
        private readonly List<GroupCloseReason> _closeReasons = new List<GroupCloseReason>();
        private readonly CheckpointWriterLimits _limits;
        private NativeReservation? _native;
        private FileWriter? _writer;
        private bool _groupOpen;
        private bool _closed;

        // The opening bytes and the projected bytes of every closed group
        private long _closedBytes = OpenBytes;

        // The open group, per leaf: value bytes, levels, most value bytes in one batch, most levels in one row
        private readonly long[] _groupValueBytes = new long[Leaves];
        private readonly long[] _groupLevels = new long[Leaves];
        private readonly long[] _groupBatchValueBytes = new long[Leaves];
        private readonly long[] _groupRowLevels = new long[Leaves];
        private long _groupRowCount;

        public CheckpointFileWriter(string path, DiskLedger ledger, JobMemory memory, Compression compression, string? marker, CheckpointWriterLimits? limits = null)
        {
            _path = path;
            _ledger = ledger;
            _compression = compression;
            _limits = limits ?? CheckpointWriterLimits.Default;
            _batch = new CheckpointBatch(memory);
            _schema = CheckpointSchema.Build(marker);
            _native = memory.ReserveNative(_limits.WriterReservation);
            var opening = false;
            try
            {
                Charge(OpenBytes);
                using var properties = _limits.PropertiesForTests?.Invoke(compression) ?? CheckpointWriterSettings.Build(compression);
                using var arrowProperties = CheckpointWriterSettings.Arrow();
                opening = true;
                _writer = new FileWriter(path, _schema, properties, arrowProperties);
            }
            catch
            {
                _native.Dispose();
                _native = null;
                _batch.Release();
                _hash.Dispose();
                // Only a file the native open may have created is deleted; one it left behind keeps its charge
                if (!opening || TryDelete(path))
                {
                    _ledger.Release(Charged);
                    Charged = 0;
                }
                throw;
            }
        }

        private static bool TryDelete(string path)
        {
            try
            {
                if (File.Exists(path))
                {
                    File.Delete(path);
                }
                return true;
            }
            catch (IOException)
            {
                return false;
            }
            catch (UnauthorizedAccessException)
            {
                return false;
            }
        }

        /// <summary>
        /// The disk bytes this file holds in the ledger, never below what it has written.
        /// </summary>
        public long Charged { get; private set; }

        // Runs after each group and the footer reach the file, so tests can compare its length with the charge
        internal Action? AfterWriteForTests { get; set; }

        private void Charge(long bytes)
        {
            _ledger.Reserve(bytes);
            Charged += bytes;
        }

        // The pages a group's column chunks can hold: the codec's worst case over values and levels, and a header per page
        private long ProjectedGroupBytes(bool withBatch)
        {
            long bytes = 0;
            var rows = _groupRowCount + (withBatch ? _batch.Rows : 0);
            for (int l = 0; l < Leaves; l++)
            {
                var values = _groupValueBytes[l] + (withBatch ? _batch.ValueBytes[l] : 0);
                var levels = _groupLevels[l] + (withBatch ? _batch.Levels[l] : 0);
                var streams = CheckpointSchema.LevelStreams((Leaf)l);
                var pages = (values + CheckpointWriterSettings.DataPageSize - 1) / CheckpointWriterSettings.DataPageSize + (rows + 19_999) / 20_000 + 1;
                bytes += NativeConstants.Compressed(_compression, values + streams * levels / 2) + 1024 * pages;
            }
            return bytes;
        }

        // Before the native writer takes anything: all that disposing it could write, the open group and the footer included, is charged
        private void Cover()
        {
            var needed = _closedBytes + ProjectedGroupBytes(withBatch: true) + ProjectedFooterBytes();
            if (needed > Charged)
            {
                Charge(needed - Charged);
            }
        }

        // The footer: our schema with the stored Arrow schema and the marker, and the metadata of every chunk
        private long ProjectedFooterBytes() => (1L << 20) + (long)_groupRows.Count * Leaves * 1024;

        public long Actions { get; private set; }

        public long AddFiles { get; private set; }

        public int RowGroups => _groupRows.Count;

        // Why each closed group was closed, in order
        public IReadOnlyList<GroupCloseReason> CloseReasons => _closeReasons;

        public void Append(ReadOnlySpan<byte> record)
        {
            if (!_batch.TryAdd(record))
            {
                Flush();
                _batch.TryAdd(record);
            }
            var emitted = _batch.Row(_batch.Rows - 1);
            AppendDigest(_hash, emitted);
            Actions++;
            if (CheckpointRecord.Kind(emitted) == RecordKind.Add)
            {
                AddFiles++;
            }
        }

        // The digest's framing, which validation repeats over the file read back: each record's length, then the record
        public static void AppendDigest(IncrementalHash hash, ReadOnlySpan<byte> record)
        {
            Span<byte> length = stackalloc byte[5];
            hash.AppendData(length.Slice(0, Varint.Write(length, (uint)record.Length)));
            hash.AppendData(record);
        }

        private void Flush()
        {
            if (_batch.Rows == 0)
            {
                return;
            }
            if (_groupOpen && ShouldClose() is { } reason)
            {
                _closeReasons.Add(reason);
                _groupOpen = false;
            }
            if (!_groupOpen)
            {
                // The closing group was covered with its last batch
                if (_groupRows.Count > 0)
                {
                    _closedBytes += ProjectedGroupBytes(withBatch: false);
                }
                var flushes = _groupRows.Count > 0;
                _groupRows.Add(0);
                _groupOpen = true;
                Array.Clear(_groupValueBytes);
                Array.Clear(_groupLevels);
                Array.Clear(_groupBatchValueBytes);
                Array.Clear(_groupRowLevels);
                _groupRowCount = 0;
                // A fresh group takes one batch within the batch, action and entry caps, so these hold unless those caps grow
                if (ProjectedDecode() > _limits.DecodeCap - (8L << 20))
                {
                    throw new CheckpointDeclinedException(MaintenanceReason.DecodeCap, DeclineDependency.None, $"One batch projects a decode bound of {ProjectedDecode()} bytes");
                }
                if (ProjectedWriter() > _limits.WriterReservation)
                {
                    throw new CheckpointDeclinedException(MaintenanceReason.NativeMemoryCap, DeclineDependency.None, $"One batch projects {ProjectedWriter()} bytes of writer memory");
                }
                CheckMetadata();
                Cover();
                // Starting a group flushes the previous one to the file
                _writer!.NewBufferedRowGroup();
                if (flushes)
                {
                    AfterWriteForTests?.Invoke();
                }
            }
            else
            {
                CheckMetadata();
                Cover();
            }

            using (var batch = _batch.Build(_schema))
            {
                _writer!.WriteBufferedRecordBatch(batch);
            }
            for (int l = 0; l < Leaves; l++)
            {
                _groupValueBytes[l] += _batch.ValueBytes[l];
                _groupLevels[l] += _batch.Levels[l];
                _groupBatchValueBytes[l] = Math.Max(_groupBatchValueBytes[l], _batch.ValueBytes[l]);
                _groupRowLevels[l] = Math.Max(_groupRowLevels[l], _batch.MaxRowLevels[l]);
            }
            _groupRowCount += _batch.Rows;
            _groupRows[^1] = _groupRowCount;
            _batch.Clear();
        }

        private void CheckMetadata()
        {
            if (ProjectedMetadata(_groupRows.Count) > _limits.MetadataCap - (1L << 20))
            {
                throw new CheckpointDeclinedException(MaintenanceReason.MetadataCap, DeclineDependency.None, $"The footer of {_groupRows.Count} row groups projects {ProjectedMetadata(_groupRows.Count)} bytes of reader metadata");
            }
        }

        private GroupCloseReason? ShouldClose()
        {
            long groupValueBytes = 0;
            for (int l = 0; l < Leaves; l++)
            {
                groupValueBytes += _groupValueBytes[l];
            }
            if (groupValueBytes + _batch.TotalValueBytes > _limits.GroupValueBytes)
            {
                return GroupCloseReason.ValueBytes;
            }
            if (ProjectedWriter() > _limits.WriterReservation)
            {
                return GroupCloseReason.WriterMemory;
            }
            if (ProjectedDecode() > _limits.DecodeCap - (8L << 20))
            {
                return GroupCloseReason.DecodeBound;
            }
            return null;
        }

        // One leaf of the open group with the pending batch added
        private (long F, long Levels, long BatchMax, long RowMax, int Streams) LeafWithBatch(int l)
        {
            return (_groupValueBytes[l] + _batch.ValueBytes[l],
                _groupLevels[l] + _batch.Levels[l],
                Math.Max(_groupBatchValueBytes[l], _batch.ValueBytes[l]),
                Math.Max(_groupRowLevels[l], _batch.MaxRowLevels[l]),
                CheckpointSchema.LevelStreams((Leaf)l));
        }

        /// <summary>
        /// The projected writer memory of the open group with the pending batch.
        /// </summary>
        public long ProjectedWriter()
        {
            var rows = _groupRowCount + _batch.Rows;
            long total = 0;
            long sinks = 0;
            long overlap = 0;
            long batchLevels = 0;
            for (int l = 0; l < Leaves; l++)
            {
                var (f, levels, batchMax, rowMax, streams) = LeafWithBatch(l);
                var pageValues = Math.Min(f, CheckpointWriterSettings.DataPageSize + batchMax);
                var pageLevels = Math.Min(levels, 20_000 * Math.Max(1, rowMax));
                var rle = streams * (pageLevels / 2 + 128);
                var page = pageValues + rle;
                var scratch = NativeConstants.HasScratch(_compression) ? NativeConstants.Compressed(_compression, page) : 0;
                var pages = (f + CheckpointWriterSettings.DataPageSize - 1) / CheckpointWriterSettings.DataPageSize + (rows + 19_999) / 20_000;
                var sink = NativeConstants.Compressed(_compression, f + streams * levels / 2) + 1024 * pages;
                total += 4 * streams * pageLevels + rle + 3 * pageValues + page + scratch;
                sinks += sink;
                // Every grow-only buffer's content, the largest is the one block a reallocation can overlap
                overlap = Math.Max(overlap, Math.Max(Math.Max(2 * pageLevels, rle), Math.Max(Math.Max(pageValues, page), Math.Max(scratch, sink))));
                batchLevels = Math.Max(batchLevels, _batch.Levels[l]);
            }
            var path = 8 * batchLevels + _batch.Rows;
            overlap = Math.Max(overlap, path);
            return total + 2 * sinks + overlap + path
                + (long)_groupRows.Count * Leaves * NativeConstants.WriterChunkMetadata
                + NativeConstants.WriterCodec + NativeConstants.WriterFixed;
        }

        /// <summary>
        /// Our own reader's decode bound for the open group with the pending batch.
        /// </summary>
        public long ProjectedDecode()
        {
            long total = 0;
            long largestPage = 0;
            long overlap = 0;
            for (int l = 0; l < Leaves; l++)
            {
                var (f, levels, batchMax, rowMax, streams) = LeafWithBatch(l);
                var pageLevels = Math.Min(levels, 20_000 * Math.Max(1, rowMax));
                var page = CheckpointWriterSettings.DataPageSize + batchMax + streams * (pageLevels / 2 + 128);
                var stream = ReaderBufferSize + NativeConstants.Compressed(_compression, f + streams * levels / 2);
                total += stream + page + NativeConstants.HeaderBound + NativeConstants.CodecPerLeaf;
                largestPage = Math.Max(largestPage, page);
                overlap = Math.Max(overlap, Math.Max(stream, page));
            }
            var compressedBody = NativeConstants.Compressed(_compression, largestPage) + (64L << 10);
            return total + compressedBody + overlap + NativeConstants.ReaderFixed;
        }

        /// <summary>
        /// The native metadata our own reader would hold for this file's footer at the given group count.
        /// </summary>
        public static long ProjectedMetadata(int groups)
        {
            long chunks = (long)groups * Leaves;
            var footer = NativeConstants.OwnFooterFixed + chunks * NativeConstants.OwnFooterBytesPerChunk;
            return footer + chunks * NativeConstants.ChunkMetadata + 128 * NativeConstants.SchemaElementMetadata + groups * NativeConstants.RowGroupMetadata
                + NativeConstants.StringFactor * (NativeConstants.OwnFooterFixed + chunks * NativeConstants.OwnFooterStringBytesPerChunk)
                + NativeConstants.ListEntryBytes * chunks * NativeConstants.OwnFooterListEntriesPerChunk;
        }

        /// <summary>
        /// Writes the last group and the footer, then makes the file durable; a failure while writing the footer surfaces here.
        /// </summary>
        public CheckpointWriteResult Close()
        {
            Flush();
            if (_groupRows.Count == 0)
            {
                throw new InvalidOperationException("A checkpoint has at least a protocol and a metaData row");
            }
            Cover();
            _writer!.Close();
            _closed = true;
            using (var file = new FileStream(_path, FileMode.Open, FileAccess.Write, FileShare.ReadWrite | FileShare.Delete))
            {
                file.Flush(true);
            }
            AfterWriteForTests?.Invoke();
            var length = new FileInfo(_path).Length;
            // The projections bound what was written; the charge now follows the file
            if (length > Charged)
            {
                Charge(length - Charged);
            }
            _ledger.Release(Charged - length);
            Charged = length;
            return new CheckpointWriteResult(Actions, AddFiles, _hash.GetHashAndReset(), _groupRows.ToArray(), length);
        }

        public void Dispose()
        {
            try
            {
                _writer?.Dispose();
            }
            finally
            {
                _writer = null;
                _batch.Release();
                _hash.Dispose();
                _native?.Dispose();
                _native = null;
            }
        }

        public bool Closed => _closed;
    }
}
