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

using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Checkpoints;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance;
using ParquetSharp;
using System.Runtime.InteropServices;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat.Bounded
{
    /// <summary>
    /// Reads an admitted root as checkpoint records under runtime decode checks.
    /// </summary>
    internal sealed class BoundedCheckpointReader : IDisposable
    {
        public const int LevelBatch = 1024;
        public const int ActionCap = 4 << 20;


        private readonly ScanResult _scan;
        private readonly JobMemory _memory;
        private readonly int _entryCap;
        private readonly LeafCursor?[] _byField = new LeafCursor?[CheckpointSchema.LeafCount];
        private readonly List<LeafCursor> _cursors = new List<LeafCursor>();
        private NativeReservation? _native;
        private ParquetFileReader? _file;
        private byte[] _row = System.Array.Empty<byte>();
        private byte[] _record = System.Array.Empty<byte>();
        private int _rowLength;
        private int _rowEntries;
        private long _reservedArrays;
        // The sink threw, so its exception is not the root's decode failure
        private bool _sinkThrew;
        private readonly string _path;
        private readonly long _length;

        // The most column readers open at once
        public int MaxOpenColumnReaders { get; private set; }

        // Edits a batch as read, so tests can feed the reader batches a real file never produces
        internal LevelsHook? LevelsReadHookForTests { get; set; }

        internal delegate void LevelsHook(ProjectedLeaf leaf, Span<short> definitions, ref long levels, ref long values);

        public BoundedCheckpointReader(string path, ScanResult scan, JobMemory memory, int entryCap = TailRecordEncoder.DefaultEntryCap, long bufferSize = CheckpointPageScan.ReaderBufferSize)
        {
            // The pinned buffered stream overflows only when its buffer is at most the first header peek
            if (bufferSize <= CheckpointPageScan.FirstHeaderPeek)
            {
                throw new InvalidOperationException($"The reader buffer must stay above {CheckpointPageScan.FirstHeaderPeek} bytes");
            }
            _scan = scan;
            _memory = memory;
            _entryCap = entryCap;
            _path = path;
            // A file gone before the open fails here as I/O, never as an undecodable root
            _length = new FileInfo(path).Length;
            _native = memory.ReserveNative(scan.DecodeBound + scan.MetadataBound);
            try
            {
                using var properties = ReaderProperties.WithMemoryPool(MemoryPool.SystemMemoryPool());
                properties.EnableBufferedStream();
                properties.BufferSize = bufferSize;
                properties.SetThriftStringSizeLimit((int)Math.Max(scan.Footer.FooterLength, 1));
                properties.SetThriftContainerSizeLimit((int)ThriftBudget.DefaultMaxEntries);
                memory.CountNativeOpen();
                _file = new ParquetFileReader(path, properties);
            }
            catch (ParquetException e)
            {
                _native.Dispose();
                _native = null;
                throw NativeFailure(e, "The native reader refuses the admitted footer: ");
            }
            catch
            {
                _native.Dispose();
                _native = null;
                throw;
            }
        }

        private static CheckpointDeclinedException DecodeFailure(string detail)
        {
            return new CheckpointDeclinedException(MaintenanceReason.RootDecodeFailure, DeclineDependency.None, detail);
        }

        // Arrow reports I/O and codec errors alike as ParquetException: an OS error, an allocation failure, or a file changed since the open is transient
        private Exception NativeFailure(ParquetException e, string prefix)
        {
            var message = e.Message;
            if (message.Contains("[Windows error ", StringComparison.Ordinal) || message.Contains("[errno ", StringComparison.Ordinal)
                || message.Contains("Out of memory", StringComparison.Ordinal) || !Unchanged())
            {
                return new IOException($"Checkpoint {_path} could not be read: {message}", e);
            }
            return DecodeFailure(prefix + message);
        }

        private bool Unchanged()
        {
            try
            {
                using var file = new FileStream(_path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete, 1);
                return file.Length == _length;
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
        /// Streams one record per non-null action and returns the row count; cancellation is checked between batches.
        /// </summary>
        public long Read(RecordSink sink, CancellationToken cancellationToken = default)
        {
            long rows = 0;
            try
            {
                for (int g = 0; g < _scan.Footer.RowGroupRows.Count; g++)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    rows += ReadGroup(g, sink, cancellationToken);
                }
            }
            catch (ParquetException e) when (!_sinkThrew)
            {
                // Invalid dictionary indices, byte-array extents and codec errors surface from the native reader
                throw NativeFailure(e, "");
            }
            catch (InvalidCastException e) when (!_sinkThrew)
            {
                // A column whose native type differs from the scanned one
                throw DecodeFailure(e.Message);
            }
            if (rows != _file!.FileMetaData.NumRows)
            {
                throw DecodeFailure($"The row groups hold {rows} rows, the footer declares {_file.FileMetaData.NumRows}");
            }
            return rows;
        }

        private long ReadGroup(int group, RecordSink sink, CancellationToken cancellationToken)
        {
            using var rowGroup = _file!.RowGroup(group);
            var expected = _scan.Footer.RowGroupRows[group];
            OpenCursors(rowGroup, group);
            try
            {
                long rows = 0;
                while (rows < expected)
                {
                    if ((rows & 1023) == 0)
                    {
                        cancellationToken.ThrowIfCancellationRequested();
                    }
                    _rowLength = 0;
                    _rowEntries = 0;
                    foreach (var cursor in _cursors)
                    {
                        ReadRow(cursor);
                    }
                    Compose(sink);
                    rows++;
                }
                foreach (var cursor in _cursors)
                {
                    if (cursor.Position < cursor.Levels || cursor.Reader.HasNext || cursor.TotalLevels != cursor.Chunk.NumValues)
                    {
                        throw DecodeFailure($"Leaf {cursor.Leaf.LogicalPath} holds more or fewer levels than the {expected} rows of row group {group}");
                    }
                }
                return rows;
            }
            finally
            {
                // Every column reader of a group closes before the next group opens
                foreach (var cursor in _cursors)
                {
                    cursor.Reader.Dispose();
                    cursor.Release(_memory);
                }
                _cursors.Clear();
                System.Array.Clear(_byField);
            }
        }

        private void OpenCursors(RowGroupReader rowGroup, int group)
        {
            var leaves = _scan.Projection.Leaves;
            // Level and value arrays of every projected leaf, held across groups
            var arrays = (long)leaves.Count * LevelBatch * (2 * sizeof(short) + 16);
            if (arrays > _reservedArrays)
            {
                _memory.Reserve(arrays - _reservedArrays);
                _reservedArrays = arrays;
            }
            foreach (var leaf in leaves)
            {
                var cursor = new LeafCursor(leaf, rowGroup.Column(leaf.Index), _scan.Footer.Chunks[group][leaf.Index]!);
                _cursors.Add(cursor);
                MaxOpenColumnReaders = Math.Max(MaxOpenColumnReaders, _cursors.Count);
                if (leaf.Role == ProjectedRole.Field)
                {
                    _byField[(int)leaf.Field] = cursor;
                }
            }
        }

        // One row's levels of one leaf: the level with repetition 0, then every following level with repetition above 0
        private void ReadRow(LeafCursor cursor)
        {
            cursor.Entries = 0;
            var first = true;
            while (true)
            {
                if (cursor.Position == cursor.Levels && !Refill(cursor))
                {
                    if (first)
                    {
                        throw DecodeFailure($"Leaf {cursor.Leaf.LogicalPath} ended before its row group");
                    }
                    return;
                }
                var position = cursor.Position;
                var repetition = cursor.Leaf.MaxRepetition == 0 ? 0 : cursor.Repetition[position];
                if (!first && repetition == 0)
                {
                    return;
                }
                if ((first && repetition != 0) || repetition < 0 || repetition > cursor.Leaf.MaxRepetition)
                {
                    throw DecodeFailure($"Leaf {cursor.Leaf.LogicalPath} has repetition level {repetition} out of place");
                }
                var definition = cursor.Definition[position];
                if (definition < 0 || definition > cursor.Leaf.MaxDefinition)
                {
                    throw DecodeFailure($"Leaf {cursor.Leaf.LogicalPath} has definition level {definition} above its schema maximum");
                }
                // A further entry of the innermost collection exists, so its repeated element is defined; an outer list may continue with an empty inner one
                if (repetition == cursor.Leaf.MaxRepetition && repetition > 0 && definition < cursor.Leaf.RepeatedDefinition)
                {
                    throw DecodeFailure($"Leaf {cursor.Leaf.LogicalPath} continues a row with definition level {definition} below its entry level");
                }
                // The entry cap holds per action, over all its collections, and is checked before the entry arrays grow
                if (cursor.Leaf.EntryDefinition >= 0 && definition >= cursor.Leaf.EntryDefinition && ++_rowEntries > _entryCap)
                {
                    throw new CheckpointDeclinedException(MaintenanceReason.EntriesOverCap, DeclineDependency.Root, $"A root row has more than {_entryCap} entries");
                }
                if (cursor.Entries >= _entryCap)
                {
                    throw new CheckpointDeclinedException(MaintenanceReason.EntriesOverCap, DeclineDependency.Root, $"A root row has more than {_entryCap} entries in {cursor.Leaf.LogicalPath}");
                }
                cursor.EnsureEntries(cursor.Entries + 1, _memory);
                var entry = cursor.Entries++;
                cursor.EntryDefinition[entry] = definition;
                if (definition == cursor.Leaf.MaxDefinition)
                {
                    if (cursor.ValuePosition >= cursor.ValuesRead)
                    {
                        throw DecodeFailure($"Leaf {cursor.Leaf.LogicalPath} has fewer values than defined levels");
                    }
                    TakeValue(cursor, entry);
                    cursor.ValuePosition++;
                }
                cursor.Position++;
                first = false;
                if (cursor.Leaf.MaxRepetition == 0)
                {
                    return;
                }
            }
        }

        private void TakeValue(LeafCursor cursor, int entry)
        {
            var index = cursor.ValuePosition;
            switch (cursor.Leaf.Type)
            {
                case ParquetPhysicalType.ByteArray:
                    var value = cursor.Bytes![index];
                    // Copied before the reader advances; the action cap is checked before the row buffer grows
                    if ((long)_rowLength + value.Length > ActionCap)
                    {
                        throw new CheckpointDeclinedException(MaintenanceReason.RecordOverCap, DeclineDependency.Root, $"A root row is over the {ActionCap} byte action cap");
                    }
                    EnsureRow(_rowLength + value.Length);
                    if (value.Length > 0)
                    {
                        Marshal.Copy(value.Pointer, _row, _rowLength, value.Length);
                    }
                    cursor.EntryStart[entry] = _rowLength;
                    cursor.EntryLength[entry] = value.Length;
                    _rowLength += value.Length;
                    break;
                case ParquetPhysicalType.Int64:
                    cursor.EntryValue[entry] = cursor.Longs![index];
                    break;
                case ParquetPhysicalType.Int32:
                    cursor.EntryValue[entry] = cursor.Ints![index];
                    break;
                case ParquetPhysicalType.Boolean:
                    cursor.EntryValue[entry] = cursor.Bools![index] ? 1 : 0;
                    break;
            }
        }

        private void EnsureRow(int length)
        {
            if (length <= _row.Length)
            {
                return;
            }
            var grown = _memory.Allocate((int)Math.Min(ActionCap, Math.Max(length, Math.Max(4096L, _row.Length * 2L))));
            _row.AsSpan(0, _rowLength).CopyTo(grown);
            if (_row.Length > 0)
            {
                _memory.Free(_row);
            }
            _row = grown;
        }

        // Every batch makes progress until the chunk's levels are consumed, and defined levels match the values returned
        private bool Refill(LeafCursor cursor)
        {
            if (!cursor.Reader.HasNext)
            {
                return false;
            }
            long valuesRead;
            long levels = cursor.Leaf.Type switch
            {
                ParquetPhysicalType.ByteArray => ((ColumnReader<ByteArray>)cursor.Reader).ReadBatch(LevelBatch, cursor.Definition, cursor.Repetition, cursor.Bytes, out valuesRead),
                ParquetPhysicalType.Int64 => ((ColumnReader<long>)cursor.Reader).ReadBatch(LevelBatch, cursor.Definition, cursor.Repetition, cursor.Longs, out valuesRead),
                ParquetPhysicalType.Int32 => ((ColumnReader<int>)cursor.Reader).ReadBatch(LevelBatch, cursor.Definition, cursor.Repetition, cursor.Ints, out valuesRead),
                _ => ((ColumnReader<bool>)cursor.Reader).ReadBatch(LevelBatch, cursor.Definition, cursor.Repetition, cursor.Bools, out valuesRead)
            };
            LevelsReadHookForTests?.Invoke(cursor.Leaf, cursor.Definition, ref levels, ref valuesRead);
            if (levels <= 0 || levels > LevelBatch || valuesRead < 0 || valuesRead > levels)
            {
                throw DecodeFailure($"Leaf {cursor.Leaf.LogicalPath} returned {levels} levels and {valuesRead} values");
            }
            var defined = 0;
            for (int i = 0; i < levels; i++)
            {
                if (cursor.Definition[i] == cursor.Leaf.MaxDefinition)
                {
                    defined++;
                }
            }
            if (defined != valuesRead)
            {
                throw DecodeFailure($"Leaf {cursor.Leaf.LogicalPath} returned {valuesRead} values for {defined} defined levels");
            }
            cursor.Levels = (int)levels;
            cursor.Position = 0;
            cursor.ValuesRead = (int)valuesRead;
            cursor.ValuePosition = 0;
            cursor.TotalLevels += levels;
            return true;
        }

        private void Compose(RecordSink sink)
        {
            foreach (var cursor in _cursors)
            {
                var top = cursor.Leaf.Definitions[0];
                if (cursor.Leaf.Role == ProjectedRole.SidecarPath && cursor.EntryDefinition[0] >= top)
                {
                    throw new CheckpointDeclinedException(MaintenanceReason.SidecarRoot, DeclineDependency.Root, "The root uses sidecar files");
                }
                if (cursor.Leaf.Role == ProjectedRole.Unknown && cursor.EntryDefinition[0] >= top)
                {
                    var column = cursor.Leaf.LogicalPath.Split('.')[0];
                    throw new CheckpointDeclinedException(MaintenanceReason.UnknownColumn, DeclineDependency.Root, $"The root has values in the unknown column {column}");
                }
            }
            if (Present(Leaf.ProtocolMinReader, Leaf.ProtocolWriterFeature))
            {
                Emit(sink, RecordKind.Protocol);
            }
            if (Present(Leaf.MetaId, Leaf.MetaConfigurationValue))
            {
                Emit(sink, RecordKind.MetaData);
            }
            if (Present(Leaf.TxnAppId, Leaf.TxnLastUpdated) && Value(Leaf.TxnAppId, out _))
            {
                Emit(sink, RecordKind.Txn);
            }
            if (Present(Leaf.AddPath, Leaf.AddClusteringProvider))
            {
                Emit(sink, RecordKind.Add);
            }
            if (Present(Leaf.RemovePath, Leaf.RemoveDefaultRowCommitVersion))
            {
                Emit(sink, RecordKind.Remove);
            }
            if (Present(Leaf.DomainName, Leaf.DomainRemoved) && Value(Leaf.DomainName, out _))
            {
                Emit(sink, RecordKind.Domain);
            }
        }

        // A top-level column is non-null when any of its leaves in the file is defined at the column's level
        private bool Present(Leaf first, Leaf last)
        {
            for (var leaf = first; leaf <= last; leaf++)
            {
                if (_byField[(int)leaf] is { } cursor)
                {
                    return cursor.EntryDefinition[0] >= cursor.Leaf.Definitions[0];
                }
            }
            return false;
        }

        private bool Value(Leaf leaf, out ReadOnlySpan<byte> bytes)
        {
            bytes = default;
            if (_byField[(int)leaf] is not { } cursor || cursor.EntryDefinition[0] != cursor.Leaf.MaxDefinition)
            {
                return false;
            }
            bytes = _row.AsSpan(cursor.EntryStart[0], cursor.EntryLength[0]);
            return true;
        }

        private void Emit(RecordSink sink, RecordKind kind)
        {
            // Fields, counts and markers on top of the row's value bytes
            var entries = 0;
            foreach (var cursor in _cursors)
            {
                entries += cursor.Entries;
            }
            var bound = _rowLength + 16L * entries + 512;
            if (bound > _record.Length)
            {
                var grown = _memory.Allocate((int)bound);
                if (_record.Length > 0)
                {
                    _memory.Free(_record);
                }
                _record = grown;
            }
            var writer = new RecordWriter(_record);
            switch (kind)
            {
                case RecordKind.Add:
                    var parsedOnly = false;
                    var hasStats = Value(Leaf.AddStats, out var stats);
                    string? converted = null;
                    if (!hasStats)
                    {
                        converted = ParsedStatistics();
                        parsedOnly = converted != null;
                    }
                    writer.WriteKind(RecordKind.Add, parsedOnly);
                    String(ref writer, Leaf.AddPath);
                    Map(ref writer, Leaf.AddPartitionKey, Leaf.AddPartitionValue);
                    Long(ref writer, Leaf.AddSize);
                    Long(ref writer, Leaf.AddModificationTime);
                    Bool(ref writer, Leaf.AddDataChange);
                    if (hasStats)
                    {
                        writer.WriteString(stats);
                    }
                    else
                    {
                        writer.WriteString(converted);
                    }
                    Map(ref writer, Leaf.AddTagKey, Leaf.AddTagValue);
                    Vector(ref writer, Leaf.AddDvStorageType);
                    Long(ref writer, Leaf.AddBaseRowId);
                    Long(ref writer, Leaf.AddDefaultRowCommitVersion);
                    String(ref writer, Leaf.AddClusteringProvider);
                    break;
                case RecordKind.Remove:
                    writer.WriteKind(RecordKind.Remove, false);
                    String(ref writer, Leaf.RemovePath);
                    Long(ref writer, Leaf.RemoveDeletionTimestamp);
                    Bool(ref writer, Leaf.RemoveDataChange);
                    Bool(ref writer, Leaf.RemoveExtendedFileMetadata);
                    Map(ref writer, Leaf.RemovePartitionKey, Leaf.RemovePartitionValue);
                    Long(ref writer, Leaf.RemoveSize);
                    Vector(ref writer, Leaf.RemoveDvStorageType);
                    Long(ref writer, Leaf.RemoveBaseRowId);
                    Long(ref writer, Leaf.RemoveDefaultRowCommitVersion);
                    break;
                case RecordKind.MetaData:
                    writer.WriteKind(RecordKind.MetaData, false);
                    String(ref writer, Leaf.MetaId);
                    String(ref writer, Leaf.MetaName);
                    String(ref writer, Leaf.MetaDescription);
                    var format = StructPresent(Leaf.MetaFormatProvider, Leaf.MetaFormatOptionKey);
                    writer.WriteStruct(format);
                    if (format)
                    {
                        String(ref writer, Leaf.MetaFormatProvider);
                        Map(ref writer, Leaf.MetaFormatOptionKey, Leaf.MetaFormatOptionValue);
                    }
                    String(ref writer, Leaf.MetaSchemaString);
                    List(ref writer, Leaf.MetaPartitionColumn);
                    Long(ref writer, Leaf.MetaCreatedTime);
                    Map(ref writer, Leaf.MetaConfigurationKey, Leaf.MetaConfigurationValue);
                    break;
                case RecordKind.Protocol:
                    writer.WriteKind(RecordKind.Protocol, false);
                    Long(ref writer, Leaf.ProtocolMinReader);
                    Long(ref writer, Leaf.ProtocolMinWriter);
                    List(ref writer, Leaf.ProtocolReaderFeature);
                    List(ref writer, Leaf.ProtocolWriterFeature);
                    break;
                case RecordKind.Txn:
                    writer.WriteKind(RecordKind.Txn, false);
                    String(ref writer, Leaf.TxnAppId);
                    Long(ref writer, Leaf.TxnVersion);
                    Long(ref writer, Leaf.TxnLastUpdated);
                    break;
                case RecordKind.Domain:
                    writer.WriteKind(RecordKind.Domain, false);
                    String(ref writer, Leaf.DomainName);
                    String(ref writer, Leaf.DomainConfiguration);
                    Bool(ref writer, Leaf.DomainRemoved);
                    break;
            }
            try
            {
                sink(_record.AsSpan(0, writer.Position));
            }
            catch
            {
                _sinkThrew = true;
                throw;
            }
        }

        // Parsed record counts become JSON stats, parsed min, max or null counts decline
        private string? ParsedStatistics()
        {
            long? numRecords = null;
            bool? tightBounds = null;
            bool min = false, max = false, nulls = false;
            foreach (var cursor in _cursors)
            {
                var definition = cursor.EntryDefinition[0];
                switch (cursor.Leaf.Role)
                {
                    case ProjectedRole.StatsNumRecords when definition == cursor.Leaf.MaxDefinition:
                        numRecords = cursor.EntryValue[0];
                        break;
                    case ProjectedRole.StatsTightBounds when definition == cursor.Leaf.MaxDefinition:
                        tightBounds = cursor.EntryValue[0] != 0;
                        break;
                    // The struct under stats_parsed is non-null at this row
                    case ProjectedRole.StatsMinValues:
                        min |= definition >= cursor.Leaf.Definitions[2];
                        break;
                    case ProjectedRole.StatsMaxValues:
                        max |= definition >= cursor.Leaf.Definitions[2];
                        break;
                    case ProjectedRole.StatsNullCount:
                        nulls |= definition >= cursor.Leaf.Definitions[2];
                        break;
                }
            }
            if (numRecords == null && tightBounds == null && !min && !max && !nulls)
            {
                return null;
            }
            return CheckpointCapability.ParsedStatistics(numRecords, tightBounds, min, max, nulls);
        }

        private void String(ref RecordWriter writer, Leaf leaf)
        {
            if (Value(leaf, out var bytes))
            {
                writer.WriteString(bytes);
            }
            else
            {
                writer.WriteNull();
            }
        }

        private void Long(ref RecordWriter writer, Leaf leaf)
        {
            if (_byField[(int)leaf] is { } cursor && cursor.EntryDefinition[0] == cursor.Leaf.MaxDefinition)
            {
                writer.WriteLong(cursor.EntryValue[0]);
            }
            else
            {
                writer.WriteNull();
            }
        }

        private void Bool(ref RecordWriter writer, Leaf leaf)
        {
            if (_byField[(int)leaf] is { } cursor && cursor.EntryDefinition[0] == cursor.Leaf.MaxDefinition)
            {
                writer.WriteBool(cursor.EntryValue[0] != 0);
            }
            else
            {
                writer.WriteNull();
            }
        }

        // A nested struct is present when one of its leaves is defined at the struct's level
        private bool StructPresent(Leaf first, Leaf second)
        {
            foreach (var leaf in new[] { first, second })
            {
                if (_byField[(int)leaf] is { } cursor)
                {
                    return cursor.EntryDefinition[0] >= cursor.Leaf.Definitions[1];
                }
            }
            return false;
        }

        private void Vector(ref RecordWriter writer, Leaf first)
        {
            var present = false;
            for (var leaf = first; leaf <= first + 4; leaf++)
            {
                if (_byField[(int)leaf] is { } cursor)
                {
                    present = cursor.EntryDefinition[0] >= cursor.Leaf.Definitions[1];
                    break;
                }
            }
            writer.WriteStruct(present);
            if (!present)
            {
                return;
            }
            String(ref writer, first);
            String(ref writer, first + 1);
            Long(ref writer, first + 2);
            Long(ref writer, first + 3);
            Long(ref writer, first + 4);
        }

        // The node is defined at the third level from the leaf, its entries at the second
        private int? Entries(LeafCursor cursor)
        {
            var definitions = cursor.Leaf.Definitions;
            var first = cursor.EntryDefinition[0];
            if (first < definitions[^3])
            {
                return null;
            }
            if (first < definitions[^2])
            {
                return 0;
            }
            return cursor.Entries;
        }

        private void Map(ref RecordWriter writer, Leaf key, Leaf value)
        {
            if (_byField[(int)key] is not { } keys)
            {
                writer.WriteNull();
                return;
            }
            var count = Entries(keys);
            var values = _byField[(int)value];
            if (values != null && Entries(values) != count)
            {
                throw DecodeFailure($"Map {keys.Leaf.LogicalPath} has {count} keys and {Entries(values)} values");
            }
            writer.WriteCount(count);
            for (int i = 0; i < (count ?? 0); i++)
            {
                if (keys.EntryDefinition[i] != keys.Leaf.MaxDefinition)
                {
                    throw DecodeFailure($"Map {keys.Leaf.LogicalPath} has a null key");
                }
                writer.WriteString(_row.AsSpan(keys.EntryStart[i], keys.EntryLength[i]));
                if (values != null && values.EntryDefinition[i] == values.Leaf.MaxDefinition)
                {
                    writer.WriteString(_row.AsSpan(values.EntryStart[i], values.EntryLength[i]));
                }
                else
                {
                    writer.WriteNull();
                }
            }
        }

        private void List(ref RecordWriter writer, Leaf item)
        {
            if (_byField[(int)item] is not { } items)
            {
                writer.WriteNull();
                return;
            }
            var count = Entries(items);
            writer.WriteCount(count);
            for (int i = 0; i < (count ?? 0); i++)
            {
                if (items.EntryDefinition[i] == items.Leaf.MaxDefinition)
                {
                    writer.WriteString(_row.AsSpan(items.EntryStart[i], items.EntryLength[i]));
                }
                else
                {
                    writer.WriteNull();
                }
            }
        }

        public void Dispose()
        {
            try
            {
                foreach (var cursor in _cursors)
                {
                    cursor.Reader.Dispose();
                    cursor.Release(_memory);
                }
                _cursors.Clear();
                _file?.Dispose();
            }
            finally
            {
                _file = null;
                if (_row.Length > 0)
                {
                    _memory.Free(_row);
                    _row = System.Array.Empty<byte>();
                }
                if (_record.Length > 0)
                {
                    _memory.Free(_record);
                    _record = System.Array.Empty<byte>();
                }
                if (_reservedArrays > 0)
                {
                    _memory.Release(_reservedArrays);
                    _reservedArrays = 0;
                }
                _native?.Dispose();
                _native = null;
            }
        }

        /// <summary>
        /// One projected leaf of the open row group: its reader, the current batch and the current row's entries.
        /// </summary>
        private sealed class LeafCursor
        {
            public LeafCursor(ProjectedLeaf leaf, ColumnReader reader, FooterChunk chunk)
            {
                Leaf = leaf;
                Reader = reader;
                Chunk = chunk;
                switch (leaf.Type)
                {
                    case ParquetPhysicalType.ByteArray:
                        Bytes = new ByteArray[LevelBatch];
                        break;
                    case ParquetPhysicalType.Int64:
                        Longs = new long[LevelBatch];
                        break;
                    case ParquetPhysicalType.Int32:
                        Ints = new int[LevelBatch];
                        break;
                    default:
                        Bools = new bool[LevelBatch];
                        break;
                }
            }

            public ProjectedLeaf Leaf { get; }

            public ColumnReader Reader { get; }

            public FooterChunk Chunk { get; }

            public short[] Definition { get; } = new short[LevelBatch];

            public short[] Repetition { get; } = new short[LevelBatch];

            public ByteArray[]? Bytes { get; }

            public long[]? Longs { get; }

            public int[]? Ints { get; }

            public bool[]? Bools { get; }

            public int Levels;
            public int Position;
            public int ValuesRead;
            public int ValuePosition;
            public long TotalLevels;

            // The current row's entries: definition level and value
            public int Entries;
            public int[] EntryDefinition = new int[1];
            public int[] EntryStart = new int[1];
            public int[] EntryLength = new int[1];
            public long[] EntryValue = new long[1];
            private long _reserved;

            public void EnsureEntries(int count, JobMemory memory)
            {
                if (count <= EntryDefinition.Length)
                {
                    return;
                }
                var capacity = Math.Max(count, EntryDefinition.Length * 2);
                var bytes = (long)capacity * 20;
                memory.Reserve(bytes);
                System.Array.Resize(ref EntryDefinition, capacity);
                System.Array.Resize(ref EntryStart, capacity);
                System.Array.Resize(ref EntryLength, capacity);
                System.Array.Resize(ref EntryValue, capacity);
                memory.Release(_reserved);
                _reserved = bytes;
            }

            public void Release(JobMemory memory)
            {
                memory.Release(_reserved);
                _reserved = 0;
            }
        }
    }
}
