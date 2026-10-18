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

using Apache.Arrow;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance;
using System.Runtime.InteropServices;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Checkpoints
{
    /// <summary>
    /// One planned batch of rows over a ledger-held arena that never grows.
    /// </summary>
    internal sealed class CheckpointBatch
    {
        public const int MaxRows = 8192;
        public const long MaxValueBytes = 4L << 20;
        public const long MaxLeafValueBytes = 1L << 20;
        public const long MaxLeafLevels = 131_072;

        private const int Columns = 6;
        private const int Structs = 9;

        private readonly JobMemory _memory;
        private readonly Layout _layout = new Layout();
        private byte[] _rows = System.Array.Empty<byte>();
        private int[] _rowEnds = System.Array.Empty<int>();
        private byte[] _arena = System.Array.Empty<byte>();
        private int _rowsLength;

        // The batch so far, per leaf and per repeated node
        public readonly long[] ValueBytes = new long[CheckpointSchema.LeafCount];
        public readonly long[] Levels = new long[CheckpointSchema.LeafCount];
        public readonly long[] MaxRowLevels = new long[CheckpointSchema.LeafCount];
        private readonly long[] _dataBytes = new long[CheckpointSchema.LeafCount];
        private readonly long[] _entries = new long[CheckpointSchema.RepeatedCount];

        // The next row's contribution
        private readonly long[] _rowValueBytes = new long[CheckpointSchema.LeafCount];
        private readonly long[] _rowLevels = new long[CheckpointSchema.LeafCount];
        private readonly long[] _rowDataBytes = new long[CheckpointSchema.LeafCount];
        private readonly long[] _rowEntries = new long[CheckpointSchema.RepeatedCount];

        public CheckpointBatch(JobMemory memory)
        {
            _memory = memory;
        }

        public int Rows { get; private set; }

        public long TotalValueBytes { get; private set; }

        public long ArenaBytes => _arena.Length;

        public long RowBytes => _rows.Length + _rowEnds.Length * sizeof(int);

        /// <summary>
        /// Plans a row, false when it would cross a batch limit; an empty batch accepts any row.
        /// </summary>
        public bool TryAdd(ReadOnlySpan<byte> input)
        {
            if (Rows >= MaxRows)
            {
                return false;
            }
            // A full row buffer flushes the batch, so it grows only when empty and nothing is copied
            if (_rowsLength + input.Length > _rows.Length)
            {
                if (Rows > 0)
                {
                    return false;
                }
                EnsureRows(input.Length);
            }
            // The emitted form is planned, so the copy is made first and only kept when the row fits
            var copy = _rows.AsSpan(_rowsLength, input.Length);
            input.CopyTo(copy);
            Emit(copy);
            ReadOnlySpan<byte> record = copy;

            System.Array.Clear(_rowValueBytes);
            System.Array.Clear(_rowLevels);
            System.Array.Clear(_rowDataBytes);
            System.Array.Clear(_rowEntries);
            var sizing = new SizingVisitor(_rowValueBytes, _rowLevels, _rowDataBytes, _rowEntries);
            var column = RecordWalker.Column(CheckpointRecord.Kind(record));
            for (var c = StructNode.Txn; c <= StructNode.Domain; c++)
            {
                if (c == column)
                {
                    RecordWalker.Walk(record, ref sizing);
                }
                else
                {
                    RecordWalker.WalkNull(c, ref sizing);
                }
            }

            long rowValueBytes = 0;
            for (int l = 0; l < CheckpointSchema.LeafCount; l++)
            {
                rowValueBytes += _rowValueBytes[l];
            }
            if (Rows > 0)
            {
                if (TotalValueBytes + rowValueBytes > MaxValueBytes)
                {
                    return false;
                }
                for (int l = 0; l < CheckpointSchema.LeafCount; l++)
                {
                    if (ValueBytes[l] + _rowValueBytes[l] > MaxLeafValueBytes || Levels[l] + _rowLevels[l] > MaxLeafLevels)
                    {
                        return false;
                    }
                }
            }

            // The row is kept until the batch is built
            _rowsLength += record.Length;
            _rowEnds[Rows] = _rowsLength;
            Rows++;
            TotalValueBytes += rowValueBytes;
            for (int l = 0; l < CheckpointSchema.LeafCount; l++)
            {
                ValueBytes[l] += _rowValueBytes[l];
                Levels[l] += _rowLevels[l];
                _dataBytes[l] += _rowDataBytes[l];
                MaxRowLevels[l] = Math.Max(MaxRowLevels[l], _rowLevels[l]);
            }
            for (int n = 0; n < CheckpointSchema.RepeatedCount; n++)
            {
                _entries[n] += _rowEntries[n];
            }
            return true;
        }

        // The checkpoint form of a record: never parsed-only, and dataChange false on adds and removes
        private static void Emit(Span<byte> record)
        {
            record[0] &= 0x0F;
            var kind = CheckpointRecord.Kind(record);
            if (kind != RecordKind.Add && kind != RecordKind.Remove)
            {
                return;
            }
            var r = new RecordReader(record);
            r.ReadKind(out _);
            r.SkipString();
            if (kind == RecordKind.Add)
            {
                r.SkipMap();
                r.SkipLong();
                r.SkipLong();
            }
            else
            {
                r.SkipLong();
            }
            record[r.Position] = 1;
        }

        // The buffer is empty here; its first size holds a full batch of ordinary rows
        private void EnsureRows(int bytes)
        {
            if (bytes > _rows.Length)
            {
                if (_rows.Length > 0)
                {
                    _memory.Free(_rows);
                    _rows = System.Array.Empty<byte>();
                }
                _rows = _memory.Allocate(Math.Max(bytes, (int)(MaxValueBytes + (MaxValueBytes >> 2))));
            }
            // A batch never holds more than MaxRows rows
            if (_rowEnds.Length == 0)
            {
                _memory.Reserve(MaxRows * sizeof(int));
                _rowEnds = new int[MaxRows];
            }
        }

        public ReadOnlySpan<byte> Row(int index)
        {
            var start = index == 0 ? 0 : _rowEnds[index - 1];
            return _rows.AsSpan(start, _rowEnds[index] - start);
        }

        public void Clear()
        {
            Rows = 0;
            _rowsLength = 0;
            TotalValueBytes = 0;
            System.Array.Clear(ValueBytes);
            System.Array.Clear(Levels);
            System.Array.Clear(MaxRowLevels);
            System.Array.Clear(_dataBytes);
            System.Array.Clear(_entries);
        }

        public void Release()
        {
            if (_rows.Length > 0)
            {
                _memory.Free(_rows);
            }
            if (_arena.Length > 0)
            {
                _memory.Free(_arena);
            }
            if (_rowEnds.Length > 0)
            {
                _memory.Release(_rowEnds.Length * sizeof(int));
            }
            _rows = System.Array.Empty<byte>();
            _arena = System.Array.Empty<byte>();
            _rowEnds = System.Array.Empty<int>();
        }

        private static int Bitmap(long count) => (int)((count + 7) / 8);

        private static int Align(long size) => (int)((size + 7) & ~7L);

        /// <summary>
        /// Builds the planned rows as a record batch over the arena; the arrays are valid until the next build.
        /// </summary>
        public RecordBatch Build(Apache.Arrow.Schema schema)
        {
            var n = Rows;
            var layout = _layout;
            layout.Clear();
            long size = 0;
            for (int s = 0; s < Structs; s++)
            {
                layout.StructValidity[s] = (int)size;
                size += Align(Bitmap(n));
            }
            for (int r = 0; r < CheckpointSchema.RepeatedCount; r++)
            {
                layout.RepeatedOffsets[r] = (int)size;
                size += Align((n + 1L) * sizeof(int));
                layout.RepeatedValidity[r] = (int)size;
                size += Align(Bitmap(n));
            }
            for (int l = 0; l < CheckpointSchema.LeafCount; l++)
            {
                var leaf = (Leaf)l;
                var repeated = CheckpointSchema.RepeatedOf(leaf);
                long length = repeated >= 0 ? _entries[repeated] : n;
                layout.Length[l] = (int)length;
                layout.Validity[l] = (int)size;
                size += Align(Bitmap(length));
                layout.Values[l] = (int)size;
                size += CheckpointSchema.Type(leaf) switch
                {
                    LeafType.Int64 => Align(length * 8),
                    LeafType.Int32 => Align(length * 4),
                    LeafType.Bool => Align(Bitmap(length)),
                    _ => Align((length + 1) * sizeof(int))
                };
                layout.Data[l] = (int)size;
                size += Align(_dataBytes[l]);
            }
            if (size > int.MaxValue)
            {
                throw new InvalidOperationException("A batch arena is over 2 GiB");
            }
            // The arena is rebuilt for every batch, so the old one is released first and nothing overlaps
            if (_arena.Length < size)
            {
                if (_arena.Length > 0)
                {
                    _memory.Free(_arena);
                    _arena = System.Array.Empty<byte>();
                }
                _arena = _memory.Allocate((int)size);
            }
            _arena.AsSpan(0, (int)size).Clear();

            var fill = new FillVisitor(_arena, layout);
            for (int i = 0; i < n; i++)
            {
                var record = Row(i);
                var column = RecordWalker.Column(CheckpointRecord.Kind(record));
                for (var c = StructNode.Txn; c <= StructNode.Domain; c++)
                {
                    if (c == column)
                    {
                        RecordWalker.Walk(record, ref fill);
                    }
                    else
                    {
                        RecordWalker.WalkNull(c, ref fill);
                    }
                }
            }
            return Assemble(schema, layout, n);
        }

        private ArrowBuffer Slice(int at, long length) => new ArrowBuffer(new ReadOnlyMemory<byte>(_arena, at, (int)length));

        private IArrowArray LeafArray(Layout layout, Leaf leaf)
        {
            var l = (int)leaf;
            var length = layout.Length[l];
            var nulls = layout.Nulls[l];
            var validity = CheckpointSchema.IsMapKey(leaf) ? ArrowBuffer.Empty : Slice(layout.Validity[l], Bitmap(length));
            return CheckpointSchema.Type(leaf) switch
            {
                LeafType.Int64 => new Int64Array(Slice(layout.Values[l], length * 8L), validity, length, nulls, 0),
                LeafType.Int32 => new Int32Array(Slice(layout.Values[l], length * 4L), validity, length, nulls, 0),
                LeafType.Bool => new BooleanArray(Slice(layout.Values[l], Bitmap(length)), validity, length, nulls, 0),
                _ => new StringArray(length, Slice(layout.Values[l], (length + 1L) * sizeof(int)), Slice(layout.Data[l], layout.DataPosition[l]), validity, nulls)
            };
        }

        private IArrowArray MapArray(Layout layout, int n, RepeatedNode node, Leaf key, Leaf value)
        {
            var r = (int)node;
            var entries = new StructArray(CheckpointSchema.StringMap.KeyValueType, layout.Length[(int)key], new[] { LeafArray(layout, key), LeafArray(layout, value) }, ArrowBuffer.Empty, 0);
            return new MapArray(CheckpointSchema.StringMap, n, Slice(layout.RepeatedOffsets[r], (n + 1L) * sizeof(int)), entries, Slice(layout.RepeatedValidity[r], Bitmap(n)), layout.RepeatedNulls[r]);
        }

        private IArrowArray ListArray(Layout layout, int n, RepeatedNode node, Leaf item)
        {
            var r = (int)node;
            return new ListArray(CheckpointSchema.StringList, n, Slice(layout.RepeatedOffsets[r], (n + 1L) * sizeof(int)), LeafArray(layout, item), Slice(layout.RepeatedValidity[r], Bitmap(n)), layout.RepeatedNulls[r]);
        }

        private StructArray StructArray(Layout layout, int n, StructNode node, Apache.Arrow.Types.StructType type, params IArrowArray[] children)
        {
            var s = (int)node;
            return new StructArray(type, n, children, Slice(layout.StructValidity[s], Bitmap(n)), layout.StructNulls[s]);
        }

        private RecordBatch Assemble(Apache.Arrow.Schema schema, Layout layout, int n)
        {
            IArrowArray L(Leaf leaf) => LeafArray(layout, leaf);
            var txn = StructArray(layout, n, StructNode.Txn, CheckpointSchema.Txn, L(Leaf.TxnAppId), L(Leaf.TxnVersion), L(Leaf.TxnLastUpdated));
            var add = StructArray(layout, n, StructNode.Add, CheckpointSchema.Add,
                L(Leaf.AddPath),
                MapArray(layout, n, RepeatedNode.AddPartitionValues, Leaf.AddPartitionKey, Leaf.AddPartitionValue),
                L(Leaf.AddSize),
                L(Leaf.AddModificationTime),
                L(Leaf.AddDataChange),
                L(Leaf.AddStats),
                MapArray(layout, n, RepeatedNode.AddTags, Leaf.AddTagKey, Leaf.AddTagValue),
                StructArray(layout, n, StructNode.AddDv, CheckpointSchema.DeletionVector, L(Leaf.AddDvStorageType), L(Leaf.AddDvPath),
                    L(Leaf.AddDvOffset), L(Leaf.AddDvSize), L(Leaf.AddDvCardinality)),
                L(Leaf.AddBaseRowId),
                L(Leaf.AddDefaultRowCommitVersion),
                L(Leaf.AddClusteringProvider));
            var remove = StructArray(layout, n, StructNode.Remove, CheckpointSchema.Remove,
                L(Leaf.RemovePath),
                L(Leaf.RemoveDeletionTimestamp),
                L(Leaf.RemoveDataChange),
                L(Leaf.RemoveExtendedFileMetadata),
                MapArray(layout, n, RepeatedNode.RemovePartitionValues, Leaf.RemovePartitionKey, Leaf.RemovePartitionValue),
                L(Leaf.RemoveSize),
                StructArray(layout, n, StructNode.RemoveDv, CheckpointSchema.DeletionVector, L(Leaf.RemoveDvStorageType), L(Leaf.RemoveDvPath),
                    L(Leaf.RemoveDvOffset), L(Leaf.RemoveDvSize), L(Leaf.RemoveDvCardinality)),
                L(Leaf.RemoveBaseRowId),
                L(Leaf.RemoveDefaultRowCommitVersion));
            var metaData = StructArray(layout, n, StructNode.MetaData, CheckpointSchema.MetaData,
                L(Leaf.MetaId),
                L(Leaf.MetaName),
                L(Leaf.MetaDescription),
                StructArray(layout, n, StructNode.MetaFormat, CheckpointSchema.Format, L(Leaf.MetaFormatProvider),
                    MapArray(layout, n, RepeatedNode.MetaFormatOptions, Leaf.MetaFormatOptionKey, Leaf.MetaFormatOptionValue)),
                L(Leaf.MetaSchemaString),
                ListArray(layout, n, RepeatedNode.MetaPartitionColumns, Leaf.MetaPartitionColumn),
                L(Leaf.MetaCreatedTime),
                MapArray(layout, n, RepeatedNode.MetaConfiguration, Leaf.MetaConfigurationKey, Leaf.MetaConfigurationValue));
            var protocol = StructArray(layout, n, StructNode.Protocol, CheckpointSchema.Protocol,
                L(Leaf.ProtocolMinReader),
                L(Leaf.ProtocolMinWriter),
                ListArray(layout, n, RepeatedNode.ProtocolReaderFeatures, Leaf.ProtocolReaderFeature),
                ListArray(layout, n, RepeatedNode.ProtocolWriterFeatures, Leaf.ProtocolWriterFeature));
            var domain = StructArray(layout, n, StructNode.Domain, CheckpointSchema.DomainMetadata,
                L(Leaf.DomainName), L(Leaf.DomainConfiguration), L(Leaf.DomainRemoved));
            return new RecordBatch(schema, new IArrowArray[] { txn, add, remove, metaData, protocol, domain }, n);
        }

        /// <summary>
        /// Where each buffer starts in the arena, and the fill cursors.
        /// </summary>
        private sealed class Layout
        {
            public readonly int[] StructValidity = new int[Structs];
            public readonly int[] StructCount = new int[Structs];
            public readonly int[] StructNulls = new int[Structs];
            public readonly int[] RepeatedOffsets = new int[CheckpointSchema.RepeatedCount];
            public readonly int[] RepeatedValidity = new int[CheckpointSchema.RepeatedCount];
            public readonly int[] RepeatedCount = new int[CheckpointSchema.RepeatedCount];
            public readonly int[] RepeatedNulls = new int[CheckpointSchema.RepeatedCount];
            public readonly int[] Length = new int[CheckpointSchema.LeafCount];
            public readonly int[] Validity = new int[CheckpointSchema.LeafCount];
            public readonly int[] Values = new int[CheckpointSchema.LeafCount];
            public readonly int[] Data = new int[CheckpointSchema.LeafCount];
            public readonly int[] Count = new int[CheckpointSchema.LeafCount];
            public readonly int[] Nulls = new int[CheckpointSchema.LeafCount];
            public readonly int[] DataPosition = new int[CheckpointSchema.LeafCount];

            public void Clear()
            {
                foreach (var counter in new[] { StructCount, StructNulls, RepeatedCount, RepeatedNulls, Count, Nulls, DataPosition })
                {
                    System.Array.Clear(counter);
                }
            }
        }

        /// <summary>
        /// Plain-encoded value bytes and levels of one row; a byte array costs 4 bytes plus its length, a boolean one byte.
        /// </summary>
        private readonly struct SizingVisitor : IRecordVisitor
        {
            private readonly long[] _valueBytes;
            private readonly long[] _levels;
            private readonly long[] _dataBytes;
            private readonly long[] _entries;

            public SizingVisitor(long[] valueBytes, long[] levels, long[] dataBytes, long[] entries)
            {
                _valueBytes = valueBytes;
                _levels = levels;
                _dataBytes = dataBytes;
                _entries = entries;
            }

            public void Struct(StructNode node, bool present)
            {
            }

            public void String(Leaf leaf, bool present, ReadOnlySpan<byte> utf8)
            {
                if (CheckpointSchema.RepeatedOf(leaf) < 0)
                {
                    _levels[(int)leaf]++;
                }
                if (present)
                {
                    _valueBytes[(int)leaf] += 4 + utf8.Length;
                    _dataBytes[(int)leaf] += utf8.Length;
                }
            }

            public void Int64(Leaf leaf, long? value)
            {
                _levels[(int)leaf]++;
                if (value != null)
                {
                    _valueBytes[(int)leaf] += CheckpointSchema.Type(leaf) == LeafType.Int32 ? 4 : 8;
                }
            }

            public void Bool(Leaf leaf, bool? value)
            {
                _levels[(int)leaf]++;
                if (value != null)
                {
                    _valueBytes[(int)leaf] += 1;
                }
            }

            public void Repeated(RepeatedNode node, int? count)
            {
                var entries = count ?? 0;
                _entries[(int)node] += entries;
                for (int l = 0; l < CheckpointSchema.LeafCount; l++)
                {
                    if (CheckpointSchema.RepeatedOf((Leaf)l) == (int)node)
                    {
                        _levels[l] += Math.Max(1, entries);
                    }
                }
            }
        }

        /// <summary>
        /// Writes one row's values into the arena at each buffer's cursor.
        /// </summary>
        private readonly struct FillVisitor : IRecordVisitor
        {
            private readonly byte[] _arena;
            private readonly Layout _layout;

            public FillVisitor(byte[] arena, Layout layout)
            {
                _arena = arena;
                _layout = layout;
            }

            private void SetBit(int at, int index)
            {
                _arena[at + index / 8] |= (byte)(1 << (index % 8));
            }

            public void Struct(StructNode node, bool present)
            {
                var s = (int)node;
                var index = _layout.StructCount[s]++;
                if (present)
                {
                    SetBit(_layout.StructValidity[s], index);
                }
                else
                {
                    _layout.StructNulls[s]++;
                }
            }

            public void String(Leaf leaf, bool present, ReadOnlySpan<byte> utf8)
            {
                var l = (int)leaf;
                var index = _layout.Count[l]++;
                var offsets = MemoryMarshal.Cast<byte, int>(_arena.AsSpan(_layout.Values[l], (_layout.Length[l] + 1) * sizeof(int)));
                if (present)
                {
                    utf8.CopyTo(_arena.AsSpan(_layout.Data[l] + _layout.DataPosition[l]));
                    _layout.DataPosition[l] += utf8.Length;
                    SetBit(_layout.Validity[l], index);
                }
                else
                {
                    _layout.Nulls[l]++;
                }
                offsets[index + 1] = _layout.DataPosition[l];
            }

            public void Int64(Leaf leaf, long? value)
            {
                var l = (int)leaf;
                var index = _layout.Count[l]++;
                if (value == null)
                {
                    _layout.Nulls[l]++;
                    return;
                }
                SetBit(_layout.Validity[l], index);
                if (CheckpointSchema.Type(leaf) == LeafType.Int32)
                {
                    if (value.Value < int.MinValue || value.Value > int.MaxValue)
                    {
                        throw new CheckpointDeclinedException(MaintenanceReason.ValueOutOfRange, DeclineDependency.None, $"{leaf} holds {value.Value}, outside the int range");
                    }
                    MemoryMarshal.Write(_arena.AsSpan(_layout.Values[l] + index * 4), (int)value.Value);
                }
                else
                {
                    MemoryMarshal.Write(_arena.AsSpan(_layout.Values[l] + index * 8), value.Value);
                }
            }

            public void Bool(Leaf leaf, bool? value)
            {
                var l = (int)leaf;
                var index = _layout.Count[l]++;
                if (value == null)
                {
                    _layout.Nulls[l]++;
                    return;
                }
                SetBit(_layout.Validity[l], index);
                if (value.Value)
                {
                    SetBit(_layout.Values[l], index);
                }
            }

            public void Repeated(RepeatedNode node, int? count)
            {
                var r = (int)node;
                var index = _layout.RepeatedCount[r]++;
                var offsets = MemoryMarshal.Cast<byte, int>(_arena.AsSpan(_layout.RepeatedOffsets[r]));
                offsets[index + 1] = offsets[index] + (count ?? 0);
                if (count != null)
                {
                    SetBit(_layout.RepeatedValidity[r], index);
                }
                else
                {
                    _layout.RepeatedNulls[r]++;
                }
            }
        }
    }
}
