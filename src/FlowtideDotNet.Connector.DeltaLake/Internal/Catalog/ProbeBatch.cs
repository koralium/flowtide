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
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Stats;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Catalog
{
    // The pruning columns of a batch's distinct deleted rows, read from the stored batch
    internal sealed class ProbeBatch
    {
        private readonly PruningLayout _layout;
        private readonly ProbeColumn[] _columns;

        private ProbeBatch(PruningLayout layout, ProbeColumn[] columns, int count)
        {
            _layout = layout;
            _columns = columns;
            Count = count;
        }

        public int Count { get; }

        public PruningLayout Layout => _layout;

        public ProbeColumn Column(int column) => _columns[column];

        // Numeric columns keep a sorted 8-byte key per entry
        public static int KeyBytes(PruningLayout layout)
        {
            return 8 * layout.Columns.Count(x => x.Type is not (PruningType.Decimal or PruningType.String or PruningType.Binary));
        }

        // One probe per entry, read from the entry's stored row
        public static ProbeBatch Build(PruningLayout layout, RecordBatch stored, ReadOnlySpan<int> entryRows)
        {
            var rows = entryRows.ToArray();
            var columns = new ProbeColumn[layout.Columns.Count];
            for (int c = 0; c < columns.Length; c++)
            {
                var column = layout.Columns[c];
                columns[c] = new ProbeColumn(column.Type, stored.Column(column.FieldIndex), rows);
            }
            return new ProbeBatch(layout, columns, rows.Length);
        }

        // Whether the entry can be in the row's file
        public bool Matches(ReadOnlySpan<byte> row, int entry)
        {
            if ((row[0] & PruningLayout.HasStatistics) == 0)
            {
                return true;
            }
            var columns = _layout.Columns;
            for (int c = 0; c < columns.Count; c++)
            {
                var column = columns[c];
                if (!_columns[c].Admits(row.Slice(column.Offset, PruningCell.CellSize(column.Type)), entry))
                {
                    return false;
                }
            }
            return true;
        }
    }

    // One pruning column of the entries, ordinary ones by value, nulls and specials apart
    internal sealed class ProbeColumn
    {
        // How the stored array is read
        private enum Kind : byte
        {
            Int8,
            Int16,
            Int32,
            Int64,
            Float32,
            Double,
            Decimal,
            Date,
            Timestamp,
            Bool,
            Bytes
        }

        private readonly PruningType _type;
        private readonly Kind _kind;
        private readonly IArrowArray _array;
        private readonly int[] _rows;
        // The sorted entries' values, so a search reads one array
        private long[]? _longKeys;
        private double[]? _doubleKeys;

        public ProbeColumn(PruningType type, IArrowArray array, int[] rows)
        {
            _type = type;
            _array = array;
            _rows = rows;
            _kind = KindOf(type, array);
            var ordinary = new List<int>(rows.Length);
            var nulls = new List<int>();
            var special = new List<int>();
            for (int entry = 0; entry < rows.Length; entry++)
            {
                if (array.IsNull(rows[entry]))
                {
                    nulls.Add(entry);
                }
                else if (IsSpecial(entry))
                {
                    special.Add(entry);
                }
                else
                {
                    ordinary.Add(entry);
                }
            }
            Sorted = ordinary.ToArray();
            Nulls = nulls.ToArray();
            Special = special.ToArray();
            Sort(Sorted);
        }

        // Ordinary entries in value order
        public int[] Sorted { get; }

        public int[] Nulls { get; }

        // NaN and values no kernel accepts, they pass every cell
        public int[] Special { get; }

        private static Kind KindOf(PruningType type, IArrowArray array)
        {
            return (type, array) switch
            {
                (PruningType.Int64, Int8Array) => Kind.Int8,
                (PruningType.Int64, Int16Array) => Kind.Int16,
                (PruningType.Int64, Int32Array) => Kind.Int32,
                (PruningType.Int64, Int64Array) => Kind.Int64,
                (PruningType.Float32, FloatArray) => Kind.Float32,
                (PruningType.Double, DoubleArray) => Kind.Double,
                (PruningType.Decimal, Decimal128Array) => Kind.Decimal,
                (PruningType.Date, Date32Array) => Kind.Date,
                (PruningType.Timestamp, TimestampArray timestamps) when ((Apache.Arrow.Types.TimestampType)timestamps.Data.DataType).Unit == Apache.Arrow.Types.TimeUnit.Millisecond => Kind.Timestamp,
                (PruningType.Bool, BooleanArray) => Kind.Bool,
                (PruningType.String, StringArray) or (PruningType.Binary, BinaryArray) => Kind.Bytes,
                _ => throw new InvalidOperationException($"A {type} pruning column cannot read a stored {array.Data.DataType.Name} column")
            };
        }

        private bool IsSpecial(int entry)
        {
            return _kind switch
            {
                Kind.Float32 or Kind.Double => double.IsNaN(Double(entry)),
                Kind.Decimal => !TryDecimal(entry, out _),
                _ => false
            };
        }

        // Ticks of a date, milliseconds of a timestamp, 0 or 1 for a bool
        private long Long(int entry)
        {
            var row = _rows[entry];
            return _kind switch
            {
                Kind.Int8 => ((Int8Array)_array).Values[row],
                Kind.Int16 => ((Int16Array)_array).Values[row],
                Kind.Int32 => ((Int32Array)_array).Values[row],
                Kind.Int64 => ((Int64Array)_array).Values[row],
                Kind.Date => DateTime.UnixEpoch.Ticks + ((Date32Array)_array).Values[row] * TimeSpan.TicksPerDay,
                Kind.Timestamp => ((TimestampArray)_array).Values[row],
                _ => ((BooleanArray)_array).GetValue(row) == true ? 1 : 0
            };
        }

        private double Double(int entry)
        {
            var row = _rows[entry];
            return _kind == Kind.Float32 ? ((FloatArray)_array).Values[row] : ((DoubleArray)_array).Values[row];
        }

        // False above System.Decimal's range
        private bool TryDecimal(int entry, out decimal value)
        {
            var decimals = (Decimal128Array)_array;
            var row = _rows[entry];
            var bytes = decimals.ValueBuffer.Span.Slice((decimals.Offset + row) * 16, 16);
            var raw = System.Buffers.Binary.BinaryPrimitives.ReadInt128LittleEndian(bytes);
            var scale = ((Apache.Arrow.Types.Decimal128Type)decimals.Data.DataType).Scale;
            var negative = raw < 0;
            var magnitude = raw == Int128.MinValue ? UInt128.MaxValue : negative ? (UInt128)(-raw) : (UInt128)raw;
            // A value scaled past 96 bits may still be a decimal
            while ((magnitude >> 96 != 0 || scale > 28) && scale > 0 && magnitude % 10 == 0)
            {
                magnitude /= 10;
                scale--;
            }
            if (magnitude >> 96 != 0 || scale < 0 || scale > 28)
            {
                value = 0;
                return false;
            }
            value = new decimal((int)(uint)magnitude, (int)(uint)(magnitude >> 32), (int)(uint)(magnitude >> 64), negative, (byte)scale);
            return true;
        }

        private ReadOnlySpan<byte> Bytes(int entry)
        {
            return ((BinaryArray)_array).GetBytes(_rows[entry]);
        }

        private static ReadOnlySpan<byte> Prefix(ReadOnlySpan<byte> bytes)
        {
            return bytes.Slice(0, Math.Min(bytes.Length, PruningCell.PrefixLength));
        }

        // Whether the cell may hold the entry's value
        public bool Admits(ReadOnlySpan<byte> cell, int entry)
        {
            if (_array.IsNull(_rows[entry]))
            {
                return PruningCell.MayHoldNull(cell);
            }
            if (IsSpecial(entry))
            {
                return true;
            }
            return MinAdmits(cell, entry) && MaxAdmits(cell, entry);
        }

        // Ordinary entries only
        private bool MinAdmits(ReadOnlySpan<byte> cell, int entry)
        {
            switch (_kind)
            {
                case Kind.Float32:
                case Kind.Double:
                    return PruningCell.MinAdmits(cell, _type, Double(entry));
                case Kind.Decimal:
                    TryDecimal(entry, out var value);
                    return PruningCell.MinAdmits(cell, value);
                case Kind.Bool:
                    return PruningCell.MinAdmits(cell, Long(entry) != 0);
                case Kind.Bytes:
                    var bytes = Bytes(entry);
                    return PruningCell.MinAdmits(cell, Prefix(bytes), bytes.Length);
                default:
                    return PruningCell.MinAdmits(cell, _type, Long(entry));
            }
        }

        private bool MaxAdmits(ReadOnlySpan<byte> cell, int entry)
        {
            switch (_kind)
            {
                case Kind.Float32:
                case Kind.Double:
                    return PruningCell.MaxAdmits(cell, _type, Double(entry));
                case Kind.Decimal:
                    TryDecimal(entry, out var value);
                    return PruningCell.MaxAdmits(cell, value);
                case Kind.Bool:
                    return PruningCell.MaxAdmits(cell, Long(entry) != 0);
                case Kind.Bytes:
                    var bytes = Bytes(entry);
                    return PruningCell.MaxAdmits(cell, Prefix(bytes), bytes.Length);
                default:
                    return PruningCell.MaxAdmits(cell, _type, Long(entry));
            }
        }

        // The first sorted position the cell's min admits
        public int Lower(ReadOnlySpan<byte> cell)
        {
            // A cell below every entry admits them all
            if (Sorted.Length == 0 || MinAdmitsAt(cell, 0))
            {
                return 0;
            }
            int low = 1;
            int high = Sorted.Length;
            while (low < high)
            {
                var mid = (low + high) >>> 1;
                if (MinAdmitsAt(cell, mid))
                {
                    high = mid;
                }
                else
                {
                    low = mid + 1;
                }
            }
            return low;
        }

        // The first sorted position from lower past what the cell's max admits
        public int Upper(ReadOnlySpan<byte> cell, int lower)
        {
            // A cell above every entry admits them all
            if (lower == Sorted.Length || MaxAdmitsAt(cell, Sorted.Length - 1))
            {
                return Sorted.Length;
            }
            int low = lower;
            int high = Sorted.Length - 1;
            while (low < high)
            {
                var mid = (low + high) >>> 1;
                if (MaxAdmitsAt(cell, mid))
                {
                    low = mid + 1;
                }
                else
                {
                    high = mid;
                }
            }
            return low;
        }

        private bool MinAdmitsAt(ReadOnlySpan<byte> cell, int position)
        {
            if (_longKeys != null)
            {
                return _kind == Kind.Bool ? PruningCell.MinAdmits(cell, _longKeys[position] != 0) : PruningCell.MinAdmits(cell, _type, _longKeys[position]);
            }
            if (_doubleKeys != null)
            {
                return PruningCell.MinAdmits(cell, _type, _doubleKeys[position]);
            }
            return MinAdmits(cell, Sorted[position]);
        }

        private bool MaxAdmitsAt(ReadOnlySpan<byte> cell, int position)
        {
            if (_longKeys != null)
            {
                return _kind == Kind.Bool ? PruningCell.MaxAdmits(cell, _longKeys[position] != 0) : PruningCell.MaxAdmits(cell, _type, _longKeys[position]);
            }
            if (_doubleKeys != null)
            {
                return PruningCell.MaxAdmits(cell, _type, _doubleKeys[position]);
            }
            return MaxAdmits(cell, Sorted[position]);
        }

        // Numbers keep their keys, sorted entries skip the sort
        private void Sort(int[] entries)
        {
            var inOrder = InOrder(entries);
            switch (_kind)
            {
                case Kind.Float32:
                case Kind.Double:
                    var doubles = new double[entries.Length];
                    for (int i = 0; i < entries.Length; i++)
                    {
                        // One key for -0 and +0
                        doubles[i] = Double(entries[i]) + 0.0;
                    }
                    if (!inOrder)
                    {
                        System.Array.Sort(doubles, entries);
                    }
                    _doubleKeys = doubles;
                    break;
                case Kind.Decimal:
                    if (!inOrder)
                    {
                        var decimals = new decimal[entries.Length];
                        for (int i = 0; i < entries.Length; i++)
                        {
                            TryDecimal(entries[i], out decimals[i]);
                        }
                        System.Array.Sort(decimals, entries);
                    }
                    break;
                case Kind.Bytes:
                    if (!inOrder)
                    {
                        System.Array.Sort(entries, Comparer<int>.Create((left, right) => Compare(left, right)));
                    }
                    break;
                default:
                    var longs = new long[entries.Length];
                    for (int i = 0; i < entries.Length; i++)
                    {
                        longs[i] = Long(entries[i]);
                    }
                    if (!inOrder)
                    {
                        System.Array.Sort(longs, entries);
                    }
                    _longKeys = longs;
                    break;
            }
        }

        private bool InOrder(int[] entries)
        {
            for (int i = 1; i < entries.Length; i++)
            {
                if (Compare(entries[i - 1], entries[i]) > 0)
                {
                    return false;
                }
            }
            return true;
        }

        // Numbers ascend, bytes by prefix, then longer probes
        private int Compare(int left, int right)
        {
            switch (_kind)
            {
                case Kind.Float32:
                case Kind.Double:
                    var leftDouble = Double(left);
                    var rightDouble = Double(right);
                    return leftDouble < rightDouble ? -1 : leftDouble > rightDouble ? 1 : 0;
                case Kind.Decimal:
                    TryDecimal(left, out var leftDecimal);
                    TryDecimal(right, out var rightDecimal);
                    return leftDecimal.CompareTo(rightDecimal);
                case Kind.Bytes:
                    var leftBytes = Bytes(left);
                    var rightBytes = Bytes(right);
                    var order = Prefix(leftBytes).SequenceCompareTo(Prefix(rightBytes));
                    if (order != 0)
                    {
                        return order;
                    }
                    return (leftBytes.Length > PruningCell.PrefixLength).CompareTo(rightBytes.Length > PruningCell.PrefixLength);
                default:
                    return Long(left).CompareTo(Long(right));
            }
        }
    }
}
