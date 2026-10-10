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

using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Stats;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Catalog
{
    /// <summary>
    /// The pruning column values of the deleted rows of one batch, normalized like the statistics.
    /// A value no kernel accepts passes every file, so capturing never throws.
    /// </summary>
    internal sealed class ProbeBatch
    {
        private const byte Value = 0;
        private const byte Null = 1;
        private const byte Unsupported = 2;

        // One column's values, only the array its type needs is grown
        private sealed class ColumnValues
        {
            public byte[] Status = Array.Empty<byte>();
            public long[] Longs = Array.Empty<long>();
            public double[] Doubles = Array.Empty<double>();
            public decimal[] Decimals = Array.Empty<decimal>();
            public byte[] Prefixes = Array.Empty<byte>();
            public int[] Lengths = Array.Empty<int>();
        }

        private PruningLayout _layout;
        private ColumnValues[] _columns = Array.Empty<ColumnValues>();
        private int _count;
        private int _capacity;

        public ProbeBatch(PruningLayout layout)
        {
            _layout = layout;
            CreateColumns();
        }

        public int Count => _count;

        // Only between batches, the captured values follow the layout's columns
        public void SetLayout(PruningLayout layout)
        {
            if (_count != 0)
            {
                throw new InvalidOperationException("The layout of a probe batch cannot change while it holds rows");
            }
            if (ReferenceEquals(layout, _layout))
            {
                return;
            }
            _layout = layout;
            CreateColumns();
        }

        private void CreateColumns()
        {
            _columns = new ColumnValues[_layout.Columns.Count];
            for (int c = 0; c < _columns.Length; c++)
            {
                _columns[c] = new ColumnValues();
            }
            _capacity = 0;
        }

        public void Clear()
        {
            _count = 0;
        }

        public void Add(in ColumnRowReference row)
        {
            EnsureCapacity(_count + 1);
            var columns = _layout.Columns;
            for (int c = 0; c < columns.Count; c++)
            {
                var value = row.referenceBatch.Columns[columns[c].ColumnIndex].GetValueAt(row.RowIndex, default);
                Capture(_columns[c], _count, columns[c].Type, value);
            }
            _count++;
        }

        // For tests, one value per pruning column
        internal void Add(IReadOnlyList<IDataValue> values)
        {
            EnsureCapacity(_count + 1);
            var columns = _layout.Columns;
            for (int c = 0; c < columns.Count; c++)
            {
                Capture(_columns[c], _count, columns[c].Type, values[c]);
            }
            _count++;
        }

        private static void Capture<T>(ColumnValues column, int probe, PruningType type, in T value)
            where T : IDataValue
        {
            if (value.IsNull)
            {
                column.Status[probe] = Null;
                return;
            }
            column.Status[probe] = Value;
            switch (type)
            {
                case PruningType.Int64:
                    if (value.Type == ArrowTypeId.Int64)
                    {
                        column.Longs[probe] = value.AsLong;
                        return;
                    }
                    break;
                case PruningType.Double:
                case PruningType.Float32:
                    if (TryDoubleProbe(value, type == PruningType.Float32, out var doubleValue))
                    {
                        column.Doubles[probe] = doubleValue;
                        return;
                    }
                    break;
                case PruningType.Decimal:
                    if (value.Type == ArrowTypeId.Decimal128)
                    {
                        column.Decimals[probe] = value.AsDecimal;
                        return;
                    }
                    break;
                case PruningType.Date:
                    if (value.Type == ArrowTypeId.Timestamp)
                    {
                        column.Longs[probe] = DateProbe(value);
                        return;
                    }
                    break;
                case PruningType.Timestamp:
                    if (value.Type == ArrowTypeId.Timestamp)
                    {
                        column.Longs[probe] = TimestampProbe(value);
                        return;
                    }
                    break;
                case PruningType.Bool:
                    if (value.Type == ArrowTypeId.Boolean)
                    {
                        column.Longs[probe] = value.AsBool ? 1 : 0;
                        return;
                    }
                    break;
                case PruningType.String:
                    if (value.Type == ArrowTypeId.String)
                    {
                        CapturePrefix(column, probe, value.AsString.Span);
                        return;
                    }
                    break;
                case PruningType.Binary:
                    if (value.Type == ArrowTypeId.Binary)
                    {
                        CapturePrefix(column, probe, value.AsBinary);
                        return;
                    }
                    break;
            }
            column.Status[probe] = Unsupported;
        }

        // Stored double for float and double columns, int64 probes are accepted like the comparer always did
        private static bool TryDoubleProbe<T>(in T value, bool isFloat32, out double probe)
            where T : IDataValue
        {
            if (value.Type == ArrowTypeId.Double)
            {
                probe = value.AsDouble;
            }
            else if (value.Type == ArrowTypeId.Int64)
            {
                probe = value.AsLong;
            }
            else
            {
                probe = 0;
                return false;
            }
            if (isFloat32)
            {
                probe = StoredValue.Float32(probe);
            }
            return true;
        }

        // Ticks of the stored date
        private static long DateProbe<T>(in T value)
            where T : IDataValue
        {
            return StoredValue.Date(value.AsTimestamp.ToDateTimeOffset().DateTime).Ticks;
        }

        // Milliseconds of the stored timestamp
        private static long TimestampProbe<T>(in T value)
            where T : IDataValue
        {
            return StoredValue.TimestampMilliseconds(value.AsTimestamp);
        }

        private static void CapturePrefix(ColumnValues column, int probe, ReadOnlySpan<byte> bytes)
        {
            var length = Math.Min(bytes.Length, PruningCell.PrefixLength);
            bytes.Slice(0, length).CopyTo(column.Prefixes.AsSpan(probe * PruningCell.PrefixLength, length));
            column.Lengths[probe] = bytes.Length;
        }

        /// <summary>
        /// Whether the probe can be in the file the row describes.
        /// </summary>
        public bool Matches(ReadOnlySpan<byte> row, int probe)
        {
            if ((row[0] & PruningLayout.HasStatistics) == 0)
            {
                return true;
            }
            var columns = _layout.Columns;
            for (int c = 0; c < columns.Count; c++)
            {
                var column = columns[c];
                var values = _columns[c];
                var status = values.Status[probe];
                var cell = row.Slice(column.Offset, PruningCell.CellSize(column.Type));
                if (status == Null)
                {
                    if (!PruningCell.MayHoldNull(cell))
                    {
                        return false;
                    }
                    continue;
                }
                if (status == Unsupported)
                {
                    continue;
                }
                var match = column.Type switch
                {
                    PruningType.Int64 or PruningType.Date or PruningType.Timestamp => PruningCell.MatchInt64(cell, column.Type, values.Longs[probe]),
                    PruningType.Double or PruningType.Float32 => PruningCell.MatchDouble(cell, column.Type, values.Doubles[probe]),
                    PruningType.Decimal => PruningCell.MatchDecimal(cell, values.Decimals[probe]),
                    PruningType.Bool => PruningCell.MatchBool(cell, values.Longs[probe] != 0),
                    _ => PruningCell.MatchBytes(cell, values.Prefixes.AsSpan(probe * PruningCell.PrefixLength, Math.Min(values.Lengths[probe], PruningCell.PrefixLength)), values.Lengths[probe])
                };
                if (!match)
                {
                    return false;
                }
            }
            return true;
        }

        private void EnsureCapacity(int rows)
        {
            if (rows <= _capacity)
            {
                return;
            }
            var capacity = Math.Max(rows, Math.Max(16, _capacity * 2));
            var columns = _layout.Columns;
            for (int c = 0; c < columns.Count; c++)
            {
                var values = _columns[c];
                Array.Resize(ref values.Status, capacity);
                switch (columns[c].Type)
                {
                    case PruningType.Int64:
                    case PruningType.Date:
                    case PruningType.Timestamp:
                    case PruningType.Bool:
                        Array.Resize(ref values.Longs, capacity);
                        break;
                    case PruningType.Double:
                    case PruningType.Float32:
                        Array.Resize(ref values.Doubles, capacity);
                        break;
                    case PruningType.Decimal:
                        Array.Resize(ref values.Decimals, capacity);
                        break;
                    default:
                        Array.Resize(ref values.Prefixes, capacity * PruningCell.PrefixLength);
                        Array.Resize(ref values.Lengths, capacity);
                        break;
                }
            }
            _capacity = capacity;
        }
    }
}
