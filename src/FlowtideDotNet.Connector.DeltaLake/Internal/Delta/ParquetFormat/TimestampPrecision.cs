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

using ParquetSharp;
using System.Buffers;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat
{
    // A timestamp leaf finer than a microsecond, INT96 or INT64 nanos
    internal readonly record struct PrecisionLeaf(int ColumnIndex, bool Int96, short MaxDefinitionLevel, short MaxRepetitionLevel);

    // Finds rows holding timestamps a microsecond copy would change
    internal static class TimestampPrecision
    {
        private const int BatchSize = 4096;
        private const long NanosPerDay = 86_400_000_000_000;
        private const long MicrosPerDay = 86_400_000_000;
        private const long UnixEpochJulianDay = 2_440_588;

        // Edits each batch as read; a test acts only on its own path
        internal static LevelsHook? LevelsHookForTests { get; set; }

        internal delegate void LevelsHook(string path, PrecisionLeaf leaf, Span<short> definitions, Span<short> repetitions, ref long levels, ref long valuesRead);

        // A bitmap of flagged rows, null when no row is flagged
        public static ulong[]? FlagRowGroup(ParquetFileReader reader, string path, int rowGroup, IReadOnlyList<PrecisionLeaf> leaves)
        {
            using var group = reader.RowGroup(rowGroup);
            var rows = group.MetaData.NumRows;
            ulong[]? flags = null;
            foreach (var leaf in leaves)
            {
                using var cursor = new LeafCursor(group, path, leaf);
                for (long row = 0; row < rows; row++)
                {
                    if (cursor.Flagged(row))
                    {
                        flags ??= new ulong[(rows + 63) / 64];
                        flags[row >> 6] |= 1UL << (int)(row & 63);
                    }
                }
                cursor.Finish();
            }
            return flags;
        }

        // Not a whole microsecond, a malformed time of day, or outside a long of microseconds
        private static bool Int96Flagged(Int96 value)
        {
            var nanos = (uint)value.A | ((ulong)(uint)value.B << 32);
            if (nanos >= NanosPerDay || nanos % 1000 != 0)
            {
                return true;
            }
            var micros = ((Int128)(uint)value.C - UnixEpochJulianDay) * MicrosPerDay + (Int128)(nanos / 1000);
            return micros < long.MinValue || micros > long.MaxValue;
        }

        private static InvalidDataException DecodeFailure(string path, ColumnReader column, string detail, Exception? inner = null)
        {
            using var leafPath = column.ColumnDescriptor.Path;
            return new InvalidDataException($"Delta file {path} cannot be decoded: timestamp leaf {leafPath.ToDotString()} {detail}", inner);
        }

        // The raw leaves of one row group, read only as far as the rows asked
        internal sealed class RowGroupCursor : IDisposable
        {
            private readonly RowGroupReader _group;
            private readonly List<LeafCursor> _leaves = new List<LeafCursor>();

            public RowGroupCursor(ParquetFileReader reader, string path, int rowGroup, IReadOnlyList<PrecisionLeaf> leaves)
            {
                _group = reader.RowGroup(rowGroup);
                try
                {
                    foreach (var leaf in leaves)
                    {
                        _leaves.Add(new LeafCursor(_group, path, leaf));
                    }
                }
                catch
                {
                    Dispose();
                    throw;
                }
            }

            public long LevelsRead
            {
                get
                {
                    long levels = 0;
                    foreach (var leaf in _leaves)
                    {
                        levels += leaf.LevelsRead;
                    }
                    return levels;
                }
            }

            // Rows only move forward
            public bool Flagged(long row)
            {
                var flagged = false;
                foreach (var leaf in _leaves)
                {
                    flagged |= leaf.Flagged(row);
                }
                return flagged;
            }

            // Reads every leaf to its end, which checks its rows
            public void Finish()
            {
                foreach (var leaf in _leaves)
                {
                    leaf.Finish();
                }
            }

            public void Dispose()
            {
                foreach (var leaf in _leaves)
                {
                    leaf.Dispose();
                }
                _leaves.Clear();
                _group.Dispose();
            }
        }

        // One leaf's raw levels, consumed row by row with one level of lookahead
        internal sealed class LeafCursor : IDisposable
        {
            private readonly string _path;
            private readonly PrecisionLeaf _leaf;
            private readonly ColumnReader _column;
            private readonly long _rows;
            private readonly short[] _definitions;
            private readonly short[] _repetitions;
            private readonly Int96[]? _int96s;
            private readonly long[]? _longs;
            private int _levels;
            private int _next;
            private long _valuesRead;
            private int _value;
            private long _root = -1;
            private bool _first = true;

            public LeafCursor(RowGroupReader group, string path, PrecisionLeaf leaf)
            {
                _path = path;
                _leaf = leaf;
                _rows = group.MetaData.NumRows;
                _column = group.Column(leaf.ColumnIndex);
                try
                {
                    using (var chunk = group.MetaData.GetColumnChunkMetaData(leaf.ColumnIndex))
                    {
                        // Every row has a level, so more rows is corrupt
                        if (chunk.NumValues < _rows)
                        {
                            throw DecodeFailure(path, _column, $"has {chunk.NumValues} levels for {_rows} rows");
                        }
                    }
                }
                catch
                {
                    _column.Dispose();
                    throw;
                }
                _definitions = ArrayPool<short>.Shared.Rent(BatchSize);
                _repetitions = ArrayPool<short>.Shared.Rent(BatchSize);
                if (leaf.Int96)
                {
                    _int96s = ArrayPool<Int96>.Shared.Rent(BatchSize);
                }
                else
                {
                    _longs = ArrayPool<long>.Shared.Rent(BatchSize);
                }
            }

            public long LevelsRead { get; private set; }

            // True when a value of the row is inexact, rows only move forward
            public bool Flagged(long row)
            {
                var flagged = false;
                while (_next < _levels || ReadBatch())
                {
                    var definition = _leaf.MaxDefinitionLevel == 0 ? (short)0 : _definitions[_next];
                    var repetition = _leaf.MaxRepetitionLevel == 0 ? (short)0 : _repetitions[_next];
                    if (definition < 0 || definition > _leaf.MaxDefinitionLevel)
                    {
                        throw DecodeFailure(_path, _column, $"has definition level {definition} above its maximum {_leaf.MaxDefinitionLevel}");
                    }
                    if (repetition < 0 || repetition > _leaf.MaxRepetitionLevel || (_first && repetition != 0))
                    {
                        throw DecodeFailure(_path, _column, $"has repetition level {repetition} out of place");
                    }
                    // A new row starts at every repetition level 0
                    var root = repetition == 0 ? _root + 1 : _root;
                    if (root >= _rows)
                    {
                        throw DecodeFailure(_path, _column, $"holds more than its {_rows} rows");
                    }
                    if (root > row)
                    {
                        return flagged;
                    }
                    _first = false;
                    _root = root;
                    _next++;
                    if (definition != _leaf.MaxDefinitionLevel)
                    {
                        continue;
                    }
                    if (_value >= _valuesRead)
                    {
                        throw DecodeFailure(_path, _column, $"returned only {_valuesRead} values");
                    }
                    if (root == row && (_leaf.Int96 ? Int96Flagged(_int96s![_value]) : _longs![_value] % 1000 != 0))
                    {
                        flagged = true;
                    }
                    _value++;
                }
                return flagged;
            }

            // Reads to the end, which checks every row was there
            public void Finish()
            {
                Flagged(long.MaxValue);
            }

            private bool ReadBatch()
            {
                long levels;
                long valuesRead;
                try
                {
                    if (!_column.HasNext)
                    {
                        // Every row must be there, the end is checked when reached
                        if (_root + 1 != _rows)
                        {
                            throw DecodeFailure(_path, _column, $"ended after {_root + 1} of its {_rows} rows");
                        }
                        return false;
                    }
                    levels = _leaf.Int96
                        ? ((ColumnReader<Int96>)_column).ReadBatch(BatchSize, _definitions.AsSpan(0, BatchSize), _repetitions.AsSpan(0, BatchSize), _int96s.AsSpan(0, BatchSize), out valuesRead)
                        : ((ColumnReader<long>)_column).ReadBatch(BatchSize, _definitions.AsSpan(0, BatchSize), _repetitions.AsSpan(0, BatchSize), _longs.AsSpan(0, BatchSize), out valuesRead);
                }
                // Column read the chunk, so this is corrupt data, unless memory ran out
                catch (ParquetException e) when (!e.Message.Contains("Out of memory", StringComparison.Ordinal))
                {
                    throw DecodeFailure(_path, _column, $"fails in the native reader: {e.Message}", e);
                }
                LevelsHookForTests?.Invoke(_path, _leaf, _definitions.AsSpan(0, BatchSize), _repetitions.AsSpan(0, BatchSize), ref levels, ref valuesRead);
                if (levels <= 0 || levels > BatchSize || valuesRead < 0 || valuesRead > levels)
                {
                    throw DecodeFailure(_path, _column, $"returned {levels} levels and {valuesRead} values");
                }
                var defined = 0L;
                for (int i = 0; i < levels; i++)
                {
                    if ((_leaf.MaxDefinitionLevel == 0 ? 0 : _definitions[i]) == _leaf.MaxDefinitionLevel)
                    {
                        defined++;
                    }
                }
                if (defined != valuesRead)
                {
                    throw DecodeFailure(_path, _column, $"returned {valuesRead} values for {defined} defined levels");
                }
                LevelsRead += levels;
                _levels = (int)levels;
                _next = 0;
                _valuesRead = valuesRead;
                _value = 0;
                return true;
            }

            public void Dispose()
            {
                _column.Dispose();
                ArrayPool<short>.Shared.Return(_definitions);
                ArrayPool<short>.Shared.Return(_repetitions);
                if (_int96s != null)
                {
                    ArrayPool<Int96>.Shared.Return(_int96s);
                }
                if (_longs != null)
                {
                    ArrayPool<long>.Shared.Return(_longs);
                }
            }
        }
    }
}
