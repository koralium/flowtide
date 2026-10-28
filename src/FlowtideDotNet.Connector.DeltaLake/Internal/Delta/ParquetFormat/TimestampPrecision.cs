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
            var definitions = ArrayPool<short>.Shared.Rent(BatchSize);
            var repetitions = ArrayPool<short>.Shared.Rent(BatchSize);
            Int96[]? int96s = null;
            long[]? longs = null;
            try
            {
                foreach (var leaf in leaves)
                {
                    using var column = group.Column(leaf.ColumnIndex);
                    using (var chunk = group.MetaData.GetColumnChunkMetaData(leaf.ColumnIndex))
                    {
                        // Every row has a level, so more rows is corrupt
                        if (chunk.NumValues < rows)
                        {
                            throw DecodeFailure(path, column, $"has {chunk.NumValues} levels for {rows} rows");
                        }
                    }
                    if (leaf.Int96)
                    {
                        int96s ??= ArrayPool<Int96>.Shared.Rent(BatchSize);
                    }
                    else
                    {
                        longs ??= ArrayPool<long>.Shared.Rent(BatchSize);
                    }
                    long root = -1;
                    var first = true;
                    while (true)
                    {
                        long levels;
                        long valuesRead;
                        try
                        {
                            if (!column.HasNext)
                            {
                                break;
                            }
                            levels = leaf.Int96
                                ? ((ColumnReader<Int96>)column).ReadBatch(BatchSize, definitions.AsSpan(0, BatchSize), repetitions.AsSpan(0, BatchSize), int96s.AsSpan(0, BatchSize), out valuesRead)
                                : ((ColumnReader<long>)column).ReadBatch(BatchSize, definitions.AsSpan(0, BatchSize), repetitions.AsSpan(0, BatchSize), longs.AsSpan(0, BatchSize), out valuesRead);
                        }
                        // Column read the chunk, so this is corrupt data, unless memory ran out
                        catch (ParquetException e) when (!e.Message.Contains("Out of memory", StringComparison.Ordinal))
                        {
                            throw DecodeFailure(path, column, $"fails in the native reader: {e.Message}", e);
                        }
                        LevelsHookForTests?.Invoke(path, leaf, definitions.AsSpan(0, BatchSize), repetitions.AsSpan(0, BatchSize), ref levels, ref valuesRead);
                        if (levels <= 0 || levels > BatchSize || valuesRead < 0 || valuesRead > levels)
                        {
                            throw DecodeFailure(path, column, $"returned {levels} levels and {valuesRead} values");
                        }
                        var defined = 0L;
                        for (int i = 0; i < levels; i++)
                        {
                            if (Definition(leaf, definitions, i) == leaf.MaxDefinitionLevel)
                            {
                                defined++;
                            }
                        }
                        if (defined != valuesRead)
                        {
                            throw DecodeFailure(path, column, $"returned {valuesRead} values for {defined} defined levels");
                        }
                        var value = 0;
                        for (int i = 0; i < levels; i++)
                        {
                            var definition = Definition(leaf, definitions, i);
                            var repetition = leaf.MaxRepetitionLevel == 0 ? (short)0 : repetitions[i];
                            if (definition < 0 || definition > leaf.MaxDefinitionLevel)
                            {
                                throw DecodeFailure(path, column, $"has definition level {definition} above its maximum {leaf.MaxDefinitionLevel}");
                            }
                            if (repetition < 0 || repetition > leaf.MaxRepetitionLevel || (first && repetition != 0))
                            {
                                throw DecodeFailure(path, column, $"has repetition level {repetition} out of place");
                            }
                            first = false;
                            // A new row starts at every repetition level 0
                            if (repetition == 0 && ++root >= rows)
                            {
                                throw DecodeFailure(path, column, $"holds more than its {rows} rows");
                            }
                            if (definition != leaf.MaxDefinitionLevel)
                            {
                                continue;
                            }
                            if (value >= valuesRead)
                            {
                                throw DecodeFailure(path, column, $"returned only {valuesRead} values");
                            }
                            if (leaf.Int96 ? Int96Flagged(int96s![value]) : longs![value] % 1000 != 0)
                            {
                                flags ??= new ulong[(rows + 63) / 64];
                                flags[root >> 6] |= 1UL << (int)(root & 63);
                            }
                            value++;
                        }
                    }
                    if (root + 1 != rows)
                    {
                        throw DecodeFailure(path, column, $"ended after {root + 1} of its {rows} rows");
                    }
                }
            }
            finally
            {
                ArrayPool<short>.Shared.Return(definitions);
                ArrayPool<short>.Shared.Return(repetitions);
                if (int96s != null)
                {
                    ArrayPool<Int96>.Shared.Return(int96s);
                }
                if (longs != null)
                {
                    ArrayPool<long>.Shared.Return(longs);
                }
            }
            return flags;
        }

        // A required leaf writes no definition levels
        private static short Definition(PrecisionLeaf leaf, short[] definitions, int index)
        {
            return leaf.MaxDefinitionLevel == 0 ? (short)0 : definitions[index];
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
    }
}
