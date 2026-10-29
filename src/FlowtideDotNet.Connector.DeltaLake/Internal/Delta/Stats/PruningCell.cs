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

using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema.Types;
using System.Buffers.Binary;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Stats
{
    internal enum PruningType : byte
    {
        Int64,
        Double,
        Float32,
        Decimal,
        Date,
        Timestamp,
        Bool,
        String,
        Binary
    }

    /// <summary>
    /// The bounds of one column of one file in a fixed size cell: a flag byte, then min and max.
    /// Strings and binaries keep a prefix, a truncated bound can only prune less.
    /// </summary>
    internal static class PruningCell
    {
        public const byte HasMin = 1;
        public const byte HasMax = 2;
        public const byte MayContainNull = 4;
        public const byte MaxComplete = 16;

        public const int PrefixLength = 16;

        // The comparers' epsilons, PruningKernelTests checks that both copies agree
        private const double FloatEpsilon = 1e-8;
        private const decimal DecimalEpsilon = 0.000000001m;

        public static PruningType? TypeOf(SchemaBaseType type)
        {
            return type switch
            {
                ByteType or ShortType or IntegerType or LongType => PruningType.Int64,
                FloatType => PruningType.Float32,
                DoubleType => PruningType.Double,
                DecimalType => PruningType.Decimal,
                DateType => PruningType.Date,
                TimestampType => PruningType.Timestamp,
                BooleanType => PruningType.Bool,
                StringType => PruningType.String,
                BinaryType => PruningType.Binary,
                _ => null
            };
        }

        public static int BoundSize(PruningType type)
        {
            return type switch
            {
                PruningType.Decimal => 16,
                PruningType.Bool => 1,
                PruningType.String or PruningType.Binary => 1 + PrefixLength,
                _ => 8
            };
        }

        public static int CellSize(PruningType type)
        {
            return 1 + 2 * BoundSize(type);
        }

        // No bounds, every value may be in the file
        public static void WriteUnknown(Span<byte> cell)
        {
            cell.Clear();
            cell[0] = MayContainNull;
        }

        private static byte NullFlag(int? nullCount)
        {
            return NullMayMatch(nullCount) ? MayContainNull : (byte)0;
        }

        // A null value can only be in a file whose null count is unknown or above zero
        private static bool NullMayMatch(int? nullCount)
        {
            return !nullCount.HasValue || nullCount.Value > 0;
        }

        public static void WriteInt64(Span<byte> cell, PruningType type, long? min, long? max, int? nullCount)
        {
            cell.Clear();
            cell[0] = (byte)(NullFlag(nullCount) | (min.HasValue ? HasMin : 0) | (max.HasValue ? HasMax : 0));
            BinaryPrimitives.WriteInt64LittleEndian(cell.Slice(1, 8), min ?? 0);
            BinaryPrimitives.WriteInt64LittleEndian(cell.Slice(1 + BoundSize(type), 8), max ?? 0);
        }

        public static void WriteDouble(Span<byte> cell, PruningType type, double? min, double? max, int? nullCount)
        {
            cell.Clear();
            cell[0] = (byte)(NullFlag(nullCount) | (min.HasValue ? HasMin : 0) | (max.HasValue ? HasMax : 0));
            BinaryPrimitives.WriteDoubleLittleEndian(cell.Slice(1, 8), min ?? 0);
            BinaryPrimitives.WriteDoubleLittleEndian(cell.Slice(1 + BoundSize(type), 8), max ?? 0);
        }

        public static void WriteDecimal(Span<byte> cell, decimal? min, decimal? max, int? nullCount)
        {
            cell.Clear();
            cell[0] = (byte)(NullFlag(nullCount) | (min.HasValue ? HasMin : 0) | (max.HasValue ? HasMax : 0));
            WriteDecimal(cell.Slice(1, 16), min ?? 0);
            WriteDecimal(cell.Slice(17, 16), max ?? 0);
        }

        public static void WriteBool(Span<byte> cell, bool? min, bool? max, int? nullCount)
        {
            cell.Clear();
            cell[0] = (byte)(NullFlag(nullCount) | (min.HasValue ? HasMin : 0) | (max.HasValue ? HasMax : 0));
            cell[1] = min == true ? (byte)1 : (byte)0;
            cell[2] = max == true ? (byte)1 : (byte)0;
        }

        public static void WriteBytes(Span<byte> cell, byte[]? min, byte[]? max, int? nullCount)
        {
            cell.Clear();
            byte flags = NullFlag(nullCount);
            if (min != null)
            {
                flags |= HasMin;
                WritePrefix(cell.Slice(1, 1 + PrefixLength), min);
            }
            if (max != null)
            {
                flags |= HasMax;
                if (max.Length <= PrefixLength)
                {
                    flags |= MaxComplete;
                }
                WritePrefix(cell.Slice(2 + PrefixLength, 1 + PrefixLength), max);
            }
            cell[0] = flags;
        }

        private static void WritePrefix(Span<byte> bound, byte[] value)
        {
            var length = Math.Min(value.Length, PrefixLength);
            bound[0] = (byte)length;
            value.AsSpan(0, length).CopyTo(bound.Slice(1));
        }

        private static void WriteDecimal(Span<byte> target, decimal value)
        {
            Span<int> bits = stackalloc int[4];
            decimal.GetBits(value, bits);
            for (int i = 0; i < 4; i++)
            {
                BinaryPrimitives.WriteInt32LittleEndian(target.Slice(i * 4, 4), bits[i]);
            }
        }

        private static decimal ReadDecimal(ReadOnlySpan<byte> source)
        {
            Span<int> bits = stackalloc int[4];
            for (int i = 0; i < 4; i++)
            {
                bits[i] = BinaryPrimitives.ReadInt32LittleEndian(source.Slice(i * 4, 4));
            }
            return new decimal(bits);
        }

        public static bool MayHoldNull(ReadOnlySpan<byte> cell)
        {
            return (cell[0] & MayContainNull) != 0;
        }

        // A missing bound admits every probe on its side
        public static bool MinAdmits(ReadOnlySpan<byte> cell, PruningType type, long probe)
        {
            return (cell[0] & HasMin) == 0 || BinaryPrimitives.ReadInt64LittleEndian(cell.Slice(1, 8)) <= probe;
        }

        public static bool MaxAdmits(ReadOnlySpan<byte> cell, PruningType type, long probe)
        {
            return (cell[0] & HasMax) == 0 || BinaryPrimitives.ReadInt64LittleEndian(cell.Slice(1 + BoundSize(type), 8)) >= probe;
        }

        // Widened by the comparer's epsilon, a NaN probe is admitted
        public static bool MinAdmits(ReadOnlySpan<byte> cell, PruningType type, double probe)
        {
            return (cell[0] & HasMin) == 0 || !(BinaryPrimitives.ReadDoubleLittleEndian(cell.Slice(1, 8)) - FloatEpsilon > probe);
        }

        public static bool MaxAdmits(ReadOnlySpan<byte> cell, PruningType type, double probe)
        {
            return (cell[0] & HasMax) == 0 || !(BinaryPrimitives.ReadDoubleLittleEndian(cell.Slice(1 + BoundSize(type), 8)) + FloatEpsilon < probe);
        }

        // Widened by the comparer's epsilon, clamped at the decimal limits
        public static bool MinAdmits(ReadOnlySpan<byte> cell, decimal probe)
        {
            if ((cell[0] & HasMin) == 0)
            {
                return true;
            }
            var min = ReadDecimal(cell.Slice(1, 16));
            return min <= decimal.MinValue + DecimalEpsilon || min - DecimalEpsilon <= probe;
        }

        public static bool MaxAdmits(ReadOnlySpan<byte> cell, decimal probe)
        {
            if ((cell[0] & HasMax) == 0)
            {
                return true;
            }
            var max = ReadDecimal(cell.Slice(17, 16));
            return max >= decimal.MaxValue - DecimalEpsilon || max + DecimalEpsilon >= probe;
        }

        // False sorts before true
        public static bool MinAdmits(ReadOnlySpan<byte> cell, bool probe)
        {
            return (cell[0] & HasMin) == 0 || (cell[1] != 0).CompareTo(probe) <= 0;
        }

        public static bool MaxAdmits(ReadOnlySpan<byte> cell, bool probe)
        {
            return (cell[0] & HasMax) == 0 || (cell[2] != 0).CompareTo(probe) >= 0;
        }

        // A truncated min is below the real one, a probe below it is below the real min too
        public static bool MinAdmits(ReadOnlySpan<byte> cell, ReadOnlySpan<byte> probePrefix, int probeLength)
        {
            if ((cell[0] & HasMin) == 0)
            {
                return true;
            }
            return Compare(cell.Slice(2, cell[1]), probePrefix, probeLength) <= 0;
        }

        // The real max of a truncated one starts with it, only a byte that differs within it proves the probe is above
        public static bool MaxAdmits(ReadOnlySpan<byte> cell, ReadOnlySpan<byte> probePrefix, int probeLength)
        {
            var flags = cell[0];
            if ((flags & HasMax) == 0)
            {
                return true;
            }
            var max = cell.Slice(3 + PrefixLength, cell[2 + PrefixLength]);
            var order = Compare(max, probePrefix, probeLength);
            if ((flags & MaxComplete) != 0)
            {
                return order >= 0;
            }
            return order >= 0 || probePrefix.StartsWith(max);
        }

        // Orders a bound of at most the prefix length against the probe, the probe prefix ties are broken by its full length
        private static int Compare(ReadOnlySpan<byte> bound, ReadOnlySpan<byte> probePrefix, int probeLength)
        {
            var order = bound.SequenceCompareTo(probePrefix);
            if (order != 0)
            {
                return order;
            }
            return probeLength > probePrefix.Length ? -1 : 0;
        }
    }
}
