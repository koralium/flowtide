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

        public static bool MatchInt64(ReadOnlySpan<byte> cell, PruningType type, long probe)
        {
            var flags = cell[0];
            return Int64InBounds(
                (flags & HasMin) != 0, BinaryPrimitives.ReadInt64LittleEndian(cell.Slice(1, 8)),
                (flags & HasMax) != 0, BinaryPrimitives.ReadInt64LittleEndian(cell.Slice(1 + BoundSize(type), 8)),
                probe);
        }

        public static bool MatchDouble(ReadOnlySpan<byte> cell, PruningType type, double probe)
        {
            var flags = cell[0];
            return DoubleInBounds(
                (flags & HasMin) != 0, BinaryPrimitives.ReadDoubleLittleEndian(cell.Slice(1, 8)),
                (flags & HasMax) != 0, BinaryPrimitives.ReadDoubleLittleEndian(cell.Slice(1 + BoundSize(type), 8)),
                probe);
        }

        public static bool MatchDecimal(ReadOnlySpan<byte> cell, decimal probe)
        {
            var flags = cell[0];
            return DecimalInBounds(
                (flags & HasMin) != 0, ReadDecimal(cell.Slice(1, 16)),
                (flags & HasMax) != 0, ReadDecimal(cell.Slice(17, 16)),
                probe);
        }

        public static bool MatchBool(ReadOnlySpan<byte> cell, bool probe)
        {
            var flags = cell[0];
            return BoolInBounds((flags & HasMin) != 0, cell[1] != 0, (flags & HasMax) != 0, cell[2] != 0, probe);
        }

        // A missing bound admits every probe on its side
        private static bool Int64InBounds(bool hasMin, long min, bool hasMax, long max, long probe)
        {
            return !(hasMin && min > probe) && !(hasMax && max < probe);
        }

        // Widened by the comparer's epsilon on both sides
        private static bool DoubleInBounds(bool hasMin, double min, bool hasMax, double max, double probe)
        {
            return !(hasMin && (min - FloatEpsilon) > probe) && !(hasMax && (max + FloatEpsilon) < probe);
        }

        // Widened by the comparer's epsilon on both sides
        private static bool DecimalInBounds(bool hasMin, decimal min, bool hasMax, decimal max, decimal probe)
        {
            return !(hasMin && (min - DecimalEpsilon) > probe) && !(hasMax && (max + DecimalEpsilon) < probe);
        }

        // False sorts before true
        private static bool BoolInBounds(bool hasMin, bool min, bool hasMax, bool max, bool probe)
        {
            return !(hasMin && min.CompareTo(probe) > 0) && !(hasMax && max.CompareTo(probe) < 0);
        }

        /// <summary>
        /// The probe prefix holds at most <see cref="PrefixLength"/> bytes of a probe of the given length.
        /// A complete bound matches exactly like the comparer, a truncated one only prunes what its prefix proves.
        /// </summary>
        public static bool MatchBytes(ReadOnlySpan<byte> cell, ReadOnlySpan<byte> probePrefix, int probeLength)
        {
            var flags = cell[0];
            if ((flags & HasMin) != 0)
            {
                var min = cell.Slice(2, cell[1]);
                // A truncated min is below the real one, a probe below it is below the real min too
                if (Compare(min, probePrefix, probeLength) > 0)
                {
                    return false;
                }
            }
            if ((flags & HasMax) != 0)
            {
                var max = cell.Slice(3 + PrefixLength, cell[2 + PrefixLength]);
                var order = Compare(max, probePrefix, probeLength);
                if ((flags & MaxComplete) != 0)
                {
                    if (order < 0)
                    {
                        return false;
                    }
                }
                // The real max starts with the prefix, only a byte that differs within it proves the probe is above
                else if (order < 0 && !probePrefix.StartsWith(max))
                {
                    return false;
                }
            }
            return true;
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
