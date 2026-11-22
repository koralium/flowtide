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

using FlowtideDotNet.Core.ColumnStore.DataValues;
using System.IO.Hashing;
using System.Runtime.CompilerServices;

namespace FlowtideDotNet.Core.ColumnStore.Hash
{
    // Values that compare equal get the same hash
    internal static class RowIdentityHash
    {
        public const ulong Seed = 0x9E3779B97F4A7C15;
        private const ulong NaNBits = 0x7FF8000000000000;

        [ThreadStatic]
        private static DataValueContainer? t_value;

        // One hash per row over the picked columns
        public static void HashRows(IReadOnlyList<IColumn> columns, IReadOnlyList<int> picked, int start, Span<ulong> hashes)
        {
            hashes.Fill(Seed);
            for (int c = 0; c < picked.Count; c++)
            {
                var column = columns[picked[c]];
                if (start < 0 || hashes.Length > column.Count - start)
                {
                    throw new ArgumentOutOfRangeException(nameof(start), $"Rows {start} to {start + hashes.Length} are hashed of a column with {column.Count} rows.");
                }
                if (column is Column typed)
                {
                    typed.AddToIdentityHash(start, hashes);
                }
                else if (column is ColumnWithOffset behind)
                {
                    behind.AddToIdentityHash(start, hashes);
                }
                else
                {
                    AddValues(column, start, hashes);
                }
            }
        }

        // Value by value, for columns without a loop
        internal static void AddValues(IColumn column, int start, Span<ulong> hashes)
        {
            var value = t_value ??= new DataValueContainer();
            for (int i = 0; i < hashes.Length; i++)
            {
                column.GetValueAt(start + i, value, default);
                hashes[i] = Add(hashes[i], value);
            }
        }

        // For a data column without a loop, through a value
        internal static ulong AddValue(IDataColumn column, int index, ulong hash)
        {
            var value = new DataValueContainer();
            column.GetValueAt(index, value, default);
            return Add(hash, value);
        }

        public static ulong HashRow(IReadOnlyList<IColumn> columns, IReadOnlyList<int> picked, int row)
        {
            ulong hash = Seed;
            for (int c = 0; c < picked.Count; c++)
            {
                var column = columns[picked[c]];
                if ((uint)row >= (uint)column.Count)
                {
                    throw new ArgumentOutOfRangeException(nameof(row), $"Row {row} is hashed of a column with {column.Count} rows.");
                }
                hash = AddRow(column, row, hash);
            }
            return hash;
        }

        // One row of any column, through a value only if needed
        internal static ulong AddRow(IColumn column, int row, ulong hash)
        {
            if (column is Column typed)
            {
                return typed.AddToIdentityHash(row, hash);
            }
            if (column is ColumnWithOffset behind)
            {
                return behind.AddToIdentityHash(row, hash);
            }
            var value = t_value ??= new DataValueContainer();
            column.GetValueAt(row, value, default);
            return Add(hash, value);
        }

        public static ulong Add<T>(ulong hash, in T value)
            where T : IDataValue
        {
            var type = value.Type;
            switch (type)
            {
                case ArrowTypeId.Null:
                    return AddNull(hash);
                case ArrowTypeId.Boolean:
                    return AddBool(hash, value.AsBool);
                case ArrowTypeId.Int64:
                    return AddInt64(hash, value.AsLong);
                case ArrowTypeId.Double:
                    return AddDouble(hash, value.AsDouble);
                case ArrowTypeId.String:
                    return AddString(hash, value.AsString.Span);
                case ArrowTypeId.Binary:
                    return AddBinary(hash, value.AsBinary);
                case ArrowTypeId.Decimal128:
                    return AddDecimal(hash, value.AsDecimal);
                case ArrowTypeId.Timestamp:
                    return AddTimestamp(hash, value.AsTimestamp.ticks);
                case ArrowTypeId.List:
                    return AddList(hash, value.AsList);
                case ArrowTypeId.Map:
                    return AddMap(hash, value.AsMap);
                case ArrowTypeId.Struct:
                    return AddStruct(hash, value.AsStruct);
                case ArrowTypeId.RowRef:
                    return AddRowRef(hash, (ulong)value.AsLong);
                default:
                    throw new NotSupportedException($"A value of type {type} has no identity hash.");
            }
        }

        // The type goes first, so equal bits hash apart
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static ulong AddNull(ulong hash)
        {
            return Mix(hash, (ulong)ArrowTypeId.Null);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static ulong AddBool(ulong hash, bool value)
        {
            return Mix(Mix(hash, (ulong)ArrowTypeId.Boolean), value ? 1UL : 0UL);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static ulong AddInt64(ulong hash, long value)
        {
            return Mix(Mix(hash, (ulong)ArrowTypeId.Int64), (ulong)value);
        }

        // Zero has one sign and NaN one encoding
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static ulong AddDouble(ulong hash, double value)
        {
            ulong bits = BitConverter.DoubleToUInt64Bits(value);
            if ((bits << 1) == 0)
            {
                bits = 0;
            }
            else if (double.IsNaN(value))
            {
                bits = NaNBits;
            }
            return Mix(Mix(hash, (ulong)ArrowTypeId.Double), bits);
        }

        internal static ulong AddString(ulong hash, ReadOnlySpan<byte> utf8)
        {
            return AddBytes(Mix(hash, (ulong)ArrowTypeId.String), utf8);
        }

        internal static ulong AddBinary(ulong hash, ReadOnlySpan<byte> bytes)
        {
            return AddBytes(Mix(hash, (ulong)ArrowTypeId.Binary), bytes);
        }

        // Compared by ticks, the offset is not part of it
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static ulong AddTimestamp(ulong hash, long ticks)
        {
            return Mix(Mix(hash, (ulong)ArrowTypeId.Timestamp), (ulong)ticks);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static ulong AddRowRef(ulong hash, ulong reference)
        {
            return Mix(Mix(hash, (ulong)ArrowTypeId.RowRef), reference);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static ulong Mix(ulong hash, ulong value)
        {
            hash ^= value;
            hash *= 0xFF51AFD7ED558CCD;
            hash ^= hash >> 33;
            hash *= 0xC4CEB9FE1A85EC53;
            hash ^= hash >> 33;
            return hash;
        }

        // Hashed alone, so its length is part of the hash
        private static ulong AddBytes(ulong hash, ReadOnlySpan<byte> bytes)
        {
            return Mix(hash, XxHash3.HashToUInt64(bytes));
        }

        // 1.0 and 1.00 are the same value
        internal static ulong AddDecimal(ulong hash, decimal value)
        {
            hash = Mix(hash, (ulong)ArrowTypeId.Decimal128);
            Span<int> bits = stackalloc int[4];
            decimal.GetBits(value, bits);
            ulong low = ((ulong)(uint)bits[1] << 32) | (uint)bits[0];
            uint high = (uint)bits[2];
            int scale = (bits[3] >> 16) & 0xFF;
            if (high != 0)
            {
                return AddDecimal_Slow(hash, new UInt128(high, low), scale, bits[3] < 0);
            }
            if (low == 0)
            {
                // Zero has no sign and no scale
                return Mix(Mix(hash, 0), 0);
            }
            while (scale > 0 && low % 10 == 0)
            {
                low /= 10;
                scale--;
            }
            return Mix(Mix(hash, low), DecimalTail(0, scale, bits[3] < 0));
        }

        // A mantissa of more than 64 bits
        private static ulong AddDecimal_Slow(ulong hash, UInt128 mantissa, int scale, bool negative)
        {
            while (scale > 0)
            {
                var (quotient, remainder) = UInt128.DivRem(mantissa, 10);
                if (remainder != UInt128.Zero)
                {
                    break;
                }
                mantissa = quotient;
                scale--;
            }
            return Mix(Mix(hash, (ulong)mantissa), DecimalTail((ulong)(mantissa >> 64), scale, negative));
        }

        private static ulong DecimalTail(ulong high, int scale, bool negative)
        {
            return high | ((ulong)scale << 32) | (negative ? 1UL << 40 : 0UL);
        }

        // How a list, a map and a struct start
        internal static ulong AddListStart(ulong hash, int count)
        {
            return Mix(Mix(hash, (ulong)ArrowTypeId.List), (ulong)count);
        }

        internal static ulong AddMapStart(ulong hash, int length)
        {
            return Mix(Mix(hash, (ulong)ArrowTypeId.Map), (ulong)length);
        }

        internal static ulong AddStructStart(ulong hash, StructHeader header)
        {
            int count = header.Count;
            hash = Mix(Mix(hash, (ulong)ArrowTypeId.Struct), (ulong)count);
            for (int i = 0; i < count; i++)
            {
                hash = AddBytes(hash, header.GetColumnNameUtf8(i));
            }
            return hash;
        }

        private static ulong AddList(ulong hash, IListValue list)
        {
            int count = list.Count;
            hash = AddListStart(hash, count);
            for (int i = 0; i < count; i++)
            {
                hash = Add(hash, list.GetAt(i));
            }
            return hash;
        }

        private static ulong AddMap(ulong hash, IMapValue map)
        {
            int length = map.GetLength();
            hash = AddMapStart(hash, length);
            for (int i = 0; i < length; i++)
            {
                hash = Add(hash, map.GetKeyAt(i));
            }
            for (int i = 0; i < length; i++)
            {
                hash = Add(hash, map.GetValueAt(i));
            }
            return hash;
        }

        private static ulong AddStruct(ulong hash, IStructValue value)
        {
            var header = value.Header;
            int count = header.Count;
            hash = AddStructStart(hash, header);
            for (int i = 0; i < count; i++)
            {
                hash = Add(hash, value.GetAt(i));
            }
            return hash;
        }
    }
}
