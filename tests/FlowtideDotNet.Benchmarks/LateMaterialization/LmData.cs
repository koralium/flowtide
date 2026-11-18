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

using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Storage.Memory;

namespace FlowtideDotNet.Benchmarks.LateMaterialization
{
    internal enum LmColumnKind
    {
        Key,
        Reference,
        Number,
        Text,
        GroupId,
        GroupText,
        Items
    }

    internal sealed class LmColumn
    {
        public LmColumn(string name, LmColumnKind kind, int size = 8, long modulus = 0, bool payload = false)
        {
            Name = name;
            Kind = kind;
            Size = size;
            Modulus = modulus;
            Payload = payload;
        }

        public string Name { get; }

        public LmColumnKind Kind { get; }

        public int Size { get; }

        public long Modulus { get; set; }

        public bool Payload { get; }
    }

    internal sealed class LmTable
    {
        public LmTable(string name, long rows, bool deletes, params LmColumn[] columns)
        {
            Name = name;
            Rows = rows;
            Deletes = deletes;
            Columns = columns;
        }

        public string Name { get; }

        public long Rows { get; set; }

        public bool Deletes { get; }

        public IReadOnlyList<LmColumn> Columns { get; }

        public int GetColumnIndex(string name)
        {
            for (int i = 0; i < Columns.Count; i++)
            {
                if (Columns[i].Name.Equals(name, StringComparison.OrdinalIgnoreCase))
                {
                    return i;
                }
            }
            throw new InvalidOperationException($"Column {name} is not part of table {Name}");
        }

        public bool HasKind(LmColumnKind kind)
        {
            for (int i = 0; i < Columns.Count; i++)
            {
                if (Columns[i].Kind == kind)
                {
                    return true;
                }
            }
            return false;
        }

        /// <summary>
        /// True when a payload update changes a value of this table.
        /// </summary>
        public bool HasPayload => HasKind(LmColumnKind.Text) || HasKind(LmColumnKind.Items) || HasKind(LmColumnKind.GroupText);

        public bool HasNumbers => HasKind(LmColumnKind.Number);

        public int RowBytes
        {
            get
            {
                int bytes = 0;
                for (int i = 0; i < Columns.Count; i++)
                {
                    bytes += Columns[i].Size;
                }
                return bytes;
            }
        }
    }

    /// <summary>
    /// Row values are a function of seed, table, row and round.
    /// </summary>
    internal static class LmData
    {
        public const int PayloadUpdate = 0;
        public const int InlineUpdate = 1;
        public const int Delete = 2;

        private static readonly StructHeader ItemHeader = StructHeader.Create("sku", "qty");

        public static ulong Mix(ulong value)
        {
            value += 0x9E3779B97F4A7C15UL;
            value = (value ^ (value >> 30)) * 0xBF58476D1CE4E5B9UL;
            value = (value ^ (value >> 27)) * 0x94D049BB133111EBUL;
            return value ^ (value >> 31);
        }

        public static ulong Hash(long seed, string table, int part, long row, long version)
        {
            ulong hash = Mix((ulong)seed);
            for (int i = 0; i < table.Length; i++)
            {
                hash = Mix(hash ^ table[i]);
            }
            hash = Mix(hash ^ (ulong)part);
            hash = Mix(hash ^ (ulong)row);
            return Mix(hash ^ (ulong)version);
        }

        public static bool IsSelected(long seed, LmTable table, int kind, int round, long row)
        {
            // An update only counts when the table has such a column
            if ((kind == Delete && !table.Deletes) || (kind == PayloadUpdate && !table.HasPayload) || (kind == InlineUpdate && !table.HasNumbers))
            {
                return false;
            }
            // One percent updates, a tenth of a percent deletes
            ulong threshold = kind == Delete ? 1UL : 10UL;
            return Hash(seed, table.Name, 1000 + kind, row, round) % 1000 < threshold;
        }

        public static bool IsDeleted(long seed, LmTable table, long row, int round)
        {
            for (int r = 1; r <= round; r++)
            {
                if (IsSelected(seed, table, Delete, r, row))
                {
                    return true;
                }
            }
            return false;
        }

        public static int Version(long seed, LmTable table, int kind, long row, int round)
        {
            int version = 0;
            for (int r = 1; r <= round; r++)
            {
                if (IsSelected(seed, table, kind, r, row))
                {
                    version++;
                }
            }
            return version;
        }

        public static long ReferenceValue(long seed, LmTable table, int column, long row)
        {
            return (long)(Hash(seed, table.Name, column, row, 0) % (ulong)table.Columns[column].Modulus);
        }

        public static long NumberValue(long seed, LmTable table, int column, long row, int round)
        {
            int version = Version(seed, table, InlineUpdate, row, round);
            return (long)(Hash(seed, table.Name, column, row, version) % 1_000_000);
        }

        /// <summary>
        /// A payload update moves the row to another group.
        /// </summary>
        public static long GroupOf(long seed, LmTable table, long groups, long row, int round)
        {
            if (groups < 2)
            {
                return 0;
            }
            // A step below the group count always changes the group
            long step = 1 + (long)(Hash(seed, table.Name, 3000, row, 0) % (ulong)(groups - 1));
            return (row % groups + step * Version(seed, table, PayloadUpdate, row, round)) % groups;
        }

        public static long LongValue(long seed, LmTable table, int column, long row, int round)
        {
            var spec = table.Columns[column];
            switch (spec.Kind)
            {
                case LmColumnKind.Key:
                    return row;
                case LmColumnKind.Reference:
                    return ReferenceValue(seed, table, column, row);
                case LmColumnKind.Number:
                    return NumberValue(seed, table, column, row, round);
                case LmColumnKind.GroupId:
                    return GroupOf(seed, table, spec.Modulus, row, round);
                default:
                    throw new NotSupportedException($"Column kind {spec.Kind} is not a number");
            }
        }

        public static byte[] TextValue(long seed, LmTable table, int column, long row, int round)
        {
            var spec = table.Columns[column];
            if (spec.Kind == LmColumnKind.GroupText)
            {
                return GroupText(seed, table, column, GroupOf(seed, table, spec.Modulus, row, round));
            }
            return Text(Hash(seed, table.Name, column, row, Version(seed, table, PayloadUpdate, row, round)), spec.Size);
        }

        public static byte[] GroupText(long seed, LmTable table, int column, long group)
        {
            return Text(Hash(seed, table.Name, column, group, 0), table.Columns[column].Size);
        }

        public static ListValue ItemsValue(long seed, LmTable table, int column, long row, int round)
        {
            return Items(Hash(seed, table.Name, column, row, Version(seed, table, PayloadUpdate, row, round)), table.Columns[column].Size);
        }

        /// <summary>
        /// One text form for numbers, strings, lists and structs.
        /// </summary>
        public static string? Format(IDataValue value)
        {
            switch (value.Type)
            {
                case ArrowTypeId.Int64:
                    return value.AsLong.ToString(System.Globalization.CultureInfo.InvariantCulture);
                case ArrowTypeId.String:
                    return value.AsString.ToString();
                case ArrowTypeId.List:
                    var list = value.AsList;
                    var items = new System.Text.StringBuilder("[");
                    for (int i = 0; i < list.Count; i++)
                    {
                        items.Append(Format(list.GetAt(i))).Append(';');
                    }
                    return items.Append(']').ToString();
                case ArrowTypeId.Struct:
                    var fields = value.AsStruct;
                    var text = new System.Text.StringBuilder("{");
                    for (int i = 0; i < fields.Header.Count; i++)
                    {
                        text.Append(Format(fields.GetAt(i))).Append(';');
                    }
                    return text.Append('}').ToString();
                default:
                    return null;
            }
        }

        public static void AddValue(Column target, long seed, LmTable table, int column, long row, int round)
        {
            var spec = table.Columns[column];
            switch (spec.Kind)
            {
                case LmColumnKind.GroupText:
                case LmColumnKind.Text:
                    target.Add(new StringValue(TextValue(seed, table, column, row, round)));
                    break;
                case LmColumnKind.Items:
                    target.Add(ItemsValue(seed, table, column, row, round));
                    break;
                default:
                    target.Add(new Int64Value(LongValue(seed, table, column, row, round)));
                    break;
            }
        }

        /// <summary>
        /// Maps a position to a row, in key order or in a seeded shuffle.
        /// </summary>
        public static long RowAt(long seed, LmTable table, long index, bool shuffled)
        {
            if (!shuffled || table.Rows < 2)
            {
                return index;
            }
            // A small Feistel network, walked until in range
            int bits = 2;
            while ((1L << bits) < table.Rows)
            {
                bits += 2;
            }
            int half = bits / 2;
            ulong mask = (1UL << half) - 1;
            ulong value = (ulong)index;
            do
            {
                ulong left = value >> half;
                ulong right = value & mask;
                for (int round = 0; round < 4; round++)
                {
                    ulong next = left ^ (Hash(seed, table.Name, 2000 + round, (long)right, 0) & mask);
                    left = right;
                    right = next;
                }
                value = (left << half) | right;
            }
            while (value >= (ulong)table.Rows);
            return (long)value;
        }

        /// <summary>
        /// Builds the rows of one table for a round, in the given column order.
        /// </summary>
        public static EventBatchData CreateBatch(long seed, LmTable table, IReadOnlyList<int> tableColumns, IReadOnlyList<long> rows, int round, IMemoryAllocator allocator)
        {
            var columns = new IColumn[tableColumns.Count];
            try
            {
                for (int c = 0; c < columns.Length; c++)
                {
                    var column = Column.Create(allocator);
                    columns[c] = column;
                    for (int i = 0; i < rows.Count; i++)
                    {
                        AddValue(column, seed, table, tableColumns[c], rows[i], round);
                    }
                }
            }
            catch
            {
                for (int c = 0; c < columns.Length; c++)
                {
                    columns[c]?.Dispose();
                }
                throw;
            }
            return new EventBatchData(columns);
        }

        private static byte[] Text(ulong state, int length)
        {
            var bytes = new byte[length];
            for (int i = 0; i < bytes.Length; i++)
            {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                bytes[i] = (byte)('a' + (state % 26));
            }
            return bytes;
        }

        private static ListValue Items(ulong state, int size)
        {
            // Each item is close to fifty bytes
            int count = Math.Max(1, size / 50);
            var items = new IDataValue[count];
            for (int i = 0; i < count; i++)
            {
                ulong itemState = Mix(state + (ulong)i);
                items[i] = new StructValue(ItemHeader, new StringValue(Text(itemState, 40)), new Int64Value((long)(itemState % 100)));
            }
            return new ListValue(items);
        }
    }
}
