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

using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema.Types;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Stats;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Stats.Comparers;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Catalog
{
    /// <param name="ColumnIndex">The column in the written rows.</param>
    /// <param name="Name">The logical field name, statistics comparers are keyed by it.</param>
    /// <param name="Offset">Where the column's cell starts in a row.</param>
    internal readonly record struct PruningColumn(int ColumnIndex, string Name, PruningType Type, int Offset);

    /// <summary>
    /// A fixed size row of bounds per file: a has-statistics byte, then one cell per pruning column.
    /// </summary>
    internal sealed class PruningLayout
    {
        public const byte HasStatistics = 1;

        public PruningLayout(IReadOnlyList<PruningColumn> columns)
        {
            Columns = columns;
            RowSize = columns.Count == 0 ? 1 : columns[^1].Offset + PruningCell.CellSize(columns[^1].Type);
        }

        public IReadOnlyList<PruningColumn> Columns { get; }

        public int RowSize { get; }

        /// <summary>
        /// Written columns in order, up to the limit, structs, arrays and maps never prune.
        /// </summary>
        public static PruningLayout Create(StructType schema, IReadOnlyList<string> writtenColumns, int maxColumns)
        {
            var columns = new List<PruningColumn>();
            var offset = 1;
            for (int i = 0; i < writtenColumns.Count && columns.Count < maxColumns; i++)
            {
                var field = schema.Fields.FirstOrDefault(x => string.Equals(x.Name, writtenColumns[i], StringComparison.OrdinalIgnoreCase));
                if (field == null)
                {
                    continue;
                }
                var type = PruningCell.TypeOf(field.Type);
                if (type == null)
                {
                    continue;
                }
                columns.Add(new PruningColumn(i, field.Name, type.Value, offset));
                offset += PruningCell.CellSize(type.Value);
            }
            return new PruningLayout(columns);
        }

        /// <summary>
        /// The first columns only, the cells keep their offsets.
        /// </summary>
        public PruningLayout Take(int count)
        {
            return new PruningLayout(Columns.Take(count).ToList());
        }

        /// <summary>
        /// The row of a file from its parsed statistics, a file without statistics matches every probe.
        /// </summary>
        public void WriteRow(DeltaStatistics? statistics, Span<byte> row)
        {
            row.Slice(0, RowSize).Clear();
            if (statistics?.ValueComparers == null)
            {
                return;
            }
            row[0] = HasStatistics;
            foreach (var column in Columns)
            {
                var cell = row.Slice(column.Offset, PruningCell.CellSize(column.Type));
                if (statistics.ValueComparers.TryGetValue(column.Name, out var comparer) && comparer is IPruningBounds bounds)
                {
                    bounds.WriteBounds(column.Type, cell);
                }
                else
                {
                    PruningCell.WriteUnknown(cell);
                }
            }
        }
    }
}
