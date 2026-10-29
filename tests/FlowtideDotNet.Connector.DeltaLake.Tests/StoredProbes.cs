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
using FlowtideDotNet.Connector.DeltaLake.Internal.Catalog;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Stats;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Storage.Memory;
using Delta = FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema.Types;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    // Probe batches as the sink builds them, from rows through the delete writer
    internal static class StoredProbes
    {
        // One field c of a kernel type, decimals at scale 18
        public static Delta.StructType Table(PruningType type)
        {
            SchemaBaseType field = type switch
            {
                PruningType.Int64 => new Delta.LongType(),
                PruningType.Double => new Delta.DoubleType(),
                PruningType.Float32 => new Delta.FloatType(),
                PruningType.Decimal => new Delta.DecimalType(38, 18),
                PruningType.Date => new Delta.DateType(),
                PruningType.Timestamp => new Delta.TimestampType(),
                PruningType.Bool => new Delta.BooleanType(),
                PruningType.String => new Delta.StringType(),
                _ => new Delta.BinaryType()
            };
            return new Delta.StructType(new[] { new Delta.StructField("c", field, true, new Dictionary<string, object>()) });
        }

        // One value per table field in each row
        public static RecordBatch Store(Delta.StructType table, IReadOnlyList<IDataValue[]> rows)
        {
            var columns = table.Fields.Select(_ => FlowtideDotNet.Core.ColumnStore.Column.Create(GlobalMemoryManager.Instance)).ToArray();
            foreach (var row in rows)
            {
                for (int c = 0; c < columns.Length; c++)
                {
                    columns[c].Add(row[c]);
                }
            }
            using var data = new EventBatchData(columns.Cast<IColumn>().ToArray());
            var writer = new ParquetSharpWriter(table, table.Fields.Select(x => x.Name).ToList(), countNulls: true);
            writer.NewBatch();
            for (int i = 0; i < rows.Count; i++)
            {
                writer.AddRow(new ColumnRowReference() { referenceBatch = data, RowIndex = i });
            }
            return writer.GetRecordBatch();
        }

        // One value per layout column in each probe, the other fields null
        public static RecordBatch StoreProbes(PruningLayout layout, Delta.StructType table, IReadOnlyList<IDataValue[]> probes)
        {
            var rows = new List<IDataValue[]>(probes.Count);
            foreach (var probe in probes)
            {
                var row = Enumerable.Repeat<IDataValue>(NullValue.Instance, table.Fields.Count).ToArray();
                for (int c = 0; c < layout.Columns.Count; c++)
                {
                    row[layout.Columns[c].FieldIndex] = probe[c];
                }
                rows.Add(row);
            }
            return Store(table, rows);
        }

        // One probe per stored row
        public static ProbeBatch Build(PruningLayout layout, RecordBatch stored)
        {
            return ProbeBatch.Build(layout, stored, Enumerable.Range(0, stored.Length).ToArray());
        }
    }
}
