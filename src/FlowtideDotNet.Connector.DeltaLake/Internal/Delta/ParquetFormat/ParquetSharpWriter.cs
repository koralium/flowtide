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
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.DeletionVectors;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat.ParquetWriters;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema.Types;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Stats;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Stats.Comparers;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using Stowage;
using System.IO;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat
{
    internal class ParquetSharpWriter : IDeltaFormatWriter
    {
        private readonly StructType schema;
        private List<IParquetWriter> writers;
        private List<IParquetWriter> toWrite;
        private List<IParquetWriter> nullWriters;
        private int rowCount;
        private Apache.Arrow.Schema _schema;

        public ParquetSharpWriter(StructType schema, List<string> columnNames)
        {
            this.schema = schema;
            var visitor = new ParquetSharpWriteVisitor();

            toWrite = new List<IParquetWriter>();
            writers = new List<IParquetWriter>();
            nullWriters = new List<IParquetWriter>();
            for (int i = 0; i < schema.Fields.Count; i++)
            {
                var field = schema.Fields[i];
                var writer = visitor.Visit(field.Type);
                writers.Add(writer);
                if (columnNames.Contains(field.Name, StringComparer.OrdinalIgnoreCase))
                {
                    toWrite.Add(writer);
                }
                else
                {
                    if (!field.Nullable)
                    {
                        throw new InvalidOperationException($"Column {field.Name} is not nullable and is not present in query which means all rows would be inserted as null.");
                    }
                    nullWriters.Add(writer);
                }
            }

            var schemaVisitor = new DeltaTypeArrowTypeVisitor();

            List<Apache.Arrow.Field> fields = new List<Field>();
            foreach(var field in schema.Fields)
            {
                var type = schemaVisitor.Visit(field.Type);
                fields.Add(new Field(field.Name, type, true));
            }
            _schema = new Apache.Arrow.Schema(fields, new Dictionary<string, string>());
        }

        public async Task CopyFrom(IFileStorage storage, IOPath table, string filePath, IDeleteVector deleteVector)
        {
            using var stream = await storage.OpenRead(table.Combine(filePath));

            if (stream == null)
            {
                throw new Exception($"File not found: {filePath}");
            }

            using ParquetSharp.Arrow.FileReader fileReader = new ParquetSharp.Arrow.FileReader(stream);

            var batchReader = fileReader.GetRecordBatchReader();

            int globalIndex = 0;
            Apache.Arrow.RecordBatch batch;
            while ((batch = await batchReader.ReadNextRecordBatchAsync()) != null)
            {
                using (batch)
                {
                    for (int i = 0; i < writers.Count; i++)
                    {
                        writers[i].CopyArray(batch.Column(i), globalIndex, deleteVector, 0, batch.Length);
                    }
                    globalIndex += batch.Length;
                }
            }
            rowCount += (int)(globalIndex - deleteVector.Cardinality);
        }

        public int WrittenCount => rowCount;

        public void AddRow(ColumnRowReference row)
        {
            rowCount++;
            for (int i = 0; i < toWrite.Count; i++)
            {
                var value = row.referenceBatch.Columns[i].GetValueAt(row.RowIndex, default);
                toWrite[i].WriteValue(value);
            }
            for (int i = 0; i < nullWriters.Count; i++)
            {
                nullWriters[i].WriteNull();
            }
        }

        public void NewBatch()
        {
            rowCount = 0;
            foreach (var writer in writers)
            {
                writer.NewBatch();
            }
        }

        public RecordBatch GetRecordBatch()
        {
            List<IArrowArray> arrays = new List<IArrowArray>();
            for (int i = 0; i < writers.Count; i++)
            {
                var array = writers[i].GetArray();
                arrays.Add(array);
            }

            var batch = new RecordBatch(_schema, arrays, rowCount);
            return batch;
        }

        public DeltaStatistics GetStatistics()
        {
            Dictionary<string, IStatisticsComparer> statistics = new Dictionary<string, IStatisticsComparer>();

            for (int i = 0; i < writers.Count; i++)
            {
                var stats = writers[i].GetStatisticsComparer();
                if (stats != null)
                {
                    statistics.Add(schema.Fields[i].Name, stats);
                }
            }

            var obj = new DeltaStatistics()
            {
                NumRecords = rowCount,
                TightBounds = true,
                ValueComparers = statistics
            };

            return obj;
        }

        public async Task<int> WriteData(IFileStorage storage, IOPath tablePath, string fileName)
        {
            using var stream = await storage.OpenWrite(tablePath.Combine(fileName));

            using var writer = new ParquetSharp.Arrow.FileWriter(stream, _schema);

            var batch = GetRecordBatch();
            writer.WriteRecordBatch(batch);
            writer.Close();
            
            var length = stream.Position;

            return (int)length;
        }
    }
}
