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
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat.ParquetWriters;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema.Types;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using Stowage;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat
{
    internal class ParquetSharpWriter : IDeltaFormatWriter
    {
        private readonly StructType schema;
        private List<IParquetWriter> writers;
        private int rowCount;
        private Apache.Arrow.Schema _schema;
        public ParquetSharpWriter(StructType schema)
        {
            this.schema = schema;
            var visitor = new ParquetSharpWriteVisitor();

            writers = new List<IParquetWriter>();
            for (int i = 0; i < schema.Fields.Count; i++)
            {
                var field = schema.Fields[i];
                var writer = visitor.Visit(field.Type);
                writers.Add(writer);
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

        public void AddRow(ColumnRowReference row)
        {
            rowCount++;
            for (int i = 0; i < writers.Count; i++)
            {
                writers[i].WriteValue(row.referenceBatch.Columns[i].GetValueAt(row.RowIndex, default));
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

        public async Task<int> WriteData(IFileStorage storage, IOPath tablePath, string fileName)
        {
            using var stream = await storage.OpenWrite(tablePath.Combine(fileName));

            using var writer = new ParquetSharp.Arrow.FileWriter(stream, _schema);

            List<IArrowArray> arrays = new List<IArrowArray>();
            for (int i = 0; i < writers.Count; i++)
            {
                var array = writers[i].GetArray();
                arrays.Add(array);
            }

            var batch = new RecordBatch(_schema, arrays, rowCount);

            writer.WriteRecordBatch(batch);
            writer.Close();
            
            var length = stream.Position;

            return (int)length;
        }
    }
}
