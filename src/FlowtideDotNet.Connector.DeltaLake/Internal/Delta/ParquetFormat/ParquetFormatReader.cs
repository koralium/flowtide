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


using FlowtideDotNet.Base.Engine;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.DeletionVectors;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat.Encoders;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.ObjectConverter;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Memory;
using Parquet;
using Stowage;
using System.Text.Json;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat
{
    internal class ParquetFormatReader : IDeltaFormatReader
    {
        private const string ChangeUpdatePreImage = "update_preimage";
        private const string ChangeUpdatePostImage = "update_postimage";
        private const string ChangeInsert = "insert";
        private const string ChangeDelete = "delete";


        private List<IParquetEncoder>? _encoders;
        public void Initialize(DeltaTable table, IReadOnlyList<string> columnNames)
        {
            var visitor = new ReadEncoderVisitor();
            List<IParquetEncoder> encoders = new List<IParquetEncoder>();
            for (int i = 0; i < columnNames.Count; i++)
            {
                var name = columnNames[i];
                var field = table.Schema.Fields.FirstOrDefault(x => x.Name.Equals(name, StringComparison.OrdinalIgnoreCase));

                if (field == null)
                {
                    throw new Exception($"Field {name} not found in schema");
                }

                string physicalName = field.Name;
                if (field.Metadata.TryGetValue("delta.columnMapping.physicalName", out var mappingName))
                {
                    physicalName = ((JsonElement)mappingName)!.GetString()!;
                }
                if (table.PartitionColumns.Contains(field.Name))
                {
                    // Handle partition columns
                    encoders.Add(new PartitionValueVisitor(physicalName).Visit(field.Type));
                }
                else
                {
                    visitor.SetRootPath(physicalName);
                    encoders.Add(field.Type.Accept(visitor));
                    visitor.ClearRootPath();
                }
            }
            _encoders = encoders;
        }

        public async IAsyncEnumerable<CdcBatchResult> ReadCdcFile(IFileStorage storage, IOPath table, string path, Dictionary<string, string>? partitionValues, IMemoryAllocator memoryAllocator)
        {
            if (_encoders == null)
            {
                throw new InvalidOperationException("Initialize must be called before ReadDataFile");
            }

            var stream = await storage.OpenRead(table.Combine(path));

            if (stream == null)
            {
                throw new Exception($"File not found: {path}");
            }

            ParquetReader reader = await ParquetReader.CreateAsync(stream);
            foreach (var encoder in _encoders)
            {
                encoder.NewBatch(reader.Schema, default);
            }

            var changeTypeField = reader.Schema.DataFields.First(x => x.Name == "_change_type");

            int globalIndex = 0;
            for (int i = 0; i < reader.RowGroupCount; i++)
            {
                var rowGroupReader = reader.OpenRowGroupReader(i);
                foreach (var encoder in _encoders)
                {
                    await encoder.NewRowGroup(rowGroupReader);
                }
                var changeTypeColumn = await rowGroupReader.ReadColumnAsync(changeTypeField);
                string[] changeData = (string[])changeTypeColumn.Data;

                int numberOfBatches = (int)Math.Ceiling(rowGroupReader.RowCount / 100.0);

                int index = 0;
                for (int b = 0; b < numberOfBatches; b++)
                {
                    int rowCountInBatch = (int)Math.Min(100, rowGroupReader.RowCount - index);

                    PrimitiveList<int> weights = new PrimitiveList<int>(memoryAllocator);
                    Column[] columns = new Column[_encoders.Count];

                    for (int c = 0; c < columns.Length; c++)
                    {
                        columns[c] = new Column(memoryAllocator);
                    }
                    int batchCount = 0;
                    for (int j = 0; j < rowCountInBatch; j++, index++, globalIndex++)
                    {
                        batchCount++;

                        switch (changeData[index])
                        {
                            case ChangeUpdatePreImage:
                                weights.Add(-1);
                                break;
                            case ChangeUpdatePostImage:
                                weights.Add(1);
                                break;
                            case ChangeInsert:
                                weights.Add(1);
                                break;
                            case ChangeDelete:
                                weights.Add(-1);
                                break;
                            default:
                                throw new InvalidOperationException($"Unknown change type: {changeData[index]}");
                        }

                        for (int c = 0; c < columns.Length; c++)
                        {
                            AddToColumn(columns[c], _encoders[c]);
                        }
                    }

                    yield return new CdcBatchResult() { data = new EventBatchData(columns), count = batchCount, weights = weights };
                }
            }

            foreach (var encoder in _encoders)
            {
                encoder.FinishBatch();
            }
        }

        private void AddToColumn(Column column, IParquetEncoder encoder)
        {
            var func = new AddToColumnParquet(column);
            encoder.ReadNextData(ref func);
        }

        private void SkipIndex(IParquetEncoder encoder)
        {
            var func = new AddToColumnParquet();
            encoder.ReadNextData(ref func);
        }

        public async IAsyncEnumerable<BatchResult> ReadDataFile(
            IFileStorage storage,
            IOPath table,
            string path,
            IDeleteVector deleteVector,
            Dictionary<string, string>? partitionValues,
            IMemoryAllocator memoryAllocator)
        {
            if (_encoders == null)
            {
                throw new InvalidOperationException("Initialize must be called before ReadDataFile");
            }
            var stream = await storage.OpenRead(table.Combine(path));
            if (stream == null)
            {
                throw new Exception($"File not found: {path}");
            }

            ParquetReader reader = await ParquetReader.CreateAsync(stream);

            foreach (var encoder in _encoders)
            {
                encoder.NewBatch(reader.Schema, partitionValues);
            }

            int globalIndex = 0;
            for (int i = 0; i < reader.RowGroupCount; i++)
            {
                var rowGroupReader = reader.OpenRowGroupReader(i);
                foreach (var encoder in _encoders)
                {
                    await encoder.NewRowGroup(rowGroupReader);
                }

                int numberOfBatches = (int)Math.Ceiling(rowGroupReader.RowCount / 100.0);

                int index = 0;
                for (int b = 0; b < numberOfBatches; b++)
                {
                    int rowCountInBatch = (int)Math.Min(100, rowGroupReader.RowCount - index);

                    Column[] columns = new Column[_encoders.Count];

                    for (int c = 0; c < columns.Length; c++)
                    {
                        columns[c] = new Column(memoryAllocator);
                    }
                    int batchCount = 0;
                    for (int j = 0; j < rowCountInBatch; j++, index++, globalIndex++)
                    {
                        if (deleteVector.Contains(globalIndex))
                        {
                            for (int c = 0; c < columns.Length; c++)
                            {
                                SkipIndex(_encoders[c]);
                            }
                            continue;
                        }
                        batchCount++;

                        for (int c = 0; c < columns.Length; c++)
                        {
                            AddToColumn(columns[c], _encoders[c]);
                        }
                    }

                    yield return new BatchResult() { data = new EventBatchData(columns), count = batchCount };
                }
            }

            foreach (var encoder in _encoders)
            {
                encoder.FinishBatch();
            }
        }
    }
}
