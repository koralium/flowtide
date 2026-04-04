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

using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.DeletionVectors;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat.ArrowEncoders;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema.Types;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.ObjectConverter;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Memory;
using ParquetSharp.Arrow;
using Stowage;
using System.Diagnostics;
using System.Text.Json;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat
{
    internal class ParquetSharpReader : IDeltaFormatReader
    {
        private const string ChangeUpdatePreImage = "update_preimage";
        private const string ChangeUpdatePostImage = "update_postimage";
        private const string ChangeInsert = "insert";
        private const string ChangeDelete = "delete";

        private List<string>? _physicalColumnNamesInBatch;
        private List<IArrowEncoder>? _encoders;

        public List<StructField>? Fields { get; private set; }

        public void Initialize(DeltaTable table, IReadOnlyList<string> columnNames)
        {
            ParquetArrowTypeVisitor visitor = new ParquetArrowTypeVisitor();
            List<IArrowEncoder> encoders = new List<IArrowEncoder>();
            _physicalColumnNamesInBatch = new List<string>();
            Fields = new List<StructField>();
            for (int i = 0; i < columnNames.Count; i++)
            {
                var name = columnNames[i];
                var field = table.Schema.Fields.FirstOrDefault(x => x.Name.Equals(name, StringComparison.OrdinalIgnoreCase));

                if (field == null)
                {
                    throw new Exception($"Field {name} not found in schema");
                }

                Fields.Add(field);

                string physicalName = field.Name;
                if (field.PhysicalName != null)
                {
                    physicalName = field.PhysicalName;
                }
                if (table.PartitionColumns.Contains(field.Name))
                {
                    // Handle partition columns
                    encoders.Add(new PartitionValueEncoderVisitor(physicalName).Visit(field.Type));
                }
                else
                {
                    _physicalColumnNamesInBatch.Add(physicalName);
                    encoders.Add(field.Type.Accept(visitor));
                }
            }
            _encoders = encoders;
        }

        public async IAsyncEnumerable<CdcBatchResult> ReadCdcFile(IFileStorage storage, IOPath table, string path, Dictionary<string, string>? partitionValues, IMemoryAllocator memoryAllocator)
        {
            Debug.Assert(_physicalColumnNamesInBatch != null);
            if (_encoders == null)
            {
                throw new InvalidOperationException("Initialize must be called before ReadDataFile");
            }

            var stream = await storage.OpenRead(table.Combine(path));

            if (stream == null)
            {
                throw new Exception($"File not found: {path}");
            }

            ParquetSharp.Arrow.FileReader fileReader = new ParquetSharp.Arrow.FileReader(stream);

            foreach (var encoder in _encoders)
            {
                encoder.NewFile(partitionValues);
            }

            List<int> columnsToSelect = new List<int>();

            for (int i = 0; i < _physicalColumnNamesInBatch.Count; i++)
            {

                bool found = false;
                for (int k = 0; k < fileReader.SchemaManifest.SchemaFields.Count; k++)
                {
                    var field = fileReader.SchemaManifest.SchemaFields[k];

                    if (field.Field.Name.Equals(_physicalColumnNamesInBatch[i], StringComparison.OrdinalIgnoreCase))
                    {

                        found = true;
                        AddColumnToSelect(field, columnsToSelect);
                        break;
                    }
                }
                if (!found)
                {
                    throw new InvalidOperationException($"Could not find field {_physicalColumnNamesInBatch[i]} in batch");
                }
            }

            for (int k = 0; k < fileReader.SchemaManifest.SchemaFields.Count; k++)
            {
                var field = fileReader.SchemaManifest.SchemaFields[k];

                if (field.Field.Name.Equals("_change_type", StringComparison.OrdinalIgnoreCase))
                {
                    AddColumnToSelect(field, columnsToSelect);
                    break;
                }
            }

            int changeTypeIndex = -1;
            for (int i = 0; i < fileReader.Schema.FieldsList.Count; i++)
            {
                if (fileReader.Schema.FieldsList[i].Name == "_change_type")
                {
                    changeTypeIndex = i;
                }
            }
            if (changeTypeIndex == -1)
            {
                throw new InvalidOperationException("Could not find _change_type column in batch");
            }

            var batchReader = fileReader.GetRecordBatchReader(columns: columnsToSelect.ToArray());

            int globalIndex = 0;
            Apache.Arrow.RecordBatch batch;
            while ((batch = await batchReader.ReadNextRecordBatchAsync()) != null)
            {
                using (batch)
                {
                    PrimitiveList<int> weights = new PrimitiveList<int>(memoryAllocator);
                    var changeTypeColumn = batch.Column(changeTypeIndex);
                    var changeTypes = (Apache.Arrow.StringArray)changeTypeColumn;

                    Column[] outColumns = new Column[_encoders.Count];
                    for (int i = 0; i < outColumns.Length; i++)
                    {
                        outColumns[i] = new Column(memoryAllocator);
                    }
                    int batchCount = 0;
                    int columnIndex = 0;
                    for (int i = 0; i < _encoders.Count; i++)
                    {
                        var encoder = _encoders[i];
                        if (!encoder.IsPartitionValueEncoder)
                        {
                            encoder.NewBatch(batch.Column(columnIndex));
                            columnIndex++;
                        }
                    }

                    for (int i = 0; i < batch.Length; i++, globalIndex++)
                    {
                        batchCount++;

                        switch (changeTypes.GetString(i))
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
                                throw new InvalidOperationException($"Unknown change type: {changeTypes.GetString(i)}");
                        }

                        for (int j = 0; j < _encoders.Count; j++)
                        {
                            var encoder = _encoders[j];
                            AddToColumn(encoder, outColumns[j], i);
                        }
                    }

                    yield return new CdcBatchResult()
                    {
                        count = batchCount,
                        data = new EventBatchData(outColumns),
                        weights = weights
                    };
                }
            }
        }

        private static void AddColumnToSelect(SchemaField schemaField, List<int> columnsToSelect)
        {
            if (schemaField.ColumnIndex >= 0)
            {
                columnsToSelect.Add(schemaField.ColumnIndex);
            }
            foreach (var child in schemaField.Children)
            {
                AddColumnToSelect(child, columnsToSelect);
            }
        }

        private static void AddToColumn(IArrowEncoder encoder, Column column, int index)
        {
            var func = new AddToColumnFunc(column);
            encoder.AddValue(index, ref func);
        }

        public async IAsyncEnumerable<Apache.Arrow.RecordBatch> ReadDataFileArrowFormat(IFileStorage storage, IOPath table, string path)
        {
            using var stream = await storage.OpenRead(table.Combine(path));

            if (stream == null)
            {
                throw new Exception($"File not found: {path}");
            }

            using ParquetSharp.Arrow.FileReader fileReader = new ParquetSharp.Arrow.FileReader(stream);

            var batchReader = fileReader.GetRecordBatchReader();

            Apache.Arrow.RecordBatch batch;
            while ((batch = await batchReader.ReadNextRecordBatchAsync()) != null)
            {
                yield return batch;
            }
        }

        public async IAsyncEnumerable<BatchResultWithWeights> ReadAddRemovedDataFile(
            IFileStorage storage,
            IOPath table,
            string path,
            IEnumerable<(long id, int weight)> changedIndices,
            Dictionary<string, string>? partitionValues,
            IMemoryAllocator memoryAllocator)
        {
            Debug.Assert(_physicalColumnNamesInBatch != null);
            Debug.Assert(_encoders != null);

            using var stream = await storage.OpenRead(table.Combine(path));

            if (stream == null)
            {
                throw new Exception($"File not found: {path}");
            }

            using ParquetSharp.Arrow.FileReader fileReader = new ParquetSharp.Arrow.FileReader(stream);

            List<int> columnsToSelect = new List<int>();

            foreach (var encoder in _encoders)
            {
                encoder.NewFile(partitionValues);
            }

            for (int i = 0; i < _physicalColumnNamesInBatch.Count; i++)
            {

                bool found = false;
                for (int k = 0; k < fileReader.SchemaManifest.SchemaFields.Count; k++)
                {
                    var field = fileReader.SchemaManifest.SchemaFields[k];

                    if (field.Field.Name.Equals(_physicalColumnNamesInBatch[i], StringComparison.OrdinalIgnoreCase))
                    {

                        found = true;
                        AddColumnToSelect(field, columnsToSelect);
                        break;
                    }
                }
                if (!found)
                {
                    throw new InvalidOperationException($"Could not find field {_physicalColumnNamesInBatch[i]} in batch");
                }
            }

            var batchReader = fileReader.GetRecordBatchReader(columns: columnsToSelect.ToArray());


            var changedIndexEnumerator = changedIndices.GetEnumerator();

            var changeHasNext = changedIndexEnumerator.MoveNext();
            if (!changeHasNext)
            {
                yield break;
            }

            int globalIndex = 0;
            Apache.Arrow.RecordBatch batch;
            while ((batch = await batchReader.ReadNextRecordBatchAsync()) != null && changeHasNext)
            {
                if (globalIndex + batch.Length < changedIndexEnumerator.Current.id)
                {
                    // The index to search is not in this batch, skip it
                    globalIndex += batch.Length;
                    batch.Dispose();
                    continue;
                }
                using (batch)
                {
                    PrimitiveList<int> weights = new PrimitiveList<int>(memoryAllocator);
                    Column[] outColumns = new Column[_encoders.Count];
                    for (int i = 0; i < outColumns.Length; i++)
                    {
                        outColumns[i] = new Column(memoryAllocator);
                    }
                    int batchCount = 0;
                    int columnIndex = 0;
                    for (int i = 0; i < _encoders.Count; i++)
                    {
                        var encoder = _encoders[i];
                        if (!encoder.IsPartitionValueEncoder)
                        {
                            encoder.NewBatch(batch.Column(columnIndex));
                            columnIndex++;
                        }
                    }

                    while (changeHasNext)
                    {
                        var relativeIndex = (int)(changedIndexEnumerator.Current.id - globalIndex);
                        for (int j = 0; j < _encoders.Count; j++)
                        {
                            var encoder = _encoders[j];
                            AddToColumn(encoder, outColumns[j], relativeIndex);
                        }

                        batchCount++;
                        weights.Add(changedIndexEnumerator.Current.weight);
                        changeHasNext = changedIndexEnumerator.MoveNext();

                        if (!changeHasNext)
                        {
                            break;
                        }

                        if (changedIndexEnumerator.Current.id >= globalIndex + batch.Length)
                        {
                            break;
                        }
                    }

                    yield return new BatchResultWithWeights()
                    {
                        count = batchCount,
                        data = new EventBatchData(outColumns),
                        weights = weights
                    };
                }
            }
        }

        public async IAsyncEnumerable<BatchResult> ReadDataFile(IFileStorage storage, IOPath table, string path, IDeleteVector deleteVector, Dictionary<string, string>? partitionValues, IMemoryAllocator memoryAllocator)
        {
            Debug.Assert(_physicalColumnNamesInBatch != null);
            Debug.Assert(_encoders != null);

            using var stream = await storage.OpenRead(table.Combine(path));

            if (stream == null)
            {
                throw new Exception($"File not found: {path}");
            }

            using ParquetSharp.Arrow.FileReader fileReader = new ParquetSharp.Arrow.FileReader(stream);

            List<int> columnsToSelect = new List<int>();

            foreach (var encoder in _encoders)
            {
                encoder.NewFile(partitionValues);
            }

            HashSet<int> nullColumns = new HashSet<int>();
            for (int i = 0; i < _physicalColumnNamesInBatch.Count; i++)
            {

                bool found = false;
                for (int k = 0; k < fileReader.SchemaManifest.SchemaFields.Count; k++)
                {
                    var field = fileReader.SchemaManifest.SchemaFields[k];

                    if (field.Field.Name.Equals(_physicalColumnNamesInBatch[i], StringComparison.OrdinalIgnoreCase))
                    {

                        found = true;
                        AddColumnToSelect(field, columnsToSelect);
                        break;
                    }
                }
                if (!found)
                {
                    nullColumns.Add(i);
                }
            }

            var batchReader = fileReader.GetRecordBatchReader(columns: columnsToSelect.ToArray());

            int globalIndex = 0;
            Apache.Arrow.RecordBatch batch;
            while ((batch = await batchReader.ReadNextRecordBatchAsync()) != null)
            {
                using (batch)
                {
                    Column[] outColumns = new Column[_encoders.Count];
                    for (int i = 0; i < outColumns.Length; i++)
                    {
                        outColumns[i] = new Column(memoryAllocator);
                    }
                    int batchCount = 0;
                    int columnIndex = 0;
                    for (int i = 0; i < _encoders.Count; i++)
                    {
                        var encoder = _encoders[i];
                        if (!encoder.IsPartitionValueEncoder)
                        {
                            if (nullColumns.Contains(i))
                            {
                                encoder.NewNullBatch();
                            }
                            else
                            {
                                encoder.NewBatch(batch.Column(columnIndex));
                            }

                            columnIndex++;
                        }
                    }

                    for (int i = 0; i < batch.Length; i++, globalIndex++)
                    {
                        if (deleteVector.Contains(globalIndex))
                        {
                            continue;
                        }
                        batchCount++;
                        for (int j = 0; j < _encoders.Count; j++)
                        {
                            var encoder = _encoders[j];
                            AddToColumn(encoder, outColumns[j], i);
                        }
                    }

                    yield return new BatchResult()
                    {
                        count = batchCount,
                        data = new EventBatchData(outColumns)
                    };
                }
            }
        }
    }
}
