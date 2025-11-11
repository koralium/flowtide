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

using Avro;
using Avro.Generic;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Actions;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Stats;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Iceberg.Manifest
{
    internal class ManifestWriter
    {
        private RecordSchema _manifestSchema;
        private RecordSchema _partitionSchema;
        private RecordSchema _dataFileSchema;
        private readonly Avro.File.IFileWriter<GenericRecord> _writer;
        private readonly long snapshotId;
        private readonly Stream stream;
        private readonly string manifestPath;
        private const string ParquetFormat = "PARQUET";
        private readonly long streamStartPosition;

        private int _addedFilesCount;
        private long _addedRowCount;

        public ManifestWriter(long snapshotId, Stream stream, string manifestPath)
        {
            _partitionSchema = RecordSchema.Create("partition", new List<Field>());
            _dataFileSchema = AvroSchemas.CreateDataFileSchema(_partitionSchema);
            _manifestSchema = AvroSchemas.CreateManifestEntrySchema(_dataFileSchema);
            
            var datumWriter = new Avro.Generic.GenericDatumWriter<GenericRecord>(_manifestSchema);
            _writer = Avro.File.DataFileWriter<GenericRecord>.OpenWriter(datumWriter, stream, true);
            this.snapshotId = snapshotId;
            this.stream = stream;
            this.manifestPath = manifestPath;
            streamStartPosition = stream.Position;
        }

        public void AddDataFile(
            Delta.Schema.Types.StructType schema, 
            DeltaAddAction addAction, 
            DeltaStatistics stats,
            long snapshotId)
        {
            var dataFileRecord = new GenericRecord(_dataFileSchema);
            dataFileRecord.Add("content", 0);
            dataFileRecord.Add("file_path", addAction.Path);
            dataFileRecord.Add("file_format", ParquetFormat);

            GenericRecord partitionRecord = new GenericRecord(_partitionSchema);
            dataFileRecord.Add("partition", partitionRecord);
            dataFileRecord.Add("record_count", stats.NumRecords);
            dataFileRecord.Add("file_size_in_bytes", addAction.Size);

            List<GenericRecord> nullValueCounts = new List<GenericRecord>();
            List<GenericRecord> lowerBoundsList = new List<GenericRecord>();
            List<GenericRecord> upperBoundsList = new List<GenericRecord>();

            if (stats.ValueComparers != null)
            {
                foreach (var val in stats.ValueComparers)
                {
                    var field = schema.Fields.First(x => x.PhysicalName == val.Key);
                    var fieldId = field.FieldId!.Value;
                    if (val.Value.NullCount.HasValue)
                    {
                        GenericRecord nullCount = new GenericRecord(AvroSchemas.NullValueCountSchema);
                        nullCount.Add("key", fieldId);
                        nullCount.Add("value", (long)val.Value.NullCount.Value);

                        nullValueCounts.Add(nullCount);
                    }
                    var minValue = val.Value.GetMinValueIcebergBinary();
                    if (minValue != null)
                    {
                        GenericRecord lowerBound = new GenericRecord(AvroSchemas.lowerBoundSchema);
                        lowerBound.Add("key", fieldId);
                        lowerBound.Add("value", minValue);

                        lowerBoundsList.Add(lowerBound);
                    }
                    var maxValue = val.Value.GetMaxValueIcebergBinary();
                    if (maxValue != null)
                    {
                        GenericRecord upperBound = new GenericRecord(AvroSchemas.UpperBoundSchema);
                        upperBound.Add("key", fieldId);
                        upperBound.Add("value", maxValue);
                        upperBoundsList.Add(upperBound);
                    }
                }
            }


            dataFileRecord.Add("null_value_counts", nullValueCounts.ToArray());
            dataFileRecord.Add("lower_bounds", lowerBoundsList.ToArray());
            dataFileRecord.Add("upper_bounds", upperBoundsList.ToArray());

            GenericRecord manifestRecord = new GenericRecord(_manifestSchema);

            manifestRecord.Add("status", 1);
            manifestRecord.Add("snapshot_id", snapshotId);
            manifestRecord.Add("data_file", dataFileRecord);
            _writer.Append(manifestRecord);

            _addedFilesCount++;
            _addedRowCount += stats.NumRecords;
        }

        public ManifestFile Finish()
        {
            _writer.Flush();
            _writer.Dispose();

            var byteLength = stream.Position - streamStartPosition;

            return new ManifestFile()
            {
                AddedFilesCount = _addedFilesCount,
                AddedRowsCount = _addedRowCount,
                AddedSnapshotId = snapshotId,
                Content = 0,
                DeletedFilesCount = 0,
                DeletedRowsCount = 0,
                ExistingFilesCount = 0,
                ExistingRowsCount = 0,
                ManifestLength = byteLength,
                ManifestPath = manifestPath,
                MinSequenceNumber = 0,
                PartitionSpecId = 0,
                SequenceNumber = 0,
            };
        }
    }
}
