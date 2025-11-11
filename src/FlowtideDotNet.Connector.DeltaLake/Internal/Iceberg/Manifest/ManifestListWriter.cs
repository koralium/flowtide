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

using Avro.Generic;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Iceberg.Manifest
{
    internal class ManifestListWriter
    {
        private readonly Avro.File.IFileWriter<GenericRecord> _writer;

        public ManifestListWriter(Stream stream)
        {
            var datumWriter = new Avro.Generic.GenericDatumWriter<GenericRecord>(AvroSchemas.ManifestListSchema);
            _writer = Avro.File.DataFileWriter<GenericRecord>.OpenWriter(datumWriter, stream);
        }

        public void AddManifestFile(ManifestFile manifestFile)
        {
            GenericRecord[]? partitions = default;
            if (manifestFile.Partitions != null)
            {
                partitions = new GenericRecord[manifestFile.Partitions.Count];

                for (int i = 0; i < manifestFile.Partitions.Count; i++)
                {
                    var partitionSummary = manifestFile.Partitions[i];
                    var partitionRecord = new GenericRecord(AvroSchemas.FieldSummarySchema);
                    partitionRecord.Add("contains_null", partitionSummary.ContainsNull);

                    if (partitionSummary.ContainsNan.HasValue)
                    {
                        partitionRecord.Add("contains_nan", partitionSummary.ContainsNan.Value);
                    }
                    if (partitionSummary.LowerBound != null)
                    {
                        partitionRecord.Add("lower_bound", partitionSummary.LowerBound);
                    }
                    if (partitionSummary.UpperBound != null)
                    {
                        partitionRecord.Add("upper_bound", partitionSummary.UpperBound);
                    }
                    
                    partitions[i] = partitionRecord;
                }
            }
            

            var manifestRecord = new GenericRecord(AvroSchemas.ManifestListSchema);
            manifestRecord.Add("manifest_path", manifestFile.ManifestPath);
            manifestRecord.Add("manifest_length", manifestFile.ManifestLength);
            manifestRecord.Add("partition_spec_id", manifestFile.PartitionSpecId);
            manifestRecord.Add("content", manifestFile.Content);
            manifestRecord.Add("sequence_number", manifestFile.SequenceNumber);
            manifestRecord.Add("min_sequence_number", manifestFile.MinSequenceNumber);
            manifestRecord.Add("added_snapshot_id", manifestFile.AddedSnapshotId);
            manifestRecord.Add("added_files_count", manifestFile.AddedFilesCount);
            manifestRecord.Add("existing_files_count", manifestFile.ExistingFilesCount);
            manifestRecord.Add("deleted_files_count", manifestFile.DeletedFilesCount);
            manifestRecord.Add("added_rows_count", manifestFile.AddedRowsCount);
            manifestRecord.Add("existing_rows_count", manifestFile.ExistingRowsCount);
            manifestRecord.Add("deleted_rows_count", manifestFile.DeletedRowsCount);

            if (partitions != null)
            {
                manifestRecord.Add("partitions", partitions);
            }

            if (manifestFile.KeyMetadata != null)
            {
                manifestRecord.Add("key_metadata", manifestFile.KeyMetadata);
            }

            if (manifestFile.FirstRowId.HasValue)
            {
                manifestRecord.Add("first_row_id", manifestFile.FirstRowId.Value);
            }

            _writer.Append(manifestRecord);
        }

        public void Finish()
        {
            _writer.Flush();
            _writer.Dispose();
        }
    }
}
