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

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Iceberg.Manifest
{
    /// <summary>
    /// Class that describes a manifest file that is added to the manifest list
    /// </summary>
    internal class ManifestFile
    {
        public required string ManifestPath { get; set; }

        public required long ManifestLength { get; set; }

        public required int PartitionSpecId { get; set; }

        public required int Content { get; set; }

        public required long SequenceNumber { get; set; }

        public required long MinSequenceNumber { get; set; }

        public required long AddedSnapshotId { get; set; }

        public required int AddedFilesCount { get; set; }

        public required int ExistingFilesCount { get; set; }

        public required int DeletedFilesCount { get; set; }

        public required long AddedRowsCount { get; set; }

        public required long ExistingRowsCount { get; set; }

        public required long DeletedRowsCount { get; set; }

        public List<ManifestPartitionSummary>? Partitions { get; set; }

        public byte[]? KeyMetadata { get; set; }

        public long? FirstRowId { get; set; }
    }
}
