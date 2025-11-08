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

using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json.Serialization;
using System.Threading.Tasks;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Iceberg.Metadata
{
    internal class MetadataFile
    {
        /// <summary>
        /// An integer version number for the format. 
        /// Currently, this can be 1 or 2 based on the spec. 
        /// Implementations must throw an exception if a table's version is 
        /// higher than the supported version.
        /// </summary>
        [JsonPropertyName("format-version")]
        public int FormatVersion { get; set; }

        /// <summary>
        /// A UUID that identifies the table, generated when the table is created. 
        /// Implementations must throw an exception if a table's UUID does not match the 
        /// expected UUID after refreshing metadata.
        /// </summary>
        [JsonPropertyName("table-uuid")]
        public string? TableUuid { get; set; }

        /// <summary>
        /// The table's base location. This is used by writers to determine where 
        /// to store data files, manifest files, and table metadata files.
        /// </summary>
        [JsonPropertyName("location")]
        public string? Location { get; set; }

        /// <summary>
        /// The table's highest assigned sequence number, a monotonically 
        /// increasing long that tracks the order of snapshots in a table.
        /// </summary>
        [JsonPropertyName("last-sequence-number")]
        public long LastSequenceNumber { get; set; }

        /// <summary>
        /// Timestamp in milliseconds from the unix epoch when the table was last updated. 
        /// Each table metadata file should update this field just before writing.
        /// </summary>
        [JsonPropertyName("last-updated-ms")]
        public long LastUpdatedMs { get; set; }

        /// <summary>
        /// An integer; the highest assigned column ID for the table. This is used to 
        /// ensure columns are always assigned an unused ID when evolving schemas.
        /// </summary>
        [JsonPropertyName("last-column-id")]
        public int LastColumnId { get; set; }

        [JsonPropertyName("schemas")]
        public List<MetadataSchema>? Schemas { get; set; }

        [JsonPropertyName("current-schema-id")]
        public int CurrentSchemaId { get; set; }
    }
}
