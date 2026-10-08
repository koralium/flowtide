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

using Stowage;

namespace FlowtideDotNet.Connector.DeltaLake
{
    public class DeltaLakeOptions
    {
        public required IFileStorage StorageLocation { get; set; }

        /// <summary>
        /// Mostly used for testing, allows for full replay of the table
        /// </summary>
        public bool OneVersionPerCheckpoint { get; set; } = false;

        /// <summary>
        /// Gets or sets the checkpoint interval in number of commits.
        /// A checkpoint file (.checkpoint.parquet) is written every time this number of commits is reached.
        /// Setting this to 0 or a negative value disables checkpoint writing.
        /// </summary>
        public int CheckpointInterval { get; set; } = 20;

        public TimeSpan DeltaCheckInterval { get; set; } = TimeSpan.FromSeconds(10);

        public bool WriteChangeDataOnNewTables { get; set; } = false;

        public bool EnableDeletionVectorsOnNewTables { get; set; } = true;

        public bool EnableColumnMappingOnNewTables { get; set; } = true;

        /// <summary>
        /// Gets or sets the maximum allowed file size, in bytes. Files are usually smaller than this value because of compression.
        /// </summary>
        /// <remarks>
        /// This setting is used to determine when to roll over to a new file during writes. 
        /// If the current file being written exceeds this size, a new file will be created for subsequent data. 
        /// Setting this value too low may result in many small files, which can degrade read performance,
        /// while setting it too high may lead to fewer but larger files, which can also impact performance and manageability. 
        /// The default value is set to 100 MB.
        /// </remarks>
        public long MaxFileSizeBytes { get; set; } = 100 * 1024 * 1024; // 100 MB

        /// <summary>
        /// Gets or sets the compression codec for written Parquet files, it applies to every table this sink writes.
        /// When null the table property delta.parquet.compression.codec is used, and zstd when the table has none.
        /// </summary>
        public DeltaCompressionCodec? CompressionCodec { get; set; }

        /// <summary>
        /// Gets or sets how many written columns the sink keeps file statistics for, to skip files when deleting rows.
        /// Columns are taken in the order they are written, structs, lists and maps are skipped.
        /// </summary>
        public int StatsPruningColumns { get; set; } = 8;

        /// <summary>
        /// Gets or sets the memory, in bytes, all Delta Lake sinks in the process may use for the statistics they skip files with.
        /// The first sink that starts fixes the value for the process. Every table gets its first column before any table gets more,
        /// a table that does not fit keeps its statistics in spillable state instead.
        /// </summary>
        public long PruningMemoryBytes { get; set; } = 1024L * 1024 * 1024;

        /// <summary>
        /// The clock for every timestamp the sink writes.
        /// </summary>
        internal TimeProvider TimeProvider { get; set; } = TimeProvider.System;

        // Tests use their own reservation instead of the process wide one
        internal Internal.Catalog.PruningReservation? ReservationOverride { get; set; }

        // Catalog trees rotate once mutations exceed twice the live files and this floor
        internal long CatalogRotationFloor { get; set; } = 64 * 1024;

        internal int CatalogMigrationSlice { get; set; } = 16 * 1024;

        internal long CatalogMigrationBytes { get; set; } = 8 * 1024 * 1024;
    }
}
