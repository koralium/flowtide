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
        /// Mostly used for testing, allows for full replay of the table.
        /// Starts from the first version like <see cref="StartFromFirstVersion"/> and waits for a checkpoint after each version.
        /// </summary>
        public bool OneVersionPerCheckpoint { get; set; } = false;

        /// <summary>
        /// Only used for reading, the first start reads the oldest version of the table instead of the latest, then every later version.
        /// All of them are part of the first load, so its first checkpoint is at the latest version.
        /// </summary>
        public bool StartFromFirstVersion { get; set; } = false;

        /// <summary>
        /// Only used for reading, adds the columns __weight, __modificationDate and __version and sends every change as an insert.
        /// A deleted row has __weight -1, __modificationDate is the commit time and __version the version of the change.
        /// </summary>
        public bool IncludeChangeHistory { get; set; } = false;

        /// <summary>
        /// Gets or sets how many published versions may follow the latest checkpoint before a new one is built in the background.
        /// Setting this to 0 or a negative value turns checkpoint maintenance off.
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
        /// The first sink that reads its table fixes the value for the process. A table's first column has priority: tables holding more
        /// columns give them up at their next checkpoint, and the table that did not fit keeps its statistics in spillable state until the stream restarts.
        /// </summary>
        public long PruningMemoryBytes { get; set; } = 1024L * 1024 * 1024;

        /// <summary>
        /// Gets or sets how long published versions may wait for a checkpoint before one is built regardless of the interval; null turns the age trigger off.
        /// </summary>
        public TimeSpan? CheckpointMaxAge { get; set; } = TimeSpan.FromHours(24);

        /// <summary>
        /// Gets or sets how many maintenance jobs run at once in the process; the first sink's value applies to every sink.
        /// </summary>
        public int MaxConcurrentMaintenanceJobs { get; set; } = 2;

        /// <summary>
        /// Gets or sets the local folder for maintenance scratch files, ./data/tempFiles/deltalake when null; the first sink's value applies to every sink.
        /// </summary>
        public string? MaintenanceTempDirectory { get; set; }

        /// <summary>
        /// Gets or sets whether, when every commit since version 0 is still there, a checkpoint is rebuilt from the full log instead of building on one the sink did not write (or wrote on top of one), and instead of declining when no starting checkpoint can be used.
        /// </summary>
        public bool RebuildLegacyCheckpoints { get; set; }

        /// <summary>
        /// The clock for every timestamp the sink writes.
        /// </summary>
        internal TimeProvider TimeProvider { get; set; } = TimeProvider.System;

        // Tests use their own maintenance budget instead of the process wide one
        internal Internal.Maintenance.DeltaMaintenanceBudget? MaintenanceBudgetOverride { get; set; }

        // Called after every CommitVersion with the pending version, the publication outcome and the worker's target
        internal Action<long?, Internal.Delta.PublishOutcome?, long>? CommitVersionHookForTests { get; set; }

        // Maintenance initialization throws, so tests see the failure contained
        internal bool MaintenanceInitFailsForTests { get; set; }

        // Tests use their own reservation instead of the process wide one
        internal Internal.Catalog.PruningReservation? ReservationOverride { get; set; }
    }
}
