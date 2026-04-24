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
    }
}
