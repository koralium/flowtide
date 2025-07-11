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
    }
}
