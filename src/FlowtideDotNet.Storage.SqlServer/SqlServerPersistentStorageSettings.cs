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

namespace FlowtideDotNet.Storage.SqlServer
{
    /// <summary>
    /// Represents the settings for SQL Server persistent storage.
    /// </summary>
    public class SqlServerPersistentStorageSettings
    {
        /// <summary>
        /// Gets or sets the connection string used to connect to the SQL Server database.
        /// </summary>
        public required string ConnectionString { get; set; }

        /// <summary>
        /// Gets or sets the limit for the number of pages to be written in bulk operations. 
        /// When the number of pages to be written exceeds this limit, the pages are written in a background batch.
        /// </summary>
        public int WritePagesBulkLimit { get; set; } = 1500;

        /// <summary>
        /// Gets or sets the settings for SQL Server bulk copy operations.
        /// </summary>
        public SqlServerBulkCopySettings BulkCopySettings { get; set; } = new();

        /// <summary>
        /// Gets or sets the name of the stream table, can include schema and database name.
        /// </summary>
        public string StreamTableName { get; set; } = "[dbo].[Streams]";

        /// <summary>
        /// Gets or sets the name of the stream page table, can include schema and database name.
        /// </summary>
        public string StreamPageTableName { get; set; } = "[dbo].[StreamPages]";
    }

    /// <summary>
    /// Represents the settings for SQL Server bulk copy operations.
    /// </summary>
    public class SqlServerBulkCopySettings
    {
        /// <summary>
        /// Gets or sets the batch size for bulk copy operations.
        /// </summary>
        public int BatchSize { get; set; } = 5000;

        internal int NotifyAfter { get; set; } = 10000;

        /// <summary>
        /// Gets or sets a value indicating whether streaming is enabled for bulk copy operations.
        /// </summary>
        public bool EnableStreaming { get; set; } = true;

        /// <summary>
        /// Gets or sets the timeout duration for bulk copy operations.
        /// </summary>
        public TimeSpan Timeout { get; set; } = TimeSpan.FromSeconds(60);
    }
}
