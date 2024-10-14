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
    public class SqlServerPersistentStorageSettings
    {
        public required string ConnectionString { get; set; }

        public int WritePagesBulkLimit { get; set; } = 1500;

        public SqlServerBulkCopySettings BulkCopySettings { get; set; } = new();
    }

    public class SqlServerBulkCopySettings
    {
        public int BatchSize { get; set; } = 5000;

        public int NotifyAfter { get; set; } = 10000;

        public string DestinationTableName { get; set; } = "StreamPages";

        public bool EnableStreaming { get; set; } = true;

        public TimeSpan Timeout { get; set; } = TimeSpan.FromSeconds(60);
    }
}
