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

using FlowtideDotNet.Base;
using FlowtideDotNet.Core.Operators.Write;
using Microsoft.Data.SqlClient;
using System.Data;

namespace FlowtideDotNet.Connector.SqlServer
{
    public class SqlServerSinkOptions
    {
        public required Func<string> ConnectionStringFunc { get; set; }

        /// <summary>
        /// Set custom primary keys for the table.
        /// If not set, the primary keys are collected from the database.
        /// </summary>
        [Obsolete("Declare the primary keys in the statement instead, 'INSERT INTO table PRIMARY KEY (c1, c2) SELECT ...'. The option applies the same keys to every table the sink handles, which does not work when a stream writes to more than one table.")]
        public List<string>? CustomPrimaryKeys { get; set; }

        /// <summary>
        /// If set to false, the sink will look at any database on the server that the connection string points to.
        /// </summary>
        public bool UseDatabaseDefinedInConnectionStringOnly { get; set; }

        public ExecutionMode ExecutionMode { get; set; } = ExecutionMode.Hybrid;

        /// <summary>
        /// Bulk copy table per destination, no merge, no md_operation column.
        /// </summary>
        public Func<IReadOnlyList<string>, string?>? CustomBulkCopyDestinationTable { get; set; }

        /// <summary>
        /// Adds extra columns, args: data table, tmp table, destination parts.
        /// </summary>
        public Func<DataTable, string, IReadOnlyList<string>, ValueTask>? OnDataTableCreation { get; set; }

        /// <summary>
        /// Modifies rows, args: deleted, watermark, checkpointId, initial, tmp table, destination.
        /// </summary>
        public Action<DataRow, bool, Watermark, long, bool, string, IReadOnlyList<string>>? ModifyRow { get; set; }

        /// <summary>
        /// After batch upload, args: watermark, checkpointId, initial, tmp table, destination.
        /// </summary>
        public Func<SqlConnection, Watermark, long, bool, string, IReadOnlyList<string>, ValueTask>? OnDataUploaded { get; set; }

        /// <summary>
        /// Runs each start: next checkpointId, last committed, tmp table, destination.
        /// </summary>
        public Func<SqlConnection, long, long, string, IReadOnlyList<string>, ValueTask>? OnInitialize { get; set; }

        /// <summary>
        /// Commit phase after checkpoint or clean stop, staging table required.
        /// </summary>
        public Func<SqlConnection, long, string, IReadOnlyList<string>, ValueTask>? OnCheckpointComplete { get; set; }
    }
}
