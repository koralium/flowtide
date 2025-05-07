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
        public List<string>? CustomPrimaryKeys { get; set; }

        /// <summary>
        /// If set to false, the sink will look at any database on the server that the connection string points to.
        /// </summary>
        public bool UseDatabaseDefinedInConnectionStringOnly { get; set; }

        public ExecutionMode ExecutionMode { get; set; } = ExecutionMode.Hybrid;

        /// <summary>
        /// If set, the sink writes data to the custom table and does not trigger any merge into to another table.
        /// For custom merge into logic with custom destination table you can use OnDataUploaded event to run any sql code.
        /// When using a custom destination table, the metadata if its an upsert or delete is not sent, that can be
        /// added manually using ModifyRow.
        /// </summary>
        public string? CustomBulkCopyDestinationTable { get; set; }

        /// <summary>
        /// Allows adding extra columns to the data table that will be bulk uploaded
        /// </summary>
        public Func<DataTable, ValueTask>? OnDataTableCreation { get; set; }

        /// <summary>
        /// Allows modifying a data row adding extra metadata columns if required.
        /// First argument is the actual data row, the second is if it is a deletion row, third is the watermark, fourth is the checkpointId.
        /// The last argument is if this is the initial data upload.
        /// </summary>
        public Action<DataRow, bool, Watermark, long, bool>? ModifyRow { get; set; }

        /// <summary>
        /// Called when all data in a batch has been uploaded.
        /// First argument is the sql connection, second the watermark, third the checkpointId, fourth if it is the initial data upload or not.
        /// </summary>
        public Func<SqlConnection, Watermark, long, bool, ValueTask>? OnDataUploaded { get; set; }
    }
}
