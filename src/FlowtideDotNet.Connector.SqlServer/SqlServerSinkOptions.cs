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
        /// Picks a table to bulk copy into instead of a temporary table, per destination.
        /// The argument is the destination table name as a list of parts. Return null for the default path.
        /// No merge into is run for a custom table, add it in OnDataUploaded. The upsert or delete
        /// metadata is not sent either, add it in ModifyRow.
        /// </summary>
        public Func<IReadOnlyList<string>, string?>? CustomBulkCopyDestinationTable { get; set; }

        /// <summary>
        /// Allows adding extra columns to the data table that will be bulk uploaded.
        /// First argument is the data table, second argument is the temporary table name,
        /// third argument is the destination table name as a list of parts.
        /// </summary>
        public Func<DataTable, string, IReadOnlyList<string>, ValueTask>? OnDataTableCreation { get; set; }

        /// <summary>
        /// Allows modifying a data row adding extra metadata columns if required.
        /// First argument is the actual data row, the second is if it is a deletion row, third is the watermark, fourth is the checkpointId.
        /// Fifth argument is if this is the initial data upload.
        /// Sixth argument is the temporary table name, seventh argument is the destination table name as a list of parts.
        /// </summary>
        /// <remarks>
        /// The checkpointId is reused on rollback, so replayed rows carry the same value. Safe to store in a column.
        /// In OnCheckpoint mode it is the checkpoint about to make the rows durable. In OnWatermark, and in
        /// Hybrid after the initial data, it is the checkpoint that commits them next.
        /// </remarks>
        public Action<DataRow, bool, Watermark, long, bool, string, IReadOnlyList<string>>? ModifyRow { get; set; }

        /// <summary>
        /// Called when all data in a batch has been uploaded.
        /// First argument is the sql connection, second the watermark, third the checkpointId, fourth if it is the initial data upload or not.
        /// Fifth argument is the temporary table name, sixth argument is the destination table name as a list of parts.
        /// The checkpointId is the same value that was passed to ModifyRow for these rows, see its remarks.
        /// </summary>
        public Func<SqlConnection, Watermark, long, bool, string, IReadOnlyList<string>, ValueTask>? OnDataUploaded { get; set; }

        /// <summary>
        /// Called when the sink is initialized, before any data is uploaded.
        /// This can be used to delete old data, reconcile a two phase commit, or any other initialization logic.
        /// First argument is the sql connection, second the checkpointId, third the last committed checkpointId,
        /// fourth argument is the temporary table name, fifth argument is the destination table name as a list of parts.
        /// The checkpointId is what the first upload after this hook uses. Anything staged above the last
        /// committed one belongs to a rolled back epoch.
        /// The tables must already exist, their schemas are read before this runs.
        /// Called again on every restart, so it must be safe to run more than once.
        /// </summary>
        public Func<SqlConnection, long, long, string, IReadOnlyList<string>, ValueTask>? OnInitialize { get; set; }

        /// <summary>
        /// Called once the stream has durably committed a checkpoint, the commit phase of a two phase commit.
        /// Data staged during that checkpoint can be moved into place here.
        /// First argument is a sql connection, second the committed checkpointId, third the temporary table
        /// name, fourth the destination table name as a list of parts.
        /// </summary>
        /// <remarks>
        /// Runs from compaction, which a running stream reaches only after every substream committed the
        /// same checkpoint. The checkpoint done notification fires with no peer acknowledgement at all.
        /// The work must be idempotent, it is redone from OnInitialize whenever the stream cannot tell it
        /// already ran. Put the destination write and the staging cleanup in one transaction.
        /// Not called again after a crash, and not called on a graceful stop, so OnInitialize has to redo it.
        /// Runs on the checkpoint thread, so the hook gets its own connection.
        /// Only well defined with ExecutionMode.OnCheckpoint and CustomBulkCopyDestinationTable.
        /// </remarks>
        public Func<SqlConnection, long, string, IReadOnlyList<string>, ValueTask>? OnCheckpointComplete { get; set; }
    }
}
