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
        /// Selects a custom table that the sink bulk copies into, instead of creating a temporary table.
        /// The argument is the destination table name as a list of parts, which allows a different table
        /// to be picked per destination when the stream writes to more than one table.
        /// Return null to let the sink handle that destination in the default way, with a temporary table
        /// and a merge into statement.
        /// When a custom table is returned, no merge into is run against the destination table, custom merge
        /// into logic can be added with the OnDataUploaded event. The metadata if it's an upsert or delete is
        /// not sent either, that can be added manually using ModifyRow.
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
        /// The checkpointId is the checkpoint version the rows belong to, see the remarks on which checkpoint that is per execution mode.
        /// </summary>
        /// <remarks>
        /// The checkpointId is reused when the stream rolls back, so replayed rows carry the same value as before
        /// the rollback. That makes it safe to store in a column and use to overwrite or clean up replayed rows.
        /// In ExecutionMode.OnCheckpoint the rows are uploaded inside the checkpoint, so the id is the checkpoint
        /// that is about to make them durable. In ExecutionMode.OnWatermark, and in Hybrid once the initial data
        /// has been sent, the rows are uploaded between checkpoints, so the id is the checkpoint that will commit
        /// them next.
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
        /// The checkpointId is the checkpoint version the first upload after this hook belongs to, so it can be
        /// compared against ids stored by ModifyRow in a previous run.
        /// The last committed checkpointId is the newest checkpoint the stream restored from, so any data staged
        /// with a higher id belongs to an epoch that was rolled back.
        /// The destination table and any custom bulk copy table must already exist, their schemas are read before
        /// this hook runs, so it cannot be used to create them.
        /// This is called again on every restart of the stream, so the logic must be safe to run more than once.
        /// </summary>
        public Func<SqlConnection, long, long, string, IReadOnlyList<string>, ValueTask>? OnInitialize { get; set; }

        /// <summary>
        /// Called after the stream has durably committed a checkpoint, which makes it the commit phase of a two
        /// phase commit. Data staged during the checkpoint can be moved into its final place here, and it will
        /// never be committed for a checkpoint that the stream itself did not commit.
        /// First argument is a sql connection, second the checkpointId that was committed, third argument is the
        /// temporary table name, fourth argument is the destination table name as a list of parts.
        /// </summary>
        /// <remarks>
        /// The hook runs from the compaction step, which a stream running normally reaches only after every
        /// substream has durably committed the same checkpoint, since the substreams pair their cycles one to one.
        /// Committing on the checkpoint done notification instead would not be safe, that fires with no peer
        /// acknowledgement at all.
        /// A substream that is shutting down acknowledges once per stop drain cycle while the other side needs only
        /// the first stop barrier, so those extra acknowledgements can let a cycle complete slightly early. If that
        /// substream then fails instead of stopping cleanly, the stream can roll back below a version already
        /// committed here. The write operator replays every key touched since the restored checkpoint, so a commit
        /// that upserts and deletes by primary key overwrites those rows by itself. A commit that is not idempotent
        /// under a replay has to store the checkpointId with the committed rows and delete the rows above the last
        /// committed id from OnInitialize instead.
        /// The hook runs at most once per checkpoint, but the work it does must be idempotent, it is redone from
        /// OnInitialize whenever the stream cannot tell that it already ran. That covers a stop between writing the
        /// destination table and clearing the staged rows, a rolled back epoch that is replayed under the same
        /// checkpointId, and a commit that was interrupted part way through. Writing the destination table and
        /// clearing the staged rows in one transaction removes the first case, the rest need the destination write
        /// itself to be idempotent.
        /// This runs on the stream checkpoint thread rather than the operator thread, so the hook is given its own
        /// connection instead of the one the sink uploads with.
        /// The hook is not called again after a crash, and it is not called on a graceful stop. If the stream stops
        /// between the checkpoint being committed and this hook running, the staged data is still uncommitted on the
        /// next start, so the same work must be redone from OnInitialize using its last committed checkpointId
        /// argument. Without that step the delivery is not exactly once.
        /// Staging is only well defined with ExecutionMode.OnCheckpoint together with CustomBulkCopyDestinationTable.
        /// In the other execution modes rows are uploaded between checkpoints, and the default temporary table path
        /// merges into the destination table while it uploads, so there is nothing left to commit here.
        /// </remarks>
        public Func<SqlConnection, long, string, IReadOnlyList<string>, ValueTask>? OnCheckpointComplete { get; set; }
    }
}
