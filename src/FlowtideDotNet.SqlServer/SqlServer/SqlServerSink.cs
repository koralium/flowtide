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

using FlowtideDotNet.Core;
using FlowtideDotNet.Core.Operators.Write;
using FlowtideDotNet.Core.Storage;
using FlowtideDotNet.Storage.Serializers;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.Tree;
using Microsoft.Data.SqlClient;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Tests.SqlServer;
using System.Data;
using System.Threading.Tasks.Dataflow;
using Microsoft.Extensions.Logging;

namespace FlowtideDotNet.SqlServer.SqlServer
{
    public class SqlServerSinkState : IStatefulWriteState
    {
        public long StorageSegmentId { get; set; }
    }
    public class SqlServerSink : GroupedWriteBaseOperator<SqlServerSinkState>
    {
        private string tmpTableName;
        private readonly Func<string> connectionStringFunc;
        private readonly WriteRelation writeRelation;
        private IBPlusTree<StreamEvent, int> m_modified;
        private bool m_hasModified;
        private SqlConnection connection;
        private List<string> m_primaryKeyNames;
        private IReadOnlyList<int>? m_primaryKeys;
        private DataTable m_dataTable;
        private Action<DataTable, bool, StreamEvent> m_mapRowFunc;
        private string mergeIntoStatement;
        private SqlBulkCopy m_sqlBulkCopy;
        private SqlCommand m_mergeIntoCommand;

        public SqlServerSink(Func<string> connectionStringFunc, WriteRelation writeRelation, ExecutionDataflowBlockOptions executionDataflowBlockOptions) : base(executionDataflowBlockOptions)
        {
            this.connectionStringFunc = connectionStringFunc;
            this.writeRelation = writeRelation;
            tmpTableName = $"tmp_{Guid.NewGuid().ToString().Replace("-", "")}";
        }

        public override string DisplayName => "SQL Server Sink";

        protected override async Task<SqlServerSinkState> Checkpoint(long checkpointTime)
        {
            if (m_hasModified)
            {
                Logger.LogInformation("Starting database update");
                var iterator = m_modified.CreateIterator();

                await iterator.SeekFirst();

                // Iterate over all the values
                await foreach(var page in iterator)
                {
                    foreach(var kv in page)
                    {
                        var (rows, isDeleted) = await this.GetGroup(kv.Key);
                        
                        if (rows.Count > 1)
                        {
                            var lastRow = rows.Last();
                            m_mapRowFunc(m_dataTable, isDeleted, lastRow);
                        }
                        else if (rows.Count == 1)
                        {
                            m_mapRowFunc(m_dataTable, isDeleted, rows[0]);
                        }
                        else if (isDeleted)
                        {
                            m_mapRowFunc(m_dataTable, true, kv.Key);
                        }
                    }

                    if (m_dataTable.Rows.Count > 1_000)
                    {
                        await m_sqlBulkCopy.WriteToServerAsync(m_dataTable);
                        await m_mergeIntoCommand.ExecuteNonQueryAsync();

                        m_dataTable.Rows.Clear();
                    }
                    
                }

                if (m_dataTable.Rows.Count > 0)
                {
                    await m_sqlBulkCopy.WriteToServerAsync(m_dataTable);
                    await m_mergeIntoCommand.ExecuteNonQueryAsync();

                    m_dataTable.Rows.Clear();
                }

                // Clear the modified table
                await m_modified.Clear();
                m_hasModified = false;
                Logger.LogInformation("Database update complete");
            }
            return new SqlServerSinkState();
        }

        private async Task LoadMetadata()
        {
            if (connection == null)
            {
                connection = new SqlConnection(connectionStringFunc());
                await connection.OpenAsync();
            }
            else if (connection.State != ConnectionState.Open)
            {
                await connection.DisposeAsync();
                connection = new SqlConnection(connectionStringFunc());
                await connection.OpenAsync();
            }

            m_primaryKeyNames = await SqlServerUtils.GetPrimaryKeys(connection, writeRelation.NamedObject.DotSeperated);
            var dbSchema = await SqlServerUtils.GetWriteTableSchema(connection, writeRelation);

            List<int> primaryKeyIndices = new List<int>();
            foreach(var primaryKey in m_primaryKeyNames)
            {
                int index = -1;
                for (int i = 0; i < dbSchema.Count; i++)
                {
                    if ( dbSchema[i].ColumnName.Equals(primaryKey, StringComparison.OrdinalIgnoreCase))
                    {
                        index = i;
                    }
                }
                if (index == -1)
                {
                    throw new InvalidOperationException("All primary keys of the sink table must be sent to the sink operator.");
                }
                primaryKeyIndices.Add(index);
            }
            m_primaryKeys = primaryKeyIndices;
            await SqlServerUtils.CreateTemporaryTable(connection, dbSchema, tmpTableName);

            m_dataTable = new DataTable();
            m_dataTable.Columns.Add("md_operation");
            foreach (var column in dbSchema)
            {
                m_dataTable.Columns.Add(column.ColumnName);
            }

            m_mapRowFunc = SqlServerUtils.GetDataRowMapFunc(dbSchema, m_primaryKeys);
            mergeIntoStatement = SqlServerUtils.CreateMergeIntoProcedure(tmpTableName, writeRelation.NamedObject.DotSeperated, m_primaryKeyNames.ToHashSet(), m_dataTable);
            m_sqlBulkCopy = new SqlBulkCopy(connection);
            m_sqlBulkCopy.DestinationTableName = tmpTableName;

            m_mergeIntoCommand = connection.CreateCommand();
            m_mergeIntoCommand.CommandText = mergeIntoStatement;
            await m_mergeIntoCommand.PrepareAsync();
        }

        protected override async ValueTask<IReadOnlyList<int>> GetPrimaryKeyColumns()
        {
            if (m_primaryKeys == null)
            {
                await LoadMetadata();
            }
            return m_primaryKeys!;
        }

        protected override async Task Initialize(long restoreTime, SqlServerSinkState? state, IStateManagerClient stateManagerClient)
        {
            if (m_mapRowFunc == null || (connection != null && connection.State != ConnectionState.Open))
            {
                await LoadMetadata();
            }
            // Create a tree for storing modified data.
            m_modified = await stateManagerClient.GetOrCreateTree<StreamEvent, int>("temporary", new Storage.Tree.BPlusTreeOptions<StreamEvent, int>()
            {
                Comparer = PrimaryKeyComparer,
                ValueSerializer = new IntSerializer(),
                KeySerializer = new StreamEventBPlusTreeSerializer()
            });
            // Clear the modified tree in case of a crash
            await m_modified.Clear();
        }

        protected override async Task OnRecieve(StreamEventBatch msg, long time)
        {
            foreach (var e in msg.Events)
            {
                // Add the row to permanent storage
                await this.Insert(e);
                m_hasModified = true;
                // Add the row to the modified storage to keep track on which rows where changed
                await m_modified.Upsert(e, 0);
            }
        }
    }
}
