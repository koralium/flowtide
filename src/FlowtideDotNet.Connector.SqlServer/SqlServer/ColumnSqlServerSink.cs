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
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.Operators.Write.Column;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Tests.SqlServer;
using Microsoft.Data.SqlClient;
using System.Data;
using System.Diagnostics;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Connector.SqlServer.SqlServer
{
    class ColumnSqlServerSink : ColumnGroupedWriteOperator
    {
        private readonly SqlServerSinkOptions m_sqlServerSinkOptions;
        private readonly WriteRelation m_writeRelation;
        private readonly Func<string> m_connectionStringFunc;
        private readonly string m_tmpTableName;
        private readonly DataValueContainer m_dataValueContainer;
        private SqlConnection? m_connection;
        private IReadOnlyList<int>? m_primaryKeys;
        private DataTable? m_dataTable;
        private SqlBulkCopy? m_sqlBulkCopy;
        private SqlCommand? m_mergeIntoCommand;
        private Action<DataRow, bool, EventBatchData, int>? m_mapRowFunc;
        private readonly bool m_gotCustomTmpTableName = false;
        private long m_committedCheckpointVersion = -1;

        public ColumnSqlServerSink(
            SqlServerSinkOptions sqlServerSinkOptions,
            WriteRelation writeRelation,
            ExecutionDataflowBlockOptions executionDataflowBlockOptions)
            : base(sqlServerSinkOptions.ExecutionMode, writeRelation, executionDataflowBlockOptions)
        {
            this.m_connectionStringFunc = sqlServerSinkOptions.ConnectionStringFunc;
            this.m_sqlServerSinkOptions = sqlServerSinkOptions;
            this.m_writeRelation = writeRelation;
            m_dataValueContainer = new DataValueContainer();

            var customTableName = sqlServerSinkOptions.CustomBulkCopyDestinationTable?.Invoke(writeRelation.NamedObject.Names);
            m_gotCustomTmpTableName = customTableName != null;
            m_tmpTableName = customTableName ?? GetTmpTableName();
        }

        internal string GetTmpTableName()
        {
            return $"#tmp_{Guid.NewGuid().ToString("N")}";
        }

        public override string DisplayName => "SQL Server Sink";

        protected override void Checkpoint(long checkpointTime)
        {
        }

        protected override ValueTask<IReadOnlyList<int>> GetPrimaryKeyColumns()
        {
            Debug.Assert(m_primaryKeys != null);
            return new ValueTask<IReadOnlyList<int>>(m_primaryKeys);
        }

        private async Task LoadMetadata()
        {
            if (m_connection == null)
            {
                m_connection = new SqlConnection(m_connectionStringFunc());
                await m_connection.OpenAsync();
            }
            else
            {
                await m_connection.DisposeAsync();
                m_connection = new SqlConnection(m_connectionStringFunc());
                await m_connection.OpenAsync();
            }

            List<string>? m_primaryKeyNames = null;
            if (m_writeRelation.PrimaryKeyNames != null)
            {
                // Keys declared per table win over the sink option.
                m_primaryKeyNames = m_writeRelation.PrimaryKeyNames;
            }
#pragma warning disable CS0618 // Obsolete option must keep working until removed.
            else if (m_sqlServerSinkOptions.CustomPrimaryKeys != null)
            {
                m_primaryKeyNames = m_sqlServerSinkOptions.CustomPrimaryKeys;
            }
#pragma warning restore CS0618
            else
            {
                m_primaryKeyNames = await SqlServerUtils.GetPrimaryKeys(m_connection, m_writeRelation.NamedObject.DotSeperated);
            }

            var dbSchema = await SqlServerUtils.GetWriteTableSchema(m_connection, m_writeRelation);

            var (primaryKeyIndices, primaryKeyColumnNames) = SqlServerUtils.ResolvePrimaryKeyColumns(
                dbSchema.Select(x => x.ColumnName).ToList(),
                m_primaryKeyNames,
                m_writeRelation.NamedObject.DotSeperated);
            m_primaryKeys = primaryKeyIndices;

            m_dataTable = new DataTable();
            if (!m_gotCustomTmpTableName)
            {
                m_dataTable.Columns.Add("md_operation");
            }
            
            foreach (var column in dbSchema)
            {
                if (column.DataType == typeof(decimal))
                {
                    // explicit required for type "decimal", "money", and "numeric"
                    m_dataTable.Columns.Add(column.ColumnName, typeof(decimal));
                }
                else if (column.DataType == typeof(byte[]))
                {
                    // required for type varbinary
                    m_dataTable.Columns.Add(column.ColumnName, typeof(byte[]));
                }
                else if (column.DataType == typeof(Guid))
                {
                    m_dataTable.Columns.Add(column.ColumnName, typeof(Guid));
                }
                else if (column.DataType == typeof(DateTime))
                {
                    m_dataTable.Columns.Add(column.ColumnName, typeof(DateTime));
                }
                else
                {
                    m_dataTable.Columns.Add(column.ColumnName);
                }
            }

            if (m_sqlServerSinkOptions.OnDataTableCreation != null)
            {
                await m_sqlServerSinkOptions.OnDataTableCreation(m_dataTable, m_tmpTableName, m_writeRelation.NamedObject.Names);
            }

            m_mapRowFunc = SqlServerUtils.GetDataRowFromColumnsFunc(dbSchema, m_primaryKeys, m_dataValueContainer, !m_gotCustomTmpTableName);
            m_sqlBulkCopy = new SqlBulkCopy(m_connection);
            m_sqlBulkCopy.DestinationTableName = m_tmpTableName;


            if (m_gotCustomTmpTableName)
            {
                var columns = await SqlServerUtils.GetColumns(m_connection, m_tmpTableName);
                var columnIndexMap = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
                for (int i = 0; i < columns.Count; i++)
                {
                    columnIndexMap[columns[i]] = i;
                }

                for (int c = 0; c < m_dataTable.Columns.Count; c++)
                {
                    var dataColumn = m_dataTable.Columns[c];
                    if (columnIndexMap.TryGetValue(dataColumn.ColumnName, out int columnIndex))
                    {
                        m_sqlBulkCopy.ColumnMappings.Add(c, columnIndex);
                    }
                    else
                    {
                        throw new InvalidOperationException($"Column '{dataColumn.ColumnName}' not found in destination table '{m_tmpTableName}'.");
                    }
                }
            }

            if (!m_gotCustomTmpTableName)
            {
                await SqlServerUtils.CreateTemporaryTable(m_connection, dbSchema, m_tmpTableName);
                m_mergeIntoCommand = m_connection.CreateCommand();
                var mergeIntoStatement = SqlServerUtils.CreateMergeIntoProcedure(m_tmpTableName, string.Join(".", m_writeRelation.NamedObject.Names.Select(x => $"[{x}]")), primaryKeyColumnNames.ToHashSet(), m_dataTable);
                m_mergeIntoCommand.CommandText = mergeIntoStatement;
                await m_mergeIntoCommand.PrepareAsync();
            }   
        }

        protected override async Task InitializeOrRestore(long restoreTime, IStateManagerClient stateManagerClient)
        {
            await LoadMetadata();
            await base.InitializeOrRestore(restoreTime, stateManagerClient);

            if (m_sqlServerSinkOptions.OnInitialize != null)
            {
                Debug.Assert(m_connection != null);
                await m_sqlServerSinkOptions.OnInitialize(m_connection, CurrentCheckpointId, restoreTime, m_tmpTableName, m_writeRelation.NamedObject.Names);
            }
        }

        public override Task CheckpointDone(long checkpointVersion)
        {
            // Only this stream has committed here, in a distributed stream the peer substreams
            // may not have. The commit hook waits for Compact.
            m_committedCheckpointVersion = checkpointVersion;
            return base.CheckpointDone(checkpointVersion);
        }

        public override async Task Compact()
        {
            if (m_sqlServerSinkOptions.OnCheckpointComplete != null &&
                m_committedCheckpointVersion >= 0)
            {
                // Runs on the checkpoint thread, the sink connection can be busy uploading.
                using var connection = new SqlConnection(m_connectionStringFunc());
                await connection.OpenAsync();
                await m_sqlServerSinkOptions.OnCheckpointComplete(connection, m_committedCheckpointVersion, m_tmpTableName, m_writeRelation.NamedObject.Names);
            }
            await base.Compact();
        }

        protected override async Task UploadChanges(IAsyncEnumerable<ColumnWriteOperation> rows, Watermark watermark, bool isInitialData, CancellationToken cancellationToken)
        {
            Debug.Assert(m_dataTable != null);
            Debug.Assert(m_mapRowFunc != null);
            Debug.Assert(m_sqlBulkCopy != null);
            Debug.Assert(m_connection != null);
            
            Logger.StartingDatabaseUpdate(StreamName, Name);
            
            await foreach (var row in rows)
            {
                var dataRow = m_dataTable.NewRow();
                m_mapRowFunc(dataRow, row.IsDeleted, row.EventBatchData, row.Index);
                if (m_sqlServerSinkOptions.ModifyRow != null)
                {
                    m_sqlServerSinkOptions.ModifyRow(dataRow, row.IsDeleted, watermark, CurrentCheckpointId, isInitialData, m_tmpTableName, m_writeRelation.NamedObject.Names);
                }
                m_dataTable.Rows.Add(dataRow);

                if (m_dataTable.Rows.Count > 1_000)
                {
                    await m_sqlBulkCopy.WriteToServerAsync(m_dataTable);
                    
                    if (m_mergeIntoCommand != null)
                    {
                        await m_mergeIntoCommand.ExecuteNonQueryAsync();
                    }

                    m_dataTable.Rows.Clear();
                }
            }
            if (m_dataTable.Rows.Count > 0)
            {
                await m_sqlBulkCopy.WriteToServerAsync(m_dataTable);

                if (m_mergeIntoCommand != null)
                {
                    await m_mergeIntoCommand.ExecuteNonQueryAsync();
                }

                m_dataTable.Rows.Clear();
            }

            if (m_sqlServerSinkOptions.OnDataUploaded != null)
            {
                await m_sqlServerSinkOptions.OnDataUploaded(m_connection, watermark, CurrentCheckpointId, isInitialData, m_tmpTableName, m_writeRelation.NamedObject.Names);
            }

            Logger.DatabaseUpdateComplete(StreamName, Name);
        }
    }
}
