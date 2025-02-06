using FlowtideDotNet.Base;
using FlowtideDotNet.Core;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.Operators.Write;
using FlowtideDotNet.Core.Operators.Write.Column;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Tests.SqlServer;
using Microsoft.Data.SqlClient;
using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Connector.SqlServer.SqlServer
{
    //internal class SqlServerSinkState : ColumnWriteState
    //{
    //}

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
        private Action<DataTable, bool, EventBatchData, int>? m_mapRowFunc;

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
            m_tmpTableName = GetTmpTableName();
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
            if (m_sqlServerSinkOptions.CustomPrimaryKeys != null)
            {
                m_primaryKeyNames = m_sqlServerSinkOptions.CustomPrimaryKeys;
            }
            else
            {
                m_primaryKeyNames = await SqlServerUtils.GetPrimaryKeys(m_connection, m_writeRelation.NamedObject.DotSeperated);
            }

            var dbSchema = await SqlServerUtils.GetWriteTableSchema(m_connection, m_writeRelation);

            List<int> primaryKeyIndices = new List<int>();
            foreach (var primaryKey in m_primaryKeyNames)
            {
                int index = -1;
                for (int i = 0; i < dbSchema.Count; i++)
                {
                    if (dbSchema[i].ColumnName.Equals(primaryKey, StringComparison.OrdinalIgnoreCase))
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
            await SqlServerUtils.CreateTemporaryTable(m_connection, dbSchema, m_tmpTableName);

            m_dataTable = new DataTable();
            m_dataTable.Columns.Add("md_operation");
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

            m_mapRowFunc = SqlServerUtils.GetDataRowFromColumnsFunc(dbSchema, m_primaryKeys, m_dataValueContainer);
            var mergeIntoStatement = SqlServerUtils.CreateMergeIntoProcedure(m_tmpTableName, string.Join(".", m_writeRelation.NamedObject.Names.Select(x => $"[{x}]")), m_primaryKeyNames.ToHashSet(), m_dataTable);
            m_sqlBulkCopy = new SqlBulkCopy(m_connection);
            m_sqlBulkCopy.DestinationTableName = m_tmpTableName;

            m_mergeIntoCommand = m_connection.CreateCommand();
            m_mergeIntoCommand.CommandText = mergeIntoStatement;
            await m_mergeIntoCommand.PrepareAsync();
        }

        protected override async Task InitializeOrRestore(long restoreTime, IStateManagerClient stateManagerClient)
        {
            await LoadMetadata();
            await base.InitializeOrRestore(restoreTime, stateManagerClient);
        }

        protected override async Task UploadChanges(IAsyncEnumerable<ColumnWriteOperation> rows, Watermark watermark, CancellationToken cancellationToken)
        {
            Debug.Assert(m_dataTable != null);
            Debug.Assert(m_mapRowFunc != null);
            Debug.Assert(m_sqlBulkCopy != null);
            Debug.Assert(m_mergeIntoCommand != null);

            Logger.StartingDatabaseUpdate(StreamName, Name);

            await foreach(var row in rows)
            {
                m_mapRowFunc(m_dataTable, row.IsDeleted, row.EventBatchData, row.Index);

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
            Logger.DatabaseUpdateComplete(StreamName, Name);
        }
    }
}
