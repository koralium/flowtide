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

using FlowtideDotNet.Base.Metrics;
using FlowtideDotNet.Base.Vertices.Ingress;
using FlowtideDotNet.Core;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.Memory;
using FlowtideDotNet.Core.ColumnStore.Utils;
using FlowtideDotNet.Core.Operators.Read;
using FlowtideDotNet.SqlServer.SqlServer;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Tests.SqlServer;
using Microsoft.Data.SqlClient;
using System.Diagnostics;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Connector.SqlServer.SqlServer
{
    internal class ColumnSqlServerDataSource : ReadBaseOperator<SqlServerState>
    {
        private readonly Func<string> connectionStringFunc;
        private readonly string _tableName;
        private readonly ReadRelation readRelation;
        private readonly HashSet<string> _watermarks;
        private SqlConnection? sqlConnection;
        private SqlServerState? _state;
        private Action<SqlDataReader, IColumn>[]? _convertFunctions;
        private Task? _changesTask;
        private string _displayName;
        private List<string>? primaryKeys;
        private ICounter<long>? _eventsCounter;
        private string? filter;
        private ICounter<long>? _eventsProcessed;

        public ColumnSqlServerDataSource(Func<string> connectionStringFunc, string tableName, ReadRelation readRelation, DataflowBlockOptions options) : base(options)
        {
            this.connectionStringFunc = connectionStringFunc;
            _tableName = tableName;
            this.readRelation = readRelation;

            _watermarks = new HashSet<string>() { _tableName };
            _displayName = "SqlServer-" + tableName;

            if (readRelation.Filter != null)
            {
                var filterVisitor = new SqlServerFilterVisitor(readRelation);
                var filterResult = filterVisitor.Visit(readRelation.Filter, default);
                if (filterResult != null)
                {
                    if (filterResult.IsBoolean)
                    {
                        filter = filterResult.Content;
                    }
                    else
                    {
                        filter = $"{filterResult.Content} = 1";
                    }
                }
            }
        }

        public override string DisplayName => _displayName;

        public override Task DeleteAsync()
        {
            return Task.CompletedTask;
        }

        public override Task OnTrigger(string triggerName, object? state)
        {
            if (triggerName == "change_tracking" && (_changesTask == null || _changesTask.IsCompleted))
            {
                // Fetch data using change tracking
                _changesTask = RunTask(FetchChanges);
            }
            return Task.CompletedTask;
        }

        private async Task FetchChanges(IngressOutput<StreamEventBatch> output, object? state)
        {
            Debug.Assert(_state != null);
            Debug.Assert(sqlConnection != null);
            Debug.Assert(_convertFunctions != null);
            Debug.Assert(primaryKeys != null);
            Debug.Assert(_eventsCounter != null);
            Debug.Assert(_eventsProcessed != null);

            await output.EnterCheckpointLock();

            IColumn[] outputColumns = new IColumn[_convertFunctions.Length];
            PrimitiveList<int> weights = new PrimitiveList<int>(GlobalMemoryManager.Instance);
            PrimitiveList<uint> iterations = new PrimitiveList<uint>(GlobalMemoryManager.Instance);

            for (int i = 0; i < _convertFunctions.Length; i++)
            {
                outputColumns[i] = ColumnFactory.Get(GlobalMemoryManager.Instance);
            }

            bool error = false;
            var previousChangeVersion = _state.ChangeTrackingVersion;
            try
            {
                using var command = sqlConnection.CreateCommand();
                command.CommandText = SqlServerUtils.CreateChangesSelectStatement(readRelation, primaryKeys);
                command.Parameters.Add(new SqlParameter("ChangeVersion", _state.ChangeTrackingVersion));
                using var reader = await command.ExecuteReaderAsync();

                var changeVersionOrdinal = reader.GetOrdinal("SYS_CHANGE_VERSION");
                var changeOpOrdinal = reader.GetOrdinal("SYS_CHANGE_OPERATION");
                long changeVersion = _state.ChangeTrackingVersion;

                while (await reader.ReadAsync())
                {
                    for (int i = 0; i < _convertFunctions.Length; i++)
                    {
                        _convertFunctions[i](reader, outputColumns[i]);
                    }
                    changeVersion = reader.GetInt64(changeVersionOrdinal);
                    var operation = reader.GetString(changeOpOrdinal);

                    iterations.Add(0);
                    switch (operation)
                    {
                        case "U":
                        case "I":
                            weights.Add(1);
                            break;
                        case "D":
                            weights.Add(-1);
                            break;
                    }
                }
                reader.Close();

                _state.ChangeTrackingVersion = changeVersion;
                SetHealth(true);
            }
            catch (Exception ex)
            {
                SetHealth(false);
                Logger.ExceptionFetchingChanges(ex, StreamName, Name);
                await sqlConnection.DisposeAsync();

                // Recreate the connection
                sqlConnection = new SqlConnection(connectionStringFunc());
                await sqlConnection.OpenAsync();
                _state.ChangeTrackingVersion = previousChangeVersion;
                error = true;
            }


            if (!error && weights.Count > 0)
            {
                _eventsCounter.Add(weights.Count);
                _eventsProcessed.Add(weights.Count);
                Logger.ChangesFoundInTable(weights.Count, _tableName, StreamName, Name);
                await output.SendAsync(new StreamEventBatch(new EventBatchWeighted(weights, iterations, new EventBatchData(outputColumns))));
                await output.SendWatermark(new FlowtideDotNet.Base.Watermark(_tableName, _state.ChangeTrackingVersion));
                this.ScheduleCheckpoint(TimeSpan.FromSeconds(1));
            }
            else
            {
                // Dispose columns, weights and iterations since they are not required.
                for(int i = 0; i < outputColumns.Length; i++)
                {
                    outputColumns[i].Dispose();
                }
                weights.Dispose();
                iterations.Dispose();
            }

            output.ExitCheckpointLock();
        }

        protected override Task<IReadOnlySet<string>> GetWatermarkNames()
        {
            return Task.FromResult<IReadOnlySet<string>>(_watermarks);
        }

        private async Task GetColumnTypes()
        {
            Debug.Assert(sqlConnection != null);
            using var command = sqlConnection.CreateCommand();
            command.CommandText = SqlServerUtils.CreateSelectStatementTop1(readRelation);
            using (var reader = await command.ExecuteReaderAsync())
            {
                var columnSchema = await reader.GetColumnSchemaAsync();
                _convertFunctions = SqlServerUtils.GetColumnEventCreator(columnSchema).ToArray();
            }

            primaryKeys = await SqlServerUtils.GetPrimaryKeys(sqlConnection, _tableName);
        }

        protected override async Task InitializeOrRestore(long restoreTime, SqlServerState? state, IStateManagerClient stateManagerClient)
        {
            if (_eventsCounter == null)
            {
                _eventsCounter = Metrics.CreateCounter<long>("events");
            }
            if (_eventsProcessed == null)
            {
                _eventsProcessed = Metrics.CreateCounter<long>("events_processed");
            }
            Logger.InitializingSqlServerSource(_tableName, StreamName, Name);
            if (state == null)
            {
                state = new SqlServerState()
                {
                    ChangeTrackingVersion = -1
                };
            }
            _state = state;
            sqlConnection = new SqlConnection(connectionStringFunc());
            await sqlConnection.OpenAsync();
            await GetColumnTypes();
        }

        internal List<string> GetPrimaryKeys()
        {
            using var conn = new SqlConnection(connectionStringFunc());
            conn.Open();
            return SqlServerUtils.GetPrimaryKeys(conn, _tableName).GetAwaiter().GetResult();
        }

        internal bool IsChangeTrackingEnabled()
        {
            using var conn = new SqlConnection(connectionStringFunc());
            conn.Open();
            return SqlServerUtils.IsChangeTrackingEnabled(conn, _tableName).GetAwaiter().GetResult();
        }

        protected override Task<SqlServerState> OnCheckpoint(long checkpointTime)
        {
            Debug.Assert(_state != null);
            return Task.FromResult(_state);
        }

        protected override async Task SendInitial(IngressOutput<StreamEventBatch> output)
        {
            Debug.Assert(_state != null);
            Debug.Assert(sqlConnection != null);
            Debug.Assert(_convertFunctions != null);
            Debug.Assert(primaryKeys != null);
            Debug.Assert(_eventsCounter != null);
            Debug.Assert(_eventsProcessed != null);

            if (_state.ChangeTrackingVersion < 0)
            {
                Logger.SelectingAllData(_tableName, StreamName, Name);
                await output.EnterCheckpointLock();

                IColumn[] outputColumns = new IColumn[_convertFunctions.Length];
                PrimitiveList<int> weights = new PrimitiveList<int>(GlobalMemoryManager.Instance);
                PrimitiveList<uint> iterations = new PrimitiveList<uint>(GlobalMemoryManager.Instance);

                for (int i = 0; i < _convertFunctions.Length; i++)
                {
                    outputColumns[i] = ColumnFactory.Get(GlobalMemoryManager.Instance);
                }

                List<EventBatchWeighted> weightedBatches = new List<EventBatchWeighted>();

                // Get current change tracking version
                _state.ChangeTrackingVersion = await SqlServerUtils.GetLatestChangeVersion(sqlConnection);

                Dictionary<string, object> primaryKeyValues = new Dictionary<string, object>();

                int batchSize = 10000;
                int retryCount = 0;
                while (true)
                {
                    try
                    {
                        using var command = sqlConnection.CreateCommand();
                        if (primaryKeyValues.Count == 0)
                        {
                            command.CommandText = SqlServerUtils.CreateInitialSelectStatement(readRelation, primaryKeys, batchSize, false, filter);
                        }
                        else
                        {
                            command.CommandText = SqlServerUtils.CreateInitialSelectStatement(readRelation, primaryKeys, batchSize, true, filter);
                            foreach (var pk in primaryKeyValues)
                            {
                                command.Parameters.Add(new SqlParameter(pk.Key, pk.Value));
                            }
                        }

                        using var reader = await command.ExecuteReaderAsync(output.CancellationToken);

                        // Can probably cache this in the operator to skip memory allocation
                        List<int> primaryKeyOrdinals = new List<int>();
                        foreach (var pk in primaryKeys)
                        {
                            primaryKeyOrdinals.Add(reader.GetOrdinal(pk));
                        }

                        int elementCount = 0;

                        while (await reader.ReadAsync())
                        {
                            elementCount++;
                            weights.Add(1);
                            iterations.Add(0);
                            for (int i = 0; i < _convertFunctions.Length; i++)
                            {
                                _convertFunctions[i](reader, outputColumns[i]);
                            }
                            
                            primaryKeyValues.Clear();
                            for (int i = 0; i < primaryKeyOrdinals.Count; i++)
                            {
                                primaryKeyValues.Add(primaryKeys[i], reader.GetValue(primaryKeyOrdinals[i]));
                            }

                            if (weights.Count >= 100)
                            {
                                weightedBatches.Add(new EventBatchWeighted(weights, iterations, new EventBatchData(outputColumns)));
                                weights = new PrimitiveList<int>(GlobalMemoryManager.Instance);
                                iterations = new PrimitiveList<uint>(GlobalMemoryManager.Instance);
                                outputColumns = new IColumn[_convertFunctions.Length];
                                for (int i = 0; i < _convertFunctions.Length; i++)
                                {
                                    outputColumns[i] = ColumnFactory.Get(GlobalMemoryManager.Instance);
                                }
                            }
                        }

                        if (weights.Count > 0)
                        {
                            weightedBatches.Add(new EventBatchWeighted(weights, iterations, new EventBatchData(outputColumns)));
                            weights = new PrimitiveList<int>(GlobalMemoryManager.Instance);
                            iterations = new PrimitiveList<uint>(GlobalMemoryManager.Instance);
                            outputColumns = new IColumn[_convertFunctions.Length];
                            for (int i = 0; i < _convertFunctions.Length; i++)
                            {
                                outputColumns[i] = ColumnFactory.Get(GlobalMemoryManager.Instance);
                            }
                        }

                        foreach(var weightedBatch in weightedBatches)
                        {
                            _eventsCounter.Add(weightedBatch.Count);
                            _eventsProcessed.Add(weightedBatch.Count);
                            await output.SendAsync(new StreamEventBatch(weightedBatch));
                        }
                        weightedBatches.Clear();

                        retryCount = 0;
                        SetHealth(true);

                        if (elementCount != batchSize)
                        {
                            break;
                        }
                    }
                    catch (Exception e)
                    {
                        SetHealth(false);
                        Logger.ErrorReadingData(e, _tableName, StreamName, Name);

                        var waitTime = TimeSpan.FromSeconds(retryCount * 15);
                        Logger.WaitingSeconds(waitTime.Seconds, StreamName, Name);
                        await Task.Delay(waitTime, output.CancellationToken);

                        retryCount++;
                        Logger.RetryingCount(retryCount, StreamName, Name);
                        await sqlConnection.DisposeAsync();


                        // Recreate the connection
                        sqlConnection = new SqlConnection(connectionStringFunc());
                        await sqlConnection.OpenAsync(output.CancellationToken);

                        if (retryCount == 5)
                        {
                            throw;
                        }
                    }
                }
#if DEBUG_WRITE
                allInput.WriteLine($"Initial Done");
                await allInput.FlushAsync();
#endif
                // Send watermark information after all initial data has been loaded
                await output.SendWatermark(new FlowtideDotNet.Base.Watermark(_tableName, _state.ChangeTrackingVersion));

                output.ExitCheckpointLock();
                // Schedule a checkpoint after all the data has been sent
                this.ScheduleCheckpoint(TimeSpan.FromSeconds(1));
            }

            await this.RegisterTrigger("change_tracking", TimeSpan.FromSeconds(1));
        }

        public override ValueTask DisposeAsync()
        {
            if (sqlConnection != null)
            {
                return sqlConnection.DisposeAsync();
            }
            return ValueTask.CompletedTask;
        }
    }
}
