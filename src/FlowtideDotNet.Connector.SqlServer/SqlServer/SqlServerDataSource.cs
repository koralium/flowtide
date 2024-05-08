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

using FlowtideDotNet.Base.Vertices.Ingress;
using FlowtideDotNet.Core;
using FlowtideDotNet.Core.Operators.Read;
using FlowtideDotNet.Storage.StateManager;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using FlowtideDotNet.Substrait.Relations;
using System.Threading.Tasks.Dataflow;
using FlowtideDotNet.SqlServer.SqlServer;
using FlowtideDotNet.Base.Metrics;
using System.Diagnostics;
using FlowtideDotNet.Connector.SqlServer.SqlServer;

namespace FlowtideDotNet.Substrait.Tests.SqlServer
{
    internal class SqlServerState
    {
        public long ChangeTrackingVersion { get; set; }
    }
    internal class SqlServerDataSource : ReadBaseOperator<SqlServerState>
    {
#if DEBUG_WRITE
        private StreamWriter allInput;
#endif
        private readonly Func<string> connectionStringFunc;
        private readonly string _tableName;
        private readonly ReadRelation readRelation;
        private readonly HashSet<string> _watermarks;
        private SqlConnection? sqlConnection;
        private SqlServerState? _state;
        private Func<SqlDataReader, RowEvent>? _streamEventCreator;
        private Task? _changesTask;
        private string _displayName;
        private List<string>? primaryKeys;
        private ICounter<long>? _eventsCounter;
        private string? filter;
        private ICounter<long>? _eventsProcessed;

        public SqlServerDataSource(Func<string> connectionStringFunc, string tableName, ReadRelation readRelation, DataflowBlockOptions options) : base(options)
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
            
            //_streamEventCreator = SqlServerUtils.GetStreamEventCreator(readRelation);
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
            Debug.Assert(_streamEventCreator != null);
            Debug.Assert(primaryKeys != null);
            Debug.Assert(_eventsCounter != null);
            Debug.Assert(_eventsProcessed != null);

            await output.EnterCheckpointLock();

            List<RowEvent> result = new List<RowEvent>();
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
                    var streamEvent = _streamEventCreator(reader);
                    changeVersion = reader.GetInt64(changeVersionOrdinal);
                    var operation = reader.GetString(changeOpOrdinal);

                    switch (operation)
                    {
                        case "U":
                        case "I":
                            streamEvent.Weight = 1;
                            break;
                        case "D":
                            streamEvent.Weight = -1;
                            break;
                    }
#if DEBUG_WRITE
                    allInput.WriteLine($"{streamEvent.Weight} {streamEvent.Vector.ToJson}");
#endif
                    result.Add(streamEvent);
                }
                reader.Close();
#if DEBUG_WRITE
                await allInput.FlushAsync();
#endif
                _state.ChangeTrackingVersion = changeVersion;
                SetHealth(true);
            }
            catch(Exception ex)
            {
                SetHealth(false);
                Logger.ExceptionFetchingChanges(ex, StreamName, Name);
                await sqlConnection.DisposeAsync();

                // Recreate the connection
                sqlConnection = new SqlConnection(connectionStringFunc());
                await sqlConnection.OpenAsync();
                _state.ChangeTrackingVersion = previousChangeVersion;
                result.Clear();
            }
            

            if (result.Count > 0)
            {
                _eventsCounter.Add(result.Count);
                _eventsProcessed.Add(result.Count);
                Logger.ChangesFoundInTable(result.Count, _tableName, StreamName, Name);
                await output.SendAsync(new StreamEventBatch(result));
                await output.SendWatermark(new FlowtideDotNet.Base.Watermark(_tableName, _state.ChangeTrackingVersion));
                this.ScheduleCheckpoint(TimeSpan.FromSeconds(1));
            }
            
            output.ExitCheckpointLock();
        }

        protected override Task<IReadOnlySet<string>> GetWatermarkNames()
        {
            return Task.FromResult<IReadOnlySet<string>>(_watermarks);
        }

        protected override async Task InitializeOrRestore(long restoreTime, SqlServerState? state, IStateManagerClient stateManagerClient)
        {
#if DEBUG_WRITE
            allInput = File.CreateText($"{Name}.all.txt");
#endif
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

        private async Task GetColumnTypes()
        {
            Debug.Assert(sqlConnection != null);
            using var command = sqlConnection.CreateCommand();
            command.CommandText = SqlServerUtils.CreateSelectStatementTop1(readRelation);
            using (var reader = await command.ExecuteReaderAsync())
            {
                var columnSchema = await reader.GetColumnSchemaAsync();
                _streamEventCreator = SqlServerUtils.GetStreamEventCreator(columnSchema);
            }
                
            primaryKeys = await SqlServerUtils.GetPrimaryKeys(sqlConnection, _tableName);
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

        public override ValueTask DisposeAsync()
        {
            if (sqlConnection != null)
            {
                return sqlConnection.DisposeAsync();
            }
            return ValueTask.CompletedTask;
        }

        protected override Task<SqlServerState> OnCheckpoint(long checkpointTime)
        {
#if DEBUG_WRITE
            allInput.WriteLine("Checkpoint");
            allInput.Flush();
#endif
            Debug.Assert(_state != null);
            return Task.FromResult(_state);
        }

        protected override async Task SendInitial(IngressOutput<StreamEventBatch> output)
        {
#if DEBUG_WRITE
            allInput.WriteLine($"Initial");
#endif
            Debug.Assert(_state != null);
            Debug.Assert(sqlConnection != null);
            Debug.Assert(_streamEventCreator != null);
            Debug.Assert(primaryKeys != null);
            Debug.Assert(_eventsCounter != null);
            Debug.Assert(_eventsProcessed != null);
            
            // Check if we have never read the initial data before
            if (_state.ChangeTrackingVersion < 0)
            {
                Logger.SelectingAllData(_tableName, StreamName, Name);
                await output.EnterCheckpointLock();

                // Get current change tracking version
                _state.ChangeTrackingVersion = await SqlServerUtils.GetLatestChangeVersion(sqlConnection);

                Dictionary<string, object> primaryKeyValues = new Dictionary<string, object>();

                int batchSize = 10000;
                List<RowEvent> cache = new List<RowEvent>();
                int retryCount = 0;
                while(true)
                {
                    try
                    {
                        (cache, primaryKeyValues) = await SqlServerUtils.InitialSelect(readRelation, sqlConnection, primaryKeys, batchSize, primaryKeyValues, _streamEventCreator, filter, output.CancellationToken);

                        List<RowEvent> outdata = new List<RowEvent>();

                        foreach (var ev in cache)
                        {
                            outdata.Add(ev);

                            if (outdata.Count >= 100)
                            {
                                _eventsCounter.Add(outdata.Count);
                                _eventsProcessed.Add(outdata.Count);
                                await output.SendAsync(new StreamEventBatch(outdata));
                                outdata = new List<RowEvent>();
                            }
                        }
                        if (outdata.Count > 0)
                        {
                            _eventsCounter.Add(outdata.Count);
                            _eventsProcessed.Add(outdata.Count);
                            await output.SendAsync(new StreamEventBatch(outdata));
                        }
                        retryCount = 0;
                        SetHealth(true);

                        if (cache.Count != batchSize)
                        {
                            break;
                        }
                    }
                    catch(Exception e)
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
    }
}
