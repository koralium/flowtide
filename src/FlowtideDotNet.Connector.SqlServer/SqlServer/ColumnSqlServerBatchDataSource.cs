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

using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Core.Operators.Read;
using FlowtideDotNet.SqlServer.SqlServer;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Tests.SqlServer;
using Microsoft.Data.SqlClient;
using Polly;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Connector.SqlServer.SqlServer
{
    internal class ColumnSqlServerBatchDataSource : ColumnBatchReadBaseOperator
    {
        private readonly SqlServerSourceOptions _options;
        private readonly HashSet<string> _watermarks;
        private readonly string _displayName;
        private readonly ReadRelation _readRelation;

        private List<string>? _primaryKeys;
        private List<int>? _primaryKeyOrdinals;
        private List<Action<SqlDataReader, IColumn>>? _convertFunctions;

        private readonly string? _filter;
        private readonly string _fullTableName;
        private IObjectState<SqlServerState>? _state;

        public ColumnSqlServerBatchDataSource(SqlServerSourceOptions sourceOptions, ReadRelation readRelation, IFunctionsRegister functionsRegister, DataflowBlockOptions options) : base(readRelation, functionsRegister, options)
        {
            _options = sourceOptions;
            _readRelation = readRelation;
            var namedTable = _options.TableNameTransform?.Invoke(readRelation) ?? readRelation.NamedTable.Names;
            _fullTableName = string.Join('.', namedTable);
            _watermarks = [_fullTableName];
            _displayName = $"SqlServer-{_fullTableName}";

            if (sourceOptions.IsChangeTrackingEnabled)
            {
                base.DeltaLoadInterval = sourceOptions.ChangeTrackingInterval;
            }
            else
            {
                base.DeltaLoadInterval = null;
            }

            base.FullLoadInterval = sourceOptions.FullReloadInterval;

            if (readRelation.Filter != null)
            {
                var filterVisitor = new SqlServerFilterVisitor(readRelation);
                var filterResult = filterVisitor.Visit(readRelation.Filter, default);
                if (filterResult != null)
                {
                    if (filterResult.IsBoolean)
                    {
                        _filter = filterResult.Content;
                    }
                    else
                    {
                        _filter = $"{filterResult.Content} = 1";
                    }
                }
            }
        }

        public override string DisplayName => _displayName;

        protected override async Task InitializeOrRestore(long restoreTime, IStateManagerClient stateManagerClient)
        {
            using var connection = new SqlConnection(_options.ConnectionStringFunc());
            await connection.OpenAsync();

            if (_options.IsView)
            {
                _primaryKeys = _readRelation.BaseSchema.Names;
            }
            else
            {
                _primaryKeys = await SqlServerUtils.GetPrimaryKeys(connection, _fullTableName);
            }

            _primaryKeyOrdinals ??= [];
            foreach (var key in _primaryKeys)
            {
                var foundKey = false;
                for (int i = 0; i < _readRelation.BaseSchema.Names.Count; i++)
                {
                    if (_readRelation.BaseSchema.Names[i].Equals(key, StringComparison.OrdinalIgnoreCase))
                    {
                        foundKey = true;
                        _primaryKeyOrdinals.Add(i);
                    }
                }

                if (!foundKey)
                {
                    throw new InvalidOperationException($"Primary key ordinal not found for '{key}'");
                }
            }

            using var command = connection.CreateCommand();
            command.CommandText = SqlServerUtils.CreateSelectStatementTop1(_readRelation);

            using var reader = await command.ExecuteReaderAsync();
            var schema = await reader.GetColumnSchemaAsync();
            _convertFunctions = SqlServerUtils.GetColumnEventCreator(schema);

            _state = await stateManagerClient.GetOrCreateObjectStateAsync<SqlServerState>("sqlserver_state");

            _state.Value ??= new SqlServerState
            {
                ChangeTrackingVersion = 0
            };

            await base.InitializeOrRestore(restoreTime, stateManagerClient);
        }

        protected override async Task Checkpoint(long checkpointTime)
        {
            Debug.Assert(_state != null);
            await _state.Commit();
        }

        protected override async IAsyncEnumerable<DeltaReadEvent> DeltaLoad(Func<Task> EnterCheckpointLock, Action ExitCheckpointLock, CancellationToken cancellationToken, [EnumeratorCancellation] CancellationToken enumeratorCancellationToken = default)
        {
            Debug.Assert(_options != null);
            Debug.Assert(_state?.Value != null);
            Debug.Assert(_state?.Value.ChangeTrackingVersion != null);
            Debug.Assert(_primaryKeys != null);
            Debug.Assert(_primaryKeyOrdinals != null);
            Debug.Assert(_convertFunctions != null);

            Logger.SelectingChanges(_fullTableName, StreamName, Name);

            var linkedCancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, enumeratorCancellationToken);
            await EnterCheckpointLock();

            InitializeBatchCollections(out PrimitiveList<int> weights, out PrimitiveList<uint> iterations, out Column[] columns);

            var elementCount = 0;

            var context = ResilienceContextPool.Shared.Get(linkedCancellation.Token);
            var resilienceState = new DeltaLoadResilienceState(_state, _options, _readRelation, _primaryKeys);
            var pipelineResult = await _options.ResiliencePipeline.ExecuteOutcomeAsync(static async (ctx, state) =>
            {
                try
                {
                    Debug.Assert(state?.State.Value?.ChangeTrackingVersion != null);

                    var connection = new SqlConnection(state.Options.ConnectionStringFunc());
                    await connection.OpenAsync();

                    var command = connection.CreateCommand();
                    command.CommandText = SqlServerUtils.CreateChangesSelectStatement(state.ReadRelation, state.PrimaryKeys);
                    command.Parameters.AddWithValue("ChangeVersion", state.State.Value.ChangeTrackingVersion);

                    var reader = await command.ExecuteReaderAsync(ctx.CancellationToken);
                    return Outcome.FromResult(new ResilienceResult(reader, connection, command));
                }
                catch (Exception ex)
                {
                    return Outcome.FromException<ResilienceResult>(ex);
                }

            }, context, resilienceState);

            ResilienceContextPool.Shared.Return(context);

            pipelineResult.ThrowIfException();
            Debug.Assert(pipelineResult.Result != null);
            var reader = pipelineResult.Result.Reader;

            var versionOrdinal = reader.GetOrdinal("SYS_CHANGE_VERSION");
            var operationOrdinal = reader.GetOrdinal("SYS_CHANGE_OPERATION");

            while (await reader.ReadAsync())
            {
                linkedCancellation.Token.ThrowIfCancellationRequested();
                elementCount++;
                for (int i = 0; i < columns.Length; i++)
                {
                    _convertFunctions[i](reader, columns[i]);
                }

                var changeVersion = reader.GetInt64(versionOrdinal);
                var operation = reader.GetString(operationOrdinal);

                iterations.Add(0);
                switch (operation)
                {
                    case "D":
                        weights.Add(-1);
                        break;
                    case "I":
                    case "U":
                        weights.Add(1);
                        break;
                    default:
                        break;
                }

                _state.Value.ChangeTrackingVersion = changeVersion;

                if (weights.Count >= 100)
                {
                    var eventBatchData = new EventBatchData(columns);
                    var weightedBatch = new EventBatchWeighted(weights, iterations, eventBatchData);
                    Logger.ChangesFoundInTable(weights.Count, _fullTableName, StreamName, Name);
                    yield return new DeltaReadEvent(weightedBatch, new Base.Watermark(_readRelation.NamedTable.DotSeperated, _state.Value.ChangeTrackingVersion));
                    InitializeBatchCollections(out weights, out iterations, out columns);
                }
            }

            await pipelineResult.Result.DisposeAsync();


            if (weights.Count > 0)
            {
                var eventBatchData = new EventBatchData(columns);
                var weightedBatch = new EventBatchWeighted(weights, iterations, eventBatchData);
                Logger.ChangesFoundInTable(weights.Count, _fullTableName, StreamName, Name);
                yield return new DeltaReadEvent(weightedBatch, new Base.Watermark(_readRelation.NamedTable.DotSeperated, _state.Value.ChangeTrackingVersion));
            }
            else
            {
                weights.Dispose();
                iterations.Dispose();
            }

            ExitCheckpointLock();
            linkedCancellation.Dispose();
        }


        protected override async IAsyncEnumerable<ColumnReadEvent> FullLoad(CancellationToken cancellationToken, [EnumeratorCancellation] CancellationToken enumeratorCancellationToken = default)
        {
            Debug.Assert(_state != null);
            Debug.Assert(_state.Value?.ChangeTrackingVersion != null);
            Debug.Assert(_primaryKeys != null);
            Debug.Assert(_primaryKeyOrdinals != null);
            Debug.Assert(_convertFunctions != null);

            Logger.SelectingAllData(_fullTableName, StreamName, Name);

            var linkedCancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, enumeratorCancellationToken);

            InitializeBatchCollections(out PrimitiveList<int> weights, out PrimitiveList<uint> iterations, out Column[] columns);

            var primaryKeyValues = new Dictionary<string, object>();

            if (!_options.IsChangeTrackingEnabled && _state.Value.ChangeTrackingVersion < 1)
            {
                using var connection = new SqlConnection(_options.ConnectionStringFunc());
                await connection.OpenAsync(linkedCancellation.Token);
                _state.Value.ChangeTrackingVersion = await SqlServerUtils.GetLatestChangeVersion(connection);
            }
            else if (!_options.IsChangeTrackingEnabled)
            {
                // when reading from a view or a table without change tracking, use the server timestamp as the version identifier
                using var connection = new SqlConnection(_options.ConnectionStringFunc());
                await connection.OpenAsync(linkedCancellation.Token);
                _state.Value.ChangeTrackingVersion = await SqlServerUtils.GetServerTimestamp(connection);
            }

            var batchSize = 10000;
            while (true)
            {
                linkedCancellation.Token.ThrowIfCancellationRequested();

                var context = ResilienceContextPool.Shared.Get(linkedCancellation.Token);
                var resilienceState = new FullLoadResilienceState(
                    _state,
                    _options,
                    _readRelation,
                    _primaryKeys,
                    primaryKeyValues.Count > 0,
                    batchSize,
                    _filter,
                    primaryKeyValues);

                var pipelineResult = await _options.ResiliencePipeline.ExecuteOutcomeAsync(static async (ctx, state) =>
                {
                    Debug.Assert(state?.State.Value?.ChangeTrackingVersion != null);

                    try
                    {
                        var connection = new SqlConnection(state.Options.ConnectionStringFunc());
                        await connection.OpenAsync();

                        var command = connection.CreateCommand();
                        command.CommandText = SqlServerUtils.CreateInitialSelectStatement(state.ReadRelation, state.PrimaryKeys, state.BatchSize, state.IncludePkParameters, state.Filter);

                        foreach (var pk in state.PrimaryKeyValues)
                        {
                            command.Parameters.AddWithValue($"@{pk.Key}", pk.Value);
                        }

                        var reader = await command.ExecuteReaderAsync(ctx.CancellationToken);

                        return Outcome.FromResult(new ResilienceResult(reader, connection, command));
                    }
                    catch (Exception ex)
                    {
                        return Outcome.FromException<ResilienceResult>(ex);
                    }

                }, context, resilienceState);

                ResilienceContextPool.Shared.Return(context);

                pipelineResult.ThrowIfException();
                Debug.Assert(pipelineResult.Result != null);
                var reader = pipelineResult.Result.Reader;

                int elementCount = 0;

                while (await reader.ReadAsync(linkedCancellation.Token))
                {
                    elementCount++;
                    weights.Add(1);
                    iterations.Add(0);

                    primaryKeyValues.Clear();
                    for (int i = 0; i < _primaryKeyOrdinals.Count; i++)
                    {
                        primaryKeyValues.Add(_primaryKeys[i], reader.GetValue(_primaryKeyOrdinals[i]));
                    }

                    for (int i = 0; i < columns.Length; i++)
                    {
                        _convertFunctions[i](reader, columns[i]);
                    }

                    if (weights.Count >= 100)
                    {
                        var eventBatchData = new EventBatchData(columns);
                        var weightedBatch = new EventBatchWeighted(weights, iterations, eventBatchData);
                        yield return new ColumnReadEvent(weightedBatch, _state.Value.ChangeTrackingVersion);
                        InitializeBatchCollections(out weights, out iterations, out columns);
                    }
                }

                await pipelineResult.Result.DisposeAsync();

                if (elementCount != batchSize)
                {
                    break;
                }
            }

            if (weights.Count > 0)
            {
                var eventBatchData = new EventBatchData(columns);
                var weightedBatch = new EventBatchWeighted(weights, iterations, eventBatchData);
                yield return new ColumnReadEvent(weightedBatch, _state.Value.ChangeTrackingVersion);
            }
            else
            {
                weights.Dispose();
                iterations.Dispose();
            }

            linkedCancellation.Dispose();
        }

        protected override async Task OnCheckpoint(long checkpointTime)
        {
            Debug.Assert(_state != null);
            await _state.Commit();
            await base.OnCheckpoint(checkpointTime);
        }

        private void InitializeBatchCollections(out PrimitiveList<int> weights, out PrimitiveList<uint> iterations, out Column[] columns)
        {
            weights = new PrimitiveList<int>(MemoryAllocator);
            iterations = new PrimitiveList<uint>(MemoryAllocator);
            columns = new Column[_readRelation.BaseSchema.Names.Count];
            for (int i = 0; i < columns.Length; i++)
            {
                columns[i] = Column.Create(MemoryAllocator);
            }
        }

        protected override async ValueTask<List<int>> GetPrimaryKeyColumns()
        {
            Debug.Assert(_primaryKeyOrdinals != null);
            return _primaryKeyOrdinals;
        }

        protected override Task<IReadOnlySet<string>> GetWatermarkNames()
        {
            return Task.FromResult<IReadOnlySet<string>>(_watermarks);
        }


        private sealed record DeltaLoadResilienceState(
            IObjectState<SqlServerState> State,
            SqlServerSourceOptions Options,
            ReadRelation ReadRelation,
            List<string> PrimaryKeys)
        {
        }

        private sealed record FullLoadResilienceState(
            IObjectState<SqlServerState> State,
            SqlServerSourceOptions Options,
            ReadRelation ReadRelation,
            List<string> PrimaryKeys,
            bool IncludePkParameters,
            int BatchSize,
            string? Filter,
            Dictionary<string, object> PrimaryKeyValues)
        {
        }

        private sealed record ResilienceResult(SqlDataReader Reader, SqlConnection Connection, SqlCommand Command) : IAsyncDisposable
        {
            public async ValueTask DisposeAsync()
            {
                await Reader.DisposeAsync();
                await Connection.DisposeAsync();
                await Command.DisposeAsync();
            }
        }
    }
}
