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
        private readonly Func<SqlConnection, Task<List<string>>> _pkAction;
        private readonly Func<SqlConnection, Task<List<int>>> _pkOrdinalsAction;
        private readonly ReadRelation _readRelation;

        // todo: these are added as dictionary to act as a cache, should they be placed in another location or handled in another way?
        private readonly Dictionary<string, List<string>> _primaryKeys = [];
        private readonly Dictionary<string, List<int>> _primaryKeyOrdinals = [];
        private readonly Dictionary<string, Action<SqlDataReader, IColumn>[]> _convertFunctions = [];

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

            if (sourceOptions.ChangeTrackingInterval.HasValue && sourceOptions.IsChangeTrackingEnabled)
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

            if (_options.IsView)
            {
                _pkAction = (connection) => SqlServerUtils.GetColumns(connection, _fullTableName);
                _pkOrdinalsAction = (connection) => SqlServerUtils.GetColumnOrdinals(connection, _fullTableName);
            }
            else
            {
                _pkAction = (connection) => SqlServerUtils.GetPrimaryKeys(connection, _fullTableName);
                _pkOrdinalsAction = (connection) => SqlServerUtils.GetPrimaryKeyOrdinals(connection, _fullTableName);
            }
        }

        public override string DisplayName => _displayName;

        protected override async Task InitializeOrRestore(long restoreTime, IStateManagerClient stateManagerClient)
        {
            using var connection = new SqlConnection(_options.ConnectionStringFunc());
            await connection.OpenAsync();

            if (!_primaryKeys.ContainsKey(Name))
            {
                _primaryKeys.Add(Name, await _pkAction(connection));
            }

            if (!_primaryKeyOrdinals.ContainsKey(Name))
            {
                _primaryKeyOrdinals.Add(Name, [.. (await _pkOrdinalsAction(connection)).Select(s => s - 1)]);
            }

            if (!_convertFunctions.ContainsKey(Name))
            {
                using var command = connection.CreateCommand();
                command.CommandText = SqlServerUtils.CreateSelectStatementTop1(_readRelation);

                using var reader = await command.ExecuteReaderAsync();
                var schema = await reader.GetColumnSchemaAsync();
                _convertFunctions.Add(Name, [.. SqlServerUtils.GetColumnEventCreator(schema)]);
            }

            _state = await stateManagerClient.GetOrCreateObjectStateAsync<SqlServerState>("sqlserver_state");
            _state.Value ??= new SqlServerState
            {
                ChangeTrackingVersion = -1
            };

            await base.InitializeOrRestore(restoreTime, stateManagerClient);
        }

        protected override Task Checkpoint(long checkpointTime)
        {
            return Task.CompletedTask;
        }

        private sealed record DeltaLoadResilienceState(
            IObjectState<SqlServerState> State,
            SqlServerSourceOptions Options,
            ReadRelation ReadRelation,
            List<string> PrimaryKeys,
            int Offset,
            int BatchSize);

        private sealed record ResilienceResult(SqlDataReader Reader, SqlConnection Connection, SqlCommand Command) : IAsyncDisposable
        {
            public async ValueTask DisposeAsync()
            {
                await Reader.DisposeAsync();
                await Connection.DisposeAsync();
                await Command.DisposeAsync();
            }
        }

        protected override async IAsyncEnumerable<DeltaReadEvent> DeltaLoad(Func<Task> EnterCheckpointLock, Action ExitCheckpointLock, CancellationToken cancellationToken, [EnumeratorCancellation] CancellationToken enumeratorCancellationToken = default)
        {
            Debug.Assert(_options != null);
            Debug.Assert(_state?.Value != null);
            Debug.Assert(_state?.Value.ChangeTrackingVersion != null);
            Debug.Assert(_primaryKeys != null);
            Debug.Assert(_primaryKeyOrdinals != null);
            Debug.Assert(_convertFunctions != null);

            var linkedCancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, enumeratorCancellationToken);
            await EnterCheckpointLock();

            InitializeBatchCollections(out PrimitiveList<int> weights, out PrimitiveList<uint> iterations, out Column[] columns);

            var watermark = DateTime.UtcNow.Ticks;

            // todo: update CreateChangesSelectStatement to work with batching
            var batchSize = 10_000;
            var offset = 0;

            var elementCount = 0;

            while (true)
            {
                linkedCancellation.Token.ThrowIfCancellationRequested();

                var context = ResilienceContextPool.Shared.Get(linkedCancellation.Token);
                var pipelineResult = await _options.ResiliencePipeline.ExecuteOutcomeAsync(static async (ctx, state) =>
                {
                    try
                    {
                        Debug.Assert(state?.State.Value?.ChangeTrackingVersion != null);

                        var connection = new SqlConnection(state.Options.ConnectionStringFunc());
                        await connection.OpenAsync();

                        var command = connection.CreateCommand();
                        command.CommandText = SqlServerUtils.CreateChangesSelectStatement(state.ReadRelation, state.PrimaryKeys, state.Offset, state.BatchSize);
                        command.Parameters.AddWithValue("ChangeVersion", state.State.Value.ChangeTrackingVersion);

                        var reader = await command.ExecuteReaderAsync(ctx.CancellationToken);
                        return Outcome.FromResult(new ResilienceResult(reader, connection, command));
                    }
                    catch (Exception ex)
                    {
                        return Outcome.FromException<ResilienceResult>(ex);
                    }

                }, context,
                new DeltaLoadResilienceState(_state, _options, _readRelation, _primaryKeys[Name], offset, batchSize));
                ResilienceContextPool.Shared.Return(context);

                pipelineResult.ThrowIfException();
                Debug.Assert(pipelineResult.Result != null);
                var reader = pipelineResult.Result.Reader;

                var versionOrdinal = reader.GetOrdinal("SYS_CHANGE_VERSION");
                var operationOrdinal = reader.GetOrdinal("SYS_CHANGE_OPERATION");

                offset += batchSize;

                while (await reader.ReadAsync())
                {
                    elementCount++;
                    for (int i = 0; i < _convertFunctions.Count; i++)
                    {
                        _convertFunctions[Name][i](reader, columns[i]);
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

                    if (weights.Count > 100)
                    {
                        var eventBatchData = new EventBatchData(columns);
                        var weightedBatch = new EventBatchWeighted(weights, iterations, eventBatchData);
                        yield return new DeltaReadEvent(weightedBatch, new Base.Watermark(_readRelation.NamedTable.DotSeperated, watermark));
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
                yield return new DeltaReadEvent(weightedBatch, new Base.Watermark(_readRelation.NamedTable.DotSeperated, watermark));
                weights.Dispose();
                iterations.Dispose();
            }

            ExitCheckpointLock();
            linkedCancellation.Dispose();
        }

        private sealed record FullLoadResilienceState(
            IObjectState<SqlServerState> State,
            SqlServerSourceOptions Options,
            ReadRelation ReadRelation,
            List<string> PrimaryKeys,
            bool IncludePkParameters,
            int BatchSize,
            string? Filter,
            Dictionary<string, object> PrimaryKeyValues);

        protected override async IAsyncEnumerable<ColumnReadEvent> FullLoad(CancellationToken cancellationToken, [EnumeratorCancellation] CancellationToken enumeratorCancellationToken = default)
        {
            Debug.Assert(_state != null);
            Debug.Assert(_primaryKeys != null);
            Debug.Assert(_primaryKeyOrdinals != null);
            Debug.Assert(_convertFunctions != null);

            Logger.SelectingAllData(_fullTableName, StreamName, Name);

            var linkedCancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, enumeratorCancellationToken);

            InitializeBatchCollections(out PrimitiveList<int> weights, out PrimitiveList<uint> iterations, out Column[] columns);

            var primaryKeyValues = new Dictionary<string, object>();

            // todo: fix watermark
            var watermark = DateTime.UtcNow.Ticks;
            var batchSize = 10000;
            // todo: should we have retries?
            var retryCount = 0;
            // todo: error handling, setting health
            while (true)
            {
                linkedCancellation.Token.ThrowIfCancellationRequested();

                var context = ResilienceContextPool.Shared.Get(linkedCancellation.Token);
                var resilienceState = new FullLoadResilienceState(
                    _state,
                    _options,
                    _readRelation,
                    _primaryKeys[Name],
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
                        command.Parameters.AddWithValue("ChangeVersion", state.State.Value.ChangeTrackingVersion);

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
                    for (int i = 0; i < _primaryKeyOrdinals[Name].Count; i++)
                    {
                        primaryKeyValues.Add(_primaryKeys[Name][i], reader.GetValue(_primaryKeyOrdinals[Name][i]));
                    }

                    for (int i = 0; i < columns.Length; i++)
                    {
                        _convertFunctions[Name][i](reader, columns[i]);
                    }

                    if (weights.Count > 100)
                    {
                        var eventBatchData = new EventBatchData(columns);
                        var weightedBatch = new EventBatchWeighted(weights, iterations, eventBatchData);
                        yield return new ColumnReadEvent(weightedBatch, watermark);
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
                yield return new ColumnReadEvent(weightedBatch, watermark);
            }

            weights.Dispose();
            iterations.Dispose();
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
            using var connection = new SqlConnection(_options.ConnectionStringFunc());
            await connection.OpenAsync();
            if (_options.IsView)
            {
                return [.. (await SqlServerUtils.GetColumnOrdinals(connection, _fullTableName)).Select(s => s - 1)];
            }

            return [.. (await SqlServerUtils.GetPrimaryKeyOrdinals(connection, _fullTableName)).Select(s => s - 1)];
        }

        protected override Task<IReadOnlySet<string>> GetWatermarkNames()
        {
            return Task.FromResult<IReadOnlySet<string>>(_watermarks);
        }
    }
}
