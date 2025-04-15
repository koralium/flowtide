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
    internal abstract class ColumnSqlDeltaSource : ColumnBatchReadBaseOperator
    {
        protected SqlServerSourceOptions Options { get; }

        private readonly HashSet<string> _watermarks;

        private readonly string _displayName;
        protected ReadRelation ReadRelation { get; }

        protected List<string>? PrimaryKeys { get; private set; }
        protected List<int>? PrimaryKeyOrdinals { get; private set; }
        protected List<Action<SqlDataReader, IColumn>>? ConvertFunctions { get; private set; }

        protected string FullTableName { get; }
        protected IObjectState<SqlServerState>? State { get; private set; }

        protected ColumnSqlDeltaSource(SqlServerSourceOptions sourceOptions, ReadRelation readRelation, IFunctionsRegister functionsRegister, DataflowBlockOptions options) : base(readRelation, functionsRegister, options)
        {
            Options = sourceOptions;
            ReadRelation = readRelation;
            var namedTable = Options.TableNameTransform?.Invoke(readRelation) ?? readRelation.NamedTable.Names;
            FullTableName = string.Join('.', namedTable);
            _watermarks = [FullTableName];
            _displayName = $"SqlServer-{FullTableName}";

            if (sourceOptions.IsChangeTrackingEnabled)
            {
                base.DeltaLoadInterval = sourceOptions.ChangeTrackingInterval;
            }
            else
            {
                base.DeltaLoadInterval = null;
            }

            base.FullLoadInterval = sourceOptions.FullReloadInterval;
        }

        public override string DisplayName => _displayName;

        protected override async Task InitializeOrRestore(long restoreTime, IStateManagerClient stateManagerClient)
        {
            using var connection = new SqlConnection(Options.ConnectionStringFunc());
            await connection.OpenAsync();

            if (Options.IsView)
            {
                PrimaryKeys = ReadRelation.BaseSchema.Names;
            }
            else
            {
                PrimaryKeys = await SqlServerUtils.GetPrimaryKeys(connection, FullTableName);
            }

            PrimaryKeyOrdinals ??= [];
            foreach (var key in PrimaryKeys)
            {
                var foundKey = false;
                for (int i = 0; i < ReadRelation.BaseSchema.Names.Count; i++)
                {
                    if (ReadRelation.BaseSchema.Names[i].Equals(key, StringComparison.OrdinalIgnoreCase))
                    {
                        foundKey = true;
                        PrimaryKeyOrdinals.Add(i);
                    }
                }

                if (!foundKey)
                {
                    throw new InvalidOperationException($"Primary key ordinal not found for '{key}'");
                }
            }

            using var command = connection.CreateCommand();
            command.CommandText = SqlServerUtils.CreateSelectStatementTop1(ReadRelation);

            using var reader = await command.ExecuteReaderAsync();
            var schema = await reader.GetColumnSchemaAsync();
            ConvertFunctions = SqlServerUtils.GetColumnEventCreator(schema);

            State = await stateManagerClient.GetOrCreateObjectStateAsync<SqlServerState>("sqlserver_state");

            State.Value ??= new SqlServerState
            {
                ChangeTrackingVersion = 0
            };

            await base.InitializeOrRestore(restoreTime, stateManagerClient);
        }

        protected override async Task Checkpoint(long checkpointTime)
        {
            Debug.Assert(State != null);
            await State.Commit();
        }

        protected override async IAsyncEnumerable<DeltaReadEvent> DeltaLoad(Func<Task> EnterCheckpointLock, Action ExitCheckpointLock, CancellationToken cancellationToken, [EnumeratorCancellation] CancellationToken enumeratorCancellationToken = default)
        {
            Debug.Assert(Options != null);
            Debug.Assert(State?.Value != null);
            Debug.Assert(State?.Value.ChangeTrackingVersion != null);
            Debug.Assert(PrimaryKeys != null);
            Debug.Assert(PrimaryKeyOrdinals != null);
            Debug.Assert(ConvertFunctions != null);

            Logger.SelectingChanges(FullTableName, StreamName, Name);

            var linkedCancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, enumeratorCancellationToken);
            await EnterCheckpointLock();

            InitializeBatchCollections(out PrimitiveList<int> weights, out PrimitiveList<uint> iterations, out Column[] columns);

            var elementCount = 0;

            var context = ResilienceContextPool.Shared.Get(linkedCancellation.Token);
            var resilienceState = new DeltaLoadResilienceState(State, Options, ReadRelation, PrimaryKeys);
            var pipelineResult = await Options.ResiliencePipeline.ExecuteOutcomeAsync(static async (ctx, state) =>
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
                    ConvertFunctions[i](reader, columns[i]);
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

                State.Value.ChangeTrackingVersion = changeVersion;

                if (weights.Count >= 100)
                {
                    var eventBatchData = new EventBatchData(columns);
                    var weightedBatch = new EventBatchWeighted(weights, iterations, eventBatchData);
                    Logger.ChangesFoundInTable(weights.Count, FullTableName, StreamName, Name);
                    yield return new DeltaReadEvent(weightedBatch, new Base.Watermark(ReadRelation.NamedTable.DotSeperated, State.Value.ChangeTrackingVersion));
                    InitializeBatchCollections(out weights, out iterations, out columns);
                }
            }

            await pipelineResult.Result.DisposeAsync();


            if (weights.Count > 0)
            {
                var eventBatchData = new EventBatchData(columns);
                var weightedBatch = new EventBatchWeighted(weights, iterations, eventBatchData);
                Logger.ChangesFoundInTable(weights.Count, FullTableName, StreamName, Name);
                yield return new DeltaReadEvent(weightedBatch, new Base.Watermark(ReadRelation.NamedTable.DotSeperated, State.Value.ChangeTrackingVersion));
            }
            else
            {
                weights.Dispose();
                iterations.Dispose();
                foreach (var column in columns)
                {
                    column.Dispose();
                }
            }

            ExitCheckpointLock();
            linkedCancellation.Dispose();
        }

        protected override async Task OnCheckpoint(long checkpointTime)
        {
            Debug.Assert(State != null);
            await State.Commit();
            await base.OnCheckpoint(checkpointTime);
        }

        protected void InitializeBatchCollections(out PrimitiveList<int> weights, out PrimitiveList<uint> iterations, out Column[] columns)
        {
            weights = new PrimitiveList<int>(MemoryAllocator);
            iterations = new PrimitiveList<uint>(MemoryAllocator);
            columns = new Column[ReadRelation.BaseSchema.Names.Count];
            for (int i = 0; i < columns.Length; i++)
            {
                columns[i] = Column.Create(MemoryAllocator);
            }
        }

        protected override ValueTask<List<int>> GetPrimaryKeyColumns()
        {
            Debug.Assert(PrimaryKeyOrdinals != null);
            return ValueTask.FromResult(PrimaryKeyOrdinals);
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

        protected sealed record ResilienceResult(SqlDataReader Reader, SqlConnection Connection, SqlCommand Command) : IAsyncDisposable
        {
            public async ValueTask DisposeAsync()
            {
                await Reader.DisposeAsync();
                await Connection.DisposeAsync();
                await Command.DisposeAsync();
            }
        }
    }

    internal class PartitionedTableDataSource : ColumnSqlDeltaSource
    {
        public PartitionedTableDataSource(SqlServerSourceOptions sourceOptions, ReadRelation readRelation, IFunctionsRegister functionsRegister, DataflowBlockOptions options) : base(sourceOptions, readRelation, functionsRegister, options)
        {
            ArgumentNullException.ThrowIfNull(sourceOptions.PartitionMetadata);
        }

        protected override async IAsyncEnumerable<ColumnReadEvent> FullLoad(CancellationToken cancellationToken, [EnumeratorCancellation] CancellationToken enumeratorCancellationToken = default)
        {
            Debug.Assert(Options.PartitionMetadata != null);
            Debug.Assert(PrimaryKeys != null);
            Debug.Assert(ConvertFunctions != null);

            var linkedCancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, enumeratorCancellationToken);
            InitializeBatchCollections(out PrimitiveList<int> weights, out PrimitiveList<uint> iterations, out Column[] columns);

            using var connection = new SqlConnection(Options.ConnectionStringFunc());
            await connection.OpenAsync(cancellationToken);
            var paritionIds = await SqlServerUtils.GetParitionIds(connection, ReadRelation);

            var keys = PrimaryKeys.Where(s => s != Options.PartitionMetadata.PartitionColumn);

            var cols = string.Join(", ", ReadRelation.BaseSchema.Names.Select(x => $"[{x}]"));

            var primaryKeyValues = new Dictionary<string, object>();

            var cmd = $@"SELECT {cols}
                FROM table --fix
                WHERE $PARTITION.{Options.PartitionMetadata.PartitionFunction}({Options.PartitionMetadata.PartitionColumn}) = @PartitionId
                ORDER BY {string.Join(',', keys)}
                -- OFFSET 0 ROWS FETCH NEXT 10000 ROWS ONLY";

            foreach (var partitionId in paritionIds)
            {
                enumeratorCancellationToken.ThrowIfCancellationRequested();

                using var command = new SqlCommand(cmd, connection);
                command.Parameters.AddWithValue("partitionId", partitionId);

                using var reader = await command.ExecuteReaderAsync(cancellationToken);

                while (await reader.ReadAsync(linkedCancellation.Token))
                {
                    weights.Add(1);
                    iterations.Add(0);

                    for (int i = 0; i < columns.Length; i++)
                    {
                        ConvertFunctions[i](reader, columns[i]);
                    }

                    if (weights.Count >= 100)
                    {
                        var eventBatchData = new EventBatchData(columns);
                        var weightedBatch = new EventBatchWeighted(weights, iterations, eventBatchData);
                        yield return new ColumnReadEvent(weightedBatch, State.Value.ChangeTrackingVersion);
                        InitializeBatchCollections(out weights, out iterations, out columns);
                    }
                }
            }

            linkedCancellation.Dispose();
        }
    }

    internal class ColumnSqlServerBatchDataSource : ColumnSqlDeltaSource
    {
        private readonly string? _filter;
        public ColumnSqlServerBatchDataSource(SqlServerSourceOptions sourceOptions, ReadRelation readRelation, IFunctionsRegister functionsRegister, DataflowBlockOptions options) : base(sourceOptions, readRelation, functionsRegister, options)
        {
            FullLoadInterval = sourceOptions.FullReloadInterval;

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

        protected override async IAsyncEnumerable<ColumnReadEvent> FullLoad(CancellationToken cancellationToken, [EnumeratorCancellation] CancellationToken enumeratorCancellationToken = default)
        {
            Debug.Assert(State != null);
            Debug.Assert(State.Value?.ChangeTrackingVersion != null);
            Debug.Assert(PrimaryKeys != null);
            Debug.Assert(PrimaryKeyOrdinals != null);
            Debug.Assert(ConvertFunctions != null);

            Logger.SelectingAllData(FullTableName, StreamName, Name);

            var linkedCancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, enumeratorCancellationToken);

            InitializeBatchCollections(out PrimitiveList<int> weights, out PrimitiveList<uint> iterations, out Column[] columns);

            var primaryKeyValues = new Dictionary<string, object>();

            if (Options.IsChangeTrackingEnabled && State.Value.ChangeTrackingVersion < 1)
            {
                using var connection = new SqlConnection(Options.ConnectionStringFunc());
                await connection.OpenAsync(linkedCancellation.Token);
                State.Value.ChangeTrackingVersion = await SqlServerUtils.GetLatestChangeVersion(connection);
            }
            else if (!Options.IsChangeTrackingEnabled)
            {
                // when reading from a view or a table without change tracking, use the server timestamp as the version identifier
                using var connection = new SqlConnection(Options.ConnectionStringFunc());
                await connection.OpenAsync(linkedCancellation.Token);
                State.Value.ChangeTrackingVersion = await SqlServerUtils.GetServerTimestamp(connection);
            }

            var batchSize = 10000;
            while (true)
            {
                linkedCancellation.Token.ThrowIfCancellationRequested();

                var context = ResilienceContextPool.Shared.Get(linkedCancellation.Token);
                var resilienceState = new FullLoadResilienceState(
                    State,
                    Options,
                    ReadRelation,
                    PrimaryKeys,
                    primaryKeyValues.Count > 0,
                    batchSize,
                    _filter,
                    primaryKeyValues);

                var pipelineResult = await Options.ResiliencePipeline.ExecuteOutcomeAsync(static async (ctx, state) =>
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
                    for (int i = 0; i < PrimaryKeyOrdinals.Count; i++)
                    {
                        primaryKeyValues.Add(PrimaryKeys[i], reader.GetValue(PrimaryKeyOrdinals[i]));
                    }

                    for (int i = 0; i < columns.Length; i++)
                    {
                        ConvertFunctions[i](reader, columns[i]);
                    }

                    if (weights.Count >= 100)
                    {
                        var eventBatchData = new EventBatchData(columns);
                        var weightedBatch = new EventBatchWeighted(weights, iterations, eventBatchData);
                        yield return new ColumnReadEvent(weightedBatch, State.Value.ChangeTrackingVersion);
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
                yield return new ColumnReadEvent(weightedBatch, State.Value.ChangeTrackingVersion);
            }
            else
            {
                weights.Dispose();
                iterations.Dispose();
                foreach (var column in columns)
                {
                    column.Dispose();
                }
            }

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
            Dictionary<string, object> PrimaryKeyValues)
        {

        }
    }
}