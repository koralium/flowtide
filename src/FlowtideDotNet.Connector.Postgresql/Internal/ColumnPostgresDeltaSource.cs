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
using FlowtideDotNet.Base.Metrics;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Core.Operators.Read;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Substrait.Relations;
using Microsoft.Extensions.Logging;
using Npgsql;
using System.Data;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Connector.PostgreSQL.Internal
{
    /// <summary>
    /// PostgreSQL logical replication source operator. Snapshots the table through the base operator's reconciling full
    /// load, then consumes a change source (per-table or shared) for ongoing deltas.
    /// </summary>
    internal sealed class ColumnPostgresDeltaSource : ColumnBatchReadBaseOperator
    {
        private readonly PostgresSourceOptions _options;
        private readonly ReadRelation _readRelation;
        private readonly Func<PostgresChangeSourceContext, IPostgresChangeSource> _changeSourceFactory;
        private readonly string _schema;
        private readonly string _table;
        private readonly string _watermarkName;
        private readonly HashSet<string> _watermarks;

        private List<string>? _primaryKeys;
        private List<int>? _keyOrdinals;
        private Action<IColumn, object?>[]? _converters;
        private IObjectState<PostgresState>? _state;
        private IPostgresChangeSource? _changeSource;
        private PostgresSnapshotInfo? _snapshot;
        private bool _streamingStarted;
        private bool _pendingReload;
        private long _lastLsn;
        private ICounter<long>? _eventsCounter;

        public ColumnPostgresDeltaSource(
            PostgresSourceOptions options,
            ReadRelation readRelation,
            IFunctionsRegister functionsRegister,
            DataflowBlockOptions dataflowBlockOptions,
            Func<PostgresChangeSourceContext, IPostgresChangeSource> changeSourceFactory)
            : base(readRelation, functionsRegister, dataflowBlockOptions)
        {
            _options = options;
            _readRelation = readRelation;
            _changeSourceFactory = changeSourceFactory;
            var nameParts = options.TableNameTransform?.Invoke(readRelation) ?? readRelation.NamedTable.Names;
            (_schema, _table) = PostgresUtils.ResolveSchemaAndTable(nameParts);
            _watermarkName = readRelation.NamedTable.DotSeperated;
            _watermarks = new HashSet<string> { _watermarkName };
            DeltaLoadInterval = options.DeltaLoadInterval;
            FullLoadInterval = null;
        }

        public override string DisplayName => $"PostgreSQL-{_schema}.{_table}";

        protected override async Task InitializeOrRestore(long restoreTime, IStateManagerClient stateManagerClient)
        {
            _eventsCounter ??= Metrics.CreateCounter<long>("events");

            List<(string name, string udtName)> columns;
            await using (var connection = new NpgsqlConnection(_options.ConnectionStringFunc()))
            {
                await connection.OpenAsync();
                _primaryKeys = await PostgresUtils.GetPrimaryKeys(connection, _schema, _table, default);
                columns = await PostgresUtils.GetColumns(connection, _schema, _table, default);
            }

            // Build a converter per read-schema column from its PostgreSQL type so the snapshot (typed) and the
            // replication stream (text) decode to the same canonical values, keeping key lookups consistent.
            var typeByName = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            foreach (var (name, udtName) in columns)
            {
                typeByName[name] = udtName;
            }
            var schemaNames = _readRelation.BaseSchema.Names;
            _converters = new Action<IColumn, object?>[schemaNames.Count];
            for (int i = 0; i < schemaNames.Count; i++)
            {
                _converters[i] = PostgresUtils.BuildColumnConverter(typeByName.TryGetValue(schemaNames[i], out var udt) ? udt : "text");
            }

            if (_primaryKeys.Count == 0)
            {
                throw new InvalidOperationException(
                    $"Table {_schema}.{_table} has no primary key. A primary key (or REPLICA IDENTITY FULL) is required for replication.");
            }

            _keyOrdinals = new List<int>();
            foreach (var pk in _primaryKeys)
            {
                var index = _readRelation.BaseSchema.Names.FindIndex(n => n.Equals(pk, StringComparison.OrdinalIgnoreCase));
                if (index < 0)
                {
                    throw new InvalidOperationException($"Primary key column '{pk}' is not present in the read schema for {_schema}.{_table}.");
                }
                _keyOrdinals.Add(index);
            }

            _state = await stateManagerClient.GetOrCreateObjectStateAsync<PostgresState>("postgres_state");
            _state.Value ??= new PostgresState();

            // On a rollback-driven restart InitializeOrRestore runs again on the same instance; tear down the previous
            // change source (which drops its temporary slot) before creating a fresh one that re-snapshots.
            if (_changeSource != null)
            {
                await _changeSource.DisposeAsync();
                _changeSource = null;
                _snapshot = null;
                _streamingStarted = false;
            }

            _changeSource = _changeSourceFactory(new PostgresChangeSourceContext
            {
                StreamName = StreamName,
                Schema = _schema,
                Table = _table,
                SchemaNames = _readRelation.BaseSchema.Names,
                KeySchemaIndices = _keyOrdinals,
                FaultHandler = OnChangeSourceFaultAsync
            });

            _snapshot = await _changeSource.InitializeAsync(default);

            await base.InitializeOrRestore(restoreTime, stateManagerClient);
        }

        private async Task OnChangeSourceFaultAsync(Exception exception)
        {
            Logger.LogError(exception, "PostgreSQL replication stream for {Schema}.{Table} faulted; rolling back to re-establish the slot and re-snapshot.", _schema, _table);
            SetHealth(false);
            // The temporary slot is gone, so rolling back to the last checkpoint and re-initializing is the recovery
            // path: a fresh slot is created and the table is re-snapshotted, reconciled against existing state.
            await FailAndRollback(exception);
        }

        protected override ValueTask<List<int>> GetPrimaryKeyColumns()
        {
            Debug.Assert(_keyOrdinals != null);
            return ValueTask.FromResult(_keyOrdinals);
        }

        protected override Task<IReadOnlySet<string>> GetWatermarkNames()
        {
            return Task.FromResult<IReadOnlySet<string>>(_watermarks);
        }

        protected override async Task Checkpoint(long checkpointTime)
        {
            Debug.Assert(_state != null);
            _state.Value!.LastLsn = _lastLsn;
            await _state.Commit();
        }

        public override Task OnTrigger(string triggerName, object? state)
        {
            // If a change could not be applied incrementally, redirect the next delta tick into a reconciling full reload.
            if (triggerName == DeltaLoadTriggerName && _changeSource != null && (_pendingReload || _changeSource.NeedsResnapshot))
            {
                _pendingReload = false;
                _changeSource.ClearResnapshot();
                return base.OnTrigger(FullLoadTriggerName, state);
            }
            return base.OnTrigger(triggerName, state);
        }

        protected override async IAsyncEnumerable<ColumnReadEvent> FullLoad(
            CancellationToken cancellationToken,
            [EnumeratorCancellation] CancellationToken enumeratorCancellationToken = default)
        {
            Debug.Assert(_state != null);
            Debug.Assert(_changeSource != null);

            var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, enumeratorCancellationToken);
            var columnNames = _readRelation.BaseSchema.Names;
            bool useSnapshot = !_streamingStarted && _snapshot != null;

            await using var connection = new NpgsqlConnection(_options.ConnectionStringFunc());
            await connection.OpenAsync(linked.Token);

            NpgsqlTransaction? transaction = null;
            if (useSnapshot)
            {
                transaction = await connection.BeginTransactionAsync(IsolationLevel.RepeatableRead, linked.Token);
                using var setSnapshot = connection.CreateCommand();
                setSnapshot.Transaction = transaction;
                setSnapshot.CommandText = $"SET TRANSACTION SNAPSHOT '{_snapshot!.SnapshotName}'";
                await setSnapshot.ExecuteNonQueryAsync(linked.Token);
                _lastLsn = (long)_snapshot.ConsistentLsn;
            }

            using (var command = connection.CreateCommand())
            {
                command.Transaction = transaction;
                command.CommandText = PostgresUtils.BuildSnapshotSelect(_schema, _table, columnNames, _primaryKeys!);

                InitializeBatchCollections(out var weights, out var iterations, out var columns);
                int countInBatch = 0;

                await using var reader = await command.ExecuteReaderAsync(linked.Token);
                while (await reader.ReadAsync(linked.Token))
                {
                    for (int i = 0; i < columns.Length; i++)
                    {
                        _converters![i](columns[i], reader.IsDBNull(i) ? null : reader.GetValue(i));
                    }
                    weights.Add(1);
                    iterations.Add(0);
                    countInBatch++;

                    if (countInBatch >= 100)
                    {
                        yield return new ColumnReadEvent(new EventBatchWeighted(weights, iterations, new EventBatchData(columns)), _lastLsn);
                        InitializeBatchCollections(out weights, out iterations, out columns);
                        countInBatch = 0;
                    }
                }

                if (countInBatch > 0)
                {
                    yield return new ColumnReadEvent(new EventBatchWeighted(weights, iterations, new EventBatchData(columns)), _lastLsn);
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
            }

            if (transaction != null)
            {
                await transaction.DisposeAsync();
            }

            if (!_streamingStarted)
            {
                await _changeSource.SnapshotCompleteAsync(linked.Token);
                _streamingStarted = true;
            }

            linked.Dispose();
        }

        protected override async IAsyncEnumerable<DeltaReadEvent> DeltaLoad(
            Func<Task> enterCheckpointLock,
            Action exitCheckpointLock,
            CancellationToken cancellationToken,
            [EnumeratorCancellation] CancellationToken enumeratorCancellationToken = default)
        {
            Debug.Assert(_changeSource != null);
            Debug.Assert(_eventsCounter != null);

            await enterCheckpointLock();

            InitializeBatchCollections(out var weights, out var iterations, out var columns);
            int countInBatch = 0;
            bool any = false;

            while (_changeSource.TryRead(out var change))
            {
                any = true;
                _lastLsn = (long)change.Lsn;

                switch (change.Kind)
                {
                    case PostgresChangeKind.Insert:
                        AppendRow(columns, weights, iterations, change.Values, 1);
                        countInBatch++;
                        break;
                    case PostgresChangeKind.Delete:
                        AppendRow(columns, weights, iterations, change.Values, -1);
                        countInBatch++;
                        break;
                    case PostgresChangeKind.Update:
                        if (HasUnchangedToast(change.Values))
                        {
                            // Cannot apply incrementally without the omitted TOAST value; reconcile via a full reload.
                            _pendingReload = true;
                            break;
                        }
                        if (change.OldKeyValues != null)
                        {
                            AppendRow(columns, weights, iterations, change.OldKeyValues, -1);
                            countInBatch++;
                        }
                        AppendRow(columns, weights, iterations, change.Values, 1);
                        countInBatch++;
                        break;
                }

                if (countInBatch >= 100)
                {
                    _eventsCounter.Add(countInBatch);
                    yield return new DeltaReadEvent(new EventBatchWeighted(weights, iterations, new EventBatchData(columns)), null);
                    InitializeBatchCollections(out weights, out iterations, out columns);
                    countInBatch = 0;
                }
            }

            if (countInBatch > 0)
            {
                _eventsCounter.Add(countInBatch);
                var watermark = new Watermark(_watermarkName, LongWatermarkValue.Create(_lastLsn));
                yield return new DeltaReadEvent(new EventBatchWeighted(weights, iterations, new EventBatchData(columns)), watermark);
            }
            else
            {
                weights.Dispose();
                iterations.Dispose();
                foreach (var column in columns)
                {
                    column.Dispose();
                }
                if (any)
                {
                    yield return new DeltaReadEvent(null, new Watermark(_watermarkName, LongWatermarkValue.Create(_lastLsn)));
                }
            }

            if (any)
            {
                _changeSource.Acknowledge((ulong)_lastLsn);
            }

            exitCheckpointLock();
        }

        private static bool HasUnchangedToast(object?[] values)
        {
            for (int i = 0; i < values.Length; i++)
            {
                if (ReferenceEquals(values[i], PostgresUtils.UnchangedToast))
                {
                    return true;
                }
            }
            return false;
        }

        private void AppendRow(Column[] columns, PrimitiveList<int> weights, PrimitiveList<uint> iterations, object?[] values, int weight)
        {
            for (int i = 0; i < columns.Length; i++)
            {
                var value = i < values.Length ? values[i] : null;
                if (ReferenceEquals(value, PostgresUtils.UnchangedToast))
                {
                    value = null;
                }
                _converters![i](columns[i], value);
            }
            weights.Add(weight);
            iterations.Add(0);
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

        public override async ValueTask DisposeAsync()
        {
            if (_changeSource != null)
            {
                await _changeSource.DisposeAsync();
            }
            await base.DisposeAsync();
        }
    }
}
