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
using FlowtideDotNet.Base.Vertices;
using FlowtideDotNet.Core;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Core.Operators.Read;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Type;
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
        private HashSet<int>? _keyColumnSet;
        private Dictionary<int, int>? _schemaToValueIndex;
        private Action<IColumn, object?>[]? _converters;
        private HashSet<string>? _castToTextColumns;
        private IObjectState<PostgresState>? _state;
        private IPostgresChangeSource? _changeSource;
        private PostgresSnapshotInfo? _snapshot;
        private bool _streamingStarted;
        private bool _pendingReload;
        private bool _faultRaised;
        private long _lastLsn;
        private long _pendingConfirmLsn;
        private IObservableGauge<long>? _appliedLsnGauge;

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
            // The base operator already registers and increments an "events" counter for emitted rows.
            _appliedLsnGauge ??= Metrics.CreateObservableGauge("postgres_applied_lsn", () => Interlocked.Read(ref _lastLsn));

            List<(string name, string udtName)> columns;
            await using (var connection = new NpgsqlConnection(_options.ConnectionStringFunc()))
            {
                await PostgresUtils.OpenWithResilienceAsync(connection, _options.ResiliencePipeline, default);
                // Re-validate prerequisites on every (re)start, not just at plan build: the server config or the table
                // can change while the stream is stopped, so a rollback-driven restart must surface a clear error here
                // rather than failing opaquely deep inside slot creation or the replication loop.
                var walLevel = await PostgresUtils.GetWalLevel(connection, default);
                if (!string.Equals(walLevel, "logical", StringComparison.OrdinalIgnoreCase))
                {
                    throw new InvalidOperationException(
                        $"PostgreSQL logical replication requires 'wal_level = logical' but the server reports '{walLevel}'.");
                }
                _primaryKeys = await PostgresUtils.GetPrimaryKeys(connection, _schema, _table, default);
                columns = await PostgresUtils.GetColumns(connection, _schema, _table, default);
                var replicaIdentity = await PostgresUtils.GetReplicaIdentity(connection, _schema, _table, default);
                if (replicaIdentity == 'n')
                {
                    throw new InvalidOperationException(
                        $"Table {_schema}.{_table} has REPLICA IDENTITY NOTHING, so UPDATE/DELETE changes carry no key and cannot be applied. Set REPLICA IDENTITY to DEFAULT (the primary key) or FULL.");
                }
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
            _castToTextColumns = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            for (int i = 0; i < schemaNames.Count; i++)
            {
                var udt = typeByName.TryGetValue(schemaNames[i], out var u) ? u : "text";
                _converters[i] = PostgresUtils.BuildColumnConverter(udt);
                // Types without a typed converter (arrays, ranges, network, geometric, ...) are read as text in both
                // the snapshot and the replication stream so the two paths produce identical values.
                if (PostgresUtils.GetSubstraitType(udt) is AnyType)
                {
                    _castToTextColumns.Add(schemaNames[i]);
                }
            }

            if (_primaryKeys.Count == 0)
            {
                throw new InvalidOperationException(
                    $"Table {_schema}.{_table} has no primary key. A primary key is required for replication.");
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
            _keyColumnSet = new HashSet<int>(_keyOrdinals);

            _state = await stateManagerClient.GetOrCreateObjectStateAsync<PostgresState>("postgres_state");
            _state.Value ??= new PostgresState();
            // The restored LSN belongs to a globally-committed checkpoint (we recovered from it), so it is safe to confirm.
            // Seed both the confirm cursor AND the live position from durable state: on a persistent-slot resume no
            // snapshot runs to set _lastLsn, so without this an idle checkpoint would persist LastLsn=0 (defeating the
            // resume on the next restart), and a post-rollback resume would leave _lastLsn stale (risking confirming a
            // position past data not yet re-applied). On the snapshot path FullLoad overwrites _lastLsn anyway.
            _pendingConfirmLsn = _state.Value.LastLsn;
            _lastLsn = _state.Value.LastLsn;

            // On a rollback-driven restart InitializeOrRestore runs again on the same instance; tear down the previous
            // change source (which drops its temporary slot) before creating a fresh one that re-snapshots.
            if (_changeSource != null)
            {
                await _changeSource.DisposeAsync();
                _changeSource = null;
                _snapshot = null;
                _streamingStarted = false;
            }

            _faultRaised = false;
            _changeSource = _changeSourceFactory(new PostgresChangeSourceContext
            {
                StreamName = StreamName,
                OperatorName = Name,
                Schema = _schema,
                Table = _table,
                SchemaNames = _readRelation.BaseSchema.Names,
                KeySchemaIndices = _keyOrdinals
            });

            // Pass the last durably-checkpointed LSN so a persistent slot can resume from it instead of re-snapshotting.
            _snapshot = await _changeSource.InitializeAsync(_state.Value!.LastLsn, default);

            await base.InitializeOrRestore(restoreTime, stateManagerClient);

            // Map each non-key schema column to its position in a looked-up value reference, so unchanged-TOAST
            // columns can be backfilled from the previous row stored in the persistent tree.
            _schemaToValueIndex = new Dictionary<int, int>();
            var nonKeyColumns = NonKeyColumnIndexes;
            for (int j = 0; j < nonKeyColumns.Count; j++)
            {
                _schemaToValueIndex[nonKeyColumns[j]] = j;
            }
        }

        protected override async Task SendInitial(IngressOutput<StreamEventBatch> output)
        {
            await base.SendInitial(output);

            // On a cold start the base ran the full load (which starts streaming). On a restore the base skips it
            // (initial data was already sent), so we resume here:
            //   - snapshot available (temporary slot, or a persistent slot that was invalid/lost) -> re-snapshot, which
            //     reconciles the fresh snapshot against the restored state and restarts the stream;
            //   - no snapshot (persistent slot resuming from its confirmed position) -> just restart streaming.
            if (!_streamingStarted)
            {
                if (_snapshot != null)
                {
                    await DoFullLoad(output);
                }
                else
                {
                    await _changeSource!.BeginStreamingAsync(output.CancellationToken);
                    _streamingStarted = true;
                }
            }
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
            // Confirm only the PREVIOUS checkpoint's LSN, which is now globally committed. Checkpoints are sequential,
            // so checkpoint N-1 has fully completed before checkpoint N runs; the in-flight checkpoint is not yet
            // known-durable, and confirming it would risk losing data if this global checkpoint failed and rolled back
            // (PostgreSQL would have released WAL we never durably stored). Lagging by one checkpoint is safe: a resume
            // just replays a little more, which the reconciling apply makes idempotent.
            _changeSource?.SetConfirmedFlushLsn((ulong)_pendingConfirmLsn);
            _pendingConfirmLsn = _lastLsn;
        }

        public override Task OnTrigger(string triggerName, object? state)
        {
            // Surface a replication-loop fault from the operator thread (never from the loop): roll back to re-establish
            // the slot and re-snapshot/resume. Done once per fault; a fresh change source resets _faultRaised.
            if (!_faultRaised && _changeSource?.Fault is { } fault)
            {
                _faultRaised = true;
                Logger.LogError(fault, "PostgreSQL replication stream for {Schema}.{Table} faulted; rolling back to recover.", _schema, _table);
                SetHealth(false);
                return FailAndRollback(fault);
            }

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
            await PostgresUtils.OpenWithResilienceAsync(connection, _options.ResiliencePipeline, linked.Token);

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
                command.CommandText = PostgresUtils.BuildSnapshotSelect(_schema, _table, columnNames, _primaryKeys!, _castToTextColumns);

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

                    if (countInBatch >= _options.SnapshotBatchSize)
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

            await enterCheckpointLock();

            // Read the last fully-committed position BEFORE draining, so the watermark we emit below can only cover
            // transactions whose changes are already in the channel (and which we therefore drain in full here). A
            // transaction that commits while we drain is released on the next tick instead - never mid-transaction.
            var commitLsn = (long)_changeSource.LastCommitLsn;

            InitializeBatchCollections(out var weights, out var iterations, out var columns);
            int countInBatch = 0;

            while (_changeSource.TryRead(out var change))
            {
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
                        if (change.OldKeyValues != null)
                        {
                            AppendRow(columns, weights, iterations, change.OldKeyValues, -1);
                            countInBatch++;
                        }
                        if (HasUnchangedToast(change.Values))
                        {
                            // An UPDATE omitted an unchanged out-of-line (TOAST) column. Backfill it from the previous
                            // row stored in the persistent tree so the row is emitted with its full, correct value.
                            var (found, previous) = await LookupPreviousRowAsync(change.Values);
                            if (found)
                            {
                                AppendUpdateRowWithBackfill(columns, weights, iterations, change.Values, previous);
                                countInBatch++;
                            }
                            else
                            {
                                // The row is unexpectedly absent; fall back to a reconciling full reload for safety.
                                _pendingReload = true;
                            }
                        }
                        else
                        {
                            AppendRow(columns, weights, iterations, change.Values, 1);
                            countInBatch++;
                        }
                        break;
                }

                if (countInBatch >= 100)
                {
                    yield return new DeltaReadEvent(new EventBatchWeighted(weights, iterations, new EventBatchData(columns)), null);
                    InitializeBatchCollections(out weights, out iterations, out columns);
                    countInBatch = 0;
                }
            }

            // Advance the watermark to the last committed transaction, and only when it moves forward, so a transaction
            // is released exactly once - never as a duplicate of an earlier watermark, and never sitting on the
            // snapshot's consistent point (the commit LSN is always strictly past it).
            bool advance = commitLsn > _lastLsn;
            if (advance)
            {
                _lastLsn = commitLsn;
            }
            var watermark = advance ? new Watermark(_watermarkName, LongWatermarkValue.Create(commitLsn)) : null;

            if (countInBatch > 0)
            {
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
                if (watermark != null)
                {
                    yield return new DeltaReadEvent(null, watermark);
                }
            }

            if (advance)
            {
                _changeSource.Acknowledge((ulong)commitLsn);
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

        /// <summary>
        /// Looks up the previous version of a row (by primary key) in the persistent tree, used to backfill
        /// unchanged-TOAST columns. The returned value reference is only valid until the next tree operation.
        /// </summary>
        private async ValueTask<(bool found, ColumnRowReference value)> LookupPreviousRowAsync(object?[] values)
        {
            Debug.Assert(_keyColumnSet != null);
            Debug.Assert(_converters != null);

            var schemaWidth = _readRelation.BaseSchema.Names.Count;
            var keyColumns = new Column[schemaWidth];
            for (int i = 0; i < schemaWidth; i++)
            {
                keyColumns[i] = Column.Create(MemoryAllocator);
                if (_keyColumnSet.Contains(i))
                {
                    var keyValue = i < values.Length ? values[i] : null;
                    _converters[i](keyColumns[i], keyValue);
                }
                else
                {
                    // Only the key columns are read by the tree comparer, but every column needs a value at row 0.
                    keyColumns[i].Add(NullValue.Instance);
                }
            }

            var keyReference = new ColumnRowReference { referenceBatch = new EventBatchData(keyColumns), RowIndex = 0 };
            var result = await LookupRowValue(keyReference);

            foreach (var column in keyColumns)
            {
                column.Dispose();
            }
            return result;
        }

        private void AppendUpdateRowWithBackfill(Column[] columns, PrimitiveList<int> weights, PrimitiveList<uint> iterations, object?[] values, ColumnRowReference previous)
        {
            Debug.Assert(_schemaToValueIndex != null);

            for (int i = 0; i < columns.Length; i++)
            {
                var value = i < values.Length ? values[i] : null;
                if (ReferenceEquals(value, PostgresUtils.UnchangedToast) && _schemaToValueIndex.TryGetValue(i, out var valueColumn))
                {
                    columns[i].Add(previous.referenceBatch.Columns[valueColumn].GetValueAt(previous.RowIndex, default));
                }
                else
                {
                    if (ReferenceEquals(value, PostgresUtils.UnchangedToast))
                    {
                        value = null;
                    }
                    _converters![i](columns[i], value);
                }
            }
            weights.Add(1);
            iterations.Add(0);
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
