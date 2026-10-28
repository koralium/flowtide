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

using Apache.Arrow;
using FlowtideDotNet.Connector.DeltaLake.Internal.Catalog;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Actions;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Checkpoints;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat.Comparers;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema.Converters;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema.Types;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Stats;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Utils;
using FlowtideDotNet.Connector.DeltaLake.Internal.Deletes;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.Deletion;
using FlowtideDotNet.Core;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Core.Operators.Write;
using FlowtideDotNet.Storage.Serializers;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.Tree;
using FlowtideDotNet.Substrait.Relations;
using Microsoft.Extensions.Logging;
using Stowage;
using System.Diagnostics;
using System.Text.Json;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Connector.DeltaLake.Internal
{
    internal class DeltaLakePendingCommit
    {
        public long Version { get; set; }

        public string StagedFile { get; set; } = string.Empty;

        /// <summary>
        /// Null for commits staged before stage ids existed.
        /// </summary>
        public string? StageId { get; set; }

        public long? Length { get; set; }

        /// <summary>
        /// The checkpoint that staged the commit, it is published once that checkpoint is committed.
        /// </summary>
        public long? CheckpointId { get; set; }
    }

    internal class DeltaLakeSink : WriteBaseOperator
    {
        private const int MaxRowsPerFile = 10_000_000;

        /// <summary>
        /// Called with the table name before the temporary tree is committed.
        /// </summary>
        internal static Action<string>? TemporaryTreeCommitHookForTests;

        /// <summary>
        /// Run by table name on the sink's thread once the catalog holds the published head, before it is used.
        /// </summary>
        internal static readonly System.Collections.Concurrent.ConcurrentDictionary<string, Func<DeltaSinkCatalog, Task>> CatalogHooksForTests = new System.Collections.Concurrent.ConcurrentDictionary<string, Func<DeltaSinkCatalog, Task>>();

        // How long a stopping run waits for its maintenance job before counting it detached
        private static readonly TimeSpan s_maintenanceJoin = TimeSpan.FromSeconds(2);

        private readonly DeltaLakeOptions _options;
        private readonly WriteRelation _writeRelation;
        private IBPlusTree<ColumnRowReference, int, ColumnKeyStorageContainer, PrimitiveListValueContainer<int>>? _temporaryTree;
        private TemporaryTreeIntake? _intake;
        private string _tableName;
        private IOPath _tablePath;
        // Validated once, so restores reuse it and a changed storage instance is detected
        private readonly CleanupConfiguration _cleanup;
        // The published value of the validated retention, null with retention off
        private readonly string? _retentionValue;
        // Validated once at construction, as retention is
        private readonly (long Requests, long Bytes) _maintenanceRates;

        private IObjectState<bool>? _firstInsertDone;
        private IObjectState<DeltaLakePendingCommit>? _pendingCommit;
        // True while the committed temporary tree may hold rows that wait for a publication
        private IObjectState<bool>? _bufferDurable;
        // The staged bytes and catalog changes of this run's pending commit
        private CommitBytes? _stagedBytes;
        private CatalogOverlay? _stagedOverlay;
        private DeltaSinkCatalog? _catalog;
        private ProbeBatch? _probes;
        private DeleteLimits? _limits;
        // The last commit's matching, read by the passes gauge
        private DeleteStatistics? _deleteStatistics;
        private bool _halted;
        private string? _haltReason;
        private bool _gaugesCreated;
        private DeltaMaintenanceWorker? _worker;
        private ProducerHandle? _producers;
        private DeltaMaintenanceBudget? _producersBudget;

        // Earlier runs' workers whose detached jobs still run, replaced whole so the gauge reads it unlocked
        private volatile DeltaMaintenanceWorker[] _stoppedWorkers = System.Array.Empty<DeltaMaintenanceWorker>();
        // The head the latest Validate saw this run, handed off when there is nothing to publish
        private long _validatedHead = -1;
        private bool _maintenanceGaugesCreated;
        private bool _budgetMismatchWarned;
        private readonly HashSet<string> _reportedCheckpointFailures = new HashSet<string>();
        private readonly HashSet<string> _unknownCodecs = new HashSet<string>();
        private string _fileExtension = ".parquet";
        private bool _discardFailureWarned;
        private bool _discardKeptWarned;
        private bool _discardDroppedWarned;
        private long _metadataCommitFailures;
        private bool _metadataCommitWarned;
        // A properties or anchor commit failed; its retries without rows stay contained
        private bool _metadataRetry;
        // Chosen once per run: queued to the worker, or deleted inline by the attempt
        private DiscardMode _discardMode;
        // The run's producer handle on its discard route, in queued mode
        private DiscardHandle? _discards;
        // The stage this sink last marked failed closed
        private string? _failedClosedStage;
        // The inline fallback's executor, made for the storage it deletes through
        private DeletionExecutor? _inlineExecutor;
        private readonly DiscardCounters _discardCounters = new DiscardCounters();
        private readonly SweepCounters _sweepCounters = new SweepCounters();
        private readonly RetentionCounters _retentionCounters = new RetentionCounters();

        // Discarded outputs whose best-effort delete failed
        internal long DiscardDeleteFailures => _discardCounters.Failed;

        internal DiscardCounters DiscardsForTests => _discardCounters;

        internal DiscardMode DiscardModeForTests => _discardMode;

        internal DiscardHandle? DiscardRouteForTests => _discards;

        // The worker of the latest start, set after that run's discard mode and route
        internal DeltaMaintenanceWorker? StartedWorkerForTests { get; private set; }

        // The settings the latest worker was created with
        internal MaintenanceWorkerSettings? WorkerSettingsForTests { get; private set; }

        // Properties or anchor commits that failed to stage and wait for the next checkpoint
        internal long MetadataCommitFailures => Volatile.Read(ref _metadataCommitFailures);

        // The intake's path counters, read by tests and probes
        internal TemporaryTreeIntake? Intake => Volatile.Read(ref _intake);

        public DeltaLakeSink(DeltaLakeOptions options, WriteRelation writeRelation, ExecutionDataflowBlockOptions executionDataflowBlockOptions) : base(executionDataflowBlockOptions)
        {
            this._options = options;
            this._writeRelation = writeRelation;
            _tableName = string.Join("/", writeRelation.NamedObject.Names);
            // On the original name, before path normalization can hide a segment
            _cleanup = CleanupConfiguration.Validate(() => options.StorageLocation, _tableName, options.AssumeCompleteStorageListing, new CleanupOptIns(options.HistoryRetention, options.OrphanFileSweepInterval));
            _retentionValue = _cleanup.OptIns.Retention is { } retention ? RetentionProperties.Format(retention) : null;
            _maintenanceRates = MaintenanceBudgetSettings.Rates(options);
            _tablePath = _tableName;

            if (writeRelation.TableSchema.Struct != null)
            {
                for (int i = 0; i < writeRelation.TableSchema.Struct.Types.Count; i++)
                {
                    if (writeRelation.TableSchema.Struct.Types[i].Type == Substrait.Type.SubstraitType.Any)
                    {
                        var columnName = writeRelation.TableSchema.Names[i];
                        throw new NotSupportedException($"Delta Lake Sink does not support columns of type Any, destination: '{_tableName}', columnName: '{columnName}'");
                    }
                }
            }
            options.SinkCreatedForTests?.Invoke(this);
        }

        public override string DisplayName => $"DeltaLakeSink({_tableName})";

        public override Task Compact()
        {
            return Task.CompletedTask;
        }

        public override Task DeleteAsync()
        {
            return Task.CompletedTask;
        }

        protected override async Task InitializeOrRestore(long restoreTime, IStateManagerClient stateManagerClient)
        {
            _temporaryTree = await stateManagerClient.GetOrCreateTree("temporary", new BPlusTreeOptions<ColumnRowReference, int, ColumnKeyStorageContainer, PrimitiveListValueContainer<int>>()
            {
                Comparer = new ColumnComparer(_writeRelation.OutputLength),
                KeySerializer = new ColumnStoreSerializer(_writeRelation.OutputLength, MemoryAllocator),
                ValueSerializer = new PrimitiveListValueContainerSerializer<int>(MemoryAllocator),
                MemoryAllocator = MemoryAllocator,
                UseByteBasedPageSizes = true,
            });
            _limits = _options.DeleteLimitsOverride ?? DeleteLimits.Default;
            _intake = new TemporaryTreeIntake(_temporaryTree, _writeRelation.OutputLength, _limits);
            _firstInsertDone = await stateManagerClient.GetOrCreateObjectStateAsync<bool>("isFirstInsert");
            _pendingCommit = await stateManagerClient.GetOrCreateObjectStateAsync<DeltaLakePendingCommit>("pendingCommit");
            _bufferDurable = await stateManagerClient.GetOrCreateObjectStateAsync<bool>("bufferDurable");

            // Vertices are reused across restarts
            _stagedBytes = null;
            _stagedOverlay = null;
            _halted = false;
            _metadataRetry = false;
            SetHealth(true);
            _catalog?.Dispose();
            _catalog = await DeltaSinkCatalog.Open(stateManagerClient, _options, _tablePath, _writeRelation.TableSchema.Names, MemoryAllocator, Logger);
            _probes = null;

            // Meters outlive restarts
            if (!_gaugesCreated)
            {
                _gaugesCreated = true;
                Metrics.CreateObservableGauge("delta_catalog_files", () => _catalog?.LiveFiles ?? 0);
                Metrics.CreateObservableGauge("delta_catalog_pruning_columns", () => _catalog?.ScanLayout.Columns.Count ?? 0);
                Metrics.CreateObservableGauge("delta_catalog_pruning_bytes", () => _catalog?.PruningBytes ?? 0);
                Metrics.CreateObservableGauge("delta_catalog_tree_mode", () => _catalog?.TreeMode == true ? 1 : 0);
                Metrics.CreateObservableGauge("delta_delete_passes", () => _deleteStatistics?.Passes ?? 0);
            }

            _validatedHead = -1;
            await StartMaintenance();
        }

        // A failure here only turns maintenance off for this run; it never reaches the engine
        private async Task StartMaintenance()
        {
            await StopWorker();
            // Earlier runs' jobs that settled since leave the gauge
            _stoppedWorkers = Detached(_stoppedWorkers, null);
            // Read at every start, outside the construction snapshot
            var deletesDisabled = MaintenanceSwitches.DeletesDisabled();
            var mode = DiscardMode.Inline;
            DiscardHandle? discards = null;
            var budget = _options.MaintenanceBudgetOverride;
            var shared = _options.SharedBudgetForTests ?? DeltaMaintenanceBudget.SharedSource;
            // An existing budget's producer entry is held before the settings or the worker can fail
            if ((budget ?? shared.Existing) is { } existing)
            {
                HoldProducers(existing);
            }
            try
            {
                if (_options.MaintenanceSettingsFailForTests)
                {
                    throw new ArgumentException("Maintenance settings fault");
                }
                if (budget == null)
                {
                    var settings = MaintenanceBudgetSettings.From(_options, _maintenanceRates);
                    var matches = true;
                    budget = shared.Get(settings, out matches);
                    HoldProducers(budget);
                    if (!matches && !_budgetMismatchWarned)
                    {
                        _budgetMismatchWarned = true;
                        Logger.LogWarning("Delta table {table} keeps the process's first maintenance settings: {jobs} concurrent jobs in {directory}, {requests} requests and {bytes} bytes a second", _tableName,
                            budget.Settings.MaxConcurrentJobs, budget.Settings.TempDirectory, budget.Settings.RequestsPerSecond, budget.Settings.BytesPerSecond);
                    }
                }
                if (_options.MaintenanceInitFailsForTests)
                {
                    throw new InvalidOperationException("Maintenance initialization fault");
                }
                var periodic = _options.CheckpointInterval > 0;
                // A storage replaced since construction is metered as S3 and is not eligible
                var replaced = _cleanup.StorageChanged(_options.StorageLocation);
                var paging = replaced ? ListingPaging.S3 : _cleanup.Store.Store.Paging;
                // Queued only to a worker whose listings and targets qualify, which eligibility implies, while maintenance deletes are on
                var queued = !deletesDisabled && !replaced && _cleanup.Store.Eligible;
                // The drain and the staged sweep run with checkpoints off too
                var cleanup = new DeltaTableCleanup(_cleanup, MeteredFileStorage.Wrap(_options.StorageLocation, budget.Buckets, paging), _options.TimeProvider, _tablePath,
                    queued ? budget.Discards : null, () => _options.StorageLocation, _discardCounters, Logger, _tableName, _sweepCounters, _retentionCounters);
                var cleanupEligible = _cleanup.Store.Eligible && !replaced;
                var workerSettings = new MaintenanceWorkerSettings(_options.CheckpointInterval, _options.CheckpointMaxAge, _options.TimeProvider)
                {
                    Codec = _options.CompressionCodec,
                    RebuildLegacyCheckpoints = _options.RebuildLegacyCheckpoints,
                    PeriodicEnabled = periodic,
                    // Retention needs the worker's inspection and repairs with periodic builds off too
                    InspectionEnabled = periodic || (_cleanup.OptIns.Retention != null && cleanupEligible),
                    Paging = paging,
                    CleanupEligible = cleanupEligible,
                    Retention = _cleanup.OptIns.Retention
                };
                WorkerSettingsForTests = workerSettings;
                var worker = new DeltaMaintenanceWorker(budget, workerSettings, _options.StorageLocation, _tablePath, _tableName, Logger)
                { Cleanup = cleanup };
                if (queued)
                {
                    // Formed from the sink's own storage; the worker forms it from its meter
                    discards = budget.Discards.Acquire(DiscardRoute.Of(_options.StorageLocation, _tablePath));
                }
                else
                {
                    // A run that cannot drain drops inherited hints, never deleting them inline
                    _discardCounters.AddDropped(budget.Discards.DropQueued(DiscardRoute.Of(_options.StorageLocation, _tablePath)));
                }
                budget.Register(worker);
                _worker = worker;
                mode = queued ? DiscardMode.Queued : DiscardMode.Inline;
            }
            catch (Exception e)
            {
                discards?.Dispose();
                discards = null;
                _worker = null;
                if (budget != null)
                {
                    budget.InitFailed = true;
                }
                Logger.LogWarning(e, "Delta table {table} runs without checkpoint maintenance: {reason}", _tableName, MaintenanceReason.InitFailed);
            }
            // The new handle is taken before the earlier run's goes, so a route both hold keeps its hints
            var previous = _discards;
            _discards = discards;
            previous?.Dispose();
            _discardMode = mode;
            StartedWorkerForTests = _worker;
            if (!_maintenanceGaugesCreated)
            {
                _maintenanceGaugesCreated = true;
                Metrics.CreateObservableGauge("delta_checkpoint_version", () => _worker?.LastCompleted ?? -1);
                Metrics.CreateObservableGauge("delta_checkpoint_lag_versions", () => _worker?.LagVersions ?? 0);
                Metrics.CreateObservableGauge("delta_checkpoint_oldest_uncheckpointed_seconds", () => _worker?.OldestUncheckpointedSeconds ?? 0);
                Metrics.CreateObservableGauge("delta_checkpoint_consecutive_failures", () => _worker?.ConsecutiveFailures ?? 0);
                Metrics.CreateObservableGauge("delta_checkpoint_skipped", () => _worker?.Skipped == true ? 1 : 0);
                Metrics.CreateObservableGauge("delta_checkpoint_dangling_hint", () => _worker?.RepairDue != null ? 1 : 0);
                Metrics.CreateObservableGauge("delta_checkpoint_last_duration_ms", () => _worker?.LastDurationMs ?? 0);
                Metrics.CreateObservableGauge("delta_checkpoint_last_validation_ms", () => _worker?.LastBuild?.ValidationMs ?? 0);
                Metrics.CreateObservableGauge("delta_checkpoint_last_spill_bytes", () => _worker?.LastBuild?.SpillBytes ?? 0);
                Metrics.CreateObservableGauge("delta_checkpoint_last_transfer_bytes", () => _worker?.LastBuild?.TransferBytes ?? 0);
                Metrics.CreateObservableGauge("delta_checkpoint_last_mode", () => _worker?.LastBuild?.Mode == BuildMode.Sort ? 1 : 0);
                Metrics.CreateObservableGauge("delta_checkpoint_inherited_gap", () => _worker?.LastBuild?.InheritedGap == true ? 1 : 0);
                Metrics.CreateObservableGauge("delta_cleanup_reserved_floor", () => _worker?.Fence.ReservedFloor ?? ControlFence.NoReservation);
                Metrics.CreateObservableGauge("delta_cleanup_uninterpreted_controls", () => _worker?.Fence.Uninterpreted ?? 0);
                // The array first: a stopping worker is cleared before it is added, so its job is never counted twice
                Metrics.CreateObservableGauge("delta_checkpoint_detached_jobs", () => _stoppedWorkers.Sum(x => x.DetachedJobs) + (_worker?.DetachedJobs ?? 0));
                Metrics.CreateObservableGauge("delta_cleanup_discard_queued_mode", () => _discardMode == DiscardMode.Queued ? 1 : 0);
                Metrics.CreateObservableGauge("delta_cleanup_discard_hints", () => _discards?.Entry.Hints ?? 0);
                Metrics.CreateObservableGauge("delta_cleanup_discard_dropped", () => _discardCounters.Dropped);
                Metrics.CreateObservableGauge("delta_cleanup_discard_deleted", () => _discardCounters.Deleted + _discardCounters.Absent);
                Metrics.CreateObservableGauge("delta_cleanup_discard_kept", () => _discardCounters.Kept);
                Metrics.CreateObservableGauge("delta_cleanup_discard_failed", () => _discardCounters.Failed);
                Metrics.CreateObservableGauge("delta_cleanup_discard_leaked", () => _discardCounters.Leaked);
                Metrics.CreateObservableGauge("delta_cleanup_sweep_leaked", () => _sweepCounters.PayloadLeaked);
                Metrics.CreateObservableGauge("delta_cleanup_sweep_leaked_stages", () => _sweepCounters.LeakedStages);
                Metrics.CreateObservableGauge("delta_retention_commit_failures", () => MetadataCommitFailures);
                Metrics.CreateObservableGauge("delta_retention_oldest_version", () => TableCleanup?.OldestVersion ?? -1);
                Metrics.CreateObservableGauge("delta_retention_adoption_version", () => TableCleanup?.AdoptionVersion ?? -1);
                Metrics.CreateObservableGauge("delta_retention_state", () => (int)RetentionGaugeState());
                Metrics.CreateObservableGauge("delta_retention_backlog_seconds", () => TableCleanup?.BacklogSeconds ?? -1);
                Metrics.CreateObservableGauge("delta_retention_repair_wait_seconds", () => _worker?.RepairWaitSeconds ?? 0);
                Metrics.CreateObservableGauge("delta_retention_repair_blocked", () => _worker?.CoverageState == RetentionState.RepairBlocked ? 1 : 0);
                Metrics.CreateObservableGauge("delta_retention_payload_remaining", () => TableCleanup?.PayloadRemaining ?? 0);
                Metrics.CreateObservableGauge("delta_retention_failed_slices", () => TableCleanup?.FailedSlices ?? 0);
                Metrics.CreateObservableGauge("delta_retention_last_gate_ms", () => _retentionCounters.LastGateMs);
                Metrics.CreateObservableGauge("delta_retention_last_slice_ms", () => _retentionCounters.LastSliceMs);
                Metrics.CreateObservableGauge("delta_cleanup_path_rejections", () => _retentionCounters.PathRejections);
                Metrics.CreateObservableGauge("delta_cleanup_deletes", () => _retentionCounters.DeleteMeasurements());
                Metrics.CreateObservableGauge("delta_cleanup_listing_declines", () => _retentionCounters.DeclineMeasurements());
                Metrics.CreateObservableGauge("delta_cleanup_successor_probes", () => _retentionCounters.ProbeMeasurements());
                Metrics.CreateObservableGauge("delta_cleanup_orphan_listed", () => _retentionCounters.OrphanListed);
                Metrics.CreateObservableGauge("delta_cleanup_orphan_kept", () => _retentionCounters.OrphanKeptMeasurements());
                Metrics.CreateObservableGauge("delta_cleanup_orphan_barrier", () => _retentionCounters.BarrierMeasurements());
                Metrics.CreateObservableGauge("delta_cleanup_orphan_candidate_probes", () => _retentionCounters.CandidateProbeMeasurements());
            }
        }

        // The current run's cleanup, null while no worker runs
        private DeltaTableCleanup? TableCleanup => _worker?.Cleanup as DeltaTableCleanup;

        // Off without a worker; both parts from one read of it
        private RetentionState RetentionGaugeState()
        {
            var worker = _worker;
            return worker == null ? RetentionState.Off : RetentionStates.Of(worker.CoverageState, (worker.Cleanup as DeltaTableCleanup)?.AdvanceGaugeState ?? RetentionState.Idle);
        }

        internal RetentionCounters RetentionCountersForTests => _retentionCounters;

        // Released at dispose; the entry outlives a restart only while another handle, a producer or a barrier uses it
        private void HoldProducers(DeltaMaintenanceBudget budget)
        {
            string key;
            try
            {
                key = TableLeases.KeyOf(_options.StorageLocation, _tablePath);
            }
            catch (Exception e)
            {
                Logger.LogWarning(e, "Delta table {table} writes without producer coordination: {reason}", _tableName, MaintenanceReason.InitFailed);
                return;
            }
            if (_producers != null && ReferenceEquals(_producersBudget, budget) && _producers.Key == key)
            {
                return;
            }
            _producers?.Dispose();
            _producers = budget.Producers.Acquire(key);
            _producersBudget = budget;
        }

        // The producer entry this sink writes outputs under, null without coordination
        internal ProducerHandle? ProducersForTests => _producers;

        internal CleanupConfiguration CleanupForTests => _cleanup;

        /// <summary>
        /// The stopped workers whose detached jobs still run, the newly stopped one included.
        /// </summary>
        internal static DeltaMaintenanceWorker[] Detached(DeltaMaintenanceWorker[] stopped, DeltaMaintenanceWorker? stopping)
        {
            return (stopping != null ? stopped.Append(stopping) : stopped).Where(x => x.DetachedJobs > 0).ToArray();
        }

        // The engine disposes the sink before it initializes it again, so both paths keep a worker whose job still runs
        private async Task StopWorker()
        {
            var worker = _worker;
            if (worker == null)
            {
                return;
            }
            await worker.StopAsync(s_maintenanceJoin);
            // Cleared before the array is replaced, so the gauge never counts the job twice
            _worker = null;
            _stoppedWorkers = Detached(_stoppedWorkers, worker);
        }

        // Maintenance never makes the dispose throw
        public override async ValueTask DisposeAsync()
        {
            await StopWorker();
            _producers?.Dispose();
            _producers = null;
            _producersBudget = null;
            // A detached drain keeps the route's hints until it settles; otherwise they are dropped here
            _discards?.Dispose();
            _discards = null;
            _catalog?.Dispose();
            _catalog = null;
            await base.DisposeAsync();
        }

        protected override async Task OnCheckpoint(long checkpointTime)
        {
            Debug.Assert(_firstInsertDone != null);
            Debug.Assert(_pendingCommit != null);
            Debug.Assert(_bufferDurable != null);
            Debug.Assert(_catalog != null);

            // Another table may need the memory, applied here so a halted sink still gives it up
            _catalog.ApplyRevocation();

            // Stop skipped the commit, rows wait in the tree.
            if (_pendingCommit.Value == null)
            {
                await SaveData();
            }
            else
            {
                MarkFailedClosed(_pendingCommit.Value);
                await KeepWaitingRows();
            }
            await _firstInsertDone.Commit();
            await _pendingCommit.Commit();
            await _bufferDurable.Commit();
        }

        // A halted sink's waiting stage, marked once for the worker to drop its hints
        private void MarkFailedClosed(DeltaLakePendingCommit pending)
        {
            if (_halted && _discards is { } discards && pending.StageId is { } stage && stage != _failedClosedStage)
            {
                _failedClosedStage = stage;
                discards.Entry.MarkFailedClosed(new FailedClosedMark(pending.Version, stage));
            }
        }

        // Rows that wait for an unpublished commit must survive a restart, the checkpoint already moved the sources past them
        private async Task KeepWaitingRows()
        {
            Debug.Assert(_temporaryTree != null);
            Debug.Assert(_bufferDurable != null);

            if (_bufferDurable.Value || await HasRows())
            {
                TemporaryTreeCommitHookForTests?.Invoke(_tableName);
                await _temporaryTree.Commit();
                _bufferDurable.Value = true;
            }
        }

        private async Task<bool> HasRows()
        {
            Debug.Assert(_temporaryTree != null);

            using var iterator = _temporaryTree.CreateIterator();
            await iterator.SeekFirst();
            await foreach (var page in iterator)
            {
                if (page.Values.Data.Count > 0)
                {
                    return true;
                }
            }
            return false;
        }

        // Empties the tree after its rows were staged
        private async Task ClearRows()
        {
            Debug.Assert(_temporaryTree != null);
            Debug.Assert(_bufferDurable != null);

            if (!_bufferDurable.Value)
            {
                await _temporaryTree.Clear();
                return;
            }

            // Clear would orphan the committed pages, deletes release them
            while (true)
            {
                EventBatchData? keys = null;
                using (var iterator = _temporaryTree.CreateIterator())
                {
                    await iterator.SeekFirst();
                    await foreach (var page in iterator)
                    {
                        if (page.Keys.Data.Count > 0)
                        {
                            var columns = new IColumn[page.Keys.Data.Columns.Count];
                            for (int i = 0; i < columns.Length; i++)
                            {
                                columns[i] = page.Keys.Data.Columns[i].Copy(MemoryAllocator);
                            }
                            keys = new EventBatchData(columns);
                            break;
                        }
                    }
                }
                if (keys == null)
                {
                    break;
                }
                using (keys)
                {
                    for (int i = 0; i < keys.Count; i++)
                    {
                        var key = new ColumnRowReference() { referenceBatch = keys, RowIndex = i };
                        await _temporaryTree.Delete(in key);
                    }
                }
            }
            TemporaryTreeCommitHookForTests?.Invoke(_tableName);
            await _temporaryTree.Commit();
            _bufferDurable.Value = false;
        }

        public override async Task CommitVersion(long version)
        {
            Debug.Assert(_pendingCommit != null);

            var pendingCommit = _pendingCommit.Value;
            var outcome = pendingCommit != null && IsPublishable(pendingCommit, version) ? await Publish(pendingCommit) : (PublishOutcome?)null;
            // One hand-off for every outcome, decided by HandOffVersion
            HandOff(HandOffVersion(pendingCommit, version, outcome, _validatedHead));
            _options.CommitVersionHookForTests?.Invoke(pendingCommit?.Version, outcome, _worker?.Target ?? -1);
        }

        private async Task<PublishOutcome> Publish(DeltaLakePendingCommit pendingCommit)
        {
            // The first attempt of a commit staged in this run follows the listing that chose its version
            var checkSuccessor = _stagedBytes == null || _halted;
            var result = await DeltaTransactionWriter.PublishCommit(_options.StorageLocation, _tablePath, pendingCommit, _stagedBytes, checkSuccessor);
            if (result.Outcome == PublishOutcome.FailedClosed)
            {
                Halt(result.Reason!);
                return result.Outcome;
            }
            if (_halted)
            {
                _halted = false;
                SetHealth(true);
                Logger.LogInformation("Delta table {table} published version {version}, the sink continues", _tableName, pendingCommit.Version);
            }
            // Applied by the next SaveData, never while OnRecieve runs
            if (_stagedOverlay != null)
            {
                _catalog!.MarkPublished(_stagedOverlay);
                _stagedOverlay = null;
            }
            _stagedBytes = null;
            _pendingCommit!.Value = null;
            return result.Outcome;
        }

        // One InterlockedMax; the build runs in the background
        private void HandOff(long version)
        {
            if (version >= 0)
            {
                _worker?.Published(version);
            }
        }

        internal DeleteStatistics? DeleteStatisticsForTests => _deleteStatistics;

        /// <summary>
        /// The version CommitVersion hands off, -1 for none: the validated head when nothing is pending, the commit once it is published.
        /// </summary>
        internal static long HandOffVersion(DeltaLakePendingCommit? pending, long version, PublishOutcome? outcome, long validatedHead)
        {
            if (pending == null)
            {
                // The head this run validated is durable, which arms the worker after an idle start
                return validatedHead;
            }
            if (!IsPublishable(pending, version) || outcome is null or PublishOutcome.FailedClosed)
            {
                return -1;
            }
            return pending.Version;
        }

        /// <summary>
        /// A stop drain cycle stages after the stop checkpoint, its rows are replayed after the restart and must not be published.
        /// </summary>
        internal static bool IsPublishable(DeltaLakePendingCommit pending, long version)
        {
            return !pending.CheckpointId.HasValue || pending.CheckpointId.Value <= version;
        }

        // The sink keeps buffering rows, a later checkpoint publishes once the conflict is gone
        private void Halt(string reason)
        {
            // Logged again only when the reason changes
            if (_halted && reason == _haltReason)
            {
                return;
            }
            _halted = true;
            _haltReason = reason;
            SetHealth(false);
            Logger.LogError("Delta table {table} stopped publishing: {reason}. Rows are kept and publishing is tried again at every checkpoint. Remove the conflicting commit file while no later version exists, or reset the stream state, which writes every row again: INSERT OVERWRITE replaces the table, INSERT INTO appends to it", _tableName, reason);
        }

        private DeltaParquetCodec ResolveCodec(IReadOnlyDictionary<string, string>? configuration)
        {
            var codec = DeltaParquetCompression.Resolve(configuration, _options.CompressionCodec, out var unknownValue);
            if (unknownValue != null && _unknownCodecs.Add(unknownValue))
            {
                Logger.LogWarning("Delta table {table} has the unsupported compression codec {codec}, zstd is used instead", _tableName, unknownValue);
            }
            return codec;
        }

        private void Validated()
        {
            _validatedHead = _catalog!.Head;
            _worker?.SetTableId(_catalog.Header?.Metadata.Id);
        }

        private async Task SaveData()
        {
            Debug.Assert(_temporaryTree != null);
            Debug.Assert(_firstInsertDone != null);
            Debug.Assert(_pendingCommit != null);
            Debug.Assert(_catalog != null);

            await _catalog.ApplyPublished();
            // Read once; a staged commit acknowledges it to this worker
            var anchorWorker = _worker;
            var anchor = anchorWorker?.AnchorRequest ?? 0;
            if (_catalog.IsReady && CatalogHooksForTests.TryGetValue(_tableName, out var hook))
            {
                await hook(_catalog);
            }
            var readOptions = new DeltaReadOptions() { SkipTombstones = true, Logger = Logger, ReportedCheckpointFailures = _reportedCheckpointFailures };
            var overwrite = !_firstInsertDone.Value && _writeRelation.Overwrite;
            // A first read lists the log itself
            var validated = false;
            if (!_catalog.IsReady)
            {
                // A failed attempt can leave the catalog unready; without rows the retry stays contained
                if (_metadataRetry && !overwrite && !await HasRows())
                {
                    try
                    {
                        await _catalog.Validate(readOptions);
                    }
                    catch (Exception e)
                    {
                        MetadataCommitFailed(e);
                        return;
                    }
                }
                else
                {
                    await _catalog.Validate(readOptions);
                }
                Validated();
                validated = true;
            }
            if (_catalog.Header != null && !overwrite && !await HasRows())
            {
                // Uncontained, before any metadata attempt
                await ClearRows();
                _firstInsertDone.Value = true;
                if (anchor == 0 && !PropertiesPending(_catalog.Header))
                {
                    return;
                }
                // Properties or an anchor without rows never fail the checkpoint
                try
                {
                    await WriteCommit(readOptions, validated, overwrite, true, anchorWorker, anchor);
                }
                catch (Exception e)
                {
                    MetadataCommitFailed(e);
                }
                return;
            }
            await WriteCommit(readOptions, validated, overwrite, false, anchorWorker, anchor);
        }

        // Retention on and the table's two keys missing or different; two lookups
        private bool PropertiesPending(DeltaTable header)
        {
            return _retentionValue != null && !RetentionProperties.Matches(header.Metadata.Configuration, _retentionValue);
        }

        private void MetadataCommitFailed(Exception e)
        {
            Interlocked.Increment(ref _metadataCommitFailures);
            _metadataRetry = true;
            Logger.Log(_metadataCommitWarned ? LogLevel.Debug : LogLevel.Warning, e, "Delta table {table} could not stage its retention properties or anchor commit, it is tried again at the next checkpoint", _tableName);
            _metadataCommitWarned = true;
        }

        // With metadataOnly the temporary tree holds no rows
        private async Task WriteCommit(DeltaReadOptions readOptions, bool validated, bool overwrite, bool metadataOnly, DeltaMaintenanceWorker? anchorWorker, long anchor)
        {
            Debug.Assert(_temporaryTree != null);
            Debug.Assert(_limits != null);
            Debug.Assert(_firstInsertDone != null);
            Debug.Assert(_pendingCommit != null);
            Debug.Assert(_catalog != null);

            // Before the version, the codec or any file is chosen, another writer may have moved the head
            if (!validated)
            {
                await _catalog.Validate(readOptions);
                Validated();
            }
            var table = _catalog.Header;

            using var iterator = _temporaryTree.CreateIterator();
            await iterator.SeekFirst();

            long nextVersion = 0;
            DeltaTable? newHeader = null;
            List<DeltaAction> actions = new List<DeltaAction>();
            var currentTime = _options.TimeProvider.GetUtcNow().ToUnixTimeMilliseconds();
            var stageId = Guid.NewGuid().ToString("N");
            // The paths this commit writes fresh, recorded in its commitInfo
            var created = new List<string>();
            // Change data files the attempt wrote and then dropped from its actions
            string[]? discarded = null;

            bool changeDataEnabled = false;
            bool deletionVectorEnabled = false;
            bool columnMappingEnabled = false;

            int maxColumnId = 0;

            StructType? schema;
            IReadOnlyDictionary<string, string>? configuration;
            DeltaMetadataAction? updatedMetadata = null;
            if (table == null)
            {
                changeDataEnabled = _options.WriteChangeDataOnNewTables;
                deletionVectorEnabled = _options.EnableDeletionVectorsOnNewTables;
                columnMappingEnabled = _options.EnableColumnMappingOnNewTables;

                // Create schema
                schema = SubstraitTypeToDeltaType.GetSchema(_writeRelation.TableSchema, ref maxColumnId, columnMappingEnabled);

                var jsonOptions = new JsonSerializerOptions();
                jsonOptions.Converters.Add(new TypeConverter());
                var schemaString = JsonSerializer.Serialize(schema as SchemaBaseType, jsonOptions);

                actions.Add(new DeltaAction()
                {
                    CommitInfo = new DeltaCommitInfoAction()
                    {
                        StageId = stageId,
                        AdoptedAt = 0,
                        Timestamp = currentTime,
                        Operation = "CREATE TABLE",
                        CreatedFiles = created
                    }
                });

                var tableConfiguration = new Dictionary<string, string>();
                configuration = tableConfiguration;

                if (changeDataEnabled)
                {
                    tableConfiguration.Add("delta.enableChangeDataFeed", "true");
                }
                if (deletionVectorEnabled)
                {
                    tableConfiguration.Add("delta.enableDeletionVectors", "true");
                }
                if (columnMappingEnabled)
                {
                    tableConfiguration.Add("delta.columnMapping.mode", "name");
                    tableConfiguration.Add("delta.columnMapping.maxColumnId", maxColumnId.ToString());
                }
                if (_retentionValue != null)
                {
                    tableConfiguration.Add(RetentionProperties.LogKey, _retentionValue);
                    tableConfiguration.Add(RetentionProperties.DeletedFileKey, _retentionValue);
                }

                var metadataAction = new DeltaMetadataAction()
                {
                    Id = Guid.NewGuid().ToString(),
                    SchemaString = schemaString,
                    Configuration = tableConfiguration,
                    Format = new DeltaMetadataFormat()
                    {
                        Provider = "parquet",
                        Options = new Dictionary<string, string>()
                    },
                    PartitionColumns = new List<string>(),
                    CreatedTime = currentTime
                };
                actions.Add(new DeltaAction()
                {
                    MetaData = metadataAction
                });

                var writerFeatures = new List<string>();
                if (deletionVectorEnabled)
                {
                    writerFeatures.Add("deletionVectors");
                }
                if (changeDataEnabled)
                {
                    writerFeatures.Add("changeDataFeed");
                }

                var readerFeatures = new List<string>();
                if (deletionVectorEnabled)
                {
                    readerFeatures.Add("deletionVectors");
                }
                if (columnMappingEnabled)
                {
                    writerFeatures.Add("columnMapping");
                    readerFeatures.Add("columnMapping");
                }

                var protocolAction = new DeltaProtocolAction()
                {
                    MinReaderVersion = 3,
                    MinWriterVersion = 7,
                    ReaderFeatures = readerFeatures,
                    WriterFeatures = writerFeatures
                };
                actions.Add(new DeltaAction()
                {
                    Protocol = protocolAction
                });
                newHeader = new DeltaTable(metadataAction, protocolAction, new List<DeltaAddAction>(), schema, 0, System.Array.Empty<DeltaRemoveFileAction>(), new Dictionary<string, DeltaTransactionAction>(), new Dictionary<string, DeltaDomainMetadataAction>(), null);
            }
            else
            {
                schema = table.Schema;
                configuration = table.Metadata.Configuration;
                nextVersion = _catalog.Head + 1;
                changeDataEnabled = table.ChangeDataEnabled;

                var head = _catalog.HeadProvenance;
                actions.Add(new DeltaAction()
                {
                    CommitInfo = new DeltaCommitInfoAction()
                    {
                        StageId = stageId,
                        AdoptedAt = CommitProvenance.NextEpoch(head, nextVersion),
                        Timestamp = CommitProvenance.NextTimestamp(currentTime, head),
                        Operation = metadataOnly ? "SET TBLPROPERTIES" : "WRITE",
                        CreatedFiles = created
                    }
                });

                // After commitInfo, which must stay first; recomputed against the validated header
                if (anchor != 0 || PropertiesPending(table))
                {
                    updatedMetadata = DeltaMetadataCopy.Copy(table.Metadata, _retentionValue);
                    actions.Add(new DeltaAction() { MetaData = updatedMetadata });
                }

                // A metadata-only commit writes no file and copies the partition columns
                if (table.PartitionColumns.Count > 0 && !metadataOnly)
                {
                    throw new NotImplementedException("Partition columns are not implemented yet");
                }

            }

            var overlay = _catalog.BeginCommit(nextVersion, newHeader);
            overlay.UpdatedMetadata = updatedMetadata;
            var commitInfo = actions[0].CommitInfo!;
            overlay.Provenance = new CommitProvenance(nextVersion, stageId, commitInfo.AdoptedAt, commitInfo.Timestamp);
            // Every output of this attempt is written inside one producer generation, released once it is staged or failed
            var producer = _producers?.Enter() ?? default;
            // Once the stage write starts it may exist, so nothing is deleted
            var staging = false;
            try
            {
                if (table != null && overwrite)
                {
                    await foreach (var (_, record) in _catalog.ScanFiles())
                    {
                        actions.Add(new DeltaAction()
                        {
                            Remove = new DeltaRemoveFileAction()
                            {
                                Path = record.Path,
                                DeletionVector = record.DeletionVector,
                                DataChange = true,
                                DeletionTimestamp = currentTime,
                                Stats = record.Statistics,
                                Size = record.Size,
                                PartitionValues = record.PartitionValues,
                            }
                        });
                    }
                    overlay.RemoveAll = true;
                    _catalog.RestartIds();
                }

                // Row writers, none for a metadata-only commit
                if (!metadataOnly)
                {
                    var codec = ResolveCodec(configuration);
                    _fileExtension = codec.FileExtension;
                    var writer = new ParquetSharpWriter(schema, _writeRelation.TableSchema.Names, writerProperties: codec.WriterProperties);
                    // Only used to compare deleted rows, never written
                    var deleteWriter = new ParquetSharpWriter(schema, _writeRelation.TableSchema.Names, countNulls: true);

                    ParquetSharpWriter? cdcWriter = default;
                    if (changeDataEnabled)
                    {
                        cdcWriter = new ParquetSharpWriter(schema, _writeRelation.TableSchema.Names, isCdcWriter: true, writerProperties: codec.WriterProperties);
                        cdcWriter.NewBatch();
                    }

                    writer.NewBatch();
                    deleteWriter.NewBatch();

                    // Flag that tracks if any delete was written, if this is false, no cdc file is required even if it is enabled.
                    bool deleteWritten = false;

                    // Copies of each deleted row and their probes
                    var demand = new List<long>();
                    _probes ??= new ProbeBatch(_catalog.ScanLayout);
                    _probes.Clear();
                    _probes.SetLayout(_catalog.ScanLayout);
                    var deletes = new DeleteRun(_options.StorageLocation, _tablePath);
                    // The counted table, probe views and unwritten nulls per row
                    var rowBytes = 32 + 4 * _catalog.ScanLayout.Columns.Count + (cdcWriter != null ? 8 : 0) + deleteWriter.NullBytesPerRow * _limits.WrittenSlack;
                    await foreach (var page in iterator)
                    {
                        for (int i = 0; i < page.Values.Data.Count; i++)
                        {
                            var weight = page.Values.Data[i];
                            if (weight < 0)
                            {
                                deleteWritten = true;
                                var rowRef = new ColumnRowReference() { referenceBatch = page.Keys.Data, RowIndex = i };
                                deleteWriter.AddRow(rowRef);
                                if (cdcWriter != null)
                                {
                                    // One change row per deleted copy, rolled per copy so a large weight stays bounded
                                    for (int v = 0; v > weight; v--)
                                    {
                                        cdcWriter.AddRow(rowRef, true);
                                        if (IsFull(cdcWriter))
                                        {
                                            await WriteNewCdcFile(cdcWriter, actions, created);
                                        }
                                    }
                                }

                                demand.Add(-(long)weight);
                                _probes.Add(rowRef);
                                if (deleteWriter.WrittenCount >= _limits.RowCap || deleteWriter.WrittenBytes * _limits.WrittenSlack + (long)deleteWriter.WrittenCount * rowBytes >= _limits.ByteCap)
                                {
                                    await ResolveBatch(overwrite, cdcWriter != null, table, deleteWriter, demand, deletes);
                                }
                            }
                            else
                            {
                                // One row per copy, rolled per copy so a large weight stays bounded
                                var rowRef = new ColumnRowReference() { referenceBatch = page.Keys.Data, RowIndex = i };
                                for (int v = 0; v < weight; v++)
                                {
                                    writer.AddRow(rowRef);
                                    if (IsFull(writer))
                                    {
                                        await WriteNewFile(writer, actions, currentTime, overlay, created);
                                    }
                                    if (cdcWriter != null)
                                    {
                                        cdcWriter.AddRow(rowRef);
                                        if (IsFull(cdcWriter))
                                        {
                                            await WriteNewCdcFile(cdcWriter, actions, created);
                                        }
                                    }
                                }
                            }
                        }
                    }

                    if (deleteWriter.WrittenCount > 0)
                    {
                        await ResolveBatch(overwrite, cdcWriter != null, table, deleteWriter, demand, deletes);
                    }
                    _deleteStatistics = deletes.Statistics;

                    // Inserts first, so their files stay in key order
                    if (writer.WrittenCount > 0)
                    {
                        await WriteNewFile(writer, actions, currentTime, overlay, created);
                    }

                    if (deletes.Claims.Count > 0)
                    {
                        if (table == null)
                        {
                            throw new InvalidOperationException("Table should not be null when delete is found");
                        }
                        var output = new DeleteOutput(_options.StorageLocation, _tablePath, _tableName, schema, table.ColumnMappingById, table.DeleteVectorEnabled, codec.WriterProperties, IsFull, rewriter => WriteNewFile(rewriter, actions, currentTime, overlay, created));
                        await output.WriteAsync(deletes.Claims, deletes.Records, deletes.FooterRows, actions, currentTime, overlay, created);
                    }
                    if (cdcWriter != null)
                    {
                        if (deleteWritten)
                        {
                            if (cdcWriter.WrittenCount > 0)
                            {
                                await WriteNewCdcFile(cdcWriter, actions, created);
                            }
                        }
                        else
                        {
                            // Remove written cdc files since there was no delete, the rows are included in the add actions
                            discarded = RemoveWrittenCdcFiles(actions, created);
                            if (_discardMode == DiscardMode.Inline)
                            {
                                await DeleteDiscardedInline(discarded);
                            }
                        }
                    }
                }

                if (table == null || HasChanges(actions))
                {
                    // Published in CommitVersion once the version is final.
                    staging = true;
                    var staged = await DeltaTransactionWriter.StageCommit(_options.StorageLocation, _tablePath, nextVersion, actions);
                    _pendingCommit.Value = new DeltaLakePendingCommit()
                    {
                        Version = nextVersion,
                        StagedFile = staged.FileName,
                        StageId = staged.StageId,
                        Length = staged.Length,
                        CheckpointId = CurrentCheckpointId
                    };
                    _stagedBytes = staged.Bytes;
                    _stagedOverlay = overlay;
                    if (anchor != 0)
                    {
                        anchorWorker!.AnchorStaged(anchor, nextVersion);
                    }
                    if (discarded != null && _discardMode == DiscardMode.Queued)
                    {
                        // An existing table's id may be null, a claim that never matches
                        var tableId = table != null ? table.Metadata.Id : newHeader!.Metadata.Id;
                        QueueDiscards(new DiscardGroup(tableId, commitInfo.AdoptedAt!.Value, nextVersion, stageId, discarded) { Counters = _discardCounters, QueuedAt = _options.TimeProvider.GetUtcNow() });
                    }
                }
                else if (discarded is { Length: > 0 } && _discardMode == DiscardMode.Queued)
                {
                    DroppedDiscards(discarded.Length, "nothing was staged to hand them off with");
                }
            }
            // Before staging, the attempt's files are deleted and the failure surfaces
            catch (Exception) when (!staging)
            {
                await DeleteDiscardedInline(created.ToArray());
                throw;
            }
            finally
            {
                producer.Exit();
            }

            // Last thing we do is clear the temporary tree, if the write fails we might need the tree again to recompute the files
            await ClearRows();
            // Set that the first insert is done, this is used to determine if we need to write delete files for overwrite writes
            _firstInsertDone.Value = true;
        }

        // A commit without file, metadata or protocol actions is not written
        internal static bool HasChanges(List<DeltaAction> actions)
        {
            foreach (var action in actions)
            {
                if (action.Add != null || action.Remove != null || action.Cdc != null || action.MetaData != null || action.Protocol != null || action.DomainMetadata != null)
                {
                    return true;
                }
            }
            return false;
        }

        // Every discarded change file leaves the commit and its ownership before any is deleted or handed off
        private static string[] RemoveWrittenCdcFiles(List<DeltaAction> actions, List<string> created)
        {
            var discarded = new HashSet<string>(StringComparer.Ordinal);
            actions.RemoveAll(action =>
            {
                if (action.Cdc == null)
                {
                    return false;
                }
                discarded.Add(action.Cdc.Path!);
                return true;
            });
            created.RemoveAll(discarded.Contains);
            return discarded.ToArray();
        }

        // The inline fallback: before staging, through the current storage, each by its exact qualified name; never fails ingestion
        private async Task DeleteDiscardedInline(string[] discarded)
        {
            var storage = _options.StorageLocation;
            if (_inlineExecutor == null || !ReferenceEquals(_inlineExecutor.Storage, storage))
            {
                _inlineExecutor = new DeletionExecutor(_cleanup, storage);
            }
            var result = await InlineDiscards.DeleteAsync(_inlineExecutor, discarded, _discardCounters);
            if (result.Failed > 0)
            {
                Logger.Log(_discardFailureWarned ? LogLevel.Debug : LogLevel.Warning, result.FirstError, "Delta table {table} could not delete {count} discarded files, they are left behind", _tableName, result.Failed);
                _discardFailureWarned = true;
            }
            if (result.Kept > 0)
            {
                Logger.Log(_discardKeptWarned ? LogLevel.Debug : LogLevel.Warning, "Delta table {table} keeps {count} discarded files: {reason}. {keep}", _tableName, result.Kept, MaintenanceReason.PathRejected, result.FirstKept);
                _discardKeptWarned = true;
            }
        }

        // Handed off after the stage exists, without a lock; an unroutable or overflowing hint is dropped and counted, never deleted inline
        private void QueueDiscards(DiscardGroup group)
        {
            var route = _discards;
            var storage = _options.StorageLocation;
            if (route == null || storage == null || !ReferenceEquals(RawStorage.Unwrap(storage), route.Entry.Route.Raw))
            {
                if (group.Paths.Count > 0)
                {
                    DroppedDiscards(group.Paths.Count, "the storage instance changed during the run");
                }
                return;
            }
            // Read first, the queued group keeps only its admitted paths
            var count = group.Paths.Count;
            var dropped = route.Entry.Enqueue(group);
            _discardCounters.AddQueued(count - dropped);
            if (dropped > 0)
            {
                _discardCounters.AddDropped(dropped);
                Logger.Log(_discardDroppedWarned ? LogLevel.Debug : LogLevel.Warning, "Delta table {table} dropped {count} discarded change data files from the cleanup hand-off: {reason}. They are left behind", _tableName, dropped, MaintenanceReason.DeleteBacklog);
                _discardDroppedWarned = true;
            }
        }

        private void DroppedDiscards(int count, string why)
        {
            _discardCounters.AddDropped(count);
            _discards?.Entry.Owner.AddDropped(count);
            Logger.Log(_discardDroppedWarned ? LogLevel.Debug : LogLevel.Warning, "Delta table {table} dropped {count} discarded change data files from the cleanup hand-off: {why}. They are left behind", _tableName, count, why);
            _discardDroppedWarned = true;
        }

        // Error messages only, enumerates each column up to the row
        private static string DescribeRow(RecordBatch batch, int index)
        {
            var values = new List<string>(batch.ColumnCount);
            for (int c = 0; c < batch.ColumnCount; c++)
            {
                var array = batch.Column(c);
                try
                {
                    values.Add(array.IsNull(index) ? "null" : array switch
                    {
                        Decimal128Array decimals => decimals.GetValue(index)!.Value.ToString(System.Globalization.CultureInfo.InvariantCulture),
                        System.Collections.IEnumerable items => items.Cast<object?>().ElementAt(index)?.ToString() ?? "null",
                        _ => array.Data.DataType.Name
                    });
                }
                catch (Exception)
                {
                    values.Add(array.Data.DataType.Name);
                }
            }
            return $"{{{string.Join(",", values)}}}";
        }

        // Match the batch against the candidates in id order
        private async Task ResolveBatch(bool overwrite, bool changeData, DeltaTable? table, ParquetSharpWriter deleteWriter, List<long> demand, DeleteRun deletes)
        {
            Debug.Assert(_catalog != null);
            Debug.Assert(_probes != null);
            Debug.Assert(_limits != null);

            if (table == null)
            {
                throw new InvalidOperationException("Table should not be null when delete is found");
            }
            using (var deleteBatch = deleteWriter.GetRecordBatch())
            {
                // The batch holds its own copy, so the builders can go now
                deleteWriter.NewBatch();
                deletes.Statistics.Passes++;
                // An overwrite removes every file, a delete in the same commit finds none
                var candidates = overwrite ? new Dictionary<int, List<int>>() : await _catalog.FindCandidates(_probes);
                var matched = new bool[deleteBatch.Length];
                foreach (var probes in candidates.Values)
                {
                    foreach (var probe in probes)
                    {
                        matched[probe] = true;
                    }
                }
                for (int i = 0; i < matched.Length; i++)
                {
                    if (!matched[i])
                    {
                        throw new InvalidOperationException($"Could not find any data file that contains the row {DescribeRow(deleteBatch, i)}");
                    }
                }

                var comparers = new IArrowComparer[table.Schema.Fields.Count];
                var visitor = new ArrowComparerVisitor();
                for (int i = 0; i < comparers.Length; i++)
                {
                    comparers[i] = table.Schema.Fields[i].Type.Accept(visitor);
                }
                var prefilter = _limits.Prefilter ? PrefilterColumn(table.Schema) : -1;
                var batch = new DeleteBatch(deleteBatch, demand, comparers, changeData, _limits.CollideHashes, prefilter);
                var matcher = new FileMatcher(_options.StorageLocation, _tablePath, _tableName, table.Schema, table.ColumnMappingById);
                var ids = candidates.Keys.ToArray();
                System.Array.Sort(ids);
                var missing = new List<int>();
                for (int start = 0; start < ids.Length && batch.Unresolved > 0; start += _limits.RecordGroup)
                {
                    var group = new ArraySegment<int>(ids, start, Math.Min(_limits.RecordGroup, ids.Length - start));
                    missing.Clear();
                    foreach (var id in group)
                    {
                        if (!deletes.Records.ContainsKey(id))
                        {
                            missing.Add(id);
                        }
                    }
                    foreach (var (id, record) in await _catalog.GetFiles(missing))
                    {
                        deletes.Records[id] = record;
                    }
                    foreach (var id in group)
                    {
                        if (batch.Unresolved == 0)
                        {
                            break;
                        }
                        await matcher.Scan(id, batch, deletes);
                    }
                }
            }
            demand.Clear();
            _probes.Clear();
        }

        // The first pruning column as a table column, -1 for none
        private int PrefilterColumn(StructType schema)
        {
            var columns = _catalog!.ScanLayout.Columns;
            if (columns.Count == 0)
            {
                return -1;
            }
            for (int i = 0; i < schema.Fields.Count; i++)
            {
                if (string.Equals(schema.Fields[i].Name, columns[0].Name, StringComparison.OrdinalIgnoreCase))
                {
                    return i;
                }
            }
            return -1;
        }

        private async Task WriteNewCdcFile(ParquetSharpWriter cdcWriter, List<DeltaAction> actions, List<string> created)
        {
            string addFilePath = $"_change_data/cdc-00000-{Guid.NewGuid().ToString()}{_fileExtension}";

            created.Add(addFilePath);
            var fileSize = await cdcWriter.WriteData(_options.StorageLocation, _tablePath, addFilePath);
            actions.Add(new DeltaAction()
            {
                Cdc = new DeltaCdcAction()
                {
                    // Change files never change table data (spec)
                    DataChange = false,
                    PartitionValues = new Dictionary<string, string>(),
                    Path = addFilePath,
                    Size = fileSize
                }
            });
            cdcWriter.NewBatch();
        }

        // The row cap bounds buffering when a byte estimate is wrong
        private bool IsFull(ParquetSharpWriter writer)
        {
            return writer.WrittenBytes >= _options.MaxFileSizeBytes || writer.WrittenCount >= MaxRowsPerFile;
        }

        private async Task WriteNewFile(ParquetSharpWriter writer, List<DeltaAction> actions, long currentTime, CatalogOverlay overlay, List<string> created)
        {
            string addFilePath = $"part-00000-{Guid.NewGuid().ToString()}{_fileExtension}";

            var stats = writer.GetStatistics();
            var statsString = JsonSerializer.Serialize(stats, overlay.StatisticsOptions);

            created.Add(addFilePath);
            var fileSize = await writer.WriteData(_options.StorageLocation, _tablePath, addFilePath);
            var add = new DeltaAddAction()
            {
                Path = addFilePath,
                PartitionValues = new Dictionary<string, string>(),
                Size = fileSize,
                ModificationTime = currentTime,
                DataChange = true,
                Statistics = statsString
            };
            actions.Add(new DeltaAction()
            {
                Add = add
            });
            overlay.Add(_catalog!.AllocateId(), add);
            writer.NewBatch();
        }

        protected override Task OnRecieve(StreamEventBatch msg, long time)
        {
            Debug.Assert(_intake != null);
            return _intake.Apply(msg.Data.EventBatchData, msg.Data.Weights);
        }
    }
}
