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

using FlowtideDotNet.Connector.DeltaLake.Internal.Delta;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Actions;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema.Types;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Stats;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.StateManager;
using Microsoft.Extensions.Logging;
using Stowage;
using System.Text.Json;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Catalog
{
    /// <summary>
    /// The sink's view of the live files, so a checkpoint does not replay the log.
    /// Bootstrapped from the log once per run, then changed only by this sink's published commits.
    /// A head moved by another writer is read again from the log.
    /// </summary>
    internal sealed class DeltaSinkCatalog : IDisposable
    {
        private readonly DeltaLakeOptions _options;
        private readonly IOPath _tablePath;
        private readonly IReadOnlyList<string> _writtenColumns;
        private readonly IMemoryAllocator _memoryAllocator;
        private readonly ILogger _logger;
        private readonly RotatingTree<DeltaFileRecord> _cold;
        private readonly RotatingTree<byte[]> _bounds;
        private readonly Stack<int> _freeIds = new Stack<int>();
        private PruningProjection? _projection;
        private PruningLayout _layout = new PruningLayout(Array.Empty<PruningColumn>());
        private JsonSerializerOptions? _statisticsOptions;
        private DeltaTable? _header;
        private CatalogOverlay? _published;
        private bool _ready;
        private long _head = -1;
        private long _adoptedAt;
        private int _nextId;
        private int _liveFiles;
        private bool _capacityWarned;
        // Tree mode lasts for the run, a later bootstrap does not take the arrays back
        private bool _treeModeForRun;

        private DeltaSinkCatalog(DeltaLakeOptions options, IOPath tablePath, IReadOnlyList<string> writtenColumns, IMemoryAllocator memoryAllocator, ILogger logger, RotatingTree<DeltaFileRecord> cold, RotatingTree<byte[]> bounds)
        {
            _options = options;
            _tablePath = tablePath;
            _writtenColumns = writtenColumns;
            _memoryAllocator = memoryAllocator;
            _logger = logger;
            _cold = cold;
            _bounds = bounds;
        }

        public static async Task<DeltaSinkCatalog> Open(IStateManagerClient stateManagerClient, DeltaLakeOptions options, IOPath tablePath, IReadOnlyList<string> writtenColumns, IMemoryAllocator memoryAllocator, ILogger logger)
        {
            var cold = await RotatingTree<DeltaFileRecord>.Open(stateManagerClient, "catalog_cold", new DeltaFileRecordSerializer(), memoryAllocator);
            var bounds = await RotatingTree<byte[]>.Open(stateManagerClient, "catalog_bounds", new BoundsRowSerializer(), memoryAllocator);
            return new DeltaSinkCatalog(options, tablePath, writtenColumns, memoryAllocator, logger, cold, bounds);
        }

        public bool IsReady => _ready;

        /// <summary>
        /// The table without its files, null while the table does not exist.
        /// </summary>
        public DeltaTable? Header => _header;

        public long Head => _head;

        public long AdoptedAt => _adoptedAt;

        public int LiveFiles => _liveFiles;

        /// <summary>
        /// The columns a delete batch captures and is matched on.
        /// </summary>
        public PruningLayout ScanLayout => _projection?.Layout ?? _layout;

        public bool TreeMode => _projection?.TreeMode ?? false;

        public long PruningBytes => _projection?.ChargedBytes ?? 0;

        public int Rotations => _cold.Rotations + _bounds.Rotations;

        public long RecordsMigrated => _cold.RecordsMigrated + _bounds.RecordsMigrated;

        public TimeSpan LastClearTime => _cold.LastClearTime > _bounds.LastClearTime ? _cold.LastClearTime : _bounds.LastClearTime;

        public TimeSpan LastSliceTime => _cold.LastSliceTime > _bounds.LastSliceTime ? _cold.LastSliceTime : _bounds.LastSliceTime;

        private PruningReservation Reservation => _options.ReservationOverride ?? PruningReservation.Shared;

        /// <summary>
        /// Lists the log, the catalog is read again when the head is not the one it holds.
        /// </summary>
        public async Task Validate(DeltaReadOptions readOptions)
        {
            var listing = await DeltaTransactionReader.ListLog(_options.StorageLocation, _tablePath);
            if (_ready && listing.Head == _head)
            {
                return;
            }
            await Bootstrap(listing, readOptions);
        }

        private async Task Bootstrap(DeltaTransactionReader.LogListing listing, DeltaReadOptions readOptions)
        {
            var wasReady = _ready;
            await ResetFiles();
            _ready = false;
            var table = await DeltaTransactionReader.ReadTable(_options.StorageLocation, _tablePath, listing, options: readOptions);
            if (table == null)
            {
                _header = null;
                _head = -1;
                _ready = true;
                return;
            }
            if (wasReady)
            {
                _logger.LogInformation("Delta table {table} has commits from another writer, the catalog is read again at version {version}", _tablePath.Full, table.Version);
            }
            _header = WithoutFiles(table);
            _head = table.Version;
            _adoptedAt = await ResolveAdoptedAt(table.Version);
            SetSchema(table.Schema, table.AddFiles.Count);

            var row = new byte[_layout.RowSize];
            foreach (var add in table.AddFiles)
            {
                var statistics = CatalogOverlay.ParseStatistics(add.Statistics, _statisticsOptions!);
                var id = _nextId++;
                await _cold.Upsert(id, DeltaFileRecord.FromAdd(add, statistics?.NumRecords));
                _layout.WriteRow(statistics, row);
                await _projection!.Set(id, row);
                _liveFiles++;
            }
            _ready = true;
        }

        // The epoch continues only from a head that a Flowtide sink with stage ids wrote
        private async Task<long> ResolveAdoptedAt(long head)
        {
            var commitInfo = await DeltaTransactionReader.ReadFirstCommitInfo(_options.StorageLocation, _tablePath, head);
            if (commitInfo?.StageId != null && commitInfo.AdoptedAt.HasValue)
            {
                return commitInfo.AdoptedAt.Value;
            }
            return head + 1;
        }

        private static DeltaTable WithoutFiles(DeltaTable table)
        {
            return new DeltaTable(table.Metadata, table.Protocol, new List<DeltaAddAction>(), table.Schema, table.Version, Array.Empty<DeltaRemoveFileAction>(), table.Transactions, table.DomainMetadata, table.StartCheckpointVersion);
        }

        private void SetSchema(StructType schema, int expectedFiles)
        {
            _layout = PruningLayout.Create(schema, _writtenColumns, _options.StatsPruningColumns);
            _statisticsOptions = StatisticsOptions(schema);
            _projection?.Dispose();
            _projection = PruningProjection.Create(_memoryAllocator, Reservation, _options.PruningMemoryBytes, _bounds, _layout, expectedFiles, _treeModeForRun, out var capacityMismatch);
            if (capacityMismatch && !_capacityWarned)
            {
                _capacityWarned = true;
                _logger.LogWarning("PruningMemoryBytes {requested} differs from the process wide pruning memory {capacity} fixed by the first Delta sink, the first value is used", _options.PruningMemoryBytes, Reservation.Capacity);
            }
            if (_projection.TreeMode && !_treeModeForRun)
            {
                _treeModeForRun = true;
                _logger.LogWarning("Delta table {table} keeps its pruning bounds in a spilled tree, the process wide pruning memory is used up", _tablePath.Full);
            }
        }

        private static JsonSerializerOptions StatisticsOptions(StructType schema)
        {
            var options = new JsonSerializerOptions();
            options.Converters.Add(new DeltaStatisticsConverter(schema));
            return options;
        }

        /// <summary>
        /// A commit on the current head, or the commit that creates the table.
        /// </summary>
        public CatalogOverlay BeginCommit(long version, long adoptedAt, DeltaTable? newHeader)
        {
            if (newHeader != null)
            {
                return new CatalogOverlay(version, adoptedAt, PruningLayout.Create(newHeader.Schema, _writtenColumns, _options.StatsPruningColumns), StatisticsOptions(newHeader.Schema), newHeader);
            }
            return new CatalogOverlay(version, adoptedAt, _layout, _statisticsOptions!, null);
        }

        /// <summary>
        /// An overwrite replaces every file, its new files take ids from the start.
        /// Nothing reads the old ids before the overwrite is applied, a failed commit restarts the sink.
        /// </summary>
        public void RestartIds()
        {
            _nextId = 0;
            _freeIds.Clear();
        }

        // Ids of removed files are free once their removal is applied
        public int AllocateId()
        {
            return _freeIds.Count > 0 ? _freeIds.Pop() : _nextId++;
        }

        public IAsyncEnumerable<(int Id, DeltaFileRecord Record)> ScanFiles()
        {
            return _cold.ScanAll();
        }

        public Task<Dictionary<int, DeltaFileRecord>> GetFiles(IReadOnlyCollection<int> ids)
        {
            return _cold.Get(ids);
        }

        /// <summary>
        /// The files each probe can be in, by file id. One pass over the projection for the whole batch.
        /// </summary>
        public async Task<Dictionary<int, List<int>>> FindCandidates(ProbeBatch probes)
        {
            var candidates = new Dictionary<int, List<int>>();
            if (probes.Count == 0 || _projection == null)
            {
                return candidates;
            }
            await _projection.Scan((id, row) =>
            {
                List<int>? matches = null;
                for (int p = 0; p < probes.Count; p++)
                {
                    if (probes.Matches(row.Span, p))
                    {
                        matches ??= new List<int>();
                        matches.Add(p);
                    }
                }
                if (matches != null)
                {
                    candidates[id] = matches;
                }
            });
            return candidates;
        }

        public void MarkPublished(CatalogOverlay overlay)
        {
            _published = overlay;
        }

        /// <summary>
        /// Applies the published commit, exactly once, before anything reads the catalog.
        /// </summary>
        public async Task ApplyPublished()
        {
            var overlay = _published;
            if (overlay == null)
            {
                return;
            }
            _published = null;
            if (overlay.NewHeader != null)
            {
                _header = overlay.NewHeader;
                SetSchema(overlay.NewHeader.Schema, overlay.Adds.Count);
            }
            if (overlay.RemoveAll)
            {
                await _cold.Clear();
                await _bounds.Clear();
                _freeIds.Clear();
                _liveFiles = 0;
                SetSchema(_header!.Schema, overlay.Adds.Count);
            }
            foreach (var id in overlay.Removes)
            {
                await _cold.Delete(id);
                await _projection!.Remove(id);
                _freeIds.Push(id);
                _liveFiles--;
            }
            foreach (var (id, record) in overlay.Updates)
            {
                await _cold.Upsert(id, record);
            }
            foreach (var (id, record, row) in overlay.Adds)
            {
                await _cold.Upsert(id, record);
                await _projection!.Set(id, row);
                _liveFiles++;
            }
            _head = overlay.Version;
            _adoptedAt = overlay.AdoptedAt;
        }

        /// <summary>
        /// A bounded slice of rotation work, and the extra columns another table asked for.
        /// </summary>
        public async Task Maintain()
        {
            _treeModeForRun |= _projection?.TreeMode == true;
            _cold.RotateIfDue(_liveFiles, _options.CatalogRotationFloor);
            await _cold.MigrateSlice(_options.CatalogMigrationSlice, _options.CatalogMigrationBytes);
            if (_projection != null && _projection.TreeMode)
            {
                _bounds.RotateIfDue(_liveFiles, _options.CatalogRotationFloor);
                await _bounds.MigrateSlice(_options.CatalogMigrationSlice, _options.CatalogMigrationBytes);
            }
        }

        public void ApplyRevocation()
        {
            _projection?.ApplyRevocation();
        }

        private async Task ResetFiles()
        {
            await _cold.Clear();
            await _bounds.Clear();
            _freeIds.Clear();
            _nextId = 0;
            _liveFiles = 0;
            _published = null;
            _projection?.Dispose();
            _projection = null;
        }

        public void Dispose()
        {
            _projection?.Dispose();
            _projection = null;
        }
    }
}
