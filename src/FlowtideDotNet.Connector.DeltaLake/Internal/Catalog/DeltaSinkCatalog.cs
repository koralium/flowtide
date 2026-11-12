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
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.Compaction;
using FlowtideDotNet.Storage.Comparers;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Serializers;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.Tree;
using Microsoft.Extensions.Logging;
using Stowage;
using System.Diagnostics;
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
        private readonly IBPlusTree<int, DeltaFileRecord, PrimitiveListKeyContainer<int>, ManagedValueContainer<DeltaFileRecord>> _cold;
        private readonly IBPlusTree<int, byte[], PrimitiveListKeyContainer<int>, ManagedValueContainer<byte[]>> _bounds;
        private readonly PrimitiveListComparer<int> _comparer = new PrimitiveListComparer<int>();
        private readonly Stack<int> _freeIds = new Stack<int>();
        private PruningProjection? _projection;
        private PruningLayout _layout = new PruningLayout(Array.Empty<PruningColumn>());
        private JsonSerializerOptions? _statisticsOptions;
        private DeltaTable? _header;
        private CatalogOverlay? _published;
        private bool _ready;
        private long _head = -1;
        private CommitProvenance? _headProvenance;
        private int _nextId;
        private int _liveFiles;
        private bool _capacityWarned;
        // Tree mode lasts for the run, a later bootstrap does not take the arrays back
        private bool _treeModeForRun;
        // Rising across every catalog in the process
        private static long s_catalogEpoch;
        private long _catalogEpoch;
        private bool _compactionEffective;
        // Allocated once compaction is effective
        private long[]? _classFiles;
        private long[]? _classBytes;
        // The sink's stored window, null with compaction off
        private readonly CompactionWindow? _window;
        private CompactionGuard? _guard;
        // One cursor per due rule, and the table's declines
        private CompactionCursor[]? _cursors;
        private CompactionDeclines? _declines;
        // Made once, so a removal can hand back the path
        private readonly GenericWriteFunction<DeltaFileRecord> _removeRecord;
        private static readonly GenericWriteFunction<DeltaFileRecord> s_deleteIfFound = (input, current, exists) => (null, exists ? GenericWriteOperation.Delete : GenericWriteOperation.None);
        private string? _removedPath;

        private DeltaSinkCatalog(DeltaLakeOptions options, IOPath tablePath, IReadOnlyList<string> writtenColumns, IMemoryAllocator memoryAllocator, ILogger logger, IBPlusTree<int, DeltaFileRecord, PrimitiveListKeyContainer<int>, ManagedValueContainer<DeltaFileRecord>> cold, IBPlusTree<int, byte[], PrimitiveListKeyContainer<int>, ManagedValueContainer<byte[]>> bounds, CompactionWindow? window)
        {
            _window = window;
            _removeRecord = RemoveRecord;
            _options = options;
            _tablePath = tablePath;
            _writtenColumns = writtenColumns;
            _memoryAllocator = memoryAllocator;
            _logger = logger;
            _cold = cold;
            _bounds = bounds;
            BumpEpoch();
        }

        // Both catalog trees: file ids as keys, values sized in bytes
        internal static BPlusTreeOptions<int, V, PrimitiveListKeyContainer<int>, ManagedValueContainer<V>> TreeOptions<V>(IBplusTreeValueSerializer<V, ManagedValueContainer<V>> serializer, IMemoryAllocator memoryAllocator)
        {
            return new BPlusTreeOptions<int, V, PrimitiveListKeyContainer<int>, ManagedValueContainer<V>>()
            {
                Comparer = new PrimitiveListComparer<int>(),
                KeySerializer = new PrimitiveListKeyContainerSerializer<int>(memoryAllocator),
                ValueSerializer = serializer,
                MemoryAllocator = memoryAllocator,
                UseByteBasedPageSizes = true
            };
        }

        public static async Task<DeltaSinkCatalog> Open(IStateManagerClient stateManagerClient, DeltaLakeOptions options, IOPath tablePath, IReadOnlyList<string> writtenColumns, IMemoryAllocator memoryAllocator, ILogger logger, CompactionWindow? window = null)
        {
            var cold = await stateManagerClient.GetOrCreateEphemeralTree("catalog_cold", TreeOptions(new DeltaFileRecordSerializer(), memoryAllocator));
            var bounds = await stateManagerClient.GetOrCreateEphemeralTree("catalog_bounds", TreeOptions(new BoundsRowSerializer(), memoryAllocator));
            // A reopen in the same process keeps the contents
            await cold.Clear();
            await bounds.Clear();
            return new DeltaSinkCatalog(options, tablePath, writtenColumns, memoryAllocator, logger, cold, bounds, window);
        }

        public bool IsReady => _ready;

        /// <summary>
        /// The table without its files, null while the table does not exist.
        /// </summary>
        public DeltaTable? Header => _header;

        public long Head => _head;

        /// <summary>
        /// The head commit's provenance, null while the table does not exist.
        /// </summary>
        public CommitProvenance? HeadProvenance => _headProvenance;

        public int LiveFiles => _liveFiles;

        // Changes whenever file ids or the file set restart
        public long CatalogEpoch => Volatile.Read(ref _catalogEpoch);

        public bool CompactionEffective => Volatile.Read(ref _compactionEffective);

        public long ClassFiles(int sizeClass) => !CompactionEffective || _classFiles == null ? 0 : Volatile.Read(ref _classFiles[sizeClass]);

        public long ClassBytes(int sizeClass) => !CompactionEffective || _classBytes == null ? 0 : Volatile.Read(ref _classBytes[sizeClass]);

        // Null unless compaction is effective
        public CompactionGuard? Guard => CompactionEffective ? Volatile.Read(ref _guard) : null;

        public CompactionCursor? Cursor(DueRule rule) => CompactionEffective ? Volatile.Read(ref _cursors)?[(int)rule] : null;

        public CompactionDeclines? Declines => CompactionEffective ? Volatile.Read(ref _declines) : null;

        /// <summary>
        /// The columns a delete batch captures and is matched on.
        /// </summary>
        public PruningLayout ScanLayout => _projection?.Layout ?? _layout;

        public bool TreeMode => _projection?.TreeMode ?? false;

        public long PruningBytes => _projection?.ChargedBytes ?? 0;

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
                _headProvenance = null;
                SetGuard(false);
                _ready = true;
                return;
            }
            _headProvenance = await ReadHeadProvenance(table, listing);
            if (wasReady)
            {
                _logger.LogInformation("Delta table {table} has commits from another writer, the catalog is read again at version {version}", _tablePath.Full, table.Version);
            }
            _header = WithoutFiles(table);
            _head = table.Version;
            var effective = _options.EnableAutoCompaction;
            if (effective)
            {
                EnsureClasses();
            }
            SetSchema(table.Schema, table.AddFiles.Count);

            var row = new byte[_layout.RowSize];
            // Outputs another writer removed leave the guard
            var guard = effective ? _guard : null;
            var livePaths = guard != null && guard.HasOutputs ? new HashSet<string>(StringComparer.Ordinal) : null;
            try
            {
                foreach (var add in table.AddFiles)
                {
                    var statistics = CatalogOverlay.ParseStatistics(add.Statistics, _statisticsOptions!);
                    var id = _nextId++;
                    var record = DeltaFileRecord.FromAdd(add, statistics?.NumRecords);
                    await _cold.Upsert(id, record);
                    _layout.WriteRow(statistics, row);
                    if (effective)
                    {
                        var sizeClass = SizeClass.Of(record);
                        row[0] |= SizeClass.Flags(sizeClass);
                        CountClass(sizeClass, record.Size, 1);
                        if (livePaths != null && guard!.IsOutput(record.Path))
                        {
                            livePaths.Add(record.Path);
                        }
                    }
                    await _projection!.Set(id, row);
                    _liveFiles++;
                }
            }
            catch
            {
                // A half read table leaves nothing behind
                await ResetFiles();
                throw;
            }
            SetGuard(effective);
            if (livePaths != null)
            {
                _guard!.KeepOutputs(livePaths);
            }
            Volatile.Write(ref _compactionEffective, effective);
            _ready = true;
        }

        // A replayed head is reused; a snapshot rooted at the head checkpoint reads the head's first line once
        private async Task<CommitProvenance> ReadHeadProvenance(DeltaTable table, DeltaTransactionReader.LogListing listing)
        {
            if (table.HeadHeader != null)
            {
                return CommitProvenance.From(table.Version, table.HeadHeader);
            }
            if (!listing.Commits.TryGetValue(table.Version, out var head))
            {
                return CommitProvenance.Unknown(table.Version);
            }
            var header = await CommitHeaderReader.ReadAsync(_options.StorageLocation, head.IOEntry.Path, HeaderMode.Lenient);
            return CommitProvenance.From(table.Version, header);
        }

        private static DeltaTable WithoutFiles(DeltaTable table)
        {
            return new DeltaTable(table.Metadata, table.Protocol, new List<DeltaAddAction>(), table.Schema, table.Version, Array.Empty<DeltaRemoveFileAction>(), table.Transactions, table.DomainMetadata, table.StartCheckpointVersion);
        }

        private static DeltaTable WithMetadata(DeltaTable header, DeltaMetadataAction metadata)
        {
            return new DeltaTable(metadata, header.Protocol, new List<DeltaAddAction>(), header.Schema, header.Version, Array.Empty<DeltaRemoveFileAction>(), header.Transactions, header.DomainMetadata, header.StartCheckpointVersion);
        }

        private void SetSchema(StructType schema, int expectedFiles)
        {
            _layout = PruningLayout.Create(schema, _writtenColumns, _options.StatsPruningColumns);
            _statisticsOptions = StatisticsOptions(schema);
            LatchTreeMode();
            _projection?.Dispose();
            _projection = PruningProjection.Create(_memoryAllocator, Reservation, _options.PruningMemoryBytes, _bounds, _layout, expectedFiles, _treeModeForRun, out var capacityMismatch);
            if (capacityMismatch && !_capacityWarned)
            {
                _capacityWarned = true;
                _logger.LogWarning("PruningMemoryBytes {requested} differs from the process wide pruning memory {capacity} fixed by the first Delta sink, the first value is used", _options.PruningMemoryBytes, Reservation.Capacity);
            }
            LatchTreeMode();
        }

        // Before any projection is replaced, growth can move it to the tree at any Set
        private void LatchTreeMode()
        {
            if (_projection != null && _projection.TreeMode && !_treeModeForRun)
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
        public CatalogOverlay BeginCommit(long version, DeltaTable? newHeader)
        {
            if (newHeader != null)
            {
                // The create commit decides like a bootstrap
                return new CatalogOverlay(version, PruningLayout.Create(newHeader.Schema, _writtenColumns, _options.StatsPruningColumns), StatisticsOptions(newHeader.Schema), newHeader, _options.EnableAutoCompaction);
            }
            return new CatalogOverlay(version, _layout, _statisticsOptions!, null, _compactionEffective);
        }

        /// <summary>
        /// An overwrite replaces every file, its new files take ids from the start.
        /// The old ids are never read again: a pending commit, also a halted one, blocks the next SaveData, its publication removes every file
        /// before a new id is taken, and a thrown failure opens a new catalog.
        /// </summary>
        public void RestartIds()
        {
            _nextId = 0;
            _freeIds.Clear();
            BumpEpoch();
        }

        // Ids of removed files are free once their removal is applied
        public int AllocateId()
        {
            return _freeIds.Count > 0 ? _freeIds.Pop() : _nextId++;
        }

        // Every record in id order, the catalog must not change during the scan
        public async IAsyncEnumerable<(int Id, DeltaFileRecord Record)> ScanFiles()
        {
            using var iterator = _cold.CreateIterator();
            await iterator.SeekFirst();
            await foreach (var page in iterator)
            {
                foreach (var kv in page)
                {
                    yield return (kv.Key, kv.Value);
                }
            }
        }

        // The records of the ids that exist
        public async Task<Dictionary<int, DeltaFileRecord>> GetFiles(IReadOnlyCollection<int> ids)
        {
            var found = new Dictionary<int, DeltaFileRecord>(ids.Count);
            if (ids.Count == 0)
            {
                return found;
            }
            var keys = ids.ToArray();
            using var searcher = _cold.CreateBulkSearcher(_comparer);
            await searcher.Start(keys, keys.Length);
            while (await searcher.MoveNextLeaf())
            {
                var leaf = searcher.CurrentLeaf;
                foreach (var result in searcher.CurrentResults)
                {
                    if (result.Found)
                    {
                        found[keys[result.KeyIndex]] = leaf.values.Get(result.LowerBound);
                    }
                }
            }
            return found;
        }

        // Every live file's bounds row, nothing while no schema is set
        internal ValueTask ScanBounds(BoundsVisitor visit)
        {
            var projection = _projection;
            return projection == null ? default : projection.Scan(visit);
        }

        // The catalog must not change during a discovery slice
        public Task<CursorStop> Discover(DueRule rule, int wanted, IReadOnlySet<int>? skip, long now)
        {
            Debug.Assert(_ready && _published == null);
            if (!CompactionEffective || _cursors == null || _projection == null)
            {
                return Task.FromResult(CursorStop.Off);
            }
            return _cursors[(int)rule].Step(_projection, this, wanted, skip, now);
        }

        // A bin's inputs leave every rule
        public void Consume(DueRule rule, IReadOnlySet<int> ids)
        {
            if (_cursors == null)
            {
                return;
            }
            for (int r = 0; r < _cursors.Length; r++)
            {
                if (r == (int)rule)
                {
                    _cursors[r].Consume(ids);
                    continue;
                }
                foreach (var id in ids)
                {
                    _cursors[r].Forget(id, removed: true);
                }
            }
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
                if (overlay.CompactionEffective)
                {
                    EnsureClasses();
                }
                SetGuard(overlay.CompactionEffective);
                _guard?.ClearOutputs();
                Volatile.Write(ref _compactionEffective, overlay.CompactionEffective);
                ClearClasses();
                BumpEpoch();
            }
            else if (overlay.UpdatedMetadata != null && _header != null)
            {
                // Same schema: the projection and the files stay
                _header = WithMetadata(_header, overlay.UpdatedMetadata);
            }
            Debug.Assert(overlay.CompactionEffective == _compactionEffective, "The commit and the catalog disagree on compaction");
            if (overlay.RemoveAll)
            {
                await _cold.Clear();
                await _bounds.Clear();
                _freeIds.Clear();
                _liveFiles = 0;
                SetSchema(_header!.Schema, overlay.Adds.Count);
                ClearClasses();
                _guard?.ClearOutputs();
                BumpEpoch();
            }
            Debug.Assert(overlay.RemovedClasses == null || overlay.RemovedClasses.Count == overlay.Removes.Count, "Removals and their classes are out of step");
            for (int i = 0; i < overlay.Removes.Count; i++)
            {
                var id = overlay.Removes[i];
                // Only compaction needs the path back
                var operation = await _cold.RMWNoResult(id, null, _compactionEffective ? _removeRecord : s_deleteIfFound);
                var (live, flags) = await _projection!.Remove(id);
                Debug.Assert(operation == GenericWriteOperation.Delete && live, $"Removed file id {id} is not live");
                _freeIds.Push(id);
                _liveFiles--;
                if (_compactionEffective)
                {
                    var (size, sizeClass) = overlay.RemovedClasses![i];
                    Debug.Assert(SizeClass.FromFlags(flags) == sizeClass, $"Removed file id {id} has another size class");
                    CountClass(sizeClass, size, -1);
                    if (_removedPath != null)
                    {
                        _guard?.RemoveOutput(_removedPath);
                        _declines?.Removed(id, _removedPath);
                        _removedPath = null;
                    }
                    ForgetFile(id, removed: true);
                }
            }
            foreach (var (id, record) in overlay.Updates)
            {
                await _cold.Upsert(id, record);
                if (_compactionEffective)
                {
                    ForgetFile(id, removed: false);
                }
            }
            foreach (var (id, record, row) in overlay.Adds)
            {
                await _cold.Upsert(id, record);
                await _projection!.Set(id, row);
                _liveFiles++;
                if (_compactionEffective)
                {
                    CountClass(SizeClass.FromFlags(row[0]), record.Size, 1);
                }
            }
            LatchTreeMode();
            _head = overlay.Version;
            _headProvenance = overlay.Provenance ?? CommitProvenance.Unknown(overlay.Version);
        }

        public void ApplyRevocation()
        {
            _projection?.ApplyRevocation();
        }

        private async Task ResetFiles()
        {
            LatchTreeMode();
            await _cold.Clear();
            await _bounds.Clear();
            _freeIds.Clear();
            _nextId = 0;
            _liveFiles = 0;
            _published = null;
            _projection?.Dispose();
            _projection = null;
            Volatile.Write(ref _compactionEffective, false);
            ClearClasses();
            BumpEpoch();
        }

        private (DeltaFileRecord? result, GenericWriteOperation operation) RemoveRecord(DeltaFileRecord? input, DeltaFileRecord? current, bool exists)
        {
            if (!exists)
            {
                return (null, GenericWriteOperation.None);
            }
            _removedPath = current!.Path;
            return (null, GenericWriteOperation.Delete);
        }

        private static long NextCatalogEpoch()
        {
            return Interlocked.Increment(ref s_catalogEpoch);
        }

        private void BumpEpoch()
        {
            Volatile.Write(ref _catalogEpoch, NextCatalogEpoch());
            _guard?.Restart(Now());
            _declines?.Clear();
            if (_cursors != null)
            {
                foreach (var cursor in _cursors)
                {
                    cursor.Reset();
                }
            }
        }

        private void ForgetFile(int id, bool removed)
        {
            if (_cursors == null)
            {
                return;
            }
            foreach (var cursor in _cursors)
            {
                cursor.Forget(id, removed);
            }
        }

        // The guard and its outputs live while compaction is effective
        private void SetGuard(bool effective)
        {
            if (!effective)
            {
                Volatile.Write(ref _guard, null);
                Volatile.Write(ref _cursors, null);
                Volatile.Write(ref _declines, null);
                return;
            }
            var limits = _options.CompactionLimitsOverride ?? CompactionLimits.Default;
            if (_guard == null)
            {
                Volatile.Write(ref _guard, new CompactionGuard(_window ?? new CompactionWindow(), _options.CompactionColdAfter, limits, Now(), _logger, _tablePath.Full));
            }
            if (_declines == null)
            {
                var declines = new CompactionDeclines(limits.MemoEntries, _logger, _tablePath.Full);
                Volatile.Write(ref _cursors, new[]
                {
                    new CompactionCursor(limits, _options.CompactionColdAfter, declines),
                    new CompactionCursor(limits, _options.CompactionColdAfter, declines),
                    new CompactionCursor(limits, _options.CompactionColdAfter, declines)
                });
                Volatile.Write(ref _declines, declines);
            }
        }

        private long Now()
        {
            return _options.TimeProvider.GetUtcNow().ToUnixTimeMilliseconds();
        }

        private void EnsureClasses()
        {
            _classFiles ??= new long[SizeClass.Counted];
            _classBytes ??= new long[SizeClass.Counted];
        }

        private void CountClass(byte sizeClass, long size, int sign)
        {
            if (sizeClass == SizeClass.Ineligible)
            {
                return;
            }
            Volatile.Write(ref _classFiles![sizeClass], _classFiles[sizeClass] + sign);
            Volatile.Write(ref _classBytes![sizeClass], _classBytes[sizeClass] + sign * size);
        }

        private void ClearClasses()
        {
            if (_classFiles == null)
            {
                return;
            }
            for (int c = 0; c < _classFiles.Length; c++)
            {
                Volatile.Write(ref _classFiles[c], 0);
                Volatile.Write(ref _classBytes![c], 0);
            }
        }

        public void Dispose()
        {
            _projection?.Dispose();
            _projection = null;
        }
    }
}
