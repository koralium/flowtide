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
using FlowtideDotNet.Storage.Comparers;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Serializers;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.Tree;
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
        private int _nextId;
        private int _liveFiles;
        private bool _capacityWarned;
        // Tree mode lasts for the run, a later bootstrap does not take the arrays back
        private bool _treeModeForRun;

        private DeltaSinkCatalog(DeltaLakeOptions options, IOPath tablePath, IReadOnlyList<string> writtenColumns, IMemoryAllocator memoryAllocator, ILogger logger, IBPlusTree<int, DeltaFileRecord, PrimitiveListKeyContainer<int>, ManagedValueContainer<DeltaFileRecord>> cold, IBPlusTree<int, byte[], PrimitiveListKeyContainer<int>, ManagedValueContainer<byte[]>> bounds)
        {
            _options = options;
            _tablePath = tablePath;
            _writtenColumns = writtenColumns;
            _memoryAllocator = memoryAllocator;
            _logger = logger;
            _cold = cold;
            _bounds = bounds;
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

        public static async Task<DeltaSinkCatalog> Open(IStateManagerClient stateManagerClient, DeltaLakeOptions options, IOPath tablePath, IReadOnlyList<string> writtenColumns, IMemoryAllocator memoryAllocator, ILogger logger)
        {
            var cold = await stateManagerClient.GetOrCreateEphemeralTree("catalog_cold", TreeOptions(new DeltaFileRecordSerializer(), memoryAllocator));
            var bounds = await stateManagerClient.GetOrCreateEphemeralTree("catalog_bounds", TreeOptions(new BoundsRowSerializer(), memoryAllocator));
            // A reopen in the same process keeps the contents
            await cold.Clear();
            await bounds.Clear();
            return new DeltaSinkCatalog(options, tablePath, writtenColumns, memoryAllocator, logger, cold, bounds);
        }

        public bool IsReady => _ready;

        /// <summary>
        /// The table without its files, null while the table does not exist.
        /// </summary>
        public DeltaTable? Header => _header;

        public long Head => _head;

        public int LiveFiles => _liveFiles;

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
                _ready = true;
                return;
            }
            if (wasReady)
            {
                _logger.LogInformation("Delta table {table} has commits from another writer, the catalog is read again at version {version}", _tablePath.Full, table.Version);
            }
            _header = WithoutFiles(table);
            _head = table.Version;
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

        private static DeltaTable WithoutFiles(DeltaTable table)
        {
            return new DeltaTable(table.Metadata, table.Protocol, new List<DeltaAddAction>(), table.Schema, table.Version, Array.Empty<DeltaRemoveFileAction>(), table.Transactions, table.DomainMetadata, table.StartCheckpointVersion);
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
                return new CatalogOverlay(version, PruningLayout.Create(newHeader.Schema, _writtenColumns, _options.StatsPruningColumns), StatisticsOptions(newHeader.Schema), newHeader);
            }
            return new CatalogOverlay(version, _layout, _statisticsOptions!, null);
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

        /// <summary>
        /// The files each probe can be in, by file id. One pass over the projection for the whole batch.
        /// </summary>
        public async Task<Dictionary<int, List<int>>> FindCandidates(ProbeBatch probes)
        {
            var candidates = new Dictionary<int, List<int>>();
            if (probes.Count == 0)
            {
                return candidates;
            }
            await ScanBounds((id, row) =>
            {
                List<int>? matches = null;
                for (int p = 0; p < probes.Count; p++)
                {
                    if (probes.Matches(row, p))
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

        // Every live file's bounds row, nothing while no schema is set
        internal ValueTask ScanBounds(BoundsVisitor visit)
        {
            var projection = _projection;
            return projection == null ? default : projection.Scan(visit);
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
            LatchTreeMode();
            _head = overlay.Version;
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
        }

        public void Dispose()
        {
            _projection?.Dispose();
            _projection = null;
        }
    }
}
