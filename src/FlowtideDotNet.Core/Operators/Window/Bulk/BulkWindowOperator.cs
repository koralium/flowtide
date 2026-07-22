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
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.Comparers;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Core.Compute.Columnar;
using FlowtideDotNet.Core.Compute.Columnar.Functions.WindowFunctions.Bulk;
using FlowtideDotNet.Core.Operators.Aggregate.Bulk;
using FlowtideDotNet.Storage.Serializers;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.Tree;
using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Substrait.Relations;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Core.Operators.Window.Bulk
{
    /// <summary>
    /// Window operator that applies incoming rows in bulk against its persistent tree and recomputes only
    /// the rows that a change can affect. Incoming batches are sorted and written with a single pass per
    /// touched leaf. Changed rows are recorded in a temporary tree, and on watermark each dirty partition is
    /// scanned starting at the first changed row, seeding window function state from the stored values of
    /// the preceding rows, and the scan stops as soon as every function reports its remaining stored values
    /// are still valid.
    /// </summary>
    internal class BulkWindowOperator : UnaryVertex<StreamEventBatch>
    {
        private const int OutputBatchSize = 100;

        /// <summary>
        /// Incoming batches are applied to the trees in chunks of this many rows, with an output flush
        /// between chunks, bounding how many retraction rows can accumulate in memory for one batch.
        /// </summary>
        private const int ReceiveChunkSize = 2048;

        private readonly ConsistentPartitionWindowRelation _relation;
        private readonly IBulkWindowFunction[] _functions;
        private readonly List<int> _emitList;

        private List<int> _partitionColumns;
        private List<int> _otherColumns;
        private List<Action<EventBatchData, int, ColumnStore.Column>> _partitionCalculateExpressions;
        private readonly Func<EventBatchData, int, EventBatchData, int, int>? _orderCompareFunction;

        private readonly int _inputColumnCount;
        private readonly int _totalKeyColumns;
        private readonly int _totalStateColumns;
        private readonly int[] _auxiliaryStartIndices;
        private readonly long _maxAffectedRowsBefore;
        private readonly int[] _emitRequiredFunctions;

        private IBPlusTree<ColumnRowReference, BulkWindowValue, ColumnKeyStorageContainer, BulkWindowValueContainer>? _persistentTree;
        private IBPlusTreeBulkInserter<ColumnRowReference, BulkWindowValue, ColumnKeyStorageContainer, BulkWindowValueContainer>? _persistentBulkInserter;
        private IBPlusTree<ColumnRowReference, int, ColumnKeyStorageContainer, PrimitiveListValueContainer<int>>? _temporaryTree;
        private IBPlusTreeBulkInserter<ColumnRowReference, int, ColumnKeyStorageContainer, PrimitiveListValueContainer<int>>? _temporaryBulkInserter;
        private IBPlusTreeIterator<ColumnRowReference, BulkWindowValue, ColumnKeyStorageContainer, BulkWindowValueContainer>? _scanIterator;
        private IObjectState<bool>? _hasSentInitialData;
        private IObjectState<string>? _stateLayout;

        private BulkWindowEmitter? _emitter;
        private BulkWindowSeedReader? _seedReader;
        private BulkWindowBackwardPartitionReader? _backwardReader;
        private BulkWindowInsertComparer? _markerComparer;
        private BulkWindowPartitionComparer? _partitionRangeComparer;
        private BulkWindowPartitionComparer? _partitionSeekComparer;
        private BulkWindowRowContext? _rowContext;

        private DataValueContainer[] _resultContainers = Array.Empty<DataValueContainer>();
        private IDataValue[] _resultValues = Array.Empty<IDataValue>();
        private IDataValue[] _oldValueScratch = Array.Empty<IDataValue>();
        private IDataValue[] _mutatorScratch = Array.Empty<IDataValue>();
        private bool[] _rowValueStable = Array.Empty<bool>();

        // Scratch batch used to group changed rows per partition during the watermark scan.
        private ColumnStore.Column[]? _markerColumns;
        private EventBatchData? _markerBatch;
        private int _markerCount;

        // Scratch row holding the partition values of the partition currently being scanned during the
        // initial full computation.
        private ColumnStore.Column[]? _partitionScratchColumns;
        private EventBatchData? _partitionScratchBatch;

        // Scratch row holding the key of the row a scan starts at when functions need rows before the
        // first change recomputed, found by walking backwards from the first marker.
        private ColumnStore.Column[]? _scanStartColumns;
        private EventBatchData? _scanStartBatch;

        private ColumnRowReference[] _keyScratch = Array.Empty<ColumnRowReference>();
        private BulkWindowValue[] _valueScratch = Array.Empty<BulkWindowValue>();
        private int[] _tempValueScratch = Array.Empty<int>();
        private int[] _duplicateTagScratch = Array.Empty<int>();
        private int[] _chunkSortedIndices = Array.Empty<int>();
        private int[] _chunkTagScratch = Array.Empty<int>();

        // Radix based batch sorting of incoming rows, available when every order by key is a plain
        // column. The layout mirrors BulkWindowInsertComparer's ordering: partition columns, the order by
        // columns with their directions and the remaining columns ascending, so the sorted batch order is
        // exactly the tree order.
        private readonly ColumnStore.Sort.BatchSorter? _batchSorter;
        private readonly int[]? _sortLayoutColumns;
        private readonly IColumn[]? _sortLayoutScratch;
        private int[] _sortIndicesScratch = Array.Empty<int>();

        private ICounter<long>? _eventsOutCounter;
        private ICounter<long>? _eventsInCounter;
        private bool _initialComputeSawData;

        /// <summary>
        /// Creates a bulk window operator when every window function in the relation has a bulk
        /// implementation that supports its configuration. Returns false when the relation must fall back
        /// to the non bulk <see cref="WindowOperator"/>.
        /// </summary>
        public static bool TryCreate(
            ConsistentPartitionWindowRelation relation,
            FunctionsRegister functionsRegister,
            ExecutionDataflowBlockOptions executionDataflowBlockOptions,
            [NotNullWhen(true)] out BulkWindowOperator? bulkWindowOperator)
        {
            var functions = new IBulkWindowFunction[relation.WindowFunctions.Count];
            for (int i = 0; i < relation.WindowFunctions.Count; i++)
            {
                if (!functionsRegister.TryCreateBulkWindowFunction(relation.WindowFunctions[i], out var function))
                {
                    bulkWindowOperator = null;
                    return false;
                }
                functions[i] = function;
            }
            bulkWindowOperator = new BulkWindowOperator(relation, functionsRegister, functions, executionDataflowBlockOptions);
            return true;
        }

        public BulkWindowOperator(
            ConsistentPartitionWindowRelation relation,
            IFunctionsRegister functionsRegister,
            IBulkWindowFunction[] functions,
            ExecutionDataflowBlockOptions executionDataflowBlockOptions) : base(executionDataflowBlockOptions)
        {
            _relation = relation;
            _functions = functions;
            _inputColumnCount = relation.Input.OutputLength;

            if (relation.OrderBy.Count > 0)
            {
                _orderCompareFunction = SortFieldCompareCompiler.CreateComparerFunction(relation.OrderBy, functionsRegister);
            }
            CreatePartitionColumnExpressions(functionsRegister);
            _totalKeyColumns = _inputColumnCount + _partitionCalculateExpressions.Count;

            if (relation.EmitSet)
            {
                _emitList = relation.Emit.ToList();
            }
            else
            {
                _emitList = new List<int>();
                for (int i = 0; i < relation.Input.OutputLength; i++)
                {
                    _emitList.Add(i);
                }
                for (int i = 0; i < relation.WindowFunctions.Count; i++)
                {
                    _emitList.Add(_emitList.Count);
                }
            }

            _auxiliaryStartIndices = new int[functions.Length];
            var auxIndex = functions.Length;
            long maxAffectedBefore = 0;
            for (int i = 0; i < functions.Length; i++)
            {
                _auxiliaryStartIndices[i] = auxIndex;
                auxIndex += functions[i].AuxiliaryStateColumnCount;
                maxAffectedBefore = Math.Max(maxAffectedBefore, functions[i].AffectedRowsBefore);
            }
            _totalStateColumns = auxIndex;
            _maxAffectedRowsBefore = maxAffectedBefore;

            // Functions whose null output means the row is dropped by a filter above, so the row is not
            // emitted at all. A logical row is emitted only when every such function's value is non null.
            List<int> emitRequired = new List<int>();
            for (int i = 0; i < relation.WindowFunctions.Count; i++)
            {
                var options = relation.WindowFunctions[i].Options;
                if (options != null &&
                    options.ContainsKey(BulkWindowFunctionOptions.EmitOnlyWithinMaxRowNumber) &&
                    options.ContainsKey(BulkWindowFunctionOptions.MaxRowNumber))
                {
                    emitRequired.Add(i);
                }
            }
            _emitRequiredFunctions = emitRequired.ToArray();

            var sortLayout = TryBuildSortLayout();
            if (sortLayout != null)
            {
                _sortLayoutColumns = sortLayout.Value.columns;
                _batchSorter = new ColumnStore.Sort.BatchSorter(_sortLayoutColumns.Length, sortLayout.Value.directions);
                _sortLayoutScratch = new IColumn[_sortLayoutColumns.Length];
            }
        }

        /// <summary>
        /// Builds the batch sorter's column layout when every order by key is a direct field reference.
        /// Order by expressions that are computed cannot be radix sorted and keep the comparison sort.
        /// </summary>
        private (int[] columns, ColumnStore.Sort.SortColumnDirection[] directions)? TryBuildSortLayout()
        {
            var orderColumns = new List<(int column, ColumnStore.Sort.SortColumnDirection direction)>();
            foreach (var sortField in _relation.OrderBy)
            {
                if (sortField.Expression is not DirectFieldReference directFieldReference ||
                    directFieldReference.ReferenceSegment is not StructReferenceSegment structReferenceSegment ||
                    structReferenceSegment.Child != null)
                {
                    return null;
                }
                ColumnStore.Sort.SortColumnDirection direction;
                switch (sortField.SortDirection)
                {
                    case SortDirection.SortDirectionAscNullsFirst:
                    case SortDirection.SortDirectionUnspecified:
                        direction = ColumnStore.Sort.SortColumnDirection.AscendingNullsFirst;
                        break;
                    case SortDirection.SortDirectionAscNullsLast:
                        direction = ColumnStore.Sort.SortColumnDirection.AscendingNullsLast;
                        break;
                    case SortDirection.SortDirectionDescNullsFirst:
                        direction = ColumnStore.Sort.SortColumnDirection.DescendingNullsFirst;
                        break;
                    case SortDirection.SortDirectionDescNullsLast:
                        direction = ColumnStore.Sort.SortColumnDirection.DescendingNullsLast;
                        break;
                    default:
                        return null;
                }
                orderColumns.Add((structReferenceSegment.Field, direction));
            }

            var columns = new int[_partitionColumns.Count + orderColumns.Count + _otherColumns.Count];
            var directions = new ColumnStore.Sort.SortColumnDirection[columns.Length];
            int index = 0;
            for (int i = 0; i < _partitionColumns.Count; i++)
            {
                columns[index] = _partitionColumns[i];
                directions[index] = ColumnStore.Sort.SortColumnDirection.AscendingNullsFirst;
                index++;
            }
            for (int i = 0; i < orderColumns.Count; i++)
            {
                columns[index] = orderColumns[i].column;
                directions[index] = orderColumns[i].direction;
                index++;
            }
            for (int i = 0; i < _otherColumns.Count; i++)
            {
                columns[index] = _otherColumns[i];
                directions[index] = ColumnStore.Sort.SortColumnDirection.AscendingNullsFirst;
                index++;
            }
            return (columns, directions);
        }

        /// <summary>
        /// True when every emission gating function has a non null value, meaning the row would survive
        /// the row_number filter that sits (or sat, before it was removed) above this relation.
        /// </summary>
        private bool ShouldEmitRow(IDataValue[] functionValues)
        {
            for (int i = 0; i < _emitRequiredFunctions.Length; i++)
            {
                if (functionValues[_emitRequiredFunctions[i]].IsNull)
                {
                    return false;
                }
            }
            return true;
        }

        [MemberNotNull(nameof(_partitionColumns), nameof(_partitionCalculateExpressions), nameof(_otherColumns))]
        private void CreatePartitionColumnExpressions(IFunctionsRegister functionsRegister)
        {
            List<int> partitionColumns = new List<int>();
            int extraColumnCounter = _relation.Input.OutputLength;
            List<Action<EventBatchData, int, ColumnStore.Column>> partitionCalculateExpressions = new List<Action<EventBatchData, int, ColumnStore.Column>>();
            foreach (var partitionExpr in _relation.PartitionBy)
            {
                if (partitionExpr is DirectFieldReference directFieldReference &&
                    directFieldReference.ReferenceSegment is StructReferenceSegment structReferenceSegment)
                {
                    partitionColumns.Add(structReferenceSegment.Field);
                }
                else
                {
                    var exprFunc = ColumnProjectCompiler.Compile(partitionExpr, functionsRegister);
                    partitionCalculateExpressions.Add(exprFunc);
                    partitionColumns.Add(extraColumnCounter++);
                }
            }
            _partitionColumns = partitionColumns;
            _partitionCalculateExpressions = partitionCalculateExpressions;

            _otherColumns = new List<int>();
            for (int i = 0; i < _relation.Input.OutputLength; i++)
            {
                if (!_partitionColumns.Contains(i))
                {
                    _otherColumns.Add(i);
                }
            }
        }

        public override string DisplayName => "BulkWindowOperator";

        public override Task Compact()
        {
            return Task.CompletedTask;
        }

        public override Task DeleteAsync()
        {
            return Task.CompletedTask;
        }

        public override async Task OnCheckpoint()
        {
            Debug.Assert(_persistentTree != null);
            Debug.Assert(_temporaryTree != null);
            Debug.Assert(_hasSentInitialData != null);

            await _persistentTree.Commit();
            await _temporaryTree.Commit();
            for (int i = 0; i < _functions.Length; i++)
            {
                await _functions[i].Commit();
            }
            await _hasSentInitialData.Commit();
            await _stateLayout!.Commit();
        }

        protected override IAsyncEnumerable<StreamEventBatch> OnLockingEventPrepare()
        {
            return SendData();
        }

        protected override IAsyncEnumerable<StreamEventBatch> OnWatermark(Watermark watermark)
        {
            return SendData();
        }

        public override async IAsyncEnumerable<StreamEventBatch> OnRecieve(StreamEventBatch msg, long time)
        {
            Debug.Assert(_persistentBulkInserter != null);
            Debug.Assert(_temporaryBulkInserter != null);
            Debug.Assert(_emitter != null);
            Debug.Assert(_eventsInCounter != null);
            Debug.Assert(_eventsOutCounter != null);
            Debug.Assert(_hasSentInitialData != null);

            var dataCount = msg.Data.Weights.Count;
            if (dataCount == 0)
            {
                yield break;
            }
            _eventsInCounter.Add(dataCount);

            // Compute partition expressions that are not direct column references into extra columns.
            ColumnStore.Column[] extraPartitionColumns = new ColumnStore.Column[_partitionCalculateExpressions.Count];
            for (int i = 0; i < extraPartitionColumns.Length; i++)
            {
                extraPartitionColumns[i] = new ColumnStore.Column(MemoryAllocator);
            }

            IColumn[] columns = new IColumn[_totalKeyColumns];
            for (int i = 0; i < msg.Data.EventBatchData.Columns.Count; i++)
            {
                columns[i] = msg.Data.EventBatchData.Columns[i];
            }
            for (int i = 0; i < extraPartitionColumns.Length; i++)
            {
                columns[msg.Data.EventBatchData.Columns.Count + i] = extraPartitionColumns[i];
            }
            var extendedBatch = new EventBatchData(columns);

            try
            {
                for (int k = 0; k < _partitionCalculateExpressions.Count; k++)
                {
                    var expression = _partitionCalculateExpressions[k];
                    var targetColumn = extraPartitionColumns[k];
                    for (int i = 0; i < dataCount; i++)
                    {
                        expression(msg.Data.EventBatchData, i, targetColumn);
                    }
                }

                var scratchSize = dataCount;
                if (_keyScratch.Length < scratchSize)
                {
                    _keyScratch = new ColumnRowReference[scratchSize];
                    _valueScratch = new BulkWindowValue[scratchSize];
                    _tempValueScratch = new int[scratchSize];
                    _duplicateTagScratch = new int[scratchSize];
                    _sortIndicesScratch = new int[scratchSize];
                }

                Debug.Assert(_markerComparer != null);
                var totalByteSize = extendedBatch.GetByteSize();
                var mutator = new BulkWindowMutator(_emitter, _functions.Length, _totalStateColumns, _mutatorScratch, _emitRequiredFunctions);

                // The bulk inserter and containers require that a key's row index equals its array index.
                for (int i = 0; i < dataCount; i++)
                {
                    _keyScratch[i] = new ColumnRowReference()
                    {
                        referenceBatch = extendedBatch,
                        RowIndex = i
                    };
                    _valueScratch[i] = new BulkWindowValue()
                    {
                        weight = msg.Data.Weights[i]
                    };
                    _tempValueScratch[i] = 1;
                }

                int[] sortedIndices;
                if (_batchSorter != null)
                {
                    sortedIndices = SortBatchWithRadix(extendedBatch, dataCount);
                }
                else
                {
                    sortedIndices = _persistentBulkInserter.SortAndGetIndices(_keyScratch, dataCount);

                    int currentTag = 0;
                    _duplicateTagScratch[0] = 0;
                    for (int i = 1; i < dataCount; i++)
                    {
                        var previous = _keyScratch[sortedIndices[i - 1]];
                        var current = _keyScratch[sortedIndices[i]];
                        if (_markerComparer.CompareTo(in previous, in current) != 0)
                        {
                            currentTag++;
                        }
                        _duplicateTagScratch[i] = currentTag;
                    }
                }

                // The batch is applied in sorted order chunks with an output flush between them, so the
                // retractions a large delete batch produces do not all accumulate in memory before the
                // first emission. A chunk ends on a duplicate boundary so equal rows share one apply.
                for (int chunkStart = 0; chunkStart < dataCount;)
                {
                    var chunkEnd = Math.Min(chunkStart + ReceiveChunkSize, dataCount);
                    while (chunkEnd < dataCount && _duplicateTagScratch[chunkEnd] == _duplicateTagScratch[chunkEnd - 1])
                    {
                        chunkEnd++;
                    }
                    var chunkLength = chunkEnd - chunkStart;

                    var chunkSortedIndices = sortedIndices;
                    var chunkTags = _duplicateTagScratch;
                    if (chunkStart > 0 || chunkEnd < dataCount)
                    {
                        if (_chunkSortedIndices.Length < chunkLength)
                        {
                            _chunkSortedIndices = new int[chunkLength];
                            _chunkTagScratch = new int[chunkLength];
                        }
                        Array.Copy(sortedIndices, chunkStart, _chunkSortedIndices, 0, chunkLength);
                        Array.Copy(_duplicateTagScratch, chunkStart, _chunkTagScratch, 0, chunkLength);
                        chunkSortedIndices = _chunkSortedIndices;
                        chunkTags = _chunkTagScratch;
                    }

                    var chunkByteSize = (int)((long)totalByteSize * chunkLength / dataCount) + (chunkLength * 8);
                    await _persistentBulkInserter.ApplyBatch(_keyScratch, _valueScratch, chunkLength, chunkSortedIndices, chunkTags, mutator, chunkByteSize);

                    if (_hasSentInitialData.Value)
                    {
                        await _temporaryBulkInserter.ApplyBatch(_keyScratch, _tempValueScratch, chunkLength, chunkSortedIndices, chunkTags, new BulkTemporaryMutator(), chunkByteSize);
                    }

                    // The retractions emitted by the mutator reference the extended batch, copy them out
                    // before the extra partition columns can be disposed.
                    _emitter.FlushPending();

                    if (_emitter.Count >= OutputBatchSize)
                    {
                        var chunkBatch = _emitter.GetCurrentBatch();
                        _eventsOutCounter.Add(chunkBatch.Weights.Count);
                        yield return new StreamEventBatch(chunkBatch);
                    }

                    chunkStart = chunkEnd;
                }
            }
            finally
            {
                for (int i = 0; i < extraPartitionColumns.Length; i++)
                {
                    extraPartitionColumns[i].Dispose();
                }
            }

            if (_emitter.Count > 0)
            {
                var batch = _emitter.GetCurrentBatch();
                _eventsOutCounter.Add(batch.Weights.Count);
                yield return new StreamEventBatch(batch);
            }
        }

        /// <summary>
        /// Sorts the incoming batch with the radix batch sorter, producing the sorted order and the
        /// duplicate tags in one cache friendly pass instead of the comparison sort and the tag loop.
        /// </summary>
        private int[] SortBatchWithRadix(EventBatchData extendedBatch, int dataCount)
        {
            Debug.Assert(_batchSorter != null);
            Debug.Assert(_sortLayoutColumns != null);
            Debug.Assert(_sortLayoutScratch != null);

            for (int i = 0; i < dataCount; i++)
            {
                _sortIndicesScratch[i] = i;
            }
            for (int k = 0; k < _sortLayoutColumns.Length; k++)
            {
                _sortLayoutScratch[k] = extendedBatch.Columns[_sortLayoutColumns[k]];
            }
            var indexSpan = _sortIndicesScratch.AsSpan(0, dataCount);
            var tagSpan = _duplicateTagScratch.AsSpan(0, dataCount);
            _batchSorter.SortDataWithTags(_sortLayoutScratch, ref indexSpan, ref tagSpan);
            return _sortIndicesScratch;
        }

        private async IAsyncEnumerable<StreamEventBatch> SendData()
        {
            Debug.Assert(_hasSentInitialData != null);
            Debug.Assert(_emitter != null);
            Debug.Assert(_eventsOutCounter != null);
            Debug.Assert(_temporaryTree != null);

            if (!_hasSentInitialData.Value)
            {
                await foreach (var batch in InitialFullCompute())
                {
                    _eventsOutCounter.Add(batch.Data.Weights.Count);
                    yield return batch;
                }
                // Only switch to incremental change tracking once actual data has been scanned, so an
                // early empty checkpoint does not disable the faster initial full computation.
                if (_initialComputeSawData)
                {
                    _hasSentInitialData.Value = true;
                }
                yield break;
            }

            Debug.Assert(_markerColumns != null);
            Debug.Assert(_markerBatch != null);
            Debug.Assert(_partitionSeekComparer != null);

            using var temporaryTreeIterator = _temporaryTree.CreateIterator();
            await temporaryTreeIterator.SeekFirst();

            ClearMarkers();

            await foreach (var partitionPage in temporaryTreeIterator)
            {
                foreach (var markerKv in partitionPage)
                {
                    var markerRow = markerKv.Key;
                    if (_markerCount > 0)
                    {
                        var lastMarker = new ColumnRowReference()
                        {
                            referenceBatch = _markerBatch,
                            RowIndex = _markerCount - 1
                        };
                        if (_partitionSeekComparer.CompareTo(in lastMarker, in markerRow) != 0)
                        {
                            await foreach (var batch in ScanPartition())
                            {
                                _eventsOutCounter.Add(batch.Data.Weights.Count);
                                yield return batch;
                            }
                            ClearMarkers();
                        }
                    }
                    for (int c = 0; c < _totalKeyColumns; c++)
                    {
                        _markerColumns[c].Add(markerRow.referenceBatch.Columns[c].GetValueAt(markerRow.RowIndex, default));
                    }
                    _markerCount++;
                }
            }

            if (_markerCount > 0)
            {
                await foreach (var batch in ScanPartition())
                {
                    _eventsOutCounter.Add(batch.Data.Weights.Count);
                    yield return batch;
                }
                ClearMarkers();
            }

            if (_emitter.Count > 0)
            {
                var batch = _emitter.GetCurrentBatch();
                _eventsOutCounter.Add(batch.Weights.Count);
                yield return new StreamEventBatch(batch);
            }

            await _temporaryTree.Clear();
        }

        private void ClearMarkers()
        {
            Debug.Assert(_markerColumns != null);
            for (int c = 0; c < _totalKeyColumns; c++)
            {
                _markerColumns[c].Clear();
            }
            _markerCount = 0;
        }

        /// <summary>
        /// Scans the partition of the currently collected marker rows, recomputing values from the first
        /// changed row until every function is stable.
        /// </summary>
        private async IAsyncEnumerable<StreamEventBatch> ScanPartition()
        {
            Debug.Assert(_markerBatch != null);
            Debug.Assert(_scanIterator != null);
            Debug.Assert(_seedReader != null);
            Debug.Assert(_emitter != null);
            Debug.Assert(_markerComparer != null);
            Debug.Assert(_partitionRangeComparer != null);
            Debug.Assert(_partitionSeekComparer != null);
            Debug.Assert(_rowContext != null);

            var firstMarker = new ColumnRowReference()
            {
                referenceBatch = _markerBatch,
                RowIndex = 0
            };

            // The scan must start early enough that every row a change can affect is recomputed. Functions
            // whose frames reach ahead of the current row report how many rows before a change they need,
            // and the scan start is walked backwards that many logical rows from the first changed row.
            var scanAnchor = firstMarker;
            bool fromPartitionStart = _maxAffectedRowsBefore == long.MaxValue;
            if (!fromPartitionStart && _maxAffectedRowsBefore > 0)
            {
                Debug.Assert(_backwardReader != null);
                Debug.Assert(_scanStartColumns != null);
                Debug.Assert(_scanStartBatch != null);

                await _backwardReader.Reset(firstMarker);
                long walked = 0;
                bool anchorFound = false;
                while (walked < _maxAffectedRowsBefore && await _backwardReader.MoveNextRow())
                {
                    walked += _backwardReader.Weight;
                    anchorFound = true;
                }
                if (anchorFound && walked < _maxAffectedRowsBefore)
                {
                    // The partition start, or the tree start, was reached before the full walk. The
                    // reader no longer rests on a partition row, so the scan starts at the partition's
                    // first row instead of at an anchor.
                    fromPartitionStart = true;
                }
                else if (anchorFound)
                {
                    for (int c = 0; c < _totalKeyColumns; c++)
                    {
                        _scanStartColumns[c].UpdateAt(0, _backwardReader.Batch.Columns[c].GetValueAt(_backwardReader.RowIndex, default));
                    }
                    scanAnchor = new ColumnRowReference()
                    {
                        referenceBatch = _scanStartBatch,
                        RowIndex = 0
                    };
                }
                // With no rows before the first marker the scan starts at the marker itself.
            }

            if (fromPartitionStart)
            {
                await _scanIterator.Seek(firstMarker, _partitionSeekComparer);
                _seedReader.ResetEmpty();
            }
            else
            {
                await _scanIterator.Seek(scanAnchor);
                await _seedReader.Reset(scanAnchor);
            }

            for (int f = 0; f < _functions.Length; f++)
            {
                await _functions[f].StartScan(firstMarker, _seedReader, fromPartitionStart);
            }

            int markerIndex = 0;
            // Distance in logical rows from the last consumed change to the next row to scan.
            long nextRowDistance = long.MaxValue / 2;

            var enumerator = _scanIterator.GetAsyncEnumerator();
            bool firstPage = true;
            bool stop = false;

            while (!stop && await enumerator.MoveNextAsync())
            {
                var page = enumerator.Current;
                if (page.CurrentPage == null || page.Keys == null || page.Keys.Count == 0)
                {
                    continue;
                }

                _partitionRangeComparer.FindIndex(in firstMarker, page.Keys);
                if (_partitionRangeComparer.noMatch)
                {
                    if (firstPage)
                    {
                        firstPage = false;
                        continue;
                    }
                    break;
                }

                int startIndex = _partitionRangeComparer.start;
                int endIndex = _partitionRangeComparer.end;

                if (firstPage && !fromPartitionStart)
                {
                    var bounds = _markerComparer.FindBoundries(in scanAnchor, page.Keys, startIndex, endIndex);
                    var lower = bounds.lowerBounds;
                    if (lower < 0)
                    {
                        lower = ~lower;
                    }
                    startIndex = lower;
                }
                firstPage = false;

                if (startIndex > endIndex)
                {
                    if (endIndex >= page.Keys.Count - 1)
                    {
                        // The partition may continue on the next page.
                        continue;
                    }
                    break;
                }

                bool pageDirty = false;
                var keyBatch = page.Keys.Data;
                var values = page.Values;

                for (int i = startIndex; i <= endIndex; i++)
                {
                    bool consumedMarker = false;
                    while (markerIndex < _markerCount)
                    {
                        var marker = new ColumnRowReference()
                        {
                            referenceBatch = _markerBatch,
                            RowIndex = markerIndex
                        };
                        var currentRow = new ColumnRowReference()
                        {
                            referenceBatch = keyBatch,
                            RowIndex = i
                        };
                        if (_markerComparer.CompareTo(in marker, in currentRow) <= 0)
                        {
                            markerIndex++;
                            consumedMarker = true;
                        }
                        else
                        {
                            break;
                        }
                    }
                    if (consumedMarker)
                    {
                        nextRowDistance = 0;
                    }

                    var weight = values._weights.Get(i);
                    if (weight <= 0)
                    {
                        continue;
                    }

                    var rowDistance = nextRowDistance;
                    var rowChanged = await ProcessLogicalRows(page, keyBatch, values, i, weight, rowDistance);
                    pageDirty |= rowChanged;

                    // A changed row can hide a weight change, so the next logical row is treated as
                    // distance one from the change no matter the row's weight.
                    nextRowDistance = consumedMarker ? 1 : nextRowDistance + weight;

                    if (markerIndex >= _markerCount)
                    {
                        bool allStable = true;
                        for (int f = 0; f < _functions.Length; f++)
                        {
                            var function = _functions[f];
                            // Distance stability: no future row can have a change inside its frame.
                            // Value stability: the last row kept its stored output and auxiliary state and
                            // sits far enough after the change that its value covers it. It must also not
                            // be the changed row itself, since a weight change on the row would not be
                            // visible in its values.
                            bool stable = nextRowDistance > function.AffectedRowsAfter ||
                                (function.StableByValueEquality &&
                                    rowDistance >= Math.Max(1L, function.EqualityStableAfterRows) &&
                                    _rowValueStable[f]);
                            if (!stable)
                            {
                                allStable = false;
                                break;
                            }
                        }
                        if (allStable)
                        {
                            stop = true;
                            break;
                        }
                    }
                }

                // The emitter's pending block copies reference this page's batch, copy them out before the
                // page is released.
                _emitter.FlushPending();

                if (pageDirty)
                {
                    await page.SavePage(false);
                }

                if (_emitter.Count >= OutputBatchSize)
                {
                    yield return new StreamEventBatch(_emitter.GetCurrentBatch());
                }

                if (!stop && endIndex < page.Keys.Count - 1)
                {
                    // The partition ended within this page.
                    break;
                }
            }

            for (int f = 0; f < _functions.Length; f++)
            {
                await _functions[f].EndScan();
            }
        }

        /// <summary>
        /// Computes all functions for every logical duplicate of a physical row, emits changes and updates
        /// the stored state lists. Returns true when the page was modified.
        /// The leaf's write lock is held while the stored state is mutated so a concurrent page
        /// serialization from cache eviction cannot observe a half written page. Functions never mutate
        /// the page themselves, their auxiliary values are buffered in the row context and applied here.
        /// </summary>
        private async ValueTask<bool> ProcessLogicalRows(
            ILockableObject pageLock,
            EventBatchData keyBatch,
            BulkWindowValueContainer values,
            int rowIndex,
            int weight,
            long rowsSinceChange)
        {
            Debug.Assert(_rowContext != null);
            Debug.Assert(_emitter != null);

            var context = _rowContext;
            context.Batch = keyBatch;
            context.RowIndex = rowIndex;
            context.Weight = weight;
            context.Values = values;

            var storedLength = _functions.Length > 0 ? values._functionStates[0].GetListLength(rowIndex) : 0;
            bool pageDirty = false;

            for (int f = 0; f < _functions.Length; f++)
            {
                _rowValueStable[f] = true;
            }

            pageLock.EnterWriteLock();
            for (int dup = 0; dup < weight; dup++)
            {
                context.DupIndex = dup;
                context.RowsSinceLastChange = rowsSinceChange + dup;

                bool anyChanged = false;
                for (int f = 0; f < _functions.Length; f++)
                {
                    context.ResetPendingAux();
                    if (!_functions[f].TryComputeRow(context, _resultContainers[f]))
                    {
                        // The asynchronous path may load pages, the lock cannot be held across it.
                        pageLock.ExitWriteLock();
                        await _functions[f].ComputeRow(context, _resultContainers[f]);
                        pageLock.EnterWriteLock();
                    }
                    if (context._pendingAuxCount > 0 && ApplyPendingAux(values, rowIndex, dup))
                    {
                        _rowValueStable[f] = false;
                        pageDirty = true;
                    }
                }

                if (dup < storedLength)
                {
                    bool valueChanged = false;
                    for (int f = 0; f < _functions.Length; f++)
                    {
                        _oldValueScratch[f] = values._functionStates[f].GetListElementValue(rowIndex, dup);
                        if (DataValueComparer.Instance.Compare(_oldValueScratch[f], _resultContainers[f]) != 0)
                        {
                            valueChanged = true;
                            _rowValueStable[f] = false;
                        }
                    }
                    if (valueChanged)
                    {
                        anyChanged = true;
                        // A suppressed row was never sent downstream, so it has nothing to retract and an
                        // update into suppression only retracts the previously sent values.
                        if (ShouldEmitRow(_oldValueScratch))
                        {
                            _emitter.AddOutputRow(keyBatch, rowIndex, _oldValueScratch, -1);
                        }
                        for (int f = 0; f < _functions.Length; f++)
                        {
                            values._functionStates[f].UpdateListElement(rowIndex, dup, _resultContainers[f]);
                        }
                        if (ShouldEmitRow(_resultValues))
                        {
                            _emitter.AddOutputRow(keyBatch, rowIndex, _resultValues, 1);
                        }
                    }
                }
                else
                {
                    anyChanged = true;
                    for (int f = 0; f < _functions.Length; f++)
                    {
                        values._functionStates[f].AppendToList(rowIndex, _resultContainers[f]);
                        _rowValueStable[f] = false;
                    }
                    if (ShouldEmitRow(_resultValues))
                    {
                        _emitter.AddOutputRow(keyBatch, rowIndex, _resultValues, 1);
                    }
                }

                if (anyChanged)
                {
                    values._previousValueSent.Set(rowIndex);
                    pageDirty = true;
                }
            }
            pageLock.ExitWriteLock();

            return pageDirty;
        }

        /// <summary>
        /// Applies the auxiliary values a function buffered for the current logical row. Must be called
        /// while the leaf's write lock is held. Returns true when any stored auxiliary value changed.
        /// </summary>
        private bool ApplyPendingAux(BulkWindowValueContainer values, int rowIndex, int dupIndex)
        {
            Debug.Assert(_rowContext != null);
            var context = _rowContext;
            bool changed = false;
            for (int i = 0; i < context._pendingAuxCount; i++)
            {
                var slot = context._pendingAuxSlots[i];
                var value = context._pendingAuxValues[slot]!;
                var list = values._functionStates[slot];
                var listLength = list.GetListLength(rowIndex);
                if (dupIndex < listLength)
                {
                    var existing = list.GetListElementValue(rowIndex, dupIndex);
                    if (DataValueComparer.Instance.Compare(existing, value) != 0)
                    {
                        list.UpdateListElement(rowIndex, dupIndex, value);
                        changed = true;
                    }
                }
                else
                {
                    Debug.Assert(dupIndex == listLength, "Auxiliary state must be appended in duplicate order");
                    list.AppendToList(rowIndex, value);
                    changed = true;
                }
            }
            return changed;
        }

        /// <summary>
        /// First computation after start, scans the whole persistent tree partition by partition and emits
        /// every value. The temporary tree is not used before the initial values have been sent.
        /// </summary>
        private async IAsyncEnumerable<StreamEventBatch> InitialFullCompute()
        {
            Debug.Assert(_scanIterator != null);
            Debug.Assert(_seedReader != null);
            Debug.Assert(_emitter != null);
            Debug.Assert(_partitionRangeComparer != null);
            Debug.Assert(_partitionScratchColumns != null);
            Debug.Assert(_partitionScratchBatch != null);

            _initialComputeSawData = false;
            await _scanIterator.SeekFirst();
            var enumerator = _scanIterator.GetAsyncEnumerator();

            var partitionRow = new ColumnRowReference()
            {
                referenceBatch = _partitionScratchBatch,
                RowIndex = 0
            };
            bool hasPartition = false;

            while (await enumerator.MoveNextAsync())
            {
                var page = enumerator.Current;
                if (page.CurrentPage == null || page.Keys == null || page.Keys.Count == 0)
                {
                    continue;
                }

                var keyBatch = page.Keys.Data;
                var values = page.Values;
                bool pageDirty = false;
                _initialComputeSawData = true;

                int index = 0;
                while (index < page.Keys.Count)
                {
                    int endIndex;
                    if (hasPartition)
                    {
                        _partitionRangeComparer.FindIndex(in partitionRow, page.Keys);
                        if (!_partitionRangeComparer.noMatch && index >= _partitionRangeComparer.start && index <= _partitionRangeComparer.end)
                        {
                            endIndex = _partitionRangeComparer.end;
                        }
                        else
                        {
                            // A new partition starts at this row.
                            await StartNewPartition(keyBatch, index);
                            _partitionRangeComparer.FindIndex(in partitionRow, page.Keys);
                            endIndex = _partitionRangeComparer.end;
                        }
                    }
                    else
                    {
                        await StartNewPartition(keyBatch, index);
                        hasPartition = true;
                        _partitionRangeComparer.FindIndex(in partitionRow, page.Keys);
                        endIndex = _partitionRangeComparer.end;
                    }

                    for (int i = index; i <= endIndex; i++)
                    {
                        var weight = values._weights.Get(i);
                        if (weight <= 0)
                        {
                            continue;
                        }
                        var rowChanged = await ProcessLogicalRows(page, keyBatch, values, i, weight, long.MaxValue / 2);
                        pageDirty |= rowChanged;
                    }

                    index = endIndex + 1;
                }

                // The emitter's pending block copies reference this page's batch, copy them out before the
                // page is released.
                _emitter.FlushPending();

                if (pageDirty)
                {
                    await page.SavePage(false);
                }

                if (_emitter.Count >= OutputBatchSize)
                {
                    yield return new StreamEventBatch(_emitter.GetCurrentBatch());
                }
            }

            if (hasPartition)
            {
                for (int f = 0; f < _functions.Length; f++)
                {
                    await _functions[f].EndScan();
                }
            }

            if (_emitter.Count > 0)
            {
                yield return new StreamEventBatch(_emitter.GetCurrentBatch());
            }
        }

        private async ValueTask StartNewPartition(EventBatchData keyBatch, int rowIndex)
        {
            Debug.Assert(_partitionScratchColumns != null);
            Debug.Assert(_partitionScratchBatch != null);
            Debug.Assert(_seedReader != null);

            for (int f = 0; f < _functions.Length; f++)
            {
                await _functions[f].EndScan();
            }

            for (int c = 0; c < _totalKeyColumns; c++)
            {
                _partitionScratchColumns[c].UpdateAt(0, keyBatch.Columns[c].GetValueAt(rowIndex, default));
            }

            _seedReader.ResetEmpty();
            var partitionRow = new ColumnRowReference()
            {
                referenceBatch = _partitionScratchBatch,
                RowIndex = 0
            };
            for (int f = 0; f < _functions.Length; f++)
            {
                await _functions[f].StartScan(partitionRow, _seedReader, true);
            }
        }

        protected override async Task InitializeOrRestore(IStateManagerClient stateManagerClient)
        {
            if (_eventsOutCounter == null)
            {
                _eventsOutCounter = Metrics.CreateCounter<long>("events");
            }
            if (_eventsInCounter == null)
            {
                _eventsInCounter = Metrics.CreateCounter<long>("events_processed");
            }

            // The non bulk WindowOperator stored its state under "persistent"/"temporary" with an
            // incompatible layout. Restoring such state silently as an empty bulk tree would produce
            // permanently wrong output, so fail loudly instead.
            if (stateManagerClient.StateExists("persistent"))
            {
                throw new InvalidOperationException(
                    "Found existing window operator state written by the previous non bulk window operator. " +
                    "The bulk window operator stores its state differently and cannot restore it. " +
                    "Reset the stream state, or run a version that uses the previous window operator.");
            }

            _hasSentInitialData = await stateManagerClient.GetOrCreateObjectStateAsync<bool>("initialDataSent");

            // The state column layout depends on each function's auxiliary column count, which can
            // change between builds. Restoring a checkpoint with another layout would silently read
            // shifted columns, so the layout is stored with the state and validated on restore.
            var auxCounts = new string[_functions.Length];
            for (int f = 0; f < _functions.Length; f++)
            {
                auxCounts[f] = _functions[f].AuxiliaryStateColumnCount.ToString();
            }
            var layout = string.Join(",", auxCounts);
            _stateLayout = await stateManagerClient.GetOrCreateObjectStateAsync<string>("state_layout");
            if (_stateLayout.Value == null)
            {
                _stateLayout.Value = layout;
            }
            else if (_stateLayout.Value != layout)
            {
                throw new InvalidOperationException(
                    $"Found existing bulk window operator state with a different function state layout, stored auxiliary column counts '{_stateLayout.Value}' but this version uses '{layout}'. " +
                    "Reset the stream state, or run a version with a matching window function state layout.");
            }

            _persistentTree = await stateManagerClient.GetOrCreateTree("persistent_v1",
                new BPlusTreeOptions<ColumnRowReference, BulkWindowValue, ColumnKeyStorageContainer, BulkWindowValueContainer>()
                {
                    Comparer = new BulkWindowInsertComparer(_orderCompareFunction, _partitionColumns, _otherColumns),
                    KeySerializer = new ColumnStoreSerializer(_totalKeyColumns, MemoryAllocator),
                    ValueSerializer = new BulkWindowValueContainerSerializer(_totalStateColumns, MemoryAllocator),
                    MemoryAllocator = MemoryAllocator,
                    UseByteBasedPageSizes = true,
                    UsePreviousPointers = true
                });
            _persistentBulkInserter = _persistentTree.CreateBulkInserter();
            _scanIterator = _persistentTree.CreateIterator();

            _temporaryTree = await stateManagerClient.GetOrCreateTree("temporary_v1",
                new BPlusTreeOptions<ColumnRowReference, int, ColumnKeyStorageContainer, PrimitiveListValueContainer<int>>()
                {
                    Comparer = new BulkWindowInsertComparer(_orderCompareFunction, _partitionColumns, _otherColumns),
                    KeySerializer = new ColumnStoreSerializer(_totalKeyColumns, MemoryAllocator),
                    ValueSerializer = new PrimitiveListValueContainerSerializer<int>(MemoryAllocator),
                    MemoryAllocator = MemoryAllocator,
                    UseByteBasedPageSizes = true
                });
            _temporaryBulkInserter = _temporaryTree.CreateBulkInserter();

            _emitter = new BulkWindowEmitter(_inputColumnCount, _emitList, MemoryAllocator);
            _markerComparer = new BulkWindowInsertComparer(_orderCompareFunction, _partitionColumns, _otherColumns);
            _partitionRangeComparer = new BulkWindowPartitionComparer(_partitionColumns);
            _partitionSeekComparer = new BulkWindowPartitionComparer(_partitionColumns);
            _backwardReader = new BulkWindowBackwardPartitionReader(
                _persistentTree,
                new BulkWindowInsertComparer(_orderCompareFunction, _partitionColumns, _otherColumns),
                _partitionColumns);
            _seedReader = new BulkWindowSeedReader(_backwardReader, _totalKeyColumns, _totalStateColumns, MemoryAllocator);
            _rowContext = new BulkWindowRowContext(_totalStateColumns);

            _resultContainers = new DataValueContainer[_functions.Length];
            _resultValues = new IDataValue[_functions.Length];
            _oldValueScratch = new IDataValue[_functions.Length];
            _mutatorScratch = new IDataValue[_functions.Length];
            _rowValueStable = new bool[_functions.Length];
            for (int f = 0; f < _functions.Length; f++)
            {
                _resultContainers[f] = new DataValueContainer();
                _resultValues[f] = _resultContainers[f];
            }

            _markerColumns = new ColumnStore.Column[_totalKeyColumns];
            _partitionScratchColumns = new ColumnStore.Column[_totalKeyColumns];
            _scanStartColumns = new ColumnStore.Column[_totalKeyColumns];
            for (int c = 0; c < _totalKeyColumns; c++)
            {
                _markerColumns[c] = new ColumnStore.Column(MemoryAllocator);
                _partitionScratchColumns[c] = new ColumnStore.Column(MemoryAllocator);
                _partitionScratchColumns[c].Add(NullValue.Instance);
                _scanStartColumns[c] = new ColumnStore.Column(MemoryAllocator);
                _scanStartColumns[c].Add(NullValue.Instance);
            }
            _markerBatch = new EventBatchData(_markerColumns);
            _partitionScratchBatch = new EventBatchData(_partitionScratchColumns);
            _scanStartBatch = new EventBatchData(_scanStartColumns);
            _markerCount = 0;

            for (int f = 0; f < _functions.Length; f++)
            {
                await _functions[f].Initialize(new BulkWindowFunctionContext()
                {
                    PersistentTree = _persistentTree,
                    PartitionColumns = _partitionColumns,
                    CreateInsertComparer = () => new BulkWindowInsertComparer(_orderCompareFunction, _partitionColumns, _otherColumns),
                    FunctionIndex = f,
                    AuxiliaryColumnStartIndex = _auxiliaryStartIndices[f],
                    MemoryAllocator = MemoryAllocator,
                    StateManagerClient = stateManagerClient.GetChildManager(f.ToString())
                });
            }
        }
    }
}
