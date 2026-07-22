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
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Core.Operators.Window.Bulk;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Substrait.Expressions;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;

namespace FlowtideDotNet.Core.Compute.Columnar.Functions.WindowFunctions.Bulk
{
    internal class BulkLastValueWindowFunctionDefinition : BulkWindowFunctionDefinition
    {
        public override bool TryCreate(WindowFunction windowFunction, IFunctionsRegister functionsRegister, [NotNullWhen(true)] out IBulkWindowFunction? bulkWindowFunction)
        {
            bulkWindowFunction = null;
            if (windowFunction.Arguments.Count < 1)
            {
                return false;
            }
            var compiledValue = ColumnProjectCompiler.CompileToValue(windowFunction.Arguments[0], functionsRegister);

            bool ignoreNull = false;
            if (windowFunction.Options != null &&
                windowFunction.Options.TryGetValue("NULL_TREATMENT", out var ignoreNullStr) &&
                ignoreNullStr.Equals("IGNORE_NULLS", StringComparison.OrdinalIgnoreCase))
            {
                ignoreNull = true;
            }

            var bounds = BulkWindowFrameBounds.Parse(windowFunction);
            switch (bounds.Kind)
            {
                case BulkWindowFrameKind.WholePartition:
                    bulkWindowFunction = new BulkLastValueWindowFunctionUnbounded(compiledValue, ignoreNull);
                    return true;
                case BulkWindowFrameKind.UnboundedPreceding:
                case BulkWindowFrameKind.BoundedRows:
                    if (bounds.To == long.MaxValue)
                    {
                        // Respect nulls to the partition end is the whole partition variant.
                        bulkWindowFunction = ignoreNull
                            ? new BulkLastValueIgnoreNullsSuffixWindowFunction(compiledValue, bounds.From)
                            : new BulkLastValueWindowFunctionUnbounded(compiledValue, false);
                        return true;
                    }
                    if (ignoreNull && bounds.From != long.MinValue && bounds.From > bounds.To)
                    {
                        // Ignore nulls with an empty frame is always null.
                        bulkWindowFunction = new BulkEmptyFrameWindowFunction();
                        return true;
                    }
                    if (bounds.To > 0)
                    {
                        bulkWindowFunction = ignoreNull
                            ? new BulkLastValueIgnoreNullsLookaheadWindowFunction(compiledValue, bounds.From, bounds.To)
                            : new BulkLastValueRespectLookaheadWindowFunction(compiledValue, bounds.To);
                        return true;
                    }
                    bulkWindowFunction = ignoreNull
                        ? new BulkLastValueIgnoreNullsDelayWindowFunction(compiledValue, bounds.From, bounds.To)
                        : new BulkLastValueRespectDelayWindowFunction(compiledValue, bounds.To);
                    return true;
                default:
                    return false;
            }
        }
    }

    /// <summary>
    /// last_value respect nulls, frame ending at or before the row, the value at the frame end.
    /// </summary>
    internal class BulkLastValueRespectDelayWindowFunction : IBulkWindowFunction
    {
        private readonly Func<EventBatchData, int, IDataValue> _fetchValueFunction;
        private readonly long _to;
        private BulkWindowValueRing? _pending;

        public BulkLastValueRespectDelayWindowFunction(Func<EventBatchData, int, IDataValue> fetchValueFunction, long to)
        {
            Debug.Assert(to <= 0);
            _fetchValueFunction = fetchValueFunction;
            _to = to;
        }

        public long AffectedRowsBefore => 0;

        public long AffectedRowsAfter => -_to;

        public bool StableByValueEquality => false;

        public long EqualityStableAfterRows => long.MaxValue;

        public int AuxiliaryStateColumnCount => 0;

        public Task Initialize(BulkWindowFunctionContext context)
        {
            _pending = new BulkWindowValueRing(-_to + 1, context.MemoryAllocator);
            return Task.CompletedTask;
        }

        public ValueTask Commit()
        {
            return ValueTask.CompletedTask;
        }

        public async ValueTask StartScan(ColumnRowReference partitionValues, BulkWindowSeedReader seedReader, bool fromPartitionStart)
        {
            Debug.Assert(_pending != null);
            _pending.Clear();
            if (fromPartitionStart || _to == 0)
            {
                return;
            }
            await seedReader.EnsureRows((int)Math.Min(-_to, int.MaxValue));
            var available = (int)Math.Min(-_to, seedReader.MaterializedRows);
            for (int back = available; back >= 1; back--)
            {
                var row = seedReader.GetRow(back);
                _pending.Push(_fetchValueFunction(row.referenceBatch, row.RowIndex));
            }
        }

        public bool TryComputeRow(BulkWindowRowContext context, DataValueContainer result)
        {
            Debug.Assert(_pending != null);
            _pending.Push(_fetchValueFunction(context.Batch, context.RowIndex));
            if (_pending.Count > -_to)
            {
                _pending.PopOldest().CopyToContainer(result);
            }
            else
            {
                result._type = ArrowTypeId.Null;
            }
            return true;
        }

        public ValueTask ComputeRow(BulkWindowRowContext context, DataValueContainer result)
        {
            TryComputeRow(context, result);
            return ValueTask.CompletedTask;
        }

        public ValueTask EndScan()
        {
            return ValueTask.CompletedTask;
        }
    }

    /// <summary>
    /// last_value respect nulls, frame ending ahead, read through a lookahead.
    /// </summary>
    internal class BulkLastValueRespectLookaheadWindowFunction : IBulkWindowFunction
    {
        private readonly Func<EventBatchData, int, IDataValue> _fetchValueFunction;
        private readonly long _to;

        private BulkWindowForwardPartitionReader? _lookahead;
        private IMemoryAllocator? _memoryAllocator;
        private Column? _lastValueColumn;
        private bool _anyFed;

        private long _currentPosition;
        private long _nextFeedPosition;
        private long _lookaheadPosition;
        private bool _lookaheadStarted;
        private bool _lookaheadDone;

        public BulkLastValueRespectLookaheadWindowFunction(Func<EventBatchData, int, IDataValue> fetchValueFunction, long to)
        {
            Debug.Assert(to > 0);
            _fetchValueFunction = fetchValueFunction;
            _to = to;
        }

        public long AffectedRowsBefore => _to;

        public long AffectedRowsAfter => 0;

        public bool StableByValueEquality => false;

        public long EqualityStableAfterRows => long.MaxValue;

        public int AuxiliaryStateColumnCount => 0;

        public Task Initialize(BulkWindowFunctionContext context)
        {
            _memoryAllocator = context.MemoryAllocator;
            _lookahead = new BulkWindowForwardPartitionReader(context.PersistentTree, context.PartitionColumns, context.CreateInsertComparer());
            _lastValueColumn = Column.Create(context.MemoryAllocator);
            _lastValueColumn.Add(NullValue.Instance);
            return Task.CompletedTask;
        }

        public ValueTask Commit()
        {
            return ValueTask.CompletedTask;
        }

        public ValueTask StartScan(ColumnRowReference partitionValues, BulkWindowSeedReader seedReader, bool fromPartitionStart)
        {
            Debug.Assert(_lastValueColumn != null);
            _lastValueColumn.UpdateAt(0, NullValue.Instance);
            _anyFed = false;
            _currentPosition = -1;
            _nextFeedPosition = 0;
            _lookaheadStarted = false;
            _lookaheadDone = false;
            return ValueTask.CompletedTask;
        }

        public bool TryComputeRow(BulkWindowRowContext context, DataValueContainer result)
        {
            return false;
        }

        public async ValueTask ComputeRow(BulkWindowRowContext context, DataValueContainer result)
        {
            Debug.Assert(_lookahead != null);
            Debug.Assert(_memoryAllocator != null);
            Debug.Assert(_lastValueColumn != null);

            _currentPosition++;

            if (!_lookaheadStarted)
            {
                _lookaheadStarted = true;
                await _lookahead.ResetAtRow(new ColumnRowReference() { referenceBatch = context.Batch, RowIndex = context.RowIndex }, _memoryAllocator);
                _lookaheadPosition = _currentPosition - context.DupIndex - 1;
            }

            while (!_lookaheadDone && _nextFeedPosition <= _currentPosition + _to)
            {
                if (!await _lookahead.MoveNextLogical())
                {
                    _lookaheadDone = true;
                    break;
                }
                _lookaheadPosition++;
                if (_lookaheadPosition < _nextFeedPosition)
                {
                    continue;
                }
                _lastValueColumn.UpdateAt(0, _fetchValueFunction(_lookahead.Batch, _lookahead.RowIndex));
                _anyFed = true;
                _nextFeedPosition++;
            }

            if (_anyFed)
            {
                _lastValueColumn.GetValueAt(0, result, default);
            }
            else
            {
                result._type = ArrowTypeId.Null;
            }
        }

        public ValueTask EndScan()
        {
            return ValueTask.CompletedTask;
        }
    }

    /// <summary>
    /// last_value ignore nulls, frame ending at or before the row, candidate position in aux state.
    /// </summary>
    internal class BulkLastValueIgnoreNullsDelayWindowFunction : IBulkWindowFunction
    {
        private readonly Func<EventBatchData, int, IDataValue> _fetchValueFunction;
        private readonly long _from;
        private readonly long _to;
        private int _functionIndex;
        private int _auxStartIndex;

        private BulkWindowValueRing? _pending;
        private Column? _candidateColumn;
        private bool _hasCandidate;
        private long _candidateOffsetBack;
        private readonly DataValueContainer _auxOffsetContainer = new DataValueContainer();

        public BulkLastValueIgnoreNullsDelayWindowFunction(Func<EventBatchData, int, IDataValue> fetchValueFunction, long from, long to)
        {
            Debug.Assert(to <= 0);
            _fetchValueFunction = fetchValueFunction;
            _from = from;
            _to = to;
        }

        public long AffectedRowsBefore => 0;

        public long AffectedRowsAfter => _from == long.MinValue ? long.MaxValue : -_from;

        public bool StableByValueEquality => _from == long.MinValue;

        public long EqualityStableAfterRows => -_to;

        public int AuxiliaryStateColumnCount => _from == long.MinValue ? 0 : 1;

        public Task Initialize(BulkWindowFunctionContext context)
        {
            _functionIndex = context.FunctionIndex;
            _auxStartIndex = context.AuxiliaryColumnStartIndex;
            _pending = new BulkWindowValueRing(-_to + 1, context.MemoryAllocator);
            _candidateColumn = Column.Create(context.MemoryAllocator);
            _candidateColumn.Add(NullValue.Instance);
            return Task.CompletedTask;
        }

        public ValueTask Commit()
        {
            return ValueTask.CompletedTask;
        }

        public async ValueTask StartScan(ColumnRowReference partitionValues, BulkWindowSeedReader seedReader, bool fromPartitionStart)
        {
            Debug.Assert(_pending != null);
            Debug.Assert(_candidateColumn != null);

            _pending.Clear();
            _hasCandidate = false;

            if (fromPartitionStart)
            {
                return;
            }

            var lookback = (int)Math.Min(Math.Max(1, -_to), int.MaxValue);
            await seedReader.EnsureRows(lookback);
            if (seedReader.MaterializedRows == 0)
            {
                return;
            }

            // A null previous output seeds the same as no candidate.
            var previousOutput = seedReader.GetState(1, _functionIndex);
            if (!previousOutput.IsNull)
            {
                _hasCandidate = true;
                _candidateColumn.UpdateAt(0, previousOutput);
                if (_from == long.MinValue)
                {
                    _candidateOffsetBack = 0;
                }
                else
                {
                    var offsetState = seedReader.GetState(1, _auxStartIndex);
                    _candidateOffsetBack = offsetState.IsNull ? 0 : offsetState.AsLong;
                }
            }

            var pendingRows = (int)Math.Min(-_to, seedReader.MaterializedRows);
            for (int back = pendingRows; back >= 1; back--)
            {
                var row = seedReader.GetRow(back);
                _pending.Push(_fetchValueFunction(row.referenceBatch, row.RowIndex));
            }
        }

        public bool TryComputeRow(BulkWindowRowContext context, DataValueContainer result)
        {
            Debug.Assert(_pending != null);
            Debug.Assert(_candidateColumn != null);

            if (_hasCandidate)
            {
                _candidateOffsetBack++;
            }

            _pending.Push(_fetchValueFunction(context.Batch, context.RowIndex));
            if (_pending.Count > -_to)
            {
                var fed = _pending.PopOldest();
                if (!fed.IsNull)
                {
                    _hasCandidate = true;
                    _candidateOffsetBack = -_to;
                    _candidateColumn.UpdateAt(0, fed);
                }
            }

            bool insideFrame = _hasCandidate && (_from == long.MinValue || _candidateOffsetBack <= -_from);
            if (insideFrame)
            {
                _candidateColumn.GetValueAt(0, result, default);
            }
            else
            {
                result._type = ArrowTypeId.Null;
            }

            if (_from != long.MinValue)
            {
                if (insideFrame)
                {
                    _auxOffsetContainer._type = ArrowTypeId.Int64;
                    _auxOffsetContainer._int64Value = new Int64Value(_candidateOffsetBack);
                    context.SetAuxValue(_auxStartIndex, _auxOffsetContainer);
                }
                else
                {
                    context.SetAuxValue(_auxStartIndex, NullValue.Instance);
                }
            }
            return true;
        }

        public ValueTask ComputeRow(BulkWindowRowContext context, DataValueContainer result)
        {
            TryComputeRow(context, result);
            return ValueTask.CompletedTask;
        }

        public ValueTask EndScan()
        {
            return ValueTask.CompletedTask;
        }
    }

    /// <summary>
    /// last_value ignore nulls, frame ending ahead, absolute candidate position.
    /// </summary>
    internal class BulkLastValueIgnoreNullsLookaheadWindowFunction : IBulkWindowFunction
    {
        private readonly Func<EventBatchData, int, IDataValue> _fetchValueFunction;
        private readonly long _from;
        private readonly long _to;
        private int _functionIndex;
        private int _auxStartIndex;

        private BulkWindowForwardPartitionReader? _lookahead;
        private IMemoryAllocator? _memoryAllocator;
        private Column? _candidateColumn;
        private bool _hasCandidate;
        private long _candidatePosition;
        private readonly DataValueContainer _auxOffsetContainer = new DataValueContainer();

        private long _currentPosition;
        private long _nextFeedPosition;
        private long _lookaheadPosition;
        private bool _lookaheadStarted;
        private bool _lookaheadDone;

        public BulkLastValueIgnoreNullsLookaheadWindowFunction(Func<EventBatchData, int, IDataValue> fetchValueFunction, long from, long to)
        {
            Debug.Assert(to > 0);
            _fetchValueFunction = fetchValueFunction;
            _from = from;
            _to = to;
        }

        public long AffectedRowsBefore => _to;

        public long AffectedRowsAfter => _from == long.MinValue ? long.MaxValue : Math.Max(0, -_from);

        public bool StableByValueEquality => _from == long.MinValue;

        public long EqualityStableAfterRows => 0;

        public int AuxiliaryStateColumnCount => _from == long.MinValue ? 0 : 1;

        public Task Initialize(BulkWindowFunctionContext context)
        {
            _functionIndex = context.FunctionIndex;
            _auxStartIndex = context.AuxiliaryColumnStartIndex;
            _memoryAllocator = context.MemoryAllocator;
            _lookahead = new BulkWindowForwardPartitionReader(context.PersistentTree, context.PartitionColumns, context.CreateInsertComparer());
            _candidateColumn = Column.Create(context.MemoryAllocator);
            _candidateColumn.Add(NullValue.Instance);
            return Task.CompletedTask;
        }

        public ValueTask Commit()
        {
            return ValueTask.CompletedTask;
        }

        public async ValueTask StartScan(ColumnRowReference partitionValues, BulkWindowSeedReader seedReader, bool fromPartitionStart)
        {
            Debug.Assert(_candidateColumn != null);

            _hasCandidate = false;
            _currentPosition = -1;
            _nextFeedPosition = 0;
            _lookaheadStarted = false;
            _lookaheadDone = false;

            if (!fromPartitionStart && await seedReader.EnsureRows(1))
            {
                var previousOutput = seedReader.GetState(1, _functionIndex);
                if (!previousOutput.IsNull)
                {
                    _hasCandidate = true;
                    _candidateColumn.UpdateAt(0, previousOutput);
                    if (_from == long.MinValue)
                    {
                        _candidatePosition = -1;
                    }
                    else
                    {
                        var offsetState = seedReader.GetState(1, _auxStartIndex);
                        _candidatePosition = -1 - (offsetState.IsNull ? 0 : offsetState.AsLong);
                    }
                }
                // Previous output already covers rows up to its frame end.
                _nextFeedPosition = _to;
            }
        }

        public bool TryComputeRow(BulkWindowRowContext context, DataValueContainer result)
        {
            return false;
        }

        public async ValueTask ComputeRow(BulkWindowRowContext context, DataValueContainer result)
        {
            Debug.Assert(_lookahead != null);
            Debug.Assert(_memoryAllocator != null);
            Debug.Assert(_candidateColumn != null);

            _currentPosition++;

            if (!_lookaheadStarted)
            {
                _lookaheadStarted = true;
                await _lookahead.ResetAtRow(new ColumnRowReference() { referenceBatch = context.Batch, RowIndex = context.RowIndex }, _memoryAllocator);
                _lookaheadPosition = _currentPosition - context.DupIndex - 1;
            }

            while (!_lookaheadDone && _nextFeedPosition <= _currentPosition + _to)
            {
                if (!await _lookahead.MoveNextLogical())
                {
                    _lookaheadDone = true;
                    break;
                }
                _lookaheadPosition++;
                if (_lookaheadPosition < _nextFeedPosition)
                {
                    continue;
                }
                var value = _fetchValueFunction(_lookahead.Batch, _lookahead.RowIndex);
                if (!value.IsNull)
                {
                    _hasCandidate = true;
                    _candidatePosition = _nextFeedPosition;
                    _candidateColumn.UpdateAt(0, value);
                }
                _nextFeedPosition++;
            }

            bool insideFrame = _hasCandidate && (_from == long.MinValue || _candidatePosition >= _currentPosition + _from);
            if (insideFrame)
            {
                _candidateColumn.GetValueAt(0, result, default);
            }
            else
            {
                result._type = ArrowTypeId.Null;
            }

            if (_from != long.MinValue)
            {
                if (insideFrame)
                {
                    _auxOffsetContainer._type = ArrowTypeId.Int64;
                    _auxOffsetContainer._int64Value = new Int64Value(_currentPosition - _candidatePosition);
                    context.SetAuxValue(_auxStartIndex, _auxOffsetContainer);
                }
                else
                {
                    context.SetAuxValue(_auxStartIndex, NullValue.Instance);
                }
            }
        }

        public ValueTask EndScan()
        {
            return ValueTask.CompletedTask;
        }
    }

    /// <summary>
    /// last_value over the whole partition, computed with a single pre-scan to the partition end.
    /// </summary>
    internal class BulkLastValueWindowFunctionUnbounded : IBulkWindowFunction
    {
        private readonly Func<EventBatchData, int, IDataValue> _fetchValueFunction;
        private readonly bool _ignoreNulls;

        private BulkWindowForwardPartitionReader? _reader;
        private Column? _lastValueColumn;
        private bool _hasValue;

        public BulkLastValueWindowFunctionUnbounded(Func<EventBatchData, int, IDataValue> fetchValueFunction, bool ignoreNulls)
        {
            _fetchValueFunction = fetchValueFunction;
            _ignoreNulls = ignoreNulls;
        }

        public long AffectedRowsBefore => long.MaxValue;

        public long AffectedRowsAfter => long.MaxValue;

        public bool StableByValueEquality => true;

        public long EqualityStableAfterRows => 0;

        public int AuxiliaryStateColumnCount => 0;

        public Task Initialize(BulkWindowFunctionContext context)
        {
            _reader = new BulkWindowForwardPartitionReader(context.PersistentTree, context.PartitionColumns);
            _lastValueColumn = Column.Create(context.MemoryAllocator);
            _lastValueColumn.Add(NullValue.Instance);
            return Task.CompletedTask;
        }

        public ValueTask Commit()
        {
            return ValueTask.CompletedTask;
        }

        public async ValueTask StartScan(ColumnRowReference partitionValues, BulkWindowSeedReader seedReader, bool fromPartitionStart)
        {
            Debug.Assert(_reader != null);
            Debug.Assert(_lastValueColumn != null);

            _hasValue = false;
            await _reader.Reset(partitionValues);
            while (await _reader.MoveNextRow())
            {
                var value = _fetchValueFunction(_reader.Batch, _reader.RowIndex);
                if (_ignoreNulls && value.IsNull)
                {
                    continue;
                }
                _lastValueColumn.UpdateAt(0, value);
                _hasValue = true;
            }
        }

        public bool TryComputeRow(BulkWindowRowContext context, DataValueContainer result)
        {
            Debug.Assert(_lastValueColumn != null);
            if (_hasValue)
            {
                _lastValueColumn.GetValueAt(0, result, default);
            }
            else
            {
                result._type = ArrowTypeId.Null;
            }
            return true;
        }

        public ValueTask ComputeRow(BulkWindowRowContext context, DataValueContainer result)
        {
            TryComputeRow(context, result);
            return ValueTask.CompletedTask;
        }

        public ValueTask EndScan()
        {
            return ValueTask.CompletedTask;
        }
    }

    /// <summary>
    /// last_value ignore nulls to the partition end, e.g. ROWS BETWEEN 4 PRECEDING AND UNBOUNDED FOLLOWING.
    /// A pre-scan finds the last non null, null when the frame starts after it.
    /// </summary>
    internal class BulkLastValueIgnoreNullsSuffixWindowFunction : IBulkWindowFunction
    {
        private readonly Func<EventBatchData, int, IDataValue> _fetchValueFunction;
        private readonly long _from;

        private BulkWindowForwardPartitionReader? _reader;
        private Column? _lastValueColumn;
        private bool _hasValue;
        private long _lastValuePosition;
        private long _currentPosition;

        public BulkLastValueIgnoreNullsSuffixWindowFunction(Func<EventBatchData, int, IDataValue> fetchValueFunction, long from)
        {
            Debug.Assert(from != long.MinValue);
            _fetchValueFunction = fetchValueFunction;
            _from = from;
        }

        public long AffectedRowsBefore => long.MaxValue;

        public long AffectedRowsAfter => Math.Max(0, -_from);

        public bool StableByValueEquality => false;

        public long EqualityStableAfterRows => long.MaxValue;

        public int AuxiliaryStateColumnCount => 0;

        public Task Initialize(BulkWindowFunctionContext context)
        {
            _reader = new BulkWindowForwardPartitionReader(context.PersistentTree, context.PartitionColumns);
            _lastValueColumn = Column.Create(context.MemoryAllocator);
            _lastValueColumn.Add(NullValue.Instance);
            return Task.CompletedTask;
        }

        public ValueTask Commit()
        {
            return ValueTask.CompletedTask;
        }

        public async ValueTask StartScan(ColumnRowReference partitionValues, BulkWindowSeedReader seedReader, bool fromPartitionStart)
        {
            Debug.Assert(_reader != null);
            Debug.Assert(_lastValueColumn != null);

            _hasValue = false;
            _lastValuePosition = -1;
            _currentPosition = -1;

            long position = 0;
            await _reader.Reset(partitionValues);
            while (await _reader.MoveNextRow())
            {
                var value = _fetchValueFunction(_reader.Batch, _reader.RowIndex);
                if (!value.IsNull)
                {
                    _lastValueColumn.UpdateAt(0, value);
                    _hasValue = true;
                    // The row's last duplicate is the frame position.
                    _lastValuePosition = position + _reader.Weight - 1;
                }
                position += _reader.Weight;
            }
        }

        public bool TryComputeRow(BulkWindowRowContext context, DataValueContainer result)
        {
            Debug.Assert(_lastValueColumn != null);

            _currentPosition++;
            if (_hasValue && _lastValuePosition >= _currentPosition + _from)
            {
                _lastValueColumn.GetValueAt(0, result, default);
            }
            else
            {
                result._type = ArrowTypeId.Null;
            }
            return true;
        }

        public ValueTask ComputeRow(BulkWindowRowContext context, DataValueContainer result)
        {
            TryComputeRow(context, result);
            return ValueTask.CompletedTask;
        }

        public ValueTask EndScan()
        {
            return ValueTask.CompletedTask;
        }
    }
}
