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
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Core.Compute.Columnar.Functions.WindowFunctions.MinMax;
using FlowtideDotNet.Core.Operators.Window.Bulk;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Substrait.Expressions;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;

namespace FlowtideDotNet.Core.Compute.Columnar.Functions.WindowFunctions.Bulk
{
    internal class BulkSumWindowFunctionDefinition : BulkWindowFunctionDefinition
    {
        public override bool TryCreate(WindowFunction windowFunction, IFunctionsRegister functionsRegister, [NotNullWhen(true)] out IBulkWindowFunction? bulkWindowFunction)
        {
            bulkWindowFunction = null;
            if (windowFunction.Arguments.Count < 1)
            {
                return false;
            }
            var compiledValue = ColumnProjectCompiler.CompileToValue(windowFunction.Arguments[0], functionsRegister);
            var bounds = BulkWindowFrameBounds.Parse(windowFunction);

            // Variant selection matches the non bulk SumWindowFunctionDefinition.
            switch (bounds.Kind)
            {
                case BulkWindowFrameKind.BoundedRows:
                    if (bounds.To == long.MaxValue)
                    {
                        bulkWindowFunction = bounds.From > 0
                            ? new BulkSumWindowFunctionSuffixFollowing(compiledValue, bounds.From)
                            : new BulkSumWindowFunctionSuffix(compiledValue, bounds.From);
                        return true;
                    }
                    if (bounds.From > bounds.To)
                    {
                        bulkWindowFunction = new BulkEmptyFrameWindowFunction();
                        return true;
                    }
                    if (bounds.To > 0)
                    {
                        bulkWindowFunction = new BulkSumWindowFunctionBoundedFollowing(compiledValue, bounds.From, bounds.To);
                        return true;
                    }
                    bulkWindowFunction = new BulkSumWindowFunctionBounded(compiledValue, bounds.From, bounds.To);
                    return true;
                case BulkWindowFrameKind.UnboundedPreceding:
                    if (bounds.To > 0)
                    {
                        bulkWindowFunction = new BulkSumWindowFunctionUnboundedFromFollowing(compiledValue, bounds.To);
                        return true;
                    }
                    bulkWindowFunction = new BulkSumWindowFunctionUnboundedFrom(compiledValue, bounds.To);
                    return true;
                default:
                    bulkWindowFunction = new BulkSumWindowFunctionUnbounded(compiledValue);
                    return true;
            }
        }
    }

    internal static class BulkSumUtils
    {
        /// <summary>
        /// Copies a sum state (int64, double, decimal or null) into a container.
        /// </summary>
        public static void CopySumValue(IDataValue source, DataValueContainer target)
        {
            switch (source.Type)
            {
                case ArrowTypeId.Int64:
                    target._type = ArrowTypeId.Int64;
                    target._int64Value = new Int64Value(source.AsLong);
                    break;
                case ArrowTypeId.Double:
                    target._type = ArrowTypeId.Double;
                    target._doubleValue = new DoubleValue(source.AsDouble);
                    break;
                case ArrowTypeId.Decimal128:
                    target._type = ArrowTypeId.Decimal128;
                    target._decimalValue = new DecimalValue(source.AsDecimal);
                    break;
                default:
                    target._type = ArrowTypeId.Null;
                    break;
            }
        }
    }

    /// <summary>
    /// Sum over a bounded frame not reaching ahead, e.g. ROWS BETWEEN 4 PRECEDING AND 1 PRECEDING.
    /// </summary>
    internal class BulkSumWindowFunctionBounded : IBulkWindowFunction
    {
        private readonly Func<EventBatchData, int, IDataValue> _fetchValueFunction;
        private readonly long _from;
        private readonly long _to;
        private readonly long _frameSize;

        private BulkWindowValueRing? _pending;
        private BulkWindowValueRing? _frame;
        private readonly DataValueContainer _sumState = new DataValueContainer();

        public BulkSumWindowFunctionBounded(Func<EventBatchData, int, IDataValue> fetchValueFunction, long from, long to)
        {
            Debug.Assert(from <= to && to <= 0);
            _fetchValueFunction = fetchValueFunction;
            _from = from;
            _to = to;
            _frameSize = to - from + 1;
        }

        public long AffectedRowsBefore => 0;

        public long AffectedRowsAfter => -_from;

        public bool StableByValueEquality => false;

        public long EqualityStableAfterRows => long.MaxValue;

        public int AuxiliaryStateColumnCount => 0;

        public Task Initialize(BulkWindowFunctionContext context)
        {
            _pending = new BulkWindowValueRing(-_to + 1, context.MemoryAllocator);
            _frame = new BulkWindowValueRing(_frameSize + 1, context.MemoryAllocator);
            return Task.CompletedTask;
        }

        public ValueTask Commit()
        {
            return ValueTask.CompletedTask;
        }

        public async ValueTask StartScan(ColumnRowReference partitionValues, BulkWindowSeedReader seedReader, bool fromPartitionStart)
        {
            Debug.Assert(_pending != null);
            Debug.Assert(_frame != null);

            _pending.Clear();
            _frame.Clear();
            _sumState._type = ArrowTypeId.Null;

            if (fromPartitionStart)
            {
                return;
            }

            var lookback = (int)Math.Min(-_from, int.MaxValue);
            await seedReader.EnsureRows(lookback);
            var available = Math.Min(lookback, seedReader.MaterializedRows);
            for (int back = available; back >= 1; back--)
            {
                var row = seedReader.GetRow(back);
                Feed(_fetchValueFunction(row.referenceBatch, row.RowIndex));
            }
        }

        private void Feed(IDataValue value)
        {
            Debug.Assert(_pending != null);
            Debug.Assert(_frame != null);

            _pending.Push(value);
            if (_pending.Count > -_to)
            {
                var enteringFrame = _pending.PopOldest();
                SumWindowUtils.DoSum(enteringFrame, _sumState, 1);
                _frame.Push(enteringFrame);
                while (_frame.Count > _frameSize)
                {
                    var leavingFrame = _frame.PopOldest();
                    SumWindowUtils.DoSum(leavingFrame, _sumState, -1);
                }
            }
        }

        public bool TryComputeRow(BulkWindowRowContext context, DataValueContainer result)
        {
            Feed(_fetchValueFunction(context.Batch, context.RowIndex));
            BulkSumUtils.CopySumValue(_sumState, result);
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
    /// Running sum from the partition start, seeded from the previous row's stored sum.
    /// </summary>
    internal class BulkSumWindowFunctionUnboundedFrom : IBulkWindowFunction
    {
        private readonly Func<EventBatchData, int, IDataValue> _fetchValueFunction;
        private readonly long _to;
        private int _functionIndex;

        private BulkWindowValueRing? _pending;
        private readonly DataValueContainer _sumState = new DataValueContainer();

        public BulkSumWindowFunctionUnboundedFrom(Func<EventBatchData, int, IDataValue> fetchValueFunction, long to)
        {
            Debug.Assert(to <= 0);
            _fetchValueFunction = fetchValueFunction;
            _to = to;
        }

        public long AffectedRowsBefore => 0;

        public long AffectedRowsAfter => long.MaxValue;

        public bool StableByValueEquality => true;

        public long EqualityStableAfterRows => -_to;

        public int AuxiliaryStateColumnCount => 0;

        public Task Initialize(BulkWindowFunctionContext context)
        {
            _functionIndex = context.FunctionIndex;
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
            _sumState._type = ArrowTypeId.Null;

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

            // Seed the sum, pending rows enter it after their frame end.
            BulkSumUtils.CopySumValue(seedReader.GetState(1, _functionIndex), _sumState);
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

            _pending.Push(_fetchValueFunction(context.Batch, context.RowIndex));
            if (_pending.Count > -_to)
            {
                var enteringSum = _pending.PopOldest();
                SumWindowUtils.DoSum(enteringSum, _sumState, 1);
            }
            BulkSumUtils.CopySumValue(_sumState, result);
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
    /// Sum over a bounded frame reaching ahead, e.g. ROWS BETWEEN 2 PRECEDING AND 4 FOLLOWING.
    /// </summary>
    internal class BulkSumWindowFunctionBoundedFollowing : IBulkWindowFunction
    {
        private readonly Func<EventBatchData, int, IDataValue> _fetchValueFunction;
        private readonly long _from;
        private readonly long _to;
        private readonly long _frameSize;

        private BulkWindowForwardPartitionReader? _lookahead;
        private BulkWindowValueRing? _frame;
        private IMemoryAllocator? _memoryAllocator;
        private readonly DataValueContainer _sumState = new DataValueContainer();

        private long _currentPosition;
        private long _nextFeedPosition;
        private long _oldestFramePosition;
        private long _lookaheadPosition;
        private bool _lookaheadStarted;
        private bool _lookaheadDone;

        public BulkSumWindowFunctionBoundedFollowing(Func<EventBatchData, int, IDataValue> fetchValueFunction, long from, long to)
        {
            Debug.Assert(from <= to && to > 0);
            _fetchValueFunction = fetchValueFunction;
            _from = from;
            _to = to;
            _frameSize = to - from + 1;
        }

        public long AffectedRowsBefore => _to;

        public long AffectedRowsAfter => Math.Max(0, -_from);

        public bool StableByValueEquality => false;

        public long EqualityStableAfterRows => long.MaxValue;

        public int AuxiliaryStateColumnCount => 0;

        public Task Initialize(BulkWindowFunctionContext context)
        {
            _memoryAllocator = context.MemoryAllocator;
            _lookahead = new BulkWindowForwardPartitionReader(context.PersistentTree, context.PartitionColumns, context.CreateInsertComparer());
            _frame = new BulkWindowValueRing(_frameSize + 2, context.MemoryAllocator);
            return Task.CompletedTask;
        }

        public ValueTask Commit()
        {
            return ValueTask.CompletedTask;
        }

        public async ValueTask StartScan(ColumnRowReference partitionValues, BulkWindowSeedReader seedReader, bool fromPartitionStart)
        {
            Debug.Assert(_frame != null);

            _frame.Clear();
            _sumState._type = ArrowTypeId.Null;
            _currentPosition = -1;
            _lookaheadStarted = false;
            _lookaheadDone = false;

            long seedRows = 0;
            if (!fromPartitionStart && _from < 0)
            {
                await seedReader.EnsureRows((int)Math.Min(-_from, int.MaxValue));
                seedRows = Math.Min(-_from, seedReader.MaterializedRows);
            }
            _nextFeedPosition = -seedRows;
            _oldestFramePosition = -seedRows;
            for (int back = (int)seedRows; back >= 1; back--)
            {
                var row = seedReader.GetRow(back);
                Feed(_fetchValueFunction(row.referenceBatch, row.RowIndex));
            }
        }

        private void Feed(IDataValue value)
        {
            Debug.Assert(_frame != null);
            if (_frame.Count == 0)
            {
                _oldestFramePosition = _nextFeedPosition;
            }
            SumWindowUtils.DoSum(value, _sumState, 1);
            _frame.Push(value);
            _nextFeedPosition++;
        }

        public bool TryComputeRow(BulkWindowRowContext context, DataValueContainer result)
        {
            // Feeding the lookahead may load pages, so computation is always asynchronous.
            return false;
        }

        public async ValueTask ComputeRow(BulkWindowRowContext context, DataValueContainer result)
        {
            Debug.Assert(_lookahead != null);
            Debug.Assert(_frame != null);
            Debug.Assert(_memoryAllocator != null);

            _currentPosition++;

            if (!_lookaheadStarted)
            {
                _lookaheadStarted = true;
                await _lookahead.ResetAtRow(new ColumnRowReference() { referenceBatch = context.Batch, RowIndex = context.RowIndex }, _memoryAllocator);
                // Lookahead yields the current physical row's first duplicate first.
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
                Feed(_fetchValueFunction(_lookahead.Batch, _lookahead.RowIndex));
            }

            while (_frame.Count > 0 && _oldestFramePosition < _currentPosition + _from)
            {
                var leavingFrame = _frame.PopOldest();
                SumWindowUtils.DoSum(leavingFrame, _sumState, -1);
                _oldestFramePosition++;
            }

            BulkSumUtils.CopySumValue(_sumState, result);
        }

        public ValueTask EndScan()
        {
            return ValueTask.CompletedTask;
        }
    }

    /// <summary>
    /// Sum from the partition start reaching ahead, e.g. ROWS BETWEEN UNBOUNDED PRECEDING AND 2 FOLLOWING.
    /// </summary>
    internal class BulkSumWindowFunctionUnboundedFromFollowing : IBulkWindowFunction
    {
        private readonly Func<EventBatchData, int, IDataValue> _fetchValueFunction;
        private readonly long _to;
        private int _functionIndex;

        private BulkWindowForwardPartitionReader? _lookahead;
        private IMemoryAllocator? _memoryAllocator;
        private readonly DataValueContainer _sumState = new DataValueContainer();

        private long _currentPosition;
        private long _nextFeedPosition;
        private long _lookaheadPosition;
        private bool _lookaheadStarted;
        private bool _lookaheadDone;

        public BulkSumWindowFunctionUnboundedFromFollowing(Func<EventBatchData, int, IDataValue> fetchValueFunction, long to)
        {
            Debug.Assert(to > 0);
            _fetchValueFunction = fetchValueFunction;
            _to = to;
        }

        public long AffectedRowsBefore => _to;

        public long AffectedRowsAfter => long.MaxValue;

        public bool StableByValueEquality => true;

        public long EqualityStableAfterRows => 0;

        public int AuxiliaryStateColumnCount => 0;

        public Task Initialize(BulkWindowFunctionContext context)
        {
            _functionIndex = context.FunctionIndex;
            _memoryAllocator = context.MemoryAllocator;
            _lookahead = new BulkWindowForwardPartitionReader(context.PersistentTree, context.PartitionColumns, context.CreateInsertComparer());
            return Task.CompletedTask;
        }

        public ValueTask Commit()
        {
            return ValueTask.CompletedTask;
        }

        public async ValueTask StartScan(ColumnRowReference partitionValues, BulkWindowSeedReader seedReader, bool fromPartitionStart)
        {
            _sumState._type = ArrowTypeId.Null;
            _currentPosition = -1;
            _lookaheadStarted = false;
            _lookaheadDone = false;
            _nextFeedPosition = 0;

            if (!fromPartitionStart && await seedReader.EnsureRows(1))
            {
                // Seeded sum covers through the previous frame end, to - 1 rows ahead.
                BulkSumUtils.CopySumValue(seedReader.GetState(1, _functionIndex), _sumState);
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
                SumWindowUtils.DoSum(_fetchValueFunction(_lookahead.Batch, _lookahead.RowIndex), _sumState, 1);
                _nextFeedPosition++;
            }

            BulkSumUtils.CopySumValue(_sumState, result);
        }

        public ValueTask EndScan()
        {
            return ValueTask.CompletedTask;
        }
    }

    /// <summary>
    /// Sum to the partition end from at or before the row, e.g. ROWS BETWEEN 2 PRECEDING AND UNBOUNDED FOLLOWING.
    /// Total from a pre-scan minus the prefix before each frame.
    /// </summary>
    internal class BulkSumWindowFunctionSuffix : IBulkWindowFunction
    {
        private readonly Func<EventBatchData, int, IDataValue> _fetchValueFunction;
        private readonly long _from;

        private BulkWindowForwardPartitionReader? _reader;
        private BulkWindowValueRing? _recentValues;
        private readonly DataValueContainer _totalState = new DataValueContainer();
        private readonly DataValueContainer _prefixState = new DataValueContainer();

        private long _currentPosition;
        private long _nextExcludePosition;

        public BulkSumWindowFunctionSuffix(Func<EventBatchData, int, IDataValue> fetchValueFunction, long from)
        {
            Debug.Assert(from <= 0);
            _fetchValueFunction = fetchValueFunction;
            _from = from;
        }

        public long AffectedRowsBefore => long.MaxValue;

        public long AffectedRowsAfter => long.MaxValue;

        public bool StableByValueEquality => true;

        public long EqualityStableAfterRows => -_from;

        public int AuxiliaryStateColumnCount => 0;

        public Task Initialize(BulkWindowFunctionContext context)
        {
            _reader = new BulkWindowForwardPartitionReader(context.PersistentTree, context.PartitionColumns);
            _recentValues = new BulkWindowValueRing(-_from + 2, context.MemoryAllocator);
            return Task.CompletedTask;
        }

        public ValueTask Commit()
        {
            return ValueTask.CompletedTask;
        }

        public async ValueTask StartScan(ColumnRowReference partitionValues, BulkWindowSeedReader seedReader, bool fromPartitionStart)
        {
            Debug.Assert(_reader != null);
            Debug.Assert(_recentValues != null);

            _totalState._type = ArrowTypeId.Null;
            _prefixState._type = ArrowTypeId.Null;
            _recentValues.Clear();
            _currentPosition = -1;
            _nextExcludePosition = 0;

            await _reader.Reset(partitionValues);
            while (await _reader.MoveNextRow())
            {
                var value = _fetchValueFunction(_reader.Batch, _reader.RowIndex);
                SumWindowUtils.DoSum(value, _totalState, _reader.Weight);
            }
        }

        public bool TryComputeRow(BulkWindowRowContext context, DataValueContainer result)
        {
            Debug.Assert(_recentValues != null);

            _currentPosition++;
            _recentValues.Push(_fetchValueFunction(context.Batch, context.RowIndex));
            while (_nextExcludePosition < _currentPosition + _from)
            {
                var excluded = _recentValues.PopOldest();
                SumWindowUtils.DoSum(excluded, _prefixState, 1);
                _nextExcludePosition++;
            }

            BulkSumUtils.CopySumValue(_totalState, result);
            SumWindowUtils.DoSum(_prefixState, result, -1);
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
    /// Sum to the partition end from after the row, e.g. ROWS BETWEEN 2 FOLLOWING AND UNBOUNDED FOLLOWING.
    /// Empty frame at the partition end is null.
    /// </summary>
    internal class BulkSumWindowFunctionSuffixFollowing : IBulkWindowFunction
    {
        private readonly Func<EventBatchData, int, IDataValue> _fetchValueFunction;
        private readonly long _from;

        private BulkWindowForwardPartitionReader? _reader;
        private BulkWindowForwardPartitionReader? _lookahead;
        private IMemoryAllocator? _memoryAllocator;
        private readonly DataValueContainer _totalState = new DataValueContainer();
        private readonly DataValueContainer _prefixState = new DataValueContainer();

        private long _partitionRowCount;
        private long _currentPosition;
        private long _nextPrefixPosition;
        private long _lookaheadPosition;
        private bool _lookaheadStarted;
        private bool _lookaheadDone;

        public BulkSumWindowFunctionSuffixFollowing(Func<EventBatchData, int, IDataValue> fetchValueFunction, long from)
        {
            Debug.Assert(from > 0);
            _fetchValueFunction = fetchValueFunction;
            _from = from;
        }

        public long AffectedRowsBefore => long.MaxValue;

        public long AffectedRowsAfter => 0;

        public bool StableByValueEquality => false;

        public long EqualityStableAfterRows => long.MaxValue;

        public int AuxiliaryStateColumnCount => 0;

        public Task Initialize(BulkWindowFunctionContext context)
        {
            _memoryAllocator = context.MemoryAllocator;
            _reader = new BulkWindowForwardPartitionReader(context.PersistentTree, context.PartitionColumns);
            _lookahead = new BulkWindowForwardPartitionReader(context.PersistentTree, context.PartitionColumns, context.CreateInsertComparer());
            return Task.CompletedTask;
        }

        public ValueTask Commit()
        {
            return ValueTask.CompletedTask;
        }

        public async ValueTask StartScan(ColumnRowReference partitionValues, BulkWindowSeedReader seedReader, bool fromPartitionStart)
        {
            Debug.Assert(_reader != null);

            _totalState._type = ArrowTypeId.Null;
            _prefixState._type = ArrowTypeId.Null;
            _partitionRowCount = 0;
            _currentPosition = -1;
            _nextPrefixPosition = 0;
            _lookaheadStarted = false;
            _lookaheadDone = false;

            await _reader.Reset(partitionValues);
            while (await _reader.MoveNextRow())
            {
                var value = _fetchValueFunction(_reader.Batch, _reader.RowIndex);
                SumWindowUtils.DoSum(value, _totalState, _reader.Weight);
                _partitionRowCount += _reader.Weight;
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

            _currentPosition++;

            if (_currentPosition + _from > _partitionRowCount - 1)
            {
                // The frame starts past the partition end and is empty.
                result._type = ArrowTypeId.Null;
                return;
            }

            if (!_lookaheadStarted)
            {
                // Lookahead covers the prefix rows before each frame.
                _lookaheadStarted = true;
                await _lookahead.ResetAtRow(new ColumnRowReference() { referenceBatch = context.Batch, RowIndex = context.RowIndex }, _memoryAllocator);
                _lookaheadPosition = _currentPosition - context.DupIndex - 1;
            }

            while (!_lookaheadDone && _nextPrefixPosition <= _currentPosition + _from - 1)
            {
                if (!await _lookahead.MoveNextLogical())
                {
                    _lookaheadDone = true;
                    break;
                }
                _lookaheadPosition++;
                if (_lookaheadPosition < _nextPrefixPosition)
                {
                    continue;
                }
                SumWindowUtils.DoSum(_fetchValueFunction(_lookahead.Batch, _lookahead.RowIndex), _prefixState, 1);
                _nextPrefixPosition++;
            }

            BulkSumUtils.CopySumValue(_totalState, result);
            SumWindowUtils.DoSum(_prefixState, result, -1);
        }

        public ValueTask EndScan()
        {
            return ValueTask.CompletedTask;
        }
    }

    /// <summary>
    /// Sum over the whole partition, total from a pre-scan.
    /// </summary>
    internal class BulkSumWindowFunctionUnbounded : IBulkWindowFunction
    {
        private readonly Func<EventBatchData, int, IDataValue> _fetchValueFunction;
        private BulkWindowForwardPartitionReader? _reader;
        private readonly DataValueContainer _sumState = new DataValueContainer();

        public BulkSumWindowFunctionUnbounded(Func<EventBatchData, int, IDataValue> fetchValueFunction)
        {
            _fetchValueFunction = fetchValueFunction;
        }

        public long AffectedRowsBefore => long.MaxValue;

        public long AffectedRowsAfter => long.MaxValue;

        public bool StableByValueEquality => true;

        public long EqualityStableAfterRows => 0;

        public int AuxiliaryStateColumnCount => 0;

        public Task Initialize(BulkWindowFunctionContext context)
        {
            _reader = new BulkWindowForwardPartitionReader(context.PersistentTree, context.PartitionColumns);
            return Task.CompletedTask;
        }

        public ValueTask Commit()
        {
            return ValueTask.CompletedTask;
        }

        public async ValueTask StartScan(ColumnRowReference partitionValues, BulkWindowSeedReader seedReader, bool fromPartitionStart)
        {
            Debug.Assert(_reader != null);

            _sumState._type = ArrowTypeId.Null;
            await _reader.Reset(partitionValues);
            while (await _reader.MoveNextRow())
            {
                var value = _fetchValueFunction(_reader.Batch, _reader.RowIndex);
                SumWindowUtils.DoSum(value, _sumState, _reader.Weight);
            }
        }

        public bool TryComputeRow(BulkWindowRowContext context, DataValueContainer result)
        {
            BulkSumUtils.CopySumValue(_sumState, result);
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
