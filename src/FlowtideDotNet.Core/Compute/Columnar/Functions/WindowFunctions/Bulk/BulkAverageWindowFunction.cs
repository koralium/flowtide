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
using FlowtideDotNet.Core.Operators.Window.Bulk;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Substrait.Expressions;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;

namespace FlowtideDotNet.Core.Compute.Columnar.Functions.WindowFunctions.Bulk
{
    internal class BulkAverageWindowFunctionDefinition : BulkWindowFunctionDefinition
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

            switch (bounds.Kind)
            {
                case BulkWindowFrameKind.BoundedRows:
                    if (bounds.To == long.MaxValue)
                    {
                        bulkWindowFunction = bounds.From > 0
                            ? new BulkAverageWindowFunctionSuffixFollowing(compiledValue, bounds.From)
                            : new BulkAverageWindowFunctionSuffix(compiledValue, bounds.From);
                        return true;
                    }
                    if (bounds.From > bounds.To)
                    {
                        bulkWindowFunction = new BulkEmptyFrameWindowFunction();
                        return true;
                    }
                    if (bounds.To > 0)
                    {
                        bulkWindowFunction = new BulkAverageWindowFunctionBoundedFollowing(compiledValue, bounds.From, bounds.To);
                        return true;
                    }
                    bulkWindowFunction = new BulkAverageWindowFunctionBounded(compiledValue, bounds.From, bounds.To);
                    return true;
                case BulkWindowFrameKind.UnboundedPreceding:
                    if (bounds.To > 0)
                    {
                        bulkWindowFunction = new BulkAverageWindowFunctionUnboundedFromFollowing(compiledValue, bounds.To);
                        return true;
                    }
                    bulkWindowFunction = new BulkAverageWindowFunctionUnboundedFrom(compiledValue, bounds.To);
                    return true;
                default:
                    bulkWindowFunction = new BulkAverageWindowFunctionUnbounded(compiledValue);
                    return true;
            }
        }
    }

    internal static class BulkAverageUtils
    {
        /// <summary>
        /// Writes sum / count, same typing as <see cref="AverageWindowUtils.DivideWithCount{T}"/>.
        /// </summary>
        public static void DivideToContainer(DataValueContainer sum, long count, DataValueContainer result)
        {
            if (count == 0)
            {
                result._type = ArrowTypeId.Null;
                return;
            }
            switch (sum.Type)
            {
                case ArrowTypeId.Int64:
                    result._type = ArrowTypeId.Double;
                    result._doubleValue = new DoubleValue((double)sum.AsLong / count);
                    break;
                case ArrowTypeId.Double:
                    result._type = ArrowTypeId.Double;
                    result._doubleValue = new DoubleValue(sum.AsDouble / count);
                    break;
                case ArrowTypeId.Decimal128:
                    result._type = ArrowTypeId.Decimal128;
                    result._decimalValue = new DecimalValue(sum.AsDecimal / count);
                    break;
                default:
                    result._type = ArrowTypeId.Null;
                    break;
            }
        }
    }

    /// <summary>
    /// Average mirror of <see cref="BulkSumWindowFunctionBounded"/>, adds a count.
    /// </summary>
    internal class BulkAverageWindowFunctionBounded : IBulkWindowFunction
    {
        private readonly Func<EventBatchData, int, IDataValue> _fetchValueFunction;
        private readonly long _from;
        private readonly long _to;
        private readonly long _frameSize;

        private BulkWindowValueRing? _pending;
        private BulkWindowValueRing? _frame;
        private readonly DataValueContainer _sumState = new DataValueContainer();
        private long _count;

        public BulkAverageWindowFunctionBounded(Func<EventBatchData, int, IDataValue> fetchValueFunction, long from, long to)
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
            _count = 0;

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
                AverageWindowUtils.ModifyCount(ref _count, 1, enteringFrame.Type);
                _frame.Push(enteringFrame);
                while (_frame.Count > _frameSize)
                {
                    var leavingFrame = _frame.PopOldest();
                    SumWindowUtils.DoSum(leavingFrame, _sumState, -1);
                    AverageWindowUtils.ModifyCount(ref _count, -1, leavingFrame.Type);
                }
            }
        }

        public bool TryComputeRow(BulkWindowRowContext context, DataValueContainer result)
        {
            Feed(_fetchValueFunction(context.Batch, context.RowIndex));
            BulkAverageUtils.DivideToContainer(_sumState, _count, result);
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
    /// Average mirror of <see cref="BulkSumWindowFunctionBoundedFollowing"/>.
    /// </summary>
    internal class BulkAverageWindowFunctionBoundedFollowing : IBulkWindowFunction
    {
        private readonly Func<EventBatchData, int, IDataValue> _fetchValueFunction;
        private readonly long _from;
        private readonly long _to;
        private readonly long _frameSize;

        private BulkWindowForwardPartitionReader? _lookahead;
        private BulkWindowValueRing? _frame;
        private IMemoryAllocator? _memoryAllocator;
        private readonly DataValueContainer _sumState = new DataValueContainer();
        private long _count;

        private long _currentPosition;
        private long _nextFeedPosition;
        private long _oldestFramePosition;
        private long _lookaheadPosition;
        private bool _lookaheadStarted;
        private bool _lookaheadDone;

        public BulkAverageWindowFunctionBoundedFollowing(Func<EventBatchData, int, IDataValue> fetchValueFunction, long from, long to)
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
            // The first row loads positions 0..to before eviction, so size by to and the seeded rows.
            _frame = new BulkWindowValueRing(_to + Math.Max(0, -_from) + 2, context.MemoryAllocator);
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
            _count = 0;
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
            AverageWindowUtils.ModifyCount(ref _count, 1, value.Type);
            _frame.Push(value);
            _nextFeedPosition++;
        }

        public bool TryComputeRow(BulkWindowRowContext context, DataValueContainer result)
        {
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
                AverageWindowUtils.ModifyCount(ref _count, -1, leavingFrame.Type);
                _oldestFramePosition++;
            }

            BulkAverageUtils.DivideToContainer(_sumState, _count, result);
        }

        public ValueTask EndScan()
        {
            return ValueTask.CompletedTask;
        }
    }

    /// <summary>
    /// Running average, seeded from the previous row's stored aux sum and count.
    /// </summary>
    internal class BulkAverageWindowFunctionUnboundedFrom : IBulkWindowFunction
    {
        private readonly Func<EventBatchData, int, IDataValue> _fetchValueFunction;
        private readonly long _to;
        private int _auxStartIndex;

        private BulkWindowValueRing? _pending;
        private readonly DataValueContainer _sumState = new DataValueContainer();
        private long _count;
        private readonly DataValueContainer _auxCountContainer = new DataValueContainer();

        public BulkAverageWindowFunctionUnboundedFrom(Func<EventBatchData, int, IDataValue> fetchValueFunction, long to)
        {
            Debug.Assert(to <= 0);
            _fetchValueFunction = fetchValueFunction;
            _to = to;
        }

        public long AffectedRowsBefore => 0;

        public long AffectedRowsAfter => long.MaxValue;

        public bool StableByValueEquality => true;

        public long EqualityStableAfterRows => -_to;

        public int AuxiliaryStateColumnCount => 2;

        public Task Initialize(BulkWindowFunctionContext context)
        {
            _auxStartIndex = context.AuxiliaryColumnStartIndex;
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
            _count = 0;

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

            BulkSumUtils.CopySumValue(seedReader.GetState(1, _auxStartIndex), _sumState);
            var countState = seedReader.GetState(1, _auxStartIndex + 1);
            _count = countState.IsNull ? 0 : countState.AsLong;

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
                AverageWindowUtils.ModifyCount(ref _count, 1, enteringSum.Type);
            }

            _auxCountContainer._type = ArrowTypeId.Int64;
            _auxCountContainer._int64Value = new Int64Value(_count);
            context.SetAuxValue(_auxStartIndex, _sumState);
            context.SetAuxValue(_auxStartIndex + 1, _auxCountContainer);

            BulkAverageUtils.DivideToContainer(_sumState, _count, result);
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
    /// Average from the partition start reaching ahead, aux mirror of the sum variant.
    /// </summary>
    internal class BulkAverageWindowFunctionUnboundedFromFollowing : IBulkWindowFunction
    {
        private readonly Func<EventBatchData, int, IDataValue> _fetchValueFunction;
        private readonly long _to;
        private int _auxStartIndex;

        private BulkWindowForwardPartitionReader? _lookahead;
        private IMemoryAllocator? _memoryAllocator;
        private readonly DataValueContainer _sumState = new DataValueContainer();
        private long _count;
        private readonly DataValueContainer _auxCountContainer = new DataValueContainer();

        private long _currentPosition;
        private long _nextFeedPosition;
        private long _lookaheadPosition;
        private bool _lookaheadStarted;
        private bool _lookaheadDone;

        public BulkAverageWindowFunctionUnboundedFromFollowing(Func<EventBatchData, int, IDataValue> fetchValueFunction, long to)
        {
            Debug.Assert(to > 0);
            _fetchValueFunction = fetchValueFunction;
            _to = to;
        }

        public long AffectedRowsBefore => _to;

        public long AffectedRowsAfter => long.MaxValue;

        public bool StableByValueEquality => true;

        public long EqualityStableAfterRows => 0;

        public int AuxiliaryStateColumnCount => 2;

        public Task Initialize(BulkWindowFunctionContext context)
        {
            _auxStartIndex = context.AuxiliaryColumnStartIndex;
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
            _count = 0;
            _currentPosition = -1;
            _lookaheadStarted = false;
            _lookaheadDone = false;
            _nextFeedPosition = 0;

            if (!fromPartitionStart && await seedReader.EnsureRows(1))
            {
                BulkSumUtils.CopySumValue(seedReader.GetState(1, _auxStartIndex), _sumState);
                var countState = seedReader.GetState(1, _auxStartIndex + 1);
                _count = countState.IsNull ? 0 : countState.AsLong;
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
                var value = _fetchValueFunction(_lookahead.Batch, _lookahead.RowIndex);
                SumWindowUtils.DoSum(value, _sumState, 1);
                AverageWindowUtils.ModifyCount(ref _count, 1, value.Type);
                _nextFeedPosition++;
            }

            _auxCountContainer._type = ArrowTypeId.Int64;
            _auxCountContainer._int64Value = new Int64Value(_count);
            context.SetAuxValue(_auxStartIndex, _sumState);
            context.SetAuxValue(_auxStartIndex + 1, _auxCountContainer);

            BulkAverageUtils.DivideToContainer(_sumState, _count, result);
        }

        public ValueTask EndScan()
        {
            return ValueTask.CompletedTask;
        }
    }

    /// <summary>
    /// Average mirror of <see cref="BulkSumWindowFunctionSuffix"/>.
    /// </summary>
    internal class BulkAverageWindowFunctionSuffix : IBulkWindowFunction
    {
        private readonly Func<EventBatchData, int, IDataValue> _fetchValueFunction;
        private readonly long _from;

        private BulkWindowForwardPartitionReader? _reader;
        private BulkWindowValueRing? _recentValues;
        private readonly DataValueContainer _totalState = new DataValueContainer();
        private readonly DataValueContainer _prefixState = new DataValueContainer();
        private readonly DataValueContainer _resultSumState = new DataValueContainer();
        private long _totalCount;
        private long _prefixCount;

        private long _currentPosition;
        private long _nextExcludePosition;

        public BulkAverageWindowFunctionSuffix(Func<EventBatchData, int, IDataValue> fetchValueFunction, long from)
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
            _totalCount = 0;
            _prefixCount = 0;
            _recentValues.Clear();
            _currentPosition = -1;
            _nextExcludePosition = 0;

            await _reader.Reset(partitionValues);
            while (await _reader.MoveNextRow())
            {
                var value = _fetchValueFunction(_reader.Batch, _reader.RowIndex);
                SumWindowUtils.DoSum(value, _totalState, _reader.Weight);
                if (value.Type == ArrowTypeId.Int64 || value.Type == ArrowTypeId.Double || value.Type == ArrowTypeId.Decimal128)
                {
                    _totalCount += _reader.Weight;
                }
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
                AverageWindowUtils.ModifyCount(ref _prefixCount, 1, excluded.Type);
                _nextExcludePosition++;
            }

            BulkSumUtils.CopySumValue(_totalState, _resultSumState);
            SumWindowUtils.DoSum(_prefixState, _resultSumState, -1);
            BulkAverageUtils.DivideToContainer(_resultSumState, _totalCount - _prefixCount, result);
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
    /// Average mirror of <see cref="BulkSumWindowFunctionSuffixFollowing"/>.
    /// </summary>
    internal class BulkAverageWindowFunctionSuffixFollowing : IBulkWindowFunction
    {
        private readonly Func<EventBatchData, int, IDataValue> _fetchValueFunction;
        private readonly long _from;

        private BulkWindowForwardPartitionReader? _reader;
        private BulkWindowForwardPartitionReader? _lookahead;
        private IMemoryAllocator? _memoryAllocator;
        private readonly DataValueContainer _totalState = new DataValueContainer();
        private readonly DataValueContainer _prefixState = new DataValueContainer();
        private readonly DataValueContainer _resultSumState = new DataValueContainer();
        private long _totalCount;
        private long _prefixCount;

        private long _currentPosition;
        private long _nextPrefixPosition;
        private long _lookaheadPosition;
        private bool _lookaheadStarted;
        private bool _lookaheadDone;

        public BulkAverageWindowFunctionSuffixFollowing(Func<EventBatchData, int, IDataValue> fetchValueFunction, long from)
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
            _totalCount = 0;
            _prefixCount = 0;
            _currentPosition = -1;
            _nextPrefixPosition = 0;
            _lookaheadStarted = false;
            _lookaheadDone = false;

            await _reader.Reset(partitionValues);
            while (await _reader.MoveNextRow())
            {
                var value = _fetchValueFunction(_reader.Batch, _reader.RowIndex);
                SumWindowUtils.DoSum(value, _totalState, _reader.Weight);
                if (value.Type == ArrowTypeId.Int64 || value.Type == ArrowTypeId.Double || value.Type == ArrowTypeId.Decimal128)
                {
                    _totalCount += _reader.Weight;
                }
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
                var value = _fetchValueFunction(_lookahead.Batch, _lookahead.RowIndex);
                SumWindowUtils.DoSum(value, _prefixState, 1);
                AverageWindowUtils.ModifyCount(ref _prefixCount, 1, value.Type);
                _nextPrefixPosition++;
            }

            BulkSumUtils.CopySumValue(_totalState, _resultSumState);
            SumWindowUtils.DoSum(_prefixState, _resultSumState, -1);
            BulkAverageUtils.DivideToContainer(_resultSumState, _totalCount - _prefixCount, result);
        }

        public ValueTask EndScan()
        {
            return ValueTask.CompletedTask;
        }
    }

    /// <summary>
    /// Average over the whole partition, computed with a single pre-scan.
    /// </summary>
    internal class BulkAverageWindowFunctionUnbounded : IBulkWindowFunction
    {
        private readonly Func<EventBatchData, int, IDataValue> _fetchValueFunction;
        private BulkWindowForwardPartitionReader? _reader;
        private readonly DataValueContainer _sumState = new DataValueContainer();
        private long _count;

        public BulkAverageWindowFunctionUnbounded(Func<EventBatchData, int, IDataValue> fetchValueFunction)
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
            _count = 0;
            await _reader.Reset(partitionValues);
            while (await _reader.MoveNextRow())
            {
                var value = _fetchValueFunction(_reader.Batch, _reader.RowIndex);
                SumWindowUtils.DoSum(value, _sumState, _reader.Weight);
                if (value.Type == ArrowTypeId.Int64 || value.Type == ArrowTypeId.Double || value.Type == ArrowTypeId.Decimal128)
                {
                    _count += _reader.Weight;
                }
            }
        }

        public bool TryComputeRow(BulkWindowRowContext context, DataValueContainer result)
        {
            BulkAverageUtils.DivideToContainer(_sumState, _count, result);
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
