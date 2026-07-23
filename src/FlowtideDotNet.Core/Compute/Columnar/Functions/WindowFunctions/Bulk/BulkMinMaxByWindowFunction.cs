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
using FlowtideDotNet.Core.ColumnStore.Comparers;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Core.Compute.Columnar.Functions.WindowFunctions.MinMax;
using FlowtideDotNet.Core.Operators.Window.Bulk;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Substrait.Expressions;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;

namespace FlowtideDotNet.Core.Compute.Columnar.Functions.WindowFunctions.Bulk
{
    internal class BulkMinByWindowFunctionDefinition : BulkWindowFunctionDefinition
    {
        public override bool TryCreate(WindowFunction windowFunction, IFunctionsRegister functionsRegister, [NotNullWhen(true)] out IBulkWindowFunction? bulkWindowFunction)
        {
            return BulkMinMaxByUtils.TryCreate(windowFunction, functionsRegister, isMin: true, out bulkWindowFunction);
        }
    }

    internal class BulkMaxByWindowFunctionDefinition : BulkWindowFunctionDefinition
    {
        public override bool TryCreate(WindowFunction windowFunction, IFunctionsRegister functionsRegister, [NotNullWhen(true)] out IBulkWindowFunction? bulkWindowFunction)
        {
            return BulkMinMaxByUtils.TryCreate(windowFunction, functionsRegister, isMin: false, out bulkWindowFunction);
        }
    }

    internal static class BulkMinMaxByUtils
    {
        public static bool TryCreate(WindowFunction windowFunction, IFunctionsRegister functionsRegister, bool isMin, [NotNullWhen(true)] out IBulkWindowFunction? bulkWindowFunction)
        {
            bulkWindowFunction = null;
            if (windowFunction.Arguments.Count < 2)
            {
                return false;
            }
            var compiledValue = ColumnProjectCompiler.CompileToValue(windowFunction.Arguments[0], functionsRegister);
            var compiledCompareValue = ColumnProjectCompiler.CompileToValue(windowFunction.Arguments[1], functionsRegister);

            MinMaxBoundUtils.GetBoundInfo(windowFunction, out var isRowBounded, out var lowerBoundRowOffset, out var upperBoundRowOffset);

            if (isRowBounded && lowerBoundRowOffset != long.MinValue)
            {
                if (upperBoundRowOffset == long.MaxValue)
                {
                    bulkWindowFunction = new BulkMinMaxByWindowFunctionSuffix(compiledValue, compiledCompareValue, lowerBoundRowOffset, isMin);
                    return true;
                }
                if (lowerBoundRowOffset > upperBoundRowOffset)
                {
                    bulkWindowFunction = new BulkEmptyFrameWindowFunction();
                    return true;
                }
                if (upperBoundRowOffset == 0 && lowerBoundRowOffset <= 0)
                {
                    bulkWindowFunction = new BulkMinMaxByWindowFunctionBounded(compiledValue, compiledCompareValue, lowerBoundRowOffset, isMin);
                    return true;
                }
                if (upperBoundRowOffset < 0)
                {
                    bulkWindowFunction = new BulkMinMaxByWindowFunctionFrameDelay(compiledValue, compiledCompareValue, lowerBoundRowOffset, upperBoundRowOffset, isMin);
                    return true;
                }
                bulkWindowFunction = new BulkMinMaxByWindowFunctionFrameLookahead(compiledValue, compiledCompareValue, lowerBoundRowOffset, upperBoundRowOffset, isMin);
                return true;
            }
            if (isRowBounded && lowerBoundRowOffset == long.MinValue && upperBoundRowOffset == long.MaxValue)
            {
                bulkWindowFunction = new BulkMinMaxByWindowFunctionUnbounded(compiledValue, compiledCompareValue, isMin);
                return true;
            }
            if (isRowBounded && lowerBoundRowOffset == long.MinValue)
            {
                if (upperBoundRowOffset == 0)
                {
                    bulkWindowFunction = new BulkMinMaxByWindowFunctionUnboundedFrom(compiledValue, compiledCompareValue, isMin);
                    return true;
                }
                if (upperBoundRowOffset < 0)
                {
                    bulkWindowFunction = new BulkMinMaxByWindowFunctionUnboundedFromDelay(compiledValue, compiledCompareValue, upperBoundRowOffset, isMin);
                    return true;
                }
                bulkWindowFunction = new BulkMinMaxByWindowFunctionUnboundedFromLookahead(compiledValue, compiledCompareValue, upperBoundRowOffset, isMin);
                return true;
            }
            // No frame given, the non bulk operator does not support this either.
            return false;
        }
    }

    /// <summary>
    /// min_by/max_by over a trailing frame, e.g. ROWS BETWEEN 364 PRECEDING AND CURRENT ROW.
    /// Stores the best's offset back and compare value, rescans backwards when it drops out.
    /// </summary>
    internal class BulkMinMaxByWindowFunctionBounded : IBulkWindowFunction
    {
        private readonly Func<EventBatchData, int, IDataValue> _fetchValueFunction;
        private readonly Func<EventBatchData, int, IDataValue> _fetchCompareValueFunction;
        private readonly long _from;
        private readonly bool _isMin;

        private int _functionIndex;
        private int _auxStartIndex;
        private BulkWindowBackwardPartitionReader? _rescanReader;

        private bool _hasBest;
        private long _bestOffsetBack;
        private Column? _bestCompareColumn;
        private Column? _bestValueColumn;
        private readonly DataValueContainer _auxOffsetContainer = new DataValueContainer();

        public BulkMinMaxByWindowFunctionBounded(
            Func<EventBatchData, int, IDataValue> fetchValueFunction,
            Func<EventBatchData, int, IDataValue> fetchCompareValueFunction,
            long from,
            bool isMin)
        {
            Debug.Assert(from <= 0);
            _fetchValueFunction = fetchValueFunction;
            _fetchCompareValueFunction = fetchCompareValueFunction;
            _from = from;
            _isMin = isMin;
        }

        public long AffectedRowsBefore => 0;

        public long AffectedRowsAfter => -_from;

        public bool StableByValueEquality => false;

        public long EqualityStableAfterRows => long.MaxValue;

        public int AuxiliaryStateColumnCount => 2;

        public Task Initialize(BulkWindowFunctionContext context)
        {
            _functionIndex = context.FunctionIndex;
            _auxStartIndex = context.AuxiliaryColumnStartIndex;
            _rescanReader = new BulkWindowBackwardPartitionReader(
                context.PersistentTree,
                context.CreateInsertComparer(),
                context.PartitionColumns);
            _bestCompareColumn = Column.Create(context.MemoryAllocator);
            _bestValueColumn = Column.Create(context.MemoryAllocator);
            _bestCompareColumn.Add(NullValue.Instance);
            _bestValueColumn.Add(NullValue.Instance);
            return Task.CompletedTask;
        }

        public ValueTask Commit()
        {
            return ValueTask.CompletedTask;
        }

        public async ValueTask StartScan(ColumnRowReference partitionValues, BulkWindowSeedReader seedReader, bool fromPartitionStart)
        {
            _hasBest = false;
            if (!fromPartitionStart && await seedReader.EnsureRows(1))
            {
                var offsetState = seedReader.GetState(1, _auxStartIndex);
                if (!offsetState.IsNull)
                {
                    _hasBest = true;
                    _bestOffsetBack = offsetState.AsLong;
                    SetBest(seedReader.GetState(1, _auxStartIndex + 1), seedReader.GetState(1, _functionIndex));
                }
            }
        }

        private void SetBest(IDataValue compareValue, IDataValue value)
        {
            Debug.Assert(_bestCompareColumn != null);
            Debug.Assert(_bestValueColumn != null);
            _bestCompareColumn.UpdateAt(0, compareValue);
            _bestValueColumn.UpdateAt(0, value);
        }

        private bool IsBetterStrict(IDataValue candidate)
        {
            Debug.Assert(_bestCompareColumn != null);
            var currentBest = _bestCompareColumn.GetValueAt(0, default);
            var compareResult = DataValueComparer.Instance.Compare(candidate, currentBest);
            return _isMin ? compareResult < 0 : compareResult > 0;
        }

        private bool IsBetterOrEqual(IDataValue candidate)
        {
            Debug.Assert(_bestCompareColumn != null);
            var currentBest = _bestCompareColumn.GetValueAt(0, default);
            var compareResult = DataValueComparer.Instance.Compare(candidate, currentBest);
            return _isMin ? compareResult <= 0 : compareResult >= 0;
        }

        public bool TryComputeRow(BulkWindowRowContext context, DataValueContainer result)
        {
            if (_hasBest && _bestOffsetBack + 1 > -_from)
            {
                // Best drops out here, the backwards rescan may load pages.
                return false;
            }
            FastPathStep(context);
            WriteResult(context, result);
            return true;
        }

        public async ValueTask ComputeRow(BulkWindowRowContext context, DataValueContainer result)
        {
            if (_hasBest && _bestOffsetBack + 1 > -_from)
            {
                // Best dropped out, rescan the frame backwards.
                await Rescan(context);
            }
            else
            {
                FastPathStep(context);
            }
            WriteResult(context, result);
        }

        private void FastPathStep(BulkWindowRowContext context)
        {
            if (_hasBest)
            {
                _bestOffsetBack++;
            }
            var compareValue = _fetchCompareValueFunction(context.Batch, context.RowIndex);
            if (!compareValue.IsNull && (!_hasBest || IsBetterStrict(compareValue)))
            {
                _hasBest = true;
                _bestOffsetBack = 0;
                SetBest(compareValue, _fetchValueFunction(context.Batch, context.RowIndex));
            }
        }

        private void WriteResult(BulkWindowRowContext context, DataValueContainer result)
        {
            Debug.Assert(_bestValueColumn != null);
            if (_hasBest)
            {
                _bestValueColumn.GetValueAt(0, result, default);
                _auxOffsetContainer._type = ArrowTypeId.Int64;
                _auxOffsetContainer._int64Value = new Int64Value(_bestOffsetBack);
                context.SetAuxValue(_auxStartIndex, _auxOffsetContainer);
                context.SetAuxValue(_auxStartIndex + 1, _bestCompareColumn!.GetValueAt(0, default));
            }
            else
            {
                result._type = ArrowTypeId.Null;
                context.SetAuxValue(_auxStartIndex, NullValue.Instance);
                context.SetAuxValue(_auxStartIndex + 1, NullValue.Instance);
            }
        }

        private async ValueTask Rescan(BulkWindowRowContext context)
        {
            Debug.Assert(_rescanReader != null);

            _hasBest = false;
            var reach = -_from;

            // Ties prefer the oldest entry.
            var compareValue = _fetchCompareValueFunction(context.Batch, context.RowIndex);
            if (!compareValue.IsNull)
            {
                _hasBest = true;
                _bestOffsetBack = Math.Min(context.DupIndex, reach);
                SetBest(compareValue, _fetchValueFunction(context.Batch, context.RowIndex));
            }

            long offset = context.DupIndex;
            if (offset >= reach)
            {
                return;
            }

            var anchor = new ColumnRowReference()
            {
                referenceBatch = context.Batch,
                RowIndex = context.RowIndex
            };
            await _rescanReader.Reset(anchor);
            while (offset < reach && await _rescanReader.MoveNextRow())
            {
                var rowCompare = _fetchCompareValueFunction(_rescanReader.Batch, _rescanReader.RowIndex);
                var rowWeight = _rescanReader.Weight;
                if (!rowCompare.IsNull && (!_hasBest || IsBetterOrEqual(rowCompare)))
                {
                    _hasBest = true;
                    _bestOffsetBack = Math.Min(offset + rowWeight, reach);
                    SetBest(rowCompare, _fetchValueFunction(_rescanReader.Batch, _rescanReader.RowIndex));
                }
                offset += rowWeight;
            }
        }

        public ValueTask EndScan()
        {
            return ValueTask.CompletedTask;
        }
    }

    /// <summary>
    /// min_by/max_by from the partition start to the current row, seeded from the previous best.
    /// </summary>
    internal class BulkMinMaxByWindowFunctionUnboundedFrom : IBulkWindowFunction
    {
        private readonly Func<EventBatchData, int, IDataValue> _fetchValueFunction;
        private readonly Func<EventBatchData, int, IDataValue> _fetchCompareValueFunction;
        private readonly bool _isMin;

        private int _functionIndex;
        private int _auxStartIndex;

        private bool _hasBest;
        private Column? _bestCompareColumn;
        private Column? _bestValueColumn;

        public BulkMinMaxByWindowFunctionUnboundedFrom(
            Func<EventBatchData, int, IDataValue> fetchValueFunction,
            Func<EventBatchData, int, IDataValue> fetchCompareValueFunction,
            bool isMin)
        {
            _fetchValueFunction = fetchValueFunction;
            _fetchCompareValueFunction = fetchCompareValueFunction;
            _isMin = isMin;
        }

        public long AffectedRowsBefore => 0;

        public long AffectedRowsAfter => long.MaxValue;

        public bool StableByValueEquality => true;

        public long EqualityStableAfterRows => 0;

        public int AuxiliaryStateColumnCount => 1;

        public Task Initialize(BulkWindowFunctionContext context)
        {
            _functionIndex = context.FunctionIndex;
            _auxStartIndex = context.AuxiliaryColumnStartIndex;
            _bestCompareColumn = Column.Create(context.MemoryAllocator);
            _bestValueColumn = Column.Create(context.MemoryAllocator);
            _bestCompareColumn.Add(NullValue.Instance);
            _bestValueColumn.Add(NullValue.Instance);
            return Task.CompletedTask;
        }

        public ValueTask Commit()
        {
            return ValueTask.CompletedTask;
        }

        public async ValueTask StartScan(ColumnRowReference partitionValues, BulkWindowSeedReader seedReader, bool fromPartitionStart)
        {
            Debug.Assert(_bestCompareColumn != null);
            Debug.Assert(_bestValueColumn != null);

            _hasBest = false;
            if (!fromPartitionStart && await seedReader.EnsureRows(1))
            {
                var compareState = seedReader.GetState(1, _auxStartIndex);
                if (!compareState.IsNull)
                {
                    _hasBest = true;
                    _bestCompareColumn.UpdateAt(0, compareState);
                    _bestValueColumn.UpdateAt(0, seedReader.GetState(1, _functionIndex));
                }
            }
        }

        public bool TryComputeRow(BulkWindowRowContext context, DataValueContainer result)
        {
            Debug.Assert(_bestCompareColumn != null);
            Debug.Assert(_bestValueColumn != null);

            var compareValue = _fetchCompareValueFunction(context.Batch, context.RowIndex);
            if (!compareValue.IsNull)
            {
                bool better = !_hasBest;
                if (!better)
                {
                    var compareResult = DataValueComparer.Instance.Compare(compareValue, _bestCompareColumn.GetValueAt(0, default));
                    better = _isMin ? compareResult < 0 : compareResult > 0;
                }
                if (better)
                {
                    _hasBest = true;
                    _bestCompareColumn.UpdateAt(0, compareValue);
                    _bestValueColumn.UpdateAt(0, _fetchValueFunction(context.Batch, context.RowIndex));
                }
            }

            if (_hasBest)
            {
                _bestValueColumn.GetValueAt(0, result, default);
                context.SetAuxValue(_auxStartIndex, _bestCompareColumn.GetValueAt(0, default));
            }
            else
            {
                result._type = ArrowTypeId.Null;
                context.SetAuxValue(_auxStartIndex, NullValue.Instance);
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
    /// min_by/max_by over the whole partition, best from a pre-scan.
    /// </summary>
    internal class BulkMinMaxByWindowFunctionUnbounded : IBulkWindowFunction
    {
        private readonly Func<EventBatchData, int, IDataValue> _fetchValueFunction;
        private readonly Func<EventBatchData, int, IDataValue> _fetchCompareValueFunction;
        private readonly bool _isMin;

        private BulkWindowForwardPartitionReader? _reader;
        private bool _hasBest;
        private Column? _bestCompareColumn;
        private Column? _bestValueColumn;

        public BulkMinMaxByWindowFunctionUnbounded(
            Func<EventBatchData, int, IDataValue> fetchValueFunction,
            Func<EventBatchData, int, IDataValue> fetchCompareValueFunction,
            bool isMin)
        {
            _fetchValueFunction = fetchValueFunction;
            _fetchCompareValueFunction = fetchCompareValueFunction;
            _isMin = isMin;
        }

        public long AffectedRowsBefore => long.MaxValue;

        public long AffectedRowsAfter => long.MaxValue;

        public bool StableByValueEquality => true;

        public long EqualityStableAfterRows => 0;

        public int AuxiliaryStateColumnCount => 0;

        public Task Initialize(BulkWindowFunctionContext context)
        {
            _reader = new BulkWindowForwardPartitionReader(context.PersistentTree, context.PartitionColumns);
            _bestCompareColumn = Column.Create(context.MemoryAllocator);
            _bestValueColumn = Column.Create(context.MemoryAllocator);
            _bestCompareColumn.Add(NullValue.Instance);
            _bestValueColumn.Add(NullValue.Instance);
            return Task.CompletedTask;
        }

        public ValueTask Commit()
        {
            return ValueTask.CompletedTask;
        }

        public async ValueTask StartScan(ColumnRowReference partitionValues, BulkWindowSeedReader seedReader, bool fromPartitionStart)
        {
            Debug.Assert(_reader != null);
            Debug.Assert(_bestCompareColumn != null);
            Debug.Assert(_bestValueColumn != null);

            _hasBest = false;
            await _reader.Reset(partitionValues);
            while (await _reader.MoveNextRow())
            {
                var compareValue = _fetchCompareValueFunction(_reader.Batch, _reader.RowIndex);
                if (compareValue.IsNull)
                {
                    continue;
                }
                bool better = !_hasBest;
                if (!better)
                {
                    var compareResult = DataValueComparer.Instance.Compare(compareValue, _bestCompareColumn.GetValueAt(0, default));
                    better = _isMin ? compareResult < 0 : compareResult > 0;
                }
                if (better)
                {
                    _hasBest = true;
                    _bestCompareColumn.UpdateAt(0, compareValue);
                    _bestValueColumn.UpdateAt(0, _fetchValueFunction(_reader.Batch, _reader.RowIndex));
                }
            }
        }

        public bool TryComputeRow(BulkWindowRowContext context, DataValueContainer result)
        {
            Debug.Assert(_bestValueColumn != null);
            if (_hasBest)
            {
                _bestValueColumn.GetValueAt(0, result, default);
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
    /// Shared frame rings for min_by/max_by, rescanned when the best entry leaves. Ties keep the oldest.
    /// </summary>
    internal sealed class BulkMinMaxFrameState : IDisposable
    {
        private readonly bool _isMin;
        private readonly BulkWindowValueRing _compareRing;
        private readonly BulkWindowValueRing _valueRing;
        private readonly Column _bestCompareColumn;
        private readonly Column _bestValueColumn;

        public long OldestPosition;
        public long NextFeedPosition;
        public bool HasBest;
        public long BestPosition;

        public BulkMinMaxFrameState(bool isMin, long ringCapacity, IMemoryAllocator memoryAllocator)
        {
            _isMin = isMin;
            _compareRing = new BulkWindowValueRing(ringCapacity, memoryAllocator);
            _valueRing = new BulkWindowValueRing(ringCapacity, memoryAllocator);
            _bestCompareColumn = Column.Create(memoryAllocator);
            _bestValueColumn = Column.Create(memoryAllocator);
            _bestCompareColumn.Add(NullValue.Instance);
            _bestValueColumn.Add(NullValue.Instance);
        }

        public void Clear(long startPosition)
        {
            _compareRing.Clear();
            _valueRing.Clear();
            HasBest = false;
            OldestPosition = startPosition;
            NextFeedPosition = startPosition;
        }

        public void Feed(IDataValue compareValue, IDataValue value)
        {
            _compareRing.Push(compareValue);
            _valueRing.Push(value);
            if (!compareValue.IsNull)
            {
                bool better = !HasBest;
                if (!better)
                {
                    var compareResult = DataValueComparer.Instance.Compare(compareValue, _bestCompareColumn.GetValueAt(0, default));
                    better = _isMin ? compareResult < 0 : compareResult > 0;
                }
                if (better)
                {
                    HasBest = true;
                    BestPosition = NextFeedPosition;
                    _bestCompareColumn.UpdateAt(0, compareValue);
                    _bestValueColumn.UpdateAt(0, value);
                }
            }
            NextFeedPosition++;
        }

        /// <summary>
        /// Drops entries before the frame start, rescans if the best was among them.
        /// </summary>
        public void Evict(long frameStartPosition)
        {
            bool bestEvicted = false;
            while (_compareRing.Count > 0 && OldestPosition < frameStartPosition)
            {
                if (HasBest && BestPosition == OldestPosition)
                {
                    bestEvicted = true;
                }
                _compareRing.PopOldest();
                _valueRing.PopOldest();
                OldestPosition++;
            }
            if (_compareRing.Count == 0)
            {
                OldestPosition = NextFeedPosition;
            }
            if (bestEvicted)
            {
                Rescan();
            }
        }

        private void Rescan()
        {
            HasBest = false;
            int bestIndex = -1;
            for (int i = 0; i < _compareRing.Count; i++)
            {
                var compareValue = _compareRing.GetAt(i);
                if (compareValue.IsNull)
                {
                    continue;
                }
                bool better = !HasBest;
                if (!better)
                {
                    var compareResult = DataValueComparer.Instance.Compare(compareValue, _compareRing.GetAt(bestIndex));
                    better = _isMin ? compareResult < 0 : compareResult > 0;
                }
                if (better)
                {
                    HasBest = true;
                    bestIndex = i;
                }
            }
            if (HasBest)
            {
                BestPosition = OldestPosition + bestIndex;
                _bestCompareColumn.UpdateAt(0, _compareRing.GetAt(bestIndex));
                _bestValueColumn.UpdateAt(0, _valueRing.GetAt(bestIndex));
            }
        }

        public void WriteResult(DataValueContainer result)
        {
            if (HasBest)
            {
                _bestValueColumn.GetValueAt(0, result, default);
            }
            else
            {
                result._type = ArrowTypeId.Null;
            }
        }

        public void Dispose()
        {
            _compareRing.Dispose();
            _valueRing.Dispose();
            _bestCompareColumn.Dispose();
            _bestValueColumn.Dispose();
        }
    }

    /// <summary>
    /// min_by/max_by over a frame ending before the row, e.g. ROWS BETWEEN 5 PRECEDING AND 2 PRECEDING.
    /// </summary>
    internal class BulkMinMaxByWindowFunctionFrameDelay : IBulkWindowFunction
    {
        private readonly Func<EventBatchData, int, IDataValue> _fetchValueFunction;
        private readonly Func<EventBatchData, int, IDataValue> _fetchCompareValueFunction;
        private readonly long _from;
        private readonly long _to;

        private BulkWindowValueRing? _pendingCompare;
        private BulkWindowValueRing? _pendingValue;
        private BulkMinMaxFrameState? _frame;
        private readonly bool _isMin;
        private long _currentPosition;

        public BulkMinMaxByWindowFunctionFrameDelay(
            Func<EventBatchData, int, IDataValue> fetchValueFunction,
            Func<EventBatchData, int, IDataValue> fetchCompareValueFunction,
            long from,
            long to,
            bool isMin)
        {
            Debug.Assert(from <= to && to < 0);
            _fetchValueFunction = fetchValueFunction;
            _fetchCompareValueFunction = fetchCompareValueFunction;
            _from = from;
            _to = to;
            _isMin = isMin;
        }

        public long AffectedRowsBefore => 0;

        public long AffectedRowsAfter => -_from;

        public bool StableByValueEquality => false;

        public long EqualityStableAfterRows => long.MaxValue;

        public int AuxiliaryStateColumnCount => 0;

        public Task Initialize(BulkWindowFunctionContext context)
        {
            _pendingCompare = new BulkWindowValueRing(-_to + 1, context.MemoryAllocator);
            _pendingValue = new BulkWindowValueRing(-_to + 1, context.MemoryAllocator);
            _frame = new BulkMinMaxFrameState(_isMin, _to - _from + 2, context.MemoryAllocator);
            return Task.CompletedTask;
        }

        public ValueTask Commit()
        {
            return ValueTask.CompletedTask;
        }

        public async ValueTask StartScan(ColumnRowReference partitionValues, BulkWindowSeedReader seedReader, bool fromPartitionStart)
        {
            Debug.Assert(_pendingCompare != null);
            Debug.Assert(_pendingValue != null);
            Debug.Assert(_frame != null);

            _currentPosition = -1;
            _pendingCompare.Clear();
            _pendingValue.Clear();

            long seedRows = 0;
            if (!fromPartitionStart)
            {
                await seedReader.EnsureRows((int)Math.Min(-_from, int.MaxValue));
                seedRows = Math.Min(-_from, seedReader.MaterializedRows);
            }
            _frame.Clear(-seedRows);
            for (int back = (int)seedRows; back >= 1; back--)
            {
                var row = seedReader.GetRow(back);
                Push(_fetchCompareValueFunction(row.referenceBatch, row.RowIndex), _fetchValueFunction(row.referenceBatch, row.RowIndex));
            }
        }

        private void Push(IDataValue compareValue, IDataValue value)
        {
            Debug.Assert(_pendingCompare != null);
            Debug.Assert(_pendingValue != null);
            Debug.Assert(_frame != null);

            _pendingCompare.Push(compareValue);
            _pendingValue.Push(value);
            if (_pendingCompare.Count > -_to)
            {
                _frame.Feed(_pendingCompare.PopOldest(), _pendingValue.PopOldest());
            }
        }

        public bool TryComputeRow(BulkWindowRowContext context, DataValueContainer result)
        {
            Debug.Assert(_frame != null);

            _currentPosition++;
            Push(_fetchCompareValueFunction(context.Batch, context.RowIndex), _fetchValueFunction(context.Batch, context.RowIndex));
            _frame.Evict(_currentPosition + _from);
            _frame.WriteResult(result);
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
    /// min_by/max_by over a frame reaching ahead, e.g. ROWS BETWEEN 2 PRECEDING AND 3 FOLLOWING.
    /// </summary>
    internal class BulkMinMaxByWindowFunctionFrameLookahead : IBulkWindowFunction
    {
        private readonly Func<EventBatchData, int, IDataValue> _fetchValueFunction;
        private readonly Func<EventBatchData, int, IDataValue> _fetchCompareValueFunction;
        private readonly long _from;
        private readonly long _to;
        private readonly bool _isMin;

        private BulkWindowForwardPartitionReader? _lookahead;
        private IMemoryAllocator? _memoryAllocator;
        private BulkMinMaxFrameState? _frame;

        private long _currentPosition;
        private long _lookaheadPosition;
        private bool _lookaheadStarted;
        private bool _lookaheadDone;

        public BulkMinMaxByWindowFunctionFrameLookahead(
            Func<EventBatchData, int, IDataValue> fetchValueFunction,
            Func<EventBatchData, int, IDataValue> fetchCompareValueFunction,
            long from,
            long to,
            bool isMin)
        {
            Debug.Assert(from <= to && to > 0);
            _fetchValueFunction = fetchValueFunction;
            _fetchCompareValueFunction = fetchCompareValueFunction;
            _from = from;
            _to = to;
            _isMin = isMin;
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
            // The first row loads positions 0..to before any eviction, so the ring must hold to + the
            // seeded preceding rows, not just the frame width.
            _frame = new BulkMinMaxFrameState(_isMin, _to + Math.Max(0, -_from) + 2, context.MemoryAllocator);
            return Task.CompletedTask;
        }

        public ValueTask Commit()
        {
            return ValueTask.CompletedTask;
        }

        public async ValueTask StartScan(ColumnRowReference partitionValues, BulkWindowSeedReader seedReader, bool fromPartitionStart)
        {
            Debug.Assert(_frame != null);

            _currentPosition = -1;
            _lookaheadStarted = false;
            _lookaheadDone = false;

            long seedRows = 0;
            if (!fromPartitionStart && _from < 0)
            {
                await seedReader.EnsureRows((int)Math.Min(-_from, int.MaxValue));
                seedRows = Math.Min(-_from, seedReader.MaterializedRows);
            }
            _frame.Clear(-seedRows);
            for (int back = (int)seedRows; back >= 1; back--)
            {
                var row = seedReader.GetRow(back);
                _frame.Feed(_fetchCompareValueFunction(row.referenceBatch, row.RowIndex), _fetchValueFunction(row.referenceBatch, row.RowIndex));
            }
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

            while (!_lookaheadDone && _frame.NextFeedPosition <= _currentPosition + _to)
            {
                if (!await _lookahead.MoveNextLogical())
                {
                    _lookaheadDone = true;
                    break;
                }
                _lookaheadPosition++;
                if (_lookaheadPosition < _frame.NextFeedPosition)
                {
                    continue;
                }
                _frame.Feed(_fetchCompareValueFunction(_lookahead.Batch, _lookahead.RowIndex), _fetchValueFunction(_lookahead.Batch, _lookahead.RowIndex));
            }

            _frame.Evict(_currentPosition + _from);
            _frame.WriteResult(result);
        }

        public ValueTask EndScan()
        {
            return ValueTask.CompletedTask;
        }
    }

    /// <summary>
    /// min_by/max_by from the partition start to an offset before the row, best never drops out.
    /// </summary>
    internal class BulkMinMaxByWindowFunctionUnboundedFromDelay : IBulkWindowFunction
    {
        private readonly Func<EventBatchData, int, IDataValue> _fetchValueFunction;
        private readonly Func<EventBatchData, int, IDataValue> _fetchCompareValueFunction;
        private readonly long _to;
        private readonly bool _isMin;
        private int _functionIndex;
        private int _auxStartIndex;

        private BulkWindowValueRing? _pendingCompare;
        private BulkWindowValueRing? _pendingValue;
        private Column? _bestCompareColumn;
        private Column? _bestValueColumn;
        private bool _hasBest;

        public BulkMinMaxByWindowFunctionUnboundedFromDelay(
            Func<EventBatchData, int, IDataValue> fetchValueFunction,
            Func<EventBatchData, int, IDataValue> fetchCompareValueFunction,
            long to,
            bool isMin)
        {
            Debug.Assert(to < 0);
            _fetchValueFunction = fetchValueFunction;
            _fetchCompareValueFunction = fetchCompareValueFunction;
            _to = to;
            _isMin = isMin;
        }

        public long AffectedRowsBefore => 0;

        public long AffectedRowsAfter => long.MaxValue;

        public bool StableByValueEquality => true;

        public long EqualityStableAfterRows => -_to;

        public int AuxiliaryStateColumnCount => 1;

        public Task Initialize(BulkWindowFunctionContext context)
        {
            _functionIndex = context.FunctionIndex;
            _auxStartIndex = context.AuxiliaryColumnStartIndex;
            _pendingCompare = new BulkWindowValueRing(-_to + 1, context.MemoryAllocator);
            _pendingValue = new BulkWindowValueRing(-_to + 1, context.MemoryAllocator);
            _bestCompareColumn = Column.Create(context.MemoryAllocator);
            _bestValueColumn = Column.Create(context.MemoryAllocator);
            _bestCompareColumn.Add(NullValue.Instance);
            _bestValueColumn.Add(NullValue.Instance);
            return Task.CompletedTask;
        }

        public ValueTask Commit()
        {
            return ValueTask.CompletedTask;
        }

        public async ValueTask StartScan(ColumnRowReference partitionValues, BulkWindowSeedReader seedReader, bool fromPartitionStart)
        {
            Debug.Assert(_pendingCompare != null);
            Debug.Assert(_pendingValue != null);
            Debug.Assert(_bestCompareColumn != null);
            Debug.Assert(_bestValueColumn != null);

            _pendingCompare.Clear();
            _pendingValue.Clear();
            _hasBest = false;

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

            var compareState = seedReader.GetState(1, _auxStartIndex);
            if (!compareState.IsNull)
            {
                _hasBest = true;
                _bestCompareColumn.UpdateAt(0, compareState);
                _bestValueColumn.UpdateAt(0, seedReader.GetState(1, _functionIndex));
            }

            var pendingRows = (int)Math.Min(-_to, seedReader.MaterializedRows);
            for (int back = pendingRows; back >= 1; back--)
            {
                var row = seedReader.GetRow(back);
                _pendingCompare.Push(_fetchCompareValueFunction(row.referenceBatch, row.RowIndex));
                _pendingValue.Push(_fetchValueFunction(row.referenceBatch, row.RowIndex));
            }
        }

        private void Offer(IDataValue compareValue, IDataValue value)
        {
            Debug.Assert(_bestCompareColumn != null);
            Debug.Assert(_bestValueColumn != null);

            if (compareValue.IsNull)
            {
                return;
            }
            bool better = !_hasBest;
            if (!better)
            {
                var compareResult = DataValueComparer.Instance.Compare(compareValue, _bestCompareColumn.GetValueAt(0, default));
                better = _isMin ? compareResult < 0 : compareResult > 0;
            }
            if (better)
            {
                _hasBest = true;
                _bestCompareColumn.UpdateAt(0, compareValue);
                _bestValueColumn.UpdateAt(0, value);
            }
        }

        public bool TryComputeRow(BulkWindowRowContext context, DataValueContainer result)
        {
            Debug.Assert(_pendingCompare != null);
            Debug.Assert(_pendingValue != null);
            Debug.Assert(_bestCompareColumn != null);
            Debug.Assert(_bestValueColumn != null);

            _pendingCompare.Push(_fetchCompareValueFunction(context.Batch, context.RowIndex));
            _pendingValue.Push(_fetchValueFunction(context.Batch, context.RowIndex));
            if (_pendingCompare.Count > -_to)
            {
                Offer(_pendingCompare.PopOldest(), _pendingValue.PopOldest());
            }

            if (_hasBest)
            {
                _bestValueColumn.GetValueAt(0, result, default);
                context.SetAuxValue(_auxStartIndex, _bestCompareColumn.GetValueAt(0, default));
            }
            else
            {
                result._type = ArrowTypeId.Null;
                context.SetAuxValue(_auxStartIndex, NullValue.Instance);
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
    /// min_by/max_by from the partition start to an offset ahead, fed through a lookahead.
    /// </summary>
    internal class BulkMinMaxByWindowFunctionUnboundedFromLookahead : IBulkWindowFunction
    {
        private readonly Func<EventBatchData, int, IDataValue> _fetchValueFunction;
        private readonly Func<EventBatchData, int, IDataValue> _fetchCompareValueFunction;
        private readonly long _to;
        private readonly bool _isMin;
        private int _functionIndex;
        private int _auxStartIndex;

        private BulkWindowForwardPartitionReader? _lookahead;
        private IMemoryAllocator? _memoryAllocator;
        private Column? _bestCompareColumn;
        private Column? _bestValueColumn;
        private bool _hasBest;

        private long _currentPosition;
        private long _nextFeedPosition;
        private long _lookaheadPosition;
        private bool _lookaheadStarted;
        private bool _lookaheadDone;

        public BulkMinMaxByWindowFunctionUnboundedFromLookahead(
            Func<EventBatchData, int, IDataValue> fetchValueFunction,
            Func<EventBatchData, int, IDataValue> fetchCompareValueFunction,
            long to,
            bool isMin)
        {
            Debug.Assert(to > 0);
            _fetchValueFunction = fetchValueFunction;
            _fetchCompareValueFunction = fetchCompareValueFunction;
            _to = to;
            _isMin = isMin;
        }

        public long AffectedRowsBefore => _to;

        public long AffectedRowsAfter => long.MaxValue;

        public bool StableByValueEquality => true;

        public long EqualityStableAfterRows => 0;

        public int AuxiliaryStateColumnCount => 1;

        public Task Initialize(BulkWindowFunctionContext context)
        {
            _functionIndex = context.FunctionIndex;
            _auxStartIndex = context.AuxiliaryColumnStartIndex;
            _memoryAllocator = context.MemoryAllocator;
            _lookahead = new BulkWindowForwardPartitionReader(context.PersistentTree, context.PartitionColumns, context.CreateInsertComparer());
            _bestCompareColumn = Column.Create(context.MemoryAllocator);
            _bestValueColumn = Column.Create(context.MemoryAllocator);
            _bestCompareColumn.Add(NullValue.Instance);
            _bestValueColumn.Add(NullValue.Instance);
            return Task.CompletedTask;
        }

        public ValueTask Commit()
        {
            return ValueTask.CompletedTask;
        }

        public async ValueTask StartScan(ColumnRowReference partitionValues, BulkWindowSeedReader seedReader, bool fromPartitionStart)
        {
            Debug.Assert(_bestCompareColumn != null);
            Debug.Assert(_bestValueColumn != null);

            _hasBest = false;
            _currentPosition = -1;
            _nextFeedPosition = 0;
            _lookaheadStarted = false;
            _lookaheadDone = false;

            if (!fromPartitionStart && await seedReader.EnsureRows(1))
            {
                var compareState = seedReader.GetState(1, _auxStartIndex);
                if (!compareState.IsNull)
                {
                    _hasBest = true;
                    _bestCompareColumn.UpdateAt(0, compareState);
                    _bestValueColumn.UpdateAt(0, seedReader.GetState(1, _functionIndex));
                }
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
            Debug.Assert(_bestCompareColumn != null);
            Debug.Assert(_bestValueColumn != null);

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
                var compareValue = _fetchCompareValueFunction(_lookahead.Batch, _lookahead.RowIndex);
                if (!compareValue.IsNull)
                {
                    bool better = !_hasBest;
                    if (!better)
                    {
                        var compareResult = DataValueComparer.Instance.Compare(compareValue, _bestCompareColumn.GetValueAt(0, default));
                        better = _isMin ? compareResult < 0 : compareResult > 0;
                    }
                    if (better)
                    {
                        _hasBest = true;
                        _bestCompareColumn.UpdateAt(0, compareValue);
                        _bestValueColumn.UpdateAt(0, _fetchValueFunction(_lookahead.Batch, _lookahead.RowIndex));
                    }
                }
                _nextFeedPosition++;
            }

            if (_hasBest)
            {
                _bestValueColumn.GetValueAt(0, result, default);
                context.SetAuxValue(_auxStartIndex, _bestCompareColumn.GetValueAt(0, default));
            }
            else
            {
                result._type = ArrowTypeId.Null;
                context.SetAuxValue(_auxStartIndex, NullValue.Instance);
            }
        }

        public ValueTask EndScan()
        {
            return ValueTask.CompletedTask;
        }
    }

    /// <summary>
    /// min_by/max_by to the partition end, e.g. ROWS BETWEEN 2 PRECEDING AND UNBOUNDED FOLLOWING.
    /// A pre-scan builds the suffix best change points (monotonic stack), result is the first at or after the frame start.
    /// </summary>
    internal class BulkMinMaxByWindowFunctionSuffix : IBulkWindowFunction
    {
        private readonly Func<EventBatchData, int, IDataValue> _fetchValueFunction;
        private readonly Func<EventBatchData, int, IDataValue> _fetchCompareValueFunction;
        private readonly long _from;
        private readonly bool _isMin;

        private BulkWindowForwardPartitionReader? _reader;
        private Column? _stackCompareColumn;
        private Column? _stackValueColumn;
        private readonly List<long> _stackPositions = new List<long>();
        private int _stackCount;
        private int _cursor;
        private long _currentPosition;

        public BulkMinMaxByWindowFunctionSuffix(
            Func<EventBatchData, int, IDataValue> fetchValueFunction,
            Func<EventBatchData, int, IDataValue> fetchCompareValueFunction,
            long from,
            bool isMin)
        {
            Debug.Assert(from != long.MinValue);
            _fetchValueFunction = fetchValueFunction;
            _fetchCompareValueFunction = fetchCompareValueFunction;
            _from = from;
            _isMin = isMin;
        }

        public long AffectedRowsBefore => long.MaxValue;

        public long AffectedRowsAfter => Math.Max(0, -_from);

        public bool StableByValueEquality => false;

        public long EqualityStableAfterRows => long.MaxValue;

        public int AuxiliaryStateColumnCount => 0;

        public Task Initialize(BulkWindowFunctionContext context)
        {
            _reader = new BulkWindowForwardPartitionReader(context.PersistentTree, context.PartitionColumns);
            _stackCompareColumn = Column.Create(context.MemoryAllocator);
            _stackValueColumn = Column.Create(context.MemoryAllocator);
            return Task.CompletedTask;
        }

        public ValueTask Commit()
        {
            return ValueTask.CompletedTask;
        }

        public async ValueTask StartScan(ColumnRowReference partitionValues, BulkWindowSeedReader seedReader, bool fromPartitionStart)
        {
            Debug.Assert(_reader != null);
            Debug.Assert(_stackCompareColumn != null);
            Debug.Assert(_stackValueColumn != null);

            _stackCount = 0;
            _stackPositions.Clear();
            _cursor = 0;
            _currentPosition = -1;

            long position = 0;
            await _reader.Reset(partitionValues);
            while (await _reader.MoveNextRow())
            {
                var compareValue = _fetchCompareValueFunction(_reader.Batch, _reader.RowIndex);
                if (!compareValue.IsNull)
                {
                    // Pop strictly worse entries, keep equal ones so the oldest wins.
                    while (_stackCount > 0)
                    {
                        var compareResult = DataValueComparer.Instance.Compare(_stackCompareColumn.GetValueAt(_stackCount - 1, default), compareValue);
                        if (_isMin ? compareResult > 0 : compareResult < 0)
                        {
                            _stackCount--;
                            _stackPositions.RemoveAt(_stackCount);
                        }
                        else
                        {
                            break;
                        }
                    }
                    var lastDupPosition = position + _reader.Weight - 1;
                    // The columns are reused across partition scans, so grow only past their real length.
                    if (_stackCount == _stackCompareColumn.Count)
                    {
                        _stackCompareColumn.Add(compareValue);
                        _stackValueColumn.Add(_fetchValueFunction(_reader.Batch, _reader.RowIndex));
                    }
                    else
                    {
                        _stackCompareColumn.UpdateAt(_stackCount, compareValue);
                        _stackValueColumn.UpdateAt(_stackCount, _fetchValueFunction(_reader.Batch, _reader.RowIndex));
                    }
                    _stackPositions.Add(lastDupPosition);
                    _stackCount++;
                }
                position += _reader.Weight;
            }
        }

        public bool TryComputeRow(BulkWindowRowContext context, DataValueContainer result)
        {
            Debug.Assert(_stackValueColumn != null);

            _currentPosition++;
            var frameStart = _currentPosition + _from;
            while (_cursor < _stackCount && _stackPositions[_cursor] < frameStart)
            {
                _cursor++;
            }
            if (_cursor < _stackCount)
            {
                _stackValueColumn.GetValueAt(_cursor, result, default);
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
