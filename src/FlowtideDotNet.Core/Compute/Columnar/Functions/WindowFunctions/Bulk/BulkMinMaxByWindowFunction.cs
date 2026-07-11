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
                if (upperBoundRowOffset != 0 || lowerBoundRowOffset > 0 || -lowerBoundRowOffset > int.MaxValue)
                {
                    // Only frames ending at the current row are supported in bulk mode.
                    return false;
                }
                bulkWindowFunction = new BulkMinMaxByWindowFunctionBounded(compiledValue, compiledCompareValue, lowerBoundRowOffset, isMin);
                return true;
            }
            if (isRowBounded && lowerBoundRowOffset == long.MinValue && upperBoundRowOffset == long.MaxValue)
            {
                bulkWindowFunction = new BulkMinMaxByWindowFunctionUnbounded(compiledValue, compiledCompareValue, isMin);
                return true;
            }
            if (isRowBounded && lowerBoundRowOffset == long.MinValue)
            {
                if (upperBoundRowOffset != 0)
                {
                    return false;
                }
                bulkWindowFunction = new BulkMinMaxByWindowFunctionUnboundedFrom(compiledValue, compiledCompareValue, isMin);
                return true;
            }
            // No frame given, the non bulk operator does not support this either.
            return false;
        }
    }

    /// <summary>
    /// min_by/max_by over a trailing rows frame ending at the current row, for example
    /// ROWS BETWEEN 364 PRECEDING AND CURRENT ROW. Each logical row stores, besides its output value, how
    /// many rows back the best value sits and its compare value. When rows are appended the previous row's
    /// stored position is shifted and only compared against the new value, so no frame scan is needed
    /// unless the best value falls out of the frame, in which case the frame is rescanned backwards.
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

        public async ValueTask ComputeRow(BulkWindowRowContext context, DataValueContainer result)
        {
            Debug.Assert(_bestValueColumn != null);

            if (_hasBest)
            {
                _bestOffsetBack++;
            }

            if (_hasBest && _bestOffsetBack > -_from)
            {
                // The best value fell out of the frame, rescan the frame backwards from the current row.
                await Rescan(context);
            }
            else
            {
                var compareValue = _fetchCompareValueFunction(context.Batch, context.RowIndex);
                if (!compareValue.IsNull && (!_hasBest || IsBetterStrict(compareValue)))
                {
                    _hasBest = true;
                    _bestOffsetBack = 0;
                    SetBest(compareValue, _fetchValueFunction(context.Batch, context.RowIndex));
                }
            }

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

            // The current row and its earlier duplicates, ties prefer the oldest entry in the frame.
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
    /// min_by/max_by from the partition start to the current row. The running best value and its compare
    /// value are seeded from the previous row's stored state, so appended rows are a single comparison. The
    /// function is stable once a recomputed row keeps both its output and its stored compare value.
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

        public ValueTask ComputeRow(BulkWindowRowContext context, DataValueContainer result)
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
            return ValueTask.CompletedTask;
        }

        public ValueTask EndScan()
        {
            return ValueTask.CompletedTask;
        }
    }

    /// <summary>
    /// min_by/max_by over the whole partition. The partition best is computed with a single pre-scan; when
    /// it is unchanged the emit scan stops after the first recomputed row.
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

        public ValueTask ComputeRow(BulkWindowRowContext context, DataValueContainer result)
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
            return ValueTask.CompletedTask;
        }

        public ValueTask EndScan()
        {
            return ValueTask.CompletedTask;
        }
    }
}
