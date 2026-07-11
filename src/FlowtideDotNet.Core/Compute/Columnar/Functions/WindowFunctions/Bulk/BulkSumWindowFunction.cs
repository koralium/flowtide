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
using FlowtideDotNet.Substrait.Expressions;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;

namespace FlowtideDotNet.Core.Compute.Columnar.Functions.WindowFunctions.Bulk
{
    internal class BulkSumWindowFunctionDefinition : BulkWindowFunctionDefinition
    {
        /// <summary>
        /// Largest bounded frame reach kept in memory during a scan. Larger frames fall back to the non
        /// bulk operator which keeps its frame in persistent storage.
        /// </summary>
        internal const long MaxInMemoryFrameSize = 1_000_000;

        public override bool TryCreate(WindowFunction windowFunction, IFunctionsRegister functionsRegister, [NotNullWhen(true)] out IBulkWindowFunction? bulkWindowFunction)
        {
            bulkWindowFunction = null;
            if (windowFunction.Arguments.Count < 1)
            {
                return false;
            }
            var compiledValue = ColumnProjectCompiler.CompileToValue(windowFunction.Arguments[0], functionsRegister);
            MinMaxBoundUtils.GetBoundInfo(windowFunction, out var isRowBounded, out var lowerBoundRowOffset, out var upperBoundRowOffset);

            // The variant selection matches SumWindowFunctionDefinition so results stay identical with the
            // non bulk operator.
            if (isRowBounded && lowerBoundRowOffset != long.MinValue)
            {
                if (upperBoundRowOffset > 0 || lowerBoundRowOffset > upperBoundRowOffset || -lowerBoundRowOffset > MaxInMemoryFrameSize)
                {
                    // Frames that reach ahead of the current row are not supported in bulk mode yet.
                    return false;
                }
                bulkWindowFunction = new BulkSumWindowFunctionBounded(compiledValue, lowerBoundRowOffset, upperBoundRowOffset);
                return true;
            }
            if (isRowBounded && lowerBoundRowOffset == long.MinValue && upperBoundRowOffset == long.MaxValue)
            {
                bulkWindowFunction = new BulkSumWindowFunctionUnbounded(compiledValue);
                return true;
            }
            if (isRowBounded && lowerBoundRowOffset == long.MinValue)
            {
                if (upperBoundRowOffset > 0 || -upperBoundRowOffset > MaxInMemoryFrameSize)
                {
                    return false;
                }
                bulkWindowFunction = new BulkSumWindowFunctionUnboundedFrom(compiledValue, upperBoundRowOffset);
                return true;
            }
            bulkWindowFunction = new BulkSumWindowFunctionUnbounded(compiledValue);
            return true;
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
    /// Sum over a bounded rows frame that does not reach ahead of the current row,
    /// for example ROWS BETWEEN 4 PRECEDING AND 1 PRECEDING. The frame contents are kept in an in memory
    /// ring during the scan, seeded from the rows before the scan start, so a change only recomputes the
    /// rows whose frame can contain the change.
    /// </summary>
    internal class BulkSumWindowFunctionBounded : IBulkWindowFunction
    {
        private readonly Func<EventBatchData, int, IDataValue> _fetchValueFunction;
        private readonly long _from;
        private readonly long _to;
        private readonly int _frameSize;

        private BulkWindowValueRing? _pending;
        private BulkWindowValueRing? _frame;
        private readonly DataValueContainer _sumState = new DataValueContainer();

        public BulkSumWindowFunctionBounded(Func<EventBatchData, int, IDataValue> fetchValueFunction, long from, long to)
        {
            Debug.Assert(from <= to && to <= 0);
            _fetchValueFunction = fetchValueFunction;
            _from = from;
            _to = to;
            _frameSize = (int)(to - from + 1);
        }

        public long AffectedRowsBefore => 0;

        public long AffectedRowsAfter => -_from;

        public bool StableByValueEquality => false;

        public long EqualityStableAfterRows => long.MaxValue;

        public int AuxiliaryStateColumnCount => 0;

        public Task Initialize(BulkWindowFunctionContext context)
        {
            _pending = new BulkWindowValueRing((int)(-_to) + 1, context.MemoryAllocator);
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

            var lookback = (int)(-_from);
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
    /// Sum from the partition start up to the current row or a preceding offset, for example a running sum.
    /// The sum is seeded from the stored value of the row before the scan start, so appended rows only add
    /// to the previous sum instead of rescanning the partition. The function is stable once a recomputed
    /// row keeps its stored sum.
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
            _pending = new BulkWindowValueRing((int)(-_to) + 1, context.MemoryAllocator);
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

            var lookback = Math.Max(1, (int)(-_to));
            await seedReader.EnsureRows(lookback);
            if (seedReader.MaterializedRows == 0)
            {
                return;
            }

            // The previous row's stored output is the sum up to its own frame end, feed the rows after that
            // frame end into the pending ring so they enter the sum at the right rows.
            BulkSumUtils.CopySumValue(seedReader.GetState(1, _functionIndex), _sumState);
            var pendingRows = Math.Min((int)(-_to), seedReader.MaterializedRows);
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
    /// Sum over the whole partition. Any change requires re-emitting every row of the partition, so the
    /// partition total is computed with a single pre-scan; when the total is unchanged the scan stops after
    /// the first recomputed row.
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
