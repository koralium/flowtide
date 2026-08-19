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
using FlowtideDotNet.Core.Compute.Columnar.Functions.TableFunctions;
using FlowtideDotNet.Core.Operators.Window.Bulk;
using FlowtideDotNet.Substrait.Expressions;
using System.Diagnostics.CodeAnalysis;

namespace FlowtideDotNet.Core.Compute.Columnar.Functions.WindowFunctions.Bulk
{
    internal class BulkSessionWindowFunctionDefinition : BulkWindowFunctionDefinition
    {
        public override bool TryCreate(WindowFunction windowFunction, IFunctionsRegister functionsRegister, [NotNullWhen(true)] out IBulkWindowFunction? bulkWindowFunction)
        {
            if (windowFunction.Arguments.Count != 2)
            {
                throw new ArgumentException("session_window function requires two arguments: (gap_amount, gap_unit)");
            }
            var gapTicks = WindowTickResolver.ResolveTicks(windowFunction.Arguments[0], windowFunction.Arguments[1], "session_window", "gap");
            bulkWindowFunction = new BulkSessionWindowFunction(gapTicks);
            return true;
        }
    }

    /// <summary>
    /// Assigns each row the start of its session.
    /// A session is a run of rows within the gap.
    /// </summary>
    internal class BulkSessionWindowFunction : IBulkWindowFunction
    {
        private readonly long _gapTicks;
        private int _functionIndex;
        private Func<EventBatchData, int, IDataValue>? _orderValueFunction;

        // Carried across rows within a scan.
        private bool _hasPrevious;
        private long _previousTicks;
        private long _sessionStartTicks;
        private long _sessionStartOffset;

        public BulkSessionWindowFunction(long gapTicks)
        {
            _gapTicks = gapTicks;
        }

        /// <summary>
        /// A change only ever moves rows after it.
        /// </summary>
        public long AffectedRowsBefore => 0;

        /// <summary>
        /// Reaches every following row, value equality stops it.
        /// </summary>
        public long AffectedRowsAfter => long.MaxValue;

        /// <summary>
        /// Carried state is the key value and the output.
        /// </summary>
        public bool StableByValueEquality => true;

        public long EqualityStableAfterRows => 0;

        public int AuxiliaryStateColumnCount => 0;

        public Task Initialize(BulkWindowFunctionContext context)
        {
            _functionIndex = context.FunctionIndex;

            // The gap is measured on the order by column.
            if (context.OrderBy.Count != 1)
            {
                throw new InvalidOperationException("session_window requires exactly one order by column");
            }
            var sortField = context.OrderBy[0];
            if (sortField.SortDirection != SortDirection.SortDirectionAscNullsFirst &&
                sortField.SortDirection != SortDirection.SortDirectionAscNullsLast &&
                sortField.SortDirection != SortDirection.SortDirectionUnspecified)
            {
                throw new InvalidOperationException("session_window requires an ascending order by column");
            }
            _orderValueFunction = ColumnProjectCompiler.CompileToValue(sortField.Expression, context.FunctionsRegister);
            return Task.CompletedTask;
        }

        public ValueTask Commit()
        {
            return ValueTask.CompletedTask;
        }

        public async ValueTask StartScan(ColumnRowReference partitionValues, BulkWindowSeedReader seedReader, bool fromPartitionStart)
        {
            // An empty seed is the only partition start signal.
            if (fromPartitionStart || !await seedReader.EnsureRows(1))
            {
                _hasPrevious = false;
                return;
            }

            var previousStart = seedReader.GetState(1, _functionIndex);
            var previousRow = seedReader.GetRow(1);
            var previousValue = _orderValueFunction!(previousRow.referenceBatch, previousRow.RowIndex);

            // Nothing to carry, so start a new session.
            if (previousStart.Type != ArrowTypeId.Timestamp || previousValue.Type != ArrowTypeId.Timestamp)
            {
                _hasPrevious = false;
                return;
            }

            var previousStartTimestamp = previousStart.AsTimestamp;
            _hasPrevious = true;
            _previousTicks = previousValue.AsTimestamp.ticks;
            _sessionStartTicks = previousStartTimestamp.ticks;
            _sessionStartOffset = previousStartTimestamp.offset;
        }

        public bool TryComputeRow(BulkWindowRowContext context, DataValueContainer result)
        {
            var value = _orderValueFunction!(context.Batch, context.RowIndex);

            // No value means no session, and the run is unbroken.
            if (value.Type != ArrowTypeId.Timestamp)
            {
                result._type = ArrowTypeId.Null;
                return true;
            }

            var timestamp = value.AsTimestamp;
            if (!_hasPrevious || timestamp.ticks - _previousTicks > _gapTicks)
            {
                _sessionStartTicks = timestamp.ticks;
                _sessionStartOffset = timestamp.offset;
            }
            _previousTicks = timestamp.ticks;
            _hasPrevious = true;

            result._type = ArrowTypeId.Timestamp;
            result._timestampValue = new TimestampTzValue(_sessionStartTicks, _sessionStartOffset);
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

        public void Dispose()
        {
        }
    }
}
