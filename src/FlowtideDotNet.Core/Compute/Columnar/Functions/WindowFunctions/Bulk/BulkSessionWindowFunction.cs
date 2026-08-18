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
    /// Assigns each row the start of the session it belongs to. A session is a maximal run of rows
    /// where no two neighbours are further apart than the gap, so it is a pure function of the
    /// order by value and the run the row sits in.
    ///
    /// The scan visits rows in order by value order, so the run is found by carrying the previous
    /// row's value forward. The previous row's stored output is the session start, which is why a
    /// scan starting at a changed row only needs a single seed row rather than a walk back to the
    /// nearest gap.
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
        /// A change never moves the session start of a row ahead of it in order: an insert only ever
        /// extends or merges forward, and a delete only ever shrinks or splits forward.
        /// </summary>
        public long AffectedRowsBefore => 0;

        /// <summary>
        /// A change can reach every following row, up to the next gap. Distance cannot bound it,
        /// the value equality below is what stops the scan.
        /// </summary>
        public long AffectedRowsAfter => long.MaxValue;

        /// <summary>
        /// The state carried forward is the row's own order by value and its session start. The
        /// value comes from an immutable key column and the session start is the stored output, so
        /// one row keeping its stored value means every row after it keeps its value too.
        /// </summary>
        public bool StableByValueEquality => true;

        public long EqualityStableAfterRows => 0;

        public int AuxiliaryStateColumnCount => 0;

        public Task Initialize(BulkWindowFunctionContext context)
        {
            _functionIndex = context.FunctionIndex;

            // The scan order is the only ordering the carried state can rely on, so the gap must be
            // measured on the column the rows are ordered by.
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
            // With AffectedRowsBefore 0 the operator never reports the partition start, an empty
            // seed is the only signal that there is no row before the scan.
            if (fromPartitionStart || !await seedReader.EnsureRows(1))
            {
                _hasPrevious = false;
                return;
            }

            var previousStart = seedReader.GetState(1, _functionIndex);
            var previousRow = seedReader.GetRow(1);
            var previousValue = _orderValueFunction!(previousRow.referenceBatch, previousRow.RowIndex);

            // A previous row without a session carries nothing forward, the scan starts a new one.
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

            // Rows without a usable value belong to no session. They sort together at one end, so
            // leaving the carried state untouched keeps them from splitting a run.
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
