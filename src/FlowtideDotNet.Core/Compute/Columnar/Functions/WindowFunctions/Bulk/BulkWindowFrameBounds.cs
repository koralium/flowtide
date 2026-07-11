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

namespace FlowtideDotNet.Core.Compute.Columnar.Functions.WindowFunctions.Bulk
{
    internal enum BulkWindowFrameKind
    {
        /// <summary>
        /// A rows frame with a finite lower bound, for example ROWS BETWEEN 4 PRECEDING AND CURRENT ROW.
        /// The upper bound may still be long.MaxValue (UNBOUNDED FOLLOWING).
        /// </summary>
        BoundedRows,

        /// <summary>
        /// UNBOUNDED PRECEDING to a finite upper bound, for example a running aggregate.
        /// </summary>
        UnboundedPreceding,

        /// <summary>
        /// The whole partition, either explicit unbounded bounds or no frame at all.
        /// </summary>
        WholePartition
    }

    /// <summary>
    /// Shared classification of a window function's frame bounds, so every bulk function definition maps
    /// bound shapes to variants the same way as the non bulk definitions.
    /// </summary>
    internal readonly struct BulkWindowFrameBounds
    {
        public BulkWindowFrameKind Kind { get; }

        /// <summary>
        /// Lower bound offset relative to the current row, negative for preceding. long.MinValue when the
        /// frame starts at the partition start.
        /// </summary>
        public long From { get; }

        /// <summary>
        /// Upper bound offset relative to the current row, negative for preceding. long.MaxValue when the
        /// frame ends at the partition end.
        /// </summary>
        public long To { get; }

        private BulkWindowFrameBounds(BulkWindowFrameKind kind, long from, long to)
        {
            Kind = kind;
            From = from;
            To = to;
        }

        /// <summary>
        /// Parses the window function's bounds using the same rules as the non bulk function definitions:
        /// no bounds at all maps to the whole partition.
        /// </summary>
        public static BulkWindowFrameBounds Parse(WindowFunction windowFunction)
        {
            MinMaxBoundUtils.GetBoundInfo(windowFunction, out var isRowBounded, out var from, out var to);
            if (!isRowBounded || (from == long.MinValue && to == long.MaxValue))
            {
                return new BulkWindowFrameBounds(BulkWindowFrameKind.WholePartition, long.MinValue, long.MaxValue);
            }
            if (from == long.MinValue)
            {
                return new BulkWindowFrameBounds(BulkWindowFrameKind.UnboundedPreceding, from, to);
            }
            return new BulkWindowFrameBounds(BulkWindowFrameKind.BoundedRows, from, to);
        }
    }

    /// <summary>
    /// Window function for degenerate frames whose start lies after their end (for example
    /// ROWS BETWEEN 1 PRECEDING AND 3 PRECEDING). The frame is always empty, so every row is null.
    /// </summary>
    internal sealed class BulkEmptyFrameWindowFunction : IBulkWindowFunction
    {
        public long AffectedRowsBefore => 0;

        public long AffectedRowsAfter => 0;

        public bool StableByValueEquality => false;

        public long EqualityStableAfterRows => long.MaxValue;

        public int AuxiliaryStateColumnCount => 0;

        public Task Initialize(BulkWindowFunctionContext context)
        {
            return Task.CompletedTask;
        }

        public ValueTask Commit()
        {
            return ValueTask.CompletedTask;
        }

        public ValueTask StartScan(ColumnRowReference partitionValues, BulkWindowSeedReader seedReader, bool fromPartitionStart)
        {
            return ValueTask.CompletedTask;
        }

        public bool TryComputeRow(BulkWindowRowContext context, DataValueContainer result)
        {
            result._type = ArrowTypeId.Null;
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
