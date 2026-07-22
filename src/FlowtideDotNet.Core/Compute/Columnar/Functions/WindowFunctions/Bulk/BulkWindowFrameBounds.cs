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
        /// Finite lower bound, the upper may still be UNBOUNDED FOLLOWING.
        /// </summary>
        BoundedRows,

        /// <summary>
        /// UNBOUNDED PRECEDING to a finite upper bound.
        /// </summary>
        UnboundedPreceding,

        /// <summary>
        /// The whole partition, unbounded both ends or no frame.
        /// </summary>
        WholePartition
    }

    /// <summary>
    /// Shared frame bound classification, matches the non bulk definitions.
    /// </summary>
    internal readonly struct BulkWindowFrameBounds
    {
        public BulkWindowFrameKind Kind { get; }

        /// <summary>
        /// Lower offset from the current row, long.MinValue at the partition start.
        /// </summary>
        public long From { get; }

        /// <summary>
        /// Upper offset from the current row, long.MaxValue at the partition end.
        /// </summary>
        public long To { get; }

        private BulkWindowFrameBounds(BulkWindowFrameKind kind, long from, long to)
        {
            Kind = kind;
            From = from;
            To = to;
        }

        /// <summary>
        /// Parses the bounds, no frame maps to the whole partition.
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
    /// Frame with start after end, always empty so every row is null.
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
