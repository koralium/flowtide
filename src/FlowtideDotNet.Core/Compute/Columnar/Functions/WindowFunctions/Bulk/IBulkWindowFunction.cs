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
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Core.Operators.Window.Bulk;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.Tree;
using System.Diagnostics;

namespace FlowtideDotNet.Core.Compute.Columnar.Functions.WindowFunctions.Bulk
{
    /// <summary>
    /// The current logical row of a bulk window scan. The operator mutates the fields between rows, so
    /// implementations must not hold on to the instance across rows.
    /// </summary>
    internal sealed class BulkWindowRowContext
    {
        /// <summary>
        /// The key batch of the current leaf page.
        /// </summary>
        public EventBatchData Batch = null!;

        /// <summary>
        /// The physical row index within <see cref="Batch"/>.
        /// </summary>
        public int RowIndex;

        /// <summary>
        /// The duplicate index of the logical row, 0 based, smaller than <see cref="Weight"/>.
        /// </summary>
        public int DupIndex;

        /// <summary>
        /// The weight (number of logical duplicates) of the physical row.
        /// </summary>
        public int Weight;

        /// <summary>
        /// Logical rows scanned since the last change position was passed. Zero when the current row is a
        /// changed row or directly follows a deleted row.
        /// </summary>
        public long RowsSinceLastChange;

        internal BulkWindowValueContainer Values = null!;

        /// <summary>
        /// Set when a function stored a different auxiliary value for the current logical row, used by the
        /// operator for stability tracking. Reset by the operator before each function call.
        /// </summary>
        internal bool AuxChanged;

        /// <summary>
        /// Stores an auxiliary state value for the current logical row.
        /// </summary>
        public void SetAuxValue(int auxColumnIndex, IDataValue value)
        {
            var list = Values._functionStates[auxColumnIndex];
            var listLength = list.GetListLength(RowIndex);
            if (DupIndex < listLength)
            {
                var existing = list.GetListElementValue(RowIndex, DupIndex);
                if (DataValueComparer.Instance.Compare(existing, value) != 0)
                {
                    list.UpdateListElement(RowIndex, DupIndex, value);
                    AuxChanged = true;
                }
            }
            else
            {
                Debug.Assert(DupIndex == listLength, "Auxiliary state must be appended in duplicate order");
                list.AppendToList(RowIndex, value);
                AuxChanged = true;
            }
        }
    }

    /// <summary>
    /// Resources handed to a bulk window function at initialization.
    /// </summary>
    internal sealed class BulkWindowFunctionContext
    {
        public required IBPlusTree<ColumnRowReference, BulkWindowValue, ColumnKeyStorageContainer, BulkWindowValueContainer> PersistentTree { get; init; }

        public required List<int> PartitionColumns { get; init; }

        /// <summary>
        /// Creates a new comparer instance over the full row ordering of the persistent tree. Comparer
        /// instances hold scratch state, so each reader needs its own instance.
        /// </summary>
        public required Func<BulkWindowInsertComparer> CreateInsertComparer { get; init; }

        /// <summary>
        /// The index of the function, which is also the state column index of its output values.
        /// </summary>
        public required int FunctionIndex { get; init; }

        /// <summary>
        /// The state column index of the function's first auxiliary column.
        /// </summary>
        public required int AuxiliaryColumnStartIndex { get; init; }

        public required IMemoryAllocator MemoryAllocator { get; init; }

        public required IStateManagerClient StateManagerClient { get; init; }
    }

    /// <summary>
    /// A window function implementation for the bulk window operator. Functions compute values during a
    /// forward scan over a partition and can seed their state from the stored values of rows before the
    /// scan start, which allows recomputing only the rows a change can affect.
    /// </summary>
    internal interface IBulkWindowFunction
    {
        /// <summary>
        /// How many logical rows before a changed row can have their value affected by that change.
        /// long.MaxValue means the scan must always start at the partition start.
        /// </summary>
        long AffectedRowsBefore { get; }

        /// <summary>
        /// How many logical rows after the last changed row can have their value affected. When the scan has
        /// passed this many rows since the last change the function is stable. long.MaxValue means the
        /// function never becomes stable by distance alone.
        /// </summary>
        long AffectedRowsAfter { get; }

        /// <summary>
        /// When true, the function is stable once a row's computed output and stored auxiliary state are
        /// unchanged and at least <see cref="EqualityStableAfterRows"/> logical rows have passed since the
        /// last change.
        /// </summary>
        bool StableByValueEquality { get; }

        /// <summary>
        /// The minimum number of logical rows that must have passed since the last change before value
        /// equality implies stability.
        /// </summary>
        long EqualityStableAfterRows { get; }

        /// <summary>
        /// The number of extra state list columns the function needs besides its output column.
        /// </summary>
        int AuxiliaryStateColumnCount { get; }

        Task Initialize(BulkWindowFunctionContext context);

        ValueTask Commit();

        /// <summary>
        /// Called before rows are scanned. <paramref name="partitionValues"/> references a row belonging to
        /// the partition and stays valid for the whole scan. When <paramref name="fromPartitionStart"/> is
        /// false the seed reader provides the logical rows preceding the scan start; the reader may reach the
        /// partition start early, in which case the function should treat the scan start as the partition start.
        /// </summary>
        ValueTask StartScan(ColumnRowReference partitionValues, BulkWindowSeedReader seedReader, bool fromPartitionStart);

        /// <summary>
        /// Computes the function value for the current logical row into <paramref name="result"/> when it
        /// can be done synchronously, which is the common case. Returns false when asynchronous work such
        /// as a storage read is needed, in which case <see cref="ComputeRow"/> must be awaited instead and
        /// this call must not have modified any state.
        /// </summary>
        bool TryComputeRow(BulkWindowRowContext context, DataValueContainer result);

        /// <summary>
        /// Computes the function value for the current logical row into <paramref name="result"/>.
        /// </summary>
        ValueTask ComputeRow(BulkWindowRowContext context, DataValueContainer result);

        /// <summary>
        /// Called when the partition scan has ended.
        /// </summary>
        ValueTask EndScan();
    }
}
