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
    /// The current scan row, reused between rows so do not hold on to it.
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
        /// Logical rows since the last change, zero on a changed row.
        /// </summary>
        public long RowsSinceLastChange;

        internal BulkWindowValueContainer Values = null!;

        // Aux values buffered here, the operator applies them under the page lock.
        internal readonly IDataValue?[] _pendingAuxValues;
        internal readonly int[] _pendingAuxSlots;
        internal int _pendingAuxCount;

        public BulkWindowRowContext(int totalStateColumns)
        {
            _pendingAuxValues = new IDataValue?[totalStateColumns];
            _pendingAuxSlots = new int[totalStateColumns];
        }

        /// <summary>
        /// Buffers an aux value, must stay valid until the compute call returns.
        /// </summary>
        public void SetAuxValue(int auxColumnIndex, IDataValue value)
        {
            if (_pendingAuxValues[auxColumnIndex] == null)
            {
                _pendingAuxSlots[_pendingAuxCount++] = auxColumnIndex;
            }
            _pendingAuxValues[auxColumnIndex] = value;
        }

        internal void ResetPendingAux()
        {
            for (int i = 0; i < _pendingAuxCount; i++)
            {
                _pendingAuxValues[_pendingAuxSlots[i]] = null;
            }
            _pendingAuxCount = 0;
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
        /// Comparers hold scratch state, each reader needs its own instance.
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
    /// Computes values during a forward partition scan, seeded from earlier stored rows.
    /// </summary>
    internal interface IBulkWindowFunction
    {
        /// <summary>
        /// Rows before a change it can affect, long.MaxValue means scan from the partition start.
        /// </summary>
        long AffectedRowsBefore { get; }

        /// <summary>
        /// Rows after a change it can affect, long.MaxValue means never stable by distance.
        /// </summary>
        long AffectedRowsAfter { get; }

        /// <summary>
        /// True when unchanged stored values imply stability after <see cref="EqualityStableAfterRows"/>.
        /// </summary>
        bool StableByValueEquality { get; }

        /// <summary>
        /// Rows since the last change before value equality implies stability.
        /// </summary>
        long EqualityStableAfterRows { get; }

        /// <summary>
        /// Extra state list columns needed besides the output column.
        /// </summary>
        int AuxiliaryStateColumnCount { get; }

        Task Initialize(BulkWindowFunctionContext context);

        ValueTask Commit();

        /// <summary>
        /// Called before scanning, the seed reader provides rows before the scan start.
        /// </summary>
        ValueTask StartScan(ColumnRowReference partitionValues, BulkWindowSeedReader seedReader, bool fromPartitionStart);

        /// <summary>
        /// Sync compute, false when a storage read is needed (must not have mutated state then).
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
