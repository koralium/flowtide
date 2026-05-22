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
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.StateManager;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.Compute.Columnar.Functions.BulkAggregations
{
    public interface IColumnBulkAggregation
    {
        /// <summary>
        /// A new incoming batch of data, use this method to do any projections for arguments to the aggregate method.
        /// </summary>
        /// <param name="weights"></param>
        /// <param name="batchData"></param>
        void NewBatch(PrimitiveList<int> weights, EventBatchData batchData);

        /// <summary>
        /// If the measure needs to store any data to its own state handling, if stateless this can return just a ValueTask.CompletedTask
        /// </summary>
        /// <param name="weights"></param>
        /// <param name="groupValueColumns"></param>
        /// <param name="incoming"></param>
        /// <param name="sortedByGroupIndices"></param>
        /// <returns></returns>
        ValueTask StoreAsync(PrimitiveList<int> weights, IColumn[] groupValueColumns, EventBatchData incoming, ReadOnlySpan<int> sortedByGroupIndices);

        /// <summary>
        /// Runs once per group, this is run in a hot path when data is RMW to state storage, so no complex I/O or anything in here.
        /// Return true if the grouping actually needs to send output, or false if it doesnt, say MIN/MAX might not need to output
        /// a new value since it didnt change.
        /// </summary>
        /// <typeparam name="TValue"></typeparam>
        /// <param name="groupStartIndex"></param>
        /// <param name="groupEndIndex"></param>
        /// <param name="indices"></param>
        /// <param name="data"></param>
        /// <param name="groupState"></param>
        bool Compute(int groupStartIndex, int groupEndIndex, PrimitiveList<int> weights, ReadOnlySpan<int> indices, EventBatchData data, ColumnReference groupState);

        /// <summary>
        /// Fetches the current value, the grouping values are sorted by group key in ascending order.
        /// Each row in the grouping values have its respective group state, each row should add to outputColumn.
        /// </summary>
        /// <param name="groupingValuesSorted"></param>
        /// <param name="groupStates"></param>
        /// <param name="outputColumn"></param>
        /// <returns></returns>
        ValueTask GetValuesAsync(IColumn[] groupingValuesSorted, ColumnReference[] groupStates, Column outputColumn);

        /// <summary>
        /// Initializes the measurement, allows creating of custom state handling mechanisms.
        /// </summary>
        /// <param name="groupingLength"></param>
        /// <param name="stateManagerClient"></param>
        /// <param name="memoryAllocator"></param>
        /// <returns></returns>
        Task InitializeAsync(int groupingLength, IStateManagerClient stateManagerClient, IMemoryAllocator memoryAllocator);

        /// <summary>
        /// Called on checkpoint to allow the measurement to commit any of its own state handling.
        /// </summary>
        /// <returns></returns>
        Task CommitAsync();
    }
}
