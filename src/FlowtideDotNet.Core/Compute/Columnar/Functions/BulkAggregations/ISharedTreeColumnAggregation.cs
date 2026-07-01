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
using FlowtideDotNet.Core.Compute.Columnar.Functions.BulkAggregations.Stateful;
using FlowtideDotNet.Storage.Tree;
using FlowtideDotNet.Substrait.Expressions;
using System;

namespace FlowtideDotNet.Core.Compute.Columnar.Functions.BulkAggregations
{
    public interface ISharedTreeColumnAggregation : IColumnBulkAggregation
    {
        /// <summary>
        /// Indicates whether this aggregation supports consuming a shared tree instead of owning its own B+ Tree.
        /// </summary>
        bool SupportsSharedTree => true;

        /// <summary>
        /// Indicates whether null values should be ignored by the shared tree.
        /// </summary>
        bool IgnoreNulls => true;

        /// <summary>
        /// Returns the Substrait Expression representing the aggregated value.
        /// This is used by the operator to group measures that share the same expression.
        /// </summary>
        Expression ValueExpression { get; }

        /// <summary>
        /// Exposes the compiled projection function that extracts the aggregated value from an incoming batch.
        /// This is retrieved by the operator to populate the shared B+ Tree.
        /// </summary>
        Func<EventBatchData, int, IDataValue> ValueProjection { get; }

        /// <summary>
        /// Binds the shared B+ Tree instance to this aggregation.
        /// </summary>
        void BindSharedTree(
            IBPlusTree<BulkGroupValueRowReference, int, BulkGroupValueKeyContainer, PrimitiveListValueContainer<int>> sharedTree,
            int groupingLength);

        /// <summary>
        /// A hook called during the shared B+ Tree bulk insertion process when a key/value pair is added, updated, or deleted.
        /// This allows reactive state tracking (like updating distinct count counters).
        /// </summary>
        void OnValueMutated(BulkGroupValueRowReference key, bool exists, int oldWeight, int newWeight, int sortedIndex);

        /// <summary>
        /// Sets the mapping from physical row index to group sorted index in the main batch.
        /// </summary>
        void SetGroupMapping(int[] groupSortLookup) { }
    }
}
