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
using FlowtideDotNet.Core.Compute.Columnar.Functions.BulkAggregations;
using FlowtideDotNet.Core.Operators.Aggregate.Column;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Tree;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.Operators.Aggregate.Bulk
{
    internal struct BulkAggregateMutator : IRowMutator<ColumnRowReference, ColumnAggregateStateReference>
    {
        private readonly IColumnBulkAggregation[] measures;
        private readonly Func<EventBatchData, int, bool>?[] measureFilters;
        private readonly int[] filterScratch;
        private readonly PrimitiveList<int> weights;
        private readonly EventBatchData incomingData;
        private readonly int[] groupSortIndices;
        private readonly List<AggregateComputeRange> computeRanges;
        private readonly ColumnStore.Column[] outputColumns;
        private readonly PrimitiveList<int> outputWeights;
        private readonly bool[] outputToTemp;
        private readonly bool[] isDeleted;

        public BulkAggregateMutator(
            IColumnBulkAggregation[] measures,
            Func<EventBatchData, int, bool>?[] measureFilters,
            int[] filterScratch,
            PrimitiveList<int> weights,
            EventBatchData incomingData,
            int[] indices,
            List<AggregateComputeRange> computeRanges,
            ColumnStore.Column[] outputColumns,
            PrimitiveList<int> outputWeights,
            bool[] outputToTemp,
            bool[] isDeleted)
        {
            this.measures = measures;
            this.measureFilters = measureFilters;
            this.filterScratch = filterScratch;
            this.weights = weights;
            this.incomingData = incomingData;
            this.groupSortIndices = indices;
            this.computeRanges = computeRanges;
            this.outputColumns = outputColumns;
            this.outputWeights = outputWeights;
            this.outputToTemp = outputToTemp;
            this.isDeleted = isDeleted;
        }

        /// <summary>
        /// Restricts a group's row range to the rows that pass the measure's FILTER predicate. Stateless
        /// measures (sum/sum0/count/avg) aggregate over the indices they are given, so a FILTER is applied by
        /// handing Compute only the rows that match. Measures without a filter get the full range unchanged.
        /// The scratch buffer is reused per measure since Compute consumes the span synchronously.
        /// </summary>
        private ReadOnlySpan<int> ApplyMeasureFilter(int measureIndex, ReadOnlySpan<int> indiceSpan)
        {
            var filter = measureFilters[measureIndex];
            if (filter == null)
            {
                return indiceSpan;
            }
            int count = 0;
            for (int j = 0; j < indiceSpan.Length; j++)
            {
                var physicalIndex = indiceSpan[j];
                if (filter(incomingData, physicalIndex))
                {
                    filterScratch[count++] = physicalIndex;
                }
            }
            return filterScratch.AsSpan(0, count);
        }

        public void GetSizePrefixSum(ColumnRowReference[] keys, ReadOnlySpan<int> indices, Span<int> sizes)
        {
            var columns = keys[0].referenceBatch.GetColumns_Unsafe();
            for (int i = 0; i < columns.Length; i++)
            {
                columns[i].GetPrefixSumByteSizes(indices, sizes);
            }
        }

        public GenericWriteOperation Process(ColumnRowReference key, bool exists, in ColumnAggregateStateReference existing, ref ColumnAggregateStateReference incoming, int sortedIndex)
        {
            if (exists)
            {
                incoming.weight += existing.weight;
                incoming.valueSent = existing.valueSent;

                bool writeToTemp = false;
                
                var stateColumns = existing.referenceBatch.GetColumns_Unsafe();
                for (int i = 0; i < measures.Length; i++)
                {
                    var stateCol = stateColumns[i];
                    var colReference = new ColumnReference(stateCol, existing.RowIndex, default);
                    var computeRange = computeRanges[sortedIndex];
                    var indiceSpan = groupSortIndices.AsSpan(computeRange.start, computeRange.length);
                    ReadOnlySpan<int> measureSpan = ApplyMeasureFilter(i, indiceSpan);
                    writeToTemp |= measures[i].Compute(measureSpan, weights, incomingData, colReference, sortedIndex);
                }
                
                if (incoming.weight == 0)
                {
                    var keyColumns = key.referenceBatch.GetColumns_Unsafe();
                    var keylength = keyColumns.Length;
                    if (keylength == 0)
                    {
                        for (int i = 0; i < measures.Length; i++)
                        {
                            var col = stateColumns[i];
                            col.UpdateAt(existing.RowIndex, NullValue.Instance);
                        }
                        outputToTemp[sortedIndex] = true;
                        return GenericWriteOperation.Upsert;
                    }

                    if (existing.valueSent)
                    {
                        for (int i = 0; i < keylength; i++)
                        {
                            var col = keyColumns[i];
                            outputColumns[i].InsertRangeFrom(outputColumns[i].Count, col, key.RowIndex, 1);
                        }

                        for (int i = 0; i < measures.Length; i++)
                        {
                            var col = stateColumns[measures.Length + i];
                            outputColumns[keylength + i].InsertRangeFrom(outputColumns[keylength + i].Count, col, existing.RowIndex, 1);
                        }
                        outputWeights.Add(-1);
                    }

                    // The group no longer exists in the persisted tree. Always mark it as deleted, even
                    // if its value was never emitted, so that any entry queued in the temporary tree by
                    // an insert earlier in the same watermark interval is removed (pushed as -1). Leaving
                    // it would make the temporary tree reference a key that is gone from the persisted
                    // tree, and the next watermark would index the persisted leaf with a negative
                    // bulk-search result and throw an IndexOutOfRangeException.
                    outputToTemp[sortedIndex] = false;
                    isDeleted[sortedIndex] = true;
                    return GenericWriteOperation.Delete;
                }
                else
                {
                    outputToTemp[sortedIndex] = writeToTemp;
                    return GenericWriteOperation.Upsert;
                }
            }
            else
            {
                var stateColumns = incoming.referenceBatch.GetColumns_Unsafe();
                for (int i = 0; i < measures.Length; i++)
                {
                    var stateCol = stateColumns[i];
                    var colReference = new ColumnReference(stateCol, incoming.RowIndex, default);
                    // Compute should be called here for each row, need alot of extra input here though.
                    var computeRange = computeRanges[sortedIndex];
                    var indiceSpan = groupSortIndices.AsSpan(computeRange.start, computeRange.length);
                    ReadOnlySpan<int> measureSpan = ApplyMeasureFilter(i, indiceSpan);
                    measures[i].Compute(measureSpan, weights, incomingData, colReference, sortedIndex);
                }
                // New data should always be output no matter what compute says
                outputToTemp[sortedIndex] = true;
            }
            return GenericWriteOperation.Upsert;
        }
    }
}
