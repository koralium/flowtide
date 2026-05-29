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
        private readonly PrimitiveList<int> weights;
        private readonly EventBatchData incomingData;
        private readonly int[] indices;
        private readonly List<AggregateComputeRange> computeRanges;
        private readonly ColumnStore.Column[] outputColumns;
        private readonly PrimitiveList<int> outputWeights;
        private readonly bool[] outputToTemp;
        private readonly bool[] isDeleted;

        public BulkAggregateMutator(
            IColumnBulkAggregation[] measures, 
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
            this.weights = weights;
            this.incomingData = incomingData;
            this.indices = indices;
            this.computeRanges = computeRanges;
            this.outputColumns = outputColumns;
            this.outputWeights = outputWeights;
            this.outputToTemp = outputToTemp;
            this.isDeleted = isDeleted;
        }

        public void GetSizePrefixSum(ColumnRowReference[] keys, ReadOnlySpan<int> indices, Span<int> sizes)
        {
            throw new NotImplementedException();
        }

        public GenericWriteOperation Process(ColumnRowReference key, bool exists, in ColumnAggregateStateReference existing, ref ColumnAggregateStateReference incoming, int sortedIndex)
        {
            if (exists)
            {
                incoming.weight += existing.weight;
                incoming.valueSent = existing.valueSent;

                bool writeToTemp = false;
                
                for (int i = 0; i < measures.Length; i++)
                {
                    var stateCol = existing.referenceBatch.Columns[i];
                    var colReference = new ColumnReference(stateCol, existing.RowIndex, default);
                    var computeRange = computeRanges[i];
                    var indiceSpan = indices.AsSpan(computeRange.start, computeRange.length);
                    writeToTemp |= measures[i].Compute(indiceSpan, weights, incomingData, colReference);
                }
                
                if (incoming.weight == 0)
                {
                    if (existing.valueSent)
                    {
                        var keylength = key.referenceBatch.Columns.Count;
                        for (int i = 0; i < keylength; i++)
                        {
                            var col = key.referenceBatch.Columns[i];
                            outputColumns[i].InsertRangeFrom(outputColumns[i].Count, col, key.RowIndex, 1);
                        }

                        for (int i = 0; i < measures.Length; i++)
                        {
                            var col = existing.referenceBatch.Columns[measures.Length + i];
                            outputColumns[keylength + i].InsertRangeFrom(outputColumns[keylength + i].Count, col, existing.RowIndex, 1);
                        }
                        outputToTemp[sortedIndex] = false;
                        isDeleted[sortedIndex] = true;
                        outputWeights.Add(-1);
                    }
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
                for (int i = 0; i < measures.Length; i++)
                {
                    var stateCol = incoming.referenceBatch.Columns[i];
                    var colReference = new ColumnReference(stateCol, incoming.RowIndex, default);
                    // Compute should be called here for each row, need alot of extra input here though.
                    var computeRange = computeRanges[sortedIndex];
                    var indiceSpan = indices.AsSpan(computeRange.start, computeRange.length);
                    measures[i].Compute(indiceSpan, weights, incomingData, colReference);
                }
                // New data should always be output no matter what compute says
                outputToTemp[sortedIndex] = true;
            }
            return GenericWriteOperation.Upsert;
        }
    }
}
