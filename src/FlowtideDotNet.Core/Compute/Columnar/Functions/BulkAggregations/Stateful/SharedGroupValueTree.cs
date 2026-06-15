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
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Tree;
using FlowtideDotNet.Storage.DataStructures;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.Compute.Columnar.Functions.BulkAggregations.Stateful
{
    internal class SharedGroupValueTree
    {
        public string TreeName { get; }
        public Func<EventBatchData, int, IDataValue> ProjectionFunction { get; }

        public IBPlusTree<BulkGroupValueRowReference, int, BulkGroupValueKeyContainer, PrimitiveListValueContainer<int>> Tree { get; set; }
        public IBPlusTreeBulkInserter<BulkGroupValueRowReference, int, BulkGroupValueKeyContainer, PrimitiveListValueContainer<int>> BulkInserter { get; set; }

        public Func<EventBatchData, int, bool>? Filter { get; }
        private Column _projectedDataColumn;
        private readonly List<ISharedTreeColumnAggregation> _boundMeasures = new();

        public SharedGroupValueTree(string treeName, Func<EventBatchData, int, IDataValue> projectionFunction, Func<EventBatchData, int, bool>? filter = null)
        {
            TreeName = treeName;
            ProjectionFunction = projectionFunction;
            Filter = filter;
        }

        public void BindMeasure(ISharedTreeColumnAggregation measure)
        {
            _boundMeasures.Add(measure);
        }

        public void NewBatch(PrimitiveList<int> weights, EventBatchData batchData, IMemoryAllocator memoryAllocator)
        {
            _projectedDataColumn?.Dispose();
            _projectedDataColumn = new Column(memoryAllocator);
            for (int i = 0; i < weights.Count; i++)
            {
                _projectedDataColumn.Add(ProjectionFunction(batchData, i));
            }
        }

        public ValueTask StoreAsync(PrimitiveList<int> weights, IColumn[] groupValueColumns, ReadOnlySpan<int> sortedByGroupIndices, EventBatchData incoming)
        {
            int len = 0;
            if (Filter != null)
            {
                for (int i = 0; i < sortedByGroupIndices.Length; i++)
                {
                    if (Filter(incoming, sortedByGroupIndices[i]))
                    {
                        len++;
                    }
                }
            }
            else
            {
                len = sortedByGroupIndices.Length;
            }

            BulkGroupValueRowReference[] rowReferences = new BulkGroupValueRowReference[len];
            int[] weightArray = new int[len];

            var allColumns = new IColumn[groupValueColumns.Length + 1];
            System.Array.Copy(groupValueColumns, allColumns, groupValueColumns.Length);
            allColumns[groupValueColumns.Length] = _projectedDataColumn;
            var groupingBatch = new EventBatchData(allColumns);

            int writeIndex = 0;
            for (int i = 0; i < sortedByGroupIndices.Length; i++)
            {
                var physicalIndex = sortedByGroupIndices[i];
                if (Filter == null || Filter(incoming, physicalIndex))
                {
                    rowReferences[writeIndex] = new BulkGroupValueRowReference()
                    {
                        batch = groupingBatch,
                        index = physicalIndex
                    };
                    weightArray[writeIndex] = weights.Get(physicalIndex);
                    writeIndex++;
                }
            }

            int totalBatchSize = 0;
            for (int i = 0; i < groupValueColumns.Length; i++)
            {
                totalBatchSize += groupValueColumns[i].GetByteSize();
            }
            totalBatchSize += _projectedDataColumn.GetByteSize();

            if (len > 1)
            {
                var insertComparer = new BulkMinInsertComparer(groupValueColumns.Length);
                System.Array.Sort(rowReferences, weightArray, 0, len, new BulkGroupValueRowReferenceComparer(insertComparer));
            }

            var mutator = new SharedRowMutator(_boundMeasures);
            return BulkInserter.ApplyBatch(rowReferences, weightArray, len, mutator, totalBatchSize);
        }
    }

    internal class BulkGroupValueRowReferenceComparer : IComparer<BulkGroupValueRowReference>
    {
        private readonly BulkMinInsertComparer _comparer;
        public BulkGroupValueRowReferenceComparer(BulkMinInsertComparer comparer)
        {
            _comparer = comparer;
        }
        public int Compare(BulkGroupValueRowReference x, BulkGroupValueRowReference y)
        {
            return _comparer.CompareTo(x, y);
        }
    }

    internal struct SharedRowMutator : IRowMutator<BulkGroupValueRowReference, int>
    {
        private readonly List<ISharedTreeColumnAggregation> _boundMeasures;

        public SharedRowMutator(List<ISharedTreeColumnAggregation> boundMeasures)
        {
            _boundMeasures = boundMeasures;
        }

        public void GetSizePrefixSum(BulkGroupValueRowReference[] keys, ReadOnlySpan<int> indices, Span<int> sizes)
        {
        }

        public GenericWriteOperation Process(BulkGroupValueRowReference key, bool exists, in int existing, ref int incoming, int sortedIndex)
        {
            int oldWeight = exists ? existing : 0;
            int newWeight = incoming;
            if (exists)
            {
                newWeight += existing;
            }

            // Notify each bound measure of the mutation (insertion, update, or deletion) with its unique sortedIndex
            for (int i = 0; i < _boundMeasures.Count; i++)
            {
                _boundMeasures[i].OnValueMutated(key, exists, oldWeight, newWeight, sortedIndex);
            }

            if (newWeight == 0)
            {
                return GenericWriteOperation.Delete;
            }
            incoming = newWeight;
            return GenericWriteOperation.Upsert;
        }
    }
}
