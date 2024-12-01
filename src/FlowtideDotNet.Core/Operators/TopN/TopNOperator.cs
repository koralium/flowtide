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

using FlowtideDotNet.Base.Metrics;
using FlowtideDotNet.Base.Vertices.Unary;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Core.Compute.Columnar;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Serializers;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.Tree;
using FlowtideDotNet.Substrait.Relations;
using System.Diagnostics;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Core.Operators.TopN
{
    internal class TopNOperator : UnaryVertex<StreamEventBatch, object?>
    {
        private readonly TopNRelation _relation;
        private readonly TopNComparer _sortComparer;
        private readonly int _outputLength;
        private IBPlusTree<ColumnRowReference, int, ColumnKeyStorageContainer, PrimitiveListValueContainer<int>>? _tree;

        private ICounter<long>? _eventsOutCounter;
        private ICounter<long>? _eventsProcessed;

        public TopNOperator(TopNRelation relation, IFunctionsRegister functionsRegister, ExecutionDataflowBlockOptions executionDataflowBlockOptions) : base(executionDataflowBlockOptions)
        {
            _outputLength = relation.OutputLength;
            _sortComparer = new TopNComparer(SortFieldCompareCompiler.CreateComparer(relation.Sorts, functionsRegister));
            this._relation = relation;
        }

        public override string DisplayName => $"Top ({_relation.Count})";

        public override Task Compact()
        {
            return Task.CompletedTask;
        }

        public override Task DeleteAsync()
        {
            return Task.CompletedTask;
        }

        public override async Task<object?> OnCheckpoint()
        {
            Debug.Assert(_tree != null);
            await _tree.Commit();
            return default;
        }

        public override async IAsyncEnumerable<StreamEventBatch> OnRecieve(StreamEventBatch msg, long time)
        {
            Debug.Assert(_tree != null);
            Debug.Assert(_eventsProcessed != null);
            Debug.Assert(_eventsOutCounter != null, nameof(_eventsOutCounter));

            var inputWeights = msg.Data.Weights;
            var inputBatch = msg.Data.EventBatchData;

            _eventsProcessed.Add(inputWeights.Count);

            PrimitiveList<int> inBatchWeights = new PrimitiveList<int>(MemoryAllocator);
            PrimitiveList<uint> inBatchIterations = new PrimitiveList<uint>(MemoryAllocator);
            PrimitiveList<int> foundOffsets = new PrimitiveList<int>(MemoryAllocator);

            PrimitiveList<int> deleteWeights = new PrimitiveList<int>(MemoryAllocator);
            PrimitiveList<uint> deleteIterations = new PrimitiveList<uint>(MemoryAllocator);

            IColumn[] deleteColumns = new IColumn[_outputLength];
            for (int i = 0; i < _outputLength; i++)
            {
                deleteColumns[i] = Column.Create(MemoryAllocator);
            }

            using var iterator = _tree.CreateIterator();

            for (int i = 0; i < inputWeights.Count; i++)
            {
                var rowReference = inputBatch.GetRowReference(in i);
                var op = await _tree.RMWNoResult(rowReference, inputWeights[i], (input, current, exist) =>
                {
                    if (exist)
                    {
                        var newWeight = current + input;
                        if (newWeight == 0)
                        {
                            return (0, GenericWriteOperation.Delete);
                        }
                        return (newWeight, GenericWriteOperation.Upsert);
                    }
                    else
                    {
                        return (input, GenericWriteOperation.Upsert);
                    }
                });

                await GetOutputValues(
                    inputWeights[i],
                    rowReference,
                    inBatchWeights,
                    inBatchIterations,
                    foundOffsets,
                    deleteWeights,
                    deleteIterations,
                    deleteColumns,
                    iterator,
                    op
                    );
            }

            if (foundOffsets.Count > 0)
            {
                _eventsOutCounter.Add(foundOffsets.Count);

                IColumn[] outputColumns = new IColumn[_outputLength];

                for (int i = 0; i < outputColumns.Length; i++)
                {
                    outputColumns[i] = new ColumnWithOffset(inputBatch.Columns[i], foundOffsets, false);
                }

                yield return new StreamEventBatch(new EventBatchWeighted(inBatchWeights, inBatchIterations, new EventBatchData(outputColumns)));
            }
            else
            {
                foundOffsets.Dispose();
                inBatchIterations.Dispose();
                inBatchWeights.Dispose();
            }

            if (deleteWeights.Count > 0)
            {
                _eventsOutCounter.Add(deleteWeights.Count);
                yield return new StreamEventBatch(new EventBatchWeighted(deleteWeights, deleteIterations, new EventBatchData(deleteColumns)));
            }
            else
            {
                deleteWeights.Dispose();
                deleteIterations.Dispose();
                for (int i = 0; i < deleteColumns.Length; i++)
                {
                    deleteColumns[i].Dispose();
                }
            }
        }

        private async Task GetOutputValues(
            int inputWeight,
            ColumnRowReference ev, 
            PrimitiveList<int> inBatchWeights, 
            PrimitiveList<uint> inBatchIterations,
            PrimitiveList<int> foundOffsets,
            PrimitiveList<int> deleteWeights,
            PrimitiveList<uint> deleteIterations,
            IColumn[] deleteBatchColumns,
            IBPlusTreeIterator<ColumnRowReference, int, ColumnKeyStorageContainer, PrimitiveListValueContainer<int>> iterator, 
            GenericWriteOperation op)
        {
            await iterator.SeekFirst();
            int count = 0;
            var enumerator = iterator.GetAsyncEnumerator();
            int bumpCount = -1;
            int bumpWeightModifier = -1;
            int pageIndex = -1;

            while (await enumerator.MoveNextAsync())
            {
                var page = enumerator.Current;
                var dataBatch = page.Keys._data;
                
                var index = _sortComparer.FindIndex(ev, page.Keys);
                var loopEndIndex = index;
                if (loopEndIndex < 0)
                {
                    loopEndIndex = ~loopEndIndex;
                }
                // Loop through all elements to get the count
                for (int i = 0; i < loopEndIndex; i++)
                {
                    // Count all the weights in the page, since one row could have many duplicates
                    count += page.Values.Get(i);
                    if (count >= _relation.Count)
                    {
                        break;
                    }
                }
                // Check if all N rows have already been accounted, then no output will be given.
                if (count >= _relation.Count)
                {
                    break;
                }
                // Check if we did not find the row we inserted, if so, continue to the next page
                if (index < 0 && loopEndIndex == page.Values.Count)
                {
                    continue;
                }

                // If we reached here, the row was found and it should output values.

                // Set the index where the element is
                pageIndex = loopEndIndex;
                // If it is an upsert, output the event
                if (op == GenericWriteOperation.Upsert)
                {
                    if (index >= 0)
                    {
                        // Check if this value already outputs enough rows to satisfy the count
                        if ((count + page.Values.Get(index) - inputWeight) >= _relation.Count)
                        {
                            // Break and do nothing, the count is already satisfied.
                            break;
                        }
                        var outputWeight = Math.Min(_relation.Count - count, inputWeight);

                        // Add to output, found offsets is used for rows sent in, this allows zero copy
                        foundOffsets.Add(ev.RowIndex);
                        inBatchWeights.Add(outputWeight);
                        inBatchIterations.Add(0);
                        bumpCount = outputWeight;
                        bumpWeightModifier = -1;
                        break;
                    }
                    else
                    {
                        throw new InvalidOperationException("Got an upsert for a value that does not exist in the tree");
                    }
                }
                else if (op == GenericWriteOperation.Delete)
                {
                    foundOffsets.Add(ev.RowIndex);
                    inBatchWeights.Add(inputWeight);
                    inBatchIterations.Add(0);
                    //output.Add(ev);
                    bumpCount = inputWeight * -1;
                    bumpWeightModifier = 1;
                    break;
                }
                else
                {
                    throw new NotSupportedException();
                }
            }

            if (bumpCount > 0)
            {
                var stopCount = _relation.Count - bumpCount;
                if (bumpWeightModifier < 0)
                {
                    // if we should remove elements, we look at an element infront of the count.
                    // If it is a delete, the element has already been removed from the tree, so we should not add with 1.
                    stopCount += 1;
                }
                // Iterate until the stop count where we should start adding or removing events from the output.
                int bumpStartIndex = -1;
                do
                {
                    var page = enumerator.Current;
                    for (int i = pageIndex; i < page.Values.Count; i++)
                    {
                        count += page.Values.Get(i);
                        if (count > stopCount)
                        {
                            bumpStartIndex = i;
                            break;
                        }
                    }
                    if (count > stopCount)
                    {
                        break;
                    }
                    pageIndex = 0;
                } while (await enumerator.MoveNextAsync());

                // Check if we have an index where we should start bumping from
                if (bumpStartIndex >= 0)
                {
                    var page = enumerator.Current;
                    while (bumpCount > 0)
                    {
                        for (int i = bumpStartIndex; i < page.Values.Count; i++)
                        {
                            // Take the min value of the bump count and the weight of the row
                            var weightToRemove = Math.Min(bumpCount, page.Values.Get(i));


                            deleteWeights.Add(weightToRemove * bumpWeightModifier);
                            deleteIterations.Add(0);

                            for (int columnIndex = 0; columnIndex < _outputLength; columnIndex++)
                            {
                                deleteBatchColumns[columnIndex].InsertRangeFrom(deleteBatchColumns[columnIndex].Count, page.Keys._data.Columns[columnIndex], i, 1);
                            }

                            bumpCount -= weightToRemove;
                            if (bumpCount == 0)
                            {
                                break;
                            }
                        }
                        if (bumpCount == 0)
                        {
                            break;
                        }
                        if (await enumerator.MoveNextAsync())
                        {
                            page = enumerator.Current;
                            bumpStartIndex = 0;
                        }
                        else
                        {
                            break;
                        }
                    }
                }
            }
        }


        protected override async Task InitializeOrRestore(object? state, IStateManagerClient stateManagerClient)
        {
            if (_eventsOutCounter == null)
            {
                _eventsOutCounter = Metrics.CreateCounter<long>("events");
            }
            if (_eventsProcessed == null)
            {
                _eventsProcessed = Metrics.CreateCounter<long>("events_processed");
            }

            _tree = await stateManagerClient.GetOrCreateTree("tree", new BPlusTreeOptions<ColumnRowReference, int, ColumnKeyStorageContainer, PrimitiveListValueContainer<int>>()
            {
                KeySerializer = new ColumnStoreSerializer(_relation.Input.OutputLength, MemoryAllocator),
                MemoryAllocator = MemoryAllocator,
                ValueSerializer = new PrimitiveListValueContainerSerializer<int>(MemoryAllocator),
                Comparer = _sortComparer,
                UseByteBasedPageSizes = true
            });
        }
    }
}
