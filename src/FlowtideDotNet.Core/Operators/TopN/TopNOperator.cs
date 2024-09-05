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
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Core.Compute.Internal;
using FlowtideDotNet.Core.Storage;
using FlowtideDotNet.Storage.Serializers;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.Tree;
using FlowtideDotNet.Substrait.Relations;
using System.Diagnostics;
using System.Diagnostics.Tracing;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Core.Operators.TopN
{
    internal class TopNOperator : UnaryVertex<StreamEventBatch, object?>
    {
        private readonly TopNComparer _comparer;
        private readonly TopNRelation relation;
        private IBPlusTree<RowEvent, int, ListKeyContainer<RowEvent>, ListValueContainer<int>>? _tree;
        private ICounter<long>? _eventsOutCounter;
        private ICounter<long>? _eventsProcessed;

        public TopNOperator(TopNRelation relation, FunctionsRegister functionsRegister, ExecutionDataflowBlockOptions executionDataflowBlockOptions) : base(executionDataflowBlockOptions)
        {
            var compareFunc = SortFieldCompareCreator.CreateComparer<RowEvent>(relation.Sorts, functionsRegister);
            _comparer = new TopNComparer(compareFunc);
            this.relation = relation;
        }

        public override string DisplayName => $"Top ({relation.Count})";

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
            _eventsProcessed.Add(msg.Events.Count);

            var iterator = _tree.CreateIterator();
            List<RowEvent> output = new List<RowEvent>();
            foreach(var e in msg.Events)
            {
                // Insert the value into the tree
                var (op, _) = await _tree.RMW(e, e.Weight, (input, current, exists) =>
                {
                    if (exists)
                    {
                        var newWeight = current + input;
                        if (newWeight == 0)
                        {
                            return (0, GenericWriteOperation.Delete);
                        }
                        return (current + e.Weight, GenericWriteOperation.Upsert);
                    }
                    else
                    {
                        return (input, GenericWriteOperation.Upsert);
                    }
                });

                // Iterate over the tree, find the Nth value, check if this value is greater or smaller than that
                await GetOutputValues(e, output, iterator, op);
            }

            if (output.Count > 0)
            {
                Debug.Assert(_eventsOutCounter != null, nameof(_eventsOutCounter));
                _eventsOutCounter.Add(output.Count);
                yield return new StreamEventBatch(output, relation.OutputLength);
            }
        }

        private async Task GetOutputValues(RowEvent ev, List<RowEvent> output, IBPlusTreeIterator<RowEvent, int, ListKeyContainer<RowEvent>, ListValueContainer<int>> iterator, GenericWriteOperation op)
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
                var index = page.Keys.BinarySearch(ev, _comparer);
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
                    if (count >= relation.Count)
                    {
                        break;
                    }
                }
                // Check if all N rows have already been accounted, then no output will be given.
                if (count >= relation.Count)
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
                        if ((count + page.Values.Get(index) - ev.Weight) >= relation.Count)
                        {
                            // Break and do nothing, the count is already satisfied.
                            break;
                        }
                        var outputWeight = Math.Min(relation.Count - count, ev.Weight);
                        output.Add(new RowEvent(outputWeight, 0, ev.RowData));
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
                    output.Add(ev);
                    bumpCount = ev.Weight * -1;
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
                var stopCount = relation.Count - bumpCount;
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
                            output.Add(new RowEvent(weightToRemove * bumpWeightModifier, 0, page.Keys.Get(i).RowData));
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
            // Create tree that will hold all rows
            _tree = await stateManagerClient.GetOrCreateTree("topn", 
                new FlowtideDotNet.Storage.Tree.BPlusTreeOptions<RowEvent, int, ListKeyContainer<RowEvent>, ListValueContainer<int>>()
            {
                Comparer = new BPlusTreeListComparer<RowEvent>(_comparer),
                KeySerializer = new KeyListSerializer<RowEvent>(new StreamEventBPlusTreeSerializer()),
                ValueSerializer = new ValueListSerializer<int>(new IntSerializer())
            });
        }
    }
}
