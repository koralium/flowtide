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

using FlowtideDotNet.Base.Vertices.Unary;
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Core.Compute.Internal;
using FlowtideDotNet.Core.Storage;
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
        private readonly TopNComparer _comparer;
        private readonly TopNRelation relation;
        private IBPlusTree<RowEvent, int>? _tree;
        public TopNOperator(TopNRelation relation, FunctionsRegister functionsRegister, ExecutionDataflowBlockOptions executionDataflowBlockOptions) : base(executionDataflowBlockOptions)
        {
            var compareFunc = SortFieldCompareCreator.CreateComparer<RowEvent>(relation.Sorts, functionsRegister);
            _comparer = new TopNComparer(compareFunc);
            this.relation = relation;
        }

        public override string DisplayName => "TopN";

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
            var iterator = _tree.CreateIterator();
            List<RowEvent> output = new List<RowEvent>();
            foreach(var e in msg.Events)
            {
                // Insert the value into the tree
                bool valueExists = false;
                var (op, val) = await _tree.RMW(e, e.Weight, (input, current, exists) =>
                {
                    if (exists)
                    {
                        valueExists = exists;
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
                await GetOutputValues(e, output, iterator, op, valueExists);
            }

            yield return new StreamEventBatch(output);
        }

        private async Task GetOutputValues(RowEvent ev, List<RowEvent> output, IBPlusTreeIterator<RowEvent, int> iterator, GenericWriteOperation op, bool valueExists)
        {
            await iterator.SeekFirst();
            int count = 0;
            var enumerator = iterator.GetAsyncEnumerator();
            bool findBumpedValue = false;
            int bumpWeightModifier = -1;
            while (await enumerator.MoveNextAsync())
            {
                var page = enumerator.Current;
                count += page.Keys.Count;
                if (count >= relation.Count)
                {
                    var index = page.Keys.Count - (count - relation.Count) - 1;
                    var key = page.Keys[index];
                    var compareVal = _comparer.Compare(ev, key);
                    if (compareVal <= 0)
                    {
                        if (op == GenericWriteOperation.Upsert)
                        {
                            if (valueExists)
                            {
                                // Value already exists, no value will be bumped out
                                output.Add(ev);
                                break;
                            }
                            else
                            {
                                output.Add(ev);
                                // Need to check if any value need to be removed from the output.
                                findBumpedValue = true;
                                bumpWeightModifier = -1;
                                break;
                            }
                        }
                        else if (op == GenericWriteOperation.Delete)
                        {
                            output.Add(ev);
                            findBumpedValue = true;
                            bumpWeightModifier = 1;
                        }
                        else
                        {
                            throw new NotSupportedException();
                        }
                    }
                    // If it is above the Nth value, then we do nothing
                    break;
                }
            }

            // Could not find the Nth value, the current output count is smaller than the limit
            // Add the value to the output
            if (count < relation.Count)
            {
                output.Add(ev);
            }
            if (findBumpedValue)
            {
                // Check if the bumped value already is in the loaded page
                if (count > relation.Count)
                {
                    var index = enumerator.Current.Keys.Count - (count - relation.Count);
                    output.Add(new RowEvent(enumerator.Current.Values[index] * bumpWeightModifier, 0, enumerator.Current.Keys[index].RowData));
                }
                else
                {
                    // Need to load the next page
                    if (await enumerator.MoveNextAsync())
                    {
                        var index = 0;
                        output.Add(new RowEvent(enumerator.Current.Values[index] * bumpWeightModifier, 0, enumerator.Current.Keys[index].RowData));
                    }
                }
            }
        }

        protected override async Task InitializeOrRestore(object? state, IStateManagerClient stateManagerClient)
        {
            // Create tree that will hold all rows
            _tree = await stateManagerClient.GetOrCreateTree("topn", new FlowtideDotNet.Storage.Tree.BPlusTreeOptions<RowEvent, int>()
            {
                Comparer = _comparer,
                KeySerializer = new StreamEventBPlusTreeSerializer(),
                ValueSerializer = new IntSerializer()
            });
        }
    }
}
