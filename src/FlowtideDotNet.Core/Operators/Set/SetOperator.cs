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
using FlowtideDotNet.Base.Vertices.MultipleInput;
using FlowtideDotNet.Core.Storage;
using FlowtideDotNet.Storage.Serializers;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.Tree;
using FlowtideDotNet.Substrait.Relations;
using System.Diagnostics;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Core.Operators.Set
{
    internal class SetOperator : MultipleInputVertex<StreamEventBatch, object>
    {
        private readonly SetRelation setRelation;
        
        private readonly List<IBPlusTree<RowEvent, int, ListKeyContainer<RowEvent>, ListValueContainer<int>>> _storages;
        private readonly Func<int, StreamEventBatch, long, IAsyncEnumerable<StreamEventBatch>> _operation;
        private readonly Func<int, int, int> _unionWeightFunction;

        private ICounter<long>? _eventsCounter;
        private ICounter<long>? _eventsProcessed;

#if DEBUG_WRITE
        // TODO: Tmp remove
        private StreamWriter allInput;
        private StreamWriter outputWriter;
#endif

        public override string DisplayName => "Union";

        public SetOperator(SetRelation setRelation, ExecutionDataflowBlockOptions executionDataflowBlockOptions) : base(setRelation.Inputs.Count, executionDataflowBlockOptions)
        {
            this.setRelation = setRelation;
            _storages = new List<IBPlusTree<RowEvent, int, ListKeyContainer<RowEvent>, ListValueContainer<int>>>();

            if (setRelation.Operation == SetOperation.UnionAll)
            {
                _operation = UnionAll;
                _unionWeightFunction = UnionAllWeightFunction;
            }
            else if (setRelation.Operation == SetOperation.UnionDistinct)
            {
                _operation = UnionAll;
                _unionWeightFunction = UnionDistinctWeightFunction;
            }
            else
            {
                throw new NotImplementedException();
            }
        }

        private IRowData CreateOutputRowData(IRowData row)
        {
            if (setRelation.EmitSet)
            {
                return ArrayRowData.Create(row, setRelation.Emit);
            }
            return row;
        }

        private static int UnionAllWeightFunction(int weight1, int weight2)
        {
            return Math.Max(weight1, weight2);
        }

        private static int UnionDistinctWeightFunction(int weight1, int weight2)
        {
            var weight = Math.Max(weight1, weight2);
            if (weight > 1)
            {
                return 1;
            }
            return weight;
        }

        private async IAsyncEnumerable<StreamEventBatch> UnionAll(int targetId, StreamEventBatch msg, long time)
        {
            var storage = _storages[targetId];
            List<RowEvent> output = new List<RowEvent>();

            foreach (var e in msg.Events)
            {
                var newWeightResult = await storage.RMW(e, e.Weight, (input, current, found) =>
                {
                    if (found)
                    {
                        current += input;
                        if (current == 0)
                        {
                            return (0, GenericWriteOperation.Delete);
                        }
                        return (current, GenericWriteOperation.Upsert);
                    }
                    return (input, GenericWriteOperation.Upsert);
                });

                var newWeight = newWeightResult.result;
                var previousWeight = newWeight - e.Weight;

                // Check all other inputs
                for (int i = 0; i < _storages.Count; i++)
                {
                    if (i == targetId)
                    {
                        continue;
                    }

                    var otherWeightResult = await _storages[i].GetValue(e);

                    var otherWeight = 0;
                    if (otherWeightResult.found)
                    {
                        otherWeight = otherWeightResult.value;
                    }
                    // Do weight operation
                    previousWeight = _unionWeightFunction(previousWeight, otherWeight);
                    newWeight = _unionWeightFunction(newWeight, otherWeight);
                }

                if (_storages.Count == 1)
                {
                    newWeight = _unionWeightFunction(newWeight, newWeight);
                    previousWeight = _unionWeightFunction(previousWeight, previousWeight);
                }

                // Check if there is a difference in weight
                if (newWeight != previousWeight)
                {
                    output.Add(new RowEvent(newWeight - previousWeight, e.Iteration, CreateOutputRowData(e.RowData)));
                }
            }

            if (output.Count > 0)
            {
#if DEBUG_WRITE
                foreach (var o in output)
                {
                    outputWriter.WriteLine($"{o.Weight} {o.ToJson()}");
                }
#endif
                Debug.Assert(_eventsCounter != null, nameof(_eventsCounter));
                _eventsCounter.Add(output.Count);
                yield return new StreamEventBatch(output);
            }
#if DEBUG_WRITE
            await outputWriter.FlushAsync();
#endif
        }

        public override Task Compact()
        {
            return Task.CompletedTask;
        }

        public override async Task<object?> OnCheckpoint()
        {
#if DEBUG_WRITE
            allInput.WriteLine("Checkpoint");
            allInput.Flush();
#endif
            foreach (var storage in _storages)
            {
                await storage.Commit();
            }
            return new object();
        }

        public override IAsyncEnumerable<StreamEventBatch> OnRecieve(int targetId, StreamEventBatch msg, long time)
        {
            Debug.Assert(_eventsProcessed != null, nameof(_eventsProcessed));
            _eventsProcessed.Add(msg.Events.Count);
#if DEBUG_WRITE
            allInput.WriteLine("New batch");
            foreach (var e in msg.Events)
            {
                allInput.WriteLine($"{targetId}, {e.Weight} {e.ToJson()}");
            }
            allInput.Flush();
#endif
            return _operation(targetId, msg, time);
        }

        protected override async Task InitializeOrRestore(object? state, IStateManagerClient stateManagerClient)
        {
#if DEBUG_WRITE
            if (!Directory.Exists("debugwrite"))
            {
                Directory.CreateDirectory("debugwrite");
            }
            if (allInput == null)
            {
                allInput = File.CreateText($"debugwrite/{StreamName}_{Name}.all.txt");
                outputWriter = File.CreateText($"debugwrite/{StreamName}_{Name}.output.txt");
            }
            else
            {
                allInput.WriteLine("Restart");
                allInput.Flush();
            }
            
#endif
            if (_eventsCounter == null)
            {
                _eventsCounter = Metrics.CreateCounter<long>("events");
            }
            if (_eventsProcessed == null)
            {
                _eventsProcessed = Metrics.CreateCounter<long>("events_processed");
            }
            
            await InitializeTrees(stateManagerClient);
        }

        private async Task InitializeTrees(IStateManagerClient stateManagerClient)
        {
            _storages.Clear();
            for (int i = 0; i < setRelation.Inputs.Count; i++)
            {
                _storages.Add(await stateManagerClient.GetOrCreateTree(i.ToString(), 
                    new BPlusTreeOptions<RowEvent, int, ListKeyContainer<RowEvent>, ListValueContainer<int>>()
                {
                    Comparer = new BPlusTreeListComparer<RowEvent>(new BPlusTreeStreamEventComparer()),
                    KeySerializer = new KeyListSerializer<RowEvent>(new StreamEventBPlusTreeSerializer()),
                    ValueSerializer = new ValueListSerializer<int>(new IntSerializer())
                }));
                
            }
        }

        public override ValueTask DisposeAsync()
        {
            return base.DisposeAsync();
        }

        public override Task DeleteAsync()
        {
            return Task.CompletedTask;
        }
    }
}
