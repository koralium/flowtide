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

using FlowtideDotNet.Base.Vertices.MultipleInput;
using FlowtideDotNet.Storage.Serializers;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.Tree;
using FlowtideDotNet.Substrait.Relations;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Core.Operators.Set
{
    internal class SetOperator : MultipleInputVertex<StreamEventBatch, object>
    {
        private readonly SetRelation setRelation;
        
        private readonly List<IBPlusTree<StreamEvent, int>> _storages;
        private readonly Func<int, StreamEventBatch, long, IAsyncEnumerable<StreamEventBatch>> _operation;

        private Counter<long>? _eventsCounter;

#if DEBUG_WRITE
        // TODO: Tmp remove
        private StreamWriter allInput;
        private StreamWriter outputWriter;
#endif

        public override string DisplayName => "Union";

        public SetOperator(SetRelation setRelation, ExecutionDataflowBlockOptions executionDataflowBlockOptions) : base(setRelation.Inputs.Count, executionDataflowBlockOptions)
        {
            this.setRelation = setRelation;
            _storages = new List<IBPlusTree<StreamEvent, int>>();

            if (setRelation.Operation == SetOperation.UnionAll)
            {
                _operation = UnionAll;
            }
            else
            {
                throw new NotImplementedException();
            }
        }

        private static int UnionAllWeightFunction(int weight1, int weight2)
        {
            return Math.Max(weight1, weight2);
        }

        private async IAsyncEnumerable<StreamEventBatch> UnionAll(int targetId, StreamEventBatch msg, long time)
        {
            var storage = _storages[targetId];
            List<StreamEvent> output = new List<StreamEvent>();

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
                    previousWeight = UnionAllWeightFunction(previousWeight, otherWeight);
                    newWeight = UnionAllWeightFunction(newWeight, otherWeight);
                }

                // Check if there is a difference in weight
                if (newWeight != previousWeight)
                {
                    output.Add(new StreamEvent(newWeight - previousWeight, 0, e.Memory));
                }
            }

            if (output.Count > 0)
            {
#if DEBUG_WRITE
                foreach (var o in output)
                {
                    outputWriter.WriteLine($"{o.Weight} {o.Vector.ToJson}");
                }
#endif
                Debug.Assert(_eventsCounter != null, nameof(_eventsCounter));
                _eventsCounter.Add(output.Count);
                yield return new StreamEventBatch(null, output);
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
#if DEBUG_WRITE
            allInput.WriteLine("New batch");
            foreach (var e in msg.Events)
            {
                allInput.WriteLine($"{targetId}, {e.Weight} {e.Vector.ToJson}");
            }
            allInput.Flush();
#endif
            return _operation(targetId, msg, time);
        }

        protected override async Task InitializeOrRestore(object? state, IStateManagerClient stateManagerClient)
        {
#if DEBUG_WRITE
            allInput = File.CreateText($"{Name}.all.txt");
            outputWriter = File.CreateText($"{Name}.output.txt");
#endif

            _eventsCounter = Metrics.CreateCounter<long>("events");
            await InitializeTrees(stateManagerClient);
        }

        private async Task InitializeTrees(IStateManagerClient stateManagerClient)
        {
            for (int i = 0; i < setRelation.Inputs.Count; i++)
            {
                _storages.Add(await stateManagerClient.GetOrCreateTree(i.ToString(), new BPlusTreeOptions<StreamEvent, int>()
                {
                    Comparer = new BPlusTreeStreamEventComparer(),
                    KeySerializer = new BPlusTreeStreamEventSerializer(),
                    ValueSerializer = new IntSerializer()
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
