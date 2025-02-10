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
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Core.Operators.Set.Structs;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Serializers;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.Tree;
using FlowtideDotNet.Substrait.Relations;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Core.Operators.Set
{
    internal class ColumnSetOperator<TStruct> : MultipleInputVertex<StreamEventBatch>
        where TStruct: unmanaged, IInputWeight
    {
        private readonly SetRelation _setRelation;
        private IBPlusTree<ColumnRowReference, TStruct, ColumnKeyStorageContainer, PrimitiveListValueContainer<TStruct>>? _tree;
        private Func<TStruct, int> _weightCalculator;
        private int[] _emit;
        private int _inputLength;
        private readonly string _displayName;

        // Metrics
        private ICounter<long>? _eventsCounter;
        private ICounter<long>? _eventsProcessed;

#if DEBUG_WRITE
        private StreamWriter? allInput;
        private StreamWriter? outputWriter;
#endif

        public ColumnSetOperator(SetRelation setRelation, ExecutionDataflowBlockOptions executionDataflowBlockOptions) : base(setRelation.Inputs.Count, executionDataflowBlockOptions)
        {
            this._setRelation = setRelation;

            if (setRelation.Operation == SetOperation.UnionAll)
            {
                throw new NotSupportedException("UnionAllSetOperator should be used for UnionAll.");
            }

            if (setRelation.Inputs.Count <= 0)
            {
                throw new ArgumentException("Set operator must have at least one input");
            }

            _inputLength = setRelation.Inputs[0].OutputLength;
            for (int i = 0; i < setRelation.Inputs.Count; i++)
            {
                if (setRelation.Inputs[i].OutputLength != _inputLength)
                {
                    throw new ArgumentException("All inputs must have the same length");
                }
            }

            if (setRelation.Operation == SetOperation.UnionDistinct)
            {
                _weightCalculator = UnionDistinctWeightFunction;
                _displayName = "Set(UnionDistinct)";
            }
            else if (setRelation.Operation == SetOperation.MinusPrimary)
            {
                _weightCalculator = MinusPrimaryWeightFunction;
                _displayName = "Set(MinusPrimary)";
            }
            else if (setRelation.Operation == SetOperation.MinusPrimaryAll)
            {
                _weightCalculator = MinusPrimaryAllWeightFunction;
                _displayName = "Set(MinusPrimaryAll)";
            }
            else if (setRelation.Operation == SetOperation.IntersectionMultiset)
            {
                _weightCalculator = IntersectMultisetWeightFunction;
                _displayName = "Set(IntersectMultiset)";
            }
            else if (setRelation.Operation == SetOperation.IntersectionMultisetAll)
            {
                _weightCalculator = IntersectMultisetAllWeightFunction;
                _displayName = "Set(IntersectMultisetAll)";
            }
            else
            {
                throw new NotSupportedException();
            }
            if (setRelation.EmitSet)
            {
                _emit = setRelation.Emit.ToArray();
            }
            else
            {
                _emit = new int[setRelation.OutputLength];

                for (int i = 0; i < setRelation.OutputLength; i++)
                {
                    _emit[i] = i;
                }
            }
        }

        private static int UnionDistinctWeightFunction(TStruct weights)
        {
            var weight = weights.GetValue(0);
            for (int i = 1; i < weights.Count; i++)
            {
                weight = Math.Max(weight, weights.GetValue(i));
            }
            if (weight > 1)
            {
                return 1;
            }
            return weight;
        }

        private static int MinusPrimaryWeightFunction(TStruct weights)
        {
            var primaryWeight = Math.Min(weights.GetValue(0), 1);
            for (int i = 1; i < weights.Count; i++)
            {
                if (weights.GetValue(i) > 0)
                {
                    primaryWeight = 0;
                }
            }
            return primaryWeight;
        }

        private static int MinusPrimaryAllWeightFunction(TStruct weights)
        {
            var primaryWeight = weights.GetValue(0);
            for (int i = 1; i < weights.Count; i++)
            {
                primaryWeight -= weights.GetValue(i);
            }
            return primaryWeight;
        }

        private static int IntersectMultisetWeightFunction(TStruct weights)
        {
            var weight = Math.Min(weights.GetValue(0), 1);
            for (int i = 1; i < weights.Count; i++)
            {
                weight = Math.Min(weight, weights.GetValue(i));
            }
            return weight;
        }

        private static int IntersectMultisetAllWeightFunction(TStruct weights)
        {
            var weight = weights.GetValue(0);
            for (int i = 1; i < weights.Count; i++)
            {
                weight = Math.Min(weight, weights.GetValue(i));
            }
            return weight;
        }

        public override string DisplayName => _displayName;

        public override Task Compact()
        {
            return Task.CompletedTask;
        }

        public override Task DeleteAsync()
        {
            return Task.CompletedTask;
        }

        public override async Task OnCheckpoint()
        {
            Debug.Assert(_tree != null);

            await _tree.Commit().ConfigureAwait(false);
        }

        public override async IAsyncEnumerable<StreamEventBatch> OnRecieve(int targetId, StreamEventBatch msg, long time)
        {
            Debug.Assert(_tree != null);
            Debug.Assert(_eventsCounter != null);
            Debug.Assert(_eventsProcessed != null);

            PrimitiveList<int> foundOffsets = new PrimitiveList<int>(MemoryAllocator);
            PrimitiveList<int> outputWeights = new PrimitiveList<int>(MemoryAllocator);
            PrimitiveList<uint> iterations = new PrimitiveList<uint>(MemoryAllocator);

            var weights = msg.Data.Weights;
            _eventsCounter.Add(weights.Count);
            for (int i = 0; i < weights.Count; i++)
            {
                var inputWeight = new TStruct();
                inputWeight.SetValue(targetId, weights[i]);
                var rowRef = new ColumnRowReference() { referenceBatch = msg.Data.EventBatchData, RowIndex = i };

#if DEBUG_WRITE
                Debug.Assert(allInput != null);
                allInput.WriteLine($"{targetId}, {weights[i]} {rowRef}");
#endif

                await _tree.RMWNoResult(rowRef, inputWeight, (input, current, exists) =>
                {
                    if (exists)
                    {
                        var previousWeight = _weightCalculator(current);
                        InputWeightExtensions.Add(ref current, input);
                        var newWeight = _weightCalculator(current);

                        var difference = newWeight - previousWeight;
                        if (difference != 0)
                        {
                            outputWeights.Add(difference);
                            foundOffsets.Add(i);
                            iterations.Add(msg.Data.Iterations[i]);
                        }

                        if (current.IsAllZero())
                        {
                            return (current, GenericWriteOperation.Delete);
                        }
                        return (current, GenericWriteOperation.Upsert);
                    }
                    var weight = _weightCalculator(input);
                    if (weight != 0)
                    {
                        iterations.Add(msg.Data.Iterations[i]);
                        outputWeights.Add(weight);
                        foundOffsets.Add(i);
                    }
                    return (input, GenericWriteOperation.Upsert);
                });
            }

            if (outputWeights.Count > 0)
            {
                IColumn[] outputColumns = new IColumn[_emit.Length];
                for (int i = 0; i < _emit.Length; i++)
                {
                    outputColumns[i] = new ColumnWithOffset(msg.Data.EventBatchData.Columns[_emit[i]], foundOffsets, false);
                }
                var batch = new EventBatchData(outputColumns);
                _eventsProcessed.Add(outputWeights.Count);
#if DEBUG_WRITE
                Debug.Assert(outputWriter != null);
                for (int i = 0; i < outputWeights.Count; i++)
                {
                    var rowRef = new ColumnRowReference() { referenceBatch = batch, RowIndex = i };
                    outputWriter.WriteLine($"{outputWeights[i]} {rowRef}");
                }
                outputWriter.Flush();
#endif

                yield return new StreamEventBatch(new EventBatchWeighted(outputWeights, iterations, new EventBatchData(outputColumns)));
            }
#if DEBUG_WRITE
            Debug.Assert(allInput != null);
            allInput.Flush();
#endif
        }

        protected override async Task InitializeOrRestore(IStateManagerClient stateManagerClient)
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

            _tree = await stateManagerClient.GetOrCreateTree("tree", new BPlusTreeOptions<ColumnRowReference, TStruct, ColumnKeyStorageContainer, PrimitiveListValueContainer<TStruct>>()
            {
                Comparer = new ColumnComparer(_inputLength),
                KeySerializer = new ColumnStoreSerializer(_inputLength, MemoryAllocator),
                ValueSerializer = new PrimitiveListValueContainerSerializer<TStruct>(MemoryAllocator),
                MemoryAllocator = MemoryAllocator
            });
        }
    }
}
