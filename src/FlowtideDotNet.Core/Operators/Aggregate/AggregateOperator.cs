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

using FlexBuffers;
using FlowtideDotNet.Base;
using FlowtideDotNet.Base.Vertices.Unary;
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Core.Compute.Internal;
using FlowtideDotNet.Core.Operators.Set;
using FlowtideDotNet.Core.Storage;
using FlowtideDotNet.Storage.Serializers;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.Tree;
using FlowtideDotNet.Substrait.Relations;
using System.Buffers;
using System.Threading.Tasks.Dataflow;
using static Substrait.Protobuf.AggregateRel.Types;

namespace FlowtideDotNet.Core.Operators.Aggregate
{
    internal class AggregateOperatorState
    {

    }
    internal class AggregateOperator : UnaryVertex<StreamEventBatch, AggregateOperatorState>
    {
        private static byte[] EmptyVector = FlexBufferBuilder.Vector(b => { });
        private readonly AggregateRelation aggregateRelation;
        private List<Func<StreamEvent, FlxValue>>? groupExpressions;
        private IBPlusTree<StreamEvent, AggregateRowState> _tree;
        private FlexBuffer _flexBufferNewValue;
        private FlexBuffer _flexBufferOldValue;
        private List<Func<StreamEvent, byte[]?, long, byte[]>> _measuresStateUpdate;
        private List<Func<byte[]?, FlxValue>> _measureGetValues;

        /// <summary>
        /// All the mapping functions for the measures.
        /// </summary>
        private List<Func<StreamEvent, byte[]?, int, (FlxValue? oldValue, FlxValue newValue, byte[] newState)>> mappingFunctions;
        public AggregateOperator(AggregateRelation aggregateRelation, FunctionsRegister functionsRegister, ExecutionDataflowBlockOptions executionDataflowBlockOptions) : base(executionDataflowBlockOptions)
        {
            _measuresStateUpdate = new List<Func<StreamEvent, byte[]?, long, byte[]>>();
            _measureGetValues = new List<Func<byte[]?, FlxValue>>();
            _flexBufferNewValue = new FlexBuffer(ArrayPool<byte>.Shared);
            _flexBufferOldValue = new FlexBuffer(ArrayPool<byte>.Shared);
            if (aggregateRelation.Groupings != null && aggregateRelation.Groupings.Count > 0)
            {
                if (aggregateRelation.Groupings.Count > 1)
                {
                    throw new InvalidOperationException("Aggregate operator only supports one grouping set at this point");
                }

                var grouping = aggregateRelation.Groupings[0];

                groupExpressions = new List<Func<StreamEvent, FlxValue>>();
                foreach (var expr in grouping.GroupingExpressions)
                {
                    groupExpressions.Add(ProjectCompiler.Compile(expr, functionsRegister));
                }
            }
            if (aggregateRelation.Measures != null && aggregateRelation.Measures.Count > 0)
            {
                foreach(var measure in aggregateRelation.Measures)
                {
                    var (stateUpdateFunc, getValueFunc) = MeasureCompiler.CompileMeasure(measure.Measure, functionsRegister);
                    _measuresStateUpdate.Add(stateUpdateFunc);
                    _measureGetValues.Add(getValueFunc);
                }
            }

            this.aggregateRelation = aggregateRelation;
        }

        public override string DisplayName => "Aggregation";

        public override Task Compact()
        {
            return Task.CompletedTask;
        }

        public override Task DeleteAsync()
        {
            return Task.CompletedTask;
        }

        public override Task<AggregateOperatorState> OnCheckpoint()
        {
            return Task.FromResult(new AggregateOperatorState());
        }

        protected override async IAsyncEnumerable<StreamEventBatch> OnWatermark(Watermark watermark)
        {
            List<StreamEvent> outputs = new List<StreamEvent>();
            // If no group expressions, then we are just doing a global aggregation, so we fetch the new value and compare it with the old
            if (groupExpressions == null || groupExpressions.Count == 0)
            {
                // TODO: Change to RMW?
                // Might be better to RMW than to fetch and then update.
                await _tree.RMW(new StreamEvent(0, 0, EmptyVector), default, (input, current, exist) =>
                {
                    if (exist)
                    {
                        _flexBufferNewValue.NewObject();
                        var vectorStart = _flexBufferNewValue.StartVector();
                        for (int i = 0; i < current.MeasureStates.Length; i++)
                        {
                            var measureResult = _measureGetValues[i](current.MeasureStates[i]);
                            _flexBufferNewValue.Add(measureResult);
                        }
                        _flexBufferNewValue.EndVector(vectorStart, false, false);
                        var outputData = _flexBufferNewValue.Finish();
                        var outputEvent = new StreamEvent(1, 0, outputData);
                        if (current.PreviousValue != null)
                        {
                            // If the row has changed from previous
                            if (!current.PreviousValue.SequenceEqual(outputData))
                            {
                                var oldEvent = new StreamEvent(-1, 0, current.PreviousValue);
                                outputs.Add(outputEvent);
                                outputs.Add(oldEvent);
                            }
                        }
                        else
                        {
                            outputs.Add(outputEvent);
                        }
                        // Replace the previous value with the new value
                        current.PreviousValue = outputData;
                    }
                    else
                    {
                        current = new AggregateRowState()
                        {
                            MeasureStates = new byte[_measuresStateUpdate.Count][]
                        };
                        _flexBufferNewValue.NewObject();
                        var vectorStart = _flexBufferNewValue.StartVector();
                        for (int i = 0; i < current.MeasureStates.Length; i++)
                        {
                            var measureResult = _measureGetValues[i](current.MeasureStates[i]);
                            _flexBufferNewValue.Add(measureResult);
                        }
                        _flexBufferNewValue.EndVector(vectorStart, false, false);
                        var outputData = _flexBufferNewValue.Finish();
                        current.PreviousValue = outputData;
                        outputs.Add(new StreamEvent(1, 0, outputData));
                    }
                    return (current, GenericWriteOperation.Upsert);
                });
            }

            if (outputs.Count > 0)
            {
                yield return new StreamEventBatch(null, outputs);
            }
        }

        public override async IAsyncEnumerable<StreamEventBatch> OnRecieve(StreamEventBatch msg, long time)
        {
            foreach(var e in msg.Events)
            {
                // Create the key
                StreamEvent? key = default;
                if (groupExpressions != null)
                {
                    _flexBufferNewValue.NewObject();
                    var vectorStart = _flexBufferNewValue.StartVector();
                    foreach (var groupExpr in groupExpressions)
                    {
                        var result = groupExpr(e);
                        _flexBufferNewValue.Add(result);
                    }
                    _flexBufferNewValue.EndVector(vectorStart, false, false);
                    var keyBytes = _flexBufferNewValue.Finish();
                    key = new StreamEvent(e.Weight, 0, keyBytes);
                }
                else
                {
                    key = new StreamEvent(e.Weight, 0, EmptyVector);
                }

                await _tree.RMW(key.Value, default, (_, current, exist) =>
                {
                    // Check if there are measures to update state for, if not
                    // this is just a group by, so we only update weight for this group.
                    // Updating weight is required so we know if this group should be deleted or not.
                    if (_measuresStateUpdate.Count > 0)
                    {
                        if (exist)
                        {
                            for (int i = 0; i < _measuresStateUpdate.Count; i++)
                            {
                                current!.MeasureStates[i] = _measuresStateUpdate[i](e, current!.MeasureStates[i], e.Weight);
                            }
                            current!.Weight += e.Weight;
                        }
                        else
                        {
                            current = new AggregateRowState()
                            {
                                MeasureStates = new byte[_measuresStateUpdate.Count][]
                            };
                            for (int i = 0; i < _measuresStateUpdate.Count; i++)
                            {
                                current.MeasureStates[i] = _measuresStateUpdate[i](e, null, e.Weight);
                            }
                            current.Weight += e.Weight;
                        }
                    }
                    else
                    {
                        if (exist)
                        {
                            current.Weight += e.Weight;
                        }
                        else
                        {
                            current = new AggregateRowState()
                            {
                                Weight = e.Weight
                            };
                        }
                    }
                    
                    return (current, GenericWriteOperation.Upsert);
                });
            }

            yield break;
        }

        protected override async Task InitializeOrRestore(AggregateOperatorState? state, IStateManagerClient stateManagerClient)
        {
            _tree = await stateManagerClient.GetOrCreateTree<StreamEvent, AggregateRowState>("grouping_set_1_v1", new FlowtideDotNet.Storage.Tree.BPlusTreeOptions<StreamEvent, AggregateRowState>()
            {
                KeySerializer = new StreamEventBPlusTreeSerializer(),
                ValueSerializer = new AggregateRowStateSerializer(),
                Comparer = new BPlusTreeStreamEventComparer()
            });
        }
    }
}
