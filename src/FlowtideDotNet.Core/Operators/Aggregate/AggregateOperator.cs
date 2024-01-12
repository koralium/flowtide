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
using FlowtideDotNet.Base.Metrics;
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
using System.Diagnostics;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Core.Operators.Aggregate
{
    internal class AggregateOperatorState
    {

    }
    internal class AggregateOperator : UnaryVertex<StreamEventBatch, AggregateOperatorState>
    {
        private static byte[] EmptyVector = FlexBufferBuilder.Vector(b => { });
        private readonly AggregateRelation aggregateRelation;
        private readonly FunctionsRegister functionsRegister;
        private List<Func<RowEvent, FlxValue>>? groupExpressions;
        private IBPlusTree<RowEvent, AggregateRowState>? _tree;
        private IBPlusTree<RowEvent, int>? _temporaryTree;
        private FlexBuffer _flexBufferNewValue;
        private List<IAggregateContainer> _measures;
        private ICounter<long>? _eventsProcessed;

        public AggregateOperator(AggregateRelation aggregateRelation, FunctionsRegister functionsRegister, ExecutionDataflowBlockOptions executionDataflowBlockOptions) : base(executionDataflowBlockOptions)
        {
            _measures = new List<IAggregateContainer>();
            _flexBufferNewValue = new FlexBuffer(ArrayPool<byte>.Shared);

            if (aggregateRelation.Groupings != null && aggregateRelation.Groupings.Count > 0)
            {
                if (aggregateRelation.Groupings.Count > 1)
                {
                    throw new InvalidOperationException("Aggregate operator only supports one grouping set at this point");
                }

                var grouping = aggregateRelation.Groupings[0];

                groupExpressions = new List<Func<RowEvent, FlxValue>>();
                foreach (var expr in grouping.GroupingExpressions)
                {
                    groupExpressions.Add(ProjectCompiler.Compile(expr, functionsRegister));
                }
            }

            this.aggregateRelation = aggregateRelation;
            this.functionsRegister = functionsRegister;
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

        public override async Task<AggregateOperatorState> OnCheckpoint()
        {
            Debug.Assert(_tree != null, "Tree should not be null");
            await _tree.Commit();

            // Commit each measure
            foreach (var measure in _measures)
            {
                await measure.Commit();
            }
            return new AggregateOperatorState();
        }

        protected override async IAsyncEnumerable<StreamEventBatch> OnWatermark(Watermark watermark)
        {
            Debug.Assert(_tree != null, "Tree should not be null");
            Debug.Assert(_temporaryTree != null, "Temporary tree should not be null");

            List<RowEvent> outputs = new List<RowEvent>();
            // If no group expressions, then we are just doing a global aggregation, so we fetch the new value and compare it with the old
            if (groupExpressions == null || groupExpressions.Count == 0)
            {
                var (found, val) = await _tree.GetValue(new RowEvent(0, 0, new CompactRowData(EmptyVector, FlxValue.FromMemory(EmptyVector).AsVector)));

                if (found)
                {
                    Debug.Assert(val != null, "Value should not be null");
                    Debug.Assert(val.MeasureStates != null, "Measure states should not be null");
                    _flexBufferNewValue.NewObject();
                    var vectorStart = _flexBufferNewValue.StartVector();
                    for (int i = 0; i < val.MeasureStates.Length; i++)
                    {
                        var measureResult = await _measures[i].GetValue(new RowEvent(0, 0, new CompactRowData(EmptyVector, FlxValue.FromMemory(EmptyVector).AsVector)), val.MeasureStates[i]);
                        _flexBufferNewValue.Add(measureResult);
                    }
                    _flexBufferNewValue.EndVector(vectorStart, false, false);
                    var outputData = _flexBufferNewValue.Finish();
                    var outputEvent = new RowEvent(1, 0, new CompactRowData(outputData));
                    if (val.PreviousValue != null)
                    {
                        // If the row has changed from previous
                        if (!val.PreviousValue.SequenceEqual(outputData))
                        {
                            var oldEvent = new RowEvent(-1, 0, new CompactRowData(val.PreviousValue));
                            outputs.Add(outputEvent);
                            outputs.Add(oldEvent);
                        }
                    }
                    else
                    {
                        outputs.Add(outputEvent);
                    }

                    if (outputs.Count > 100)
                    {
                        yield return new StreamEventBatch(outputs);
                        outputs = new List<RowEvent>();
                    }
                    
                    // Replace the previous value with the new value
                    val.PreviousValue = outputData;
                }
                else
                {
                    val = new AggregateRowState()
                    {
                        MeasureStates = new byte[_measures.Count][]
                    };
                    _flexBufferNewValue.NewObject();
                    var vectorStart = _flexBufferNewValue.StartVector();
                    for (int i = 0; i < val.MeasureStates.Length; i++)
                    {
                        var measureResult = await _measures[i].GetValue(new RowEvent(0, 0, new CompactRowData(EmptyVector)), val.MeasureStates[i]);
                        _flexBufferNewValue.Add(measureResult);
                    }
                    _flexBufferNewValue.EndVector(vectorStart, false, false);
                    var outputData = _flexBufferNewValue.Finish();
                    val.PreviousValue = outputData;
                    outputs.Add(new RowEvent(1, 0, new CompactRowData(outputData)));

                    if (outputs.Count > 100)
                    {
                        yield return new StreamEventBatch(outputs);
                        outputs = new List<RowEvent>();
                    }
                    
                }
                await _tree.Upsert(new RowEvent(0, 0, new CompactRowData(EmptyVector)), val);
            }
            else
            {
                var iterator = _temporaryTree.CreateIterator();
                await iterator.SeekFirst();

                await foreach(var page in iterator)
                {
                    foreach(var kv in page)
                    {
                        var (found, val) = await _tree.GetValue(kv.Key);

                        if (!found)
                        {
                            continue;
                        }

                        // If weight is zero, then we need to delete the row
                        if (val!.Weight == 0)
                        {
                            // CHeck if a value has been emitted
                            if (val.PreviousValue != null)
                            {
                                // Negate that row on the stream
                                outputs.Add(new RowEvent(-1, 0, new CompactRowData(val.PreviousValue)));
                            }
                            await _tree.Delete(kv.Key);
                        }
                        _flexBufferNewValue.NewObject();
                        var vectorStart = _flexBufferNewValue.StartVector();
                        for (int i = 0; i < groupExpressions.Count; i++)
                        {
                            _flexBufferNewValue.Add(kv.Key.GetColumn(i));
                        }
                        if (aggregateRelation.Measures != null && aggregateRelation.Measures.Count > 0)
                        {
                            Debug.Assert(val.MeasureStates != null, "Measure states should not be null");
                            for (int i = 0; i < val.MeasureStates.Length; i++)
                            {
                                var measureResult = await _measures[i].GetValue(kv.Key, val.MeasureStates[i]);
                                _flexBufferNewValue.Add(measureResult);
                            }
                        }
                        
                        _flexBufferNewValue.EndVector(vectorStart, false, false);
                        var newObjectValue = _flexBufferNewValue.Finish();
                        if (val.PreviousValue != null)
                        {
                            // If the row has changed from previous
                            if (!val.PreviousValue.SequenceEqual(newObjectValue))
                            {
                                var oldEvent = new RowEvent(-1, 0, new CompactRowData(val.PreviousValue));
                                outputs.Add(oldEvent);
                                outputs.Add(new RowEvent(1, 0, new CompactRowData(newObjectValue)));
                            }
                            else
                            {
                                continue;
                            }
                        }
                        else
                        {
                            outputs.Add(new RowEvent(1, 0, new CompactRowData(newObjectValue)));
                        }

                        if (outputs.Count > 100)
                        {
                            yield return new StreamEventBatch(outputs);
                            outputs = new List<RowEvent>();
                        }
                        

                        val.PreviousValue = newObjectValue;
                        await _tree.Upsert(kv.Key, val);
                    }
                }

                // Output only 100 rows per batch to reduce memory consumption
                if (outputs.Count > 100)
                {
                    yield return new StreamEventBatch(outputs);
                    outputs = new List<RowEvent>();
                }

                await _temporaryTree.Clear();
            }

            if (outputs.Count > 0)
            {
                yield return new StreamEventBatch(outputs);
            }
        }

        public override async IAsyncEnumerable<StreamEventBatch> OnRecieve(StreamEventBatch msg, long time)
        {
            Debug.Assert(_tree != null, "Tree should not be null");
            Debug.Assert(_temporaryTree != null, "Temporary tree should not be null");
            Debug.Assert(_eventsProcessed != null, "Events processed should not be null");
            _eventsProcessed.Add(msg.Events.Count);
            foreach (var e in msg.Events)
            {
                // Create the key
                RowEvent? key = default;
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
                    key = new RowEvent(e.Weight, 0, new CompactRowData(keyBytes));

                    // Store the key in the temporary tree
                    await _temporaryTree.RMW(key.Value, default, (_, current, exist) =>
                    {
                        if (exist)
                        {
                            return (1, GenericWriteOperation.None);
                        }
                        else
                        {
                            return (1, GenericWriteOperation.Upsert);
                        }
                        
                    });
                }
                else
                {
                    key = new RowEvent(e.Weight, 0, new CompactRowData(EmptyVector));
                }

                var (found, val) = await _tree.GetValue(key.Value);

                if (_measures.Count > 0)
                {
                    if (found)
                    {
                        Debug.Assert(val != null, "Value should not be null");
                        Debug.Assert(val.MeasureStates != null, "Measure states should not be null");
                        for (int i = 0; i < _measures.Count; i++)
                        {
                            val!.MeasureStates[i] = await _measures[i].Compute(key.Value, e, val!.MeasureStates[i], e.Weight);
                        }
                    }
                    else
                    {
                        val = new AggregateRowState()
                        {
                            MeasureStates = new byte[_measures.Count][]
                        };
                        for (int i = 0; i < _measures.Count; i++)
                        {
                            val.MeasureStates[i] = await _measures[i].Compute(key.Value, e, null, e.Weight);
                        }
                        val.Weight += e.Weight;
                    }
                }
                else
                {
                    if (found)
                    {
                        val!.Weight += e.Weight;
                    }
                    else
                    {
                        val = new AggregateRowState()
                        {
                            Weight = e.Weight
                        };
                    }
                }
                await _tree.Upsert(key.Value, val);
            }

            yield break;
        }

        protected override async Task InitializeOrRestore(AggregateOperatorState? state, IStateManagerClient stateManagerClient)
        {
            if (aggregateRelation.Measures != null && aggregateRelation.Measures.Count > 0)
            {
                _measures.Clear();
                for (int i = 0; i < aggregateRelation.Measures.Count; i++)
                {
                    var measure = aggregateRelation.Measures[i];
                    var aggregateContainer = await MeasureCompiler.CompileMeasure(groupExpressions?.Count ?? 0, stateManagerClient.GetChildManager(i.ToString()), measure.Measure, functionsRegister);
                    _measures.Add(aggregateContainer);
                }
            }

            if (_eventsProcessed == null)
            {
                _eventsProcessed = Metrics.CreateCounter<long>("events_processed", "events", "Total events processed");
            }

            _tree = await stateManagerClient.GetOrCreateTree<RowEvent, AggregateRowState>("grouping_set_1_v1", new FlowtideDotNet.Storage.Tree.BPlusTreeOptions<RowEvent, AggregateRowState>()
            {
                KeySerializer = new StreamEventBPlusTreeSerializer(),
                ValueSerializer = new AggregateRowStateSerializer(),
                Comparer = new BPlusTreeStreamEventComparer()
            });
            _temporaryTree = await stateManagerClient.GetOrCreateTree<RowEvent, int>("grouping_set_1_v1_temp", new FlowtideDotNet.Storage.Tree.BPlusTreeOptions<RowEvent, int>()
            {
                KeySerializer = new StreamEventBPlusTreeSerializer(),
                ValueSerializer = new IntSerializer(),
                Comparer = new BPlusTreeStreamEventComparer()
            });
            await _temporaryTree.Clear();
        }
    }
}
