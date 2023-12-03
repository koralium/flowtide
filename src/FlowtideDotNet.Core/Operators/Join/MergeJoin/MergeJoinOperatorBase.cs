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
using FlowtideDotNet.Core.Operators.Join.NestedLoopJoin;
using FlowtideDotNet.Substrait.Relations;
using System.Threading.Tasks.Dataflow;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.Tree;
using Microsoft.Extensions.Logging;
using System.Diagnostics.Metrics;
using System.Buffers;
using System.Diagnostics;
using FlowtideDotNet.Core.Compute.Internal;
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Base.Metrics;

namespace FlowtideDotNet.Core.Operators.Join.MergeJoin
{
    internal class MergeJoinOperatorBase : MultipleInputVertex<StreamEventBatch, JoinState>
    {
        private readonly JoinComparerLeft leftComparer;
        private readonly JoinComparerRight rightComparer;
        protected readonly MergeJoinRelation mergeJoinRelation;
        protected IBPlusTree<JoinStreamEvent, JoinStorageValue>? _leftTree;
        protected IBPlusTree<JoinStreamEvent, JoinStorageValue>? _rightTree;
        private readonly int _leftSize;
        private readonly Dictionary<JoinStreamEvent, int> leftJoinWeight = new Dictionary<JoinStreamEvent, int>();
        private ICounter<long>? _eventsCounter;
        
        protected readonly Func<JoinStreamEvent, JoinStreamEvent, bool> _keyCondition;
        protected readonly Func<JoinStreamEvent, JoinStreamEvent, bool> _postCondition;

        private FlexBuffers.FlexBuffer _flexBuffer;

#if DEBUG_WRITE
        // TODO: Tmp remove
        private StreamWriter allInput;
        private StreamWriter leftInput;
        private StreamWriter rightInput;
        private StreamWriter outputWriter;
#endif

        public MergeJoinOperatorBase(MergeJoinRelation mergeJoinRelation, FunctionsRegister functionsRegister, ExecutionDataflowBlockOptions executionDataflowBlockOptions) : base(2, executionDataflowBlockOptions)
        {
            
            this.mergeJoinRelation = mergeJoinRelation;

            var compileResult = MergeJoinExpressionCompiler.Compile(mergeJoinRelation);

            leftComparer = new JoinComparerLeft(compileResult.LeftCompare, compileResult.SeekCompare);
            rightComparer = new JoinComparerRight(compileResult.RightCompare, compileResult.SeekCompare);
            _keyCondition = compileResult.CheckCondition;

            if (mergeJoinRelation.PostJoinFilter != null)
            {
                _postCondition = BooleanCompiler.Compile<JoinStreamEvent>(mergeJoinRelation.PostJoinFilter, functionsRegister, mergeJoinRelation.Left.OutputLength);
            }
            else
            {
                _postCondition = (left, right) => true;
            }
            _leftSize = mergeJoinRelation.Left.OutputLength;
            _flexBuffer = new FlexBuffers.FlexBuffer(ArrayPool<byte>.Shared);
        }

        public override string DisplayName => "Merge Join";

        public override ValueTask DisposeAsync()
        {
            return base.DisposeAsync();
        }

        public override Task Compact()
        {
            return Task.CompletedTask;
        }

        public override async Task DeleteAsync()
        {
        }

        public override async Task<JoinState?> OnCheckpoint()
        {
#if DEBUG_WRITE
            allInput.WriteLine("Checkpoint");
            await allInput.FlushAsync();
#endif
            Debug.Assert(_leftTree != null, nameof(_leftTree));
            Debug.Assert(_rightTree != null, nameof(_rightTree));

            await _leftTree.Commit();
            await _rightTree.Commit();
            return new JoinState();
        }

        protected StreamEvent OnConditionSuccess(JoinStreamEvent left, JoinStreamEvent right, in int weight)
        {
            _flexBuffer.NewObject();
            var vectorStart = _flexBuffer.StartVector();
            var leftSpan = left.Vector.Span;
            var rightSpan = right.Vector.Span;

            if (mergeJoinRelation.EmitSet)
            {
                for (int i = 0; i < mergeJoinRelation.Emit.Count; i++)
                {
                    var index = mergeJoinRelation.Emit[i];


                    if (index < _leftSize)
                    {
                        _flexBuffer.Add(left.Vector.GetWithSpan(index, leftSpan));
                    }
                    else
                    {
                        _flexBuffer.Add(right.Vector.GetWithSpan(index - _leftSize, rightSpan));
                    }
                }
            }
            else
            {
                for (int i = 0; i < left.Vector.Length; i++)
                {
                    _flexBuffer.Add(left.Vector.GetWithSpan(i, leftSpan));
                }
                for (int i = 0; i < right.Vector.Length; i++)
                {
                    _flexBuffer.Add(right.Vector.GetWithSpan(i, rightSpan));
                }
            }

            _flexBuffer.EndVector(vectorStart, false, false);
            var bytes = _flexBuffer.Finish();

            var ev = new StreamEvent(weight, 0, bytes);

            return ev;
        }

        protected StreamEvent CreateLeftWithNullRightEvent(int weight, JoinStreamEvent e)
        {
            _flexBuffer.NewObject();
            var vectorStart = _flexBuffer.StartVector();
            var exitingSpan = e.Vector.Span;

            if (mergeJoinRelation.EmitSet)
            {
                for (int i = 0; i < mergeJoinRelation.Emit.Count; i++)
                {
                    var index = mergeJoinRelation.Emit[i];
                    if (index < _leftSize)
                    {
                        _flexBuffer.Add(e.Vector.GetWithSpan(index, exitingSpan));
                    }
                    else
                    {
                        _flexBuffer.AddNull();
                    }
                }
            }
            else
            {
                for (int i = 0; i < e.Vector.Length; i++)
                {
                    _flexBuffer.Add(e.Vector.GetWithSpan(i, exitingSpan));
                }
                for (int i = 0; i < mergeJoinRelation.Right.OutputLength; i++)
                {
                    _flexBuffer.AddNull();
                }
            }
            _flexBuffer.EndVector(vectorStart, false, false);
            var bytes = _flexBuffer.Finish();

            var o = new StreamEvent(weight, 0, bytes);

            return o;
        }

        protected async IAsyncEnumerable<StreamEventBatch> OnRecieveLeft(StreamEventBatch msg, long time)
        {
            Debug.Assert(_leftTree != null, nameof(_leftTree));
            Debug.Assert(_rightTree != null, nameof(_rightTree));
            Debug.Assert(_eventsCounter != null, nameof(_eventsCounter));

            List<StreamEvent> output = new List<StreamEvent>();
            var it = _rightTree.CreateIterator();
            //using var it = _rightTree.CreateIterator();

            foreach (var e in msg.Events)
            {
#if DEBUG_WRITE
                leftInput.WriteLine($"{e.Weight} {e.Vector.ToJson}");
#endif
                var joinEventCheck = new JoinStreamEvent(e.Memory, 0, 1, e.Vector);

                await it.Seek(joinEventCheck);

                bool shouldBreak = false;
                // Iterate pages
                int joinWeight = 0;
                await foreach(var page in it)
                {
                    // Iterate values in the page
                    foreach(var kv in page)
                    {
                        if (_keyCondition(joinEventCheck, kv.Key))
                        {
                            if (_postCondition(joinEventCheck, kv.Key))
                            {
                                int outputWeight = e.Weight * kv.Value.Weight;
                                output.Add(OnConditionSuccess(joinEventCheck, kv.Key, outputWeight));
                                joinWeight += outputWeight;

                                if (output.Count > 100)
                                {
                                    _eventsCounter.Add(output.Count);
                                    yield return new StreamEventBatch(null, output);
                                    output = new List<StreamEvent>();
                                }
                                
                            }
                        }
                        else
                        {
                            shouldBreak = true;
                            break;
                        }
                    }
                    if (shouldBreak)
                    {
                        break;
                    }
                }

                var joinEvent = new JoinStreamEvent(e.Memory, 0, 0, e.Vector);
                if (joinWeight == 0 && mergeJoinRelation.Type == JoinType.Left)
                {
                    // Emit null if left join or full outer join
                    output.Add(CreateLeftWithNullRightEvent(e.Weight, joinEventCheck));
                    if (output.Count > 100)
                    {
                        _eventsCounter.Add(output.Count);
                        yield return new StreamEventBatch(null, output);
                        output = new List<StreamEvent>();
                    }
                    
                    await _leftTree.RMW(joinEvent, new JoinStorageValue() { Weight = e.Weight, JoinWeight = joinWeight }, (input, current, found) =>
                    {
                        if (found)
                        {
                            current!.Weight += input!.Weight;
                            current.JoinWeight += input.JoinWeight;
                            if (current.Weight == 0)
                            {
                                return (default, GenericWriteOperation.Delete);
                            }
                            return (current, GenericWriteOperation.Upsert);
                        }
                        return (input, GenericWriteOperation.Upsert);
                    });
                    //var weights = _leftTree.UpsertAndGetWeights(joinEvent, e.Weight, true);
                }
                else
                {
                    await _leftTree.RMW(joinEvent, new JoinStorageValue() { Weight = e.Weight, JoinWeight = joinWeight }, (input, current, found) =>
                    {
                        if (found)
                        {
                            current!.Weight += input!.Weight;
                            current.JoinWeight += input.JoinWeight;
                            if (current.Weight == 0)
                            {
                                return (default, GenericWriteOperation.Delete);
                            }
                            return (current, GenericWriteOperation.Upsert);
                        }
                        return (input, GenericWriteOperation.Upsert);
                    });
                }
            }

            if (output.Count > 0)
            {
                _eventsCounter.Add(output.Count);
#if DEBUG_WRITE
                foreach(var o in output)
                {
                    outputWriter.WriteLine($"{o.Weight} {o.Vector.ToJson}");
                }
#endif
                yield return new StreamEventBatch(null, output);
            }
#if DEBUG_WRITE
            await leftInput.FlushAsync();
            await outputWriter.FlushAsync();
#endif
        }

        protected async IAsyncEnumerable<StreamEventBatch> OnRecieveRight(StreamEventBatch msg, long time)
        {
            Debug.Assert(_leftTree != null, nameof(_leftTree));
            Debug.Assert(_rightTree != null, nameof(_rightTree));
            Debug.Assert(_eventsCounter != null, nameof(_eventsCounter));

            List<StreamEvent> output = new List<StreamEvent>();

            var it = _leftTree.CreateIterator();

            foreach (var e in msg.Events)
            {
#if DEBUG_WRITE
                rightInput.WriteLine($"{e.Weight} {e.Vector.ToJson}");
#endif
                var joinEventCheck = new JoinStreamEvent(e.Memory, 0, 1, e.Vector);

                await it.Seek(joinEventCheck);

                bool shouldBreak = false;
                await foreach(var page in it)
                {
                    foreach(var kv in page)
                    {
                        if (_keyCondition(kv.Key, joinEventCheck))
                        {
                            if (_postCondition(kv.Key, joinEventCheck))
                            {
                                int outputWeight = e.Weight * kv.Value.Weight;
                                output.Add(OnConditionSuccess(kv.Key, joinEventCheck, outputWeight));

                                if (mergeJoinRelation.Type == JoinType.Left)
                                {
                                    // If it is a left join, we need to always check the new weight of a row
                                    if (leftJoinWeight.TryGetValue(kv.Key, out var currentWeight))
                                    {
                                        leftJoinWeight[kv.Key] = currentWeight + outputWeight;
                                    }
                                    else
                                    {
                                        leftJoinWeight.Add(kv.Key, outputWeight);
                                    }
                                }

                                if (output.Count > 100)
                                {
                                    _eventsCounter.Add(output.Count);
                                    yield return new StreamEventBatch(null, output);
                                    output = new List<StreamEvent>();
                                }
                                
                            }
                        }
                        else
                        {
                            shouldBreak = true;
                            break;
                        }
                    }
                    if (shouldBreak)
                    {
                        break;
                    }
                }

                var joinEvent = new JoinStreamEvent(e.Memory, 0, 0, e.Vector);
                await _rightTree.RMW(joinEvent, new JoinStorageValue() { Weight = e.Weight }, (input, current, found) =>
                {
                    if (found)
                    {
                        current!.Weight += input!.Weight;
                        if (current.Weight == 0)
                        {
                            return (default, GenericWriteOperation.Delete);
                        }
                        return (current, GenericWriteOperation.Upsert);
                    }
                    return (input, GenericWriteOperation.Upsert);
                });
            }

            if (leftJoinWeight.Count > 0)
            {
                foreach(var kv in leftJoinWeight)
                {
                    bool zeroJoinWeight = false;
                    var (op, val) = await _leftTree.RMW(kv.Key, new JoinStorageValue() { JoinWeight = kv.Value}, (input, current, found) =>
                    {
                        if (found)
                        {
                            if (current!.JoinWeight == 0)
                            {
                                zeroJoinWeight = true;
                            }
                            current.JoinWeight += input!.JoinWeight;
                            return (current, GenericWriteOperation.Upsert);
                        }
                        return (default, GenericWriteOperation.None);
                    });

                    if (zeroJoinWeight)
                    {
                        // If it was zero before, we must emit a left with right null to negate previous value
                        output.Add(CreateLeftWithNullRightEvent(-val!.Weight, kv.Key));
                    }
                    if (val!.JoinWeight == 0)
                    {
                        output.Add(CreateLeftWithNullRightEvent(val.Weight, kv.Key));
                    }

                    if (output.Count > 100)
                    {
                        _eventsCounter.Add(output.Count);
                        yield return new StreamEventBatch(null, output);
                        output = new List<StreamEvent>();
                    }
                    
                }
                leftJoinWeight.Clear();
            }

            if (output.Count > 0)
            {
                _eventsCounter.Add(output.Count);
#if DEBUG_WRITE
                foreach (var o in output)
                {
                    outputWriter.WriteLine($"{o.Weight} {o.Vector.ToJson}");
                }
#endif
                yield return new StreamEventBatch(null, output);
            }
#if DEBUG_WRITE
            await rightInput.FlushAsync();
            await outputWriter.FlushAsync();
#endif
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
            if (targetId == 0)
            {
                return OnRecieveLeft(msg, time);
            }
            if (targetId == 1)
            {
                return OnRecieveRight(msg, time);
            }
            throw new NotSupportedException("Unknown targetId");
        }

        protected override async Task InitializeOrRestore(JoinState? state, IStateManagerClient stateManagerClient)
        {
#if DEBUG_WRITE
            allInput = File.CreateText($"{Name}.all.txt");
            leftInput = File.CreateText($"{Name}.left.txt");
            rightInput = File.CreateText($"{Name}.right.txt");
            outputWriter = File.CreateText($"{Name}.output.txt");
#endif
            Logger.LogInformation("Initializing merge join operator.");
            if(_eventsCounter == null)
            {
                _eventsCounter = Metrics.CreateCounter<long>("events");
            }

            _flexBuffer.Clear();
            if (state == null)
            {
                state = new JoinState();
            }
            _leftTree = await stateManagerClient.GetOrCreateTree("left", new BPlusTreeOptions<JoinStreamEvent, JoinStorageValue>()
            {
                Comparer = leftComparer,
                KeySerializer = new JoinStreamEvenBPlusTreeSerializer(),
                ValueSerializer = new JoinStorageValueBPlusTreeSerializer()
            });
            _rightTree = await stateManagerClient.GetOrCreateTree("right", new BPlusTreeOptions<JoinStreamEvent, JoinStorageValue>()
            {
                Comparer = rightComparer,
                KeySerializer = new JoinStreamEvenBPlusTreeSerializer(),
                ValueSerializer = new JoinStorageValueBPlusTreeSerializer()
            });
        }
    }
}
