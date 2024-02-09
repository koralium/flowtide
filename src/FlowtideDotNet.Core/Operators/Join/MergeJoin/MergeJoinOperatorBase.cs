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
using System.Buffers;
using System.Diagnostics;
using FlowtideDotNet.Core.Compute.Internal;
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Base.Metrics;
using FlowtideDotNet.Core.Utils;

namespace FlowtideDotNet.Core.Operators.Join.MergeJoin
{
    internal class MergeJoinOperatorBase : MultipleInputVertex<StreamEventBatch, JoinState>
    {
        private readonly JoinComparerLeft leftComparer;
        private readonly JoinComparerRight rightComparer;
        protected readonly MergeJoinRelation mergeJoinRelation;
        protected IBPlusTree<JoinStreamEvent, JoinStorageValue>? _leftTree;
        protected IBPlusTree<JoinStreamEvent, JoinStorageValue>? _rightTree;
        private ICounter<long>? _eventsCounter;
        private ICounter<long>? _eventsProcessed;

        protected readonly Func<JoinStreamEvent, JoinStreamEvent, bool> _keyCondition;
        protected readonly Func<JoinStreamEvent, JoinStreamEvent, bool> _postCondition;

        private readonly FlexBuffers.FlexBuffer _flexBuffer;
        private readonly IRowData _rightNullData;

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
            _flexBuffer = new FlexBuffers.FlexBuffer(ArrayPool<byte>.Shared);

            _rightNullData = RowEvent.Create(0, 0, v =>
            {
                for (int i = 0; i < mergeJoinRelation.Right.OutputLength; i++)
                {
                    v.AddNull();
                }
            }).RowData;
        }

        public override string DisplayName => "Merge Join";

        public override Task Compact()
        {
            return Task.CompletedTask;
        }

        public override Task DeleteAsync()
        {
            return Task.CompletedTask;
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

        protected RowEvent OnConditionSuccess(JoinStreamEvent left, JoinStreamEvent right, in int weight, in uint iteration)
        {
            return new RowEvent(weight, iteration, ArrayRowData.Create(left.RowData, right.RowData, mergeJoinRelation.Emit));
        }

        protected RowEvent CreateLeftWithNullRightEvent(int weight, JoinStreamEvent e, in uint iteration)
        {
            return new RowEvent(weight, iteration, ArrayRowData.Create(e.RowData, _rightNullData, mergeJoinRelation.Emit));
        }

        protected async IAsyncEnumerable<StreamEventBatch> OnRecieveLeft(StreamEventBatch msg, long time)
        {
            Debug.Assert(_leftTree != null, nameof(_leftTree));
            Debug.Assert(_rightTree != null, nameof(_rightTree));
            Debug.Assert(_eventsCounter != null, nameof(_eventsCounter));

            List<RowEvent> output = new List<RowEvent>();
            var it = _rightTree.CreateIterator();

            foreach (var e in msg.Events)
            {
#if DEBUG_WRITE
                leftInput.WriteLine($"{e.Weight} {e.ToJson()}");
#endif

                var joinEventCheck = new JoinStreamEvent(0, 1, e.RowData);

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
                                output.Add(OnConditionSuccess(joinEventCheck, kv.Key, outputWeight, e.Iteration));
                                joinWeight += outputWeight;

                                if (output.Count > 100)
                                {
                                    _eventsCounter.Add(output.Count);
                                    yield return new StreamEventBatch(output);
                                    output = new List<RowEvent>();
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

                var joinEvent = new JoinStreamEvent(0, 0, e.RowData);
                if (joinWeight == 0 && mergeJoinRelation.Type == JoinType.Left)
                {
                    // Emit null if left join or full outer join
                    output.Add(CreateLeftWithNullRightEvent(e.Weight, joinEventCheck, e.Iteration));
                    if (output.Count > 100)
                    {
                        _eventsCounter.Add(output.Count);
                        yield return new StreamEventBatch(output);
                        output = new List<RowEvent>();
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
                    outputWriter.WriteLine($"{o.Weight} {o.ToJson()}");
                }
#endif
                yield return new StreamEventBatch(output);
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

            List<RowEvent> output = new List<RowEvent>();

            var it = _leftTree.CreateIterator();

            foreach (var e in msg.Events)
            {
#if DEBUG_WRITE
                rightInput.WriteLine($"{e.Weight} {e.ToJson()}");
#endif
                var joinEventCheck = new JoinStreamEvent(0, 1, e.RowData);

                await it.Seek(joinEventCheck);

                bool shouldBreak = false;
                await foreach(var page in it)
                {
                    bool pageUpdated = false;
                    foreach(var kv in page)
                    {
                        if (_keyCondition(kv.Key, joinEventCheck))
                        {
                            if (_postCondition(kv.Key, joinEventCheck))
                            {
                                int outputWeight = e.Weight * kv.Value.Weight;
                                output.Add(OnConditionSuccess(kv.Key, joinEventCheck, outputWeight, e.Iteration));

                                

                                if (mergeJoinRelation.Type == JoinType.Left)
                                {
                                    pageUpdated = true;
                                    if (kv.Value.JoinWeight == 0)
                                    {
                                        // If it was zero before, we must emit a left with right null to negate previous value
                                        output.Add(CreateLeftWithNullRightEvent(-kv.Value.Weight, kv.Key, e.Iteration));
                                    }

                                    kv.Value.JoinWeight += outputWeight;

                                    if (kv.Value.JoinWeight == 0)
                                    {
                                        output.Add(CreateLeftWithNullRightEvent(kv.Value.Weight, kv.Key, e.Iteration));
                                    }
                                }

                                if (output.Count > 100)
                                {
                                    _eventsCounter.Add(output.Count);
                                    yield return new StreamEventBatch(output);
                                    output = new List<RowEvent>();
                                }
                                
                            }
                        }
                        else
                        {
                            shouldBreak = true;
                            break;
                        }
                    }
                    if (pageUpdated)
                    {
                        await page.SavePage();
                    }
                    if (shouldBreak)
                    {
                        break;
                    }
                }

                var joinEvent = new JoinStreamEvent(0, 0, e.RowData);
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

            if (output.Count > 0)
            {
                _eventsCounter.Add(output.Count);
#if DEBUG_WRITE
                foreach (var o in output)
                {
                    outputWriter.WriteLine($"{o.Weight} {o.ToJson()}");
                }
#endif
                yield return new StreamEventBatch(output);
            }
#if DEBUG_WRITE
            await rightInput.FlushAsync();
            await outputWriter.FlushAsync();
#endif
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
            if (allInput != null)
            {
                allInput.WriteLine("Restart");
            }
            else
            {
                allInput = File.CreateText($"{StreamName}-{Name}.all.txt");
                leftInput = File.CreateText($"{StreamName}-{Name}.left.txt");
                rightInput = File.CreateText($"{StreamName}-{Name}.right.txt");
                outputWriter = File.CreateText($"{StreamName}-{Name}.output.txt");
            }
#endif
            Logger.InitializingMergeJoinOperator(StreamName, Name);
            if (_eventsCounter == null)
            {
                _eventsCounter = Metrics.CreateCounter<long>("events");
            }
            if (_eventsProcessed == null)
            {
                _eventsProcessed = Metrics.CreateCounter<long>("events_processed");
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
