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
using FlowtideDotNet.Storage.StateManager;
using System.Threading.Tasks.Dataflow;
using FlowtideDotNet.Storage.Tree;
using FlowtideDotNet.Core.Operators.Join.MergeJoin;
using FlowtideDotNet.Core.Storage;
using FlowtideDotNet.Base;
using FlowtideDotNet.Substrait.Relations;
using FlexBuffers;
using System.Buffers;
using Microsoft.Extensions.Logging;
using System.Diagnostics;
using FlowtideDotNet.Core.Compute.Internal;
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Base.Metrics;

namespace FlowtideDotNet.Core.Operators.Join.NestedLoopJoin
{
    /// <summary>
    /// Implemented with 4 trees, 2 persistent, and 2 temporary.
    /// Data is stored in the temporary trees until a watermark is recieved and it processes the data.
    /// </summary>
    internal class BlockNestedJoinOperator : MultipleInputVertex<StreamEventBatch, JoinState>
    {
        // Persisted trees
        protected IBPlusTree<RowEvent, JoinStorageValue>? _leftTree;
        protected IBPlusTree<RowEvent, JoinStorageValue>? _rightTree;

        // Temporary trees
        protected IBPlusTree<RowEvent, JoinStorageValue>? _leftTemporary;
        protected IBPlusTree<RowEvent, JoinStorageValue>? _rightTemporary;
        protected readonly Func<RowEvent, RowEvent, bool> _condition;
        private readonly JoinRelation joinRelation;
        private readonly FlexBuffer _flexBuffer;

        private readonly int _leftSize;

        private ICounter<long>? _eventsProcessed;

        public BlockNestedJoinOperator(JoinRelation joinRelation, FunctionsRegister functionsRegister, ExecutionDataflowBlockOptions executionDataflowBlockOptions) : base(2, executionDataflowBlockOptions)
        {
            _flexBuffer = new FlexBuffer(ArrayPool<byte>.Shared);
            if (joinRelation.Expression == null)
            {
                throw new InvalidOperationException("Join relation must have an expression");
            }
            _condition = BooleanCompiler.Compile<RowEvent>(joinRelation.Expression, functionsRegister, joinRelation.Left.OutputLength);
            this.joinRelation = joinRelation;
            _leftSize = joinRelation.Left.OutputLength;
        }

        public override string DisplayName => "Nested Loop Join";

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
            Debug.Assert(_leftTree != null, nameof(_leftTree));
            Debug.Assert(_rightTree != null, nameof(_rightTree));

            await _leftTree.Commit();
            await _rightTree.Commit();

            return new JoinState();
        }

        protected RowEvent OnConditionSuccess(RowEvent left, RowEvent right, in int weight)
        {
            _flexBuffer.NewObject();
            var vectorStart = _flexBuffer.StartVector();

            if (joinRelation.EmitSet)
            {
                for (int i = 0; i < joinRelation.Emit.Count; i++)
                {
                    var index = joinRelation.Emit[i];

                    if (index < _leftSize)
                    {
                        _flexBuffer.Add(left.GetColumn(index));
                    }
                    else
                    {
                        _flexBuffer.Add(right.GetColumn(index - _leftSize));
                    }
                }
            }
            else
            {
                for (int i = 0; i < left.Length; i++)
                {
                    _flexBuffer.Add(left.GetColumn(i));
                }
                for (int i = 0; i < right.Length; i++)
                {
                    _flexBuffer.Add(right.GetColumn(i));
                }
            }

            _flexBuffer.EndVector(vectorStart, false, false);
            var bytes = _flexBuffer.Finish();

            var ev = new RowEvent(weight, 0, new CompactRowData(bytes));

            return ev;
        }

        protected RowEvent CreateLeftWithNullRightEvent(int weight, RowEvent e)
        {
            _flexBuffer.NewObject();
            var vectorStart = _flexBuffer.StartVector();
            //var exitingSpan = e.Vector.Span;

            if (joinRelation.EmitSet)
            {
                for (int i = 0; i < joinRelation.Emit.Count; i++)
                {
                    var index = joinRelation.Emit[i];
                    if (index < _leftSize)
                    {
                        _flexBuffer.Add(e.GetColumn(index));
                    }
                    else
                    {
                        _flexBuffer.AddNull();
                    }
                }
            }
            else
            {
                for (int i = 0; i < e.Length; i++)
                {
                    _flexBuffer.Add(e.GetColumn(i));
                }
                for (int i = 0; i < joinRelation.Right.OutputLength; i++)
                {
                    _flexBuffer.AddNull();
                }
            }
            _flexBuffer.EndVector(vectorStart, false, false);
            var bytes = _flexBuffer.Finish();

            var o = new RowEvent(weight, 0, new CompactRowData(bytes));
            return o;
        }

        protected override async IAsyncEnumerable<StreamEventBatch> OnWatermark(Watermark watermark)
        {
            Debug.Assert(_leftTree != null, nameof(_leftTree));
            Debug.Assert(_rightTree != null, nameof(_rightTree));
            Debug.Assert(_leftTemporary != null, nameof(_leftTemporary));
            Debug.Assert(_rightTemporary != null, nameof(_rightTemporary));

            var rightTempIterator = _rightTemporary.CreateIterator();

            var leftPersistentIterator = _leftTree.CreateIterator();
            await leftPersistentIterator.SeekFirst();

            List<RowEvent> output = new List<RowEvent>();

            // Do the block nested loop join for the values on the right
            await foreach (var leftPage in leftPersistentIterator)
            {
                bool leftPageModified = false;

                // Must always seek the first page in the iterator
                await rightTempIterator.SeekFirst();
                await foreach (var rightTmpPage in rightTempIterator)
                {
                    bool rightModified = false;
                    foreach(var leftPageKv in leftPage)
                    {
                        foreach(var rightTmpPageKv in rightTmpPage)
                        {
                            if (_condition(leftPageKv.Key, rightTmpPageKv.Key))
                            {
                                var outputWeight = leftPageKv.Value.Weight * rightTmpPageKv.Value.Weight;
                                output.Add(OnConditionSuccess(leftPageKv.Key, rightTmpPageKv.Key, outputWeight));

                                // Check if it previously was left values with null right
                                var previousJoinWeight = leftPageKv.Value.JoinWeight;
                                rightTmpPageKv.Value.JoinWeight += outputWeight;
                                leftPageKv.Value.JoinWeight += outputWeight;

                                if (joinRelation.Type == JoinType.Left)
                                {
                                    // Check if it was previously a left values null right, then output a negation
                                    if (previousJoinWeight == 0 && leftPageKv.Value.JoinWeight > 0)
                                    {
                                        // Output a negation
                                        output.Add(CreateLeftWithNullRightEvent(-leftPageKv.Value.Weight, leftPageKv.Key));
                                    }
                                    // Went back to be a join with left values and null right
                                    if (previousJoinWeight > 0 && leftPageKv.Value.JoinWeight == 0)
                                    {
                                        output.Add(CreateLeftWithNullRightEvent(leftPageKv.Value.Weight, leftPageKv.Key));
                                    }
                                }
                                

                                rightModified = true;
                                leftPageModified = true;
                            }
                        }
                    }
                    if (rightModified)
                    {
                        await rightTmpPage.SavePage();
                    }
                }
                if (leftPageModified)
                {
                    await leftPage.SavePage();
                }
            }

            await rightTempIterator.SeekFirst();
            // Insert all right temporary values into the right persitent tree
            await foreach (var rightTmpPage in rightTempIterator)
            {
                foreach(var kv in rightTmpPage)
                {
                    await _rightTree.RMW(kv.Key, kv.Value, (input, current, exist) =>
                    {
                        if (exist)
                        {
                            current!.Weight += input!.Weight;
                            current!.JoinWeight += input!.JoinWeight;
                            if (current.Weight == 0)
                            {
                                return (default, GenericWriteOperation.Delete);
                            }
                            return (current, GenericWriteOperation.Upsert);
                        }
                        return (input, GenericWriteOperation.Upsert);
                    });

                    // TODO: On right joins or full outer joins, should check if join weight is 0, if so, output left side null with right side values.
                }
            }
            await _rightTemporary.Clear();

            var leftTempIterator = _leftTemporary.CreateIterator();
            
            var rightPersistentIterator = _rightTree.CreateIterator();
            await rightPersistentIterator.SeekFirst();

            // Go through left side temporary values against right side persistent values
            await foreach(var rightPage in rightPersistentIterator)
            {
                bool rightModified = false;

                // Must always seek the first page in the inner iterator
                await leftTempIterator.SeekFirst();
                await foreach(var leftTempPage in leftTempIterator)
                {
                    bool leftPageModified = false;
                    foreach (var rightPageKv in rightPage)
                    {
                        foreach(var leftTempPageKv in leftTempPage)
                        {
                            if (_condition(leftTempPageKv.Key, rightPageKv.Key))
                            {
                                var outputWeight = leftTempPageKv.Value.Weight * rightPageKv.Value.Weight;
                                output.Add(OnConditionSuccess(leftTempPageKv.Key, rightPageKv.Key, outputWeight));
                                rightPageKv.Value.JoinWeight += outputWeight;
                                leftTempPageKv.Value.JoinWeight += outputWeight;
                                rightModified = true;
                                leftPageModified = true;
                            }
                        }
                    }
                    if (leftPageModified)
                    {
                        await leftTempPage.SavePage();
                    }
                }
                if (rightModified)
                {
                    await rightPage.SavePage();
                }
            }

            await leftTempIterator.SeekFirst();
            await foreach(var leftTempPage in leftTempIterator)
            {
                foreach(var kv in leftTempPage)
                {
                    var (op, val) = await _leftTree.RMW(kv.Key, kv.Value, (input, current, exist) =>
                    {
                        if (exist)
                        {
                            current!.Weight += input!.Weight;
                            current!.JoinWeight += input!.JoinWeight;
                            if (current.Weight == 0)
                            {
                                return (default, GenericWriteOperation.Delete);
                            }
                            return (current, GenericWriteOperation.Upsert);
                        }
                        return (input, GenericWriteOperation.Upsert);
                    });

                    if (op == GenericWriteOperation.Upsert && val!.JoinWeight == 0 && joinRelation.Type == JoinType.Left)
                    {
                        // Output null values here
                        output.Add(CreateLeftWithNullRightEvent(kv.Value.Weight, kv.Key));
                    }
                    else
                    {

                    }
                }
            }

            await _leftTemporary.Clear();

            yield return new StreamEventBatch(output);
        }

        public override async IAsyncEnumerable<StreamEventBatch> OnRecieve(int targetId, StreamEventBatch msg, long time)
        {
            Debug.Assert(_leftTemporary != null, nameof(_leftTemporary));
            Debug.Assert(_rightTemporary != null, nameof(_rightTemporary));
            Debug.Assert(_eventsProcessed != null, nameof(_eventsProcessed));
            _eventsProcessed.Add(msg.Events.Count);

            if (targetId == 0)
            {
                foreach(var e in msg.Events)
                {
                    await _leftTemporary.RMW(e, new JoinStorageValue() { Weight = e.Weight, JoinWeight = 0}, (input, current, exist) =>
                    {
                        if (exist)
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
                yield break;
            }
            else if (targetId == 1)
            {
                foreach (var e in msg.Events)
                {
                    await _rightTemporary.RMW(e, new JoinStorageValue() { Weight = e.Weight, JoinWeight = 0 }, (input, current, exist) =>
                    {
                        if (exist)
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
                yield break;
            }
            else
            {
                throw new NotImplementedException();
            }
        }

        protected override async Task InitializeOrRestore(JoinState? state, IStateManagerClient stateManagerClient)
        {
            Logger.LogWarning("Block nested loop join in use, it will severely impact performance of the stream.");
            _leftTree = await stateManagerClient.GetOrCreateTree("left", new BPlusTreeOptions<RowEvent, JoinStorageValue>()
            {
                Comparer = new NestedJoinStreamEventComparer(),
                KeySerializer = new StreamEventBPlusTreeSerializer(),
                ValueSerializer = new JoinStorageValueBPlusTreeSerializer()
            });
            _rightTree = await stateManagerClient.GetOrCreateTree("right", new BPlusTreeOptions<RowEvent, JoinStorageValue>()
            {
                Comparer = new NestedJoinStreamEventComparer(),
                KeySerializer = new StreamEventBPlusTreeSerializer(),
                ValueSerializer = new JoinStorageValueBPlusTreeSerializer()
            });

            _leftTemporary = await stateManagerClient.GetOrCreateTree("left_tmp", new BPlusTreeOptions<RowEvent, JoinStorageValue>()
            {
                Comparer = new NestedJoinStreamEventComparer(),
                KeySerializer = new StreamEventBPlusTreeSerializer(),
                ValueSerializer = new JoinStorageValueBPlusTreeSerializer()
            });

            _rightTemporary = await stateManagerClient.GetOrCreateTree("right_tmp", new BPlusTreeOptions<RowEvent, JoinStorageValue>()
            {
                Comparer = new NestedJoinStreamEventComparer(),
                KeySerializer = new StreamEventBPlusTreeSerializer(),
                ValueSerializer = new JoinStorageValueBPlusTreeSerializer()
            });

            if (_eventsProcessed == null)
            {
                _eventsProcessed = Metrics.CreateCounter<long>("events_processed");
            }
        }
    }
}
