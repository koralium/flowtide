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
using FlowtideDotNet.Base.Utils;
using FlowtideDotNet.Base.Vertices.Unary;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Core.Compute.Columnar;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Substrait.Relations;
using System.Diagnostics;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Core.Operators.TableFunction
{
    internal class TableFunctionJoinOperator : UnaryVertex<StreamEventBatch, object?>
    {
        private readonly TableFunctionRelation _tableFunctionRelation;
        private readonly IFunctionsRegister _functionsRegister;
        private Func<EventBatchData, int, IEnumerable<EventBatchWeighted>>? _func;
        private Func<EventBatchData, int, EventBatchData, int, bool>? _joinCondition;
        private int _functionOutputLength;

        private readonly List<int> _leftOutputColumns;
        private readonly List<int> _leftOutputIndices;

        private List<int>? _rightOutputIndices;

        private ICounter<long>? _eventsCounter;
        private ICounter<long>? _eventsProcessed;

        public TableFunctionJoinOperator(
            TableFunctionRelation tableFunctionRelation,
            IFunctionsRegister functionsRegister,
            ExecutionDataflowBlockOptions executionDataflowBlockOptions) 
            : base(executionDataflowBlockOptions)
        {
            _tableFunctionRelation = tableFunctionRelation;
            _functionsRegister = functionsRegister;
            if (_tableFunctionRelation.Input == null)
            {
                throw new InvalidOperationException("Table function must have an input when used in a join");
            }
            (_leftOutputColumns, _leftOutputIndices) = GetOutputColumns(_tableFunctionRelation, 0, _tableFunctionRelation.Input.OutputLength);
        }

        public override string DisplayName => "TableFunction";

        public override Task Compact()
        {
            return Task.CompletedTask;
        }

        public override Task DeleteAsync()
        {
            return Task.CompletedTask;
        }

        public override Task<object?> OnCheckpoint()
        {
            return Task.FromResult<object?>(null);
        }

        private static (List<int> incomingIndices, List<int> outgoingIndex) GetOutputColumns(TableFunctionRelation tableFunctionRelation, int relative, int maxSize)
        {
            List<int> columns = new List<int>();
            List<int> outgoingIndices = new List<int>();
            if (tableFunctionRelation.EmitSet)
            {
                for (int i = 0; i < tableFunctionRelation.Emit.Count; i++)
                {
                    var index = tableFunctionRelation.Emit[i];
                    if (index >= relative)
                    {
                        index = index - relative;
                        if (index < maxSize)
                        {
                            columns.Add(index);
                            outgoingIndices.Add(i);
                        }
                    }

                }
            }
            else
            {
                for (int i = 0; i < tableFunctionRelation.OutputLength - relative; i++)
                {
                    if (i < maxSize)
                    {
                        columns.Add(i);
                        outgoingIndices.Add(i + relative);
                    }
                }
            }
            return (columns, outgoingIndices);
        }

        public override IAsyncEnumerable<StreamEventBatch> OnRecieve(StreamEventBatch msg, long time)
        {
            Debug.Assert(_func != null);
            Debug.Assert(_rightOutputIndices != null);
            Debug.Assert(_eventsProcessed != null);
            Debug.Assert(_eventsCounter != null);

            var data = msg.Data.EventBatchData;
            var inputWeights = msg.Data.Weights;
            var iterations = msg.Data.Iterations;

            PrimitiveList<int> foundOffsets = new PrimitiveList<int>(MemoryAllocator);
            PrimitiveList<int> outputWeights = new PrimitiveList<int>(MemoryAllocator);
            PrimitiveList<uint> outputIterations = new PrimitiveList<uint>(MemoryAllocator);
            IColumn[] outputColumns = new IColumn[_functionOutputLength];
            for (int i = 0; i < outputColumns.Length; i++)
            {
                outputColumns[i] = Column.Create(MemoryAllocator);
            }

            _eventsProcessed.Add(inputWeights.Count);
            for (int inputIndex = 0; inputIndex < inputWeights.Count; inputIndex++)
            {
                var inputWeight = inputWeights[inputIndex];
                var inputIteration = iterations[inputIndex];
                var newRows = _func(data, inputIndex);
                
                bool emittedAny = false;
                foreach(var batch in newRows)
                {
                    int matchStart = -1;
                    int matchEnd = -1;
                    for (int newIndex = 0; newIndex < batch.Weights.Count; newIndex++)
                    {
                        if (_joinCondition == null || _joinCondition(data, inputIndex, batch.EventBatchData, newIndex))
                        {
                            emittedAny = true;
                            foundOffsets.Add(inputIndex);
                            outputWeights.Add(inputWeight * batch.Weights[newIndex]);
                            outputIterations.Add(inputIteration);
                            if (matchStart == -1)
                            {
                                matchStart = newIndex;
                            }
                            matchEnd = newIndex;
                        }
                        else
                        {
                            if (matchStart != -1)
                            {
                                // Copy data over in batch from matchStart to matchEnd
                                for (int i = 0; i < outputColumns.Length; i++)
                                {
                                    outputColumns[i].InsertRangeFrom(outputColumns[i].Count, batch.EventBatchData.Columns[i], matchStart, matchEnd - matchStart + 1);
                                }
                                matchStart = -1;
                                matchEnd = -1;
                            }
                        }
                    }
                    if (matchStart != -1)
                    {
                        for (int i = 0; i < outputColumns.Length; i++)
                        {
                            outputColumns[i].InsertRangeFrom(outputColumns[i].Count, batch.EventBatchData.Columns[i], matchStart, matchEnd - matchStart + 1);
                        }
                    }
                    batch.Return();
                }

                if (_tableFunctionRelation.Type == JoinType.Left && !emittedAny)
                {
                    foundOffsets.Add(inputIndex);
                    outputWeights.Add(inputWeight);
                    outputIterations.Add(0);
                    for (int i = 0; i < outputColumns.Length; i++)
                    {
                        outputColumns[i].Add(NullValue.Instance);
                    }
                }
            }

            if (foundOffsets.Count > 0)
            {
                IColumn[] emitColumns = new IColumn[_leftOutputColumns.Count + _rightOutputIndices.Count];
                if (_leftOutputColumns.Count > 0)
                {
                    for (int i = 0; i < _leftOutputColumns.Count; i++)
                    {
                        emitColumns[_leftOutputIndices[i]] = new ColumnWithOffset(msg.Data.EventBatchData.Columns[_leftOutputColumns[i]], foundOffsets, true);
                    }
                }
                else
                {
                    foundOffsets.Dispose();
                }

                for (int i = 0; i < outputColumns.Length; i++)
                {
                    emitColumns[_rightOutputIndices[i]] = outputColumns[i];
                }

                _eventsCounter.Add(outputWeights.Count);
                var outputBatch = new StreamEventBatch(new EventBatchWeighted(outputWeights, outputIterations, new EventBatchData(emitColumns)));
                return new SingleAsyncEnumerable<StreamEventBatch>(outputBatch);
            }
            else
            {
                foundOffsets.Dispose();
                outputWeights.Dispose();
                outputIterations.Dispose();
                for (int i = 0; i < outputColumns.Length; i++)
                {
                    outputColumns[i].Dispose();
                }
            }

            return EmptyAsyncEnumerable<StreamEventBatch>.Instance;
        }

        protected override Task InitializeOrRestore(object? state, IStateManagerClient stateManagerClient)
        {
            Debug.Assert(_tableFunctionRelation.Input != null);

            if (_eventsCounter == null)
            {
                _eventsCounter = Metrics.CreateCounter<long>("events");
            }
            if (_eventsProcessed == null)
            {
                _eventsProcessed = Metrics.CreateCounter<long>("events_processed");
            }

            // Must be compiled in initialize to have access to memory allocator
            var compileResult = ColumnTableFunctionCompiler.CompileWithArg(_tableFunctionRelation.TableFunction, _functionsRegister, MemoryAllocator);
            _func = compileResult.Function;
            _functionOutputLength = _tableFunctionRelation.TableFunction.TableSchema.Names.Count;
            (_, _rightOutputIndices) = GetOutputColumns(_tableFunctionRelation, _tableFunctionRelation.Input.OutputLength, _functionOutputLength);
            if (_tableFunctionRelation.JoinCondition != null)
            {
                // Create a boolean filter function that takes both the left and right side of the join
                _joinCondition = ColumnBooleanCompiler.CompileTwoInputs(_tableFunctionRelation.JoinCondition, _functionsRegister, _tableFunctionRelation.Input.OutputLength);
            }
            return Task.CompletedTask;
        }
    }
}
