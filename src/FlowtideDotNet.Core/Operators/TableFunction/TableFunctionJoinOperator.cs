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
using FlowtideDotNet.Base.Vertices;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Core.Compute.Columnar;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Substrait.Relations;
using System.Diagnostics;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Core.Operators.TableFunction
{
    internal class TableFunctionJoinOperator : UnaryVertex<StreamEventBatch>
    {
        private readonly TableFunctionRelation _tableFunctionRelation;
        private readonly IFunctionsRegister _functionsRegister;
        private TableFunctionEmit? _func;
        private Func<EventBatchData, int, EventBatchData, int, bool>? _joinCondition;
        private int _functionOutputLength;

        // Reusable scratch used only when a join condition is present. The function appends
        // its produced rows here, the condition filters them, and the passing rows are copied
        // into the output. Cleared (not reallocated) per input row.
        private TableFunctionRowBuffer? _scratch;

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

        public override Task OnCheckpoint()
        {
            return Task.CompletedTask;
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

            // The function appends its rows straight into this output; the columns and weight
            // lists are reused for the whole incoming batch instead of one batch per row.
            var output = new TableFunctionJoinOutput(_functionOutputLength, MemoryAllocator);

            _eventsProcessed.Add(inputWeights.Count);
            for (int inputIndex = 0; inputIndex < inputWeights.Count; inputIndex++)
            {
                var inputWeight = inputWeights[inputIndex];
                var inputIteration = iterations[inputIndex];

                if (_joinCondition == null)
                {
                    // Fast path: every produced row is emitted, so the function writes directly
                    // into the output columns/weights with no intermediate copy.
                    output.InputIndex = inputIndex;
                    output.InputWeight = inputWeight;
                    output.InputIteration = inputIteration;

                    int before = output.Count;
                    _func(data, inputIndex, output);

                    if (_tableFunctionRelation.Type == JoinType.Left && output.Count == before)
                    {
                        output.AddNullRow(inputIndex, inputWeight);
                    }
                }
                else
                {
                    // Filtered path: generate into the reusable scratch, then copy only the rows
                    // that pass the join condition into the output, coalescing them into ranges.
                    var scratch = _scratch!;
                    scratch.Clear();
                    _func(data, inputIndex, scratch);

                    var scratchBatch = scratch.Batch;
                    var scratchWeights = scratch.Weights;
                    var outputColumns = output.FunctionColumns;

                    bool emittedAny = false;
                    int matchStart = -1;
                    int matchEnd = -1;
                    for (int newIndex = 0; newIndex < scratch.Count; newIndex++)
                    {
                        if (_joinCondition(data, inputIndex, scratchBatch, newIndex))
                        {
                            emittedAny = true;
                            output.FoundOffsets.Add(inputIndex);
                            output.Weights.Add(inputWeight * scratchWeights[newIndex]);
                            output.Iterations.Add(inputIteration);
                            if (matchStart == -1)
                            {
                                matchStart = newIndex;
                            }
                            matchEnd = newIndex;
                        }
                        else if (matchStart != -1)
                        {
                            for (int i = 0; i < outputColumns.Length; i++)
                            {
                                outputColumns[i].InsertRangeFrom(outputColumns[i].Count, scratchBatch.Columns[i], matchStart, matchEnd - matchStart + 1);
                            }
                            matchStart = -1;
                            matchEnd = -1;
                        }
                    }
                    if (matchStart != -1)
                    {
                        for (int i = 0; i < outputColumns.Length; i++)
                        {
                            outputColumns[i].InsertRangeFrom(outputColumns[i].Count, scratchBatch.Columns[i], matchStart, matchEnd - matchStart + 1);
                        }
                    }

                    if (_tableFunctionRelation.Type == JoinType.Left && !emittedAny)
                    {
                        output.AddNullRow(inputIndex, inputWeight);
                    }
                }
            }

            if (output.Count > 0)
            {
                IColumn[] emitColumns = new IColumn[_leftOutputColumns.Count + _rightOutputIndices.Count];
                if (_leftOutputColumns.Count > 0)
                {
                    bool shouldDisposeOffsets = true;
                    for (int i = 0; i < _leftOutputColumns.Count; i++)
                    {
                        emitColumns[_leftOutputIndices[i]] = ColumnWithOffset.CreateFlattened(msg.Data.EventBatchData.Columns[_leftOutputColumns[i]], output.FoundOffsets, MemoryAllocator, out var offsetUsed);
                        if (offsetUsed)
                        {
                            shouldDisposeOffsets = false;
                        }
                    }
                    if (shouldDisposeOffsets)
                    {
                        output.FoundOffsets.Dispose();
                    }
                }
                else
                {
                    output.FoundOffsets.Dispose();
                }

                for (int i = 0; i < output.FunctionColumns.Length; i++)
                {
                    emitColumns[_rightOutputIndices[i]] = output.FunctionColumns[i];
                }

                _eventsCounter.Add(output.Weights.Count);
                var outputBatch = new StreamEventBatch(new EventBatchWeighted(output.Weights, output.Iterations, new EventBatchData(emitColumns)));
                return new SingleAsyncEnumerable<StreamEventBatch>(outputBatch);
            }
            else
            {
                output.Dispose();
            }

            return EmptyAsyncEnumerable<StreamEventBatch>.Instance;
        }

        protected override Task InitializeOrRestore(IStateManagerClient stateManagerClient)
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
            _func = compileResult.Emit;
            _functionOutputLength = _tableFunctionRelation.TableFunction.TableSchema.Names.Count;
            (_, _rightOutputIndices) = GetOutputColumns(_tableFunctionRelation, _tableFunctionRelation.Input.OutputLength, _functionOutputLength);
            if (_tableFunctionRelation.JoinCondition != null)
            {
                if (_scratch != null)
                {
                    _scratch.Dispose();
                    _scratch = null;
                }
                // Create a boolean filter function that takes both the left and right side of the join
                _joinCondition = ColumnBooleanCompiler.CompileTwoInputs(_tableFunctionRelation.JoinCondition, _functionsRegister, _tableFunctionRelation.Input.OutputLength);
                // Reusable scratch for the filtered path.
                _scratch = new TableFunctionRowBuffer(_functionOutputLength, MemoryAllocator);
            }
            return Task.CompletedTask;
        }

        public override ValueTask DisposeAsync()
        {
            if (_scratch != null)
            {
                _scratch.Dispose();
            }
            
            return base.DisposeAsync();
        }
    }
}
