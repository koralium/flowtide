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
using FlowtideDotNet.Base.Vertices.Unary;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.Comparers;
using FlowtideDotNet.Core.ColumnStore.Memory;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Core.ColumnStore.Utils;
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Core.Compute.Internal;
using FlowtideDotNet.Core.Utils;
using FlowtideDotNet.Storage.Serializers;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.Tree;
using FlowtideDotNet.Substrait.Relations;
using SqlParser.Ast;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Core.Operators.Normalization
{
    internal class ColumnNormalizationOperator : UnaryVertex<StreamEventBatch, NormalizationState>
    {
        private readonly NormalizationRelation _normalizationRelation;
        private IBPlusTree<ColumnRowReference, ColumnRowReference, NormalizeKeyStorage, NormalizeValueStorage>? _tree;
        private readonly List<int> _keyColumns;
        private readonly List<int> _otherColumns;
        private readonly Func<RowEvent, bool>? _filter;

        private ICounter<long>? _eventsCounter;
        private ICounter<long>? _eventsProcessed;

        public ColumnNormalizationOperator(
            NormalizationRelation normalizationRelation,
            FunctionsRegister functionsRegister,
            ExecutionDataflowBlockOptions executionDataflowBlockOptions) : base(executionDataflowBlockOptions)
        {
            this._normalizationRelation = normalizationRelation;
            _keyColumns = normalizationRelation.KeyIndex;
            _otherColumns = [];

            if (normalizationRelation.Filter != null)
            {
                // Will be changed later on to use functions that can take columns
                _filter = BooleanCompiler.Compile<RowEvent>(normalizationRelation.Filter, functionsRegister);
            }

            if (normalizationRelation.EmitSet)
            {
                for (int i = 0; i < normalizationRelation.Emit.Count; i++)
                {
                    if (!_keyColumns.Contains(normalizationRelation.Emit[i]))
                    {
                        _otherColumns.Add(normalizationRelation.Emit[i]);
                    }
                }
            }
            else
            {
                for (int i = 0; i < normalizationRelation.OutputLength; i++)
                {
                    if (!_keyColumns.Contains(i))
                    {
                        _otherColumns.Add(i);
                    }
                }
            }
        }

        public override string DisplayName => "Normalize";

        public override Task Compact()
        {
            return Task.CompletedTask;
        }

        public override Task DeleteAsync()
        {
            return Task.CompletedTask;
        }

        public override async Task<NormalizationState> OnCheckpoint()
        {
            await _tree!.Commit();
            return new NormalizationState();
        }

        public override async IAsyncEnumerable<StreamEventBatch> OnRecieve(StreamEventBatch msg, long time)
        {
            Debug.Assert(_eventsProcessed != null);
            Debug.Assert(_eventsCounter != null);

            var otherColumnsMemoryManager = new BatchMemoryManager(_otherColumns.Count);

            PrimitiveList<int> toEmitOffsets = new PrimitiveList<int>(otherColumnsMemoryManager);
            PrimitiveList<int> weights = new PrimitiveList<int>(otherColumnsMemoryManager);
            PrimitiveList<uint> iterations = new PrimitiveList<uint>(otherColumnsMemoryManager);

            PrimitiveList<int> deleteBatchKeyOffsets = new PrimitiveList<int>(otherColumnsMemoryManager);
            List<IColumn> deleteBatchColumns = new List<IColumn>();
            
            for (int i = 0; i < _otherColumns.Count; i++)
            {
                deleteBatchColumns.Add(new Column(otherColumnsMemoryManager));
            }

            _eventsProcessed.Add(msg.Data.Weights.Count);
            for (int i = 0; i < msg.Data.Weights.Count; i++)
            {
                var weight = msg.Data.Weights[i];
                var iteration = msg.Data.Iterations[i];

                var columnRef = new ColumnRowReference() { referenceBatch = msg.Data.EventBatchData, RowIndex = i };

                if (weight > 0)
                {
                    if (_filter != null)
                    {
                        var rowEvent = RowEventToEventBatchData.RowReferenceToRowEvent(weight, 0, new ColumnRowReference() { referenceBatch = msg.Data.EventBatchData, RowIndex = i });
                        if (_filter(rowEvent))
                        {
                            await Upsert(i, columnRef, toEmitOffsets, weights, deleteBatchKeyOffsets, deleteBatchColumns);
                        }
                        else
                        {
                            await Delete(columnRef, deleteBatchKeyOffsets, deleteBatchColumns);
                        }
                    }
                    else
                    {
                        await Upsert(i, columnRef, toEmitOffsets, weights, deleteBatchKeyOffsets, deleteBatchColumns);
                    }
                }
                else
                {
                    await Delete(columnRef, deleteBatchKeyOffsets, deleteBatchColumns);
                }
            }

            for (int i = 0; i < toEmitOffsets.Count; i++)
            {
                iterations.Add(0);
            }

            if (_normalizationRelation.EmitSet)
            {
                IColumn[] columns = new IColumn[_normalizationRelation.Emit.Count];
                for (int i = 0; i < _normalizationRelation.Emit.Count; i++)
                {
                    columns[i] = new ColumnWithOffset(msg.Data.EventBatchData.Columns[_normalizationRelation.Emit[i]], toEmitOffsets, false);
                }
                yield return new StreamEventBatch(new EventBatchWeighted(weights, iterations, new EventBatchData(columns)));

                if (deleteBatchKeyOffsets.Count > 0)
                {
                    PrimitiveList<int> deleteWeights = new PrimitiveList<int>(otherColumnsMemoryManager);
                    PrimitiveList<uint> deleteIterations = new PrimitiveList<uint>(otherColumnsMemoryManager);

                    for (int i = 0; i < deleteBatchKeyOffsets.Count; i++)
                    {
                        deleteWeights.Add(-1);
                        deleteIterations.Add(0);
                    }

                    IColumn[] deleteColumns = new IColumn[_normalizationRelation.OutputLength];
                    for (int i = 0; i < _keyColumns.Count; i++)
                    {
                        if (_normalizationRelation.Emit.Contains(_keyColumns[i]))
                        {
                            deleteColumns[_keyColumns[i]] = new ColumnWithOffset(msg.Data.EventBatchData.Columns[_keyColumns[i]], deleteBatchKeyOffsets, false);
                        }
                    }
                    for (int i = 0; i < _otherColumns.Count; i++)
                    {
                        if (_normalizationRelation.Emit.Contains(_otherColumns[i]))
                        {
                            deleteColumns[_otherColumns[i]] = deleteBatchColumns[i];
                        }
                    }

                    yield return new StreamEventBatch(new EventBatchWeighted(deleteWeights, deleteIterations, new EventBatchData(deleteColumns)));
                }
                else
                {
                    for (int i = 0; i < deleteBatchColumns.Count; i++)
                    {
                        deleteBatchColumns[i].Dispose();
                    }
                }
            }
            else
            {
                yield return msg;
            }
            _eventsCounter.Add(msg.Data.Weights.Count);
        }

        private async Task Delete(ColumnRowReference columnRef, PrimitiveList<int> deleteBatchKeyOffsets, List<IColumn> deleteBatchColumns)
        {
            var (operation, _) = await _tree!.RMW(
                    in columnRef,
                    in columnRef,
                    (input, current, found) =>
                    {
                        if (found)
                        {
                            deleteBatchKeyOffsets.Add(input.RowIndex);
                            for (int k = 0; k < _otherColumns.Count; k++)
                            {
                                deleteBatchColumns[k].Add(current.referenceBatch.Columns[_otherColumns[k]].GetValueAt(current.RowIndex, default));
                            }
                            return (default, GenericWriteOperation.Delete);
                        }
                        return (default, GenericWriteOperation.None);
                    });
        }

        private async Task Upsert(
            int index,
            ColumnRowReference columnRef,
            PrimitiveList<int> toEmitOffsets, 
            PrimitiveList<int> weights,
            PrimitiveList<int> deleteBatchKeyOffsets, 
            List<IColumn> deleteBatchColumns)
        {
            var (operation, _) = await _tree!.RMW(
                    in columnRef,
                    in columnRef,
                    (input, current, found) =>
                    {
                        if (found)
                        {
                            // Compare here
                            for (int i = 0; i < _otherColumns.Count; i++)
                            {
                                var compareResult = DataValueComparer.Instance.Compare(
                                    input.referenceBatch.Columns[_otherColumns[i]].GetValueAt(input.RowIndex, default),
                                    current.referenceBatch.Columns[i].GetValueAt(current.RowIndex, default));

                                if (compareResult != 0)
                                {
                                    // Did not match, add the current to the delete batch
                                    deleteBatchKeyOffsets.Add(input.RowIndex);
                                    for (int k = 0; k < _otherColumns.Count; k++)
                                    {
                                        deleteBatchColumns[k].Add(current.referenceBatch.Columns[_otherColumns[k]].GetValueAt(current.RowIndex, default));
                                    }
                                    return (input, GenericWriteOperation.Upsert);
                                }
                            }
                            return (current, GenericWriteOperation.None);
                        }
                        return (input, GenericWriteOperation.Upsert);
                    });

            if (operation == GenericWriteOperation.Upsert)
            {
                toEmitOffsets.Add(index);
                weights.Add(1);
            }
        }

        protected override async Task InitializeOrRestore(NormalizationState? state, IStateManagerClient stateManagerClient)
        {
            Logger.InitializingNormalizationOperator(StreamName, Name);
            if (_eventsCounter == null)
            {
                _eventsCounter = Metrics.CreateCounter<long>("events");
            }
            if (_eventsProcessed == null)
            {
                _eventsProcessed = Metrics.CreateCounter<long>("events_processed");
            }

            _tree = await stateManagerClient.GetOrCreateTree("input",
                new BPlusTreeOptions<ColumnRowReference, ColumnRowReference, NormalizeKeyStorage, NormalizeValueStorage>()
                {
                    Comparer = new NormalizeTreeComparer(_normalizationRelation.KeyIndex),
                    KeySerializer = new NormalizeKeyStorageSerializer(_normalizationRelation.KeyIndex),
                    ValueSerializer = new NormalizeValueSerializer(_otherColumns)
                });
        }
    }
}
