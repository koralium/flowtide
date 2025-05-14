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
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Core.Compute.Columnar;
using FlowtideDotNet.Core.Utils;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.Tree;
using FlowtideDotNet.Substrait.Relations;
using System.Diagnostics;
using System.Threading.Tasks.Dataflow;
using static SqlParser.Ast.DataType;

namespace FlowtideDotNet.Core.Operators.Normalization
{
    internal class ColumnNormalizationOperator : UnaryVertex<StreamEventBatch>
    {
#if DEBUG_WRITE
        private StreamWriter? allOutput;
#endif

        private readonly NormalizationRelation _normalizationRelation;
        private IBPlusTree<ColumnRowReference, ColumnRowReference, NormalizeKeyStorage, NormalizeValueStorage>? _tree;
        private IBplusTreeUpdater<ColumnRowReference, ColumnRowReference, NormalizeKeyStorage, NormalizeValueStorage>? _updater;
        private readonly List<int> _keyColumns;
        private readonly List<int> _otherColumns;
        private readonly Func<EventBatchData, int, bool>? _filter;

        private ICounter<long>? _eventsCounter;
        private ICounter<long>? _eventsProcessed;

        private List<int> _emitList;

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
                _filter = ColumnBooleanCompiler.Compile(normalizationRelation.Filter, functionsRegister);
            }

            if (normalizationRelation.EmitSet)
            {
                _emitList = normalizationRelation.Emit;
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
                _emitList = new List<int>();
                for (int i = 0; i < normalizationRelation.OutputLength; i++)
                {
                    _emitList.Add(i);
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

        public override async Task OnCheckpoint()
        {
#if DEBUG_WRITE
            allOutput!.WriteLine("Checkpoint");
            await allOutput!.FlushAsync();
#endif
            await _updater!.Commit();
            await _tree!.Commit();
        }

        public override async IAsyncEnumerable<StreamEventBatch> OnRecieve(StreamEventBatch msg, long time)
        {
            Debug.Assert(_eventsProcessed != null);
            Debug.Assert(_eventsCounter != null);

            var otherColumnsMemoryManager = MemoryAllocator;

            PrimitiveList<int> toEmitOffsets = new PrimitiveList<int>(otherColumnsMemoryManager);
            PrimitiveList<int> weights = new PrimitiveList<int>(otherColumnsMemoryManager);
            PrimitiveList<uint> iterations = new PrimitiveList<uint>(otherColumnsMemoryManager);

            PrimitiveList<int> deleteBatchKeyOffsets = new PrimitiveList<int>(otherColumnsMemoryManager);
            List<IColumn> deleteBatchColumns = new List<IColumn>();

            for (int i = 0; i < _otherColumns.Count; i++)
            {
                deleteBatchColumns.Add(Column.Create(otherColumnsMemoryManager));
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
                        if (_filter(msg.Data.EventBatchData, i))
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

            if (weights.Count > 0)
            {
                IColumn[] columns = new IColumn[_emitList.Count];

                for (int i = 0; i < _emitList.Count; i++)
                {
                    columns[i] = new ColumnWithOffset(msg.Data.EventBatchData.Columns[_emitList[i]], toEmitOffsets, false);
                }

                yield return new StreamEventBatch(new EventBatchWeighted(weights, iterations, new EventBatchData(columns)));
            }


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
                    var emitIndex = _emitList.IndexOf(_keyColumns[i]);
                    if (emitIndex >= 0)
                    {
                        deleteColumns[emitIndex] = new ColumnWithOffset(msg.Data.EventBatchData.Columns[_keyColumns[i]], deleteBatchKeyOffsets, false);
                    }
                }
                for (int i = 0; i < _otherColumns.Count; i++)
                {
                    var emitIndex = _emitList.IndexOf(_otherColumns[i]);
                    if (emitIndex >= 0)
                    {
                        deleteColumns[emitIndex] = deleteBatchColumns[i];
                    }
                }

                var outputBatch = new StreamEventBatch(new EventBatchWeighted(deleteWeights, deleteIterations, new EventBatchData(deleteColumns)));
#if DEBUG_WRITE
                foreach (var o in outputBatch.Events)
                {
                    allOutput!.WriteLine($"{o.Weight} {o.ToJson()}");
                }
                await allOutput!.FlushAsync();
#endif
                yield return outputBatch;
            }
            else
            {
                for (int i = 0; i < deleteBatchColumns.Count; i++)
                {
                    deleteBatchColumns[i].Dispose();
                }
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
                                deleteBatchColumns[k].Add(current.referenceBatch.Columns[k].GetValueAt(current.RowIndex, default));
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
            Debug.Assert(_updater != null);

            await _updater.Seek(in columnRef);

            bool updated = false;
            if (_updater.Found)
            {
                var current = _updater.GetValue();
                for (int i = 0; i < _otherColumns.Count; i++)
                {
                    var compareResult = DataValueComparer.Instance.Compare(
                    columnRef.referenceBatch.Columns[_otherColumns[i]].GetValueAt(columnRef.RowIndex, default),
                        current.referenceBatch.Columns[i].GetValueAt(current.RowIndex, default));

                    if (compareResult != 0)
                    {
                        // Did not match, add the current to the delete batch
                        deleteBatchKeyOffsets.Add(columnRef.RowIndex);
                        for (int k = 0; k < _otherColumns.Count; k++)
                        {
                            deleteBatchColumns[k].Add(current.referenceBatch.Columns[k].GetValueAt(current.RowIndex, default));
                        }
                        await _updater.Upsert(columnRef, columnRef);
                        updated = true;
                        break;
                    }
                }
            }
            else
            {
                await _updater.Upsert(columnRef, columnRef);
                updated = true;
            }

            if (updated)
            {
                toEmitOffsets.Add(index);
                weights.Add(1);
            }
        }

        protected override async Task InitializeOrRestore(IStateManagerClient stateManagerClient)
        {
#if DEBUG_WRITE
            if (!Directory.Exists("debugwrite"))
                {
                    Directory.CreateDirectory("debugwrite");
                }
            if (allOutput == null)
            {
                allOutput = File.CreateText($"debugwrite/{StreamName}_{Name}.alloutput.txt");
            }
            else
            {
                allOutput.WriteLine("Restart");
                allOutput.Flush();
            }
#endif
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
                    KeySerializer = new NormalizeKeyStorageSerializer(_normalizationRelation.KeyIndex, MemoryAllocator),
                    ValueSerializer = new NormalizeValueSerializer(_otherColumns, MemoryAllocator),
                    UseByteBasedPageSizes = true,
                    MemoryAllocator = MemoryAllocator
                });
            _updater = _tree.CreateUpdater();
        }
    }
}
