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

using FlowtideDotNet.Base;
using FlowtideDotNet.Base.Metrics;
using FlowtideDotNet.Base.Vertices.Unary;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Serializers;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.Tree;
using FlowtideDotNet.Substrait.Relations;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Core.Operators.Buffer
{
    internal class BufferOperator : UnaryVertex<StreamEventBatch>
    {
        private ICounter<long>? _eventsCounter;
        private ICounter<long>? _eventsProcessed;
        private readonly BufferRelation _bufferRelation;
        private IBPlusTree<ColumnRowReference, int, ColumnKeyStorageContainer, PrimitiveListValueContainer<int>>? _tree;

        public BufferOperator(BufferRelation bufferRelation, ExecutionDataflowBlockOptions executionDataflowBlockOptions) : base(executionDataflowBlockOptions)
        {
            this._bufferRelation = bufferRelation;
        }

        public override string DisplayName => "Buffer";

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
            return Task.FromResult<object?>(default);
        }

        protected override async IAsyncEnumerable<StreamEventBatch> OnWatermark(Watermark watermark)
        {
            Debug.Assert(_tree != null);
            Debug.Assert(_eventsCounter != null);

            var it = _tree.CreateIterator();
            await it.SeekFirst();

            await foreach (var page in it)
            {
                IColumn[] columns = new IColumn[_bufferRelation.OutputLength];
                PrimitiveList<int> weights = page.Values.GetPrimitiveListCopy(MemoryAllocator);
                PrimitiveList<uint> iterations = new PrimitiveList<uint>(MemoryAllocator);

                iterations.InsertStaticRange(0, 0, weights.Count);

                for (int i = 0; i < page.Keys._data.Columns.Count; i++)
                {
                     columns[i] = page.Keys._data.Columns[i].Copy(MemoryAllocator);
                }
                _eventsCounter.Add(weights.Count);
                yield return new StreamEventBatch(new EventBatchWeighted(weights, iterations, new EventBatchData(columns)));
            }

            await _tree.Clear();
        }

        public override async IAsyncEnumerable<StreamEventBatch> OnRecieve(StreamEventBatch msg, long time)
        {
            Debug.Assert(_tree != null);
            Debug.Assert(_eventsProcessed != null);
            _eventsProcessed.Add(msg.Events.Count);

            EventBatchData? outputData = default;

            if (_bufferRelation.EmitSet)
            {
                var columns = new IColumn[_bufferRelation.OutputLength];

                for (int i = 0; i < _bufferRelation.Emit.Count; i++)
                {
                    columns[i] = msg.Data.EventBatchData.Columns[_bufferRelation.Emit[i]];
                }
                outputData = new EventBatchData(columns);
            }
            else
            {
                outputData = msg.Data.EventBatchData;
            }

            for (int i = 0; i < msg.Data.Weights.Count; i++)
            {
                var rowRef = new ColumnRowReference() { referenceBatch = outputData, RowIndex = i };

                await _tree.RMWNoResult(rowRef, msg.Data.Weights[i], (input, current, exists) =>
                {
                    if (exists)
                    {
                        var val = current + input;
                        if (val == 0)
                        {
                            return (val, GenericWriteOperation.Delete);
                        }
                        return (val, GenericWriteOperation.Upsert);
                    }
                    else
                    {
                        if (input == 0)
                        {
                            return (input, GenericWriteOperation.None);
                        }
                        return (input, GenericWriteOperation.Upsert);
                    }
                });
            }
            yield break;
        }

        protected override async Task InitializeOrRestore(IStateManagerClient stateManagerClient)
        {
            if (_eventsCounter == null)
            {
                _eventsCounter = Metrics.CreateCounter<long>("events");
            }
            if (_eventsProcessed == null)
            {
                _eventsProcessed = Metrics.CreateCounter<long>("events_processed");
            }

            _tree = await stateManagerClient.GetOrCreateTree("tree", new BPlusTreeOptions<ColumnRowReference, int, ColumnKeyStorageContainer, PrimitiveListValueContainer<int>>()
            {
                Comparer = new ColumnComparer(_bufferRelation.OutputLength),
                KeySerializer = new ColumnStoreSerializer(_bufferRelation.OutputLength, MemoryAllocator),
                MemoryAllocator = MemoryAllocator,
                UseByteBasedPageSizes = true,
                ValueSerializer = new PrimitiveListValueContainerSerializer<int>(MemoryAllocator)
            });
        }
    }
}
