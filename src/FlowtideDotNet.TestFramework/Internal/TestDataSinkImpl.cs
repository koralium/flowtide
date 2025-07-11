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

using Apache.Arrow.Memory;
using FlowtideDotNet.Base;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.Operators.Write;
using FlowtideDotNet.Core;
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
using static SqlParser.Ast.Statement;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.TestFramework.Internal
{
    internal class TestDataSinkImpl : WriteBaseOperator
    {
        private readonly WriteRelation writeRelation;
        private readonly Action<EventBatchData> onDataChange;
        private bool watermarkRecieved = false;
        private EventBatchData? _lastSentBatch;

        private IBPlusTree<ColumnRowReference, int, ColumnKeyStorageContainer, PrimitiveListValueContainer<int>>? _tree;

        public TestDataSinkImpl(
            WriteRelation writeRelation,
            ExecutionDataflowBlockOptions executionDataflowBlockOptions,
            Action<EventBatchData> onDataChange) : base(executionDataflowBlockOptions)
        {
            this.writeRelation = writeRelation;
            this.onDataChange = onDataChange;
        }

        public override string DisplayName => "Mock Data Sink";

        public override Task Compact()
        {
            return Task.CompletedTask;
        }

        public override Task DeleteAsync()
        {
            return Task.CompletedTask;
        }

        protected override async Task InitializeOrRestore(long restoreTime, IStateManagerClient stateManagerClient)
        {
            _tree = await stateManagerClient.GetOrCreateTree("sink", new BPlusTreeOptions<ColumnRowReference, int, ColumnKeyStorageContainer, PrimitiveListValueContainer<int>>()
            {
                Comparer = new ColumnComparer(writeRelation.OutputLength),
                KeySerializer = new ColumnStoreSerializer(writeRelation.OutputLength, MemoryAllocator),
                ValueSerializer = new PrimitiveListValueContainerSerializer<int>(MemoryAllocator),
                MemoryAllocator = MemoryAllocator,
                UseByteBasedPageSizes = true
            });
        }

        protected override Task OnWatermark(Watermark watermark)
        {
            watermarkRecieved = true;
            return base.OnWatermark(watermark);
        }

        protected override async Task OnCheckpoint(long checkpointTime)
        {
            Debug.Assert(_tree != null);

            Column[] columns = new Column[writeRelation.OutputLength];

            for (int i = 0; i < writeRelation.OutputLength; i++)
            {
                columns[i] = new Column(MemoryAllocator);
            }

            var iterator = _tree.CreateIterator();
            await iterator.SeekFirst();

            await foreach (var page in iterator)
            {
                foreach (var kv in page)
                {
                    if (kv.Value < 0)
                    {
                        throw new Exception("Row exist in sink with negaive weight: " + kv.Key.ToString());
                    }
                    for (int i = 0; i < kv.Key.referenceBatch.Columns.Count; i++)
                    {
                        var val = kv.Key.referenceBatch.Columns[i].GetValueAt(kv.Key.RowIndex, default);
                        for (int x = 0; x < kv.Value; x++)
                        {
                            columns[i].Add(val);
                        }
                    }
                }
            }

            var newData = new EventBatchData(columns);

            if (_lastSentBatch != null && watermarkRecieved)
            {
                _lastSentBatch.Dispose();
            }

            _lastSentBatch = newData;

            await _tree.Commit();

            if (watermarkRecieved)
            {
                onDataChange(newData);
                watermarkRecieved = false;
            }
        }

        protected override async Task OnRecieve(StreamEventBatch msg, long time)
        {
            Debug.Assert(_tree != null);
            for (int i = 0; i < msg.Data.Weights.Count; i++)
            {
                var rowRef = new ColumnRowReference() { referenceBatch = msg.Data.EventBatchData, RowIndex = i };
                var weight = msg.Data.Weights[i];
                await _tree.RMWNoResult(in rowRef, in weight, (input, current, exist) =>
                {
                    if (exist)
                    {
                        var newWeight = current + input;
                        if (newWeight == 0)
                        {
                            return (newWeight, GenericWriteOperation.Delete);
                        }
                        return (newWeight, GenericWriteOperation.Upsert);
                    }
                    return (input, GenericWriteOperation.Upsert);
                });
            }
        }
    }
}
