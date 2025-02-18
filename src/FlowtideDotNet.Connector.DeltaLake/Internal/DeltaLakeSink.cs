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
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta;
using FlowtideDotNet.Core;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Core.Operators.Write;
using FlowtideDotNet.Storage.Serializers;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.Tree;
using FlowtideDotNet.Storage.Tree.Internal;
using FlowtideDotNet.Substrait.Relations;
using Stowage;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Connector.DeltaLake.Internal
{
    internal class DeltaLakeSink : WriteBaseOperator
    {
        private readonly DeltaLakeOptions _options;
        private readonly WriteRelation _writeRelation;
        private IBPlusTree<ColumnRowReference, int, ColumnKeyStorageContainer, PrimitiveListValueContainer<int>>? _temporaryTree;
        private string _tableName;
        private IOPath _tablePath;

        public DeltaLakeSink(DeltaLakeOptions options, WriteRelation writeRelation, ExecutionDataflowBlockOptions executionDataflowBlockOptions) : base(executionDataflowBlockOptions)
        {
            this._options = options;
            this._writeRelation = writeRelation;
            _tableName = string.Join("/", writeRelation.NamedObject.Names);
            _tablePath = _tableName;
        }

        public override string DisplayName => "DeltaLakeSink";

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
            _temporaryTree = await stateManagerClient.GetOrCreateTree("temporary", new BPlusTreeOptions<ColumnRowReference, int, ColumnKeyStorageContainer, PrimitiveListValueContainer<int>>()
            {
                Comparer = new ColumnComparer(_writeRelation.OutputLength),
                KeySerializer = new ColumnStoreSerializer(_writeRelation.OutputLength, MemoryAllocator),
                ValueSerializer = new PrimitiveListValueContainerSerializer<int>(MemoryAllocator),
                MemoryAllocator = MemoryAllocator,
                UseByteBasedPageSizes = true,
            });
        }

        protected override Task OnCheckpoint(long checkpointTime)
        {
            return SaveData();
        }

        private async Task SaveData()
        {
            Debug.Assert(_temporaryTree != null);
            using var iterator = _temporaryTree.CreateIterator();
            await iterator.SeekFirst();


            var table = await DeltaTransactionReader.ReadTable(_options.StorageLocation, _tablePath);

            List<LeafNode<ColumnRowReference, int, ColumnKeyStorageContainer, PrimitiveListValueContainer<int>>> negativeWeightPages = new List<LeafNode<ColumnRowReference, int, ColumnKeyStorageContainer, PrimitiveListValueContainer<int>>>();
            await foreach(var page in iterator)
            {
                bool addedToNegativeWeights = false;

                foreach(var kv in page)
                {
                    if (kv.Value < 0)
                    {
                        if (!addedToNegativeWeights)
                        {
                            addedToNegativeWeights = true;

                            if (!page.CurrentPage.TryRent())
                            {
                                throw new InvalidOperationException("Could not rent page");
                            }

                            // Add the page to negative weight pages if the page has negative weights
                            negativeWeightPages.Add(page.CurrentPage);
                        }
                    }
                }

               

                // How many pages of data that should be processed at the same time when searching existing data
                if (negativeWeightPages.Count == 4)
                {
                    // This part should try and find which files that should be scanned for each negative record.
                    // After that each file is scanned for the grouping and if found the delition vector of that file is updated.

                    // Important is that partitions must also be taken into consideration
                }
            }
        }

        protected override async Task OnRecieve(StreamEventBatch msg, long time)
        {
            Debug.Assert(_temporaryTree != null);

            for (int i = 0; i < msg.Data.Weights.Count; i++)
            {
                var rowRef = new ColumnRowReference() { referenceBatch = msg.Data.EventBatchData, RowIndex = i };
                await _temporaryTree.RMWNoResult(in rowRef, msg.Data.Weights[i], (input, current, exists) =>
                {
                    if (exists)
                    {
                        var newWeight = current + input;
                        if (newWeight == 0)
                        {
                            return (0, GenericWriteOperation.Delete);
                        }
                        return (newWeight, GenericWriteOperation.Upsert);
                    }
                    return (input, GenericWriteOperation.Upsert);
                });
            }
        }
    }
}
