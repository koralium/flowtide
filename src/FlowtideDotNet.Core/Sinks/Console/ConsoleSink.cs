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
using FlowtideDotNet.Base.Vertices.Egress;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Serializers;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.Tree;
using FlowtideDotNet.Substrait.Relations;
using System.Diagnostics;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Core.Sinks
{
    internal class ConsoleSink : EgressVertex<StreamEventBatch>
    {
        private readonly WriteRelation writeRelation;
        private IBPlusTree<ColumnRowReference, int, ColumnKeyStorageContainer, PrimitiveListValueContainer<int>>? _tree;
        private bool m_initialDataSent;

        public ConsoleSink(WriteRelation writeRelation, ExecutionDataflowBlockOptions executionDataflowBlockOptions) : base(executionDataflowBlockOptions)
        {
            this.writeRelation = writeRelation;
        }

        public override string DisplayName => "Console";

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
            _tree = await stateManagerClient.GetOrCreateTree("tree", new BPlusTreeOptions<ColumnRowReference, int, ColumnKeyStorageContainer, PrimitiveListValueContainer<int>>()
            {
                Comparer = new ColumnComparer(writeRelation.OutputLength),
                KeySerializer = new ColumnStoreSerializer(writeRelation.OutputLength, MemoryAllocator),
                MemoryAllocator = MemoryAllocator,
                UseByteBasedPageSizes = true,
                ValueSerializer = new PrimitiveListValueContainerSerializer<int>(MemoryAllocator)
            });
        }

        protected override async Task<object> OnCheckpoint(long checkpointTime)
        {
            if (!m_initialDataSent)
            {
                await WriteData();
                m_initialDataSent = true;
            }
            return new object();
        }

        protected override async Task OnWatermark(Watermark watermark)
        {
            await WriteData();
        }

        private async Task WriteData()
        {
            Debug.Assert(_tree != null);

            var consoleTable = new ConsoleTable(writeRelation.TableSchema.Names.Prepend("Weight").ToArray());

            using var iterator = _tree.CreateIterator();
            await iterator.SeekFirst();

            using NativeBufferWriter memoryStream = new NativeBufferWriter(MemoryAllocator);
            Utf8JsonWriter writer = new Utf8JsonWriter(memoryStream);


            int count = 0;
            await foreach (var page in iterator)
            {
                foreach (var kv in page)
                {
                    var vals = new object[writeRelation.TableSchema.Names.Count + 1];
                    vals[0] = kv.Value;

                    for (int i = 0; i < writeRelation.TableSchema.Names.Count; i++)
                    {
                        kv.Key.referenceBatch.Columns[i].WriteToJson(in writer, kv.Key.RowIndex);
                        await writer.FlushAsync();
                        vals[i + 1] = Encoding.UTF8.GetString(memoryStream.WrittenSpan);
                        memoryStream.Reset();
                        writer.Reset();
                    }

                    consoleTable.AddRow(vals);
                    count++;

                    if (count > 100)
                    {
                        consoleTable.Write(Format.Default);
                        consoleTable.Rows.Clear();
                        count = 0;
                    }
                }
            }
            consoleTable.Write(Format.Default);
            consoleTable.Rows.Clear();
            await _tree.Clear();
        }


        protected override async Task OnRecieve(StreamEventBatch msg, long time)
        {
            Debug.Assert(_tree != null);

            for (int i = 0; i < msg.Data.Count; i++)
            {
                var row = new ColumnRowReference()
                {
                    referenceBatch = msg.Data.EventBatchData,
                    RowIndex = i
                };

                await _tree.RMWNoResult(row, msg.Data.Weights[i], (input, existing, found) =>
                {
                    if (found)
                    {
                        existing += input;
                        if (existing == 0)
                        {
                            return (existing, GenericWriteOperation.Delete);
                        }
                        return (existing, GenericWriteOperation.Upsert);
                    }
                    return (input, GenericWriteOperation.Upsert);
                });
            }
        }
    }
}
