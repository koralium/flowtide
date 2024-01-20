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
using FlowtideDotNet.Core.Operators.Set;
using FlowtideDotNet.Core.Storage;
using FlowtideDotNet.Storage.Serializers;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.Tree;
using FlowtideDotNet.Substrait.Relations;
using System.Diagnostics;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Core.Operators.Buffer
{
    internal class BufferOperator : UnaryVertex<StreamEventBatch, object?>
    {
        private ICounter<long>? _eventsCounter;
        private IBPlusTree<RowEvent, int>? _tree;
        private readonly BufferRelation bufferRelation;
        private ICounter<long>? _eventsProcessed;

        public BufferOperator(BufferRelation bufferRelation, ExecutionDataflowBlockOptions executionDataflowBlockOptions) : base(executionDataflowBlockOptions)
        {
            this.bufferRelation = bufferRelation;
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
            List<RowEvent> output = new List<RowEvent>();
            await foreach(var page in it)
            {
                foreach(var kv in page)
                {
                    output.Add(new RowEvent(kv.Value, 0, kv.Key.RowData));
                }

                if (output.Count > 100)
                {
                    _eventsCounter.Add(output.Count);
                    yield return new StreamEventBatch(output);
                    output = new List<RowEvent>();
                }
            }
            if (output.Count > 0)
            {
                _eventsCounter.Add(output.Count);
                yield return new StreamEventBatch(output);
            }
            await _tree.Clear();
        }

        public override async IAsyncEnumerable<StreamEventBatch> OnRecieve(StreamEventBatch msg, long time)
        {
            Debug.Assert(_tree != null);
            Debug.Assert(_eventsProcessed != null);
            _eventsProcessed.Add(msg.Events.Count);
            foreach(var e in msg.Events)
            {
                var ev = e;
                if (bufferRelation.EmitSet)
                {
                    ev = new RowEvent(e.Weight, e.Iteration, ArrayRowData.Create(e.RowData, bufferRelation.Emit));
                }
                await _tree.RMW(ev, ev.Weight, (input, current, exists) =>
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

        protected override async Task InitializeOrRestore(object? state, IStateManagerClient stateManagerClient)
        {
            if (_eventsCounter == null)
            {
                _eventsCounter = Metrics.CreateCounter<long>("events");
            }
            if (_eventsProcessed == null)
            {
                _eventsProcessed = Metrics.CreateCounter<long>("events_processed");
            }
            
            // Temporary tree for storing the input events
            _tree = await stateManagerClient.GetOrCreateTree("input", new FlowtideDotNet.Storage.Tree.BPlusTreeOptions<RowEvent, int>()
            {
                Comparer = new BPlusTreeStreamEventComparer(),
                KeySerializer = new StreamEventBPlusTreeSerializer(),
                ValueSerializer = new IntSerializer()
            });
        }
    }
}
