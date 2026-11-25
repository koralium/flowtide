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
using FlowtideDotNet.Base.Vertices;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Substrait.Relations;
using System.Diagnostics;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Core.Operators.LateMaterialization
{
    // Reads stored columns back through reference columns, keeps no state
    internal class FetchRequiredColumnsOperator : UnaryVertex<StreamEventBatch>
    {
        private readonly FetchRequiredColumnsRelation _relation;
        private readonly RowStoreHandle[] _handles;
        private readonly RowStoreFetcher?[] _fetches;
        private readonly RowOrderScratch _rowOrder = new RowOrderScratch();
        private readonly int _inputLength;
        // Scratch of one batch, batches come one at a time
        private readonly IColumn[][] _fetched;
        private readonly PrimitiveList<int>?[] _offsets;
        private readonly bool[][] _used;
        private ICounter<long>? _eventsCounter;
        private ICounter<long>? _eventsProcessed;

        public FetchRequiredColumnsOperator(FetchRequiredColumnsRelation relation, IReadOnlyList<RowStoreHandle> handles, ExecutionDataflowBlockOptions executionDataflowBlockOptions) : base(executionDataflowBlockOptions)
        {
            if (handles.Count != relation.Fetches.Count)
            {
                throw new ArgumentException($"Got {handles.Count} row stores for {relation.Fetches.Count} fetches.", nameof(handles));
            }
            _relation = relation;
            _inputLength = relation.Input.OutputLength;
            _handles = handles.ToArray();
            _fetches = new RowStoreFetcher?[relation.Fetches.Count];
            _fetched = new IColumn[_fetches.Length][];
            _offsets = new PrimitiveList<int>?[_fetches.Length];
            _used = new bool[_fetches.Length][];
            for (int i = 0; i < _fetches.Length; i++)
            {
                var fetch = relation.Fetches[i];
                if (fetch.ReferenceColumn < 0 || fetch.ReferenceColumn >= _inputLength)
                {
                    throw new InvalidOperationException($"A fetch from row store '{fetch.StoreName}' reads its references from column {fetch.ReferenceColumn}, the input has {_inputLength}.");
                }
                if (fetch.Columns.Count == 0)
                {
                    throw new InvalidOperationException($"A fetch from row store '{fetch.StoreName}' reads no column.");
                }
                for (int k = 0; k < i; k++)
                {
                    // One pass per store needs one fetch per reference column
                    if (relation.Fetches[k].ReferenceColumn == fetch.ReferenceColumn)
                    {
                        throw new InvalidOperationException($"Two fetches read through reference column {fetch.ReferenceColumn}, name every column of a store in one fetch.");
                    }
                }
                _used[i] = new bool[fetch.Columns.Count];
            }
        }

        public override string DisplayName => "Fetch required columns";

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

        protected override Task InitializeOrRestore(IStateManagerClient stateManagerClient)
        {
            if (_eventsCounter == null)
            {
                _eventsCounter = Metrics.CreateCounter<long>("events");
            }
            if (_eventsProcessed == null)
            {
                _eventsProcessed = Metrics.CreateCounter<long>("events_processed");
            }
            // The stores open later, readers come with the first batch
            DisposeReaders();
            for (int i = 0; i < _fetches.Length; i++)
            {
                _fetches[i] = new RowStoreFetcher(_relation.Fetches[i], _handles[i], MemoryAllocator, _rowOrder);
            }
            return Task.CompletedTask;
        }

        public override async ValueTask DisposeAsync()
        {
            DisposeReaders();
            await base.DisposeAsync();
        }

        private void DisposeReaders()
        {
            for (int i = 0; i < _fetches.Length; i++)
            {
                _fetches[i]?.Dispose();
                _fetches[i] = null;
            }
        }

        public override async IAsyncEnumerable<StreamEventBatch> OnRecieve(StreamEventBatch msg, long time)
        {
            Debug.Assert(_eventsProcessed != null);
            Debug.Assert(_eventsCounter != null);
            var data = msg.Data;
            int count = data.Count;
            _eventsProcessed.Add(count);

            var fetched = _fetched;
            // The offsets a fetch's views share, null when it needed none
            var offsets = _offsets;
            var used = _used;
            for (int i = 0; i < _fetches.Length; i++)
            {
                fetched[i] = Array.Empty<IColumn>();
                offsets[i] = null;
                Array.Clear(used[i]);
            }
            try
            {
                for (int i = 0; i < _fetches.Length; i++)
                {
                    (fetched[i], offsets[i]) = await _fetches[i]!.FetchAsync(data.EventBatchData.Columns[_relation.Fetches[i].ReferenceColumn], count, CancellationToken).ConfigureAwait(false);
                }
            }
            catch
            {
                for (int i = 0; i < fetched.Length; i++)
                {
                    RowStoreFetcher.DisposeColumns(fetched[i]);
                    offsets[i]?.Dispose();
                }
                throw;
            }

            int width = _relation.OutputLength;
            var outputColumns = new IColumn[width];
            for (int i = 0; i < width; i++)
            {
                int column = _relation.EmitSet ? _relation.Emit[i] : i;
                if (column < _inputLength)
                {
                    outputColumns[i] = data.EventBatchData.Columns[column];
                    continue;
                }
                int position = column - _inputLength;
                for (int f = 0; f < _fetches.Length; f++)
                {
                    if (position < fetched[f].Length)
                    {
                        // Sent twice it is one column at two places
                        outputColumns[i] = fetched[f][position];
                        used[f][position] = true;
                        break;
                    }
                    position -= fetched[f].Length;
                }
            }
            for (int f = 0; f < _fetches.Length; f++)
            {
                bool anySent = false;
                for (int c = 0; c < fetched[f].Length; c++)
                {
                    // Fetched and not sent
                    if (!used[f][c])
                    {
                        fetched[f][c].Dispose();
                    }
                    else
                    {
                        anySent = true;
                    }
                }
                if (!anySent)
                {
                    // No view is sent, so no rent will free the offsets
                    offsets[f]?.Dispose();
                }
            }
            _eventsCounter.Add(count);
            yield return new StreamEventBatch(new EventBatchWeighted(data.Weights, data.Iterations, new EventBatchData(outputColumns)));
        }
    }
}
