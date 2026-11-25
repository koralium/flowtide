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

using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Substrait.Relations;

namespace FlowtideDotNet.Core.Operators.LateMaterialization
{
    // One array for the fetches of an operator, they never overlap
    internal sealed class RowOrderScratch
    {
        private int[] _order = Array.Empty<int>();

        public int[] Rent(int count)
        {
            if (_order.Length < count)
            {
                _order = new int[Math.Max(count, 256)];
            }
            return _order;
        }
    }

    // Reads one fetch's columns from its store, batch by batch
    internal sealed class RowStoreFetcher : IDisposable
    {
        private readonly RowStoreHandle _handle;
        private readonly IMemoryAllocator _memoryAllocator;
        private RowHeapReader? _reader;
        private int _generation;
        // The heap's column of each fetched column
        private int[] _heapColumns = Array.Empty<int>();
        // Row order of one gather, shared by fetches in turn
        private readonly RowOrderScratch _scratch;
        // Cold loads of readers that are gone
        private long _coldLoads;

        public RowStoreFetcher(RowStoreFetch definition, RowStoreHandle handle, IMemoryAllocator memoryAllocator, RowOrderScratch scratch)
        {
            Definition = definition;
            _handle = handle;
            _memoryAllocator = memoryAllocator;
            _scratch = scratch;
        }

        public RowStoreFetch Definition { get; }

        // Pages this fetch had to load from storage
        public long ColdLoads => _coldLoads + (_reader?.ColdLoads ?? 0);

        // The store opens later, a reader comes with the first batch
        public void Dispose()
        {
            DropReader();
        }

        private void DropReader()
        {
            if (_reader != null)
            {
                _coldLoads += _reader.ColdLoads;
                _reader.Dispose();
                _reader = null;
            }
        }

        // Starts a read, the host waits before it closes
        private async ValueTask<RowHeapReader> BeginReadAsync(CancellationToken cancellationToken)
        {
            while (true)
            {
                int generation = _handle.Generation;
                var host = await _handle.GetHostAsync(cancellationToken).ConfigureAwait(false);
                if (!_handle.TryBeginRead(generation))
                {
                    // Closed or opened anew meanwhile
                    continue;
                }
                if (_reader != null && _generation == generation)
                {
                    return _reader;
                }
                try
                {
                    DropReader();
                    var stored = host.Layout.StoredColumnOfPosition;
                    if (stored == null)
                    {
                        throw new InvalidOperationException($"Row store '{_handle.Name}' was not set up by a relation, it cannot be fetched from.");
                    }
                    var columns = Definition.Columns;
                    var heapColumns = new int[columns.Count];
                    for (int i = 0; i < heapColumns.Length; i++)
                    {
                        if (columns[i] < 0 || columns[i] >= stored.Count)
                        {
                            throw new InvalidOperationException($"A fetch asks for stored column {columns[i]} of row store '{_handle.Name}', it stores {stored.Count}.");
                        }
                        heapColumns[i] = stored[columns[i]];
                    }
                    _heapColumns = heapColumns;
                    _reader = host.Store.CreateReader();
                    _generation = generation;
                    return _reader;
                }
                catch
                {
                    _handle.EndRead();
                    throw;
                }
            }
        }

        // The fetched columns, in the row order of the references
        public async ValueTask<(IColumn[] columns, PrimitiveList<int>? offsets)> FetchAsync(IColumn references, int count, CancellationToken cancellationToken)
        {
            var reader = await BeginReadAsync(cancellationToken).ConfigureAwait(false);
            Column[]? gathered = null;
            try
            {
                gathered = new Column[_heapColumns.Length];
                for (int i = 0; i < gathered.Length; i++)
                {
                    gathered[i] = Column.Create(_memoryAllocator);
                }
                var rowOrder = _scratch.Rent(count);
                // One pass over the store's pages, in reference order
                await reader.GatherAsync(references, count, _heapColumns, gathered, rowOrder).ConfigureAwait(false);
                bool inOrder = true;
                for (int i = 0; i < count; i++)
                {
                    if (rowOrder[i] != i)
                    {
                        inOrder = false;
                        break;
                    }
                }
                if (inOrder)
                {
                    return (gathered, null);
                }
                // Input row i is gathered row offsets[i]
                var offsets = new PrimitiveList<int>(_memoryAllocator, count);
                offsets.InsertStaticRange(0, 0, count);
                for (int i = 0; i < count; i++)
                {
                    offsets.Update(rowOrder[i], i);
                }
                try
                {
                    var views = new IColumn[gathered.Length];
                    for (int i = 0; i < views.Length; i++)
                    {
                        views[i] = new ColumnWithOffset(gathered[i], offsets);
                    }
                    return (views, offsets);
                }
                catch
                {
                    offsets.Dispose();
                    throw;
                }
            }
            catch
            {
                DisposeColumns(gathered);
                throw;
            }
            finally
            {
                _handle.EndRead();
            }
        }

        public static void DisposeColumns(IColumn[]? columns)
        {
            if (columns == null)
            {
                return;
            }
            for (int i = 0; i < columns.Length; i++)
            {
                columns[i]?.Dispose();
            }
        }
    }
}
