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
using FlowtideDotNet.Core.Operators.Join.MultiJoin;
using FlowtideDotNet.Storage.StateManager.Internal.Sync;

namespace FlowtideDotNet.Core.Operators.LateMaterialization
{
    // One per participant, used by one thread
    internal sealed class RowHeapReader : IDisposable
    {
        private readonly RowHeap _heap;
        private readonly PageWindow<RowHeapPage> _window;
        private readonly DataValueContainer _container;
        private ulong[] _keys;
        private int[] _lookup;
        private int[] _positions;
        // Where each run of one sequence starts, and its page
        private int[] _runStarts;
        private long[] _runPages;
        private ulong[] _movedKeys;
        private int[] _movedOrder;
        private long _coldLoads;
        private int _disposed;

        internal RowHeapReader(RowHeap heap, PageWindow<RowHeapPage> window)
        {
            _heap = heap;
            _window = window;
            _container = new DataValueContainer();
            _keys = Array.Empty<ulong>();
            _lookup = Array.Empty<int>();
            _positions = Array.Empty<int>();
            _runStarts = new int[1];
            _runPages = Array.Empty<long>();
            _movedKeys = Array.Empty<ulong>();
            _movedOrder = Array.Empty<int>();
        }

        public int HeldPages => _window.Count;

        // Pages this reader found outside the cache
        public long ColdLoads => _coldLoads;

        // Appends the asked columns of the referenced rows, page by page.
        // Output row i belongs to the batch row rowOrder[i].
        // After an error the output and the row order hold no usable rows.
        public ValueTask GatherAsync(IColumn references, int count, IReadOnlyList<int> columns, Column[] output, int[] rowOrder)
        {
            if (columns.Count != output.Length)
            {
                throw new ArgumentException($"Got {output.Length} output columns for {columns.Count} asked columns.", nameof(output));
            }
            if (count < 0 || count > references.Count)
            {
                throw new ArgumentException($"{count} references are asked for, the column holds {references.Count}.", nameof(count));
            }
            if (rowOrder.Length < count)
            {
                throw new ArgumentException($"The row order has room for {rowOrder.Length} rows, {count} are asked for.", nameof(rowOrder));
            }
            for (int i = 0; i < columns.Count; i++)
            {
                if (columns[i] < 0 || columns[i] >= _heap.ColumnCount)
                {
                    throw new ArgumentException($"Column {columns[i]} is asked for, row heap '{_heap.Name}' stores {_heap.ColumnCount} columns.", nameof(columns));
                }
            }
            ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
            if (count == 0)
            {
                return ValueTask.CompletedTask;
            }
            int nulls = SortByPage(references, count, rowOrder);
            if (nulls > 0)
            {
                for (int i = 0; i < output.Length; i++)
                {
                    output[i].InsertNullRange(output[i].Count, nulls);
                }
            }
            int runs = FindPages(nulls, count, rowOrder);
            _heap.Options.PagesResolvedHookForTests?.Invoke();
            // Resident pages are copied here, the first cold page goes on in the slow path
            int run = 0;
            while (run < runs)
            {
                if (!_heap.Client.TryGet(_heap.Client.FirstPageId + _runPages[run], _window, out var page))
                {
                    return GatherAsync_Slow(run, runs, columns, output);
                }
                run = CopyPage(page, run, runs, columns, output);
            }
            return ValueTask.CompletedTask;
        }

        private async ValueTask GatherAsync_Slow(int run, int runs, IReadOnlyList<int> columns, Column[] output)
        {
            while (run < runs)
            {
                long pageId = _heap.Client.FirstPageId + _runPages[run];
                if (!_heap.Client.TryGet(pageId, _window, out var page))
                {
                    _coldLoads++;
                    page = await _heap.Client.GetAsync(pageId, _window).ConfigureAwait(false);
                }
                run = CopyPage(page, run, runs, columns, output);
            }
        }

        // Fills the sorted references and the row each came from, returns the null count
        private int SortByPage(IColumn references, int count, int[] rowOrder)
        {
            if (_keys.Length < count)
            {
                int size = Math.Max(count, _keys.Length * 2);
                _keys = new ulong[size];
                _lookup = new int[size];
                _positions = new int[size];
                _runStarts = new int[size + 1];
                _runPages = new long[size];
            }
            int nulls = 0;
            bool sorted = true;
            ulong previous = 0;
            for (int i = 0; i < count; i++)
            {
                ulong reference = ReadReference(references, i);
                if (reference == 0)
                {
                    nulls++;
                }
                if (reference < previous)
                {
                    sorted = false;
                }
                previous = reference;
                _keys[i] = reference;
                rowOrder[i] = i;
            }
            if (!sorted)
            {
                // A null reference is zero, so nulls come first
                Array.Sort(_keys, rowOrder, 0, count);
            }
            return nulls;
        }

        // Zero stands for a null reference, no real reference is zero
        private ulong ReadReference(IColumn references, int index)
        {
            references.GetValueAt(index, _container, default);
            if (_container.Type == ArrowTypeId.Null)
            {
                return 0;
            }
            if (_container.Type != ArrowTypeId.RowRef)
            {
                throw new InvalidOperationException($"Row heap '{_heap.Name}' got a value of type {_container.Type} where a row reference was expected.");
            }
            ulong reference = (ulong)_container.AsLong;
            if (reference == 0)
            {
                throw new InvalidOperationException($"Row heap '{_heap.Name}' got the row reference 0, which no row has.");
            }
            return reference;
        }

        // Finds the page of each run, before any page is read
        private int FindPages(int position, int count, int[] rowOrder)
        {
            int runs = 0;
            bool grouped = true;
            long previous = -1;
            while (position < count)
            {
                long sequence = RowReference.Sequence(_keys[position]);
                long entry = _heap.Directory.Get(sequence);
                if (entry == 0)
                {
                    throw new InvalidOperationException($"Row reference to page {sequence} in heap '{_heap.Name}' points at no page.");
                }
                long page = RowHeap.PageOf(entry);
                if (page < previous)
                {
                    grouped = false;
                }
                previous = page;
                _runStarts[runs] = position;
                _runPages[runs] = page;
                runs++;
                position++;
                while (position < count && RowReference.Sequence(_keys[position]) == sequence)
                {
                    position++;
                }
            }
            _runStarts[runs] = count;
            if (!grouped)
            {
                GroupByPage(runs, count, rowOrder);
            }
            return runs;
        }

        // Groups runs by page so a merged page is read once
        private void GroupByPage(int runs, int count, int[] rowOrder)
        {
            if (_movedKeys.Length < _keys.Length)
            {
                _movedKeys = new ulong[_keys.Length];
                _movedOrder = new int[_keys.Length];
            }
            var order = _lookup;
            var starts = _positions;
            for (int i = 0; i < runs; i++)
            {
                order[i] = i;
            }
            Array.Sort(_runPages, order, 0, runs);
            int first = _runStarts[0];
            int at = first;
            for (int i = 0; i < runs; i++)
            {
                int start = _runStarts[order[i]];
                int length = _runStarts[order[i] + 1] - start;
                Array.Copy(_keys, start, _movedKeys, at, length);
                Array.Copy(rowOrder, start, _movedOrder, at, length);
                starts[i] = at;
                at += length;
            }
            Array.Copy(_movedKeys, first, _keys, first, count - first);
            Array.Copy(_movedOrder, first, rowOrder, first, count - first);
            Array.Copy(starts, _runStarts, runs);
        }

        // Copies one page's rows, returns the run after its last
        private int CopyPage(RowHeapPage page, int run, int runs, IReadOnlyList<int> columns, Column[] output)
        {
            try
            {
                long number = _runPages[run];
                var map = page.Map;
                int length = 0;
                do
                {
                    int start = _runStarts[run];
                    int end = _runStarts[run + 1];
                    if (map == null)
                    {
                        FindRows(page, start, end, length);
                    }
                    else
                    {
                        FindMergedRows(map, start, end, length);
                    }
                    length += end - start;
                    run++;
                }
                while (run < runs && _runPages[run] == number);
                for (int i = 0; i < output.Length; i++)
                {
                    Append(output[i], page.Data.Columns[columns[i]], _lookup, _positions, length);
                }
                return run;
            }
            finally
            {
                _window.Release(page);
            }
        }

        // An appended page, the row of a slot is the slot
        private void FindRows(RowHeapPage page, int start, int end, int at)
        {
            long sequence = RowReference.Sequence(_keys[start]);
            if (page.Sequence != sequence)
            {
                throw new InvalidOperationException($"Row heap '{_heap.Name}' read page {page.Sequence} where page {sequence} was asked for.");
            }
            for (int i = start; i < end; i++)
            {
                int slot = RowReference.Slot(_keys[i]);
                if (slot >= page.Count)
                {
                    throw new InvalidOperationException($"Row reference to slot {slot} of page {sequence} in heap '{_heap.Name}' is beyond the page's {page.Count} rows.");
                }
                _lookup[at++] = slot;
            }
        }

        // A merged page maps each sequence to its rows
        private void FindMergedRows(RowHeapSlotMap map, int start, int end, int at)
        {
            long sequence = RowReference.Sequence(_keys[start]);
            int hosted = map.Find(sequence);
            if (hosted < 0)
            {
                throw new InvalidOperationException($"Row heap '{_heap.Name}' read a merged page without the rows of page {sequence}.");
            }
            for (int i = start; i < end; i++)
            {
                int slot = RowReference.Slot(_keys[i]);
                int row = map.Row(hosted, slot);
                if (row < 0)
                {
                    throw new InvalidOperationException($"Row reference to slot {slot} of page {sequence} in heap '{_heap.Name}' points at a row that was released.");
                }
                _lookup[at++] = row;
            }
        }

        // The same choice of copy as multi join makes for its output
        internal static void Append(Column target, IColumn source, int[] rows, int[] scratch, int length)
        {
            if (MultiJoinTreeLookup.AppendedAsNulls(target, source, length))
            {
                return;
            }
            ReadOnlySpan<int> lookup = rows.AsSpan(0, length);
            if (MultiJoinTreeLookup.TakesRowsAsObjects(target, source))
            {
                MultiJoinTreeLookup.CopyRuns(target, source, lookup);
                return;
            }
            int end = target.Count;
            for (int i = 0; i < length; i++)
            {
                scratch[i] = end;
            }
            ReadOnlySpan<int> positions = scratch.AsSpan(0, length);
            target.InsertFrom(source, in lookup, in positions, ColumnWithOffset.NullValueIndex);
        }

        public void Dispose()
        {
            // The heap may dispose a reader its owner disposes too
            if (Interlocked.Exchange(ref _disposed, 1) != 0)
            {
                return;
            }
            _heap.RemoveReader(this);
            _window.Dispose();
        }
    }
}
