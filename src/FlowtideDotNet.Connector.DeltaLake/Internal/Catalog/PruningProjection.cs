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

using FlowtideDotNet.Storage.Memory;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Catalog
{
    /// <summary>
    /// The pruning rows of the live files, a scan reads no cold record.
    /// Rows sit in an array indexed by file id under the process-wide reservation, or in a spillable tree when
    /// even the first column did not fit. The tree mode lasts until the next bootstrap.
    /// </summary>
    internal sealed class PruningProjection : IDisposable
    {
        // Set in the first byte of a row that holds a live file
        private const byte Live = 2;
        private const int MinimumCapacity = 64;

        private readonly IMemoryAllocator _memoryAllocator;
        private readonly PruningReservation _reservation;
        private readonly PruningGrant _grant = new PruningGrant();
        private readonly RotatingTree<byte[]> _tree;
        private readonly PruningLayout _fullLayout;
        private PruningLayout _layout;
        private FlowtideMemory _rows;
        private int _capacity;
        private bool _treeMode;
        private bool _disposed;

        private PruningProjection(IMemoryAllocator memoryAllocator, PruningReservation reservation, RotatingTree<byte[]> tree, PruningLayout layout)
        {
            _memoryAllocator = memoryAllocator;
            _reservation = reservation;
            _tree = tree;
            _fullLayout = layout;
            _layout = layout;
        }

        /// <summary>
        /// Sized for the expected number of files, the tree is cleared and owned by this projection.
        /// </summary>
        public static PruningProjection Create(IMemoryAllocator memoryAllocator, PruningReservation reservation, long capacityRequest, RotatingTree<byte[]> tree, PruningLayout layout, int expectedFiles, bool treeMode, out bool capacityMismatch)
        {
            var projection = new PruningProjection(memoryAllocator, reservation, tree, layout);
            capacityMismatch = !reservation.Register(projection._grant, capacityRequest);
            if (treeMode)
            {
                projection._treeMode = true;
            }
            else
            {
                projection.Reserve(expectedFiles);
            }
            return projection;
        }

        /// <summary>
        /// The columns a probe is matched against.
        /// </summary>
        public PruningLayout Layout => _layout;

        public bool TreeMode => _treeMode;

        public long ChargedBytes => _grant.Charged;

        public RotatingTree<byte[]> Tree => _tree;

        private void Reserve(int expectedFiles)
        {
            var capacity = MinimumCapacity;
            while (capacity < expectedFiles)
            {
                capacity *= 2;
            }
            var first = _fullLayout.Take(Math.Min(1, _fullLayout.Columns.Count));
            if ((long)capacity * first.RowSize > Array.MaxLength || !_reservation.TryCharge(_grant, (long)capacity * first.RowSize, firstColumn: true))
            {
                _treeMode = true;
                return;
            }
            _layout = first;
            if (_fullLayout.Columns.Count > 1 && (long)capacity * _fullLayout.RowSize <= Array.MaxLength && _reservation.TryCharge(_grant, (long)capacity * (_fullLayout.RowSize - first.RowSize), firstColumn: false))
            {
                _layout = _fullLayout;
                _reservation.SetExtraColumns(_grant, true);
            }
            _capacity = capacity;
            _rows = _memoryAllocator.AllocateMemory(capacity * _layout.RowSize);
            _rows.Span.Clear();
        }

        /// <summary>
        /// The row holds every column of the full layout, array mode keeps the columns it has room for.
        /// </summary>
        public async Task Set(int id, ReadOnlyMemory<byte> row)
        {
            if (_treeMode)
            {
                await _tree.Upsert(id, row.Slice(0, _layout.RowSize).ToArray());
                return;
            }
            if (id >= _capacity && !await Grow(id))
            {
                await _tree.Upsert(id, row.Slice(0, _layout.RowSize).ToArray());
                return;
            }
            WriteRow(id, row.Span);
        }

        private void WriteRow(int id, ReadOnlySpan<byte> row)
        {
            var target = _rows.Span.Slice(id * _layout.RowSize, _layout.RowSize);
            row.Slice(0, _layout.RowSize).CopyTo(target);
            target[0] |= Live;
        }

        public async Task Remove(int id)
        {
            if (_treeMode)
            {
                await _tree.Delete(id);
                return;
            }
            ClearRow(id);
        }

        private void ClearRow(int id)
        {
            if (id < _capacity)
            {
                _rows.Span.Slice(id * _layout.RowSize, _layout.RowSize).Clear();
            }
        }

        /// <summary>
        /// Visits every live file's row, the projection must not change during the scan.
        /// </summary>
        public async Task Scan(Action<int, ReadOnlyMemory<byte>> visit)
        {
            if (_treeMode)
            {
                await foreach (var (id, row) in _tree.ScanAll())
                {
                    visit(id, row);
                }
                return;
            }
            ScanArray(visit);
        }

        private void ScanArray(Action<int, ReadOnlyMemory<byte>> visit)
        {
            var rowSize = _layout.RowSize;
            var buffer = new byte[rowSize];
            for (int id = 0; id < _capacity; id++)
            {
                var row = _rows.Span.Slice(id * rowSize, rowSize);
                if ((row[0] & Live) == 0)
                {
                    continue;
                }
                row.CopyTo(buffer);
                visit(id, buffer);
            }
        }

        // Doubles the arrays, a refused first column moves the rows to the tree
        private async Task<bool> Grow(int id)
        {
            var capacity = _capacity;
            while (capacity <= id)
            {
                capacity *= 2;
            }
            if (_layout.Columns.Count > 1)
            {
                if (TryResize(capacity, firstColumn: false))
                {
                    return true;
                }
                DropExtraColumns();
            }
            if (TryResize(capacity, firstColumn: true))
            {
                return true;
            }
            await MoveToTree();
            return false;
        }

        // The new size is charged before the copy and the old one released after it
        private bool TryResize(int capacity, bool firstColumn)
        {
            var bytes = (long)capacity * _layout.RowSize;
            if (bytes > Array.MaxLength || !_reservation.TryCharge(_grant, bytes, firstColumn))
            {
                return false;
            }
            var oldBytes = (long)_capacity * _layout.RowSize;
            _memoryAllocator.Realloc(ref _rows, (int)bytes);
            _rows.Span.Slice((int)oldBytes).Clear();
            _capacity = capacity;
            _reservation.Release(_grant, oldBytes);
            return true;
        }

        /// <summary>
        /// Gives up the columns after the first when another table needs room for its first column.
        /// </summary>
        public void ApplyRevocation()
        {
            if (_reservation.TakeRevocation(_grant) && !_treeMode && _layout.Columns.Count > 1)
            {
                DropExtraColumns();
            }
        }

        // Compacted in place, every row moves to or before its old start, then the block shrinks
        private void DropExtraColumns()
        {
            var first = _fullLayout.Take(1);
            var oldSize = _layout.RowSize;
            var span = _rows.Span;
            for (int id = 1; id < _capacity; id++)
            {
                span.Slice(id * oldSize, first.RowSize).CopyTo(span.Slice(id * first.RowSize, first.RowSize));
            }
            _memoryAllocator.Realloc(ref _rows, _capacity * first.RowSize);
            _layout = first;
            _reservation.Release(_grant, (long)_capacity * (oldSize - first.RowSize));
            _reservation.SetExtraColumns(_grant, false);
        }

        // Once per run, the rows keep the columns the arrays had
        private async Task MoveToTree()
        {
            foreach (var (id, row) in LiveRows())
            {
                await _tree.Upsert(id, row);
            }
            _memoryAllocator.Free(ref _rows);
            _reservation.Release(_grant, _grant.Charged);
            _capacity = 0;
            _treeMode = true;
        }

        private List<(int Id, byte[] Row)> LiveRows()
        {
            var rows = new List<(int, byte[])>();
            ScanArray((id, row) => rows.Add((id, row.ToArray())));
            return rows;
        }

        public void Dispose()
        {
            if (_disposed)
            {
                return;
            }
            _disposed = true;
            if (!_rows.IsNull)
            {
                _memoryAllocator.Free(ref _rows);
            }
            _reservation.Unregister(_grant);
        }
    }
}
