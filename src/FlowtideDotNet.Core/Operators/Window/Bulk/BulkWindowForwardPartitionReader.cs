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
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Tree;
using System.Diagnostics;

namespace FlowtideDotNet.Core.Operators.Window.Bulk
{
    /// <summary>
    /// Iterates one partition forwards, used for pre-scans and lookahead reads.
    /// </summary>
    internal class BulkWindowForwardPartitionReader : IDisposable
    {
        private readonly IBPlusTreeIterator<ColumnRowReference, BulkWindowValue, ColumnKeyStorageContainer, BulkWindowValueContainer> _iterator;
        private readonly BulkWindowPartitionComparer _partitionComparer;
        private readonly BulkWindowInsertComparer? _insertComparer;

        private IAsyncEnumerator<IBPlusTreePageIterator<ColumnRowReference, BulkWindowValue, ColumnKeyStorageContainer, BulkWindowValueContainer>>? _enumerator;
        private IBPlusTreePageIterator<ColumnRowReference, BulkWindowValue, ColumnKeyStorageContainer, BulkWindowValueContainer>? _currentPage;
        private ColumnRowReference _partitionRow;
        private bool _anchored;
        private int _currentIndex;
        private int _currentDup;
        private int _endIndex;
        private bool _firstPage;
        private bool _done;

        public BulkWindowForwardPartitionReader(
            IBPlusTree<ColumnRowReference, BulkWindowValue, ColumnKeyStorageContainer, BulkWindowValueContainer> tree,
            IReadOnlyList<int> partitionColumns,
            BulkWindowInsertComparer? insertComparer = null)
        {
            _iterator = tree.CreateIterator();
            _partitionComparer = new BulkWindowPartitionComparer(partitionColumns);
            _insertComparer = insertComparer;
        }

        public EventBatchData Batch => _currentPage!.Keys.Data;

        public int RowIndex => _currentIndex;

        public int Weight => _currentPage!.Values._weights.Get(_currentIndex);

        /// <summary>
        /// Starts at the row's partition, the reference must stay valid for the walk.
        /// </summary>
        public async ValueTask Reset(ColumnRowReference partitionRow)
        {
            _partitionRow = partitionRow;
            _currentPage = null;
            _firstPage = true;
            _done = false;
            _anchored = false;
            _currentDup = 0;
            await _iterator.Seek(partitionRow, _partitionComparer);
            _enumerator = _iterator.GetAsyncEnumerator();
        }

        /// <summary>
        /// Starts at the anchor row instead of the partition start, the anchor is copied.
        /// </summary>
        public async ValueTask ResetAtRow(ColumnRowReference anchorRow, IMemoryAllocator memoryAllocator)
        {
            Debug.Assert(_insertComparer != null, "An insert comparer is required for anchored resets");

            // The anchor must outlive the caller's page, the reader advances lazily.
            if (_anchorColumns == null)
            {
                _anchorColumns = new Column[anchorRow.referenceBatch.Columns.Count];
                for (int c = 0; c < _anchorColumns.Length; c++)
                {
                    _anchorColumns[c] = Column.Create(memoryAllocator);
                    _anchorColumns[c].Add(NullValue.Instance);
                }
                _anchorBatch = new EventBatchData(_anchorColumns);
            }
            for (int c = 0; c < _anchorColumns.Length; c++)
            {
                _anchorColumns[c].UpdateAt(0, anchorRow.referenceBatch.Columns[c].GetValueAt(anchorRow.RowIndex, default));
            }

            _partitionRow = new ColumnRowReference()
            {
                referenceBatch = _anchorBatch!,
                RowIndex = 0
            };
            _currentPage = null;
            _firstPage = true;
            _done = false;
            _anchored = true;
            _currentDup = 0;
            await _iterator.Seek(_partitionRow);
            _enumerator = _iterator.GetAsyncEnumerator();
        }

        private Column[]? _anchorColumns;
        private EventBatchData? _anchorBatch;

        /// <summary>
        /// Moves to the next physical row within the partition. Rows with non positive weights are skipped.
        /// </summary>
        public async ValueTask<bool> MoveNextRow()
        {
            Debug.Assert(_enumerator != null, "Reset must be called before MoveNextRow");
            if (_done)
            {
                return false;
            }
            while (true)
            {
                if (_currentPage != null)
                {
                    _currentIndex++;
                    while (_currentIndex <= _endIndex)
                    {
                        if (_currentPage.Values._weights.Get(_currentIndex) > 0)
                        {
                            return true;
                        }
                        _currentIndex++;
                    }
                }

                if (!await _enumerator.MoveNextAsync())
                {
                    _done = true;
                    return false;
                }
                var page = _enumerator.Current;
                if (page.CurrentPage == null || page.Keys == null || page.Keys.Count == 0)
                {
                    // Empty pages sit mid tree and do not end the partition.
                    _currentPage = null;
                    continue;
                }
                _partitionComparer.FindIndex(in _partitionRow, page.Keys);
                if (_partitionComparer.noMatch)
                {
                    if (_firstPage)
                    {
                        // The partition may begin on the next page when the seek landed at a page end.
                        _firstPage = false;
                        _currentPage = null;
                        continue;
                    }
                    _done = true;
                    return false;
                }
                var startIndex = _partitionComparer.start;
                if (_firstPage && _anchored)
                {
                    // Start at the anchor's position instead of the partition start.
                    var bounds = _insertComparer!.FindBoundries(in _partitionRow, page.Keys, startIndex, _partitionComparer.end);
                    var lower = bounds.lowerBounds;
                    if (lower < 0)
                    {
                        lower = ~lower;
                    }
                    startIndex = lower;
                    if (startIndex > _partitionComparer.end)
                    {
                        if (_partitionComparer.end >= page.Keys.Count - 1)
                        {
                            // The partition may continue on the next page.
                            _firstPage = false;
                            _currentPage = null;
                            continue;
                        }
                        _done = true;
                        return false;
                    }
                }
                _firstPage = false;
                _currentPage = page;
                _currentIndex = startIndex - 1;
                _endIndex = _partitionComparer.end;
            }
        }

        /// <summary>
        /// Moves to the next logical row, iterating each weight duplicate of a physical row.
        /// </summary>
        public async ValueTask<bool> MoveNextLogical()
        {
            if (_currentPage != null && _currentDup + 1 < Weight)
            {
                _currentDup++;
                return true;
            }
            _currentDup = 0;
            return await MoveNextRow();
        }

        public void Dispose()
        {
            _iterator.Dispose();
            if (_anchorColumns != null)
            {
                for (int c = 0; c < _anchorColumns.Length; c++)
                {
                    _anchorColumns[c].Dispose();
                }
                _anchorColumns = null;
            }
        }
    }
}
