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
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Storage.Tree;
using System.Diagnostics;

namespace FlowtideDotNet.Core.Operators.Window.Bulk
{
    /// <summary>
    /// Walks physical rows of the persistent window tree backwards, starting at the row just before an
    /// anchor key and stopping at the partition start. Used to seed window function state when a scan
    /// starts in the middle of a partition and to rescan bounded frames.
    /// The anchor row reference must stay valid for the duration of the walk.
    /// </summary>
    internal class BulkWindowBackwardPartitionReader : IDisposable
    {
        private readonly IBPlusTreeIterator<ColumnRowReference, BulkWindowValue, ColumnKeyStorageContainer, BulkWindowValueContainer> _iterator;
        private readonly BulkWindowInsertComparer _insertComparer;
        private readonly BulkWindowPartitionComparer _partitionComparer;

        private IAsyncEnumerator<IBPlusTreePageIterator<ColumnRowReference, BulkWindowValue, ColumnKeyStorageContainer, BulkWindowValueContainer>>? _enumerator;
        private IBPlusTreePageIterator<ColumnRowReference, BulkWindowValue, ColumnKeyStorageContainer, BulkWindowValueContainer>? _currentPage;
        private ColumnRowReference _anchor;
        private int _currentIndex;
        private bool _firstPage;
        private bool _done;

        public BulkWindowBackwardPartitionReader(
            IBPlusTree<ColumnRowReference, BulkWindowValue, ColumnKeyStorageContainer, BulkWindowValueContainer> tree,
            BulkWindowInsertComparer insertComparer,
            IReadOnlyList<int> partitionColumns)
        {
            _iterator = tree.CreateBackwardIterator();
            _insertComparer = insertComparer;
            _partitionComparer = new BulkWindowPartitionComparer(partitionColumns);
        }

        public EventBatchData Batch => _currentPage!.Keys.Data;

        public int RowIndex => _currentIndex;

        public int Weight => _currentPage!.Values._weights.Get(_currentIndex);

        public BulkWindowValueContainer Values => _currentPage!.Values;

        /// <summary>
        /// Positions the reader so the next <see cref="MoveNextRow"/> returns the last row whose key is
        /// strictly smaller than the anchor within the anchor's partition.
        /// </summary>
        public async ValueTask Reset(ColumnRowReference anchor)
        {
            _anchor = anchor;
            _currentPage = null;
            _firstPage = true;
            _done = false;
            await _iterator.Seek(anchor);
            _enumerator = _iterator.GetAsyncEnumerator();
        }

        /// <summary>
        /// Moves to the previous physical row within the partition. Rows with a non positive weight are
        /// skipped. Returns false when the partition start (or tree start) has been reached.
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
                    _currentIndex--;
                    while (_currentIndex >= 0)
                    {
                        if (!IsSamePartition(_currentIndex))
                        {
                            _done = true;
                            return false;
                        }
                        if (_currentPage.Values._weights.Get(_currentIndex) > 0)
                        {
                            return true;
                        }
                        _currentIndex--;
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
                    // Empty pages can sit in the middle of the tree and do not end the partition.
                    // The first page handling is kept so the anchor is still located on the first
                    // page that has rows.
                    _currentPage = null;
                    continue;
                }
                if (_firstPage)
                {
                    _firstPage = false;
                    var bounds = _insertComparer.FindBoundries(in _anchor, page.Keys, 0, page.Keys.Count - 1);
                    var lower = bounds.lowerBounds;
                    if (lower < 0)
                    {
                        lower = ~lower;
                    }
                    // Start one before the anchor position so only rows strictly before the anchor are returned.
                    _currentIndex = Math.Min(lower, page.Keys.Count);
                    _currentPage = page;
                }
                else
                {
                    _currentIndex = page.Keys.Count;
                    _currentPage = page;
                }
            }
        }

        private bool IsSamePartition(int index)
        {
            var row = new ColumnRowReference()
            {
                referenceBatch = _currentPage!.Keys.Data,
                RowIndex = index
            };
            return _partitionComparer.CompareTo(in row, in _anchor) == 0;
        }

        public void Dispose()
        {
            _iterator.Dispose();
        }
    }
}
