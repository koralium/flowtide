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
    /// Iterates the physical rows of one partition in the persistent window tree from the partition start.
    /// Used by window functions that need to precompute a whole partition value, for example an
    /// unbounded sum or min.
    /// </summary>
    internal class BulkWindowForwardPartitionReader : IDisposable
    {
        private readonly IBPlusTreeIterator<ColumnRowReference, BulkWindowValue, ColumnKeyStorageContainer, BulkWindowValueContainer> _iterator;
        private readonly BulkWindowPartitionComparer _partitionComparer;

        private IAsyncEnumerator<IBPlusTreePageIterator<ColumnRowReference, BulkWindowValue, ColumnKeyStorageContainer, BulkWindowValueContainer>>? _enumerator;
        private IBPlusTreePageIterator<ColumnRowReference, BulkWindowValue, ColumnKeyStorageContainer, BulkWindowValueContainer>? _currentPage;
        private ColumnRowReference _partitionRow;
        private int _currentIndex;
        private int _endIndex;
        private bool _firstPage;
        private bool _done;

        public BulkWindowForwardPartitionReader(
            IBPlusTree<ColumnRowReference, BulkWindowValue, ColumnKeyStorageContainer, BulkWindowValueContainer> tree,
            IReadOnlyList<int> partitionColumns)
        {
            _iterator = tree.CreateIterator();
            _partitionComparer = new BulkWindowPartitionComparer(partitionColumns);
        }

        public EventBatchData Batch => _currentPage!.Keys.Data;

        public int RowIndex => _currentIndex;

        public int Weight => _currentPage!.Values._weights.Get(_currentIndex);

        /// <summary>
        /// Positions the reader at the start of the partition that <paramref name="partitionRow"/> belongs to.
        /// The row reference must stay valid for the duration of the iteration.
        /// </summary>
        public async ValueTask Reset(ColumnRowReference partitionRow)
        {
            _partitionRow = partitionRow;
            _currentPage = null;
            _firstPage = true;
            _done = false;
            await _iterator.Seek(partitionRow, _partitionComparer);
            _enumerator = _iterator.GetAsyncEnumerator();
        }

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
                    _done = true;
                    return false;
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
                _firstPage = false;
                _currentPage = page;
                _currentIndex = _partitionComparer.start - 1;
                _endIndex = _partitionComparer.end;
            }
        }

        public void Dispose()
        {
            _iterator.Dispose();
        }
    }
}
