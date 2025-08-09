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

using FlowtideDotNet.Core.ColumnStore.Comparers;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using System.Collections;

namespace FlowtideDotNet.Core.ColumnStore
{
    public class EventBatchData : IDisposable, IReadOnlyList<ColumnRowReference>
    {
        private readonly IColumn[] columns;
        private bool disposedValue;

        public int Count => columns[0].Count;

        public EventBatchData(IColumn[] columns)
        {
            this.columns = columns;
        }

        public IReadOnlyList<IColumn> Columns => columns;

        public ColumnRowReference this[int index] => GetRowReference(index);

        public IColumn GetColumn(in int index)
        {
            return columns[index];
        }

        /// <summary>
        /// Compares two rows from different batches.
        /// </summary>
        /// <param name="otherBatch"></param>
        /// <param name="thisIndex"></param>
        /// <param name="otherIndex"></param>
        /// <returns></returns>
        public int CompareRows(EventBatchData otherBatch, in int thisIndex, in int otherIndex)
        {
            for (int i = 0; i < columns.Length; i++)
            {
                int compareResult = columns[i].CompareTo(otherBatch.columns[i], thisIndex, otherIndex);
                if (compareResult != 0)
                {
                    return compareResult;
                }
            }
            return 0;
        }

        public (int start, int end) FindBoundries(ColumnRowReference key, in int index, in int end, IColumnComparer<ColumnRowReference> comparer)
        {
            return BoundarySearch.SearchBoundries<ColumnRowReference>(this, key, index, end, comparer);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    for (int i = 0; i < columns.Length; i++)
                    {
                        columns[i].Dispose();
                        columns[i] = null!;
                    }
                }

                disposedValue = true;
            }
        }

        /// <summary>
        /// Call only if none of the columns should be passed further.
        /// This calls dispose on all columns.
        /// </summary>
        public void Dispose()
        {
            // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }

        public int GetByteSize()
        {
            int size = 0;
            for (int i = 0; i < columns.Length; i++)
            {
                size += columns[i].GetByteSize();
            }
            return size;
        }

        public int GetByteSize(int start, int end)
        {
            int size = 0;
            for (int i = 0; i < columns.Length; i++)
            {
                size += columns[i].GetByteSize(start, end);
            }
            return size;
        }

        public ColumnRowReference GetRowReference(in int index)
        {
            return new ColumnRowReference() { referenceBatch = this, RowIndex = index };
        }

        private IEnumerable<ColumnRowReference> GetRows()
        {
            for (int i = 0; i < Count; i++)
            {
                yield return GetRowReference(i);
            }
        }

        public IEnumerator<ColumnRowReference> GetEnumerator()
        {
            return GetRows().GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetRows().GetEnumerator();
        }
    }
}
