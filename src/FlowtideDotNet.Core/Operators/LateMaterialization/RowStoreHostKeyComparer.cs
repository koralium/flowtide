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
using FlowtideDotNet.Core.ColumnStore.BoundarySearching;
using FlowtideDotNet.Core.ColumnStore.Comparers;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Core.Operators.Normalization;
using FlowtideDotNet.Storage.Tree;

namespace FlowtideDotNet.Core.Operators.LateMaterialization
{
    // Orders input rows by key, among themselves and against a leaf
    internal sealed class RowStoreHostKeyComparer : IBplusTreeComparer<ColumnRowReference, NormalizeKeyStorage>
    {
        private readonly List<int> _keyColumns;
        private readonly DataValueContainer _left = new DataValueContainer();
        private readonly DataValueContainer _right = new DataValueContainer();

        private readonly ColumnBoundarySearch _search;

        public RowStoreHostKeyComparer(List<int> keyColumns)
        {
            _keyColumns = keyColumns;
            var stored = new List<int>();
            for (int i = 0; i < keyColumns.Count; i++)
            {
                stored.Add(i);
            }
            _search = new ColumnBoundarySearch(stored, keyColumns);
        }

        // All keys against one leaf, key i is row i
        void IBplusTreeComparer<ColumnRowReference, NormalizeKeyStorage>.FindBoundriesBulk(ReadOnlySpan<ColumnRowReference> keys, ReadOnlySpan<int> sortedLookup, in NormalizeKeyStorage keyContainer, Span<int> lowerBounds, Span<int> upperBounds, Span<int> lookupBuffer)
        {
            if (sortedLookup.Length == 0)
            {
                return;
            }
            var incoming = keys[sortedLookup[0]].referenceBatch.Columns;
            _search.SearchBoundries(keyContainer._data.Columns, incoming, sortedLookup, lowerBounds, upperBounds, 0, keyContainer.Count - 1, false, lookupBuffer);
        }

        public bool SeekNextPageForValue => false;

        // Both rows are input rows
        public int CompareTo(in ColumnRowReference x, in ColumnRowReference y)
        {
            for (int i = 0; i < _keyColumns.Count; i++)
            {
                int column = _keyColumns[i];
                x.referenceBatch.Columns[column].GetValueAt(x.RowIndex, _left, default);
                y.referenceBatch.Columns[column].GetValueAt(y.RowIndex, _right, default);
                int compare = DataValueComparer.Instance.Compare(_left, _right);
                if (compare != 0)
                {
                    return compare;
                }
            }
            return 0;
        }

        public int CompareTo(in ColumnRowReference key, in NormalizeKeyStorage keyContainer, in int index)
        {
            for (int i = 0; i < _keyColumns.Count; i++)
            {
                key.referenceBatch.Columns[_keyColumns[i]].GetValueAt(key.RowIndex, _left, default);
                keyContainer._data.Columns[i].GetValueAt(index, _right, default);
                int compare = DataValueComparer.Instance.Compare(_left, _right);
                if (compare != 0)
                {
                    return compare;
                }
            }
            return 0;
        }

        public int FindIndex(in ColumnRowReference key, in NormalizeKeyStorage keyContainer)
        {
            return FindBoundries(in key, in keyContainer, 0, keyContainer.Count - 1).lowerBounds;
        }

        public FindBoundriesResult FindBoundries(in ColumnRowReference key, in NormalizeKeyStorage keyContainer, int startIndex, int endIndex)
        {
            int start = startIndex;
            int end = endIndex;
            for (int i = 0; i < _keyColumns.Count; i++)
            {
                key.referenceBatch.Columns[_keyColumns[i]].GetValueAt(key.RowIndex, _left, default);
                var (low, high) = keyContainer._data.Columns[i].SearchBoundries(_left, start, end, default);
                if (low < 0)
                {
                    return new FindBoundriesResult(low, low);
                }
                start = low;
                end = high;
            }
            return new FindBoundriesResult(start, end);
        }
    }
}
