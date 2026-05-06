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

using FlowtideDotNet.Core.ColumnStore.Sort;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.ColumnStore.BoundarySearching
{
    internal class ColumnBoundarySearch
    {
        private int[] columnStates;
        private SearchBoundriesBulkDelegate[] _savedDelegates;
        private readonly int _columnCount;
        private readonly IReadOnlyList<int> _treeColumnOrder;
        private readonly IReadOnlyList<int> _incomingColumnOrder;

        // Containers here are for fallback where we skip heap allocation on GetValue
        // This is only used for permutations where there is no specialized code
        private DataValueContainer _xContainer;
        private DataValueContainer _yContainer;
        

        public ColumnBoundarySearch(IReadOnlyList<int> treeColumnOrder, IReadOnlyList<int> incomingColumnOrder)
        {
            this._columnCount = treeColumnOrder.Count;
            _treeColumnOrder = treeColumnOrder;
            _incomingColumnOrder = incomingColumnOrder;
            
            columnStates = new int[_columnCount];

            for (int i = 0; i < columnStates.Length; i++)
            {
                columnStates[i] = -1;
            }

            _xContainer = new DataValueContainer();
            _yContainer = new DataValueContainer();
            _savedDelegates = new SearchBoundriesBulkDelegate[_columnCount];
        }

        public void SearchBoundries(
            IReadOnlyList<IColumn> columns,
            IReadOnlyList<IColumn> inputs,
            ReadOnlySpan<int> inputSortedLookup,
            Span<int> lowerBounds, 
            Span<int> upperBounds, 
            int start, 
            int end)
        {
            lowerBounds.Fill(start);
            upperBounds.Fill(end);

            for (int i = 0; i < _columnCount; i++)
            {
                var treeColumn = columns[_treeColumnOrder[i]];
                var inputColumn = inputs[_incomingColumnOrder[i]];
                var treeColumnState = treeColumn.GetColumnState();
                var inputColumnState = inputColumn.GetColumnState();
                var searchKey = ColumnBoundarySearchDelegates.GetKey(treeColumnState, inputColumnState);
                if (columnStates[i] != searchKey)
                {
                    // If the state has changed, we need to get a new delegate for this column
                    _savedDelegates[i] = ColumnBoundarySearchDelegates.GetDelegate(searchKey);
                    columnStates[i] = searchKey;
                }
                _savedDelegates[i](treeColumn, inputColumn, inputSortedLookup, lowerBounds, upperBounds, _xContainer, _yContainer);
            }
        }
    }
}
