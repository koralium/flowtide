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
        private CompareColumnState[] columnStates;
        private SearchBoundriesBulkDelegate[] _savedDelegates;
        private readonly int columnCount;

        // Containers here are for fallback where we skip heap allocation on GetValue
        // This is only used for permutations where there is no specialized code
        private DataValueContainer _xContainer;
        private DataValueContainer _yContainer;
        

        public ColumnBoundarySearch(int columnCount)
        {
            columnStates = new CompareColumnState[columnCount];
            _xContainer = new DataValueContainer();
            _yContainer = new DataValueContainer();
            _savedDelegates = new SearchBoundriesBulkDelegate[columnCount];
            this.columnCount = columnCount;
        }

        public void SearchBoundries(
            IColumn[] columns, 
            IColumn[] inputs,
            ReadOnlySpan<int> inputSortedLookup,
            Span<int> lowerBounds, 
            Span<int> upperBounds, 
            int start, 
            int end)
        {
            lowerBounds.Fill(start);
            upperBounds.Fill(end);

            for (int i = 0; i < columnCount; i++)
            {
                var columnState = columns[i].GetColumnState();
                if (columnStates[i] != columnState)
                {
                    // If the state has changed, we need to get a new delegate for this column
                    _savedDelegates[i] = ColumnBoundarySearchDelegates.GetDelegate(columns[i].GetColumnState(), inputs[i].GetColumnState());
                    columnStates[i] = columnState;
                }
                _savedDelegates[i](columns[i], inputs[i], inputSortedLookup, lowerBounds, upperBounds, _xContainer, _yContainer);
            }
        }
    }
}
