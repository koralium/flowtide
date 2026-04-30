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

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.ColumnStore.Sort
{
    /// <summary>
    /// Helper class for sorting, not thread safe
    /// One sorter should be created per thread.
    /// This keeps track of pointer assignment and keeps
    /// the last used sort method in cache to not call concurrent dictionary.
    /// </summary>
    internal class BatchSorter
    {
        private SelfComparePointers[] _pointers;
        private UInt128 _lastKey = 0;
        private SortCompiler.SortDelegate? _lastSort;
        public BatchSorter(int columnCount)
        {
            _pointers = new SelfComparePointers[columnCount];
        }

        public void SortData(IColumn[] columns, ref Span<int> indirect)
        {
            Debug.Assert(columns.Length == _pointers.Length);
            var key = SortCompiler.CreateKey(columns);

            if (key != _lastKey || _lastSort == null)
            {
                _lastSort = SortCompiler.GetOrCompile(key, columns);
                _lastKey = key;
            }

            for (int i = 0; i < columns.Length; i++)
            {
                if (columns[i].SupportSelfCompareExpression)
                {
                    columns[i].SetSelfComparePointers(ref _pointers[i]);
                }
            }

            _lastSort(new SortCompareContext(columns, _pointers), ref indirect);
        }
    }
}
