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
    /// Directions are baked into the compiled delegates, so they add no per row cost.
    /// </summary>
    internal class BatchSorter
    {
        private readonly SelfComparePointers[] _pointers;
        private readonly SortColumnDirection[]? _directions;
        private RadixItem[] _radixItems = Array.Empty<RadixItem>();
        private UInt128 _lastKey = 0;
        private SortCompiler.SortDelegate? _lastSort;
        private SortCompiler.SortWithTagsDelegate? _lastSortWithTags;

        public BatchSorter(int columnCount)
            : this(columnCount, null)
        {
        }

        public BatchSorter(int columnCount, SortColumnDirection[]? directions)
        {
            Debug.Assert(directions == null || directions.Length == columnCount);
            _pointers = new SelfComparePointers[columnCount];
            _directions = directions;
        }

        public void SortData(IColumn[] columns, ref Span<int> indirect)
        {
            Debug.Assert(columns.Length == _pointers.Length);
            var key = SortCompiler.CreateKey(columns, _directions);

            if (key != _lastKey || _lastSort == null)
            {
                _lastSort = SortCompiler.GetOrCompile(key, columns, _directions);
                _lastKey = key;
            }

            for (int i = 0; i < columns.Length; i++)
            {
                if (columns[i].SupportSelfCompareExpression)
                {
                    columns[i].SetSelfComparePointers(ref _pointers[i]);
                }
            }

            if (_radixItems.Length < indirect.Length)
            {
                _radixItems = new RadixItem[indirect.Length];
            }

            var radixItemsSpan = _radixItems.AsSpan(0, indirect.Length);

            _lastSort(new SortCompareContext(columns, _pointers), ref indirect, ref radixItemsSpan);
        }

        /// <summary>
        /// Does a sort and an extra pass to find duplicate rows.
        /// These are created as tags, 0 0 1 1 1 1 etc, where if they share the same value in the sort columns, they get the same tag.
        /// This is helpful to create at this step since columns are already in cache.
        /// </summary>
        /// <param name="columns"></param>
        /// <param name="indirect"></param>
        /// <param name="tags"></param>
        public void SortDataWithTags(IColumn[] columns, ref Span<int> indirect, ref Span<int> tags)
        {
            Debug.Assert(columns.Length == _pointers.Length);
            var key = SortCompiler.CreateKey(columns, _directions);
            if (key != _lastKey || _lastSortWithTags == null)
            {
                _lastSortWithTags = SortCompiler.GetOrCompileWithTags(key, columns, _directions);
                _lastKey = key;
            }
            for (int i = 0; i < columns.Length; i++)
            {
                if (columns[i].SupportSelfCompareExpression)
                {
                    columns[i].SetSelfComparePointers(ref _pointers[i]);
                }
            }

            if (_radixItems.Length < indirect.Length)
            {
                _radixItems = new RadixItem[indirect.Length];
            }

            var radixItemsSpan = _radixItems.AsSpan(0, indirect.Length);
            _lastSortWithTags(new SortCompareContext(columns, _pointers), ref indirect, ref tags, ref radixItemsSpan);
        }
    }
}
