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
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Memory;

namespace FlowtideDotNet.Core.Operators.LateMaterialization
{
    // Which input columns are the key, kept inline, or stored
    internal sealed class RowStoreHostLayout
    {
        public RowStoreHostLayout(int columnCount, List<int> keyColumns, List<int> keptColumns, List<int> deferredColumns)
        {
            ColumnCount = columnCount;
            KeyColumns = keyColumns;
            KeptColumns = keptColumns;
            DeferredColumns = deferredColumns;
        }

        // Columns of an input batch
        public int ColumnCount { get; }

        public List<int> KeyColumns { get; }

        // Not in the key, stored beside it
        public List<int> KeptColumns { get; }

        // Stored in the row store behind the reference
        public List<int> DeferredColumns { get; }

        // Output positions behind the reference, when a relation gave them
        public List<int>? DeferredPositions { get; init; }

        // The stored column of each of those positions, in their order
        public List<int>? StoredColumnOfPosition { get; init; }
    }

    // The layout a host was stored with, another one is refused
    internal sealed class RowStoreHostLayoutRecord
    {
        public int ColumnCount { get; set; }

        public int Mode { get; set; }

        public List<int> KeyColumns { get; set; } = new List<int>();

        public List<int> KeptColumns { get; set; } = new List<int>();

        public List<int> DeferredColumns { get; set; } = new List<int>();

        public List<int>? DeferredPositions { get; set; }

        public static RowStoreHostLayoutRecord From(RowStoreHostLayout layout, RowStoreMode mode)
        {
            return new RowStoreHostLayoutRecord()
            {
                ColumnCount = layout.ColumnCount,
                Mode = (int)mode,
                KeyColumns = new List<int>(layout.KeyColumns),
                KeptColumns = new List<int>(layout.KeptColumns),
                DeferredColumns = new List<int>(layout.DeferredColumns),
                DeferredPositions = layout.DeferredPositions == null ? null : new List<int>(layout.DeferredPositions)
            };
        }

        private static bool SamePositions(List<int>? stored, List<int>? asked)
        {
            if (stored == null || asked == null)
            {
                return stored == null && asked == null;
            }
            return stored.SequenceEqual(asked);
        }

        public bool Matches(RowStoreHostLayout layout, RowStoreMode mode)
        {
            return ColumnCount == layout.ColumnCount &&
                Mode == (int)mode &&
                KeyColumns.SequenceEqual(layout.KeyColumns) &&
                KeptColumns.SequenceEqual(layout.KeptColumns) &&
                DeferredColumns.SequenceEqual(layout.DeferredColumns) &&
                SamePositions(DeferredPositions, layout.DeferredPositions);
        }
    }

    // What one input batch changed, the host sends it
    internal sealed class RowStoreHostResult : IDisposable
    {
        private readonly IMemoryAllocator _memoryAllocator;
        private readonly DataValueContainer _value = new DataValueContainer();

        public RowStoreHostResult(int keptColumns, IMemoryAllocator memoryAllocator)
        {
            _memoryAllocator = memoryAllocator;
            InsertRows = new PrimitiveList<int>(memoryAllocator);
            InsertReferences = Column.Create(memoryAllocator);
            DeleteRows = new PrimitiveList<int>(memoryAllocator);
            DeleteReferences = Column.Create(memoryAllocator);
            DeleteKeptColumns = new Column[keptColumns];
            for (int i = 0; i < keptColumns; i++)
            {
                DeleteKeptColumns[i] = Column.Create(memoryAllocator);
            }
        }

        // Input rows to send with weight 1
        public PrimitiveList<int> InsertRows { get; }

        public Column InsertReferences { get; }

        // Input rows whose key is sent with weight -1
        public PrimitiveList<int> DeleteRows { get; }

        // The kept columns the key had before
        public Column[] DeleteKeptColumns { get; }

        public Column DeleteReferences { get; }

        internal void AddInsert(int row, ulong reference)
        {
            InsertRows.Add(row);
            InsertReferences.Add(new RowRefValue(reference));
        }

        internal void AddDelete(int row, ColumnRowReference stored, int referenceIndex)
        {
            DeleteRows.Add(row);
            var columns = stored.referenceBatch.Columns;
            for (int k = 0; k < DeleteKeptColumns.Length; k++)
            {
                columns[k].GetValueAt(stored.RowIndex, _value, default);
                DeleteKeptColumns[k].Add(_value);
            }
            columns[referenceIndex].GetValueAt(stored.RowIndex, _value, default);
            DeleteReferences.Add(_value);
        }

        public void Dispose()
        {
            Dispose(true);
        }

        // The deleted side alone when the inserted side was sent
        internal void Dispose(bool inserts)
        {
            if (inserts)
            {
                InsertRows.Dispose();
                InsertReferences.Dispose();
            }
            DeleteRows.Dispose();
            DeleteReferences.Dispose();
            for (int i = 0; i < DeleteKeptColumns.Length; i++)
            {
                DeleteKeptColumns[i].Dispose();
            }
        }
    }
}
