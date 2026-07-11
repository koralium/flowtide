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
using System.Diagnostics;

namespace FlowtideDotNet.Core.Operators.Window.Bulk
{
    /// <summary>
    /// Lazily materializes the logical rows that precede a scan start position so window functions can seed
    /// their incremental state. Row values, weights and stored function state are copied into scratch
    /// columns since the source pages are released as the backward walk advances.
    /// Logical rows are exposed newest first: back = 1 is the row immediately before the scan start.
    /// </summary>
    internal class BulkWindowSeedReader : IDisposable
    {
        private readonly BulkWindowBackwardPartitionReader _reader;
        private readonly int _keyColumnCount;
        private readonly int _stateColumnCount;
        private Column[] _rowColumns;
        private Column[] _stateColumns;
        private readonly IMemoryAllocator _memoryAllocator;
        private EventBatchData _rowBatch;
        private int _materializedRows;
        private bool _exhausted;
        private bool _active;

        public BulkWindowSeedReader(
            BulkWindowBackwardPartitionReader reader,
            int keyColumnCount,
            int stateColumnCount,
            IMemoryAllocator memoryAllocator)
        {
            _reader = reader;
            _keyColumnCount = keyColumnCount;
            _stateColumnCount = stateColumnCount;
            _memoryAllocator = memoryAllocator;
            _rowColumns = new Column[keyColumnCount];
            _stateColumns = new Column[stateColumnCount];
            for (int i = 0; i < keyColumnCount; i++)
            {
                _rowColumns[i] = Column.Create(memoryAllocator);
            }
            for (int i = 0; i < stateColumnCount; i++)
            {
                _stateColumns[i] = Column.Create(memoryAllocator);
            }
            _rowBatch = new EventBatchData(_rowColumns);
        }

        /// <summary>
        /// The number of logical rows materialized so far.
        /// </summary>
        public int MaterializedRows => _materializedRows;

        /// <summary>
        /// Starts a new seed at the given anchor. Rows strictly before the anchor within its partition
        /// become available.
        /// </summary>
        public async ValueTask Reset(ColumnRowReference anchor)
        {
            Clear();
            _active = true;
            _exhausted = false;
            await _reader.Reset(anchor);
        }

        /// <summary>
        /// Marks the seed as empty, used when a scan starts at the partition start.
        /// </summary>
        public void ResetEmpty()
        {
            Clear();
            _active = true;
            _exhausted = true;
        }

        private void Clear()
        {
            for (int i = 0; i < _keyColumnCount; i++)
            {
                _rowColumns[i].Clear();
            }
            for (int i = 0; i < _stateColumnCount; i++)
            {
                _stateColumns[i].Clear();
            }
            _materializedRows = 0;
        }

        /// <summary>
        /// Ensures at least <paramref name="logicalRows"/> rows are materialized. Returns true when that
        /// many rows exist before the scan start, false when the partition start was reached first.
        /// </summary>
        public async ValueTask<bool> EnsureRows(int logicalRows)
        {
            Debug.Assert(_active, "Reset must be called before EnsureRows");
            while (_materializedRows < logicalRows)
            {
                if (_exhausted)
                {
                    return false;
                }
                if (!await _reader.MoveNextRow())
                {
                    _exhausted = true;
                    return _materializedRows >= logicalRows;
                }
                MaterializeCurrentRow();
            }
            return true;
        }

        private void MaterializeCurrentRow()
        {
            var batch = _reader.Batch;
            var rowIndex = _reader.RowIndex;
            var weight = _reader.Weight;
            var values = _reader.Values;

            // Duplicates are materialized newest first, the last duplicate is the closest logical row.
            for (int dup = weight - 1; dup >= 0; dup--)
            {
                for (int c = 0; c < _keyColumnCount; c++)
                {
                    _rowColumns[c].Add(batch.Columns[c].GetValueAt(rowIndex, default));
                }
                for (int s = 0; s < _stateColumnCount; s++)
                {
                    var listLength = values._functionStates[s].GetListLength(rowIndex);
                    if (dup < listLength)
                    {
                        _stateColumns[s].Add(values._functionStates[s].GetListElementValue(rowIndex, dup));
                    }
                    else
                    {
                        _stateColumns[s].Add(NullValue.Instance);
                    }
                }
                _materializedRows++;
            }
        }

        /// <summary>
        /// Returns a reference to the logical row <paramref name="back"/> rows before the scan start.
        /// back = 1 is the closest row. Only valid for back &lt;= <see cref="MaterializedRows"/>.
        /// </summary>
        public ColumnRowReference GetRow(int back)
        {
            Debug.Assert(back >= 1 && back <= _materializedRows);
            return new ColumnRowReference()
            {
                referenceBatch = _rowBatch,
                RowIndex = back - 1
            };
        }

        /// <summary>
        /// Returns the stored state value (output or auxiliary) of a previous logical row.
        /// </summary>
        public IDataValue GetState(int back, int stateColumnIndex)
        {
            Debug.Assert(back >= 1 && back <= _materializedRows);
            return _stateColumns[stateColumnIndex].GetValueAt(back - 1, default);
        }

        public void Dispose()
        {
            for (int i = 0; i < _keyColumnCount; i++)
            {
                _rowColumns[i].Dispose();
            }
            for (int i = 0; i < _stateColumnCount; i++)
            {
                _stateColumns[i].Dispose();
            }
        }
    }
}
