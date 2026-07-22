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
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Memory;
using System.Diagnostics;

namespace FlowtideDotNet.Core.Operators.Window.Bulk
{
    /// <summary>
    /// Collects output rows, input columns are block copied from the source batch.
    /// Call <see cref="FlushPending"/> before the source batch is released.
    /// </summary>
    internal class BulkWindowEmitter
    {
        private IColumn[] _columns;
        private PrimitiveList<int> _weights;
        private PrimitiveList<uint> _iterations;
        private readonly List<int> _emitList;
        private readonly IMemoryAllocator _memoryAllocator;

        // Emit slots mapped to input columns and to function values.
        private readonly (int OutputSlot, int SourceColumn)[] _inputMappings;
        private readonly (int OutputSlot, int FunctionIndex)[] _functionMappings;

        // Row offsets into the current source batch that have not been block copied yet.
        private EventBatchData? _pendingSourceBatch;
        private PrimitiveList<int> _pendingOffsets;
        private PrimitiveList<int> _targetPositionScratch;
        private PrimitiveList<int> _translatedOffsetScratch;

        public int Count => _weights.Count;

        public BulkWindowEmitter(int inputColumnCount, List<int> emitList, IMemoryAllocator memoryAllocator)
        {
            _emitList = emitList;
            _memoryAllocator = memoryAllocator;

            List<(int, int)> inputMappings = new List<(int, int)>();
            List<(int, int)> functionMappings = new List<(int, int)>();
            for (int i = 0; i < emitList.Count; i++)
            {
                var emitIndex = emitList[i];
                if (emitIndex >= inputColumnCount)
                {
                    functionMappings.Add((i, emitIndex - inputColumnCount));
                }
                else
                {
                    inputMappings.Add((i, emitIndex));
                }
            }
            _inputMappings = inputMappings.ToArray();
            _functionMappings = functionMappings.ToArray();

            _columns = CreateColumns();
            _weights = new PrimitiveList<int>(memoryAllocator);
            _iterations = new PrimitiveList<uint>(memoryAllocator);
            _pendingOffsets = new PrimitiveList<int>(memoryAllocator);
            _targetPositionScratch = new PrimitiveList<int>(memoryAllocator);
            _translatedOffsetScratch = new PrimitiveList<int>(memoryAllocator);
        }

        private IColumn[] CreateColumns()
        {
            var columns = new IColumn[_emitList.Count];
            for (int i = 0; i < _emitList.Count; i++)
            {
                columns[i] = ColumnFactory.Get(_memoryAllocator);
            }
            return columns;
        }

        public EventBatchWeighted GetCurrentBatch()
        {
            FlushPending();
            var batch = new EventBatchWeighted(_weights, _iterations, new EventBatchData(_columns));

            _columns = CreateColumns();
            _weights = new PrimitiveList<int>(_memoryAllocator);
            _iterations = new PrimitiveList<uint>(_memoryAllocator);

            return batch;
        }

        /// <summary>
        /// Adds one output row, the source batch must stay valid until the next flush.
        /// </summary>
        public void AddOutputRow(EventBatchData rowBatch, int rowIndex, IDataValue[] functionValues, int weight)
        {
            if (_pendingOffsets.Count > 0 &&
                (!ReferenceEquals(_pendingSourceBatch, rowBatch) || rowIndex < _pendingOffsets[_pendingOffsets.Count - 1]))
            {
                // Source changed, or offsets would stop being non decreasing.
                FlushPending();
            }
            _pendingSourceBatch = rowBatch;
            _pendingOffsets.Add(rowIndex);

            for (int i = 0; i < _functionMappings.Length; i++)
            {
                _columns[_functionMappings[i].OutputSlot].Add(functionValues[_functionMappings[i].FunctionIndex]);
            }
            _weights.Add(weight);
            _iterations.Add(0);
        }

        /// <summary>
        /// Copies the collected input column values in blocks from the pending source batch.
        /// </summary>
        public void FlushPending()
        {
            var count = _pendingOffsets.Count;
            if (count == 0)
            {
                return;
            }
            Debug.Assert(_pendingSourceBatch != null);

            if (_inputMappings.Length > 0)
            {
                var initialCount = _columns[_inputMappings[0].OutputSlot].Count;
                _targetPositionScratch.Clear();
                _targetPositionScratch.EnsureCapacity(count);
                for (int i = 0; i < count; i++)
                {
                    _targetPositionScratch.Add(initialCount);
                }

                ReadOnlySpan<int> offsetSpan = _pendingOffsets.Span;
                ReadOnlySpan<int> targetSpan = _targetPositionScratch.Span;

                for (int m = 0; m < _inputMappings.Length; m++)
                {
                    var sourceColumn = _pendingSourceBatch.Columns[_inputMappings[m].SourceColumn];
                    if (sourceColumn is ColumnWithOffset columnWithOffset)
                    {
                        // Translate offsets so the copy reads the inner column.
                        var innerOffsets = columnWithOffset.Offsets;
                        _translatedOffsetScratch.Clear();
                        _translatedOffsetScratch.EnsureCapacity(count);
                        for (int i = 0; i < count; i++)
                        {
                            _translatedOffsetScratch.Add(innerOffsets[offsetSpan[i]]);
                        }
                        ReadOnlySpan<int> translatedSpan = _translatedOffsetScratch.Span;
                        _columns[_inputMappings[m].OutputSlot].InsertFrom(columnWithOffset.InnerColumn, in translatedSpan, in targetSpan, ColumnWithOffset.NullValueIndex);
                    }
                    else
                    {
                        _columns[_inputMappings[m].OutputSlot].InsertFrom(sourceColumn, in offsetSpan, in targetSpan, ColumnWithOffset.NullValueIndex);
                    }
                }
            }

            _pendingOffsets.Clear();
            _pendingSourceBatch = null;
        }
    }
}
