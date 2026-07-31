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

namespace FlowtideDotNet.Core.Operators
{
    /// <summary>
    /// Buffers batches when inserts scatter over leaves, merged flushes amortize the work
    /// </summary>
    internal sealed class DeferredInsertBuffer
    {
        // Density (rows per touched leaf) below this starts buffering
        private const double EnterDensityThreshold = 4.0;

        // Flush cap targets this density, must exceed the enter threshold
        private const double TargetDensity = 8.0;

        private const int InitialRowCap = 1_000;

        // Sanity bound for the cap doubling, memory is bounded by bytes
        private const int MaxRowCap = 1_000_000;

        // Hard memory ceiling, flush happens regardless of row cap
        private const long MaxPendingBytes = 16 * 1024 * 1024;

        // Smaller batches give too noisy a density signal
        private const int MinRowsForSignal = 64;

        private readonly List<EventBatchWeighted> _pending = new List<EventBatchWeighted>();
        private int _pendingRows;
        private long _pendingBytes;
        private int _flushRowCap = InitialRowCap;

        public bool Buffering { get; private set; }

        public bool HasPending => _pending.Count > 0;

        /// <summary>
        /// Buffers a batch, returns true when the cap is reached.
        /// </summary>
        public bool Add(EventBatchWeighted data)
        {
            // Keep the batch alive until flushed
            data.Rent(1);
            _pending.Add(data);
            _pendingRows += data.Count;
            // Data size plus weights and iterations
            _pendingBytes += data.EventBatchData.GetByteSize() + (long)data.Count * 8;
            return _pendingRows >= _flushRowCap || _pendingBytes >= MaxPendingBytes;
        }

        /// <summary>
        /// Takes all pending rows as one batch, caller owns one rent.
        /// </summary>
        public EventBatchWeighted TakePending(IMemoryAllocator memoryAllocator)
        {
            EventBatchWeighted result;
            if (_pending.Count == 1)
            {
                // Single batch passes through with its rent from Add
                result = _pending[0];
            }
            else
            {
                int columnCount = _pending[0].EventBatchData.Columns.Count;
                var weights = new PrimitiveList<int>(memoryAllocator);
                var iterations = new PrimitiveList<uint>(memoryAllocator);
                IColumn[] columns = new IColumn[columnCount];
                for (int i = 0; i < columnCount; i++)
                {
                    columns[i] = Column.Create(memoryAllocator);
                }
                foreach (var batch in _pending)
                {
                    int count = batch.Count;
                    weights.AddRangeFrom(batch.Weights, 0, count);
                    iterations.AddRangeFrom(batch.Iterations, 0, count);
                    for (int c = 0; c < columnCount; c++)
                    {
                        columns[c].InsertRangeFrom(columns[c].Count, batch.EventBatchData.Columns[c], 0, count);
                    }
                    batch.Return();
                }
                result = new EventBatchWeighted(weights, iterations, new EventBatchData(columns));
                result.Rent(1);
            }
            _pending.Clear();
            _pendingRows = 0;
            _pendingBytes = 0;
            return result;
        }

        /// <summary>
        /// Updates the latch and flush cap from an insert's leaf hits.
        /// </summary>
        public void OnBatchApplied(int rowCount, int leafHits)
        {
            if (rowCount < MinRowsForSignal || leafHits <= 0)
            {
                return;
            }

            if (rowCount < leafHits * EnterDensityThreshold)
            {
                Buffering = true;
            }

            double estimatedLeaves;
            if (rowCount >= leafHits * 2)
            {
                // Dense, leaves are saturated so hits equal leaves
                estimatedLeaves = leafHits;
            }
            else
            {
                // Sparse, birthday estimate, zero collisions keeps growing
                int collisions = rowCount - leafHits;
                estimatedLeaves = (double)rowCount * rowCount / (2.0 * Math.Max(collisions, 1));
            }
            long desired = (long)(estimatedLeaves * TargetDensity);
            // At most one doubling per measurement
            long next = Math.Clamp(desired, _flushRowCap / 2L, _flushRowCap * 2L);
            _flushRowCap = (int)Math.Clamp(next, InitialRowCap, MaxRowCap);
        }

        /// <summary>
        /// Watermark reset to direct inserts, the learned cap is kept.
        /// </summary>
        public void ExitBuffering()
        {
            Buffering = false;
        }

        /// <summary>
        /// Releases pending batches without processing, used on dispose.
        /// </summary>
        public void ReturnPending()
        {
            foreach (var batch in _pending)
            {
                batch.Return();
            }
            _pending.Clear();
            _pendingRows = 0;
            _pendingBytes = 0;
        }
    }
}
