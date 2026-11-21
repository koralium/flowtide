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
using System.Runtime.CompilerServices;

namespace FlowtideDotNet.Core.Operators.LateMaterialization
{
    // Releasing rows, merging pages and retiring them
    internal sealed partial class RowHeap
    {
        // Pages are sorted by twelfths of live bytes
        private const int BucketsPerPage = 12;
        private const double MinStoredRatio = 1.1;
        private const double MaxStoredRatio = 4;
        // Measured when its live bytes may be off by this share
        private const int UncertainShare = 12;
        // Slot map bytes of one sequence in a merged page
        private const int MapEntryBytes = 24;
        // A merge may end this share above the page target
        private const int MergeOvershootShare = 4;

        // The rows of a merge before they are one page
        private sealed class Merge
        {
            public Merge(Column[] columns)
            {
                Columns = columns;
                Sequences = new List<long>();
                FirstRows = new List<int>();
                Counts = new List<int>();
                Slots = new List<ushort[]?>();
                Sources = new List<long>();
            }

            public Column[] Columns { get; }

            public List<long> Sequences { get; }

            public List<int> FirstRows { get; }

            public List<int> Counts { get; }

            public List<ushort[]?> Slots { get; }

            // The pages taken out of their queues for this merge
            public List<long> Sources { get; }

            public int Rows { get; set; }

            // Row and map bytes so far, each source summed alone
            public long Bytes { get; set; }

            public long MapBytes { get; set; }

            // The columns belong to the merged page from here
            public bool HandedOver { get; set; }
        }

        // Released rows of sequences that still have a row
        private readonly RowHeapReleasedRows _released;
        private readonly Queue<long> _small;
        private readonly Queue<long>[] _sparse;
        private readonly Queue<long> _unmeasured;
        // Sequences of the page read last and their live rows
        private readonly List<long> _readSequences;
        private readonly List<int> _readCounts;
        private List<long> _retiredNow;
        private List<long> _retiredBefore;
        // Bytes of pages in use and of their live rows
        private long _activeBytes;
        private long _liveBytes;
        private long _smallBytes;
        // Bytes the pages that wait to be measured may be off
        private long _unmeasuredSlack;
        // The pages one call may still read
        private int _share;
        // Pages that may be read until the next checkpoint
        private int _budget;
        // Released since the last checkpoint, in pages of their own size
        private double _releasedPagesNow;
        private long _duePagesNow;
        private long _duePagesBefore;
        private long _rowsReleased;
        private long _rowsMerged;
        private long _pagesMerged;
        private long _pagesDeleted;
        private long _pagesMeasured;
        private long _pagesRead;
        private int[] _positions;
        private ushort[] _slots;

        private long QueuedPages
        {
            get
            {
                long queued = _small.Count + _unmeasured.Count;
                for (int i = 0; i < _sparse.Length; i++)
                {
                    queued += _sparse[i].Count;
                }
                return queued;
            }
        }

        // Called by the store's thread when no reader holds the reference
        public void Release(ulong reference)
        {
            ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed), this);
            long sequence = RowReference.Sequence(reference);
            int slot = RowReference.Slot(reference);
            long entry = _directory.Get(sequence);
            if (entry == 0)
            {
                throw new InvalidOperationException($"Row reference to page {sequence} in heap '{_name}' points at no page, the row cannot be released.");
            }
            int live = LiveOf(entry);
            long page = PageOf(entry);
            // Nothing is changed when the row is refused
            var outcome = _released.Release(sequence, slot, live, out int rows);
            if (outcome == RowHeapReleaseOutcome.BeyondRows)
            {
                throw new InvalidOperationException($"Row reference to slot {slot} of page {sequence} in heap '{_name}' is beyond the page's {rows} rows, the row cannot be released.");
            }
            if (outcome == RowHeapReleaseOutcome.AlreadyReleased)
            {
                throw new InvalidOperationException($"The row at slot {slot} of page {sequence} in heap '{_name}' was already released.");
            }
            if (live == 1)
            {
                // No row of the sequence is left, its slot is free
                _directory.Set(sequence, 0);
                Forget(sequence);
            }
            else
            {
                _directory.Set(sequence, Pack(page, live - 1));
                if (sequence < _persistedSequence)
                {
                    _changedWords.Add((sequence << WordBits) | (uint)(slot >> 6));
                }
            }
            if (sequence < _persistedSequence)
            {
                _changedSequences.Add(sequence);
            }
            Volatile.Write(ref _rowCount, _rowCount - 1);
            Volatile.Write(ref _rowsReleased, _rowsReleased + 1);
            ref var stored = ref _pages.Get(page);
            if (stored.Live == 1)
            {
                // The last row of the page
                NoteReleased(stored.LiveBytes, stored.Bytes);
                Retire(page, ref stored);
                return;
            }
            // Taken as an average row, it may be the largest
            int freed = stored.LiveBytes / stored.Live;
            NoteReleased(freed, stored.Bytes);
            int slack = stored.MaxRow - freed;
            if (stored.Merged)
            {
                // Its listed slot goes too, the map entry with the last row
                slack += sizeof(ushort) + (live == 1 ? MapEntryBytes : 0);
            }
            stored.Live--;
            stored.LiveBytes -= freed;
            Volatile.Write(ref _liveBytes, _liveBytes - freed);
            if (stored.Small)
            {
                _smallBytes -= freed;
            }
            if (slack > 0 || stored.Uncertain > stored.LiveBytes)
            {
                AddSlack(page, ref stored, Math.Max(slack, 0));
            }
            Sort(page, ref stored);
        }

        private void NoteReleased(long bytes, int pageBytes)
        {
            if (pageBytes > 0)
            {
                _releasedPagesNow += (double)bytes / pageBytes;
            }
        }

        // A page that may be a twelfth off waits to be measured
        private void AddSlack(long page, ref RowHeapPageTable.Entry entry, int slack)
        {
            int before = entry.Uncertain;
            // No more can be missing than is taken as live
            entry.Uncertain = (int)Math.Min((long)before + slack, entry.LiveBytes);
            if (entry.Unmeasured)
            {
                _unmeasuredSlack += entry.Uncertain - before;
            }
            else if ((long)entry.Uncertain * UncertainShare >= entry.Bytes)
            {
                entry.Unmeasured = true;
                _unmeasured.Enqueue(page);
                _unmeasuredSlack += entry.Uncertain;
                _duePagesNow++;
            }
        }

        // No row of the sequence is left
        private void Forget(long sequence)
        {
            if (sequence < _persistedSequence && _released.TryGet(sequence, out var released))
            {
                for (int word = 0; word < released.WordCount; word++)
                {
                    if (released.Word(word) != 0)
                    {
                        _forgotten.Add((sequence << WordBits) | (uint)word);
                    }
                }
            }
            _released.Forget(sequence);
        }

        // The twelfths of its bytes that a page has live
        private static int BucketOf(ref RowHeapPageTable.Entry entry)
        {
            return entry.Bytes == 0 ? BucketsPerPage : (int)((long)entry.LiveBytes * BucketsPerPage / entry.Bytes);
        }

        // Puts the page in its bucket unless it waits to be measured
        private void Sort(long page, ref RowHeapPageTable.Entry entry)
        {
            int bucket = entry.Unmeasured ? BucketsPerPage : BucketOf(ref entry);
            if (bucket >= _sparse.Length)
            {
                entry.Bucket = RowHeapPageTable.NoBucket;
            }
            else if (bucket != entry.Bucket)
            {
                entry.Bucket = (byte)bucket;
                _sparse[bucket].Enqueue(page);
            }
        }

        // Small pages are merged once, two always fit one page
        private bool IsSmall(ref RowHeapPageTable.Entry entry)
        {
            return !entry.Merged && (long)entry.Bytes * 2 < _options.PageTargetBytes && entry.Rows * 2 <= RowReference.MaxRowsPerPage;
        }

        private void AddPage(long page, int rows, int byteSize, bool merged, int maxRow, int sequences)
        {
            ref var entry = ref _pages.GetOrAdd(page);
            entry.Rows = rows;
            entry.Live = rows;
            entry.Bytes = byteSize;
            entry.LiveBytes = byteSize;
            entry.MaxRow = maxRow;
            entry.Sequences = (byte)SequencesCodeOf(sequences);
            entry.Spread = (byte)SpreadOf(maxRow, byteSize - (merged ? MapEntryBytes * sequences : 0), rows);
            entry.Merged = merged;
            _activeBytes += byteSize;
            Volatile.Write(ref _liveBytes, _liveBytes + byteSize);
            Volatile.Write(ref _storedBytes, _storedBytes + byteSize);
            if (IsSmall(ref entry))
            {
                entry.Small = true;
                _small.Enqueue(page);
                _smallBytes += byteSize;
                _duePagesNow++;
            }
        }

        // Nothing points at the page, it is deleted two checkpoints on
        private void Retire(long page, ref RowHeapPageTable.Entry entry)
        {
            Volatile.Write(ref _liveBytes, _liveBytes - entry.LiveBytes);
            if (entry.Small)
            {
                _smallBytes -= entry.LiveBytes;
                entry.Small = false;
            }
            if (entry.Unmeasured)
            {
                _unmeasuredSlack -= entry.Uncertain;
                entry.Unmeasured = false;
            }
            _activeBytes -= entry.Bytes;
            entry.Live = 0;
            entry.LiveBytes = 0;
            entry.Uncertain = 0;
            entry.Bucket = RowHeapPageTable.NoBucket;
            entry.State = RowHeapPageState.Retired;
            _retiredNow.Add(page);
            if (page < _persistedPage)
            {
                _changedPages.Add(page);
            }
        }

        private void RetireMerged(long page)
        {
            Retire(page, ref _pages.Get(page));
        }

        // A page's bytes are counted in 32 bits
        private long MergeLimit => Math.Min(int.MaxValue, (long)_options.PageTargetBytes + _options.PageTargetBytes / MergeOvershootShare);

        // Stored bytes are above the allowed ratio of live bytes
        private bool GarbageIsHigh()
        {
            return _activeBytes > _liveBytes * _options.MaxStoredRatio && _activeBytes - _liveBytes > 2L * _options.PageTargetBytes;
        }

        // Due when unmeasured pages may put stored bytes above the ratio
        private bool MeasureIsDue()
        {
            long live = _liveBytes - _unmeasuredSlack;
            return _unmeasured.Count > 0 && _activeBytes > live * _options.MaxStoredRatio && _activeBytes - live > 2L * _options.PageTargetBytes;
        }

        // Called by the store's thread, true says more is left
        public ValueTask<bool> CompactAsync()
        {
            ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed), this);
            if (_budget < 1 || (!MeasureIsDue() && _smallBytes < _options.PageTargetBytes && !GarbageIsHigh()))
            {
                return new ValueTask<bool>(false);
            }
            return CompactAsync_Slow();
        }

        private void SetBudget()
        {
            double ratio = _options.MaxStoredRatio;
            // A merge reads this many pages to give one back
            long due = _duePagesNow + (long)Math.Ceiling(_releasedPagesNow * ratio / (ratio - 1));
            _budget = (int)Math.Min(int.MaxValue, _options.CompactPagesPerCall + Math.Max(due, _duePagesBefore));
            _duePagesBefore = due;
            _duePagesNow = 0;
            _releasedPagesNow = 0;
        }

        // What a restore finds above the ratio is due
        private void SetBudgetAfterRestore()
        {
            double excess = _activeBytes - _liveBytes * _options.MaxStoredRatio;
            long pages = _pages.Count - _retiredBefore.Count;
            if (excess > 0 && _activeBytes > 0)
            {
                _releasedPagesNow = excess * pages / _activeBytes;
            }
            SetBudget();
        }

        private async ValueTask<bool> CompactAsync_Slow()
        {
            _share = Math.Min(_options.CompactPagesPerCall, _budget);
            int given = _share;
            bool done = false;
            try
            {
                // Measure first, half the reads are kept for merging
                int kept = _share / 2;
                while (_share > kept && MeasureIsDue())
                {
                    long page = NextToMeasure();
                    if (page < 0)
                    {
                        break;
                    }
                    await MeasureAsync(page).ConfigureAwait(false);
                    done = true;
                }
                // Small pages are merged two or more at a time
                while (_share >= 2 && _smallBytes >= _options.PageTargetBytes && await MergeAsync(true).ConfigureAwait(false))
                {
                    done = true;
                }
                while (_share > 0 && GarbageIsHigh() && await MergeAsync(false).ConfigureAwait(false))
                {
                    done = true;
                }
            }
            finally
            {
                // Also the reads of a call that failed
                _budget -= given - _share;
            }
            // A call that did nothing does not ask for another
            return done && _budget > 0 && (MeasureIsDue() || _smallBytes >= _options.PageTargetBytes || GarbageIsHigh());
        }

        // The next page to measure, it stays queued until measured
        private long NextToMeasure()
        {
            while (_unmeasured.Count > 0)
            {
                long page = _unmeasured.Peek();
                if (WaitsIn(page, MeasureQueue))
                {
                    return page;
                }
                _unmeasured.Dequeue();
            }
            return -1;
        }

        // Reads one page and counts its live bytes
        private async Task MeasureAsync(long page)
        {
            var source = await _client.GetAsync(_client.FirstPageId + page, _window).ConfigureAwait(false);
            try
            {
                Measure(page, source);
            }
            finally
            {
                _window.Release(source);
            }
        }

        private void Measure(long page, RowHeapPage source)
        {
            Charge();
            int count = Collect(page, source, out _, out int ownMapBytes);
            long bytes = SumRowBytes(source.Data.Columns, count, out int maxRow);
            SetMeasured(page, ref _pages.Get(page), bytes + ownMapBytes, maxRow);
        }

        // A page was read, one less is left to the call
        private void Charge()
        {
            _share--;
            Volatile.Write(ref _pagesRead, _pagesRead + 1);
        }

        private void SetMeasured(long page, ref RowHeapPageTable.Entry entry, long bytes, int maxRow)
        {
            int measured = (int)Math.Min(bytes, entry.Bytes);
            int change = measured - entry.LiveBytes;
            if (change < 0)
            {
                // Released before, known only now
                NoteReleased(-change, entry.Bytes);
            }
            if (entry.Unmeasured)
            {
                _unmeasuredSlack -= entry.Uncertain;
                entry.Unmeasured = false;
                if (_unmeasured.Count > 0 && _unmeasured.Peek() == page)
                {
                    _unmeasured.Dequeue();
                }
            }
            entry.Uncertain = 0;
            entry.LiveBytes = measured;
            entry.MaxRow = Math.Min(maxRow, measured);
            Volatile.Write(ref _liveBytes, _liveBytes + change);
            if (entry.Small)
            {
                _smallBytes += change;
            }
            Volatile.Write(ref _pagesMeasured, _pagesMeasured + 1);
            Sort(page, ref entry);
        }

        private void EnsureScratch(int count)
        {
            if (_rows.Length < count)
            {
                _rows = new int[count];
                _columnSizes = new int[count];
                _sizes = new long[count];
            }
            if (_slots.Length < count)
            {
                _slots = new ushort[count];
                _positions = new int[count];
            }
        }

        // Puts the live rows and slots of a page in the scratch
        private int Collect(long page, RowHeapPage source, out int mapBytes, out int ownMapBytes)
        {
            var map = source.Map;
            int hosted = map == null ? 1 : map.Count;
            EnsureScratch(source.Count);
            _readSequences.Clear();
            _readCounts.Clear();
            int count = 0;
            mapBytes = 0;
            ownMapBytes = 0;
            for (int h = 0; h < hosted; h++)
            {
                long sequence = map == null ? source.Sequence : map.Sequence(h);
                long entry = _directory.Get(sequence);
                if (entry == 0 || PageOf(entry) != page)
                {
                    // Every row of the sequence was released
                    continue;
                }
                int kept = LiveRows(source, h, sequence, count);
                if (kept != LiveOf(entry))
                {
                    throw new InvalidOperationException($"Row heap '{_name}' counts {LiveOf(entry)} live rows for page sequence {sequence} and finds {kept}.");
                }
                _readSequences.Add(sequence);
                _readCounts.Add(kept);
                // The slots 0 to kept - 1 need no list
                mapBytes += MapEntryBytes + (_slots[count + kept - 1] == kept - 1 ? 0 : kept * sizeof(ushort));
                if (map != null)
                {
                    // What the live rows take of the map this page has
                    ownMapBytes += MapEntryBytes + (map.Slots(h) == null ? 0 : kept * sizeof(ushort));
                }
                count += kept;
            }
            return count;
        }

        // The live rows and slots of one page sequence
        private int LiveRows(RowHeapPage source, int hosted, long sequence, int from)
        {
            var map = source.Map;
            int count = map == null ? source.Count : map.RowCount(hosted);
            int firstRow = map == null ? 0 : map.FirstRow(hosted);
            bool some = _released.TryGet(sequence, out var released);
            int kept = 0;
            for (int i = 0; i < count; i++)
            {
                int slot = map == null ? i : map.SlotAt(hosted, i);
                if (some && released.IsReleased(slot))
                {
                    continue;
                }
                _rows[from + kept] = firstRow + i;
                _slots[from + kept] = (ushort)slot;
                kept++;
            }
            return kept;
        }

        private const int SmallQueue = -1;
        private const int MeasureQueue = -2;

        private bool WaitsIn(long page, int queue)
        {
            ref var entry = ref _pages.Find(page);
            if (Unsafe.IsNullRef(ref entry) || entry.State != RowHeapPageState.Stored)
            {
                return false;
            }
            if (queue == SmallQueue)
            {
                return entry.Small;
            }
            return queue == MeasureQueue ? entry.Unmeasured : entry.Bucket == queue;
        }

        // The next page to merge, it stays in its queue
        private long NextToMerge(bool small)
        {
            if (small)
            {
                while (_small.Count > 0)
                {
                    if (WaitsIn(_small.Peek(), SmallQueue))
                    {
                        return _small.Peek();
                    }
                    // Merged or retired since it was queued
                    _small.Dequeue();
                }
                return -1;
            }
            for (int bucket = 0; bucket < _sparse.Length; bucket++)
            {
                var queue = _sparse[bucket];
                while (queue.Count > 0)
                {
                    if (WaitsIn(queue.Peek(), bucket))
                    {
                        return queue.Peek();
                    }
                    // Gone, or it waits in another bucket by now
                    queue.Dequeue();
                }
            }
            return -1;
        }

        // The merge did not happen, the page waits again
        private void Requeue(long page)
        {
            ref var entry = ref _pages.Find(page);
            if (Unsafe.IsNullRef(ref entry) || entry.State != RowHeapPageState.Stored)
            {
                return;
            }
            if (IsSmall(ref entry) && !entry.Small)
            {
                entry.Small = true;
                _small.Enqueue(page);
                _smallBytes += entry.LiveBytes;
            }
            Sort(page, ref entry);
        }

        // Drops queue entries of pages that left, once they are most
        private void TrimQueues()
        {
            // A page waits at most once in each kind of queue
            long active = _pages.Count - _retiredNow.Count - _retiredBefore.Count;
            if (QueuedPages > 4 * active + 16)
            {
                var kept = new HashSet<long>();
                Trim(_small, SmallQueue, kept);
                Trim(_unmeasured, MeasureQueue, kept);
                for (int bucket = 0; bucket < _sparse.Length; bucket++)
                {
                    Trim(_sparse[bucket], bucket, kept);
                }
            }
            // A queue keeps the room of its longest time otherwise
            _small.TrimExcess();
            _unmeasured.TrimExcess();
            for (int bucket = 0; bucket < _sparse.Length; bucket++)
            {
                _sparse[bucket].TrimExcess();
            }
        }

        private void Trim(Queue<long> queue, int kind, HashSet<long> kept)
        {
            kept.Clear();
            int count = queue.Count;
            for (int i = 0; i < count; i++)
            {
                long page = queue.Dequeue();
                if (WaitsIn(page, kind) && kept.Add(page))
                {
                    queue.Enqueue(page);
                }
            }
        }

        // Merges pages that wait into one, false when none waited
        private async Task<bool> MergeAsync(bool small)
        {
            if (NextToMerge(small) < 0)
            {
                return false;
            }
            var columns = new Column[_columnCount];
            Merge? merge = null;
            try
            {
                for (int i = 0; i < columns.Length; i++)
                {
                    columns[i] = Column.Create(_memoryAllocator);
                }
                merge = new Merge(columns);
                bool full = false;
                while (!full && _share > 0 && merge.Bytes < _options.PageTargetBytes)
                {
                    long page = NextToMerge(small);
                    if (page < 0)
                    {
                        break;
                    }
                    if (merge.Rows > 0 && (merge.Bytes + _pages.LiveBytes(page) > MergeLimit || merge.Rows + _pages.Live(page) > RowReference.MaxRowsPerPage))
                    {
                        // Does not fit by its estimate, so it is not read
                        break;
                    }
                    var source = await _client.GetAsync(_client.FirstPageId + page, _window).ConfigureAwait(false);
                    try
                    {
                        full = !TakeRows(merge, page, source, small);
                    }
                    finally
                    {
                        // Given back before the merged page waits for the writer
                        _window.Release(source);
                    }
                }
                if (merge.Rows == 0)
                {
                    // The pages hold more live rows than estimated
                    for (int i = 0; i < columns.Length; i++)
                    {
                        columns[i].Dispose();
                    }
                    return true;
                }
                await PublishMergeAsync(merge).ConfigureAwait(false);
                return true;
            }
            catch
            {
                if (merge == null || !merge.HandedOver)
                {
                    for (int i = 0; i < columns.Length; i++)
                    {
                        columns[i]?.Dispose();
                    }
                }
                if (merge != null)
                {
                    // Their rows were not moved, they wait to be merged again
                    for (int i = 0; i < merge.Sources.Count; i++)
                    {
                        Requeue(merge.Sources[i]);
                    }
                }
                throw;
            }
        }

        // Measures a read page and takes its rows if they fit
        private bool TakeRows(Merge merge, long page, RowHeapPage source, bool small)
        {
            Charge();
            int count = Collect(page, source, out int mapBytes, out int ownMapBytes);
            long rowBytes = SumRowBytes(source.Data.Columns, count, out int maxRow);
            ref var entry = ref _pages.Get(page);
            if (entry.Unmeasured || entry.Uncertain > 0)
            {
                SetMeasured(page, ref entry, rowBytes + ownMapBytes, maxRow);
                if (!small && BucketOf(ref entry) >= _sparse.Length)
                {
                    // Too much of it is live after all
                    return true;
                }
            }
            if (merge.Rows == 0 && rowBytes + mapBytes > int.MaxValue)
            {
                // With its slot map it is more than a page counts
                entry.Bucket = RowHeapPageTable.NoBucket;
                return true;
            }
            if (merge.Rows > 0 && (merge.Bytes + rowBytes + mapBytes > MergeLimit || merge.Rows + count > RowReference.MaxRowsPerPage))
            {
                return false;
            }
            for (int i = 0; i < merge.Columns.Length; i++)
            {
                RowHeapReader.Append(merge.Columns[i], source.Data.Columns[i], _rows, _positions, count);
            }
            int at = 0;
            for (int i = 0; i < _readSequences.Count; i++)
            {
                int kept = _readCounts[i];
                merge.Sequences.Add(_readSequences[i]);
                merge.FirstRows.Add(merge.Rows + at);
                merge.Counts.Add(kept);
                // The slots 0 to kept - 1 need no list
                merge.Slots.Add(_slots[at + kept - 1] == kept - 1 ? null : _slots.AsSpan(at, kept).ToArray());
                at += kept;
            }
            merge.Rows += count;
            merge.Bytes += rowBytes + mapBytes;
            merge.MapBytes += mapBytes;
            // Out of its queues, by what is noted on the page
            if (entry.Small)
            {
                entry.Small = false;
                _smallBytes -= entry.LiveBytes;
            }
            entry.Bucket = RowHeapPageTable.NoBucket;
            merge.Sources.Add(page);
            return true;
        }

        private async Task PublishMergeAsync(Merge merge)
        {
            var sequences = merge.Sequences.ToArray();
            var order = new int[sequences.Length];
            for (int i = 0; i < order.Length; i++)
            {
                order[i] = i;
            }
            Array.Sort(sequences, order);
            var firstRows = new int[order.Length];
            var counts = new int[order.Length];
            var slots = new ushort[]?[order.Length];
            for (int i = 0; i < order.Length; i++)
            {
                firstRows[i] = merge.FirstRows[order[i]];
                counts[i] = merge.Counts[order[i]];
                slots[i] = merge.Slots[order[i]];
            }
            // Counted once over the merged page, as a measure counts it
            EnsureScratch(merge.Rows);
            for (int i = 0; i < merge.Rows; i++)
            {
                _rows[i] = i;
            }
            long rowBytes = SumRowBytes(merge.Columns, merge.Rows, out int maxRow);
            var map = new RowHeapSlotMap(sequences, firstRows, counts, slots);
            var data = new EventBatchData(merge.Columns);
            var merged = new RowHeapPage(data, merge.Rows, map);
            merge.HandedOver = true;
            int byteSize;
            long pageId;
            try
            {
                // One page with its slot map can pass the target
                byteSize = checked((int)(rowBytes + merge.MapBytes));
                pageId = _client.NewPageId();
                await _client.PublishAsync(pageId, merged).ConfigureAwait(false);
            }
            catch
            {
                // The client did not take the page
                merged.Return();
                throw;
            }
            long number = pageId - _client.FirstPageId;
            AddPage(number, merge.Rows, byteSize, true, maxRow, sequences.Length);
            for (int i = 0; i < sequences.Length; i++)
            {
                // From here a reader finds the rows in the merged page
                _directory.Set(sequences[i], Pack(number, counts[i]));
                if (sequences[i] < _persistedSequence)
                {
                    _changedSequences.Add(sequences[i]);
                }
            }
            for (int i = 0; i < merge.Sources.Count; i++)
            {
                RetireMerged(merge.Sources[i]);
            }
            Volatile.Write(ref _rowsMerged, _rowsMerged + merge.Rows);
            Volatile.Write(ref _pagesMerged, _pagesMerged + merge.Sources.Count);
            // From here a failure must not requeue the pages
            merge.Sources.Clear();
            await AfterPublishAsync(byteSize, MergeLimit).ConfigureAwait(false);
        }
    }
}
