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

using System.Numerics;
using System.Runtime.CompilerServices;

namespace FlowtideDotNet.Core.Operators.LateMaterialization
{
    // The heap's state, restored and written at a checkpoint
    internal sealed partial class RowHeap
    {
        // Four kinds of entries, the kind is the top of the key
        private const int KindShift = 60;
        private const long IdMask = (1L << KindShift) - 1;
        private const long DirectoryKind = 0;
        private const long PageKind = 1;
        private const long RetiredKind = 2;
        private const long ReleasedKind = 3;
        // The id of a released word is its sequence and word number
        private const int WordBits = 10;
        private const long WordMask = (1L << WordBits) - 1;

        // A page record: rows less one, bytes, merged, spread, sequences
        private const int RecordRowBits = 16;
        private const long RecordRowMask = (1L << RecordRowBits) - 1;
        private const long RecordMergedBit = 1L << 47;
        private const int RecordSpreadShift = 48;
        private const long RecordSpreadMask = 255;
        private const int RecordSequencesShift = 56;
        // Steps of the stored sequence count in a doubling
        private const int SequenceSteps = 15;

        // Stored sequences and pages changed since the last checkpoint
        private readonly HashSet<long> _changedSequences;
        private readonly HashSet<long> _changedWords;
        private readonly HashSet<long> _changedPages;
        // Words of released rows whose sequence has no row left
        private readonly List<long> _forgotten;
        // A larger list or set is trimmed after a checkpoint
        private const int SpareLimit = 1024;
        private long _persistedSequence;
        private long _persistedPage;
        // Set at the barrier, the checkpoint stores that state
        private bool _marked;
        private long _markedSequence;
        private long _markedPage;
        private long _rowsAfterMark;
        private long _bytesAfterMark;

        private async Task ReadIndexAsync(long nextPage)
        {
            var pages = new List<long>();
            var released = new Dictionary<long, ulong[]>();
            _pages.Loading = true;
            using var iterator = _index.CreateIterator();
            await iterator.SeekFirst();
            await foreach (var leaf in iterator)
            {
                leaf.EnterWriteLock();
                try
                {
                    foreach (var entry in leaf)
                    {
                        ReadEntry(entry.Key, entry.Value, nextPage, pages, released);
                    }
                }
                finally
                {
                    leaf.ExitWriteLock();
                }
            }
            if (pages.Count != _pages.Count)
            {
                throw new InvalidOperationException($"Row heap '{_name}' is stored with rows in {_pages.Count - pages.Count} pages it has no record of.");
            }
            RestoreReleased(released);
            long rows = 0;
            for (int i = 0; i < pages.Count; i++)
            {
                rows += ReadPage(pages[i]);
            }
            if (rows != _rowCount)
            {
                throw new InvalidOperationException($"Row heap '{_name}' is stored with {_rowCount} rows, its pages hold {rows}.");
            }
            _pages.Loading = false;
            SetBudgetAfterRestore();
        }

        // An entry that does not fit the heap's counters is refused
        private void ReadEntry(long key, long value, long nextPage, List<long> pages, Dictionary<long, ulong[]> released)
        {
            long id = key & IdMask;
            bool fits;
            switch (key >> KindShift)
            {
                case DirectoryKind:
                    long hostPage = PageOf(value);
                    int live = LiveOf(value);
                    fits = id >= 1 && id < _nextSequence && hostPage >= 0 && hostPage < nextPage && live >= 1 && live <= RowReference.MaxRowsPerPage;
                    if (fits)
                    {
                        _directory.Set(id, value);
                        ref var host = ref _pages.GetOrAdd(hostPage);
                        host.Live += live;
                        // Counts the page's sequences until the page is read
                        host.Uncertain++;
                    }
                    break;
                case PageKind:
                    fits = id < nextPage;
                    if (fits)
                    {
                        ref var page = ref _pages.GetOrAdd(id);
                        page.Rows = (int)(value & RecordRowMask) + 1;
                        page.Bytes = (int)((value >> RecordRowBits) & int.MaxValue);
                        page.Merged = (value & RecordMergedBit) != 0;
                        page.Spread = (byte)((value >> RecordSpreadShift) & RecordSpreadMask);
                        page.Sequences = (byte)((value >> RecordSequencesShift) & byte.MaxValue);
                        // Over the rows alone, with the fewest map entries
                        int rowBytes = page.Bytes - (page.Merged ? (int)Math.Min(page.Bytes, (long)MapEntryBytes * FewestSequences(page.Sequences)) : 0);
                        page.MaxRow = MaxRowOf(page.Spread, rowBytes, page.Rows);
                        pages.Add(id);
                    }
                    break;
                case RetiredKind:
                    fits = id < nextPage;
                    if (fits)
                    {
                        _pages.GetOrAdd(id).State = RowHeapPageState.Retired;
                        _retiredBefore.Add(id);
                    }
                    break;
                case ReleasedKind:
                    fits = (id >> WordBits) >= 1 && (id >> WordBits) < _nextSequence && value != 0;
                    if (fits)
                    {
                        ReadReleasedWord(released, id, value);
                    }
                    break;
                default:
                    fits = false;
                    break;
            }
            if (!fits)
            {
                throw new InvalidOperationException($"Row heap '{_name}' is stored with an entry that does not fit it: key {key}, value {value}.");
            }
        }

        // Largest row over average, in eighths of a doubling, rounded up
        private static long SpreadOf(int maxRow, int bytes, int rows)
        {
            if (maxRow <= 0 || bytes <= 0)
            {
                return 0;
            }
            double times = (double)maxRow * rows / bytes;
            return times <= 1 ? 0 : Math.Min(RecordSpreadMask, (long)Math.Ceiling(8 * Math.Log2(times)));
        }

        private static int MaxRowOf(long spread, int bytes, int rows)
        {
            return (int)Math.Min(bytes, Math.Ceiling((double)bytes / rows * Math.Pow(2, spread / 8.0)));
        }

        // The smallest code that stands for at least this many
        internal static int SequencesCodeOf(int sequences)
        {
            int code = sequences <= 1 ? 0 : (int)Math.Ceiling(SequenceSteps * Math.Log2(sequences));
            while (code > 0 && MostSequences(code - 1) >= sequences)
            {
                code--;
            }
            while (MostSequences(code) < sequences)
            {
                code++;
            }
            return code;
        }

        internal static int MostSequences(int code)
        {
            return (int)Math.Floor(Math.Pow(2, (double)code / SequenceSteps) + 1e-9);
        }

        internal static int FewestSequences(int code)
        {
            return code == 0 ? 1 : MostSequences(code - 1) + 1;
        }

        // The words of a sequence come in order
        private static void ReadReleasedWord(Dictionary<long, ulong[]> released, long id, long value)
        {
            long sequence = id >> WordBits;
            int word = (int)(id & WordMask);
            if (!released.TryGetValue(sequence, out var bits))
            {
                bits = new ulong[word + 1];
                released.Add(sequence, bits);
            }
            else if (bits.Length <= word)
            {
                Array.Resize(ref bits, word + 1);
                released[sequence] = bits;
            }
            bits[word] = (ulong)value;
        }

        // The rows of a sequence are its live and released rows
        private void RestoreReleased(Dictionary<long, ulong[]> released)
        {
            foreach (var (sequence, read) in released)
            {
                long entry = _directory.Get(sequence);
                int count = 0;
                for (int word = 0; word < read.Length; word++)
                {
                    count += BitOperations.PopCount(read[word]);
                }
                int rows = LiveOf(entry) + count;
                int words = (rows + 63) >> 6;
                // A released row beyond the sequence's rows is refused
                int highest = read.Length * 64 - 1 - BitOperations.LeadingZeroCount(read[read.Length - 1]);
                if (entry == 0 || count == 0 || rows > RowReference.MaxRowsPerPage || read.Length > words || highest >= rows)
                {
                    throw new InvalidOperationException($"Row heap '{_name}' is stored with released rows of page sequence {sequence} that do not fit its {LiveOf(entry)} live rows.");
                }
                _released.Restore(sequence, rows, read);
            }
        }

        // Counts a stored page and queues it, returns its live rows
        private int ReadPage(long page)
        {
            ref var entry = ref _pages.Get(page);
            if (entry.State == RowHeapPageState.Retired)
            {
                if (entry.Live != 0)
                {
                    throw new InvalidOperationException($"Row heap '{_name}' is stored with {entry.Live} live rows in page {page}, which waits to be deleted.");
                }
                return 0;
            }
            if (entry.Live == 0 || entry.Live > entry.Rows)
            {
                throw new InvalidOperationException($"Row heap '{_name}' is stored with {entry.Live} live rows in page {page} of {entry.Rows} rows, and the page is not retired.");
            }
            int liveSequences = entry.Uncertain;
            entry.Uncertain = 0;
            // By the count of rows, as a release takes it
            entry.LiveBytes = (int)((long)entry.Bytes * entry.Live / entry.Rows);
            _activeBytes += entry.Bytes;
            Volatile.Write(ref _liveBytes, _liveBytes + entry.LiveBytes);
            if (IsSmall(ref entry))
            {
                entry.Small = true;
                _small.Enqueue(page);
                _smallBytes += entry.LiveBytes;
                _duePagesNow++;
            }
            long slack;
            if (entry.Merged)
            {
                // What the released rows and dead sequences may have held
                int made = Math.Min(MostSequences(entry.Sequences), entry.Rows);
                long dead = (long)(entry.Rows - entry.Live) * ((long)entry.MaxRow + sizeof(ushort)) + (long)MapEntryBytes * Math.Max(0, made - liveSequences);
                // Less what the row count took off for them
                slack = dead - (long)entry.Bytes * (entry.Rows - entry.Live) / entry.Rows;
            }
            else
            {
                // Each released row may have been the largest
                slack = (long)(entry.Rows - entry.Live) * Math.Max(0, entry.MaxRow - entry.Bytes / entry.Rows);
            }
            if (slack > 0)
            {
                AddSlack(page, ref entry, (int)Math.Min(slack, int.MaxValue));
            }
            Sort(page, ref entry);
            return entry.Live;
        }

        // Called at the barrier when the checkpoint is committed later
        public void BeginCheckpoint()
        {
            ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed), this);
            if (_marked)
            {
                throw new InvalidOperationException($"Row heap '{_name}' has a checkpoint that is not committed yet.");
            }
            _marked = true;
            _markedSequence = _nextSequence;
            _markedPage = _client.NextPageId - _client.FirstPageId;
            _rowsAfterMark = 0;
            _bytesAfterMark = 0;
        }

        // Called once before the storage checkpoint is taken
        public async Task CheckpointAsync()
        {
            ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed), this);
            if (!_marked)
            {
                BeginCheckpoint();
            }
            for (int i = 0; i < _retiredBefore.Count; i++)
            {
                long page = _retiredBefore[i];
                _client.Delete(_client.FirstPageId + page);
                Volatile.Write(ref _storedBytes, _storedBytes - _pages.Bytes(page));
                _pages.Remove(page);
                _changedPages.Add(page);
            }
            Volatile.Write(ref _pagesDeleted, _pagesDeleted + _retiredBefore.Count);
            _retiredBefore.Clear();
            TrimQueues();
            // Writes every appended page and passes the deletes on
            await _client.CommitAsync();
            await WriteIndexAsync();
            (_retiredBefore, _retiredNow) = (_retiredNow, _retiredBefore);
            await _index.Commit();
            // Later appends are left to the next checkpoint
            _state.Value!.NextSequence = _markedSequence;
            _state.Value.RowCount = _rowCount - _rowsAfterMark;
            _state.Value.NextPage = _markedPage;
            _state.Value.StoredBytes = _storedBytes - _bytesAfterMark;
            await _state.Commit();
            _marked = false;
            TrimTracking();
            SetBudget();
        }

        // What a checkpoint emptied gives its room back
        private void TrimTracking()
        {
            Trim(_changedSequences);
            Trim(_changedWords);
            Trim(_changedPages);
            if (_forgotten.Capacity > SpareLimit)
            {
                _forgotten.TrimExcess();
            }
            if (_retiredNow.Capacity > SpareLimit)
            {
                _retiredNow.TrimExcess();
            }
            _released.TrimExcess(SpareLimit);
            _pages.TrimExcess();
        }

        private static void Trim(HashSet<long> emptied)
        {
            if (emptied.EnsureCapacity(0) > SpareLimit)
            {
                emptied.TrimExcess();
            }
        }

        private static long Key(long kind, long id)
        {
            return (kind << KindShift) | id;
        }

        private async Task WriteIndexAsync()
        {
            for (int i = 0; i < _forgotten.Count; i++)
            {
                await _index.Delete(Key(ReleasedKind, _forgotten[i]));
            }
            _forgotten.Clear();
            for (long sequence = _persistedSequence; sequence < _markedSequence; sequence++)
            {
                await WriteNewSequenceAsync(sequence);
            }
            foreach (long sequence in _changedSequences)
            {
                long entry = _directory.Get(sequence);
                if (entry == 0)
                {
                    await _index.Delete(sequence);
                }
                else
                {
                    await _index.Upsert(sequence, entry);
                }
            }
            _changedSequences.Clear();
            foreach (long id in _changedWords)
            {
                // Its sequence may have lost its last row since
                if (_released.TryGet(id >> WordBits, out var released))
                {
                    await _index.Upsert(Key(ReleasedKind, id), (long)released.Word((int)(id & WordMask)));
                }
            }
            _changedWords.Clear();
            _persistedSequence = _markedSequence;

            long nextPage = _markedPage;
            for (long page = _persistedPage; page < nextPage; page++)
            {
                if (!PageRecord(page, out long record, out bool retired))
                {
                    // The id was not used, or its publish failed
                    continue;
                }
                await _index.Upsert(Key(PageKind, page), record);
                if (retired)
                {
                    await _index.Upsert(Key(RetiredKind, page), 0);
                }
            }
            foreach (long page in _changedPages)
            {
                var state = _pages.State(page);
                if (state == RowHeapPageState.Retired)
                {
                    await _index.Upsert(Key(RetiredKind, page), 0);
                }
                else if (state == RowHeapPageState.None)
                {
                    // Deleted in this checkpoint
                    await _index.Delete(Key(PageKind, page));
                    await _index.Delete(Key(RetiredKind, page));
                }
            }
            _changedPages.Clear();
            _persistedPage = nextPage;
        }

        private bool PageRecord(long page, out long record, out bool retired)
        {
            ref var entry = ref _pages.Find(page);
            if (Unsafe.IsNullRef(ref entry) || entry.State == RowHeapPageState.None)
            {
                record = 0;
                retired = false;
                return false;
            }
            record = (long)(entry.Rows - 1) | ((long)(uint)entry.Bytes << RecordRowBits);
            if (entry.Merged)
            {
                record |= RecordMergedBit;
            }
            record |= (long)entry.Spread << RecordSpreadShift;
            record |= (long)entry.Sequences << RecordSequencesShift;
            retired = entry.State == RowHeapPageState.Retired;
            return true;
        }

        // Writes the slot and released rows of a new sequence
        private async Task WriteNewSequenceAsync(long sequence)
        {
            long entry = _directory.Get(sequence);
            if (entry == 0)
            {
                return;
            }
            await _index.Upsert(sequence, entry);
            if (_released.TryGet(sequence, out var released))
            {
                for (int word = 0; word < released.WordCount; word++)
                {
                    ulong bits = released.Word(word);
                    if (bits != 0)
                    {
                        await _index.Upsert(Key(ReleasedKind, (sequence << WordBits) | (uint)word), (long)bits);
                    }
                }
            }
        }
    }
}
