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

using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace FlowtideDotNet.Core.Operators.LateMaterialization
{
    internal enum RowHeapPageState : byte
    {
        None = 0,
        Stored = 1,
        // No row is read from it, it waits to be deleted
        Retired = 2
    }

    // What the heap knows of each stored page, store's thread only
    internal sealed class RowHeapPageTable
    {
        public const int ChunkBits = 12;
        public const int ChunkSize = 1 << ChunkBits;
        // An old chunk with this few pages moves to a dictionary
        public const int SparseLimit = 64;
        public const byte NoBucket = byte.MaxValue;
        public const int EntrySize = 32;
        public const int ChunkOverhead = 80;
        public const int MovedEntrySize = 48;
        private const int SpareLimit = 1024;

        internal struct Entry
        {
            public int Rows;
            public int Live;
            public int Bytes;
            // Bytes of the live rows, an estimate until measured
            public int LiveBytes;
            // No live row is larger than this
            public int MaxRow;
            // Live rows may be this many bytes below LiveBytes
            public int Uncertain;
            public RowHeapPageState State;
            public bool Merged;
            // Waits in the queue of small pages
            public bool Small;
            // Waits to be read and measured
            public bool Unmeasured;
            // The queue of sparse pages it was last put in
            public byte Bucket;
            // The largest row the page ever had, as its record stores it
            public byte Spread;
            // The sequences the page was made with, as its record stores them
            public byte Sequences;
        }

        private sealed class Chunk
        {
            public readonly Entry[] Entries = new Entry[ChunkSize];
            public int Count;
        }

        private readonly Dictionary<long, Chunk> _chunks;
        // Pages of moved chunks, and how many of each chunk
        private readonly Dictionary<long, Entry> _moved;
        private readonly Dictionary<long, int> _movedCounts;
        private Chunk? _lastChunk;
        private long _lastChunkIndex;
        private long _highestChunk;
        private long _count;
        private int _chunkCount;
        private int _movedCount;
        private bool _loading;

        public RowHeapPageTable()
        {
            _chunks = new Dictionary<long, Chunk>();
            _moved = new Dictionary<long, Entry>();
            _movedCounts = new Dictionary<long, int>();
            _lastChunkIndex = -1;
            _highestChunk = -1;
        }

        public long Count => Volatile.Read(ref _count);

        public long ByteSize => (long)Volatile.Read(ref _chunkCount) * (ChunkSize * EntrySize + ChunkOverhead) + (long)Volatile.Read(ref _movedCount) * MovedEntrySize;

        // During a restore pages come in any order, no chunk moves
        public bool Loading
        {
            get
            {
                return _loading;
            }
            set
            {
                _loading = value;
                if (!value)
                {
                    var chunks = new List<long>(_chunks.Keys);
                    foreach (long chunkIndex in chunks)
                    {
                        Shrink(chunkIndex);
                    }
                }
            }
        }

        public RowHeapPageState State(long page)
        {
            ref Entry entry = ref Find(page);
            return Unsafe.IsNullRef(ref entry) ? RowHeapPageState.None : entry.State;
        }

        public int Rows(long page)
        {
            ref Entry entry = ref Find(page);
            return Unsafe.IsNullRef(ref entry) ? 0 : entry.Rows;
        }

        public int Live(long page)
        {
            ref Entry entry = ref Find(page);
            return Unsafe.IsNullRef(ref entry) ? 0 : entry.Live;
        }

        public int Bytes(long page)
        {
            ref Entry entry = ref Find(page);
            return Unsafe.IsNullRef(ref entry) ? 0 : entry.Bytes;
        }

        public int LiveBytes(long page)
        {
            ref Entry entry = ref Find(page);
            return Unsafe.IsNullRef(ref entry) ? 0 : entry.LiveBytes;
        }

        public bool Merged(long page)
        {
            ref Entry entry = ref Find(page);
            return !Unsafe.IsNullRef(ref entry) && entry.Merged;
        }

        // The reference is good until a page is added or removed
        public ref Entry Get(long page)
        {
            ref Entry entry = ref Find(page);
            if (Unsafe.IsNullRef(ref entry) || entry.State == RowHeapPageState.None)
            {
                throw new InvalidOperationException($"The row heap knows no page {page}.");
            }
            return ref entry;
        }

        // The page, added when it is not there yet
        public ref Entry GetOrAdd(long page)
        {
            ref Entry entry = ref Find(page);
            if (!Unsafe.IsNullRef(ref entry) && entry.State != RowHeapPageState.None)
            {
                return ref entry;
            }
            return ref Add(page);
        }

        // A null reference when the page is not in the table
        public ref Entry Find(long page)
        {
            if (page < 0)
            {
                return ref Unsafe.NullRef<Entry>();
            }
            var chunk = FindChunk(page >> ChunkBits);
            if (chunk != null)
            {
                return ref chunk.Entries[page & (ChunkSize - 1)];
            }
            return ref CollectionsMarshal.GetValueRefOrNullRef(_moved, page);
        }

        // Most lookups go to the chunk of the lookup before
        private Chunk? FindChunk(long chunkIndex)
        {
            if (chunkIndex == _lastChunkIndex)
            {
                return _lastChunk;
            }
            _chunks.TryGetValue(chunkIndex, out var chunk);
            _lastChunkIndex = chunkIndex;
            _lastChunk = chunk;
            return chunk;
        }

        private ref Entry Add(long page)
        {
            if (page < 0)
            {
                throw new InvalidOperationException($"The row heap has no place for page {page}.");
            }
            long chunkIndex = page >> ChunkBits;
            Volatile.Write(ref _count, _count + 1);
            var chunk = FindChunk(chunkIndex);
            if (chunk == null && _movedCounts.TryGetValue(chunkIndex, out int moved))
            {
                // The chunk was moved, its pages are in the dictionary
                _movedCounts[chunkIndex] = moved + 1;
                Volatile.Write(ref _movedCount, _movedCount + 1);
                ref Entry added = ref CollectionsMarshal.GetValueRefOrAddDefault(_moved, page, out _);
                added.State = RowHeapPageState.Stored;
                added.Bucket = NoBucket;
                return ref added;
            }
            if (chunk == null)
            {
                chunk = new Chunk();
                _chunks.Add(chunkIndex, chunk);
                _lastChunk = chunk;
                Volatile.Write(ref _chunkCount, _chunkCount + 1);
                if (chunkIndex > _highestChunk)
                {
                    long closed = _highestChunk;
                    _highestChunk = chunkIndex;
                    // The chunk before gets no new pages, it may move
                    if (closed >= 0)
                    {
                        Shrink(closed);
                    }
                }
            }
            chunk.Count++;
            ref Entry entry = ref chunk.Entries[page & (ChunkSize - 1)];
            entry.State = RowHeapPageState.Stored;
            entry.Bucket = NoBucket;
            return ref entry;
        }

        public void Remove(long page)
        {
            ref Entry entry = ref Find(page);
            if (Unsafe.IsNullRef(ref entry) || entry.State == RowHeapPageState.None)
            {
                return;
            }
            long chunkIndex = page >> ChunkBits;
            Volatile.Write(ref _count, _count - 1);
            var chunk = FindChunk(chunkIndex);
            if (chunk == null)
            {
                _moved.Remove(page);
                Volatile.Write(ref _movedCount, _movedCount - 1);
                int left = _movedCounts[chunkIndex] - 1;
                if (left == 0)
                {
                    _movedCounts.Remove(chunkIndex);
                }
                else
                {
                    _movedCounts[chunkIndex] = left;
                }
                return;
            }
            entry = default;
            chunk.Count--;
            if (chunk.Count == 0)
            {
                DropChunk(chunkIndex);
                return;
            }
            Shrink(chunkIndex);
        }

        // Trims the dictionaries, no page reference is good after this
        public void TrimExcess()
        {
            int room = _moved.EnsureCapacity(0);
            if (room > SpareLimit && room > 4 * _moved.Count)
            {
                _moved.TrimExcess();
            }
            room = _movedCounts.EnsureCapacity(0);
            if (room > SpareLimit && room > 4 * _movedCounts.Count)
            {
                _movedCounts.TrimExcess();
            }
            room = _chunks.EnsureCapacity(0);
            if (room > SpareLimit && room > 4 * _chunks.Count)
            {
                _chunks.TrimExcess();
            }
        }

        private void DropChunk(long chunkIndex)
        {
            _chunks.Remove(chunkIndex);
            _lastChunkIndex = -1;
            _lastChunk = null;
            Volatile.Write(ref _chunkCount, _chunkCount - 1);
        }

        // Moves an old, nearly empty chunk's pages to the dictionary
        private void Shrink(long chunkIndex)
        {
            if (_loading || chunkIndex >= _highestChunk || !_chunks.TryGetValue(chunkIndex, out var chunk) || chunk.Count > SparseLimit)
            {
                return;
            }
            long first = chunkIndex << ChunkBits;
            for (int i = 0; i < chunk.Entries.Length; i++)
            {
                if (chunk.Entries[i].State != RowHeapPageState.None)
                {
                    _moved.Add(first + i, chunk.Entries[i]);
                }
            }
            _movedCounts.Add(chunkIndex, chunk.Count);
            Volatile.Write(ref _movedCount, _movedCount + chunk.Count);
            DropChunk(chunkIndex);
        }
    }
}
