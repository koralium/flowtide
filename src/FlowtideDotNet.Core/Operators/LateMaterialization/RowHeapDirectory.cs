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

namespace FlowtideDotNet.Core.Operators.LateMaterialization
{
    // Sequence to value, read without a lock while one thread writes
    internal sealed class RowHeapDirectory
    {
        public const int ChunkBits = 12;
        public const int ChunkSize = 1 << ChunkBits;
        // An old chunk with this few entries becomes a sorted list
        public const int SparseLimit = 64;

        private const int SparseChunkOverhead = 64;

        // The slots never change, a value is written in place
        private sealed class SparseChunk
        {
            public SparseChunk(ushort[] slots, long[] values)
            {
                Slots = slots;
                Values = values;
            }

            public ushort[] Slots { get; }

            public long[] Values { get; }

            public int Find(int slot)
            {
                return Array.BinarySearch(Slots, (ushort)slot);
            }
        }

        // A chunk is a long[], a SparseChunk or null
        private object?[] _chunks;
        private int[] _counts;
        private long _highestChunk;
        private int _denseChunks;
        private int _sparseChunks;
        // Entries the short lists have room for, zeros included
        private long _sparseEntries;

        public RowHeapDirectory()
        {
            _chunks = new object?[16];
            _counts = new int[16];
            _highestChunk = -1;
        }

        public long ByteSize
        {
            get
            {
                long table = (long)Volatile.Read(ref _chunks).Length * (sizeof(long) + sizeof(int));
                long dense = (long)Volatile.Read(ref _denseChunks) * ChunkSize * sizeof(long);
                long sparse = Volatile.Read(ref _sparseEntries) * (sizeof(long) + sizeof(ushort)) + (long)Volatile.Read(ref _sparseChunks) * SparseChunkOverhead;
                return table + dense + sparse;
            }
        }

        // Zero when the sequence has no page
        public long Get(long sequence)
        {
            var chunks = Volatile.Read(ref _chunks);
            long chunkIndex = sequence >> ChunkBits;
            // One compare for below zero and beyond the table
            if ((ulong)chunkIndex >= (ulong)chunks.Length)
            {
                return 0;
            }
            var chunk = Volatile.Read(ref chunks[chunkIndex]);
            if (chunk is long[] dense)
            {
                return Volatile.Read(ref dense[sequence & (ChunkSize - 1)]);
            }
            if (chunk is SparseChunk sparse)
            {
                int index = sparse.Find((int)(sequence & (ChunkSize - 1)));
                return index < 0 ? 0 : Volatile.Read(ref sparse.Values[index]);
            }
            return 0;
        }

        // Called by the writing thread, zero takes the sequence out
        public void Set(long sequence, long value)
        {
            long chunkIndex = sequence >> ChunkBits;
            if (sequence < 1 || chunkIndex >= Array.MaxLength)
            {
                throw new InvalidOperationException($"The row heap directory has no slot for page sequence {sequence}.");
            }
            var chunks = _chunks;
            if (chunkIndex >= chunks.Length)
            {
                if (value == 0)
                {
                    return;
                }
                int size = Math.Max(chunks.Length * 2, (int)chunkIndex + 1);
                var larger = new object?[size];
                Array.Copy(chunks, larger, chunks.Length);
                Array.Resize(ref _counts, size);
                // The chunks are shared, so a reader of the old table loses nothing
                Volatile.Write(ref _chunks, larger);
                chunks = larger;
            }
            int slot = (int)(sequence & (ChunkSize - 1));
            var chunk = chunks[chunkIndex];
            if (chunk is long[] dense)
            {
                long before = dense[slot];
                Volatile.Write(ref dense[slot], value);
                if (before != 0 && value == 0)
                {
                    Removed(chunks, chunkIndex, dense);
                }
                else if (before == 0 && value != 0)
                {
                    _counts[chunkIndex]++;
                }
                return;
            }
            if (chunk is SparseChunk sparse)
            {
                SetSparse(chunks, chunkIndex, sparse, slot, value);
                return;
            }
            if (value == 0)
            {
                return;
            }
            var created = new long[ChunkSize];
            created[slot] = value;
            Volatile.Write(ref chunks[chunkIndex], created);
            _counts[chunkIndex] = 1;
            Volatile.Write(ref _denseChunks, _denseChunks + 1);
            if (chunkIndex > _highestChunk)
            {
                long closed = _highestChunk;
                _highestChunk = chunkIndex;
                // The chunk before gets no new sequences, it may shrink
                if (closed >= 0 && chunks[closed] is long[] older)
                {
                    Shrink(chunks, closed, older);
                }
            }
        }

        private void Removed(object?[] chunks, long chunkIndex, long[] dense)
        {
            _counts[chunkIndex]--;
            if (_counts[chunkIndex] == 0)
            {
                Volatile.Write(ref chunks[chunkIndex], null);
                Volatile.Write(ref _denseChunks, _denseChunks - 1);
                return;
            }
            Shrink(chunks, chunkIndex, dense);
        }

        // Swaps an old, nearly empty dense chunk for a sorted list
        private void Shrink(object?[] chunks, long chunkIndex, long[] dense)
        {
            int count = _counts[chunkIndex];
            if (count > SparseLimit || chunkIndex >= _highestChunk)
            {
                return;
            }
            var slots = new ushort[count];
            var values = new long[count];
            int next = 0;
            for (int i = 0; i < dense.Length && next < count; i++)
            {
                if (dense[i] != 0)
                {
                    slots[next] = (ushort)i;
                    values[next] = dense[i];
                    next++;
                }
            }
            // A reader of the dense chunk still finds its pages
            Volatile.Write(ref chunks[chunkIndex], new SparseChunk(slots, values));
            Volatile.Write(ref _denseChunks, _denseChunks - 1);
            Volatile.Write(ref _sparseChunks, _sparseChunks + 1);
            Volatile.Write(ref _sparseEntries, _sparseEntries + count);
        }

        private void SetSparse(object?[] chunks, long chunkIndex, SparseChunk sparse, int slot, long value)
        {
            int index = sparse.Find(slot);
            if (index >= 0)
            {
                long before = sparse.Values[index];
                if (before == value)
                {
                    return;
                }
                // Written in place, also a zero
                Volatile.Write(ref sparse.Values[index], value);
                if (before != 0 && value == 0)
                {
                    _counts[chunkIndex]--;
                    if (_counts[chunkIndex] == 0)
                    {
                        Volatile.Write(ref chunks[chunkIndex], null);
                        Volatile.Write(ref _sparseChunks, _sparseChunks - 1);
                        Volatile.Write(ref _sparseEntries, _sparseEntries - sparse.Slots.Length);
                    }
                    else if (_counts[chunkIndex] * 2 <= sparse.Slots.Length)
                    {
                        Compact(chunks, chunkIndex, sparse);
                    }
                }
                else if (before == 0)
                {
                    _counts[chunkIndex]++;
                }
                return;
            }
            if (value == 0)
            {
                return;
            }
            // Only a restore adds a sequence to a short chunk
            int count = sparse.Slots.Length;
            int at = ~index;
            var moreSlots = new ushort[count + 1];
            var moreValues = new long[count + 1];
            Array.Copy(sparse.Slots, 0, moreSlots, 0, at);
            Array.Copy(sparse.Slots, at, moreSlots, at + 1, count - at);
            Array.Copy(sparse.Values, 0, moreValues, 0, at);
            Array.Copy(sparse.Values, at, moreValues, at + 1, count - at);
            moreSlots[at] = (ushort)slot;
            moreValues[at] = value;
            _counts[chunkIndex]++;
            Volatile.Write(ref _sparseEntries, _sparseEntries + 1);
            Volatile.Write(ref chunks[chunkIndex], new SparseChunk(moreSlots, moreValues));
        }

        // Halves a short list when half of it is zeros
        private void Compact(object?[] chunks, long chunkIndex, SparseChunk sparse)
        {
            int count = _counts[chunkIndex];
            var slots = new ushort[count];
            var values = new long[count];
            int next = 0;
            for (int i = 0; i < sparse.Slots.Length && next < count; i++)
            {
                if (sparse.Values[i] != 0)
                {
                    slots[next] = sparse.Slots[i];
                    values[next] = sparse.Values[i];
                    next++;
                }
            }
            Volatile.Write(ref chunks[chunkIndex], new SparseChunk(slots, values));
            Volatile.Write(ref _sparseEntries, _sparseEntries + count - sparse.Slots.Length);
        }
    }
}
