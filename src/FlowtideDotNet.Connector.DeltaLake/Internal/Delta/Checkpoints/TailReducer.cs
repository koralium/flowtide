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

using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance;
using System.Runtime.InteropServices;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Checkpoints
{
    internal delegate void EntryVisitor(in ArenaEntry entry);

    /// <summary>
    /// The latest record per key, in an open-addressing index over the arena, all within the arena allowance; sort mode drains it into runs.
    /// </summary>
    internal sealed class TailReducer : IDisposable
    {
        public const long DefaultArenaCap = 64L << 20;

        [StructLayout(LayoutKind.Sequential)]
        private struct Slot
        {
            public long Location;
            public int Hash;
            public int Used;
        }

        private static readonly int s_slotBytes = Marshal.SizeOf<Slot>();

        private readonly JobMemory _memory;
        private readonly long _cap;
        private readonly RecordArena _arena;
        private Slot[] _slots = Array.Empty<Slot>();
        private int _count;

        public TailReducer(JobMemory memory, long arenaCap = DefaultArenaCap)
        {
            _memory = memory;
            _cap = arenaCap;
            _arena = new RecordArena(memory, arenaCap);
        }

        public int Count => _count;

        // Arena chunks and index slots held
        public long Bytes => _arena.Bytes + (long)_slots.Length * s_slotBytes;

        private static int HashOf(ReadOnlySpan<byte> key)
        {
            var hash = new HashCode();
            hash.AddBytes(key);
            return hash.ToHashCode() | 1;
        }

        /// <summary>
        /// Keeps the record unless its key holds a later one; false when the arena allowance would be passed, and nothing is added, though an empty reducer takes any record.
        /// </summary>
        public bool TryAdd(ReadOnlySpan<byte> key, long version, long ordinal, ReadOnlySpan<byte> record)
        {
            // Index growth holds the old and the new index together
            long indexGrowth = 0;
            if ((_count + 1) * 4L > _slots.Length * 3L)
            {
                indexGrowth = (long)Math.Max(16, _slots.Length * 2) * s_slotBytes;
            }
            if (_count > 0 && Bytes + _arena.GrowthFor(key.Length, record.Length) + indexGrowth > _cap)
            {
                return false;
            }
            if (indexGrowth > 0)
            {
                Grow();
            }
            var hash = HashOf(key);
            var index = Find(key, hash);
            if (_slots[index].Used == 0)
            {
                _slots[index].Used = 1;
                _slots[index].Hash = hash;
                _count++;
            }
            else
            {
                // A root row read after the tail never replaces the tail's later entry for its key
                var earlier = _arena.Read(_slots[index].Location);
                if (earlier.Version > version || (earlier.Version == version && earlier.Ordinal > ordinal))
                {
                    return true;
                }
            }
            _slots[index].Location = _arena.Append(key, version, ordinal, record);
            return true;
        }

        public bool Contains(ReadOnlySpan<byte> key)
        {
            if (_count == 0)
            {
                return false;
            }
            return _slots[Find(key, HashOf(key))].Used != 0;
        }

        private int Find(ReadOnlySpan<byte> key, int hash)
        {
            var mask = _slots.Length - 1;
            var index = hash & mask;
            while (_slots[index].Used != 0)
            {
                if (_slots[index].Hash == hash && _arena.Read(_slots[index].Location).Key.SequenceEqual(key))
                {
                    return index;
                }
                index = (index + 1) & mask;
            }
            return index;
        }

        private void Grow()
        {
            var length = Math.Max(16, _slots.Length * 2);
            _memory.Reserve((long)length * s_slotBytes);
            var old = _slots;
            _slots = new Slot[length];
            var mask = length - 1;
            foreach (var slot in old)
            {
                if (slot.Used == 0)
                {
                    continue;
                }
                var index = slot.Hash & mask;
                while (_slots[index].Used != 0)
                {
                    index = (index + 1) & mask;
                }
                _slots[index] = slot;
            }
            _memory.Release((long)old.Length * s_slotBytes);
        }

        public void ForEach(EntryVisitor visitor)
        {
            foreach (var slot in _slots)
            {
                if (slot.Used != 0)
                {
                    visitor(_arena.Read(slot.Location));
                }
            }
        }

        /// <summary>
        /// Writes every kept entry to a run in key order, sorting the index in place, then empties the map and keeps its index for the next run.
        /// </summary>
        public void DrainSorted(RunWriter run)
        {
            var used = 0;
            for (int i = 0; i < _slots.Length; i++)
            {
                if (_slots[i].Used != 0)
                {
                    _slots[used++] = _slots[i];
                }
            }
            var arena = _arena;
            _slots.AsSpan(0, used).Sort((x, y) => arena.Read(x.Location).Key.SequenceCompareTo(arena.Read(y.Location).Key));
            for (int i = 0; i < used; i++)
            {
                run.Write(_arena.Raw(_slots[i].Location));
            }
            _arena.Clear();
            Array.Clear(_slots);
            _count = 0;
        }

        public void Clear()
        {
            _arena.Clear();
            _memory.Release((long)_slots.Length * s_slotBytes);
            _slots = Array.Empty<Slot>();
            _count = 0;
        }

        public void Dispose() => Clear();
    }
}
