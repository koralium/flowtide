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
using System.Runtime.InteropServices;

namespace FlowtideDotNet.Core.Operators.LateMaterialization
{
    internal enum RowHeapReleaseOutcome
    {
        Released,
        BeyondRows,
        AlreadyReleased
    }

    // Released rows per sequence, kept in the smallest of three forms
    internal sealed class RowHeapReleasedRows
    {
        public const int InlineRows = 120;
        // A list is kept while no larger than the bits
        public const int ListShare = 16;
        public const int InlineEntryBytes = 36;
        public const int LargeEntryBytes = 52;
        public const int ArrayOverhead = 24;

        private const int InlineHighBits = InlineRows - 64;
        private const ulong InlineHighMask = (1UL << InlineHighBits) - 1;

        // Bits of rows 0 to 119, row count in High's top byte
        internal struct Inline
        {
            public ulong Low;
            public ulong High;
        }

        internal struct Large
        {
            // One bit a row, null while a list is kept
            public ulong[]? Bits;
            // Sorted slots: the released ones, or the live ones when ListIsLive
            public ushort[]? List;
            public int Count;
            public int Rows;
            public bool ListIsLive;
        }

        // Released rows of one sequence, good until it is changed
        public readonly struct View
        {
            private readonly ulong _low;
            private readonly ulong _high;
            private readonly ulong[]? _bits;
            private readonly ushort[]? _list;
            private readonly int _count;
            private readonly bool _listIsLive;

            internal View(ulong low, ulong high, int rows)
            {
                _low = low;
                _high = high;
                Rows = rows;
            }

            internal View(in Large large)
            {
                _bits = large.Bits;
                _list = large.List;
                _count = large.Count;
                _listIsLive = large.ListIsLive;
                Rows = large.Rows;
            }

            public int Rows { get; }

            public int WordCount => (Rows + 63) >> 6;

            public bool IsReleased(int slot)
            {
                if (_bits != null)
                {
                    return (_bits[slot >> 6] & (1UL << (slot & 63))) != 0;
                }
                if (_list != null)
                {
                    return (Array.BinarySearch(_list, 0, _count, (ushort)slot) >= 0) != _listIsLive;
                }
                return slot < 64 ? (_low & (1UL << slot)) != 0 : (_high & (1UL << (slot - 64))) != 0;
            }

            // Bits of the rows from 64 * word, as stored
            public ulong Word(int word)
            {
                if (_bits != null)
                {
                    return _bits[word];
                }
                if (_list == null)
                {
                    return word == 0 ? _low : _high;
                }
                int first = word << 6;
                int at = Array.BinarySearch(_list, 0, _count, (ushort)first);
                at = at < 0 ? ~at : at;
                ulong listed = 0;
                while (at < _count && _list[at] < first + 64)
                {
                    listed |= 1UL << (_list[at] - first);
                    at++;
                }
                if (!_listIsLive)
                {
                    return listed;
                }
                int inWord = Math.Min(64, Rows - first);
                ulong all = inWord == 64 ? ulong.MaxValue : (1UL << inWord) - 1;
                return all & ~listed;
            }
        }

        private readonly Dictionary<long, Inline> _small;
        private readonly Dictionary<long, Large> _large;
        private long _arrayBytes;
        private int _smallCount;
        private int _largeCount;

        public RowHeapReleasedRows()
        {
            _small = new Dictionary<long, Inline>();
            _large = new Dictionary<long, Large>();
        }

        // Sequences that have a released row and a row left
        public int Count => Volatile.Read(ref _smallCount) + Volatile.Read(ref _largeCount);

        public long ByteSize => (long)Volatile.Read(ref _smallCount) * InlineEntryBytes + (long)Volatile.Read(ref _largeCount) * LargeEntryBytes + Volatile.Read(ref _arrayBytes);

        // The last row is only checked, the caller forgets the sequence
        public RowHeapReleaseOutcome Release(long sequence, int slot, int live, out int rows)
        {
            ref Inline small = ref CollectionsMarshal.GetValueRefOrNullRef(_small, sequence);
            if (!Unsafe.IsNullRef(ref small))
            {
                rows = (int)(small.High >> InlineHighBits);
                if (slot >= rows)
                {
                    return RowHeapReleaseOutcome.BeyondRows;
                }
                ref ulong word = ref (slot < 64 ? ref small.Low : ref small.High);
                ulong bit = 1UL << (slot & 63);
                if ((word & bit) != 0)
                {
                    return RowHeapReleaseOutcome.AlreadyReleased;
                }
                if (live > 1)
                {
                    word |= bit;
                }
                return RowHeapReleaseOutcome.Released;
            }
            ref Large large = ref CollectionsMarshal.GetValueRefOrNullRef(_large, sequence);
            if (!Unsafe.IsNullRef(ref large))
            {
                rows = large.Rows;
                if (slot >= rows)
                {
                    return RowHeapReleaseOutcome.BeyondRows;
                }
                if (new View(in large).IsReleased(slot))
                {
                    return RowHeapReleaseOutcome.AlreadyReleased;
                }
                if (live > 1)
                {
                    Add(ref large, slot, live - 1);
                }
                return RowHeapReleaseOutcome.Released;
            }
            rows = live;
            if (slot >= rows)
            {
                return RowHeapReleaseOutcome.BeyondRows;
            }
            if (live > 1)
            {
                AddSequence(sequence, slot, rows);
            }
            return RowHeapReleaseOutcome.Released;
        }

        private void AddSequence(long sequence, int slot, int rows)
        {
            if (rows <= InlineRows)
            {
                var small = new Inline() { High = (ulong)rows << InlineHighBits };
                if (slot < 64)
                {
                    small.Low = 1UL << slot;
                }
                else
                {
                    small.High |= 1UL << (slot - 64);
                }
                _small.Add(sequence, small);
                Volatile.Write(ref _smallCount, _smallCount + 1);
                return;
            }
            var large = new Large() { Rows = rows, List = new ushort[4], Count = 1 };
            large.List[0] = (ushort)slot;
            _large.Add(sequence, large);
            Volatile.Write(ref _largeCount, _largeCount + 1);
            AddArrayBytes(ListBytes(4));
        }

        // Releases a row of a larger sequence with `live` rows left
        private void Add(ref Large large, int slot, int live)
        {
            if (large.Bits != null)
            {
                large.Bits[slot >> 6] |= 1UL << (slot & 63);
                if (live * ListShare <= large.Rows)
                {
                    ToLiveList(ref large, live);
                }
                return;
            }
            var list = large.List!;
            int at = ~Array.BinarySearch(list, 0, large.Count, (ushort)slot);
            if (large.ListIsLive)
            {
                // The slot is live, so it is in the list
                at = ~at;
                Array.Copy(list, at + 1, list, at, large.Count - at - 1);
                large.Count--;
                if (large.Count * 2 <= list.Length && list.Length > 4)
                {
                    Resize(ref large, Math.Max(4, large.Count));
                }
                return;
            }
            if ((large.Count + 1) * ListShare > large.Rows)
            {
                ToBits(ref large);
                large.Bits![slot >> 6] |= 1UL << (slot & 63);
                return;
            }
            if (large.Count == list.Length)
            {
                Resize(ref large, list.Length * 2);
                list = large.List!;
            }
            Array.Copy(list, at, list, at + 1, large.Count - at);
            list[at] = (ushort)slot;
            large.Count++;
        }

        private void Resize(ref Large large, int size)
        {
            var list = new ushort[size];
            Array.Copy(large.List!, list, large.Count);
            AddArrayBytes(ListBytes(size) - ListBytes(large.List!.Length));
            large.List = list;
        }

        private void ToBits(ref Large large)
        {
            var bits = new ulong[(large.Rows + 63) >> 6];
            for (int i = 0; i < large.Count; i++)
            {
                bits[large.List![i] >> 6] |= 1UL << (large.List[i] & 63);
            }
            AddArrayBytes(BitsBytes(bits.Length) - ListBytes(large.List!.Length));
            large.Bits = bits;
            large.List = null;
            large.Count = 0;
        }

        private void ToLiveList(ref Large large, int live)
        {
            var list = new ushort[Math.Max(4, live)];
            int count = 0;
            for (int slot = 0; slot < large.Rows; slot++)
            {
                if ((large.Bits![slot >> 6] & (1UL << (slot & 63))) == 0)
                {
                    list[count] = (ushort)slot;
                    count++;
                }
            }
            AddArrayBytes(ListBytes(list.Length) - BitsBytes(large.Bits!.Length));
            large.List = list;
            large.Count = count;
            large.ListIsLive = true;
            large.Bits = null;
        }

        private static int ListBytes(int length)
        {
            return ArrayOverhead + length * sizeof(ushort);
        }

        private static int BitsBytes(int length)
        {
            return ArrayOverhead + length * sizeof(ulong);
        }

        private void AddArrayBytes(long bytes)
        {
            Volatile.Write(ref _arrayBytes, _arrayBytes + bytes);
        }

        public bool TryGet(long sequence, out View view)
        {
            if (_small.TryGetValue(sequence, out var small))
            {
                view = new View(small.Low, small.High & InlineHighMask, (int)(small.High >> InlineHighBits));
                return true;
            }
            ref Large large = ref CollectionsMarshal.GetValueRefOrNullRef(_large, sequence);
            if (!Unsafe.IsNullRef(ref large))
            {
                view = new View(in large);
                return true;
            }
            view = default;
            return false;
        }

        // The sequence has no row left
        public void Forget(long sequence)
        {
            if (_small.Remove(sequence))
            {
                Volatile.Write(ref _smallCount, _smallCount - 1);
            }
            else if (_large.Remove(sequence, out var large))
            {
                Volatile.Write(ref _largeCount, _largeCount - 1);
                AddArrayBytes(large.Bits != null ? -BitsBytes(large.Bits.Length) : -ListBytes(large.List!.Length));
            }
        }

        // Restores a sequence from its rows and released bits
        public void Restore(long sequence, int rows, ReadOnlySpan<ulong> words)
        {
            if (rows <= InlineRows)
            {
                ulong high = words.Length > 1 ? words[1] : 0;
                _small.Add(sequence, new Inline() { Low = words[0], High = high | ((ulong)rows << InlineHighBits) });
                Volatile.Write(ref _smallCount, _smallCount + 1);
                return;
            }
            var bits = new ulong[(rows + 63) >> 6];
            words.CopyTo(bits);
            int released = 0;
            for (int i = 0; i < bits.Length; i++)
            {
                released += BitOperations.PopCount(bits[i]);
            }
            var large = new Large() { Rows = rows, Bits = bits };
            AddArrayBytes(BitsBytes(bits.Length));
            if ((rows - released) * ListShare <= rows)
            {
                ToLiveList(ref large, rows - released);
            }
            else if (released * ListShare <= rows)
            {
                ToReleasedList(ref large, released);
            }
            _large.Add(sequence, large);
            Volatile.Write(ref _largeCount, _largeCount + 1);
        }

        private void ToReleasedList(ref Large large, int released)
        {
            var list = new ushort[Math.Max(4, released)];
            int count = 0;
            for (int slot = 0; slot < large.Rows; slot++)
            {
                if ((large.Bits![slot >> 6] & (1UL << (slot & 63))) != 0)
                {
                    list[count] = (ushort)slot;
                    count++;
                }
            }
            AddArrayBytes(ListBytes(list.Length) - BitsBytes(large.Bits!.Length));
            large.List = list;
            large.Count = count;
            large.Bits = null;
        }

        // The tables keep the room of their largest time otherwise
        public void TrimExcess(int spareLimit)
        {
            int room = _small.EnsureCapacity(0);
            if (room > spareLimit && room > 4 * _small.Count)
            {
                _small.TrimExcess();
            }
            room = _large.EnsureCapacity(0);
            if (room > spareLimit && room > 4 * _large.Count)
            {
                _large.TrimExcess();
            }
        }
    }
}
