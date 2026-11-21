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
    // Where a merged page keeps the rows of each sequence
    internal sealed class RowHeapSlotMap
    {
        private readonly long[] _sequences;
        private readonly int[] _firstRows;
        private readonly int[] _counts;
        private readonly ushort[]?[] _slots;

        // Sequences sorted, slots null when they are 0 to count - 1
        public RowHeapSlotMap(long[] sequences, int[] firstRows, int[] counts, ushort[]?[] slots)
        {
            _sequences = sequences;
            _firstRows = firstRows;
            _counts = counts;
            _slots = slots;
        }

        public int Count => _sequences.Length;

        public long Sequence(int index)
        {
            return _sequences[index];
        }

        public int FirstRow(int index)
        {
            return _firstRows[index];
        }

        public int RowCount(int index)
        {
            return _counts[index];
        }

        public ushort[]? Slots(int index)
        {
            return _slots[index];
        }

        public int ByteSize
        {
            get
            {
                int size = _sequences.Length * (sizeof(long) + 2 * sizeof(int) + sizeof(long));
                for (int i = 0; i < _slots.Length; i++)
                {
                    var slots = _slots[i];
                    if (slots != null)
                    {
                        size += slots.Length * sizeof(ushort);
                    }
                }
                return size;
            }
        }

        // Below zero when the page holds no row of the sequence
        public int Find(long sequence)
        {
            return Array.BinarySearch(_sequences, sequence);
        }

        // Below zero when the page does not hold the slot
        public int Row(int index, int slot)
        {
            var slots = _slots[index];
            if (slots == null)
            {
                return slot < _counts[index] ? _firstRows[index] + slot : -1;
            }
            int at = Array.BinarySearch(slots, (ushort)slot);
            return at < 0 ? -1 : _firstRows[index] + at;
        }

        public int SlotAt(int index, int position)
        {
            var slots = _slots[index];
            return slots == null ? position : slots[position];
        }
    }
}
