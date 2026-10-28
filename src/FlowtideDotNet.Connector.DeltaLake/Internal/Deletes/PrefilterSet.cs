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

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Deletes
{
    // One column's hashes, exact up to 4096 values, a filter of about 10 bits each above
    internal sealed class PrefilterSet
    {
        public const int ExactLimit = 4096;
        private const int BitsPerValue = 10;
        private const int Probes = 7;
        private readonly ulong[]? _slots;
        private readonly bool[]? _used;
        private readonly int _mask;
        private readonly ulong[]? _bits;
        private readonly ulong _bitCount;

        // Sorts the values in place, sized by the distinct ones
        public PrefilterSet(Span<ulong> values)
        {
            values.Sort();
            var distinct = 0;
            for (int i = 0; i < values.Length; i++)
            {
                if (distinct == 0 || values[i] != values[distinct - 1])
                {
                    values[distinct++] = values[i];
                }
            }
            values = values.Slice(0, distinct);
            if (values.Length <= ExactLimit)
            {
                var size = (int)BitOperations.RoundUpToPowerOf2((uint)Math.Max(2 * values.Length, 2));
                _slots = new ulong[size];
                _used = new bool[size];
                _mask = size - 1;
                foreach (var value in values)
                {
                    var slot = (int)(value & (ulong)_mask);
                    while (_used[slot])
                    {
                        slot = (slot + 1) & _mask;
                    }
                    _slots[slot] = value;
                    _used[slot] = true;
                }
                return;
            }
            _bitCount = (ulong)values.Length * BitsPerValue;
            _bits = new ulong[(_bitCount + 63) / 64];
            foreach (var value in values)
            {
                for (int i = 0; i < Probes; i++)
                {
                    var bit = Bit(value, i);
                    _bits[bit >> 6] |= 1UL << (int)(bit & 63);
                }
            }
        }

        // False only when no added value equals it
        public bool MayContain(ulong value)
        {
            if (_slots != null)
            {
                for (var slot = (int)(value & (ulong)_mask); _used![slot]; slot = (slot + 1) & _mask)
                {
                    if (_slots[slot] == value)
                    {
                        return true;
                    }
                }
                return false;
            }
            for (int i = 0; i < Probes; i++)
            {
                var bit = Bit(value, i);
                if ((_bits![bit >> 6] & (1UL << (int)(bit & 63))) == 0)
                {
                    return false;
                }
            }
            return true;
        }

        private ulong Bit(ulong value, int probe)
        {
            var first = (uint)value;
            var step = (uint)(value >> 32) | 1;
            return (first + (ulong)probe * step) % _bitCount;
        }
    }
}
