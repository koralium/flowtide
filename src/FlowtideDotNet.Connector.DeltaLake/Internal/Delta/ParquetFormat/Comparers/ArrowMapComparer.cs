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

using Apache.Arrow;
using System.Buffers;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat.Comparers
{
    internal class ArrowMapComparer : IArrowComparer
    {
        private readonly IArrowComparer _keyComparer;
        private readonly IArrowComparer _valueComparer;
        private readonly FieldsCache _entries = new FieldsCache();

        public ArrowMapComparer(IArrowComparer keyComparer, IArrowComparer valueComparer)
        {
            _keyComparer = keyComparer;
            _valueComparer = valueComparer;
        }

        // By position, else by key when both maps' keys are unique
        public bool IsEqual(int leftIndex, IArrowArray array, int rightIndex, IArrowArray otherArray)
        {
            var left = (MapArray)array;
            var right = (MapArray)otherArray;

            if (left.IsNull(leftIndex) || right.IsNull(rightIndex))
            {
                return left.IsNull(leftIndex) && right.IsNull(rightIndex);
            }

            var leftOffset = left.ValueOffsets[leftIndex];
            var rightOffset = right.ValueOffsets[rightIndex];
            var length = left.GetValueLength(leftIndex);
            if (length != right.GetValueLength(rightIndex))
            {
                return false;
            }
            var leftEntries = _entries.Get(left.KeyValues);
            var rightEntries = _entries.Get(right.KeyValues);
            if (EntriesEqual(leftEntries, leftOffset, rightEntries, rightOffset, length))
            {
                return true;
            }

            var leftOrder = ArrayPool<int>.Shared.Rent(2 * length);
            var rightOrder = ArrayPool<int>.Shared.Rent(2 * length);
            try
            {
                if (SortByKey(leftEntries[0], leftOffset, length, leftOrder) != 0 || SortByKey(rightEntries[0], rightOffset, length, rightOrder) != 0)
                {
                    return false;
                }
                for (int i = 0; i < length; i++)
                {
                    if (!_keyComparer.IsEqual(leftOrder[i], leftEntries[0], rightOrder[i], rightEntries[0]) || !_valueComparer.IsEqual(leftOrder[i], leftEntries[1], rightOrder[i], rightEntries[1]))
                    {
                        return false;
                    }
                }
                return true;
            }
            finally
            {
                ArrayPool<int>.Shared.Return(leftOrder);
                ArrayPool<int>.Shared.Return(rightOrder);
            }
        }

        private bool EntriesEqual(IReadOnlyList<IArrowArray> left, int leftOffset, IReadOnlyList<IArrowArray> right, int rightOffset, int length)
        {
            for (int i = 0; i < length; i++)
            {
                if (!_keyComparer.IsEqual(leftOffset + i, left[0], rightOffset + i, right[0]) || !_valueComparer.IsEqual(leftOffset + i, left[1], rightOffset + i, right[1]))
                {
                    return false;
                }
            }
            return true;
        }

        // The entry count, then the sum over entries, in any order
        public bool TryHash(IArrowArray? array, int index, ref ulong state)
        {
            if (ArrowHash.IsNull(array, index))
            {
                ArrowHash.Add(ref state, ArrowHash.Null);
                return true;
            }
            var map = (MapArray)array!;
            var entries = _entries.Get(map.KeyValues);
            var offset = map.ValueOffsets[index];
            var length = map.GetValueLength(index);
            ulong sum = 0;
            for (int i = 0; i < length; i++)
            {
                ulong key = 0;
                ulong value = 0;
                if (!_keyComparer.TryHash(entries[0], offset + i, ref key) || !_valueComparer.TryHash(entries[1], offset + i, ref value))
                {
                    return false;
                }
                sum += ArrowHash.Mix(key ^ ArrowHash.Mix(value ^ ArrowHash.Null));
            }
            ArrowHash.Add(ref state, (ulong)length);
            ArrowHash.Add(ref state, sum);
            return true;
        }

        // Tag 0 sorts unique keys, tag 1 keeps stored order
        public int CompareCanonical(IArrowArray? left, int leftIndex, IArrowArray? right, int rightIndex)
        {
            var nulls = ArrowHash.CompareNulls(left, leftIndex, right, rightIndex);
            if (nulls != null)
            {
                return nulls.Value;
            }
            var leftMap = (MapArray)left!;
            var rightMap = (MapArray)right!;
            var leftEntries = _entries.Get(leftMap.KeyValues);
            var rightEntries = _entries.Get(rightMap.KeyValues);
            var leftLength = leftMap.GetValueLength(leftIndex);
            var rightLength = rightMap.GetValueLength(rightIndex);
            var leftOrder = ArrayPool<int>.Shared.Rent(2 * leftLength);
            var rightOrder = ArrayPool<int>.Shared.Rent(2 * rightLength);
            try
            {
                var leftTag = SortByKey(leftEntries[0], leftMap.ValueOffsets[leftIndex], leftLength, leftOrder);
                var rightTag = SortByKey(rightEntries[0], rightMap.ValueOffsets[rightIndex], rightLength, rightOrder);
                if (leftTag != rightTag)
                {
                    return leftTag.CompareTo(rightTag);
                }
                for (int i = 0; i < Math.Min(leftLength, rightLength); i++)
                {
                    var compared = _keyComparer.CompareCanonical(leftEntries[0], leftOrder[i], rightEntries[0], rightOrder[i]);
                    if (compared == 0)
                    {
                        compared = _valueComparer.CompareCanonical(leftEntries[1], leftOrder[i], rightEntries[1], rightOrder[i]);
                    }
                    if (compared != 0)
                    {
                        return compared;
                    }
                }
                return leftLength.CompareTo(rightLength);
            }
            finally
            {
                ArrayPool<int>.Shared.Return(leftOrder);
                ArrayPool<int>.Shared.Return(rightOrder);
            }
        }

        // Sorted by key, stored order with tag 1 on a repeat
        private int SortByKey(IArrowArray keys, int offset, int length, int[] order)
        {
            for (int i = 0; i < length; i++)
            {
                order[i] = offset + i;
            }
            var entries = order.AsSpan(0, length);
            MergeSort(entries, order.AsSpan(length, length), new KeyOrder(_keyComparer, keys));
            for (int i = 1; i < length; i++)
            {
                if (_keyComparer.IsEqual(entries[i - 1], keys, entries[i], keys))
                {
                    for (int j = 0; j < length; j++)
                    {
                        order[j] = offset + j;
                    }
                    return 1;
                }
            }
            return 0;
        }

        // Bottom-up merge sort, Span.Sort boxes the comparer
        private static void MergeSort<TOrder>(Span<int> items, Span<int> scratch, TOrder order)
            where TOrder : struct, IComparer<int>
        {
            for (int width = 1; width < items.Length; width *= 2)
            {
                for (int start = 0; start + width < items.Length; start += 2 * width)
                {
                    var middle = start + width;
                    var end = Math.Min(start + 2 * width, items.Length);
                    int left = start;
                    int right = middle;
                    int next = start;
                    while (left < middle && right < end)
                    {
                        scratch[next++] = order.Compare(items[right], items[left]) < 0 ? items[right++] : items[left++];
                    }
                    while (left < middle)
                    {
                        scratch[next++] = items[left++];
                    }
                    while (right < end)
                    {
                        scratch[next++] = items[right++];
                    }
                    scratch.Slice(start, end - start).CopyTo(items.Slice(start, end - start));
                }
            }
        }

        private readonly struct KeyOrder : IComparer<int>
        {
            private readonly IArrowComparer _comparer;
            private readonly IArrowArray _keys;

            public KeyOrder(IArrowComparer comparer, IArrowArray keys)
            {
                _comparer = comparer;
                _keys = keys;
            }

            public int Compare(int x, int y)
            {
                return _comparer.CompareCanonical(_keys, x, _keys, y);
            }
        }
    }
}
