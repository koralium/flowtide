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
using Apache.Arrow.Types;
using System.Runtime.CompilerServices;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat.ParquetWriters
{
    // One output leaf; levels include list entries above
    internal readonly record struct LeafSize(long Built, long Length, long Levels, bool Repeated, long RowLevels = 1, long Window = 0);

    // The open group's builder buffers, in total and per leaf
    internal sealed class BuilderSizes
    {
        private long _leafBuilt;
        private long _leafLength;
        private long _ancestorLevels;
        private int _repeatedDepth;
        // The lists and maps above the leaf, outermost first
        private readonly List<ArrowBuffer.Builder<int>> _lists = new List<ArrowBuffer.Builder<int>>();
        private ArrowBuffer.Builder<int>? _valueOffsets;

        // Arrow writes a column in batches of this many levels
        private const int WriteBatch = 1024;

        // Measure longest rows and write-batch windows, for the flush recheck
        public bool Windows { get; set; }

        public long Capacity { get; private set; }

        public long Length { get; private set; }

        // What Build() allocates natively, with 64 B rounding and padding
        public long Built { get; private set; }

        public int Buffers { get; private set; }

        public List<LeafSize> Leaves { get; } = new List<LeafSize>();

        public void Add<T>(ArrowBuffer.Builder<T> buffer) where T : struct
        {
            var size = Unsafe.SizeOf<T>();
            Add((long)buffer.Capacity * size, (long)buffer.Length * size);
        }

        public void Add(ArrowBuffer.BitmapBuilder bitmap)
        {
            Add(bitmap.Capacity / 8, (bitmap.Length + 7) / 8);
        }

        private void Add(long capacity, long length)
        {
            var built = (capacity + 63) / 64 * 64 + 64;
            Capacity += capacity;
            Length += length;
            Built += built;
            Buffers++;
            _leafBuilt += built;
            _leafLength += length;
        }

        // Buffers above the leaves count in the totals only
        public void EndNode()
        {
            _leafBuilt = 0;
            _leafLength = 0;
        }

        // A list or map entry adds at most one level below
        public void Enter(ArrowBuffer.Builder<int> offsets, long entries)
        {
            EndNode();
            _ancestorLevels += entries;
            _repeatedDepth++;
            _lists.Add(offsets);
        }

        public void Exit(long entries)
        {
            _ancestorLevels -= entries;
            _repeatedDepth--;
            _lists.RemoveAt(_lists.Count - 1);
        }

        public void EndLeaf(long slots)
        {
            var (rowLevels, window) = Windows ? Windowed(slots) : (1L, 0L);
            Leaves.Add(new LeafSize(_leafBuilt, _leafLength, slots + _ancestorLevels, _repeatedDepth > 0, rowLevels, window));
            _valueOffsets = null;
            EndNode();
        }

        // The longest row's levels and a write batch's most value bytes
        private (long RowLevels, long Window) Windowed(long slots)
        {
            var values = _valueOffsets == null ? Span<int>.Empty : _valueOffsets.Span.Slice(0, _valueOffsets.Length);
            if (_lists.Count == 0)
            {
                // Flat batches are 1,024 values, any 1,024 in a row
                var count = values.Length - 1;
                long flat = 0;
                for (int i = 0; count > 0 && (i == 0 || i + WriteBatch <= count); i++)
                {
                    flat = Math.Max(flat, values[Math.Min(i + WriteBatch, count)] - values[i]);
                }
                return (1, flat);
            }
            // A repeated batch runs from a row until it holds 1,024 levels
            var rows = _lists[0].Length - 1;
            long longest = 1, window = 0;
            for (int start = 0, end = 0; start < rows; start++)
            {
                longest = Math.Max(longest, Span(start, start + 1).Levels);
                end = Math.Max(end, start + 1);
                while (end < rows && FewestLevels(start, end) < WriteBatch)
                {
                    end++;
                }
                if (values.Length > 0)
                {
                    var (_, a, b) = Span(start, end);
                    window = Math.Max(window, values[b] - values[a]);
                }
            }
            return (longest, window);
        }

        // At least a level per row and per leaf slot, a lower bound
        private long FewestLevels(int start, int end)
        {
            var (_, a, b) = Span(start, end);
            return Math.Max(end - start, b - a);
        }

        // Rows [start, end) as levels and the leaf's slot range
        private (long Levels, int A, int B) Span(int start, int end)
        {
            long levels = 0;
            int a = start, b = end;
            foreach (var list in _lists)
            {
                levels += b - a;
                var offsets = list.Span;
                (a, b) = (offsets[a], offsets[b]);
            }
            return (levels + b - a, a, b);
        }

        public void AddLeaf<T>(int length, ArrowBuffer.Builder<T> values, ArrowBuffer.BitmapBuilder validity) where T : struct
        {
            Add(values);
            Add(validity);
            EndLeaf(length);
        }

        public void AddLeaf(int length, ArrowBuffer.Builder<int> offsets, ArrowBuffer.Builder<byte> values, ArrowBuffer.BitmapBuilder validity)
        {
            Add(offsets);
            _valueOffsets = offsets;
            AddLeaf(length, values, validity);
        }

        public void Clear()
        {
            Capacity = 0;
            Length = 0;
            Built = 0;
            Buffers = 0;
            Leaves.Clear();
            _ancestorLevels = 0;
            _repeatedDepth = 0;
            _lists.Clear();
            _valueOffsets = null;
            Windows = false;
            EndNode();
        }
    }

    // Typed builders keep their buffers protected, these expose them
    internal sealed class SizedInt8Builder : Int8Array.Builder
    {
        public void Measure(BuilderSizes sizes) => sizes.AddLeaf(Length, ValueBuffer, ValidityBuffer);
    }

    internal sealed class SizedInt16Builder : Int16Array.Builder
    {
        public void Measure(BuilderSizes sizes) => sizes.AddLeaf(Length, ValueBuffer, ValidityBuffer);
    }

    internal sealed class SizedInt32Builder : Int32Array.Builder
    {
        public void Measure(BuilderSizes sizes) => sizes.AddLeaf(Length, ValueBuffer, ValidityBuffer);
    }

    internal sealed class SizedInt64Builder : Int64Array.Builder
    {
        public void Measure(BuilderSizes sizes) => sizes.AddLeaf(Length, ValueBuffer, ValidityBuffer);
    }

    internal sealed class SizedFloatBuilder : FloatArray.Builder
    {
        public void Measure(BuilderSizes sizes) => sizes.AddLeaf(Length, ValueBuffer, ValidityBuffer);
    }

    internal sealed class SizedDoubleBuilder : DoubleArray.Builder
    {
        public void Measure(BuilderSizes sizes) => sizes.AddLeaf(Length, ValueBuffer, ValidityBuffer);
    }

    internal sealed class SizedStringBuilder : StringArray.Builder
    {
        public void Measure(BuilderSizes sizes) => sizes.AddLeaf(Length, ValueOffsets, ValueBuffer, ValidityBuffer);
    }

    internal sealed class SizedBinaryBuilder : BinaryArray.Builder
    {
        public void Measure(BuilderSizes sizes) => sizes.AddLeaf(Length, ValueOffsets, ValueBuffer, ValidityBuffer);
    }

    internal sealed class SizedDecimal128Builder : Decimal128Array.Builder
    {
        public SizedDecimal128Builder(Decimal128Type type) : base(type)
        {
        }

        public void Measure(BuilderSizes sizes) => sizes.AddLeaf(Length, ValueBuffer, ValidityBuffer);
    }
}
