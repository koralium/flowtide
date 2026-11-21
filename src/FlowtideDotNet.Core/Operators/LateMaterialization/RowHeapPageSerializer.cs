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

using FlowtideDotNet.Core.ColumnStore.Serialization;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.StateManager.Internal;
using System.Buffers;
using System.Buffers.Binary;

namespace FlowtideDotNet.Core.Operators.LateMaterialization
{
    internal sealed class RowHeapPageSerializer : IStateSerializer<RowHeapPage>
    {
        private const byte FormatVersion = 2;
        // Format, row count and the count of merged page sequences
        private const int HeaderSize = 9;
        // Sequence, first row, row count and whether slots follow
        private const int MapEntrySize = 17;

        private readonly EventBatchSerializer _serializer;
        private readonly IMemoryAllocator _memoryAllocator;
        private readonly int _columnCount;
        private readonly Action? _writeHookForTests;

        public RowHeapPageSerializer(int columnCount, IMemoryAllocator memoryAllocator, Action? writeHookForTests = null)
        {
            _columnCount = columnCount;
            _memoryAllocator = memoryAllocator;
            _writeHookForTests = writeHookForTests;
            _serializer = new EventBatchSerializer();
        }

        public void Serialize(in IBufferWriter<byte> bufferWriter, in RowHeapPage value)
        {
            _writeHookForTests?.Invoke();
            var map = value.Map;
            int size = HeaderSize + (map == null ? sizeof(long) : MapSize(map));
            var header = bufferWriter.GetSpan(size);
            header[0] = FormatVersion;
            BinaryPrimitives.WriteInt32LittleEndian(header.Slice(1), value.Count);
            if (map == null)
            {
                BinaryPrimitives.WriteInt32LittleEndian(header.Slice(5), 0);
                BinaryPrimitives.WriteInt64LittleEndian(header.Slice(HeaderSize), value.Sequence);
            }
            else
            {
                BinaryPrimitives.WriteInt32LittleEndian(header.Slice(5), map.Count);
                WriteMap(header.Slice(HeaderSize), map);
            }
            bufferWriter.Advance(size);
            _serializer.SerializeEventBatch(bufferWriter, value.Data, value.Count);
        }

        private static int MapSize(RowHeapSlotMap map)
        {
            int size = map.Count * MapEntrySize;
            for (int i = 0; i < map.Count; i++)
            {
                if (map.Slots(i) != null)
                {
                    size += map.RowCount(i) * sizeof(ushort);
                }
            }
            return size;
        }

        private static void WriteMap(Span<byte> target, RowHeapSlotMap map)
        {
            int at = 0;
            for (int i = 0; i < map.Count; i++)
            {
                var slots = map.Slots(i);
                BinaryPrimitives.WriteInt64LittleEndian(target.Slice(at), map.Sequence(i));
                BinaryPrimitives.WriteInt32LittleEndian(target.Slice(at + 8), map.FirstRow(i));
                BinaryPrimitives.WriteInt32LittleEndian(target.Slice(at + 12), map.RowCount(i));
                target[at + 16] = slots == null ? (byte)0 : (byte)1;
                at += MapEntrySize;
                if (slots != null)
                {
                    for (int j = 0; j < slots.Length; j++)
                    {
                        BinaryPrimitives.WriteUInt16LittleEndian(target.Slice(at), slots[j]);
                        at += sizeof(ushort);
                    }
                }
            }
        }

        public void Serialize(in IBufferWriter<byte> bufferWriter, in ICacheObject value)
        {
            Serialize(bufferWriter, (RowHeapPage)value);
        }

        public RowHeapPage Deserialize(ReadOnlySequence<byte> bytes, int length)
        {
            var reader = new SequenceReader<byte>(bytes);
            if (!reader.TryRead(out byte version))
            {
                throw new InvalidOperationException("A row heap page is shorter than its header.");
            }
            if (version != FormatVersion)
            {
                throw new InvalidOperationException($"Row heap page format {version} is not supported, expected {FormatVersion}.");
            }
            if (!reader.TryReadLittleEndian(out int count) || !reader.TryReadLittleEndian(out int merged))
            {
                throw new InvalidOperationException("A row heap page is shorter than its header.");
            }
            if (count < 1 || count > RowReference.MaxRowsPerPage)
            {
                throw new InvalidOperationException($"A row heap page says it holds {count} rows.");
            }
            long sequence = 0;
            RowHeapSlotMap? map = null;
            if (merged == 0)
            {
                if (!reader.TryReadLittleEndian(out sequence))
                {
                    throw new InvalidOperationException("A row heap page is shorter than its header.");
                }
                if (sequence < 1 || sequence > RowReference.MaxSequence)
                {
                    throw new InvalidOperationException($"A row heap page says it is page sequence {sequence}.");
                }
            }
            else
            {
                map = ReadMap(ref reader, merged, count);
            }
            var deserializer = new EventBatchDeserializer(_memoryAllocator);
            var result = deserializer.DeserializeBatch(ref reader);
            if (result.Count != count || result.EventBatch.Columns.Count != _columnCount)
            {
                int columns = result.EventBatch.Columns.Count;
                result.EventBatch.Dispose();
                throw new InvalidOperationException($"A row heap page holds {result.Count} rows in {columns} columns, expected {count} rows in {_columnCount} columns.");
            }
            return map == null ? new RowHeapPage(result.EventBatch, count, sequence) : new RowHeapPage(result.EventBatch, count, map);
        }

        // A map that does not fit the page is refused here
        private static RowHeapSlotMap ReadMap(ref SequenceReader<byte> reader, int count, int pageRows)
        {
            if (count < 0 || (long)count * MapEntrySize > reader.Remaining)
            {
                throw new InvalidOperationException("A row heap page is shorter than its slot map.");
            }
            var sequences = new long[count];
            var firstRows = new int[count];
            var counts = new int[count];
            var slots = new ushort[]?[count];
            long previous = 0;
            for (int i = 0; i < count; i++)
            {
                if (!reader.TryReadLittleEndian(out sequences[i]) || !reader.TryReadLittleEndian(out firstRows[i]) ||
                    !reader.TryReadLittleEndian(out counts[i]) || !reader.TryRead(out byte listed))
                {
                    throw new InvalidOperationException("A row heap page is shorter than its slot map.");
                }
                if (sequences[i] <= previous)
                {
                    throw new InvalidOperationException($"The slot map of a row heap page lists page sequence {sequences[i]} after {previous}.");
                }
                previous = sequences[i];
                // Subtraction, a sum of the two can wrap around
                if (counts[i] < 1 || firstRows[i] < 0 || counts[i] > pageRows - firstRows[i])
                {
                    throw new InvalidOperationException($"The slot map of a row heap page puts {counts[i]} rows of page sequence {sequences[i]} at row {firstRows[i]}, the page has {pageRows} rows.");
                }
                if (listed == 0)
                {
                    continue;
                }
                if (listed != 1)
                {
                    throw new InvalidOperationException($"The slot map of a row heap page marks the slots of page sequence {sequences[i]} with {listed}.");
                }
                if ((long)counts[i] * sizeof(ushort) > reader.Remaining)
                {
                    throw new InvalidOperationException("A row heap page is shorter than its slot map.");
                }
                var listedSlots = new ushort[counts[i]];
                for (int j = 0; j < listedSlots.Length; j++)
                {
                    reader.TryReadLittleEndian(out short slot);
                    listedSlots[j] = (ushort)slot;
                    if (j > 0 && listedSlots[j] <= listedSlots[j - 1])
                    {
                        throw new InvalidOperationException($"The slot map of a row heap page lists the slots of page sequence {sequences[i]} out of order.");
                    }
                }
                slots[i] = listedSlots;
            }
            // Sorted by row, the ranges must lie end to end
            var starts = (int[])firstRows.Clone();
            var lengths = (int[])counts.Clone();
            Array.Sort(starts, lengths);
            int next = 0;
            for (int i = 0; i < count; i++)
            {
                if (starts[i] != next)
                {
                    throw new InvalidOperationException($"The slot map of a row heap page has rows from {starts[i]} where row {next} comes next, the page has {pageRows} rows.");
                }
                next += lengths[i];
            }
            if (next != pageRows)
            {
                throw new InvalidOperationException($"The slot map of a row heap page places {next} rows, the page has {pageRows}.");
            }
            return new RowHeapSlotMap(sequences, firstRows, counts, slots);
        }

        public ICacheObject DeserializeCacheObject(ReadOnlySequence<byte> bytes, int length)
        {
            return Deserialize(bytes, length);
        }

        public void ClearTemporaryAllocations()
        {
        }

        public Task CheckpointAsync<TMetadata>(IStateSerializerCheckpointWriter checkpointWriter, StateClientMetadata<TMetadata> metadata)
            where TMetadata : IStorageMetadata
        {
            return Task.CompletedTask;
        }

        public Task InitializeAsync<TMetadata>(IStateSerializerInitializeReader reader, StateClientMetadata<TMetadata> metadata)
            where TMetadata : IStorageMetadata
        {
            return Task.CompletedTask;
        }

        public void Dispose()
        {
        }
    }
}
