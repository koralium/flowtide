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

using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Tree;
using System.Buffers;
using System.Buffers.Binary;

namespace FlowtideDotNet.Core.Operators.Join.MultiJoin
{
    /// <summary>
    /// A weight under a type of its own, so the trees of the multi join get code apart from other trees that store a number.
    /// </summary>
    internal readonly struct MultiJoinWeight
    {
        public readonly int Value;

        public MultiJoinWeight(int value)
        {
            Value = value;
        }
    }

    /// <summary>
    /// The weights of the rows of a leaf. Rows leave and are moved between leaves in one pass over the list.
    /// </summary>
    internal sealed class MultiJoinWeightContainer : IValueContainer<MultiJoinWeight>
    {
        private readonly PrimitiveList<MultiJoinWeight> _values;

        public MultiJoinWeightContainer(IMemoryAllocator memoryAllocator)
        {
            _values = new PrimitiveList<MultiJoinWeight>(memoryAllocator);
        }

        public MultiJoinWeightContainer(FlowtideMemory memory, int count, IMemoryAllocator memoryAllocator)
        {
            _values = new PrimitiveList<MultiJoinWeight>(memory, count, memoryAllocator);
        }

        public Memory<byte> Memory => _values.SlicedMemory;

        public Span<byte> SlicedSpan => _values.SlicedSpan;

        public int Count => _values.Count;

        public void AddRangeFrom(IValueContainer<MultiJoinWeight> container, int start, int count)
        {
            if (container is not MultiJoinWeightContainer other)
            {
                throw new NotSupportedException();
            }
            _values.AddRangeFrom(other._values, start, count);
        }

        public void Dispose()
        {
            _values.Dispose();
        }

        public MultiJoinWeight Get(int index)
        {
            return _values.Get(index);
        }

        public ref MultiJoinWeight GetRef(int index)
        {
            return ref _values.GetRef(in index);
        }

        public void Insert(int index, MultiJoinWeight value)
        {
            _values.InsertAt(index, value);
        }

        public void RemoveAt(int index)
        {
            _values.RemoveAt(index);
        }

        public void RemoveRange(int start, int count)
        {
            _values.RemoveRange(start, count);
        }

        public void Update(int index, MultiJoinWeight value)
        {
            _values.Update(index, value);
        }

        public int GetByteSize()
        {
            return _values.Count * sizeof(int);
        }

        public int GetByteSize(int start, int end)
        {
            return (end - start + 1) * sizeof(int);
        }

        public void InsertFrom(MultiJoinWeight[] values, ReadOnlySpan<int> sortedLookup, ReadOnlySpan<int> targetPositions)
        {
            _values.InsertFrom(values, sortedLookup, targetPositions);
        }

        public void DeleteBatch(ReadOnlySpan<int> positions)
        {
            _values.DeleteBatch(positions);
        }
    }

    internal sealed class MultiJoinWeightSerializer : IBplusTreeValueSerializer<MultiJoinWeight, MultiJoinWeightContainer>
    {
        private readonly IMemoryAllocator _memoryAllocator;

        public MultiJoinWeightSerializer(IMemoryAllocator memoryAllocator)
        {
            _memoryAllocator = memoryAllocator;
        }

        public Task CheckpointAsync(IBPlusTreeSerializerCheckpointContext context)
        {
            return Task.CompletedTask;
        }

        public MultiJoinWeightContainer CreateEmpty()
        {
            return new MultiJoinWeightContainer(_memoryAllocator);
        }

        public MultiJoinWeightContainer Deserialize(ref SequenceReader<byte> reader)
        {
            if (!reader.TryReadLittleEndian(out int count))
            {
                throw new InvalidOperationException("Failed to read count");
            }
            if (!reader.TryReadLittleEndian(out int length))
            {
                throw new InvalidOperationException("Failed to read length");
            }
            var nativeMemory = _memoryAllocator.AllocateMemory(length);
            if (!reader.TryCopyTo(nativeMemory.Span.Slice(0, length)))
            {
                _memoryAllocator.Free(ref nativeMemory);
                throw new InvalidOperationException("Failed to read bytes");
            }
            reader.Advance(length);
            return new MultiJoinWeightContainer(nativeMemory, count, _memoryAllocator);
        }

        public Task InitializeAsync(IBPlusTreeSerializerInitializeContext context)
        {
            return Task.CompletedTask;
        }

        public void Serialize(in IBufferWriter<byte> writer, in MultiJoinWeightContainer values)
        {
            var data = values.SlicedSpan;
            var header = writer.GetSpan(8);
            BinaryPrimitives.WriteInt32LittleEndian(header, values.Count);
            BinaryPrimitives.WriteInt32LittleEndian(header.Slice(4), data.Length);
            writer.Advance(8);
            writer.Write(data);
        }
    }
}
