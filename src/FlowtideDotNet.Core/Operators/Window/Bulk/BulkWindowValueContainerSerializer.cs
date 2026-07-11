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

using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.Serialization;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Tree;
using System.Buffers;
using System.Buffers.Binary;

namespace FlowtideDotNet.Core.Operators.Window.Bulk
{
    internal class BulkWindowValueContainerSerializer : IBplusTreeValueSerializer<BulkWindowValue, BulkWindowValueContainer>
    {
        private readonly int _numberOfColumns;
        private readonly IMemoryAllocator _memoryAllocator;
        private readonly EventBatchSerializer _batchSerializer;

        public BulkWindowValueContainerSerializer(int numberOfColumns, IMemoryAllocator memoryAllocator)
        {
            _batchSerializer = new EventBatchSerializer();
            _numberOfColumns = numberOfColumns;
            _memoryAllocator = memoryAllocator;
        }

        public Task CheckpointAsync(IBPlusTreeSerializerCheckpointContext context)
        {
            return Task.CompletedTask;
        }

        public BulkWindowValueContainer CreateEmpty()
        {
            return new BulkWindowValueContainer(_numberOfColumns, _memoryAllocator);
        }

        public BulkWindowValueContainer Deserialize(ref SequenceReader<byte> reader)
        {
            if (!reader.TryReadLittleEndian(out int weightsMemoryLength))
            {
                throw new InvalidOperationException("Failed to read weights memory length");
            }
            if (!reader.TryReadLittleEndian(out int bitmapMemoryLength))
            {
                throw new InvalidOperationException("Failed to read bitmap memory length");
            }

            var weightsNativeMemory = _memoryAllocator.Allocate(weightsMemoryLength, 64);
            var slice = weightsNativeMemory.Memory.Span.Slice(0, weightsMemoryLength);
            if (!reader.TryCopyTo(slice))
            {
                throw new InvalidOperationException("Failed to read weights memory");
            }
            reader.Advance(weightsMemoryLength);

            var bitmapNativeMemory = _memoryAllocator.Allocate(bitmapMemoryLength, 64);
            if (!reader.TryCopyTo(bitmapNativeMemory.Memory.Span.Slice(0, bitmapMemoryLength)))
            {
                throw new InvalidOperationException("Failed to read bitmap memory");
            }
            reader.Advance(bitmapMemoryLength);

            var weights = new PrimitiveList<int>(weightsNativeMemory, weightsMemoryLength / sizeof(int), _memoryAllocator);
            var bitmap = new BitmapList(bitmapNativeMemory, weights.Count, _memoryAllocator);

            var deserializer = new EventBatchDeserializer(_memoryAllocator);
            var functionStatesResult = deserializer.DeserializeDataColumns(ref reader);

            ListColumn[] listColumns = new ListColumn[functionStatesResult.DataColumns.Length];
            for (int i = 0; i < functionStatesResult.DataColumns.Length; i++)
            {
                if (functionStatesResult.DataColumns[i] is ListColumn listColumn)
                {
                    listColumns[i] = listColumn;
                }
                else
                {
                    throw new InvalidOperationException("Invalid data column type");
                }
            }

            return new BulkWindowValueContainer(weights, listColumns, bitmap);
        }

        public Task InitializeAsync(IBPlusTreeSerializerInitializeContext context)
        {
            return Task.CompletedTask;
        }

        public void Serialize(in IBufferWriter<byte> writer, in BulkWindowValueContainer values)
        {
            var weightsmemory = values._weights.SlicedMemory.Span;
            var bitmapMemory = values._previousValueSent.MemorySlice.Span;

            var writeSpan = writer.GetSpan(weightsmemory.Length + bitmapMemory.Length + 8);

            BinaryPrimitives.WriteInt32LittleEndian(writeSpan, weightsmemory.Length);
            BinaryPrimitives.WriteInt32LittleEndian(writeSpan.Slice(4), bitmapMemory.Length);

            weightsmemory.CopyTo(writeSpan.Slice(8));
            bitmapMemory.CopyTo(writeSpan.Slice(8 + weightsmemory.Length));

            writer.Advance(weightsmemory.Length + bitmapMemory.Length + 8);

            _batchSerializer.SerializeDataColumns(writer, values._functionStates, values._weights.Count);
        }
    }
}
