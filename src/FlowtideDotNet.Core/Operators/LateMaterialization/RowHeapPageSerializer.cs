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
        private const byte FormatVersion = 1;
        private const int HeaderSize = 5;

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
            var header = bufferWriter.GetSpan(HeaderSize);
            header[0] = FormatVersion;
            BinaryPrimitives.WriteInt32LittleEndian(header.Slice(1), value.Count);
            bufferWriter.Advance(HeaderSize);
            _serializer.SerializeEventBatch(bufferWriter, value.Data, value.Count);
        }

        public void Serialize(in IBufferWriter<byte> bufferWriter, in ICacheObject value)
        {
            Serialize(bufferWriter, (RowHeapPage)value);
        }

        public RowHeapPage Deserialize(ReadOnlySequence<byte> bytes, int length)
        {
            var reader = new SequenceReader<byte>(bytes);
            if (!reader.TryRead(out byte version) || !reader.TryReadLittleEndian(out int count))
            {
                throw new InvalidOperationException("A row heap page is shorter than its header.");
            }
            if (version != FormatVersion)
            {
                throw new InvalidOperationException($"Row heap page format {version} is not supported, expected {FormatVersion}.");
            }
            var deserializer = new EventBatchDeserializer(_memoryAllocator);
            var result = deserializer.DeserializeBatch(ref reader);
            if (result.Count != count || result.EventBatch.Columns.Count != _columnCount)
            {
                int columns = result.EventBatch.Columns.Count;
                result.EventBatch.Dispose();
                throw new InvalidOperationException($"A row heap page holds {result.Count} rows in {columns} columns, expected {count} rows in {_columnCount} columns.");
            }
            return new RowHeapPage(result.EventBatch, count);
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
