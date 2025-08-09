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

using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Tree;
using System.Buffers;
using ZstdSharp;

namespace FlowtideDotNet.Core.ColumnStore.Serialization
{
    public class EventBatchBPlusTreeSerializer
    {
        private readonly EventBatchSerializer _batchSerializer;
        private readonly object _lock = new object();



        public EventBatchBPlusTreeSerializer()
        {
            _batchSerializer = new EventBatchSerializer();
        }

        public EventBatchDeserializeResult Deserialize(ref SequenceReader<byte> reader, IMemoryAllocator memoryAllocator)
        {
            var deserializer = new EventBatchDeserializer(memoryAllocator);
            return deserializer.DeserializeBatch(ref reader);
        }

        public void Serialize(IBufferWriter<byte> bufferWriter, EventBatchData eventBatch, int count)
        {
            lock (_lock)
            {
                _batchSerializer.SerializeEventBatch(bufferWriter, eventBatch, count);
            }
        }

        public Task CheckpointAsync(IBPlusTreeSerializerCheckpointContext context)
        {
            return Task.CompletedTask;
        }

        public Task InitializeAsync(IBPlusTreeSerializerInitializeContext context)
        {
            return Task.CompletedTask;
        }

        private class BatchCompressor : IBatchCompressor
        {
            private Compressor _compressor;

            public BatchCompressor()
            {
                _compressor = new Compressor();
            }

            public void ColumnChange(int columnIndex)
            {
            }

            public int Wrap(ReadOnlySpan<byte> input, Span<byte> output)
            {
                return _compressor.Wrap(input, output);
            }
        }

        private class BatchDecompressor : IBatchDecompressor
        {
            private Decompressor _decompressor;

            public BatchDecompressor()
            {
                _decompressor = new Decompressor();
            }

            public void ColumnChange(int columnIndex)
            {
            }

            public int Unwrap(ReadOnlySpan<byte> input, Span<byte> output)
            {
                return _decompressor.Unwrap(input, output);
            }
        }
    }
}
