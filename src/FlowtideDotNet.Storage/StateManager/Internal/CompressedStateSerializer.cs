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

using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.Tree.Internal;
using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ZstdSharp;

namespace FlowtideDotNet.Storage.StateManager.Internal
{
    internal class CompressedStateSerializer<TValue> : IStateSerializer<TValue>
        where TValue : ICacheObject
    {
        private readonly IStateSerializer<TValue> _serializer;
        private readonly object _writeLock = new object();
        private readonly object _readLock = new object();
        private ArrayBufferWriter<byte> _bufferWriter = new ArrayBufferWriter<byte>();
        private Compressor _compressor;
        private Decompressor _decompressor;

        public CompressedStateSerializer(IStateSerializer<TValue> serializer, int compressionLevel)
        {
            _serializer = serializer;
            _compressor = new Compressor(compressionLevel);
            _decompressor = new Decompressor();
        }

        public Task CheckpointAsync<TMetadata>(IStateSerializerCheckpointWriter checkpointWriter, StateClientMetadata<TMetadata> metadata) where TMetadata : IStorageMetadata
        {
            return _serializer.CheckpointAsync(checkpointWriter, metadata);
        }

        public TValue Deserialize(ReadOnlyMemory<byte> bytes, int length)
        {
            lock (_readLock)
            {
                var span = bytes.Span;

                var writtenLength = BinaryPrimitives.ReadInt32LittleEndian(span);
                var originalLength = BinaryPrimitives.ReadInt32LittleEndian(span.Slice(4));

                var temporaryDestination = ArrayPool<byte>.Shared.Rent(originalLength);

                _decompressor.Unwrap(span.Slice(8, writtenLength), temporaryDestination);
                var result = _serializer.Deserialize(temporaryDestination.AsMemory().Slice(0, originalLength), originalLength);
                ArrayPool<byte>.Shared.Return(temporaryDestination);
                return result;
            }
        }

        public ICacheObject DeserializeCacheObject(ReadOnlyMemory<byte> bytes, int length)
        {
            return Deserialize(bytes, length);
        }

        public Task InitializeAsync<TMetadata>(IStateSerializerInitializeReader reader, StateClientMetadata<TMetadata> metadata) where TMetadata : IStorageMetadata
        {
            return _serializer.InitializeAsync(reader, metadata);
        }

        public void Serialize(in IBufferWriter<byte> bufferWriter, in TValue value)
        {
            lock (_writeLock)
            {
                _bufferWriter.ResetWrittenCount();
                _serializer.Serialize(_bufferWriter, value);
                var span = _bufferWriter.WrittenSpan;
                var compressBound = Compressor.GetCompressBound(span.Length + 8);
                var destinationSpan = bufferWriter.GetSpan(compressBound);
                var writtenLength = _compressor.Wrap(span, destinationSpan.Slice(8));
                BinaryPrimitives.WriteInt32LittleEndian(destinationSpan, writtenLength);
                BinaryPrimitives.WriteInt32LittleEndian(destinationSpan.Slice(4), span.Length);
                bufferWriter.Advance(writtenLength + 8);
            }
        }

        public void Serialize(in IBufferWriter<byte> bufferWriter, in ICacheObject value)
        {
            if (value is TValue node)
            {
                Serialize(bufferWriter, node);
                return;
            }
            throw new NotImplementedException();
        }
    }
}
