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

using FlowtideDotNet.Storage.FileCache;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.Tree.Internal;
using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Linq;
using System.Reflection.Metadata;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;
using ZstdSharp;
using ZstdSharp.Unsafe;

namespace FlowtideDotNet.Storage.StateManager.Internal
{
    /// <summary>
    /// Implementation of both compressor and decompressor from zstdsharp, only to use own memory allocation
    /// to get that memory to metrics correctly.
    /// </summary>
    internal unsafe class FlowtideZstdCompressor : IDisposable
    {
        private ZSTD_DCtx_s* _dctx;
        private ZSTD_CCtx_s* _cctx;
        private bool _disposedValue;
        private readonly IMemoryAllocator _memoryAllocator;
        private SortedList<IntPtr, int> _allocatedMemory;
        private GCHandle _handle;
        private readonly object _lock = new object();

        public FlowtideZstdCompressor(IMemoryAllocator memoryAllocator, int compressionLevel)
        {
            _memoryAllocator = memoryAllocator;
            _allocatedMemory = new SortedList<nint, int>();

            delegate* managed<void*, nuint, void*> customAlloc = &CustomAlloc;
            delegate* managed<void*, void*, void> customFree = &CustomFree;

            _handle = GCHandle.Alloc(this);
            
            var customMem = new ZSTD_customMem()
            {
                customAlloc = customAlloc,
                customFree = customFree,
                opaque = (void*)GCHandle.ToIntPtr(_handle)
            };
            _dctx = Methods.ZSTD_createDCtx_advanced(customMem);
            _cctx = Methods.ZSTD_createCCtx_advanced(customMem);
            SetParameter(ZSTD_cParameter.ZSTD_c_compressionLevel, compressionLevel);
        }

        private void SetParameter(ZSTD_cParameter parameter, int value)
        {
            Methods.ZSTD_CCtx_setParameter(_cctx, parameter, value).EnsureZstdSuccess();
        }


        private static void* CustomAlloc(void* opaque, nuint size)
        {
            GCHandle handle = GCHandle.FromIntPtr((IntPtr)opaque);
            var instance = (FlowtideZstdCompressor)handle.Target!;
            // Register the allocated memory to metrics
            instance._memoryAllocator.RegisterAllocationToMetrics((int)size);
            var ptr = NativeMemory.Alloc(size);
            lock (instance._lock)
            {
                instance._allocatedMemory.Add(new nint(ptr), (int)size);
            }
            return ptr;
        }

        private static void CustomFree(void* opaque, void* ptr)
        {
            GCHandle handle = GCHandle.FromIntPtr((IntPtr)opaque);
            var instance = (FlowtideZstdCompressor)handle.Target!;
            lock (instance._lock)
            {
                if (instance._allocatedMemory.TryGetValue(new nint(ptr), out var size))
                {
                    // Remove allocated memory from metrics
                    instance._memoryAllocator.RegisterFreeToMetrics(size);
                    instance._allocatedMemory.Remove(new nint(ptr));
                }
            }
            NativeMemory.Free(ptr);
        }

        public int Wrap(ReadOnlySpan<byte> src, Span<byte> dest)
        {
            fixed (byte* srcPtr = src)
            fixed (byte* destPtr = dest)
            {
                return (int)Methods.ZSTD_compress2(_cctx, destPtr, (nuint)dest.Length, srcPtr, (nuint)src.Length)
                    .EnsureZstdSuccess();
            }
        }

        public int Unwrap(ReadOnlySpan<byte> src, Span<byte> dest)
        {
            fixed (byte* srcPtr = src)
            fixed (byte* destPtr = dest)
            {
                return (int)Methods
                    .ZSTD_decompressDCtx(_dctx, destPtr, (nuint)dest.Length, srcPtr, (nuint)src.Length)
                    .EnsureZstdSuccess();
            }
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!_disposedValue)
            {
                Methods.ZSTD_freeDCtx(_dctx);
                Methods.ZSTD_freeCCtx(_cctx);
                _handle.Free();
                _disposedValue = true;
            }
        }

        ~FlowtideZstdCompressor()
        {
            // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
            Dispose(disposing: false);
        }

        public void Dispose()
        {
            // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }
    }

    internal class CompressedStateSerializer<TValue> : IStateSerializer<TValue>
        where TValue : ICacheObject
    {
        private readonly IStateSerializer<TValue> _serializer;
        private readonly object _writeLock = new object();
        private readonly object _readLock = new object();
        private ArrayBufferWriter<byte> _bufferWriter = new ArrayBufferWriter<byte>();
        private FlowtideZstdCompressor _compressor;

        public CompressedStateSerializer(IStateSerializer<TValue> serializer, int compressionLevel, IMemoryAllocator memoryAllocator)
        {
            _serializer = serializer;
            _compressor = new FlowtideZstdCompressor(memoryAllocator, compressionLevel);
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

                _compressor.Unwrap(span.Slice(8, writtenLength), temporaryDestination);
                var result = _serializer.Deserialize(temporaryDestination.AsMemory().Slice(0, originalLength), originalLength);
                ArrayPool<byte>.Shared.Return(temporaryDestination);
                return result;
            }
        }

        public ICacheObject DeserializeCacheObject(ReadOnlyMemory<byte> bytes, int length)
        {
            return Deserialize(bytes, length);
        }

        public void Dispose()
        {
            _compressor.Dispose();
            _serializer.Dispose();
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
