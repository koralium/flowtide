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
using System.Buffers;
using System.Buffers.Binary;
using System.Runtime.InteropServices;
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
        private readonly int _compressionLevel;
        private SortedList<IntPtr, int> _allocatedMemory;
        private GCHandle _handle;
        private readonly object _lock = new object();
        private bool _isInitialized;

        public FlowtideZstdCompressor(IMemoryAllocator memoryAllocator, int compressionLevel)
        {
            _memoryAllocator = memoryAllocator;
            this._compressionLevel = compressionLevel;
            _allocatedMemory = new SortedList<nint, int>();
            _handle = GCHandle.Alloc(this);
            _isInitialized = false;
        }

        private void SetParameter(ZSTD_cParameter parameter, int value)
        {
            Methods.ZSTD_CCtx_setParameter(_cctx, parameter, value).EnsureZstdSuccess();
        }

        private void CreateContexts()
        {
            if (_isInitialized)
            {
                return;
            }
            delegate* managed<void*, nuint, void*> customAlloc = &CustomAlloc;
            delegate* managed<void*, void*, void> customFree = &CustomFree;

            var customMem = new ZSTD_customMem()
            {
                customAlloc = customAlloc,
                customFree = customFree,
                opaque = (void*)GCHandle.ToIntPtr(_handle)
            };
            _dctx = Methods.ZSTD_createDCtx_advanced(customMem);
            _cctx = Methods.ZSTD_createCCtx_advanced(customMem);

            SetParameter(ZSTD_cParameter.ZSTD_c_compressionLevel, _compressionLevel);
            _isInitialized = true;
        }

        /// <summary>
        /// Used to remove all allocations, is used both on dispose and on cleanup to help reduce fragmentation
        /// when the stream is on low load.
        /// </summary>
        public void ResetContexts()
        {
            if (_isInitialized)
            {
                Methods.ZSTD_freeDCtx(_dctx);
                Methods.ZSTD_freeCCtx(_cctx);
            }
            _isInitialized = false;
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
            CreateContexts();
            fixed (byte* srcPtr = src)
            fixed (byte* destPtr = dest)
            {
                return (int)Methods.ZSTD_compress2(_cctx, destPtr, (nuint)dest.Length, srcPtr, (nuint)src.Length)
                    .EnsureZstdSuccess();
            }
        }

        public int Unwrap(ReadOnlySpan<byte> src, Span<byte> dest)
        {
            CreateContexts();
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
                ResetContexts();
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

        public void ClearTemporaryAllocations()
        {
            lock (_readLock)
            {
                lock (_writeLock)
                {
                    _compressor.ResetContexts();
                    // Create a new empty buffer writer with an empty size
                    _bufferWriter = new ArrayBufferWriter<byte>();
                }
            }

            _serializer.ClearTemporaryAllocations();
        }

        public TValue Deserialize(ReadOnlySequence<byte> bytes, int length)
        {
            lock (_readLock)
            {
                var reader = new SequenceReader<byte>(bytes);

                if (!reader.TryReadLittleEndian(out int writtenLength))
                {
                    throw new Exception("Could not read written length");
                }
                if (!reader.TryReadLittleEndian(out int originalLength))
                {
                    throw new Exception("Could not read original length");
                }

                var temporaryDestination = ArrayPool<byte>.Shared.Rent(originalLength);

                IMemoryOwner<byte>? rentedMemory = default;
                ReadOnlySpan<byte> data;

                if (reader.CurrentSpan.Length < writtenLength)
                {
                    // If the span is too small, rent memory and copy
                    rentedMemory = MemoryPool<byte>.Shared.Rent(writtenLength);
                    reader.TryCopyTo(rentedMemory.Memory.Span.Slice(0, writtenLength));
                    data = rentedMemory.Memory.Span.Slice(0, writtenLength);
                }
                else
                {
                    data = reader.CurrentSpan.Slice(reader.CurrentSpanIndex, writtenLength);
                }
                
                _compressor.Unwrap(data, temporaryDestination);
                var result = _serializer.Deserialize(new ReadOnlySequence<byte>(temporaryDestination.AsMemory().Slice(0, originalLength)), originalLength);
                ArrayPool<byte>.Shared.Return(temporaryDestination);

                if (rentedMemory != null)
                {
                    rentedMemory.Dispose();
                }
                reader.Advance(writtenLength);

                return result;
            }
        }

        public ICacheObject DeserializeCacheObject(ReadOnlySequence<byte> bytes, int length)
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
