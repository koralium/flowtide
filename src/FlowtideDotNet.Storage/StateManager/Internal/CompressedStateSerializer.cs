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
using System.Reflection.Metadata;
using System.Runtime.InteropServices;
using ZstdSharp;
using ZstdSharp.Unsafe;

namespace FlowtideDotNet.Storage.StateManager.Internal
{
    // note: alias inside the namespace, the FlowtideDotNet.MiMalloc NAMESPACE otherwise
    // shadows the type name when resolving from FlowtideDotNet.* namespaces
    using MiMalloc = FlowtideDotNet.MiMalloc.MiMalloc;

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
        private GCHandle _handle;
        // Compression and decompression run under separate locks, so context creation needs
        // its own. Volatile publishes the two contexts before the flag that guards them.
        private volatile bool _isInitialized;
        private readonly object _contextsLock = new object();

        // zstd's free callback is handed only a pointer, so the size has to come from somewhere.
        // mimalloc can report it from the pointer alone; the NativeMemory fallback cannot, so on that
        // platform we have to remember it. Null whenever the allocator can answer for itself, which
        // keeps the 64-bit path free of both the dictionary and the lock.
        private readonly Dictionary<nint, int>? _allocatedSizes;
        private readonly object _sizesLock = new object();

        public FlowtideZstdCompressor(IMemoryAllocator memoryAllocator, int compressionLevel)
        {
            _memoryAllocator = memoryAllocator;
            this._compressionLevel = compressionLevel;
            _handle = GCHandle.Alloc(this);
            _isInitialized = false;
            _allocatedSizes = FlowtideMemoryAllocation.CanQueryAllocationSize ? null : new Dictionary<nint, int>();
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
            lock (_contextsLock)
            {
                if (_isInitialized)
                {
                    return;
                }
                CreateContexts_Locked();
            }
        }

        private void CreateContexts_Locked()
        {
            delegate* managed<void*, nuint, void*> customAlloc = &CustomAlloc;
            delegate* managed<void*, void*, void> customFree = &CustomFree;

            var customMem = new ZSTD_customMem()
            {
                customAlloc = customAlloc,
                customFree = customFree,
                // RS0042 tracks GCHandle but ToIntPtr takes it by value.
#pragma warning disable RS0042
                opaque = (void*)GCHandle.ToIntPtr(_handle)
#pragma warning restore RS0042
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
            lock (_contextsLock)
            {
                if (_isInitialized)
                {
                    Methods.ZSTD_freeDCtx(_dctx);
                    Methods.ZSTD_freeCCtx(_cctx);
                }
                _isInitialized = false;
            }
        }


        // zstd's workspaces go through the same allocator as everything else. Using NativeMemory here
        // instead would put them on the C runtime heap while still reporting them to the metrics, which
        // makes the reported figure impossible to reconcile with what the allocator actually holds --
        // the workspaces are ~0.6 MiB per state client, so that gap is not small.
        private const int ZstdAllocAlignment = 16;

        private static void* CustomAlloc(void* opaque, nuint size)
        {
            GCHandle handle = GCHandle.FromIntPtr((IntPtr)opaque);
            var instance = (FlowtideZstdCompressor)handle.Target!;
            var allocated = FlowtideMemoryAllocation.AllocateAligned((int)size, ZstdAllocAlignment);

            // register whatever the free path will be able to report, so the two can never drift apart
            int registered = FlowtideMemoryAllocation.CanQueryAllocationSize
                ? (int)FlowtideMemoryAllocation.GetAllocationSize(allocated.ptr)
                : allocated.length;

            if (instance._allocatedSizes != null)
            {
                lock (instance._sizesLock)
                {
                    instance._allocatedSizes[(nint)allocated.ptr] = registered;
                }
            }
            instance._memoryAllocator.RegisterAllocationToMetrics(registered);
            return allocated.ptr;
        }

        private static void CustomFree(void* opaque, void* ptr)
        {
            if (ptr == null)
            {
                return;
            }
            GCHandle handle = GCHandle.FromIntPtr((IntPtr)opaque);
            var instance = (FlowtideZstdCompressor)handle.Target!;

            int size;
            if (instance._allocatedSizes != null)
            {
                lock (instance._sizesLock)
                {
                    // a pointer we never handed out is not ours to account for
                    if (!instance._allocatedSizes.Remove((nint)ptr, out size))
                    {
                        size = 0;
                    }
                }
            }
            else
            {
                // must be read while the block is still ours
                size = (int)FlowtideMemoryAllocation.GetAllocationSize(ptr);
            }

            if (size > 0)
            {
                instance._memoryAllocator.RegisterFreeToMetrics(size);
            }
            FlowtideMemoryAllocation.FreeAligned(ptr, ZstdAllocAlignment);
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

        internal unsafe nuint CompressStream(ref ZSTD_inBuffer_s input, ref ZSTD_outBuffer_s output, ZSTD_EndDirective directive)
        {
            CreateContexts();
            fixed (ZSTD_inBuffer_s* inputPtr = &input)
            fixed (ZSTD_outBuffer_s* outputPtr = &output)
            {
                return Methods.ZSTD_compressStream2(_cctx, outputPtr, inputPtr, directive).EnsureZstdSuccess();
            }
        }

        internal nuint DecompressStream(ref ZSTD_inBuffer_s input, ref ZSTD_outBuffer_s output)
        {
            CreateContexts();
            fixed (ZSTD_inBuffer_s* inputPtr = &input)
            fixed (ZSTD_outBuffer_s* outputPtr = &output)
            {
                return Methods.ZSTD_decompressStream(_dctx, outputPtr, inputPtr).EnsureZstdSuccess();
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

        public async Task CheckpointAsync<TMetadata>(IStateSerializerCheckpointWriter checkpointWriter, StateClientMetadata<TMetadata> metadata) where TMetadata : IStorageMetadata
        {
            await _serializer.CheckpointAsync(checkpointWriter, metadata);
            ClearTemporaryAllocations();
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

                if ((reader.CurrentSpan.Length - reader.CurrentSpanIndex) < writtenLength)
                {
                    // If the span is too small, rent memory and copy
                    rentedMemory = MemoryPool<byte>.Shared.Rent(writtenLength);
                    if (!reader.TryCopyTo(rentedMemory.Memory.Span.Slice(0, writtenLength)))
                    {
                        throw new Exception("Failed to copy data for decompression");
                    }
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
