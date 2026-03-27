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
using FlowtideDotNet.Storage.StateManager.Internal;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Linq;
using System.Reflection.Metadata;
using System.Text;
using System.Threading.Tasks;
using ZstdSharp;
using ZstdSharp.Unsafe;

namespace FlowtideDotNet.Storage.Persistence.Reservoir.Internal
{
    internal class ZstdCompression : IDisposable
    {
        internal struct CompressionResult
        {
            public IMemoryOwner<byte> memoryOwner;
            public int writtenLength;
        }

        private ZSTD_outBuffer_s output;
        FlowtideZstdCompressor _compressor;
        private IMemoryOwner<byte>? _destinationOwner;
        private Memory<byte> _destination;
        private bool disposedValue;

        public ZstdCompression(
            IMemoryAllocator memoryAllocator, 
            int compressionLevel,
            int dataSize,
            int headerReserve)
        {
            var upperBoundCompressSize = Compressor.GetCompressBound(dataSize);
            _destinationOwner = memoryAllocator.Allocate(upperBoundCompressSize + headerReserve, 64);
            _destination = _destinationOwner.Memory.Slice(headerReserve);
            _compressor = new FlowtideZstdCompressor(memoryAllocator, compressionLevel);
            output = new ZSTD_outBuffer_s { pos = 0, size = (nuint)_destination.Length };
        }

        internal unsafe nuint CompressStream(
            ref ZSTD_inBuffer_s input,
            ReadOnlySpan<byte> inputBuffer,
            Span<byte> outputBuffer,
            ZSTD_EndDirective directive)
        {
            fixed (byte* inputBufferPtr = inputBuffer)
            fixed (byte* outputBufferPtr = outputBuffer)
            {
                input.src = inputBufferPtr;
                output.dst = outputBufferPtr;
                return _compressor.CompressStream(ref input, ref output, directive).EnsureZstdSuccess();
            }
        }

        public void Write(ReadOnlySpan<byte> buffer)
        {
            WriteInternal(buffer, ZSTD_EndDirective.ZSTD_e_continue);
        }

        public CompressionResult Complete()
        {
            if (_destinationOwner == null)
            {
                throw new InvalidOperationException("Compression already completed");
            }
            WriteInternal(null, ZSTD_EndDirective.ZSTD_e_end);
            var memOwner = _destinationOwner;
            _destinationOwner = null;
            return new CompressionResult()
            {
                memoryOwner = memOwner,
                writtenLength = (int)output.pos
            };
        }

        private void WriteInternal(ReadOnlySpan<byte> buffer, ZSTD_EndDirective directive)
        {
            var destinationSpan = _destination.Span;
            var input = new ZSTD_inBuffer_s { pos = 0, size = (nuint)buffer.Length };
            nuint remaining;
            do
            {
                remaining = CompressStream(ref input, buffer, destinationSpan, directive);
            } while (directive == ZSTD_EndDirective.ZSTD_e_continue ? input.pos < input.size : remaining > 0);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                _compressor.Dispose();
                if (_destinationOwner != null)
                {
                    _destinationOwner.Dispose();
                }
                
                disposedValue = true;
            }
        }

        ~ZstdCompression()
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
}
