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
using System.Buffers;
using ZstdSharp.Unsafe;

namespace FlowtideDotNet.Storage.Persistence.Reservoir.Internal
{
    internal class ZstdDecompression : IDisposable
    {
        FlowtideZstdCompressor _compressor;
        private ZSTD_inBuffer_s input;
        private bool disposedValue;

        public ZstdDecompression(IMemoryAllocator memoryAllocator)
        {
            _compressor = new FlowtideZstdCompressor(memoryAllocator, 0);
            input = new ZSTD_inBuffer_s { pos = (nuint)0, size = (nuint)0 };
        }

        private unsafe nuint DecompressStream(ref ZSTD_outBuffer_s output, ReadOnlySpan<byte> inputBuffer, Span<byte> outputBuffer)
        {
            fixed (byte* inputBufferPtr = inputBuffer)
            fixed (byte* outputBufferPtr = outputBuffer)
            {
                input.src = inputBufferPtr;
                output.dst = outputBufferPtr;
                return _compressor.DecompressStream(ref input, ref output);
            }
        }


        public int Read(ReadOnlySequence<byte> data, Span<byte> outputData)
        {
            if (outputData.Length == 0)
            {
                return 0;
            }

            SequencePosition segPos = data.Start;
            ReadOnlyMemory<byte> inputBuffer = default; 

            var output = new ZSTD_outBuffer_s { pos = 0, size = (nuint)outputData.Length };
            while (true)
            {
                // If there is still input available, or there might be data buffered in the decompressor context, flush that out
                while (input.pos < input.size)
                {
                    nuint result = DecompressStream(ref output, inputBuffer.Span, outputData);
                }

                if (data.TryGet(ref segPos, out var mem))
                {
                    inputBuffer = mem;
                }
                else
                {
                    return (int)output.pos;
                }

                input.size = (nuint)inputBuffer.Length;
                input.pos = 0;
            }
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    _compressor.Dispose();
                }

                disposedValue = true;
            }
        }

        public void Dispose()
        {
            // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }
    }
}
