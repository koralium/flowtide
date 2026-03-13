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
using System.Text;
using System.Threading.Tasks;
using ZstdSharp;
using ZstdSharp.Unsafe;

namespace FlowtideDotNet.Storage.Persistence.Reservoir.Internal
{
    internal class ZstdDecompression
    {
        FlowtideZstdCompressor _compressor;
        private bool contextDrained = true;
        private nuint lastDecompressResult = 0;

        private ZSTD_inBuffer_s input;
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
            // Guard against infinite loop (output.pos would never become non-zero)
            if (outputData.Length == 0)
            {
                return 0;
            }

            SequencePosition segPos = default;
            ReadOnlyMemory<byte> inputBuffer = default; 

            var output = new ZSTD_outBuffer_s { pos = 0, size = (nuint)outputData.Length };
            while (true)
            {
                // If there is still input available, or there might be data buffered in the decompressor context, flush that out
                while (input.pos < input.size || !contextDrained)
                {
                    nuint oldInputPos = input.pos;
                    nuint result = DecompressStream(ref output, inputBuffer.Span, outputData);
                    if (output.pos > 0 || oldInputPos != input.pos)
                    {
                        // Keep result from last decompress call that made some progress, so we known if we're at end of frame
                        lastDecompressResult = result;
                    }
                    // If decompression filled the output buffer, there might still be data buffered in the decompressor context
                    contextDrained = output.pos < output.size;
                    // If we have data to return, return it immediately, so we won't stall on Read
                    if (output.pos > 0)
                    {
                        return (int)output.pos;
                    }
                }

                if (data.TryGet(ref segPos, out var mem))
                {
                    inputBuffer = mem;
                }
                else
                {
                    return 0;
                }

                input.size = (nuint)inputBuffer.Length;
                input.pos = 0;
            }
        }
    }
}
