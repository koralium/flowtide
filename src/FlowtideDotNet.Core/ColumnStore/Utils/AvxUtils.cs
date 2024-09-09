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

using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Intrinsics.X86;
using System.Runtime.Intrinsics;
using System.Text;
using System.Threading.Tasks;
using System.Runtime.InteropServices;

namespace FlowtideDotNet.Core.ColumnStore.Utils
{
    internal static class AvxUtils
    {

        public unsafe static void AddValueToElements(Span<int> source, int addition)
        {
            int vectorSize = Vector256<int>.Count; // Size of AVX2 vector (256 bits / 32 bits per int = 8)
            Vector256<int> valueVector = Vector256.Create(addition);

            fixed (int* pArray = source)
            {
                int i = 0;
                int length = source.Length;

                if (Avx2.IsSupported)
                {
                    if ((long)pArray % 32 == 0)
                    {
                        for (; i <= source.Length - vectorSize; i += vectorSize)
                        {
                            Vector256<int> vector = Avx.LoadAlignedVector256(pArray + i);
                            vector = Avx2.Add(vector, valueVector);
                            Avx.Store(pArray + i, vector);
                        }
                    }
                    else
                    {
                        // Non aligned
                        for (; i <= source.Length - vectorSize; i += vectorSize)
                        {
                            Vector256<int> vector = Avx.LoadVector256(pArray + i);
                            vector = Avx2.Add(vector, valueVector);
                            Avx.Store(pArray + i, vector);
                        }
                    }
                }

                // Process remaining elements
                for (; i < length; i++)
                {
                    source[i] += addition;
                }
            }
        }

        /// <summary>
        /// Copy data with addition on the same array
        /// </summary>
        /// <param name="array"></param>
        /// <param name="sourceIndex"></param>
        /// <param name="destIndex"></param>
        /// <param name="length"></param>
        /// <param name="valueToAdd"></param>
        public static unsafe void InPlaceMemCopyWithAddition(Span<int> array, int sourceIndex, int destIndex, int length, int valueToAdd)
        {
            unsafe
            {
                fixed (int* pArray = array)
                {
                    // Check if there is overlap
                    if (sourceIndex < destIndex && sourceIndex + length > destIndex)
                    {
                        int i = length;
                        if (Avx2.IsSupported)
                        {
                            int vectorSize = Vector256<int>.Count; // Size of AVX2 vector (256 bits / 32 bits per int = 8)
                            Vector256<int> valueVector = Vector256.Create(valueToAdd);
                            while (i >= vectorSize)
                            {
                                i -= vectorSize;
                                Vector256<int> srcVector = Avx.LoadVector256(pArray + sourceIndex + i);
                                Vector256<int> resultVector = Avx2.Add(srcVector, valueVector);
                                Avx.Store(pArray + destIndex + i, resultVector);
                            }
                        }
                        for (int j = i - 1; j >= 0; j--)
                        {
                            array[destIndex + j] = array[sourceIndex + j] + valueToAdd;
                        }
                    }
                    else
                    {
                        int i = 0;
                        // No overlap
                        if (Avx2.IsSupported)
                        {
                            int vectorSize = Vector256<int>.Count; // Size of AVX2 vector (256 bits / 32 bits per int = 8)
                            Vector256<int> valueVector = Vector256.Create(valueToAdd);
                            while (i <= length - vectorSize)
                            {
                                Vector256<int> srcVector = Avx.LoadVector256(pArray + sourceIndex + i);
                                Vector256<int> resultVector = Avx2.Add(srcVector, valueVector);
                                Avx.Store(pArray + destIndex + i, resultVector);
                                i += vectorSize;
                            }
                        }

                        // Handle remaining elements
                        for (; i < length; i++)
                        {
                            array[destIndex + i] = array[sourceIndex + i] + valueToAdd;
                        }
                    }
                }
            }
        }

        public static void MemCpyWithAdd(Span<int> source, Span<int> destination, int addition)
        {
            unsafe
            {
                fixed (int* pSrc = source)
                fixed (int* pDst = destination)
                {
                    int i = 0;

                    // Check if we can use AVX512 this will allow moving 16 integers at a time while doing addition
                    // We only use avx512 if the data is aligned, https://stackoverflow.com/a/74787779
                    if (Avx512F.IsSupported && ((long)pSrc % 64 == 0) && ((long)pDst % 64 == 0))
                    {
                        int vectorSize = Vector512<int>.Count;
                        var vec512 = Vector512.Create(addition);
                        
                        for (; i <= source.Length - vectorSize; i += vectorSize)
                        {
                            Vector512<int> sourceVector = Avx512F.LoadAlignedVector512(pSrc + i);
                            Vector512<int> resultVector = Avx512F.Add(sourceVector, vec512);
                            Avx512F.Store(pDst + i, resultVector);
                        }
                    }
                    // Check if the pc has 256 bit support, will move 8 integers at a time.
                    else if (Avx2.IsSupported)
                    {
                        int vectorSize = Vector256<int>.Count;
                        var vec256 = Vector256.Create(addition);

                        // Check that we are aligned
                        if (((long)pSrc % 32 == 0) && ((long)pDst % 32 == 0))
                        {
                            for (; i <= source.Length - vectorSize; i += vectorSize)
                            {
                                Vector256<int> sourceVector = Avx.LoadAlignedVector256(pSrc + i);
                                Vector256<int> resultVector = Avx2.Add(sourceVector, vec256);
                                Avx.StoreAligned(pDst + i, resultVector);
                            }
                        }
                        else
                        {
                            // Non aligned
                            for (; i <= source.Length - vectorSize; i += vectorSize)
                            {
                                Vector256<int> sourceVector = Avx.LoadVector256(pSrc + i);
                                Vector256<int> resultVector = Avx2.Add(sourceVector, vec256);
                                Avx.Store(pDst + i, resultVector);
                            }
                        }
                    }
                    // Check if SSE is enabled on the pc, will move 4 integers at a time.
                    else if (Sse.IsSupported)
                    {
                        int vectorSize = Vector128<int>.Count;
                        var additionVector = Vector128.Create(addition);

                        // Check alignment
                        if (((long)pSrc % 16 == 0) && ((long)pDst % 16 == 0))
                        {
                            for (; i <= source.Length - vectorSize; i += vectorSize)
                            {
                                Vector128<int> sourceVector = Sse2.LoadAlignedVector128(pSrc + i);
                                Vector128<int> resultVector = Sse2.Add(sourceVector, additionVector);
                                Sse2.StoreAligned(pDst + i, resultVector);
                            }
                        }
                        else
                        {
                            for (; i <= source.Length - vectorSize; i += vectorSize)
                            {
                                Vector128<int> sourceVector = Sse2.LoadVector128(pSrc + i);
                                Vector128<int> resultVector = Sse2.Add(sourceVector, additionVector);
                                Sse2.Store(pDst + i, resultVector);
                            }
                        }
                    }

                    // Do remainder, or all rows if no simd operations where possible.
                    for (; i < source.Length; i++)
                    {
                        destination[i] = source[i] + addition;
                    }
                }
            }
        }
    }
}
