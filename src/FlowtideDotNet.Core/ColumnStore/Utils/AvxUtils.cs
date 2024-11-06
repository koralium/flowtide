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
            fixed (int* pArray = source)
            {
                int i = 0;
                int length = source.Length;

                if (Avx2.IsSupported)
                {
                    int vectorSize = Vector256<int>.Count; // Size of AVX2 vector (256 bits / 32 bits per int = 8)
                    Vector256<int> valueVector = Vector256.Create(addition);

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
        /// Copies data from one array to another with addition if the conditional value is met.
        /// No avx operations are done right now as the conditional value is a byte.
        /// This complicates the avx operations as we need to do a comparison on the byte value.
        /// </summary>
        /// <param name="array"></param>
        /// <param name="conditionalValues"></param>
        /// <param name="sourceIndex"></param>
        /// <param name="destIndex"></param>
        /// <param name="length"></param>
        /// <param name="valueToAdd"></param>
        /// <param name="conditionalValue"></param>
        public static unsafe void InPlaceMemCopyConditionalAdditionScalar(Span<int> array, Span<sbyte> conditionalValues, int sourceIndex, int destIndex, int length, int valueToAdd, sbyte conditionalValue)
        {
            unsafe
            {
                fixed (int* pArray = array)
                fixed(sbyte* pCond = conditionalValues)
                {
                    // Check if there is overlap
                    if (sourceIndex < destIndex && sourceIndex + length > destIndex)
                    {
                        int i = length;
                        for (int j = i - 1; j >= 0; j--)
                        {
                            if (conditionalValues[sourceIndex + j] == conditionalValue)
                            {
                                array[destIndex + j] = array[sourceIndex + j] + valueToAdd;
                            }
                            else
                            {
                                array[destIndex + j] = array[sourceIndex + j];
                            }
                        }
                    }
                    else
                    {
                        int i = 0;
                        for (; i < length; i++)
                        {
                            if (conditionalValues[sourceIndex + i] == conditionalValue)
                            {
                                array[destIndex + i] = array[sourceIndex + i] + valueToAdd;
                            }
                            else
                            {
                                array[destIndex + i] = array[sourceIndex + i];
                            }
                        }
                    }
                }
            }
        }

        public static unsafe void InPlaceMemCopyConditionalAddition(Span<int> array, Span<sbyte> conditionalValues, int sourceIndex, int destIndex, int length, int valueToAdd, sbyte conditionalValue)
        {
            if (!Avx2.IsSupported)
            {
                // Fallback to scalar implementation if AVX2 is not supported
                InPlaceMemCopyConditionalAdditionScalar(array, conditionalValues, sourceIndex, destIndex, length, valueToAdd, conditionalValue);
                return;
            }

            fixed (int* pArray = array)
            fixed (sbyte* pCond = conditionalValues)
            {
                int i = 0;
                int vecLength = Vector256<int>.Count; // Number of elements per AVX2 vector (8 for int)

                Vector256<int> valueToAddVec = Vector256.Create(valueToAdd);
                Vector128<sbyte> conditionalValueVec = Vector128.Create(conditionalValue);

                // Check if there is overlap
                if (sourceIndex < destIndex && sourceIndex + length > destIndex)
                {
                    // Process from end to start (backward) to handle overlap
                    for (int j = length - vecLength; j >= 0; j -= vecLength)
                    {
                        // Load 8 ints from source array and 8 conditionals
                        Vector256<int> srcVec = Avx2.LoadVector256(pArray + sourceIndex + j);
                        Vector128<sbyte> condVec = Avx2.LoadVector128(pCond + sourceIndex + j);

                        // Compare the conditionals with the target conditional value
                        Vector128<sbyte> cmpResult = Avx2.CompareEqual(condVec, conditionalValueVec);

                        var newVec = Avx2.ConvertToVector256Int32(cmpResult);

                        // Mask the source vector to conditionally add valueToAdd
                        Vector256<int> addedVec = Avx2.Add(srcVec, Avx2.And(newVec, valueToAddVec));

                        // Store the result
                        Avx2.Store(pArray + destIndex + j, addedVec);
                    }

                    // Process remaining elements (less than vecLength)
                    for (int j = (length % vecLength) - 1; j >= 0; j--)
                    {
                        if (pCond[sourceIndex + j] == conditionalValue)
                        {
                            pArray[destIndex + j] = pArray[sourceIndex + j] + valueToAdd;
                        }
                        else
                        {
                            pArray[destIndex + j] = pArray[sourceIndex + j];
                        }
                    }
                }
                else
                {
                    // Process from start to end (forward) for non-overlap case
                    for (i = 0; i <= length - vecLength; i += vecLength)
                    {
                        // Load 8 ints from source array and 8 conditionals
                        Vector256<int> srcVec = Avx2.LoadVector256(pArray + sourceIndex + i);
                        Vector128<sbyte> condVec = Avx2.LoadVector128(pCond + sourceIndex + i);

                        // Compare the conditionals with the target conditional value
                        Vector128<sbyte> cmpResult = Avx2.CompareEqual(condVec, conditionalValueVec);

                        var newVec = Avx2.ConvertToVector256Int32(cmpResult);

                        // Use the mask to conditionally add valueToAddVec
                        Vector256<int> addedVec = Avx2.Add(srcVec, Avx2.And(newVec, valueToAddVec));


                        // Store the result
                        Avx2.Store(pArray + destIndex + i, addedVec);
                    }

                    // Process remaining elements (less than vecLength)
                    for (; i < length; i++)
                    {
                        if (pCond[sourceIndex + i] == conditionalValue)
                        {
                            pArray[destIndex + i] = pArray[sourceIndex + i] + valueToAdd;
                        }
                        else
                        {
                            pArray[destIndex + i] = pArray[sourceIndex + i];
                        }
                    }
                }
            }
        }

        public static unsafe void InPlaceMemCopyAdditionByTypeScalar(Span<int> array, Span<sbyte> typeIds, int sourceIndex, int destIndex, int length, Span<int> toAdd)
        {
            unsafe
            {
                fixed (int* pArray = array)
                fixed (sbyte* pCond = typeIds)
                {
                    // Check if there is overlap
                    if (sourceIndex < destIndex && sourceIndex + length > destIndex)
                    {
                        int i = length;
                        for (int j = i - 1; j >= 0; j--)
                        {
                            var typeId = typeIds[sourceIndex + j];
                            array[destIndex + j] = array[sourceIndex + j] + toAdd[typeId];
                        }
                    }
                    else
                    {
                        int i = 0;
                        for (; i < length; i++)
                        {
                            var typeId = typeIds[sourceIndex + i];
                            array[destIndex + i] = array[sourceIndex + i] + toAdd[typeId];
                        }
                    }
                }
            }
        }

        public static unsafe void MemCopyAdditionByTypeScalar(
            Span<int> source,
            Span<int> destination,
            Span<sbyte> typeIds,
            int sourceIndex,
            int destIndex,
            int length,
            Span<int> toAdd)
        {
            int i = 0;
            for (; i < length; i++)
            {
                var typeId = typeIds[sourceIndex + i];
                destination[destIndex + i] = source[sourceIndex + i] + toAdd[typeId];
            }
        }

        public static unsafe void MemCopyAdditionByType(
            Span<int> source, 
            Span<int> destination,
            Span<sbyte> typeIds,
            int sourceIndex,
            int destIndex,
            int length,
            Span<int> toAdd,
            int numberOfTypes)
        {
            if (!Avx2.IsSupported || numberOfTypes > 8)
            {
                MemCopyAdditionByTypeScalar(source, destination, typeIds, sourceIndex, destIndex, length, toAdd);
                return;
            }

            fixed (int* pSource = source)
            fixed (int* pDest = destination)
            fixed (sbyte* pTypeIds = typeIds)
            fixed (int* pToAdd = toAdd)
            {
                int vecLength = Vector256<int>.Count;
                Vector256<int> valueToAddVec = Vector256.Load(pToAdd);

                int i;
                for (i = 0; i <= length - vecLength; i += vecLength)
                {
                    // Load 8 ints from source array and 8 typeids
                    Vector256<int> srcVec = Avx2.LoadVector256(pSource + sourceIndex + i);
                    Vector128<sbyte> typeIdVec = Avx2.LoadVector128(pTypeIds + sourceIndex + i);

                    var typeIdVecInt = Avx2.ConvertToVector256Int32(typeIdVec);
                    var additions = Avx2.PermuteVar8x32(valueToAddVec, typeIdVecInt);

                    // Use the mask to conditionally add valueToAddVec
                    Vector256<int> addedVec = Avx2.Add(srcVec, additions);

                    // Store the result
                    Avx2.Store(pDest + destIndex + i, addedVec);
                }

                for (; i < length; i++)
                {
                    var typeId = typeIds[sourceIndex + i];
                    pDest[destIndex + i] = pSource[sourceIndex + i] + toAdd[typeId];
                }
            }
        }

        public static unsafe void InPlaceMemCopyAdditionByType(
            Span<int> array, 
            Span<sbyte> typeIds, 
            int sourceIndex, 
            int destIndex, 
            int length, 
            Span<int> toAdd,
            int numberOfTypes)
        {
            if (!Avx2.IsSupported || numberOfTypes > 8)
            {
                // Fallback to scalar implementation if AVX2 is not supported
                // or if the number of types is greater than 8.
                InPlaceMemCopyAdditionByTypeScalar(array, typeIds, sourceIndex, destIndex, length, toAdd);
                return;
            }
            // Avx implementation if the number of types are less than 8.
            // This allows using PermuteVar8x32 to get the correct additions.
            fixed (int* pArray = array)
            fixed (sbyte* pTypeIds = typeIds)
            fixed (int* pToAdd = toAdd)
            {
                int vecLength = Vector256<int>.Count;
                Vector256<int> valueToAddVec = Vector256.Load(pToAdd);

                if (sourceIndex < destIndex && sourceIndex + length > destIndex)
                {
                    // Process from end to start (backward) to handle overlap
                    for (int j = length - vecLength; j >= 0; j -= vecLength)
                    {
                        // Load 8 ints from source array and 8 typeids
                        Vector256<int> srcVec = Avx2.LoadVector256(pArray + sourceIndex + j);
                        Vector128<sbyte> typeIdVec = Avx2.LoadVector128(pTypeIds + sourceIndex + j);

                        var typeIdVecInt = Avx2.ConvertToVector256Int32(typeIdVec);
                        var additions = Avx2.PermuteVar8x32(valueToAddVec, typeIdVecInt);

                        // Mask the source vector to conditionally add valueToAdd
                        Vector256<int> addedVec = Avx2.Add(srcVec, additions);

                        // Store the result
                        Avx2.Store(pArray + destIndex + j, addedVec);
                    }

                    for (int j = (length % vecLength) - 1; j >= 0; j--)
                    {
                        var typeId = pTypeIds[sourceIndex + j];
                        pArray[destIndex + j] = pArray[sourceIndex + j] + pToAdd[typeId];
                    }
                }
                else
                {
                    int i;
                    for (i = 0; i <= length - vecLength; i += vecLength)
                    {
                        // Load 8 ints from source array and 8 typeids
                        Vector256<int> srcVec = Avx2.LoadVector256(pArray + sourceIndex + i);
                        Vector128<sbyte> typeIdVec = Avx2.LoadVector128(pTypeIds + sourceIndex + i);

                        var typeIdVecInt = Avx2.ConvertToVector256Int32(typeIdVec);
                        var additions = Avx2.PermuteVar8x32(valueToAddVec, typeIdVecInt);

                        // Use the mask to conditionally add valueToAddVec
                        Vector256<int> addedVec = Avx2.Add(srcVec, additions);

                        // Store the result
                        Avx2.Store(pArray + destIndex + i, addedVec);
                    }

                    for (; i < length; i++)
                    {
                        var typeId = typeIds[sourceIndex + i];
                        array[destIndex + i] = array[sourceIndex + i] + toAdd[typeId];
                    }
                }
            }
        }

        public static unsafe int FindFirstOccurence(Span<sbyte> array, int startIndex, sbyte valueToFind)
        {
            unsafe
            {
                fixed (sbyte* pArray = array)
                {
                    int i = startIndex;
                    if (Avx2.IsSupported)
                    {
                        int vectorSize = Vector256<sbyte>.Count;
                        Vector256<sbyte> targetVec = Vector256.Create(valueToFind);

                        // Process chunks of 8 integers at a time
                        for (; i <= array.Length - vectorSize; i += vectorSize)
                        {
                            // Load 8 integers from the array into a Vector256<int>
                            Vector256<sbyte> arrayVec = Avx.LoadVector256(pArray + i);

                            // Compare arrayVec with targetVec, resulting in a mask
                            Vector256<sbyte> mask = Avx2.CompareEqual(arrayVec, targetVec);

                            // Move the mask to an integer (each 32-bit element in the vector will have 0xFFFFFFFF for a match, 0 otherwise)
                            int maskBits = Avx2.MoveMask(mask);

                            // Check if any match occurred (MoveMask returns a non-zero value if a match is found)
                            if (maskBits != 0)
                            {
                                // Determine the first matching element by inspecting the mask bits
                                for (int j = 0; j < vectorSize; j++)
                                {
                                    if (mask.GetElement(j) == -1) // 0xFFFFFFFF indicates a match
                                    {
                                        return i + j; // Return the index of the first match
                                    }
                                }
                            }
                        }
                    }

                    // Process the remaining elements in the array that don't fit into a full 256-bit register
                    for (; i < array.Length; i++)
                    {
                        if (array[i] == valueToFind)
                        {
                            return i;
                        }
                    }

                    // If no match is found, return -1
                    return -1;
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
