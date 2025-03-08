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
using System.Runtime.CompilerServices;

/// <summary>
/// Code from https://github.com/Tornhoof/RoaringBitmap/tree/master which is archived
/// </summary>
namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.DeletionVectors.RoaringBitmap
{
    /// <summary>
    ///     Pretty much everything in here are straight conversions from the original Util class in the java Roaring Bitmap
    ///     project.
    /// </summary>
    internal static class Util
    {
        /// <summary>
        ///     see https://en.wikipedia.org/wiki/Hamming_weight
        ///     Unfortunately there is no popcnt in c#
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int BitCount(ulong x)
        {
            x -= x >> 1 & 0x5555555555555555UL; //put count of each 2 bits into those 2 bits
            x = (x & 0x3333333333333333UL) + (x >> 2 & 0x3333333333333333UL); //put count of each 4 bits into those 4 bits 
            x = x + (x >> 4) & 0x0F0F0F0F0F0F0F0FUL; //put count of each 8 bits into those 8 bits 
            return (int)(x * 0x0101010101010101UL >> 56); //returns left 8 bits of x + (x<<8) + (x<<16) + (x<<24) + ... 
        }

        /// <summary>
        ///     see https://en.wikipedia.org/wiki/Hamming_weight
        ///     Unfortunately there is no popcnt in c#
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int BitCount(ulong[] xArray)
        {
            var result = 0;
            for (var i = 0; i < xArray.Length; i++)
            {
                result += BitCount(xArray[i]);
            }
            return result;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static void ArrayCopy(ushort[] input, int iStart, ushort[] output, int oStart, int length)
        {
            Buffer.BlockCopy(input, iStart * sizeof(ushort), output, oStart * sizeof(ushort), length * sizeof(ushort));
        }

        public static int UnionArrays(ushort[] set1, int length1, ushort[] set2, int length2, ushort[] buffer)
        {
            var pos = 0;
            int k1 = 0, k2 = 0;
            if (0 == length2)
            {
                ArrayCopy(set1, 0, buffer, 0, length1);
                return length1;
            }
            if (0 == length1)
            {
                ArrayCopy(set2, 0, buffer, 0, length2);
                return length2;
            }
            var s1 = set1[k1];
            var s2 = set2[k2];
            while (true)
            {
                int v1 = s1;
                int v2 = s2;
                if (v1 < v2)
                {
                    buffer[pos++] = s1;
                    ++k1;
                    if (k1 >= length1)
                    {
                        ArrayCopy(set2, k2, buffer, pos, length2 - k2);
                        return pos + length2 - k2;
                    }
                    s1 = set1[k1];
                }
                else if (v1 == v2)
                {
                    buffer[pos++] = s1;
                    ++k1;
                    ++k2;
                    if (k1 >= length1)
                    {
                        ArrayCopy(set2, k2, buffer, pos, length2 - k2);
                        return pos + length2 - k2;
                    }
                    if (k2 >= length2)
                    {
                        ArrayCopy(set1, k1, buffer, pos, length1 - k1);
                        return pos + length1 - k1;
                    }
                    s1 = set1[k1];
                    s2 = set2[k2];
                }
                else // if (set1[k1]>set2[k2])
                {
                    buffer[pos++] = s2;
                    ++k2;
                    if (k2 >= length2)
                    {
                        ArrayCopy(set1, k1, buffer, pos, length1 - k1);
                        return pos + length1 - k1;
                    }
                    s2 = set2[k2];
                }
            }
        }

        public static int DifferenceArrays(ushort[] set1, int length1, ushort[] set2, int length2, ushort[] buffer)
        {
            var pos = 0;
            int k1 = 0, k2 = 0;
            if (0 == length2)
            {
                ArrayCopy(set1, 0, buffer, 0, length1);
                return length1;
            }
            if (0 == length1)
            {
                return 0;
            }
            var s1 = set1[k1];
            var s2 = set2[k2];
            while (true)
            {
                if (s1 < s2)
                {
                    buffer[pos++] = s1;
                    ++k1;
                    if (k1 >= length1)
                    {
                        break;
                    }
                    s1 = set1[k1];
                }
                else if (s1 == s2)
                {
                    ++k1;
                    ++k2;
                    if (k1 >= length1)
                    {
                        break;
                    }
                    if (k2 >= length2)
                    {
                        ArrayCopy(set1, k1, buffer, pos, length1 - k1);
                        return pos + length1 - k1;
                    }
                    s1 = set1[k1];
                    s2 = set2[k2];
                }
                else // if (val1>val2)
                {
                    ++k2;
                    if (k2 >= length2)
                    {
                        ArrayCopy(set1, k1, buffer, pos, length1 - k1);
                        return pos + length1 - k1;
                    }
                    s2 = set2[k2];
                }
            }
            return pos;
        }

        public static int IntersectArrays(ushort[] set1, int length1, ushort[] set2, int length2, ushort[] buffer)
        {
            if (set1.Length << 6 < set2.Length)
            {
                return OneSidedGallopingIntersect2By2(set1, length1, set2, length2, buffer);
            }
            if (set2.Length << 6 < set1.Length)
            {
                return OneSidedGallopingIntersect2By2(set2, length2, set1, length1, buffer);
            }
            return LocalIntersect2By2(set1, length1, set2, length2, buffer);
        }

        private static int LocalIntersect2By2(ushort[] set1, int length1, ushort[] set2, int length2, ushort[] buffer)
        {
            if (0 == length1 || 0 == length2)
            {
                return 0;
            }
            var k1 = 0;
            var k2 = 0;
            var pos = 0;
            var s1 = set1[k1];
            var s2 = set2[k2];

            while (true)
            {
                int v1 = s1;
                int v2 = s2;
                if (v2 < v1)
                {
                    do
                    {
                        ++k2;
                        if (k2 == length2)
                        {
                            return pos;
                        }
                        s2 = set2[k2];
                        v2 = s2;
                    } while (v2 < v1);
                }
                if (v1 < v2)
                {
                    do
                    {
                        ++k1;
                        if (k1 == length1)
                        {
                            return pos;
                        }
                        s1 = set1[k1];
                        v1 = s1;
                    } while (v1 < v2);
                }
                else // (set2[k2] == set1[k1])
                {
                    buffer[pos++] = s1;
                    ++k1;
                    if (k1 == length1)
                    {
                        break;
                    }
                    ++k2;
                    if (k2 == length2)
                    {
                        break;
                    }
                    s1 = set1[k1];
                    s2 = set2[k2];
                }
            }
            return pos;
        }

        private static int OneSidedGallopingIntersect2By2(ushort[] smallSet, int smallLength, ushort[] largeSet, int largeLength, ushort[] buffer)
        {
            if (0 == smallLength)
            {
                return 0;
            }
            var k1 = 0;
            var k2 = 0;
            var pos = 0;
            var s1 = largeSet[k1];
            var s2 = smallSet[k2];
            while (true)
            {
                if (s1 < s2)
                {
                    k1 = AdvanceUntil(largeSet, k1, largeLength, s2);
                    if (k1 == largeLength)
                    {
                        break;
                    }
                    s1 = largeSet[k1];
                }
                if (s2 < s1)
                {
                    ++k2;
                    if (k2 == smallLength)
                    {
                        break;
                    }
                    s2 = smallSet[k2];
                }
                else // (set2[k2] == set1[k1])
                {
                    buffer[pos++] = s2;
                    ++k2;
                    if (k2 == smallLength)
                    {
                        break;
                    }
                    s2 = smallSet[k2];
                    k1 = AdvanceUntil(largeSet, k1, largeLength, s2);
                    if (k1 == largeLength)
                    {
                        break;
                    }
                    s1 = largeSet[k1];
                }
            }
            return pos;
        }

        /// <summary>
        ///     Find the smallest integer larger than pos such that array[pos]&gt;= min.
        ///     otherwise return length
        ///     -> The first line is BinarySearch with pos + 1, the second line is the bitwise complement if the value can't be
        ///     found
        /// </summary>
        public static int AdvanceUntil(ushort[] array, int pos, int length, ushort min)
        {
            var start = pos + 1; // check the next one
            if (start >= length || array[start] >= min) // the simple cases
            {
                return start;
            }
            var result = Array.BinarySearch(array, start, length - start, min);
            return result < 0 ? ~result : result;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ushort HighBits(int value)
        {
            return (ushort)(value >> 16);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ushort LowBits(int value)
        {
            return (ushort)(value & 0xFFFF);
        }

        public static int XorArrays(ushort[] set1, int length1, ushort[] set2, int length2, ushort[] buffer)
        {
            var pos = 0;
            int k1 = 0, k2 = 0;
            if (0 == length2)
            {
                ArrayCopy(set1, 0, buffer, 0, length1);
                return length1;
            }
            if (0 == length1)
            {
                ArrayCopy(set2, 0, buffer, 0, length2);
                return length2;
            }
            var s1 = set1[k1];
            var s2 = set2[k2];
            while (true)
            {
                if (s1 < s2)
                {
                    buffer[pos++] = s1;
                    ++k1;
                    if (k1 >= length1)
                    {
                        ArrayCopy(set2, k2, buffer, pos, length2 - k2);
                        return pos + length2 - k2;
                    }
                    s1 = set1[k1];
                }
                else if (s1 == s2)
                {
                    ++k1;
                    ++k2;
                    if (k1 >= length1)
                    {
                        ArrayCopy(set2, k2, buffer, pos, length2 - k2);
                        return pos + length2 - k2;
                    }
                    if (k2 >= length2)
                    {
                        ArrayCopy(set1, k1, buffer, pos, length1 - k1);
                        return pos + length1 - k1;
                    }
                    s1 = set1[k1];
                    s2 = set2[k2];
                }
                else // if (val1>val2)
                {
                    buffer[pos++] = s2;
                    ++k2;
                    if (k2 >= length2)
                    {
                        ArrayCopy(set1, k1, buffer, pos, length1 - k1);
                        return pos + length1 - k1;
                    }
                    s2 = set2[k2];
                }
            }
        }
    }
}