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
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Storage.DataStructures
{
    internal static class Utils
    {
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

        public static int UnsignedBinarySearch(ushort[] array, int begin, int end, ushort k)
        {
            return Array.BinarySearch(array, begin, end - begin, k);
        }

        public static void FillArray(long[] bitmap, ushort[] array)
        {
            int pos = 0;
            int b = 0;
            for (int k = 0; k < bitmap.Length; ++k)
            {
                long bitset = bitmap[k];
                while (bitset != 0)
                {
                    array[pos++] = (char)(b + BitOperations.TrailingZeroCount(bitset));
                    bitset &= (bitset - 1);
                }
                b += 64;
            }
        }
    }
}
