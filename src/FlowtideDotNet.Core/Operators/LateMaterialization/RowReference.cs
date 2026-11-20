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

namespace FlowtideDotNet.Core.Operators.LateMaterialization
{
    internal static class RowReference
    {
        public const int SlotBits = 16;
        public const int MaxRowsPerPage = 1 << SlotBits;
        public const long MaxSequence = (1L << (64 - SlotBits)) - 1;

        public static void ThrowIfExhausted(long sequence, string heap)
        {
            if (sequence > MaxSequence)
            {
                throw new InvalidOperationException($"Row heap '{heap}' has used all {MaxSequence} page sequences.");
            }
        }

        public static ulong Pack(long sequence, int slot)
        {
            return ((ulong)sequence << SlotBits) | (uint)slot;
        }

        public static long Sequence(ulong reference)
        {
            return (long)(reference >> SlotBits);
        }

        public static int Slot(ulong reference)
        {
            return (int)(reference & (MaxRowsPerPage - 1));
        }
    }
}
