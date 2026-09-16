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
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.ColumnStore.Hash
{
    /// <summary>
    /// Xxhash32 state for a single row. Total bytes = 16 + 8 + 16 + 4 = 44 bytes. The struct is padded to 48 bytes for alignment.
    /// </summary>
    public unsafe struct Xxh32RowState
    {
        public const uint Prime32_1 = 0x9E3779B1U;
        public const uint Prime32_2 = 0x85EBCA77U;
        public const uint Prime32_3 = 0xC2B2AE3DU;
        public const uint Prime32_4 = 0x27D4EB2FU;
        public const uint Prime32_5 = 0x165667B1U;

        public const uint SmallAcc = Prime32_5;

        private const int StripeSize = 4 * sizeof(uint);

        public uint Acc1, Acc2, Acc3, Acc4; // Accumulators, 16 bytes
        public int Length;      // Total length, 4 bytes

        // Holdback for last 16 unprocessed bytes
        public fixed byte Holdback[16];

        public void Init()
        {
            Acc1 = unchecked(Prime32_1 + Prime32_2);
            Acc2 = Prime32_2;
            Acc3 = 0; // Seed
            Acc4 = unchecked(0u - Prime32_1);
            Length = 0;
        }

        internal void ProcessStripe(ReadOnlySpan<byte> source)
        {
            Debug.Assert(source.Length >= StripeSize);
            source = source.Slice(0, StripeSize);

            Acc1 = ApplyRound(Acc1, source);
            Acc2 = ApplyRound(Acc2, source.Slice(sizeof(uint)));
            Acc3 = ApplyRound(Acc3, source.Slice(2 * sizeof(uint)));
            Acc4 = ApplyRound(Acc4, source.Slice(3 * sizeof(uint)));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static uint ApplyRound(uint acc, ReadOnlySpan<byte> lane)
        {
            acc += BinaryPrimitives.ReadUInt32LittleEndian(lane) * Prime32_2;
            acc = BitOperations.RotateLeft(acc, 13);
            acc *= Prime32_1;

            return acc;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void ProcessStripeVector(ReadOnlySpan<byte> source)
        {
            Vector128<uint> lanes = Vector128.LoadUnsafe(ref MemoryMarshal.GetReference(source)).AsUInt32();
            Vector128<uint> vPrime2 = Vector128.Create(Prime32_2);
            Vector128<uint> vAcc = Accumulators + (lanes * vPrime2);
            vAcc = (vAcc << 13) | (vAcc >>> 19);
            Vector128<uint> vPrime1 = Vector128.Create(Prime32_1);
            Accumulators = vAcc * vPrime1;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private readonly uint Converge()
        {
            return
                BitOperations.RotateLeft(Acc1, 1) +
                BitOperations.RotateLeft(Acc2, 7) +
                BitOperations.RotateLeft(Acc3, 12) +
                BitOperations.RotateLeft(Acc4, 18);
        }

        internal readonly uint Complete(int length, ReadOnlySpan<byte> remaining)
        {
            uint acc = Length >= 16 ? Converge() : SmallAcc;

            acc += (uint)length;

            while (remaining.Length >= sizeof(uint))
            {
                uint lane = BinaryPrimitives.ReadUInt32LittleEndian(remaining);
                acc += lane * Prime32_3;
                acc = BitOperations.RotateLeft(acc, 17);
                acc *= Prime32_4;

                remaining = remaining.Slice(sizeof(uint));
            }

            for (int i = 0; i < remaining.Length; i++)
            {
                uint lane = remaining[i];
                acc += lane * Prime32_5;
                acc = BitOperations.RotateLeft(acc, 11);
                acc *= Prime32_1;
            }

            acc ^= (acc >> 15);
            acc *= Prime32_2;
            acc ^= (acc >> 13);
            acc *= Prime32_3;
            acc ^= (acc >> 16);

            return acc;
        }
    }
}
