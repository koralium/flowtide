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

using SqlParser;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.ColumnStore.Hash
{
    internal class XxHash32Implementation
    {
        public const uint Prime32_1 = 0x9E3779B1U;
        public const uint Prime32_2 = 0x85EBCA77U;
        public const uint Prime32_3 = 0xC2B2AE3DU;
        public const uint Prime32_4 = 0x27D4EB2FU;
        public const uint Prime32_5 = 0x165667B1U;

        private const int HashSize = sizeof(uint);
        private const int StripeSize = 4 * sizeof(uint);

        public static unsafe void Append(ReadOnlySpan<byte> source, ref Xxh32RowState state)
        {
            int held = state.Length & 0x0F;

            if (held != 0)
            {
                int remain = StripeSize - held;

                if (source.Length >= remain)
                {
                    fixed (byte* pSource = source)
                    fixed (byte* pHoldback = state.Holdback)
                    {
                        NativeMemory.Copy(pSource, pHoldback + held, (nuint)remain);
                    }
                    fixed(byte* pHoldback = state.Holdback)
                    {
                        state.ProcessStripe(new ReadOnlySpan<byte>(pHoldback, StripeSize));
                    }

                    source = source.Slice(remain);
                    state.Length += remain;
                }
                else
                {
                    fixed (byte* pSource = source)
                    fixed (byte* pHoldback = state.Holdback)
                    {
                        NativeMemory.Copy(pSource, pHoldback + held, (nuint)source.Length);
                    }
                    state.Length += source.Length;
                    return;
                }
            }

            while (source.Length >= StripeSize)
            {
                state.ProcessStripe(source);
                source = source.Slice(StripeSize);
                state.Length += StripeSize;
            }

            if (source.Length > 0)
            {
                fixed (byte* pHoldback = state.Holdback)
                {
                    var holdbackSpan = new Span<byte>(pHoldback, 16);
                    source.CopyTo(holdbackSpan);
                }
                state.Length += source.Length;
            }
        }

        public static unsafe void AppendLong(ReadOnlySpan<byte> source, ref Xxh32RowState state)
        {
            int held = state.Length & 0x0F;

            if (held != 0)
            {
                int remain = StripeSize - held;

                if (source.Length >= remain)
                {
                    fixed (byte* pSource = source)
                    fixed (byte* pHoldback = state.Holdback)
                    {
                        NativeMemory.Copy(pSource, pHoldback + held, (nuint)remain);
                    }
                    fixed (byte* pHoldback = state.Holdback)
                    {
                        state.ProcessStripe(new ReadOnlySpan<byte>(pHoldback, StripeSize));
                    }

                    source = source.Slice(remain);
                    state.Length += remain;
                }
                else
                {
                    fixed (byte* pSource = source)
                    fixed (byte* pHoldback = state.Holdback)
                    {
                        NativeMemory.Copy(pSource, pHoldback + held, (nuint)source.Length);
                    }
                    state.Length += source.Length;
                    return;
                }
            }

            if (source.Length > 0)
            {
                fixed (byte* pHoldback = state.Holdback)
                {
                    Unsafe.WriteUnaligned(pHoldback, Unsafe.ReadUnaligned<long>(ref MemoryMarshal.GetReference(source)));
                }
                state.Length += source.Length;
            }
        }

        public static unsafe uint GetCurrentHashAsUInt32(ref Xxh32RowState state)
        {
            int remainingLength = state.Length & 0x0F;
            ReadOnlySpan<byte> remaining = ReadOnlySpan<byte>.Empty;

            fixed (byte* pHoldback = state.Holdback)
            {
                remaining = new ReadOnlySpan<byte>(pHoldback, remainingLength);
                return state.Complete(state.Length, remaining);
            }
        }

        public static uint HashSingleLong(long value)
        {
            uint h32 = Prime32_5 + 8;

            uint lane1 = (uint)(value & 0xFFFFFFFF);
            uint lane2 = (uint)(value >> 32);

            h32 += lane1 * Prime32_3;
            h32 = BitOperations.RotateLeft(h32, 17) * Prime32_4;

            h32 += lane2 * Prime32_3;
            h32 = BitOperations.RotateLeft(h32, 17) * Prime32_4;

            h32 ^= h32 >> 15;
            h32 *= Prime32_2;
            h32 ^= h32 >> 13;
            h32 *= Prime32_3;
            h32 ^= h32 >> 16;

            return h32;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static uint HashNullByte()
        {
            return 0x2A993D71U;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static unsafe void AppendLong(long value, ref Xxh32RowState state)
        {
            int held = state.Length & 0x0F;
            ulong val = (ulong)value;

            fixed (byte* pHoldback = state.Holdback)
            {
                if (held <= 8)
                {
                    Unsafe.WriteUnaligned(pHoldback + held, val);

                    if (held == 8)
                    {
                        state.ProcessStripe(new ReadOnlySpan<byte>(pHoldback, 16));
                    }
                }
                else
                {
                    int remain = 16 - held;
                    for (int j = 0; j < remain; j++)
                    {
                        pHoldback[held + j] = (byte)(val >> (j * 8));
                    }
                    state.ProcessStripe(new ReadOnlySpan<byte>(pHoldback, 16));
                    ulong overflow = val >> (remain * 8);
                    Unsafe.WriteUnaligned(pHoldback, overflow);
                }
            }

            state.Length += 8;
        }
    }
}
