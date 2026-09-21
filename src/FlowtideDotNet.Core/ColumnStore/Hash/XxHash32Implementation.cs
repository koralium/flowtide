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
using System.Buffers.Binary;
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
            return 3479547966;
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

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static unsafe uint Hash(ReadOnlySpan<byte> source)
        {
            int length = source.Length;
            uint h32;

            fixed (byte* pSource = &MemoryMarshal.GetReference(source))
            {
                byte* ptr = pSource;
                byte* end = pSource + length;

                if (length >= 16)
                {
                    byte* limit = end - 16;
                    uint v1 = unchecked(Prime32_1 + Prime32_2);
                    uint v2 = Prime32_2;
                    uint v3 = 0;
                    uint v4 = unchecked(0 - Prime32_1);

                    do
                    {
                        v1 = Round(v1, ReadUInt32LE(ptr));
                        ptr += 4;
                        v2 = Round(v2, ReadUInt32LE(ptr));
                        ptr += 4;
                        v3 = Round(v3, ReadUInt32LE(ptr));
                        ptr += 4;
                        v4 = Round(v4, ReadUInt32LE(ptr));
                        ptr += 4;
                    }
                    while (ptr <= limit);

                    h32 = BitOperations.RotateLeft(v1, 1) +
                          BitOperations.RotateLeft(v2, 7) +
                          BitOperations.RotateLeft(v3, 12) +
                          BitOperations.RotateLeft(v4, 18);
                }
                else
                {
                    h32 = Prime32_5;
                }

                h32 += (uint)length;

                // 4-bytes chunks
                while (ptr <= end - 4)
                {
                    h32 += ReadUInt32LE(ptr) * Prime32_3;
                    h32 = BitOperations.RotateLeft(h32, 17) * Prime32_4;
                    ptr += 4;
                }

                // 1-byte chunks
                while (ptr < end)
                {
                    h32 += *ptr * Prime32_5;
                    h32 = BitOperations.RotateLeft(h32, 11) * Prime32_1;
                    ptr++;
                }
            }

            // Avalanche finalization
            h32 ^= h32 >> 15;
            h32 *= Prime32_2;
            h32 ^= h32 >> 13;
            h32 *= Prime32_3;
            h32 ^= h32 >> 16;

            return h32;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static uint Round(uint seed, uint input)
        {
            return BitOperations.RotateLeft(seed + input * Prime32_2, 13) * Prime32_1;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static unsafe uint ReadUInt32LE(byte* ptr)
        {
            uint val = Unsafe.ReadUnaligned<uint>(ptr);
            if (!BitConverter.IsLittleEndian)
            {
                val = BinaryPrimitives.ReverseEndianness(val);
            }
            return val;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe static void AppendByte(byte value, ref Xxh32RowState state)
        {
            int holdbackIndex = (int)(state.Length & 15);
            state.Holdback[holdbackIndex] = value;
            state.Length++;

            if (holdbackIndex == 15)
            {
                state.ProcessStripe(MemoryMarshal.CreateReadOnlySpan(ref state.Holdback[0], 16));
            }
        }

        public static readonly uint HashTrue = XxHash32Implementation.HashSingleByte(1);
        public static readonly uint HashFalse = XxHash32Implementation.HashSingleByte(0);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static uint HashSingleByte(byte value)
        {
            uint h32 = Prime32_5 + 1U;
            h32 += value * Prime32_5;
            h32 = BitOperations.RotateLeft(h32, 11) * Prime32_1;

            // Avalanche finalization
            h32 ^= h32 >> 15;
            h32 *= Prime32_2;
            h32 ^= h32 >> 13;
            h32 *= Prime32_3;
            h32 ^= h32 >> 16;

            return h32;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static unsafe uint HashDecimal(in decimal value)
        {
            ref uint ptr = ref Unsafe.As<decimal, uint>(ref Unsafe.AsRef(in value));

            uint w0 = ptr;
            uint w1 = Unsafe.Add(ref ptr, 1);
            uint w2 = Unsafe.Add(ref ptr, 2);
            uint w3 = Unsafe.Add(ref ptr, 3);

            if (!BitConverter.IsLittleEndian)
            {
                w0 = BinaryPrimitives.ReverseEndianness(w0);
                w1 = BinaryPrimitives.ReverseEndianness(w1);
                w2 = BinaryPrimitives.ReverseEndianness(w2);
                w3 = BinaryPrimitives.ReverseEndianness(w3);
            }

            uint v1 = Round(unchecked(Prime32_1 + Prime32_2), w0);
            uint v2 = Round(Prime32_2, w1);
            uint v3 = Round(0, w2);
            uint v4 = Round(unchecked(0 - Prime32_1), w3);

            uint h32 = BitOperations.RotateLeft(v1, 1) +
                       BitOperations.RotateLeft(v2, 7) +
                       BitOperations.RotateLeft(v3, 12) +
                       BitOperations.RotateLeft(v4, 18);

            h32 += 16U;

            // Avalanche finalization
            h32 ^= h32 >> 15;
            h32 *= Prime32_2;
            h32 ^= h32 >> 13;
            h32 *= Prime32_3;
            h32 ^= h32 >> 16;

            return h32;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static unsafe void AppendDecimal(in decimal value, ref Xxh32RowState state)
        {
            int holdbackOffset = (int)(state.Length & 15);
            state.Length += 16;

            ref byte pVal = ref Unsafe.As<decimal, byte>(ref Unsafe.AsRef(in value));
            if (holdbackOffset == 0)
            {
                fixed (byte* ptr = &pVal)
                {
                    state.ProcessStripe(new ReadOnlySpan<byte>(ptr, 16));
                }
                return;
            }

            fixed (byte* pHoldback = state.Holdback)
            fixed (byte* pSrc = &pVal)
            {
                int bytesToStripe = 16 - holdbackOffset;
                Buffer.MemoryCopy(pSrc, pHoldback + holdbackOffset, bytesToStripe, bytesToStripe);
                state.ProcessStripe(new ReadOnlySpan<byte>(pHoldback, 16));
                Buffer.MemoryCopy(pSrc + bytesToStripe, pHoldback, holdbackOffset, holdbackOffset);
            }
        }
    }
}
