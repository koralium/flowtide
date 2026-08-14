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
//
// Port of mimalloc v3.4.4 `include/mimalloc/bits.h`.
// Original: Copyright (c) 2019-2024 Microsoft Research, Daan Leijen (MIT license).
//
// This port targets 64-bit only (MI_SIZE_BITS == 64). All bit operations map to
// System.Numerics.BitOperations which the JIT compiles to hardware intrinsics
// (TZCNT/LZCNT/POPCNT on x64, RBIT+CLZ/CNT on arm64). The C semantics for zero
// input (mi_ctz(0) == mi_clz(0) == 64) match BitOperations exactly.

using System.Numerics;
using System.Runtime.CompilerServices;

namespace FlowtideDotNet.MiMalloc
{
    internal static unsafe partial class Mi
    {
        // ------------------------------------------------------
        // Size of a pointer / size_t  (64-bit only)
        // ------------------------------------------------------

        public const int MI_INTPTR_SHIFT = 3;
        public const int MI_SIZE_SHIFT = 3;

        public const int MI_INTPTR_SIZE = 1 << MI_INTPTR_SHIFT;   // 8
        public const int MI_INTPTR_BITS = MI_INTPTR_SIZE * 8;     // 64
        public const int MI_SIZE_SIZE = 1 << MI_SIZE_SHIFT;       // 8
        public const int MI_SIZE_BITS = MI_SIZE_SIZE * 8;         // 64

        public const nuint MI_KiB = 1024;
        public const nuint MI_MiB = MI_KiB * MI_KiB;
        public const nuint MI_GiB = MI_MiB * MI_KiB;

        // maximum virtual address bits in a user-space pointer.
        // C uses 47 on x64 and 48 otherwise; we use 48 to cover both x64 and arm64
        // with a single configuration (only affects page-map sizing).
        public const int MI_MAX_VABITS = 48;
        // how many bits of the address are always mapped in the page_map
        public const int MI_MIN_VABITS = 43;   // 8 TiB

        // use a flat page-map or a 2-level one; MI_MAX_VABITS > 40 => two-level.
        public const bool MI_PAGE_MAP_FLAT = false;

        // ------------------------------------------------------
        // Popcount and count trailing/leading zero's
        // ------------------------------------------------------

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static nuint mi_popcount(nuint x) => (nuint)BitOperations.PopCount(x);

        // count trailing zeros; returns MI_SIZE_BITS (64) if `x == 0`
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static nuint mi_ctz(nuint x) => (nuint)BitOperations.TrailingZeroCount(x);

        // count leading zeros; returns MI_SIZE_BITS (64) if `x == 0`
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static nuint mi_clz(nuint x) => (nuint)BitOperations.LeadingZeroCount((ulong)x);

        // ------------------------------------------------------
        // find trailing/leading zero  (bit scan forward/reverse)
        // ------------------------------------------------------

        // Bit scan forward: find the least significant bit that is set (i.e. count trailing zero's)
        // return false if `x==0` (with `idx` undefined) and true otherwise,
        // with the `idx` set to the bit index (`0 <= idx < MI_SIZE_BITS`).
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool mi_bsf(nuint x, out nuint idx)
        {
            idx = mi_ctz(x);
            return x != 0;
        }

        // Bit scan reverse: find the most significant bit that is set
        // return false if `x==0` (with `idx` undefined) and true otherwise,
        // with the `idx` set to the bit index (`0 <= idx < MI_SIZE_BITS`).
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool mi_bsr(nuint x, out nuint idx)
        {
            idx = (nuint)MI_SIZE_BITS - 1 - mi_clz(x);
            return x != 0;
        }

        // ------------------------------------------------------
        // rotate
        // ------------------------------------------------------

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static nuint mi_rotr(nuint x, nuint r) => (nuint)BitOperations.RotateRight((ulong)x, (int)r);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static nuint mi_rotl(nuint x, nuint r) => (nuint)BitOperations.RotateLeft((ulong)x, (int)r);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static uint mi_rotl32(uint x, uint r) => BitOperations.RotateLeft(x, (int)r);
    }
}
