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

// Tests for Bits.cs against the semantics documented in mimalloc's bits.h:
// mi_ctz(0) == mi_clz(0) == MI_SIZE_BITS, mi_bsf/mi_bsr return false on 0, etc.

using Xunit;
using static FlowtideDotNet.MiMalloc.Mi;

namespace FlowtideDotNet.MiMalloc.Tests
{
    public class BitsTests
    {
        [Fact]
        public void Constants_Are64Bit()
        {
            Assert.Equal(8, MI_INTPTR_SIZE);
            Assert.Equal(64, MI_SIZE_BITS);
            Assert.Equal((nuint)1024, MI_KiB);
            Assert.Equal((nuint)(1024 * 1024), MI_MiB);
        }

        [Fact]
        public void Popcount_MatchesReference()
        {
            Assert.Equal((nuint)0, mi_popcount(0));
            Assert.Equal((nuint)1, mi_popcount(1));
            Assert.Equal((nuint)64, mi_popcount(nuint.MaxValue));
            Assert.Equal((nuint)32, mi_popcount(unchecked((nuint)0x5555555555555555ul)));
            Assert.Equal((nuint)2, mi_popcount(unchecked((nuint)0x8000000000000001ul)));
        }

        [Fact]
        public void Ctz_ZeroReturnsSizeBits()
        {
            Assert.Equal((nuint)64, mi_ctz(0));
            Assert.Equal((nuint)0, mi_ctz(1));
            Assert.Equal((nuint)63, mi_ctz(unchecked((nuint)0x8000000000000000ul)));
            Assert.Equal((nuint)4, mi_ctz(0x10));
        }

        [Fact]
        public void Clz_ZeroReturnsSizeBits()
        {
            Assert.Equal((nuint)64, mi_clz(0));
            Assert.Equal((nuint)63, mi_clz(1));
            Assert.Equal((nuint)0, mi_clz(unchecked((nuint)0x8000000000000000ul)));
        }

        [Fact]
        public void Bsf_FindsLeastSignificantSetBit()
        {
            Assert.False(mi_bsf(0, out _));
            Assert.True(mi_bsf(0b1000, out nuint idx));
            Assert.Equal((nuint)3, idx);
            Assert.True(mi_bsf(nuint.MaxValue, out idx));
            Assert.Equal((nuint)0, idx);
        }

        [Fact]
        public void Bsr_FindsMostSignificantSetBit()
        {
            Assert.False(mi_bsr(0, out _));
            Assert.True(mi_bsr(0b1000, out nuint idx));
            Assert.Equal((nuint)3, idx);
            Assert.True(mi_bsr(nuint.MaxValue, out idx));
            Assert.Equal((nuint)63, idx);
        }

        [Fact]
        public void Rotate_MatchesCSemantics()
        {
            Assert.Equal(unchecked((nuint)0x8000000000000000ul), mi_rotr(1, 1));
            Assert.Equal((nuint)1, mi_rotl(unchecked((nuint)0x8000000000000000ul), 1));
            // rotation by 0 and by 64 are identity (C code avoids UB explicitly here)
            Assert.Equal((nuint)0x1234, mi_rotr(0x1234, 0));
            Assert.Equal((nuint)0x1234, mi_rotr(0x1234, 64));
            Assert.Equal((nuint)0x1234, mi_rotl(0x1234, 0));
            Assert.Equal((nuint)0x1234, mi_rotl(0x1234, 64));
            Assert.Equal(0x80000000u, mi_rotl32(0x40000000u, 1));
            Assert.Equal(1u, mi_rotl32(0x80000000u, 1));
        }

        [Fact]
        public void ExhaustiveSingleBit_CtzClzBsfBsr()
        {
            for (int i = 0; i < 64; i++)
            {
                nuint x = (nuint)1 << i;
                Assert.Equal((nuint)i, mi_ctz(x));
                Assert.Equal((nuint)(63 - i), mi_clz(x));
                Assert.True(mi_bsf(x, out nuint bsf));
                Assert.Equal((nuint)i, bsf);
                Assert.True(mi_bsr(x, out nuint bsr));
                Assert.Equal((nuint)i, bsr);
                Assert.Equal((nuint)1, mi_popcount(x));
            }
        }
    }
}
