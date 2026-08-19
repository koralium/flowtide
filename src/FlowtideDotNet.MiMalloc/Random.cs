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
// Port of mimalloc v3.4.4 `src/random.c` (chacha20-based PRNG) and the
// `_mi_random_shuffle` helper from `include/mimalloc/internal.h`.
// Original: Copyright (c) 2019-2021 Microsoft Research, Daan Leijen (MIT license).

namespace FlowtideDotNet.MiMalloc
{
    internal static unsafe partial class Mi
    {
        private const int MI_CHACHA_ROUNDS = 20;

        /* ----------------------------------------------------------------------------
        Chacha20 implementation as the original algorithm with a 64-bit nonce
        and counter: https://en.wikipedia.org/wiki/Salsa20
        The input matrix has sixteen 32-bit values:
        Position  0 to  3: constant key
        Position  4 to 11: the key
        Position 12 to 13: the counter.
        Position 14 to 15: the nonce.
        -----------------------------------------------------------------------------*/

        // note: public (class is internal) so tests can check the RFC 8439 vectors
        public static void qround(uint* x, nuint a, nuint b, nuint c, nuint d)
        {
            x[a] += x[b]; x[d] = mi_rotl32(x[d] ^ x[a], 16);
            x[c] += x[d]; x[b] = mi_rotl32(x[b] ^ x[c], 12);
            x[a] += x[b]; x[d] = mi_rotl32(x[d] ^ x[a], 8);
            x[c] += x[d]; x[b] = mi_rotl32(x[b] ^ x[c], 7);
        }

        public static void chacha_block(mi_random_ctx_t* ctx)
        {
            // scramble into `x`
            uint* x = stackalloc uint[16];
            for (nuint i = 0; i < 16; i++)
            {
                x[i] = ctx->input[i];
            }
            for (nuint i = 0; i < MI_CHACHA_ROUNDS; i += 2)
            {
                qround(x, 0, 4, 8, 12);
                qround(x, 1, 5, 9, 13);
                qround(x, 2, 6, 10, 14);
                qround(x, 3, 7, 11, 15);
                qround(x, 0, 5, 10, 15);
                qround(x, 1, 6, 11, 12);
                qround(x, 2, 7, 8, 13);
                qround(x, 3, 4, 9, 14);
            }

            // add scrambled data to the initial state
            for (nuint i = 0; i < 16; i++)
            {
                ctx->output[i] = x[i] + ctx->input[i];
            }
            ctx->output_available = 16;

            // increment the counter for the next round
            ctx->input[12] += 1;
            if (ctx->input[12] == 0)
            {
                ctx->input[13] += 1;
                if (ctx->input[13] == 0)   // and keep increasing into the nonce
                {
                    ctx->input[14] += 1;
                }
            }
        }

        private static uint chacha_next32(mi_random_ctx_t* ctx)
        {
            if (ctx->output_available <= 0)
            {
                chacha_block(ctx);
                ctx->output_available = 16;   // (assign again to suppress static analysis warning)
            }
            uint x = ctx->output[16 - ctx->output_available];
            ctx->output[16 - ctx->output_available] = 0;   // reset once the data is handed out
            ctx->output_available--;
            return x;
        }

        private static uint read32(byte* p, nuint idx32)
        {
            nuint i = 4 * idx32;
            return (uint)p[i + 0] | ((uint)p[i + 1] << 8) | ((uint)p[i + 2] << 16) | ((uint)p[i + 3] << 24);
        }

        // "expand 32-byte k" in ASCII
        private static ReadOnlySpan<byte> mi_chacha_sigma => "expand 32-byte k"u8;

        private static void chacha_init(mi_random_ctx_t* ctx, byte* key, ulong nonce)
        {
            // since we only use chacha for randomness (and not encryption) we
            // do not _need_ to read 32-bit values as little endian but we do anyways
            // just for being compatible :-)
            ctx->output_available = 0;
            for (int i = 0; i < 16; i++) { ctx->output[i] = 0; }
            fixed (byte* sigma = mi_chacha_sigma)
            {
                for (nuint i = 0; i < 4; i++)
                {
                    ctx->input[i] = read32(sigma, i);
                }
            }
            for (nuint i = 0; i < 8; i++)
            {
                ctx->input[i + 4] = read32(key, i);
            }
            ctx->input[12] = 0;
            ctx->input[13] = 0;
            ctx->input[14] = (uint)nonce;
            ctx->input[15] = (uint)(nonce >> 32);
        }

        private static void chacha_split(mi_random_ctx_t* ctx, ulong nonce, mi_random_ctx_t* ctx_new)
        {
            _mi_memzero(ctx_new, (nuint)sizeof(mi_random_ctx_t));
            ctx_new->weak = ctx->weak;
            for (int i = 0; i < 16; i++) { ctx_new->input[i] = ctx->input[i]; }
            ctx_new->input[12] = 0;
            ctx_new->input[13] = 0;
            ctx_new->input[14] = (uint)nonce;
            ctx_new->input[15] = (uint)(nonce >> 32);
            mi_assert_internal(ctx->input[14] != ctx_new->input[14] || ctx->input[15] != ctx_new->input[15]);   // do not reuse nonces!
            chacha_block(ctx_new);
        }

        /* ----------------------------------------------------------------------------
        Random interface
        -----------------------------------------------------------------------------*/

        public static void _mi_random_split(mi_random_ctx_t* ctx, mi_random_ctx_t* ctx_new)
        {
            mi_assert_internal(ctx != ctx_new);
            nuint nonce_rnd = _mi_random_next(ctx);
            chacha_split(ctx, (ulong)((nuint)ctx_new ^ nonce_rnd) /*nonce*/, ctx_new);
        }

        public static nuint _mi_random_next(mi_random_ctx_t* ctx)
        {
            nuint r;
            do
            {
                // MI_INTPTR_SIZE == 8
                r = ((nuint)chacha_next32(ctx) << 32) | chacha_next32(ctx);
            } while (r == 0);
            return r;
        }

        /* ----------------------------------------------------------------------------
        To initialize a fresh random context.
        If we cannot get good randomness, we fall back to weak randomness based on a timer and ASLR.
        -----------------------------------------------------------------------------*/

        // note: C uses the (ASLR-random) address of a function; we use a per-process
        // random-ish seed base allocated in native memory (its address serves the same role).
        private static readonly nuint mi_random_weak_seed_base = (nuint)System.Runtime.InteropServices.NativeMemory.Alloc(8);

        public static nuint _mi_os_random_weak(nuint extra_seed)
        {
            nuint x = mi_random_weak_seed_base ^ extra_seed;   // ASLR makes the address random
            x ^= (nuint)(ulong)_mi_prim_clock_now();
            // and do a few randomization steps
            nuint max = ((x ^ (x >> 17)) & 0x0F) + 1;
            for (nuint i = 0; i < max || x == 0; i++, x++)
            {
                x = _mi_random_shuffle(x);
            }
            mi_assert_internal(x != 0);
            return x;
        }

        private static void mi_random_init_ex(mi_random_ctx_t* ctx, bool use_weak)
        {
            byte* key = stackalloc byte[32];
            if (use_weak || !_mi_prim_random_buf(key, 32))
            {
                // if we fail to get random data from the OS, we fall back to a
                // weak random source based on the current time
                if (!use_weak) { _mi_warning_message("unable to use secure randomness"); }
                nuint x = _mi_os_random_weak(0);
                for (nuint i = 0; i < 32; i += 4, x++)
                {
                    x = _mi_random_shuffle(x);
                    key[i] = (byte)x;
                    key[i + 1] = (byte)(x >> 8);
                    key[i + 2] = (byte)(x >> 16);
                    key[i + 3] = (byte)(x >> 24);
                }
                ctx->weak = true;
            }
            else
            {
                ctx->weak = false;
            }
            chacha_init(ctx, key, (ulong)(nuint)ctx /*nonce*/);
            for (int i = 0; i < 32; i++) { key[i] = 0; }
        }

        public static void _mi_random_init(mi_random_ctx_t* ctx) => mi_random_init_ex(ctx, false);

        public static void _mi_random_init_weak(mi_random_ctx_t* ctx) => mi_random_init_ex(ctx, true);

        public static void _mi_random_reinit_if_weak(mi_random_ctx_t* ctx)
        {
            if (ctx->weak)
            {
                _mi_random_init(ctx);
            }
        }

        // -------------------------------------------------------------------
        // Fast "random" shuffle (from internal.h)
        // -------------------------------------------------------------------

        public static nuint _mi_random_shuffle(nuint x)
        {
            if (x == 0) { x = 17; }   // ensure we don't get stuck in generating zeros
            // by Sebastiano Vigna, see: <http://xoshiro.di.unimi.it/splitmix64.c>
            x ^= x >> 30;
            x = unchecked(x * (nuint)0xbf58476d1ce4e5b9UL);
            x ^= x >> 27;
            x = unchecked(x * (nuint)0x94d049bb133111ebUL);
            x ^= x >> 31;
            return x;
        }
    }
}
